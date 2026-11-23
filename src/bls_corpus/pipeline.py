"""Download every LABSTAT catalog and write one JSON file per series."""
from __future__ import annotations

import argparse
import codecs
import shutil
import tempfile
import hashlib
import json
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path

import duckdb
import httpx

# -- settings --------------------------------------------------------------

@dataclass
class AppConfig:
    program: str = ""
    source_root: str = "https://download.bls.gov/pub/time.series"
    data_dir: Path = Path("data")
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; BLS-Corpus-Research/1.0; "
        "+https://www.bls.gov/data/) bls-corpus/0.1"
    )
    concurrency: int = 1
    # Minimum gap between any two requests to BLS in a run (listings and files).
    # A throttle doubles it up to max_interval_s; successes ease it back down.
    min_interval_s: float = 0.5
    max_interval_s: float = 10.0
    connect_timeout_s: float = 15.0
    read_timeout_s: float = 120.0
    memory_limit: str = "auto"
    threads: int = 2
    max_retries: int = 5
    backoff_base_s: float = 1.0
    backoff_max_s: float = 60.0
    # Directory listings are one small request per catalog; wait longer between
    # tries so a server-side throttle has time to clear.
    listing_backoff_base_s: float = 5.0
    listing_backoff_max_s: float = 120.0
    # After a catalog attempt is lost to throttling, pause before the next attempt.
    # The pause doubles for each throttled attempt in a row, up to
    # throttle_cooldown_max_s, so an unattended run waits out a long block (about
    # 2.5 hours at the defaults) before stopping. 0 means never stop.
    throttle_cooldown_s: float = 300.0
    throttle_cooldown_max_s: float = 1800.0
    max_consecutive_throttled: int = 8
    # A throttled catalog is tried again after the pause instead of being dropped;
    # files that already downloaded are verified and skipped. Catalogs still
    # throttled after that get one more try at the end of the run. 0 disables both.
    throttled_catalog_retries: int = 1
    pacer: RequestPacer | None = field(default=None, repr=False, compare=False)

    @property
    def program_dir(self) -> str:
        return self.program.lower()

    @property
    def program_url(self) -> str:
        return f"{self.source_root.rstrip('/')}/{self.program_dir}/"

    @property
    def catalog_dir(self) -> Path:
        return self.data_dir / self.program_dir

    @property
    def raw_root(self) -> Path:
        return self.catalog_dir / "raw"

    @property
    def series_root(self) -> Path:
        return self.catalog_dir / "series"


class RequestPacer:
    """One request clock shared by every listing and download in a run.

    Requests are spaced at least ``interval`` seconds apart across all threads.
    A throttle blocks every caller until the server's wait has passed and doubles
    the interval; each success eases it back toward the base.
    """

    def __init__(self, min_interval_s: float, max_interval_s: float | None = None,
                 sleep=time.sleep, clock=time.monotonic):
        self.base = max(0.0, min_interval_s)
        self.max_interval = max(self.base, max_interval_s if max_interval_s is not None
                                else self.base)
        self.interval = self.base
        self._sleep = sleep
        self._clock = clock
        self._lock = threading.Lock()
        self._next_at = 0.0
        self._blocked_until = 0.0
        self._epoch = 0

    def wait(self) -> None:
        with self._lock:
            now = self._clock()
            start = max(now, self._next_at, self._blocked_until)
            self._next_at = start + self.interval
            epoch = self._epoch
        if start > now:
            self._sleep(start - now)
        # A throttle reported while this caller slept moves the start later.
        with self._lock:
            if self._epoch == epoch:
                return
            now = self._clock()
            start = max(now, self._blocked_until)
            self._next_at = max(self._next_at, start + self.interval)
        if start > now:
            self._sleep(start - now)

    def throttled(self, wait_s: float) -> None:
        with self._lock:
            self._blocked_until = max(self._blocked_until, self._clock() + max(0.0, wait_s))
            self.interval = min(max(self.interval * 2, self.base), self.max_interval)
            self._epoch += 1

    def succeeded(self) -> None:
        with self._lock:
            self.interval = max(self.base, self.interval * 0.9)


def load_config(path: str | Path | None = None) -> AppConfig:
    return AppConfig()


# -- listing ---------------------------------------------------------------

_LINE_RE = re.compile(
    r"(?P<date>\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s*[AP]M)\s+"
    r"(?P<size>&lt;dir&gt;|\d+)\s+"
    r'<A HREF="(?P<href>[^"]+)">(?P<name>[^<]+)</A>',
    re.IGNORECASE,
)
_KNOWN_ROLES: list[tuple[re.Pattern[str], str, bool]] = [
    (re.compile(r"\.series$"), "series_catalog", True),
    (re.compile(r"\.data\.\d+\..+$"), "observations", True),
    (re.compile(r"\.footnote$"), "footnotes", True),
    (re.compile(r"\.txt$"), "documentation", False),
    (re.compile(r"\.contacts$"), "documentation", False),
]
SKIP_ROOT_DIRS = {"compressed", "sdmx", "esbr"}
SKIP_PREFIXES = {"pf", "pi", "lf"}
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
THROTTLE_STATUS = {429, 503}


@dataclass
class FileEntry:
    name: str
    relative_path: str
    source_url: str
    remote_size: int | None = None
    listing_last_modified: str | None = None
    is_dir: bool = False
    file_role: str = "unclassified"
    required: bool = False

    def classify(self, program_dir: str) -> None:
        if self.is_dir:
            self.file_role = "directory"
            return
        for pattern, role, required in _KNOWN_ROLES:
            if pattern.search(self.name):
                self.file_role, self.required = role, required
                return
        if re.fullmatch(rf"{re.escape(program_dir)}\.[a-z0-9_]+", self.name):
            self.file_role, self.required = "mapping", True
            return
        self.file_role, self.required = "unclassified", False


@dataclass
class DiscoveryResult:
    program: str
    source_url: str
    fetched_at: str
    listing_sha256: str
    files: list[FileEntry] = field(default_factory=list)

    @property
    def required_missing(self) -> list[str]:
        roles = {f.file_role for f in self.files if not f.is_dir}
        missing = []
        if "series_catalog" not in roles:
            missing.append("series_catalog (*.series)")
        if "observations" not in roles:
            missing.append("observations (*.data.*)")
        return missing


class DiscoveryError(RuntimeError):
    pass


class ThrottledError(DiscoveryError):
    """The server kept refusing a directory listing after every retry."""


def fetch_listing(url: str, user_agent: str, timeout_s: float = 30.0, transport=None,
                  max_retries: int = 5, backoff_base_s: float = 5.0,
                  backoff_max_s: float = 120.0, sleep=time.sleep,
                  pacer: RequestPacer | None = None) -> str:
    """GET a directory listing, retrying throttles and transient errors with backoff."""
    pacer = pacer or RequestPacer(0.0, sleep=sleep)
    headers = {"User-Agent": user_agent, "Accept-Encoding": "identity"}
    last = ""
    with httpx.Client(headers=headers, timeout=timeout_s, transport=transport,
                      follow_redirects=True) as client:
        for attempt in range(max_retries + 1):
            retry_after = None
            throttled = False
            pacer.wait()
            try:
                resp = client.get(url)
                if resp.status_code not in RETRYABLE_STATUS:
                    resp.raise_for_status()
                    pacer.succeeded()
                    return resp.text
                last = f"HTTP {resp.status_code}"
                throttled = resp.status_code in THROTTLE_STATUS
                retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last = f"{type(exc).__name__}: {exc}"
            wait = _retry_wait(attempt, retry_after, backoff_base_s, backoff_max_s)
            if throttled:
                # Holds back every request in the run, including the next try here.
                pacer.throttled(wait)
            if attempt == max_retries:
                break
            print(f"  listing {url}: {last}; retry {attempt + 1}/{max_retries} "
                  f"in {wait:.0f}s", flush=True)
            if not throttled:
                sleep(wait)
    raise ThrottledError(f"{last} for {url} after {max_retries + 1} attempts")


def _retry_wait(attempt: int, retry_after: float | None, base_s: float, max_s: float) -> float:
    if retry_after is not None:
        return min(retry_after, 300.0)
    base = min(base_s * (2 ** attempt), max_s)
    return base + random.uniform(0, base * 0.25)


def parse_listing(html: str, program: str, source_url: str) -> DiscoveryResult:
    program_dir = program.lower()
    files: list[FileEntry] = []
    for m in _LINE_RE.finditer(html):
        name = m.group("name").strip()
        if name.startswith("[To Parent"):
            continue
        is_dir = m.group("size") == "&lt;dir&gt;"
        entry = FileEntry(
            name=name,
            relative_path=name,
            source_url=f"{source_url.rstrip('/')}/{name}",
            remote_size=None if is_dir else int(m.group("size")),
            listing_last_modified=m.group("date").strip(),
            is_dir=is_dir,
        )
        entry.classify(program_dir)
        files.append(entry)
    files.sort(key=lambda f: f.name)
    return DiscoveryResult(
        program=program.upper(),
        source_url=source_url,
        fetched_at=datetime.now(timezone.utc).isoformat(),
        listing_sha256=hashlib.sha256(html.encode()).hexdigest(),
        files=files,
    )


def discover_remote(program: str, source_url: str, user_agent: str,
                    timeout_s: float = 30.0, transport=None, **retry) -> DiscoveryResult:
    html = fetch_listing(source_url, user_agent, timeout_s, transport, **retry)
    result = parse_listing(html, program, source_url)
    if result.required_missing:
        raise DiscoveryError(
            f"Required LABSTAT file(s) missing for {program} at {source_url}: "
            + ", ".join(result.required_missing)
        )
    return result


def list_catalog_prefixes(html: str) -> list[str]:
    found: set[str] = set()
    for m in _LINE_RE.finditer(html):
        name = m.group("name").strip().rstrip("/")
        if name.startswith("[To Parent") or m.group("size") != "&lt;dir&gt;":
            continue
        low = name.lower()
        if re.fullmatch(r"[a-z]{2}", low) and low not in SKIP_ROOT_DIRS | SKIP_PREFIXES:
            found.add(low)
    return sorted(found)


def fetch_root_listing(source_root: str, user_agent: str,
                       timeout_s: float = 30.0, transport=None, **retry) -> tuple[str, list[str]]:
    url = source_root.rstrip("/") + "/"
    html = fetch_listing(url, user_agent, timeout_s, transport, **retry)
    return html, list_catalog_prefixes(html)


# -- download --------------------------------------------------------------

@dataclass
class FileRecord:
    relative_path: str
    source_url: str
    file_role: str = "unclassified"
    required: bool = False
    remote_size: int | None = None
    local_size: int | None = None
    last_modified: str | None = None
    etag: str | None = None
    sha256: str | None = None
    download_status: str = "pending"
    attempts: int = 0
    error_message: str | None = None
    http_status: int | None = None


@dataclass
class Manifest:
    program_id: str
    source_url: str
    created_utc: str
    listing_sha256: str = ""
    files: list[FileRecord] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    def save(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")
        os.replace(tmp, path)
        return path

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        files = [FileRecord(**{
            k: v for k, v in f.items() if k in FileRecord.__dataclass_fields__
        }) for f in raw.pop("files", [])]
        known = set(cls.__dataclass_fields__)
        raw = {k: v for k, v in raw.items() if k in known}
        return cls(files=files, **raw)


def new_manifest(discovery: DiscoveryResult) -> Manifest:
    manifest = Manifest(
        program_id=discovery.program,
        source_url=discovery.source_url,
        created_utc=datetime.now(timezone.utc).isoformat(),
        listing_sha256=discovery.listing_sha256,
    )
    for f in discovery.files:
        if f.is_dir:
            continue
        manifest.files.append(FileRecord(
            relative_path=f.relative_path,
            source_url=f.source_url,
            file_role=f.file_role,
            required=f.required,
            remote_size=f.remote_size,
            last_modified=f.listing_last_modified,
        ))
    return manifest


def sha256_file(path: Path, _buf: int = 64 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(_buf):
            h.update(chunk)
    return h.hexdigest()


def _listing_retry(cfg: AppConfig) -> dict:
    return {"max_retries": cfg.max_retries, "backoff_base_s": cfg.listing_backoff_base_s,
            "backoff_max_s": cfg.listing_backoff_max_s, "pacer": cfg.pacer}


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def _to_http_date(listing_or_http: str) -> str:
    for fmt in ("%m/%d/%Y %I:%M %p", "%a, %d %b %Y %H:%M:%S %Z"):
        try:
            dt = datetime.strptime(listing_or_http, fmt)
            return format_datetime(dt.replace(tzinfo=timezone.utc), usegmt=True)
        except ValueError:
            continue
    return listing_or_http


class _RetryableError(Exception):
    def __init__(self, msg: str, retry_after: float | None = None, throttled: bool = False):
        super().__init__(msg)
        self.retry_after = retry_after
        self.throttled = throttled


class _PermanentError(Exception):
    pass


class DownloadError(RuntimeError):
    pass


class Downloader:
    def __init__(self, cfg: AppConfig, transport=None, sleep=time.sleep,
                 client: httpx.Client | None = None):
        self.cfg = cfg
        self.sleep = sleep
        self.transport = transport
        self._client = client
        self.pacer = cfg.pacer or RequestPacer(cfg.min_interval_s, cfg.max_interval_s, sleep=sleep)

    def _backoff(self, attempt: int, retry_after: float | None) -> float:
        return _retry_wait(attempt, retry_after, self.cfg.backoff_base_s, self.cfg.backoff_max_s)

    def _client_or_new(self) -> httpx.Client:
        if self._client is not None:
            return self._client
        timeout = httpx.Timeout(connect=self.cfg.connect_timeout_s,
                                read=self.cfg.read_timeout_s,
                                write=self.cfg.read_timeout_s, pool=30.0)
        return httpx.Client(headers={"User-Agent": self.cfg.user_agent,
                                     "Accept-Encoding": "identity"},
                            timeout=timeout, transport=self.transport,
                            follow_redirects=True)

    def download_one(self, record: FileRecord, dest_dir: Path,
                     prior: FileRecord | None = None) -> FileRecord:
        client = self._client_or_new()
        try:
            return self._download_one(client, record, dest_dir, prior)
        finally:
            if self._client is None:
                client.close()

    def _download_one(self, client: httpx.Client, record: FileRecord, dest_dir: Path,
                      prior: FileRecord | None) -> FileRecord:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / record.relative_path
        part = dest.with_name(dest.name + ".part")
        headers: dict[str, str] = {}
        if (prior and prior.sha256 and prior.source_url == record.source_url and dest.exists()
                and dest.stat().st_size == prior.local_size
                and (record.remote_size is None or record.remote_size == prior.local_size)
                and sha256_file(dest) == prior.sha256):
            if prior.etag:
                headers["If-None-Match"] = prior.etag
            if prior.last_modified:
                headers["If-Modified-Since"] = _to_http_date(prior.last_modified)
            record.sha256, record.local_size = prior.sha256, prior.local_size
            record.etag = prior.etag
            if not headers and record.last_modified == prior.last_modified:
                record.download_status = "skipped_verified"
                return record
        last_error: str | None = None
        for attempt in range(self.cfg.max_retries + 1):
            record.attempts = attempt + 1
            self.pacer.wait()
            try:
                status = self._attempt(client, record, dest, part, dict(headers))
                self.pacer.succeeded()
                record.download_status = "skipped_not_modified" if status == "not_modified" else "downloaded"
                record.local_size = dest.stat().st_size
                record.sha256 = sha256_file(dest)
                record.error_message = None
                return record
            except _PermanentError as exc:
                record.download_status = "failed"
                record.error_message = f"permanent: {exc}"
                return record
            except _RetryableError as exc:
                last_error = str(exc)
                headers.clear()
                wait = self._backoff(attempt, exc.retry_after)
                if exc.throttled:
                    # Holds back every request in the run, including the next try here.
                    self.pacer.throttled(wait)
                elif attempt < self.cfg.max_retries:
                    self.sleep(wait)
            except Exception as exc:
                record.download_status = "failed"
                record.error_message = f"unexpected: {type(exc).__name__}: {exc}"
                return record
        record.download_status = "failed"
        record.error_message = f"exhausted retries: {last_error}"
        return record

    def _attempt(self, client: httpx.Client, record: FileRecord, dest: Path,
                 part: Path, headers: dict[str, str]) -> str:
        resume_meta = part.with_name(part.name + ".json")
        resume_from = part.stat().st_size if part.exists() else 0
        saved = {}
        if resume_from:
            try:
                saved = json.loads(resume_meta.read_text())
            except (OSError, ValueError):
                pass
            if (saved.get("source_url") != record.source_url or not saved.get("validator")
                    or saved.get("remote_size") != record.remote_size
                    or (record.remote_size is not None and resume_from >= record.remote_size)):
                # An unverified prefix may be from another source revision.
                part.unlink(missing_ok=True)
                resume_meta.unlink(missing_ok=True)
                resume_from = 0
            else:
                headers["Range"] = f"bytes={resume_from}-"
                headers["If-Range"] = saved["validator"]
        try:
            with client.stream("GET", record.source_url, headers=headers) as resp:
                record.http_status = resp.status_code
                if resp.status_code == 304:
                    if (not dest.exists() or not record.sha256
                            or sha256_file(dest) != record.sha256
                            or (record.remote_size is not None and dest.stat().st_size != record.remote_size)):
                        raise _RetryableError("304 without a verified complete local file")
                    return "not_modified"
                if resp.status_code in RETRYABLE_STATUS:
                    raise _RetryableError(f"HTTP {resp.status_code}",
                                          retry_after=_parse_retry_after(resp.headers.get("Retry-After")),
                                          throttled=resp.status_code in THROTTLE_STATUS)
                if resp.status_code == 416 and resume_from:
                    part.unlink(missing_ok=True)
                    resume_meta.unlink(missing_ok=True)
                    raise _RetryableError("HTTP 416: discarded stale partial file")
                if resp.status_code >= 400:
                    raise _PermanentError(f"HTTP {resp.status_code} for {record.source_url}")
                if resp.status_code not in (200, 206):
                    raise _PermanentError(f"unexpected HTTP {resp.status_code}")
                etag = resp.headers.get("ETag")
                modified = resp.headers.get("Last-Modified")
                validator = etag if etag and not etag.startswith("W/") else modified
                clen = resp.headers.get("Content-Length")
                length = int(clen) if clen is not None else None
                total = None
                append = resp.status_code == 206
                if append:
                    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", resp.headers.get("Content-Range", ""))
                    valid = bool(match and resume_from and headers.get("If-Range"))
                    if match:
                        start, end, total = map(int, match.groups())
                        valid = (valid and start == resume_from and start <= end < total
                                 and end == total - 1 and (length is None or length == end-start+1)
                                 and (record.remote_size is None or total == record.remote_size)
                                 and validator == saved.get("validator"))
                    if not valid:
                        part.unlink(missing_ok=True)
                        resume_meta.unlink(missing_ok=True)
                        raise _RetryableError("invalid or changed Content-Range/validator; restarting")
                else:
                    resume_from = 0
                    total = length
                record.etag, record.last_modified = etag, modified
                _write_json(resume_meta, {"source_url": record.source_url, "validator": validator,
                                          "remote_size": record.remote_size})
                written = resume_from
                with part.open("ab" if append else "wb") as handle:
                    for chunk in resp.iter_bytes(64 * 1024):
                        handle.write(chunk)
                        written += len(chunk)
                if total is not None and written != total:
                    raise _RetryableError(f"truncated: got {written} of {total} bytes")
                if record.remote_size is not None and written != record.remote_size:
                    part.unlink(missing_ok=True)
                    resume_meta.unlink(missing_ok=True)
                    raise _RetryableError(f"size mismatch vs listing: got {written}, listing={record.remote_size}")
        except (httpx.TimeoutException, httpx.TransportError, httpx.DecodingError) as exc:
            if isinstance(exc, httpx.DecodingError):
                part.unlink(missing_ok=True)
                resume_meta.unlink(missing_ok=True)
            raise _RetryableError(f"{type(exc).__name__}: {exc}") from exc
        part.replace(dest)
        resume_meta.unlink(missing_ok=True)
        return "downloaded"

    def download_all(self, manifest: Manifest, dest_dir: Path,
                     prior: Manifest | None = None,
                     only: set[str] | None = None, manifest_path: Path | None = None) -> Manifest:
        prior_by_path = {r.relative_path: r for r in prior.files} if prior else {}
        records = [r for r in manifest.files if only is None or r.relative_path in only]
        if manifest_path:
            manifest.save(manifest_path)
        try:
            workers = max(1, self.cfg.concurrency)
            if workers == 1 or len(records) <= 1:
                for r in records:
                    self.download_one(r, dest_dir, prior_by_path.get(r.relative_path))
                    if manifest_path:
                        manifest.save(manifest_path)
            else:
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futs = {pool.submit(self.download_one, r, dest_dir,
                                        prior_by_path.get(r.relative_path)): r for r in records}
                    for fut in as_completed(futs):
                        fut.result()
                        if manifest_path:
                            manifest.save(manifest_path)
        finally:
            if manifest_path:
                manifest.save(manifest_path)
        failed_required = [r for r in records if r.download_status == "failed" and r.required]
        if failed_required:
            names = ", ".join(r.relative_path for r in failed_required)
            raise DownloadError(f"Required file(s) failed to download: {names}")
        return manifest


# -- layout and write ------------------------------------------------------

INSPECTION_BYTES = 64 * 1024
PIPELINE_VERSION = 3
SHARD_MAX_BYTES = 128 * 1024 * 1024
SHARD_MAX_ROWS = 100_000


def _default_memory_limit() -> str:
    """Pick a DuckDB budget from the job's memory allocation, not the laptop default."""
    candidates: list[int] = []
    for path in (Path("/sys/fs/cgroup/memory.max"),
                 Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")):
        try:
            raw = path.read_text().strip()
            if raw and raw != "max":
                candidates.append(int(raw))
        except (OSError, ValueError):
            pass
    slurm = os.environ.get("SLURM_MEM_PER_NODE") or os.environ.get("SLURM_MEM_PER_CPU")
    if slurm:
        try:
            candidates.append(int(float(slurm) * 1024 * 1024))
        except ValueError:
            pass
    try:
        candidates.append(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))
    except (AttributeError, ValueError, OSError):
        pass
    bytes_ = min(candidates) if candidates else 1024 * 1024 * 1024
    budget = max(1024 * 1024 * 1024, int(bytes_ * 0.6))
    if budget >= 1024 ** 3:
        return f"{max(1, budget // (1024 ** 3))}GB"
    return f"{max(1, budget // (1024 ** 2))}MB"


def resolve_memory_limit(value: str | None) -> str:
    text = (value or "auto").strip()
    if not text or text.lower() == "auto":
        return _default_memory_limit()
    return text


def sample_file(path: Path, budget: int = INSPECTION_BYTES, windows: int = 4) -> list[bytes]:
    """Read at most budget bytes spread over the file, never materializing it."""
    if budget < windows * 4 or windows < 1:
        raise ValueError("sample budget must allow at least four bytes per window")
    size = path.stat().st_size
    with path.open("rb") as handle:
        if size <= budget:
            return [handle.read(budget)]
        width = budget // windows
        samples = []
        for i in range(windows):
            handle.seek((size - width) * i // max(1, windows - 1))
            samples.append(handle.read(width))
        return samples


def sniff_encoding(path: Path, nbytes: int = INSPECTION_BYTES) -> str:
    samples = sample_file(path, nbytes)
    for i, sample in enumerate(samples):
        # A window can start/end in the middle of a valid UTF-8 character.
        if i:
            sample = sample.lstrip(bytes(range(0x80, 0xc0)))
        try:
            codecs.getincrementaldecoder("utf-8")().decode(sample, final=i == len(samples) - 1)
        except UnicodeDecodeError:
            return "latin-1"
    return "utf-8"


def read_header(path: Path) -> tuple[list[str], str]:
    enc = sniff_encoding(path)
    with path.open("rb") as handle:
        first = handle.readline(INSPECTION_BYTES + 1)
    if len(first) > INSPECTION_BYTES:
        raise ValueError(f"{path}: header exceeds 64 KiB")
    cols = [c.strip().lstrip("\ufeff\x00") for c in first.decode(enc).split("\t")]
    while cols and cols[-1] == "":
        cols.pop()
    return cols, enc


def legacy_mapping_rows(path: Path, encoding: str):
    """Read BG/BP's numeric-code tables and join indented wrapped labels."""
    current = None
    with path.open(encoding=encoding) as handle:
        for line in handle:
            match = re.match(r"^\s*(\d+)\s+(\S.*)$", line.rstrip())
            if match:
                if current:
                    yield current
                current = [match[1], match[2].strip()]
            elif current and line.strip():
                current[1] += " " + line.strip()
    if current:
        yield current


def legacy_mapping_columns(path: Path, encoding: str, table: str) -> list[str]:
    with path.open(encoding=encoding) as handle:
        for _ in range(50):
            line = handle.readline(INSPECTION_BYTES + 1)
            if not line:
                break
            if len(line) > INSPECTION_BYTES:
                raise ValueError(f"{path}: mapping line exceeds 64 KiB")
            cols = line.split()
            if (len(cols) == 2 and cols[0].endswith("_code")
                    and cols[1].endswith(("_text", "_name", "_title"))):
                return cols
    return [table + "_code", table + "_text"]


def _suffix(name: str, program: str) -> str:
    prefix = program.lower() + "."
    return name[len(prefix):] if name.lower().startswith(prefix) else name


def _data_rank(name: str) -> int:
    n = name.lower()
    if any(tag in n for tag in ("allitems", "alldata", "allcesseries")):
        return 0
    if "current" in n:
        return 2
    return 1


def _looks_like_observations(cols: list[str]) -> bool:
    return {"series_id", "year", "period", "value"} <= {c.lower() for c in cols}


@dataclass
class MappingSpec:
    table: str
    path: str
    columns: list[str]
    code_col: str
    label_col: str | None
    links_to: list[str]
    context_column: str | None = None
    key_columns: list[str] = field(default_factory=list)
    catalog_keys: list[str] = field(default_factory=list)
    layout: str = "tab"
    encoding: str = "utf-8"


@dataclass
class ProgramSchema:
    program: str
    encoding: str
    catalog_path: str | None
    catalog_columns: list[str]
    title_column: str | None
    seasonal_column: str | None
    periodicity_column: str | None
    data_files: list[dict]
    data_columns: list[str]
    footnote_path: str | None
    mappings: list[MappingSpec] = field(default_factory=list)
    documentation: list[str] = field(default_factory=list)
    auxiliary: list[str] = field(default_factory=list)
    unclassified: list[str] = field(default_factory=list)


def _first(cols: list[str], names: tuple[str, ...]) -> str | None:
    lower = {c.lower(): c for c in cols}
    for n in names:
        if n in lower:
            return lower[n]
    return None


def _stem(name: str) -> str:
    return re.sub(r"(_code|_text|_name|_title)$", "", name.lower())


def _label_column(cols: list[str]) -> str | None:
    for name in cols[1:]:
        if name.lower().endswith(("_name", "_text", "_title")):
            return name
    return cols[1] if len(cols) > 1 else None


def _link_mapping(table: str, filename: str, cols: list[str],
                  catalog_cols: list[str]) -> MappingSpec:
    label_col = next((c for c in cols if c.lower() in
                      (table + "_text", table + "_name", table + "_title")), None)
    label_col = label_col or _label_column(cols)
    aliases = {"duration_code": "contract_duration", "selection_code": "contract_selection"}
    keys, catalog_keys = [], []
    for col in cols:
        if col == label_col or not col.endswith("_code"):
            continue
        link = next((c for c in catalog_cols
                     if c == col or c == _stem(col) or c == aliases.get(col)), None)
        if link:
            keys.append(col)
            catalog_keys.append(link)
    # Only attach labels when their own code participates in the full key.
    if not label_col or not any(_stem(k) == _stem(label_col) for k in keys):
        keys, catalog_keys = [], []
    primary = next((k for k in keys if _stem(k) == _stem(label_col or "")), cols[0])
    if primary != cols[0] and cols[0].endswith("_code") and cols[0] not in keys:
        # A contextual first key must never be dropped to make a join possible.
        keys, catalog_keys = [], []
    context = next((c for k, c in zip(keys, catalog_keys) if k != primary), None)
    links = [catalog_keys[keys.index(primary)]] if keys else []
    return MappingSpec(table=table, path=filename, columns=cols, code_col=primary,
                       label_col=label_col, links_to=links, context_column=context,
                       key_columns=keys, catalog_keys=catalog_keys)


def infer_schema(raw_dir: Path, program: str,
                 names: list[str] | None = None) -> ProgramSchema:
    raw_dir = Path(raw_dir)
    program = program.upper()
    files = sorted(names or [p.name for p in raw_dir.iterdir() if p.is_file()])
    encodings: list[str] = []
    catalog_path = None
    catalog_cols: list[str] = []
    data_files: list[dict] = []
    data_cols: list[str] = []
    footnote_path = None
    mapping_files: list[tuple[str, Path, list[str], str, str]] = []
    documentation: list[str] = []
    auxiliary: list[str] = []
    unclassified: list[str] = []
    for name in files:
        path = raw_dir / name
        if not path.is_file() or name.endswith(".part"):
            continue
        low = name.lower()
        if not low.startswith(program.lower() + ".") or low.endswith((".json", ".jsonl")):
            unclassified.append(name)
            continue
        if low.endswith(".txt") or low.endswith(".contacts"):
            documentation.append(name)
            continue
        cols, enc = read_header(path)
        encodings.append(enc)
        if low.endswith(".series"):
            catalog_path, catalog_cols = name, cols
            continue
        if ".data." in low:
            data_files.append({"path": name, "rank": _data_rank(name),
                               "columns": cols, "encoding": enc})
            if not data_cols:
                data_cols = cols
            continue
        if low.endswith(".footnote"):
            footnote_path = name
            continue
        if _looks_like_observations(cols):
            auxiliary.append(name)
            continue
        table = _suffix(name, program)
        layout = "tab"
        if len(cols) < 2 and program in {"BG", "BP"}:
            if next(legacy_mapping_rows(path, enc), None):
                cols = legacy_mapping_columns(path, enc, table)
                layout = "legacy"
        if len(cols) >= 2:
            mapping_files.append((table, path, cols, layout, enc))
            continue
        unclassified.append(name)
    mappings = []
    for table, path, cols, layout, enc in mapping_files:
        m = _link_mapping(table, path.name, cols, catalog_cols)
        m.layout, m.encoding = layout, enc
        mappings.append(m)
    data_files.sort(key=lambda d: (d["rank"], d["path"]))
    return ProgramSchema(
        program=program,
        encoding="latin-1" if "latin-1" in encodings else "utf-8",
        catalog_path=catalog_path, catalog_columns=catalog_cols,
        title_column=_first(catalog_cols, ("series_title", "series_name")),
        seasonal_column=_first(catalog_cols, ("seasonal", "seasonal_code")),
        periodicity_column=_first(catalog_cols, ("periodicity_code", "periodicity")),
        data_files=data_files, data_columns=data_cols,
        footnote_path=footnote_path, mappings=mappings,
        documentation=documentation, auxiliary=auxiliary,
        unclassified=unclassified,
    )


def required_missing(schema: ProgramSchema) -> list[str]:
    missing = []
    if not schema.catalog_path:
        missing.append("series catalog (*.series)")
    if not schema.data_files:
        missing.append("observation file (*.data.*)")
    else:
        for item in schema.data_files:
            have = {c.lower() for c in item["columns"]}
            if len(have) != len(item["columns"]):
                missing.append(f"duplicate header fields in {item['path']}")
            for col in ("series_id", "year", "period", "value"):
                if col not in have:
                    missing.append(f"{item['path']}: observation column '{col}'")
    if "series_id" not in schema.catalog_columns:
        missing.append("catalog column 'series_id'")
    return missing


def _ident(name: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)
    if not cleaned or cleaned[0].isdigit():
        cleaned = "c_" + cleaned
    return cleaned


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


def _lines_sql(path: Path, encoding: str) -> str:
    return (
        f"read_csv({_sql_str(str(path))}, columns={{'line':'VARCHAR'}}, "
        f"delim='\\x1f', quote='', escape='', header=false, auto_detect=false, "
        f"encoding={_sql_str(encoding)}, strict_mode=true, ignore_errors=false, max_line_size=16777216) "
        f"WITH ORDINALITY AS t(line, rec)"
    )


def _split_sql(path: Path, encoding: str, skip_header: bool = True) -> str:
    where = "rec > 1 AND " if skip_header else ""
    return (
        f"SELECT rec, string_split(line, chr(9)) AS f FROM {_lines_sql(path, encoding)} "
        f"WHERE {where}line IS NOT NULL AND trim(line) <> ''"
    )


@dataclass
class ProcessResult:
    program: str
    series: int = 0
    observations: int = 0
    rows_in: int = 0
    duplicate_identical: int = 0
    duplicate_conflicts: int = 0
    malformed_rows: int = 0
    missing_values: int = 0
    non_numeric_values: int = 0
    unknown_periods: int = 0
    duplicate_rows_removed: int = 0
    orphan_observations: int = 0
    identical_data_files: list[str] = field(default_factory=list)
    exported_series: int = 0
    exported_observations: int = 0
    elapsed_s: float = 0.0

    def parse_stats(self) -> dict:
        return {
            "rows_in": self.rows_in, "rows_written": self.observations,
            "malformed_rows": self.malformed_rows,
            "non_numeric_values": self.non_numeric_values,
            "missing_values": self.missing_values,
            "unknown_periods": self.unknown_periods,
            "duplicate_identical": self.duplicate_identical,
            "duplicate_conflicts": self.duplicate_conflicts,
            "duplicate_rows_removed": self.duplicate_rows_removed,
            "orphan_observations": self.orphan_observations,
            "identical_data_files": self.identical_data_files,
            "exported_series": self.exported_series,
            "exported_observations": self.exported_observations,
        }


class _ShardWriter:
    """Append JSON lines into size-bounded shards; never split one series_id."""

    def __init__(self, root: Path, kind: str, max_bytes: int | None = None,
                 max_rows: int | None = None):
        self.root = root
        self.kind = kind
        self.max_bytes = SHARD_MAX_BYTES if max_bytes is None else max_bytes
        self.max_rows = SHARD_MAX_ROWS if max_rows is None else max_rows
        self.root.mkdir(parents=True, exist_ok=True)
        self.shards: list[dict] = []
        self._index = -1
        self._handle = None
        self._hasher = None
        self._bytes = 0
        self._rows = 0
        self._first = None
        self._last = None
        self._current_series = None
        self._path = None

    def write(self, series_id: str, line: str) -> None:
        payload = line if line.endswith("\n") else line + "\n"
        encoded = payload.encode("utf-8")
        rollover = (self._handle is None
                    or ((self._bytes + len(encoded) > self.max_bytes
                         or self._rows >= self.max_rows)
                        and series_id != self._current_series))
        if rollover:
            self._rotate()
        self._handle.write(encoded)
        self._hasher.update(encoded)
        self._bytes += len(encoded)
        self._rows += 1
        self._current_series = series_id
        if self._first is None:
            self._first = series_id
        self._last = series_id

    def close(self) -> list[dict]:
        self._rotate(final=True)
        return self.shards

    def _rotate(self, final: bool = False) -> None:
        if self._handle is not None:
            self._handle.close()
            self.shards.append({
                "path": f"{self.kind}/{self._path.name}",
                "first_series_id": self._first,
                "last_series_id": self._last,
                "rows": self._rows,
                "sha256": self._hasher.hexdigest(),
                "size": self._bytes,
            })
            self._handle = None
        if final:
            return
        self._index += 1
        self._path = self.root / f"part-{self._index:05d}.jsonl"
        self._handle = self._path.open("wb")
        self._hasher = hashlib.sha256()
        self._bytes = self._rows = 0
        self._first = self._last = self._current_series = None


class Engine:
    def __init__(self, raw_dir: Path, out_dir: Path, program: str,
                 memory_limit: str = "auto", threads: int = 2):
        self.raw_dir = Path(raw_dir)
        self.out_dir = Path(out_dir)
        self.program = program.upper()
        self.memory_limit = resolve_memory_limit(memory_limit)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.out_dir / "work.duckdb"
        if self.db_path.exists():
            self.db_path.unlink()
        self.con = duckdb.connect(str(self.db_path))
        self.con.execute(f"SET memory_limit={_sql_str(self.memory_limit)}")
        self.con.execute(f"SET threads={int(threads)}")
        self.con.execute("SET preserve_insertion_order=false")
        spill = self.out_dir / "spill"
        spill.mkdir(exist_ok=True)
        self.con.execute(f"SET temp_directory={_sql_str(str(spill))}")
        self.con.execute("""
            CREATE MACRO period_kind(p) AS (
              CASE
                WHEN p IS NULL THEN 'unknown_period'
                WHEN regexp_matches(p, '^M(0[1-9]|1[0-2])$') THEN 'monthly'
                WHEN p IN ('M13', 'Q05', 'S03') THEN 'annual_average'
                WHEN regexp_matches(p, '^Q0[1-4]$') THEN 'quarterly'
                WHEN p IN ('S01', 'S02') THEN 'semiannual'
                WHEN p IN ('A01', 'ANN', 'A') THEN 'annual'
                ELSE 'unknown_period'
              END
            );
        """)
        self.map_tables: dict[str, MappingSpec] = {}
        self.observation_fields: list[str] = []
        self.identical_data_files: list[str] = []
        self.skipped_identical_rows = 0

    def close(self) -> None:
        self.con.close()

    def process(self, schema: ProgramSchema | None = None,
                file_receipts: list[dict] | None = None) -> ProcessResult:
        started = time.perf_counter()
        schema = schema or infer_schema(self.raw_dir, self.program)
        missing = required_missing(schema)
        if missing:
            raise RuntimeError(f"{self.program}: missing required pieces: {', '.join(missing)}")
        self.schema = schema
        self._load_catalog(schema)
        self._load_mappings(schema)
        self._load_footnotes(schema)
        self._load_observations(schema, file_receipts or [])
        stats = self._stats()
        self.stats = stats
        if stats.orphan_observations or stats.malformed_rows or stats.duplicate_conflicts:
            _write_json(self.out_dir / "validation.json", stats.parse_stats())
            raise RuntimeError(
                f"{self.program}: validation failed: {stats.malformed_rows} malformed rows, "
                f"{stats.orphan_observations} orphan observations, "
                f"{stats.duplicate_conflicts} conflicting observation keys")
        stats.elapsed_s = time.perf_counter() - started
        return stats

    def _load_catalog(self, schema: ProgramSchema) -> None:
        path = self.raw_dir / schema.catalog_path
        n = len(schema.catalog_columns)
        selects = ", ".join(
            f"trim(f[{i}]) AS {_ident(col)}"
            for i, col in enumerate(schema.catalog_columns, start=1)
        )
        self.con.execute(f"""
            CREATE TABLE catalog AS
            SELECT {selects},
                   (series_id IS NOT NULL AND series_id <> ''
                    AND len(f) >= {n}
                    AND len(list_filter(f[{n}+1:], lambda x: trim(x) <> '')) = 0
                    AND regexp_full_match(series_id, '[A-Za-z0-9_-][A-Za-z0-9_.-]*')
                    AND series_id NOT IN ('.', '..')) AS ok
            FROM ({_split_sql(path, read_header(path)[1])})
        """)
        bad = self.con.execute("SELECT count(*) FROM catalog WHERE NOT ok").fetchone()[0]
        duplicates = self.con.execute(
            "SELECT count(*) FROM (SELECT series_id FROM catalog WHERE ok "
            "GROUP BY 1 HAVING count(*) > 1)").fetchone()[0]
        if bad or duplicates:
            raise RuntimeError(f"invalid catalog: {bad} malformed rows, "
                               f"{duplicates} duplicate IDs")
        self.con.execute("DELETE FROM catalog WHERE NOT ok")
        self.con.execute("ALTER TABLE catalog DROP COLUMN ok")

    def _map_table_name(self, mapping: MappingSpec) -> str:
        return f"map_{_ident(mapping.table)}"

    def _load_mappings(self, schema: ProgramSchema) -> None:
        self.map_tables = {}
        for m in schema.mappings:
            if not m.key_columns or not m.label_col:
                continue
            table = self._map_table_name(m)
            key_defs = ", ".join(f"{_ident(c)} VARCHAR" for c in m.key_columns)
            self.con.execute(
                f"CREATE TABLE {table} ({key_defs}, {_ident(m.label_col)} VARCHAR)")
            path = self.raw_dir / m.path
            if m.layout == "legacy":
                batch = []
                for row in legacy_mapping_rows(path, m.encoding):
                    keys = [row[m.columns.index(k)] for k in m.key_columns]
                    label = row[m.columns.index(m.label_col)] or None
                    batch.append(tuple(keys + [label]))
                    if len(batch) == 1000:
                        self.con.executemany(
                            f"INSERT INTO {table} VALUES ({', '.join('?' for _ in batch[0])})",
                            batch)
                        batch.clear()
                if batch:
                    self.con.executemany(
                        f"INSERT INTO {table} VALUES ({', '.join('?' for _ in batch[0])})",
                        batch)
            else:
                key_expr = ", ".join(f"trim(f[{m.columns.index(k)+1}])" for k in m.key_columns)
                label_i = m.columns.index(m.label_col) + 1
                self.con.execute(f"""
                    INSERT INTO {table}
                    SELECT {key_expr}, nullif(trim(f[{label_i}]), '')
                    FROM ({_split_sql(path, m.encoding)})
                """)
            key_cols = ", ".join(_ident(c) for c in m.key_columns)
            ambiguous = self.con.execute(
                f"SELECT count(*) FROM (SELECT {key_cols} FROM {table} "
                f"GROUP BY {key_cols} HAVING count(*) > 1)").fetchone()[0]
            if ambiguous:
                raise RuntimeError(f"{m.path}: {ambiguous} duplicate lookup keys "
                                   f"for {m.key_columns}")
            self.map_tables[m.table] = m

    def _load_footnotes(self, schema: ProgramSchema) -> None:
        self.con.execute(
            "CREATE TABLE footnotes (footnote_code VARCHAR, footnote_text VARCHAR, "
            "source_file VARCHAR)")
        if not schema.footnote_path:
            return
        path = self.raw_dir / schema.footnote_path
        header, encoding = read_header(path)
        text_i = 2 if len(header) > 1 else 1
        self.con.execute(f"""
            INSERT INTO footnotes
            SELECT trim(f[1]), trim(f[{text_i}]), {_sql_str(schema.footnote_path)}
            FROM ({_split_sql(path, encoding)})
        """)

    def _digest_for(self, name: str, receipts: list[dict]) -> str | None:
        for receipt in receipts:
            if receipt.get("name") == name and receipt.get("sha256"):
                return receipt["sha256"]
        path = self.raw_dir / name
        return sha256_file(path) if path.exists() else None

    def _load_observations(self, schema: ProgramSchema, receipts: list[dict]) -> None:
        self.observation_fields = sorted({
            c for item in schema.data_files for c in item["columns"]
            if c.lower() not in {"series_id", "year", "period", "value", "footnote_codes"}
        })
        seen: dict[str, str] = {}
        active: list[dict] = []
        self.identical_data_files = []
        self.skipped_identical_rows = 0
        for item in schema.data_files:
            digest = self._digest_for(item["path"], receipts)
            if digest and digest in seen:
                self.identical_data_files.append(item["path"])
                # Count rows without loading values so stats stay honest.
                path = self.raw_dir / item["path"]
                n = self.con.execute(
                    f"SELECT count(*) FROM ({_split_sql(path, item['encoding'])})"
                ).fetchone()[0]
                self.skipped_identical_rows += int(n)
                continue
            if digest:
                seen[digest] = item["path"]
            active.append(item)
        if not active:
            raise RuntimeError(f"{self.program}: no observation files left after "
                               f"skipping identical copies")

        parts = []
        for order, item in enumerate(active):
            cols = {c.lower(): i + 1 for i, c in enumerate(item["columns"])}
            get = lambda name, cols=cols: f"trim(f[{cols[name]}])"
            fn_expr = get("footnote_codes") if "footnote_codes" in cols else "''"
            extra_cols = [(c, i + 1) for i, c in enumerate(item["columns"])
                          if c.lower() not in {"series_id", "year", "period", "value",
                                               "footnote_codes"}]
            if extra_cols:
                extras = ", ".join(f"{_sql_str(c)}, trim(f[{i}])" for c, i in extra_cols)
                fields_expr = f"CAST(json_object({extras}) AS VARCHAR)"
            else:
                fields_expr = "CAST(NULL AS VARCHAR)"
            n = len(item["columns"])
            parts.append(f"""
                SELECT {get('series_id')} AS series_id,
                       TRY_CAST({get('year')} AS INTEGER) AS year,
                       {get('period')} AS period,
                       {get('value')} AS value_raw,
                       {fn_expr} AS footnote_codes,
                       {fields_expr} AS source_fields,
                       ({order}::BIGINT * 1099511627776 + rec) AS ord,
                       len(f) AS n_fields,
                       len(list_filter(f[{n}+1:], lambda x: trim(x) <> '')) AS extra,
                       {n} AS expected_fields
                FROM ({_split_sql(self.raw_dir / item['path'], item['encoding'])})
            """)
        union_sql = " UNION ALL ".join(parts)
        active_rows = self.con.execute(f"SELECT count(*) FROM ({union_sql})").fetchone()[0]
        self.con.execute(f"""
            CREATE TABLE observations AS
            SELECT series_id, year, period,
                   arg_min(value_raw, ord) AS value_raw,
                   arg_min(footnote_codes, ord) AS footnote_codes,
                   arg_min(source_fields, ord) AS source_fields,
                   count(*)::BIGINT AS n,
                   (count(DISTINCT struct_pack(
                        v := coalesce(value_raw, chr(0)),
                        f := coalesce(footnote_codes, chr(0)))) = 1) AS same
            FROM ({union_sql})
            WHERE year BETWEEN 1 AND 9999 AND series_id <> '' AND period <> ''
              AND n_fields >= expected_fields AND extra = 0
            GROUP BY series_id, year, period
        """)
        self._obs_rows_in = int(active_rows) + self.skipped_identical_rows
        self._obs_valid = int(self.con.execute(
            "SELECT coalesce(sum(n), 0) FROM observations").fetchone()[0])
        self._obs_active_rows = int(active_rows)

    def _stats(self) -> ProcessResult:
        kept = self.con.execute("SELECT count(*) FROM observations").fetchone()[0]
        identical, conflicts, removed = self.con.execute("""
            SELECT coalesce(sum(n - 1) FILTER (WHERE same), 0),
                   count(*) FILTER (WHERE NOT same),
                   coalesce(sum(n - 1), 0)
            FROM observations
        """).fetchone()
        identical = int(identical) + self.skipped_identical_rows
        removed = int(removed) + self.skipped_identical_rows
        missing, nonnum, unknown = self.con.execute("""
            SELECT count(*) FILTER (
                     WHERE value_raw IS NULL OR trim(value_raw) IN
                       ('', '-', '--', '.', 'NA', 'N/A', '(NA)', 'null')),
                   count(*) FILTER (
                     WHERE value_raw IS NOT NULL
                       AND trim(value_raw) NOT IN
                         ('', '-', '--', '.', 'NA', 'N/A', '(NA)', 'null')
                       AND (TRY_CAST(replace(value_raw, ',', '') AS DOUBLE) IS NULL
                            OR NOT isfinite(TRY_CAST(replace(value_raw, ',', '') AS DOUBLE)))),
                   count(*) FILTER (WHERE period_kind(period) = 'unknown_period')
            FROM observations
        """).fetchone()
        series = self.con.execute("SELECT count(*) FROM catalog").fetchone()[0]
        orphans = self.con.execute(
            "SELECT count(*) FROM observations o ANTI JOIN catalog c "
            "ON c.series_id = o.series_id").fetchone()[0]
        malformed = self._obs_active_rows - self._obs_valid
        return ProcessResult(
            program=self.program, series=series, observations=kept,
            rows_in=int(self._obs_rows_in),
            duplicate_identical=identical, duplicate_conflicts=int(conflicts),
            duplicate_rows_removed=removed, orphan_observations=orphans,
            malformed_rows=max(0, malformed), missing_values=missing,
            non_numeric_values=nonnum, unknown_periods=unknown,
            identical_data_files=list(self.identical_data_files),
        )

    def _join_stats(self, schema: ProgramSchema) -> list[dict]:
        joins = []
        used = {_ident(c) for c in schema.catalog_columns}
        for m in sorted(self.map_tables.values(), key=lambda m: -len(m.key_columns)):
            table = self._map_table_name(m)
            blank = " OR ".join(
                f"c.{_ident(c)} IS NULL OR c.{_ident(c)} = ''" for c in m.catalog_keys)
            on = " AND ".join(
                f"c.{_ident(a)} = mp.{_ident(b)}"
                for a, b in zip(m.catalog_keys, m.key_columns))
            matched, blanks, unmatched = self.con.execute(f"""
                SELECT count(*) FILTER (WHERE NOT ({blank}) AND mp.{_ident(m.label_col)} IS NOT NULL),
                       count(*) FILTER (WHERE {blank}),
                       count(*) FILTER (WHERE NOT ({blank}) AND mp.{_ident(m.label_col)} IS NULL)
                FROM catalog c LEFT JOIN {table} mp ON {on}
            """).fetchone()
            alias = _ident(m.label_col)
            joins.append({
                "file": m.path, "catalog_column": m.links_to[0],
                "catalog_columns": m.catalog_keys, "lookup_columns": m.key_columns,
                **({"context_column": m.context_column} if m.context_column else {}),
                "label_column": m.label_col, "exported": alias not in used,
                "matched": matched, "blank": blanks, "unmatched": unmatched,
                "label_path": f"labels/{m.table}.json",
            })
            if alias not in used:
                used.add(alias)
        return joins

    def _write_labels(self, schema: ProgramSchema) -> list[dict]:
        labels_dir = self.out_dir / "labels"
        labels_dir.mkdir(parents=True, exist_ok=True)
        receipts = []
        for m in self.map_tables.values():
            table = self._map_table_name(m)
            cols = list(m.key_columns) + [m.label_col]
            rows = self.con.execute(
                f"SELECT {', '.join(_ident(c) for c in cols)} FROM {table}").fetchall()
            payload = {
                "file": m.path,
                "catalog_columns": m.catalog_keys,
                "lookup_columns": m.key_columns,
                "label_column": m.label_col,
                "rows": [dict(zip(cols, row)) for row in rows],
            }
            path = labels_dir / f"{m.table}.json"
            _write_json(path, payload)
            receipts.append({
                "path": f"labels/{m.table}.json",
                "table": m.table,
                "sha256": sha256_file(path),
                "size": path.stat().st_size,
                "rows": len(rows),
            })
        return receipts

    def _stream_shards(self, sql: str, kind: str) -> list[dict]:
        writer = _ShardWriter(self.out_dir / kind, kind)
        result = self.con.execute(sql)
        while True:
            batch = result.fetchmany(50_000)
            if not batch:
                break
            for series_id, line in batch:
                writer.write(series_id, line)
        return writer.close()

    def export_series(self, schema: ProgramSchema,
                      extra: dict | None = None) -> Path:
        extra = extra or {}
        joins = self._join_stats(schema)
        label_receipts = self._write_labels(schema)

        catalog_fields = ", ".join(
            f"{_ident(c)} := c.{_ident(c)}" for c in schema.catalog_columns)
        series_sql = f"""
            SELECT c.series_id,
                   CAST(to_json(struct_pack(
                       {catalog_fields},
                       observation_count := coalesce(o.n, 0)
                   )) AS VARCHAR) AS line
            FROM catalog c
            LEFT JOIN (SELECT series_id, count(*)::BIGINT AS n
                       FROM observations GROUP BY 1) o USING (series_id)
            ORDER BY c.series_id
        """
        base_obs = ("struct_pack(series_id := series_id, year := CAST(year AS VARCHAR), "
                    "period := period, value := value_raw, "
                    "footnote_codes := coalesce(footnote_codes, ''))")
        obs_sql = f"""
            SELECT series_id,
                   CASE WHEN source_fields IS NULL
                        THEN CAST(to_json({base_obs}) AS VARCHAR)
                        ELSE CAST(json_merge_patch(to_json({base_obs}),
                                                   CAST(source_fields AS JSON)) AS VARCHAR)
                   END AS line
            FROM observations
            ORDER BY series_id, year, period
        """
        series_shards = self._stream_shards(series_sql, "series")
        obs_shards = self._stream_shards(obs_sql, "observations")

        exported_series = sum(s["rows"] for s in series_shards)
        exported_observations = sum(s["rows"] for s in obs_shards)
        self.stats.exported_series = exported_series
        self.stats.exported_observations = exported_observations
        if (exported_series, exported_observations) != (self.stats.series, self.stats.observations):
            raise RuntimeError("export counts do not reconcile with processed series/observations")
        empty, year_min, year_max = self.con.execute("""
            SELECT (SELECT count(*) FROM catalog c
                    ANTI JOIN (SELECT DISTINCT series_id FROM observations) o
                      ON o.series_id = c.series_id),
                   (SELECT min(year) FROM observations),
                   (SELECT max(year) FROM observations)
        """).fetchone()
        card = {
            "program": self.program, "pipeline_version": PIPELINE_VERSION,
            "listing_sha256": extra.get("listing_sha256"), "files": extra.get("files", []),
            "catalog_fields": schema.catalog_columns, "joins": joins,
            "observation_fields": self.observation_fields,
            "unlinked_mappings": [m.path for m in schema.mappings if not m.key_columns],
            "auxiliary_files": schema.auxiliary, "unclassified_files": schema.unclassified,
            "memory_limit": self.memory_limit,
            "series_shards": series_shards,
            "observation_shards": obs_shards,
            "labels": label_receipts,
            "coverage": {"series": exported_series, "observations": exported_observations,
                         "catalog_without_observations": empty,
                         "year_min": year_min, "year_max": year_max},
            "validation": self.stats.parse_stats(),
        }
        card_path = self.out_dir / "catalog.json"
        _write_json(card_path, card)
        _write_json(self.out_dir / "complete.json", {
            "pipeline_version": PIPELINE_VERSION, "listing_sha256": extra.get("listing_sha256"),
            "series": exported_series, "observations": exported_observations,
            "files": {
                "catalog.json": {"sha256": sha256_file(card_path),
                                 "size": card_path.stat().st_size},
                **{s["path"]: {"sha256": s["sha256"], "size": s["size"]}
                   for s in series_shards + obs_shards},
                **{r["path"]: {"sha256": r["sha256"], "size": r["size"]}
                   for r in label_receipts},
            },
        })
        return card_path


def _shard_for(shards: list[dict], series_id: str) -> dict | None:
    for shard in shards:
        if shard["first_series_id"] <= series_id <= shard["last_series_id"]:
            return shard
    return None


def read_series(catalog_dir: Path, series_id: str) -> dict | None:
    """Return the series metadata row (codes only) for series_id."""
    card = json.loads((Path(catalog_dir) / "catalog.json").read_text())
    shard = _shard_for(card.get("series_shards", []), series_id)
    if shard is None:
        return None
    path = Path(catalog_dir) / shard["path"]
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("series_id") == series_id:
                return row
    return None


def read_observations(catalog_dir: Path, series_id: str) -> list[dict]:
    """Return the observation rows for series_id."""
    card = json.loads((Path(catalog_dir) / "catalog.json").read_text())
    shard = _shard_for(card.get("observation_shards", []), series_id)
    if shard is None:
        return []
    path = Path(catalog_dir) / shard["path"]
    out = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("series_id") == series_id:
                out.append({k: v for k, v in row.items() if k != "series_id"})
            elif out:
                break
    return out


def load_series(catalog_dir: Path, series_id: str) -> dict | None:
    """Rebuild the old one-record view: codes + names + observations."""
    catalog_dir = Path(catalog_dir)
    row = read_series(catalog_dir, series_id)
    if row is None:
        return None
    card = json.loads((catalog_dir / "catalog.json").read_text())
    for label in card.get("labels", []):
        payload = json.loads((catalog_dir / label["path"]).read_text())
        keys = payload["lookup_columns"]
        catalog_keys = payload["catalog_columns"]
        label_col = payload["label_column"]
        want = tuple(row.get(c) for c in catalog_keys)
        if any(v is None or v == "" for v in want):
            row[label_col] = None
            continue
        match = None
        for entry in payload["rows"]:
            if tuple(entry.get(k) for k in keys) == want:
                match = entry.get(label_col)
                break
        row[label_col] = match
    row["observations"] = read_observations(catalog_dir, series_id)
    return row


def process_catalog(raw_dir: Path, out_dir: Path, program: str,
                    extra: dict | None = None, memory_limit: str = "auto", threads: int = 2,
                    names: list[str] | None = None) -> tuple[ProcessResult, ProgramSchema]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    # Any interrupted rebuild must invalidate the old completion marker.
    (out_dir / "complete.json").unlink(missing_ok=True)
    schema = infer_schema(raw_dir, program, names=names)
    limit = resolve_memory_limit(memory_limit)
    with tempfile.TemporaryDirectory(prefix=".export-", dir=out_dir) as temporary:
        stage = Path(temporary)
        engine = Engine(raw_dir, stage, program, memory_limit=limit, threads=threads)
        try:
            stats = engine.process(schema, file_receipts=(extra or {}).get("files") or [])
            engine.export_series(schema, extra=extra)
        except Exception:
            if (stage / "validation.json").exists():
                shutil.copyfile(stage / "validation.json", out_dir / "validation.json")
            raise
        finally:
            engine.close()
        for name in ("series", "observations", "labels"):
            previous = out_dir / f".{name}.previous"
            if previous.exists():
                shutil.rmtree(previous)
            current = out_dir / name
            if current.exists():
                current.rename(previous)
            staged = stage / name
            if staged.exists():
                staged.rename(current)
            if previous.exists():
                shutil.rmtree(previous)
        for name in ("catalog.json", "complete.json"):
            os.replace(stage / name, out_dir / name)
        for stale in ("index.json", "outputs.jsonl"):
            (out_dir / stale).unlink(missing_ok=True)
        (out_dir / "validation.json").unlink(missing_ok=True)
    return stats, schema

# -- one run ---------------------------------------------------------------

def _file_receipt(raw_dir: Path, manifest: Manifest) -> list[dict]:
    rows = []
    for rec in manifest.files:
        path = raw_dir / rec.relative_path
        sha = rec.sha256
        if path.exists() and not sha:
            sha = sha256_file(path)
        rows.append({
            "name": rec.relative_path,
            "sha256": sha,
            "size": rec.local_size or (path.stat().st_size if path.exists() else None),
            "role": rec.file_role,
            "download_status": rec.download_status,
            "required": rec.required,
            "http_status": rec.http_status,
            "attempts": rec.attempts,
            "error": rec.error_message,
        })
    return rows


def _unchanged(catalog_json: Path, listing_sha256: str) -> bool:
    """Skip only a completed, version-compatible export with verified shards."""
    root = catalog_json.parent
    try:
        complete = json.loads((root / "complete.json").read_text())
        if (complete.get("pipeline_version") != PIPELINE_VERSION
                or not listing_sha256 or complete.get("listing_sha256") != listing_sha256
                or (root / "state.json").exists() and
                json.loads((root / "state.json").read_text()).get("status") not in {"ok", "skipped"}):
            return False
        card = json.loads(catalog_json.read_text())
        if card.get("pipeline_version") != PIPELINE_VERSION:
            return False
        files = complete.get("files") or {}
        for name, receipt in files.items():
            path = root / name
            if (not path.exists() or path.stat().st_size != receipt["size"]
                    or sha256_file(path) != receipt["sha256"]):
                return False
        series_rows = sum(s["rows"] for s in card.get("series_shards", []))
        obs_rows = sum(s["rows"] for s in card.get("observation_shards", []))
        if (series_rows, obs_rows) != (complete["series"], complete["observations"]):
            return False
        for receipt in card.get("files", []):
            if receipt["download_status"] == "failed":
                if receipt.get("required"):
                    return False
                continue
            path = root / "raw" / receipt["name"]
            if not receipt["sha256"] or path.stat().st_size != receipt["size"]:
                return False
            if sha256_file(path) != receipt["sha256"]:
                return False
        return True
    except (OSError, ValueError, KeyError, TypeError):
        return False


def process_program(cfg: AppConfig, force: bool = False,
                    offline: bool = False) -> dict:
    program = cfg.program.upper()
    raw_dir, out_dir = cfg.raw_root, cfg.catalog_dir
    started = time.perf_counter()
    result = {"program": program, "status": "running", "series": 0,
              "observations": 0, "error": None, "elapsed_s": 0.0,
              "started_utc": datetime.now(timezone.utc).isoformat()}
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = None
    def save():
        result["elapsed_s"] = round(time.perf_counter() - started, 2)
        _write_json(out_dir / "state.json", result)
    try:
        # Check the prior state before replacing it with this run's started state.
        if offline:
            save()
            if not raw_dir.exists():
                raise FileNotFoundError(f"no local raw files at {raw_dir}")
            extra, names = {"listing_sha256": None, "files": []}, None
            manifest_path = out_dir / "download.json"
            if manifest_path.exists():
                manifest = Manifest.load(manifest_path)
                names = []
                for rec in manifest.files:
                    path = raw_dir / rec.relative_path
                    if not path.exists():
                        if rec.required:
                            raise DownloadError(f"offline required file missing: {rec.relative_path}")
                        continue
                    if rec.remote_size is not None and path.stat().st_size != rec.remote_size:
                        raise DownloadError(f"offline size mismatch: {rec.relative_path}")
                    digest = sha256_file(path)
                    if rec.sha256 and rec.sha256 != digest:
                        raise DownloadError(f"offline checksum mismatch: {rec.relative_path}")
                    rec.download_status = "offline_verified" if rec.sha256 else "offline_unverified"
                    rec.sha256, rec.local_size = digest, path.stat().st_size
                    names.append(rec.relative_path)
                extra["files"] = _file_receipt(raw_dir, manifest)
        else:
            disc = discover_remote(cfg.program, cfg.program_url, cfg.user_agent,
                                   timeout_s=cfg.read_timeout_s, **_listing_retry(cfg))
            if not force and _unchanged(out_dir / "catalog.json", disc.listing_sha256):
                card = json.loads((out_dir / "catalog.json").read_text())
                result.update(status="skipped", series=card["coverage"]["series"],
                              observations=card["coverage"]["observations"])
                return result
            save()
            (out_dir / "complete.json").unlink(missing_ok=True)
            manifest_path = out_dir / "download.json"
            try:
                prior = Manifest.load(manifest_path) if manifest_path.exists() else None
            except (ValueError, TypeError, KeyError):
                prior = None
            manifest = new_manifest(disc)
            Downloader(cfg).download_all(manifest, raw_dir, prior=prior, manifest_path=manifest_path)
            extra = {"listing_sha256": disc.listing_sha256,
                     "files": _file_receipt(raw_dir, manifest)}
            names = [r.relative_path for r in manifest.files if r.download_status != "failed"]
        stats, _schema = process_catalog(raw_dir, out_dir, program, extra=extra,
                                         memory_limit=cfg.memory_limit, threads=cfg.threads, names=names)
        result.update(status="ok", series=stats.exported_series,
                      observations=stats.exported_observations,
                      duplicate_identical=stats.duplicate_identical,
                      duplicate_conflicts=stats.duplicate_conflicts)
    except Exception as exc:
        # A parsing/database/HTTP error in one catalog must not suppress the remaining catalogs.
        result.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        # Tell run_all when the server, not the data, is the reason for the failure.
        if isinstance(exc, ThrottledError) or (
                isinstance(exc, DownloadError) and manifest is not None
                and any(r.download_status == "failed" and r.http_status == 429
                        for r in manifest.files)):
            result["throttled"] = True
    except BaseException:
        result.update(status="interrupted", error="catalog interrupted before completion")
        raise
    finally:
        result["finished_utc"] = datetime.now(timezone.utc).isoformat()
        save()
    return result


def run_all(cfg: AppConfig, only: list[str] | None = None, force: bool = False,
            offline: bool = False, prefixes: list[str] | None = None,
            sleep=time.sleep) -> dict:
    if cfg.pacer is None:
        cfg = replace(cfg, pacer=RequestPacer(cfg.min_interval_s, cfg.max_interval_s))
    log = {"started_utc": datetime.now(timezone.utc).isoformat(), "prefixes": [], "catalogs": []}
    log_path = Path(cfg.data_dir) / "run.json"
    _write_json(log_path, log)
    if prefixes is None:
        if only:
            prefixes = [p.lower() for p in only]
        else:
            try:
                _html, prefixes = fetch_root_listing(cfg.source_root, cfg.user_agent,
                                                      timeout_s=cfg.read_timeout_s,
                                                      **_listing_retry(cfg))
                if not prefixes:
                    raise DiscoveryError("no catalog directories discovered")
            except Exception as exc:
                log.update(error=f"{type(exc).__name__}: {exc}", failed=1,
                           finished_utc=datetime.now(timezone.utc).isoformat())
                _write_json(log_path, log)
                return log
    prefixes = list(dict.fromkeys(p.lower() for p in prefixes))
    if any(not re.fullmatch(r"[a-z]{2}", p) for p in prefixes):
        raise ValueError("catalog prefixes must contain exactly two letters")
    log["prefixes"] = prefixes
    log["catalogs"] = [{"program": p.upper(), "status": "not_reached"} for p in prefixes]
    _write_json(log_path, log)
    streak = 0  # throttled attempts in a row, across catalogs and retries

    def ready(prefix: str) -> bool:
        """Cool down before an attempt that follows a throttled one. False means stop."""
        if not streak:
            return True
        if cfg.max_consecutive_throttled and streak >= cfg.max_consecutive_throttled:
            return False
        cooldown = min(cfg.throttle_cooldown_s * 2 ** (streak - 1), cfg.throttle_cooldown_max_s)
        print(f"[{prefix}] waiting {cooldown:.0f}s after {streak} throttled attempt(s) in a row",
              flush=True)
        sleep(cooldown)
        return True

    def attempt(i: int, attempts: int) -> bool:
        """Run one catalog once and record it. True when the server throttled it."""
        nonlocal streak
        prefix = prefixes[i]
        print(f"[{prefix}] start" if attempts == 1 else
              f"[{prefix}] retry after throttle (attempt {attempts})", flush=True)
        log["catalogs"][i] = {"program": prefix.upper(), "status": "running"}
        _write_json(log_path, log)
        try:
            one = process_program(replace(cfg, program=prefix.upper(), data_dir=Path(cfg.data_dir)),
                                  force=force, offline=offline)
        except BaseException:
            log["catalogs"][i].update(status="interrupted")
            _write_json(log_path, log)
            raise
        one["attempts"] = attempts
        log["catalogs"][i] = one
        print(f"[{prefix}] {one['status']} series={one['series']} "
              f"obs={one['observations']} {one.get('error') or ''}", flush=True)
        _write_json(log_path, log)
        streak = streak + 1 if one.get("throttled") else 0
        return bool(one.get("throttled"))

    retries = max(0, cfg.throttled_catalog_retries)
    stopped = False
    for i, prefix in enumerate(prefixes):
        for tries in range(retries + 1):
            if not ready(prefix):
                stopped = True
                break
            # A retry resumes the catalog: verified files are not downloaded again.
            if not attempt(i, tries + 1):
                break
        if stopped:
            break
    if retries and not stopped:
        # Last chance for catalogs the server was still refusing when we moved on.
        for i in [i for i, c in enumerate(log["catalogs"]) if c.get("throttled")]:
            if not ready(prefixes[i]):
                stopped = True
                break
            attempt(i, log["catalogs"][i]["attempts"] + 1)
    if stopped:
        # Unfinished catalogs keep their status; a rerun picks them up.
        unreached = sum(1 for c in log["catalogs"] if c["status"] == "not_reached")
        log["stopped_early"] = (f"{streak} catalog attempts in a row were throttled by "
                                f"the server; {unreached} catalogs not attempted. Rerun later.")
        print(f"stopping: {log['stopped_early']}", flush=True)
    log["finished_utc"] = datetime.now(timezone.utc).isoformat()
    log["failed"] = sum(1 for c in log["catalogs"] if c["status"] == "failed")
    log["not_reached"] = sum(1 for c in log["catalogs"] if c["status"] == "not_reached")
    _write_json(log_path, log)
    return log


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Download every BLS LABSTAT catalog and write sharded series/observation JSONL.")
    p.add_argument("--only", default=None,
                   help="Comma-separated prefixes for tests (WS,CU). Default: all catalogs.")
    p.add_argument("--force", action="store_true")
    p.add_argument("--offline", action="store_true")
    p.add_argument("--data-dir", default=None)
    p.add_argument("--source-root", default=None)
    p.add_argument("--memory-limit", default="auto",
                   help="DuckDB memory budget (e.g. 16GB) or 'auto' from the job/VM allocation.")
    p.add_argument("--threads", type=int, default=2)
    p.add_argument("--concurrency", type=int, default=1,
                   help="Files downloaded at once. Requests stay spaced by --min-interval either way.")
    p.add_argument("--min-interval", type=float, default=0.5,
                   help="Seconds between any two requests to BLS.")
    p.add_argument("--max-throttled", type=int, default=8,
                   help="Stop after this many throttled catalog attempts in a row (0 = never stop).")
    p.add_argument("--catalog-retries", type=int, default=1,
                   help="Times a throttled catalog is tried again after cooling down (0 = move on).")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    memory_limit = resolve_memory_limit(args.memory_limit)
    cfg = AppConfig(memory_limit=memory_limit, threads=args.threads,
                    concurrency=args.concurrency, min_interval_s=args.min_interval,
                    max_consecutive_throttled=args.max_throttled,
                    throttled_catalog_retries=args.catalog_retries)
    if args.threads < 1:
        print("--threads must be positive", file=sys.stderr)
        return 2
    if args.concurrency < 1:
        print("--concurrency must be positive", file=sys.stderr)
        return 2
    if args.min_interval < 0 or args.max_throttled < 0 or args.catalog_retries < 0:
        print("--min-interval, --max-throttled and --catalog-retries cannot be negative",
              file=sys.stderr)
        return 2
    if args.data_dir:
        cfg.data_dir = Path(args.data_dir)
    if args.source_root:
        cfg.source_root = args.source_root
    only = [p.strip().upper() for p in args.only.split(",") if p.strip()] if args.only else None
    if args.offline and not only:
        print("--offline requires --only", file=sys.stderr)
        return 2
    print(f"duckdb memory_limit={cfg.memory_limit}", flush=True)
    log = run_all(cfg, only=only, force=args.force, offline=args.offline)
    print(json.dumps({k: log[k] for k in ("prefixes", "failed", "not_reached", "stopped_early",
                                          "finished_utc") if k in log}, indent=2))
    return 1 if log.get("failed") or log.get("not_reached") else 0
