"""Download every LABSTAT catalog and write one JSON file per series."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import sys
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
    concurrency: int = 2
    min_interval_s: float = 0.35
    connect_timeout_s: float = 15.0
    read_timeout_s: float = 120.0
    max_retries: int = 5
    backoff_base_s: float = 1.0
    backoff_max_s: float = 60.0

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
                    timeout_s: float = 30.0, transport=None) -> DiscoveryResult:
    headers = {"User-Agent": user_agent, "Accept-Encoding": "identity"}
    with httpx.Client(headers=headers, timeout=timeout_s, transport=transport,
                      follow_redirects=True) as client:
        resp = client.get(source_url)
        resp.raise_for_status()
        html = resp.text
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
                       timeout_s: float = 30.0, transport=None) -> tuple[str, list[str]]:
    url = source_root.rstrip("/") + "/"
    headers = {"User-Agent": user_agent, "Accept-Encoding": "identity"}
    with httpx.Client(headers=headers, timeout=timeout_s, transport=transport,
                      follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
        html = resp.text
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


def sha256_file(path: Path, _buf: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(_buf):
            h.update(chunk)
    return h.hexdigest()


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
    def __init__(self, msg: str, retry_after: float | None = None):
        super().__init__(msg)
        self.retry_after = retry_after


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
        self._last_request_at = 0.0

    def _pace(self) -> None:
        wait = self.cfg.min_interval_s - (time.monotonic() - self._last_request_at)
        if wait > 0:
            self.sleep(wait)
        self._last_request_at = time.monotonic()

    def _backoff(self, attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return min(retry_after, 300.0)
        base = min(self.cfg.backoff_base_s * (2 ** attempt), self.cfg.backoff_max_s)
        return base + random.uniform(0, base * 0.25)

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
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / record.relative_path
        part = dest.with_name(dest.name + ".part")
        client = self._client_or_new()
        headers: dict[str, str] = {}
        if prior and prior.sha256 and dest.exists() and dest.stat().st_size == (prior.local_size or -1):
            if sha256_file(dest) == prior.sha256:
                if prior.etag:
                    headers["If-None-Match"] = prior.etag
                if prior.last_modified:
                    headers["If-Modified-Since"] = _to_http_date(prior.last_modified)
                record.sha256, record.local_size = prior.sha256, prior.local_size
                record.etag, record.last_modified = prior.etag, prior.last_modified
                if not headers:
                    record.download_status = "skipped_verified"
                    return record
            else:
                dest.unlink()
                headers.clear()
        last_error: str | None = None
        for attempt in range(self.cfg.max_retries + 1):
            record.attempts = attempt + 1
            self._pace()
            try:
                status = self._attempt(client, record, dest, part, dict(headers))
                if status == "not_modified":
                    record.download_status = "skipped_not_modified"
                    record.local_size = dest.stat().st_size
                    if not record.sha256:
                        record.sha256 = sha256_file(dest)
                    return record
                record.download_status = "downloaded"
                record.local_size = dest.stat().st_size
                record.sha256 = sha256_file(dest)
                return record
            except _PermanentError as exc:
                record.download_status = "failed"
                record.error_message = f"permanent: {exc}"
                return record
            except _RetryableError as exc:
                last_error = str(exc)
                if attempt < self.cfg.max_retries:
                    self.sleep(self._backoff(attempt, exc.retry_after))
            except Exception as exc:  # noqa: BLE001
                record.download_status = "failed"
                record.error_message = f"unexpected: {type(exc).__name__}: {exc}"
                return record
        record.download_status = "failed"
        record.error_message = f"exhausted retries: {last_error}"
        return record

    def _attempt(self, client: httpx.Client, record: FileRecord, dest: Path,
                 part: Path, headers: dict[str, str]) -> str:
        resume_from = part.stat().st_size if part.exists() else 0
        if resume_from > 0:
            headers["Range"] = f"bytes={resume_from}-"
            if record.etag:
                headers["If-Range"] = record.etag
        try:
            with client.stream("GET", record.source_url, headers=headers) as resp:
                if resp.status_code == 304:
                    if not dest.exists():
                        raise _RetryableError("304 received but local file is missing")
                    return "not_modified"
                if resp.status_code in RETRYABLE_STATUS:
                    raise _RetryableError(
                        f"HTTP {resp.status_code}",
                        retry_after=_parse_retry_after(resp.headers.get("Retry-After")))
                if resp.status_code >= 400:
                    raise _PermanentError(f"HTTP {resp.status_code} for {record.source_url}")
                record.etag = resp.headers.get("ETag", record.etag)
                record.last_modified = resp.headers.get("Last-Modified", record.last_modified)
                append = False
                if resume_from > 0 and resp.status_code == 206:
                    append = True
                elif resume_from > 0:
                    resume_from = 0
                expected_total = None
                clen = resp.headers.get("Content-Length")
                if clen is not None:
                    expected_total = (resume_from if append else 0) + int(clen)
                written = resume_from if append else 0
                with open(part, "ab" if append else "wb") as fh:
                    for chunk in resp.iter_bytes(1024 * 256):
                        fh.write(chunk)
                        written += len(chunk)
                if expected_total is not None and written != expected_total:
                    raise _RetryableError(
                        f"truncated: got {written} of {expected_total} bytes")
                if record.remote_size is not None and written != record.remote_size \
                        and expected_total is None:
                    raise _RetryableError(
                        f"size mismatch vs listing: got {written}, listing={record.remote_size}")
        except (httpx.TimeoutException, httpx.TransportError,
                httpx.DecodingError) as exc:
            if isinstance(exc, httpx.DecodingError):
                part.unlink(missing_ok=True)
            raise _RetryableError(f"{type(exc).__name__}: {exc}") from exc
        part.replace(dest)
        return "downloaded"

    def download_all(self, manifest: Manifest, dest_dir: Path,
                     prior: Manifest | None = None,
                     only: set[str] | None = None) -> Manifest:
        prior_by_path = {r.relative_path: r for r in prior.files} if prior else {}
        records = [r for r in manifest.files if only is None or r.relative_path in only]
        workers = max(1, self.cfg.concurrency)
        if workers == 1 or len(records) <= 1:
            for r in records:
                self.download_one(r, dest_dir, prior_by_path.get(r.relative_path))
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futs = {pool.submit(self.download_one, r, dest_dir,
                                    prior_by_path.get(r.relative_path)): r for r in records}
                for fut in as_completed(futs):
                    fut.result()
        failed_required = [r for r in records if r.download_status == "failed" and r.required]
        if failed_required:
            names = ", ".join(r.relative_path for r in failed_required)
            raise DownloadError(f"Required file(s) failed to download: {names}")
        return manifest


# -- layout and write ------------------------------------------------------

def sniff_encoding(path: Path, nbytes: int = 65536) -> str:
    sample = path.read_bytes()[:nbytes]
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def read_header(path: Path) -> tuple[list[str], str]:
    enc = sniff_encoding(path)
    text = path.read_bytes()[:8192].decode(enc, errors="replace")
    first = text.splitlines()[0] if text.strip() else ""
    cols = [c.strip() for c in first.split("\t")]
    while cols and cols[-1] == "":
        cols.pop()
    return [c.strip() for c in cols], enc


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
    return re.sub(r"(_code|_text|_name)$", "", name.lower())


def _label_column(cols: list[str]) -> str | None:
    for name in cols[1:]:
        if name.lower().endswith(("_name", "_text", "_title")):
            return name
    return cols[1] if len(cols) > 1 else None


def _link_mapping(table: str, filename: str, cols: list[str],
                  catalog_cols: list[str]) -> MappingSpec:
    if (filename.lower() == "pc.product"
            and {"industry_code", "product_code", "product_name"}.issubset(cols)
            and {"industry_code", "product_code"}.issubset(catalog_cols)):
        return MappingSpec(
            table=table, path=filename, columns=cols, code_col="product_code",
            label_col="product_name", links_to=["product_code"],
            context_column="industry_code",
        )
    code_col = cols[0]
    label_col = _label_column(cols)
    links: list[str] = []
    if label_col and _stem(code_col) == _stem(label_col):
        code_l = code_col.lower()
        stem = _stem(code_col)
        for c in catalog_cols:
            cl = c.lower()
            if cl == code_l or cl == stem or cl == stem + "_code":
                links.append(c)
    return MappingSpec(table=table, path=filename, columns=cols,
                       code_col=code_col, label_col=label_col, links_to=links)


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
    mapping_files: list[tuple[str, Path, list[str]]] = []
    documentation: list[str] = []
    auxiliary: list[str] = []
    unclassified: list[str] = []
    for name in files:
        path = raw_dir / name
        if not path.is_file() or name.endswith(".part"):
            continue
        low = name.lower()
        if low.endswith(".txt") or low.endswith(".contacts"):
            documentation.append(name)
            continue
        cols, enc = read_header(path)
        encodings.append(enc)
        if low.endswith(".series"):
            catalog_path, catalog_cols = name, cols
            continue
        if ".data." in low:
            data_files.append({"path": name, "rank": _data_rank(name)})
            if not data_cols:
                data_cols = cols
            continue
        if low.endswith(".footnote"):
            footnote_path = name
            continue
        if _looks_like_observations(cols):
            auxiliary.append(name)
            continue
        if len(cols) >= 2:
            mapping_files.append((_suffix(name, program), path, cols))
            continue
        unclassified.append(name)
    mappings = [_link_mapping(table, path.name, cols, catalog_cols)
                for table, path, cols in mapping_files]
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
        have = {c.lower() for c in schema.data_columns}
        for col in ("series_id", "year", "period", "value"):
            if col not in have:
                missing.append(f"observation column '{col}'")
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
        f"encoding={_sql_str(encoding)}, store_rejects=true, ignore_errors=true) "
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
        }


class Engine:
    def __init__(self, raw_dir: Path, out_dir: Path, program: str):
        self.raw_dir = Path(raw_dir)
        self.out_dir = Path(out_dir)
        self.program = program.upper()
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.out_dir / "work.duckdb"
        if self.db_path.exists():
            self.db_path.unlink()
        self.con = duckdb.connect(str(self.db_path))
        self.con.execute("SET preserve_insertion_order=true")
        spill = self.out_dir / "spill"
        spill.mkdir(exist_ok=True)
        self.con.execute(f"SET temp_directory={_sql_str(str(spill))}")
        self.con.execute("""
            CREATE MACRO period_start(y, p) AS (
              CASE
                WHEN p IS NULL OR y IS NULL THEN NULL
                WHEN regexp_matches(p, '^M(0[1-9]|1[0-2])$')
                  THEN make_date(y, CAST(substr(p, 2, 2) AS INTEGER), 1)
                WHEN p IN ('M13', 'Q05', 'S03', 'A01', 'ANN', 'A') THEN make_date(y, 1, 1)
                WHEN regexp_matches(p, '^Q0[1-4]$')
                  THEN make_date(y, (CAST(substr(p, 3, 1) AS INTEGER) - 1) * 3 + 1, 1)
                WHEN p = 'S01' THEN make_date(y, 1, 1)
                WHEN p = 'S02' THEN make_date(y, 7, 1)
                ELSE NULL
              END
            );
            CREATE MACRO period_finish(y, p) AS (
              CASE
                WHEN p IS NULL OR y IS NULL THEN NULL
                WHEN regexp_matches(p, '^M(0[1-9]|1[0-2])$')
                  THEN last_day(make_date(y, CAST(substr(p, 2, 2) AS INTEGER), 1))
                WHEN p IN ('M13', 'Q05', 'S03', 'A01', 'ANN', 'A') THEN make_date(y, 12, 31)
                WHEN regexp_matches(p, '^Q0[1-4]$')
                  THEN last_day(make_date(y, CAST(substr(p, 3, 1) AS INTEGER) * 3, 1))
                WHEN p = 'S01' THEN make_date(y, 6, 30)
                WHEN p = 'S02' THEN make_date(y, 12, 31)
                ELSE NULL
              END
            );
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

    def close(self) -> None:
        self.con.close()

    def process(self, schema: ProgramSchema | None = None) -> ProcessResult:
        started = time.perf_counter()
        schema = schema or infer_schema(self.raw_dir, self.program)
        missing = required_missing(schema)
        if missing:
            raise RuntimeError(f"{self.program}: missing required pieces: {', '.join(missing)}")
        self._load_catalog(schema)
        self._load_mappings(schema)
        self._load_footnotes(schema)
        self._load_observations(schema)
        self._enrich_catalog(schema)
        stats = self._stats()
        self.stats = stats
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
            CREATE TABLE catalog_raw AS
            SELECT rec, {selects}, len(f) AS n_fields,
                   len(list_filter(f[{n}+1:], lambda x: trim(x) <> '')) AS extra
            FROM ({_split_sql(path, schema.encoding)})
        """)

    def _load_mappings(self, schema: ProgramSchema) -> None:
        self.con.execute(
            "CREATE TABLE mappings (table_name VARCHAR, code VARCHAR, label VARCHAR, source_file VARCHAR, context_code VARCHAR)")
        for m in schema.mappings:
            path = self.raw_dir / m.path
            code_i = m.columns.index(m.code_col) + 1
            label_expr = (f"trim(f[{m.columns.index(m.label_col)+1}])"
                          if m.label_col in m.columns else "NULL")
            context_expr = (f"trim(f[{m.columns.index(m.context_column)+1}])"
                            if m.context_column else "NULL")
            self.con.execute(f"""
                INSERT INTO mappings
                SELECT {_sql_str(m.table)}, trim(f[{code_i}]), {label_expr}, {_sql_str(m.path)}, {context_expr}
                FROM ({_split_sql(path, schema.encoding)})
                WHERE len(f) >= 1
            """)

    def _load_footnotes(self, schema: ProgramSchema) -> None:
        self.con.execute(
            "CREATE TABLE footnotes (footnote_code VARCHAR, footnote_text VARCHAR, source_file VARCHAR)")
        if not schema.footnote_path:
            return
        path = self.raw_dir / schema.footnote_path
        header, _ = read_header(path)
        text_i = 2 if len(header) > 1 else 1
        self.con.execute(f"""
            INSERT INTO footnotes
            SELECT trim(f[1]), trim(f[{text_i}]), {_sql_str(schema.footnote_path)}
            FROM ({_split_sql(path, schema.encoding)})
        """)

    def _load_observations(self, schema: ProgramSchema) -> None:
        cols = {c.lower(): i + 1 for i, c in enumerate(schema.data_columns)}
        sid, year, period = cols["series_id"], cols["year"], cols["period"]
        value, n = cols["value"], len(schema.data_columns)
        fn = cols.get("footnote_codes")
        fn_expr = f"nullif(trim(f[{fn}]), '')" if fn else "NULL"
        parts = []
        for item in schema.data_files:
            path = self.raw_dir / item["path"]
            parts.append(f"""
                SELECT trim(f[{sid}]) AS series_id,
                       TRY_CAST(trim(f[{year}]) AS INTEGER) AS year,
                       trim(f[{period}]) AS period,
                       trim(f[{value}]) AS value_raw,
                       {fn_expr} AS footnote_codes,
                       {int(item['rank'])} AS file_rank,
                       {_sql_str(item['path'])} AS source_file,
                       rec AS source_row,
                       len(f) AS n_fields,
                       len(list_filter(f[{n}+1:], lambda x: trim(x) <> '')) AS extra
                FROM ({_split_sql(path, schema.encoding)})
            """)
        self.con.execute("CREATE TABLE obs_raw AS " + " UNION ALL ".join(parts))
        self.con.execute("""
            CREATE TABLE observations AS
            SELECT series_id, year, period,
                   CAST(period_start(year, period) AS VARCHAR) AS period_start,
                   CAST(period_finish(year, period) AS VARCHAR) AS period_end,
                   period_kind(period) AS period_kind,
                   value_raw,
                   TRY_CAST(replace(value_raw, ',', '') AS DOUBLE) AS value_numeric,
                   CASE
                     WHEN value_raw IS NULL OR trim(value_raw) IN ('', '-', '--', '.', 'NA', 'N/A', '(NA)', 'null')
                       THEN 'missing_token'
                     WHEN TRY_CAST(replace(value_raw, ',', '') AS DOUBLE) IS NULL THEN 'non_numeric'
                     ELSE 'numeric'
                   END AS value_status,
                   footnote_codes, source_file, source_row
            FROM obs_raw
            WHERE year IS NOT NULL AND series_id <> '' AND period <> ''
              AND n_fields >= 4 AND extra = 0
            QUALIFY row_number() OVER (
                PARTITION BY series_id, year, period
                ORDER BY file_rank, source_file, source_row
            ) = 1
        """)

    def _enrich_catalog(self, schema: ProgramSchema) -> None:
        sid = _ident("series_id")
        self.con.execute(f"""
            CREATE TABLE series_stats AS
            SELECT series_id,
                   count(*)::BIGINT AS observation_count
            FROM observations GROUP BY 1
        """)
        self.con.execute(f"""
            CREATE TABLE catalog AS
            SELECT c.{sid} AS series_id,
                   coalesce(s.observation_count, 0) AS observation_count
            FROM catalog_raw c
            LEFT JOIN series_stats s ON s.series_id = c.{sid}
            WHERE c.{sid} IS NOT NULL AND c.{sid} <> ''
              AND c.n_fields >= {len(schema.catalog_columns)} AND c.extra = 0
        """)

    def _stats(self) -> ProcessResult:
        rows_in = self.con.execute("SELECT count(*) FROM obs_raw").fetchone()[0]
        kept = self.con.execute("SELECT count(*) FROM observations").fetchone()[0]
        malformed = self.con.execute(
            "SELECT count(*) FROM obs_raw WHERE year IS NULL OR series_id = '' OR period = '' "
            "OR n_fields < 4 OR extra > 0").fetchone()[0]
        keys = self.con.execute("""
            SELECT count(*), sum(n) - count(*),
                   count(*) FILTER (WHERE nd > 1)
            FROM (
              SELECT series_id, year, period, count(*) n,
                     count(DISTINCT coalesce(value_raw,'') || '|' || coalesce(footnote_codes,'')) nd
              FROM obs_raw
              WHERE year IS NOT NULL AND series_id <> '' AND period <> ''
                AND n_fields >= 4 AND extra = 0
              GROUP BY 1,2,3
            )
        """).fetchone()
        missing, nonnum, unknown = self.con.execute("""
            SELECT count(*) FILTER (WHERE value_status = 'missing_token'),
                   count(*) FILTER (WHERE value_status = 'non_numeric'),
                   count(*) FILTER (WHERE period_kind IN ('unknown_period', 'invalid_date'))
            FROM observations
        """).fetchone()
        series = self.con.execute("SELECT count(*) FROM catalog").fetchone()[0]
        return ProcessResult(
            program=self.program,
            series=series, observations=kept, rows_in=rows_in,
            duplicate_identical=int(keys[1] or 0),
            duplicate_conflicts=int(keys[2] or 0),
            malformed_rows=malformed, missing_values=missing,
            non_numeric_values=nonnum, unknown_periods=unknown,
        )

    def export_series(self, schema: ProgramSchema,
                      extra: dict | None = None) -> tuple[Path, Path]:
        extra = extra or {}
        series_dir = self.out_dir / "series"
        if series_dir.exists():
            for old in series_dir.glob("*.json"):
                old.unlink()
        series_dir.mkdir(parents=True, exist_ok=True)
        label_selects = []
        used = {_ident(c) for c in schema.catalog_columns}
        for m in schema.mappings:
            if not m.links_to or not m.label_col:
                continue
            alias = _ident(m.label_col)
            if alias in used:
                continue
            used.add(alias)
            code_col = _ident(m.links_to[0])
            context_match = (
                f"AND context_code = c.{_ident(m.context_column)} "
                if m.context_column else ""
            )
            label_selects.append(
                f"(SELECT label FROM mappings WHERE table_name = {_sql_str(m.table)} "
                f"AND code = c.{code_col} {context_match}LIMIT 1) AS {alias}"
            )
        catalog_selects = [f"c.{_ident(c)} AS {_ident(c)}" for c in schema.catalog_columns]
        select_list = ", ".join(catalog_selects + label_selects) or "c.series_id"
        self.con.execute("CREATE OR REPLACE TABLE catalog_flat AS "
                         f"SELECT {select_list} FROM catalog_raw c "
                         f"WHERE c.{_ident('series_id')} IS NOT NULL "
                         f"AND c.{_ident('series_id')} <> ''")
        self.con.execute("""
            CREATE OR REPLACE TABLE obs_grouped AS
            SELECT series_id,
                   to_json(list(json_object(
                       'year', CAST(year AS VARCHAR),
                       'period', period,
                       'value', value_raw,
                       'footnote_codes', coalesce(footnote_codes, '')
                   ) ORDER BY year, period)) AS observations
            FROM observations
            GROUP BY 1
        """)
        tmp = self.out_dir / "_series.jsonl"
        self.con.execute(f"""
            COPY (
              SELECT f.*, coalesce(o.observations, '[]'::JSON) AS observations
              FROM catalog_flat f
              LEFT JOIN obs_grouped o ON o.series_id = f.series_id
            ) TO {_sql_str(str(tmp))} (FORMAT JSON)
        """)
        index: dict[str, dict] = {}
        with tmp.open(encoding="utf-8") as handle:
            for line in handle:
                rec = json.loads(line)
                if rec.get("observations") is None:
                    rec["observations"] = []
                sid = rec.get("series_id") or ""
                if not sid:
                    continue
                dest = series_dir / f"{sid}.json"
                dest.write_text(json.dumps(rec, indent=2), encoding="utf-8")
                index[sid] = {
                    "path": f"series/{sid}.json",
                    "series_title": rec.get("series_title") or rec.get("series_name") or "",
                    "observation_count": len(rec["observations"]),
                }
        tmp.unlink(missing_ok=True)
        joins = []
        for m in schema.mappings:
            if not m.links_to or not m.label_col:
                continue
            col = _ident(m.links_to[0])
            context_match = (
                f"AND mp.context_code = c.{_ident(m.context_column)}"
                if m.context_column else ""
            )
            row = self.con.execute(f"""
                SELECT
                  count(*) FILTER (WHERE c.{col} IS NOT NULL AND c.{col} <> ''
                    AND mp.label IS NOT NULL),
                  count(*) FILTER (WHERE c.{col} IS NULL OR c.{col} = ''),
                  count(*) FILTER (WHERE c.{col} IS NOT NULL AND c.{col} <> ''
                    AND mp.label IS NULL)
                FROM catalog_raw c
                LEFT JOIN mappings mp
                  ON mp.table_name = {_sql_str(m.table)} AND mp.code = c.{col}
                  {context_match}
            """).fetchone()
            joins.append({
                "file": m.path,
                "catalog_column": m.links_to[0],
                **({"context_column": m.context_column} if m.context_column else {}),
                "label_column": m.label_col,
                "matched": int(row[0] or 0),
                "blank": int(row[1] or 0),
                "unmatched": int(row[2] or 0),
            })
        coverage = self.con.execute("""
            SELECT (SELECT count(*) FROM catalog),
                   (SELECT count(*) FROM observations),
                   (SELECT count(*) FROM catalog WHERE observation_count = 0),
                   (SELECT min(year) FROM observations),
                   (SELECT max(year) FROM observations)
        """).fetchone()
        card = {
            "program": self.program,
            "listing_sha256": extra.get("listing_sha256"),
            "files": extra.get("files", []),
            "catalog_fields": schema.catalog_columns,
            "joins": joins,
            "coverage": {
                "series": coverage[0],
                "observations": coverage[1],
                "catalog_without_observations": coverage[2],
                "year_min": coverage[3],
                "year_max": coverage[4],
            },
            "validation": self.stats.parse_stats() if getattr(self, "stats", None) else {},
        }
        card_path = self.out_dir / "catalog.json"
        index_path = self.out_dir / "index.json"
        _write_json(card_path, card)
        _write_json(index_path, index)
        return card_path, index_path


def process_catalog(raw_dir: Path, out_dir: Path, program: str,
                    extra: dict | None = None) -> tuple[ProcessResult, ProgramSchema]:
    schema = infer_schema(raw_dir, program)
    engine = Engine(raw_dir, out_dir, program)
    try:
        stats = engine.process(schema)
        engine.export_series(schema, extra=extra)
        return stats, schema
    finally:
        engine.close()
        engine.db_path.unlink(missing_ok=True)
        spill = Path(out_dir) / "spill"
        if spill.exists():
            for p in spill.iterdir():
                p.unlink()
            spill.rmdir()


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
        })
    return rows


def _unchanged(catalog_json: Path, listing_sha256: str) -> bool:
    if not catalog_json.exists():
        return False
    try:
        card = json.loads(catalog_json.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    return bool(card.get("listing_sha256") and card["listing_sha256"] == listing_sha256)


def process_program(cfg: AppConfig, force: bool = False,
                    offline: bool = False) -> dict:
    program = cfg.program.upper()
    raw_dir = cfg.raw_root
    out_dir = cfg.catalog_dir
    started = time.perf_counter()
    result = {
        "program": program, "status": "pending", "series": 0,
        "observations": 0, "error": None, "elapsed_s": 0.0,
    }
    try:
        if offline:
            if not raw_dir.exists():
                raise FileNotFoundError(f"no local raw files at {raw_dir}")
            extra = {"listing_sha256": None, "files": []}
        else:
            disc = discover_remote(cfg.program, cfg.program_url, cfg.user_agent,
                                   timeout_s=cfg.read_timeout_s)
            if not force and _unchanged(out_dir / "catalog.json", disc.listing_sha256):
                card = json.loads((out_dir / "catalog.json").read_text(encoding="utf-8"))
                cov = card.get("coverage") or {}
                result.update({
                    "status": "skipped",
                    "series": cov.get("series", 0),
                    "observations": cov.get("observations", 0),
                    "elapsed_s": round(time.perf_counter() - started, 2),
                })
                return result
            manifest_path = out_dir / "download.json"
            prior = Manifest.load(manifest_path) if manifest_path.exists() else None
            manifest = new_manifest(disc)
            Downloader(cfg).download_all(manifest, raw_dir, prior=prior)
            manifest.save(manifest_path)
            extra = {
                "listing_sha256": disc.listing_sha256,
                "files": _file_receipt(raw_dir, manifest),
            }
        stats, _schema = process_catalog(raw_dir, out_dir, program, extra=extra)
        result.update({
            "status": "ok",
            "series": stats.series,
            "observations": stats.observations,
            "duplicate_identical": stats.duplicate_identical,
            "duplicate_conflicts": stats.duplicate_conflicts,
            "elapsed_s": round(time.perf_counter() - started, 2),
        })
    except (DiscoveryError, DownloadError, RuntimeError, FileNotFoundError, OSError) as exc:
        result.update({
            "status": "failed",
            "error": str(exc),
            "elapsed_s": round(time.perf_counter() - started, 2),
        })
    return result


def run_all(cfg: AppConfig, only: list[str] | None = None, force: bool = False,
            offline: bool = False, prefixes: list[str] | None = None) -> dict:
    if prefixes is None:
        if only:
            prefixes = [p.lower() for p in only]
        else:
            _html, prefixes = fetch_root_listing(
                cfg.source_root, cfg.user_agent, timeout_s=cfg.read_timeout_s)
    log = {
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "prefixes": prefixes,
        "catalogs": [],
    }
    for prefix in prefixes:
        print(f"[{prefix}] start", flush=True)
        one = process_program(replace(cfg, program=prefix.upper(), data_dir=Path(cfg.data_dir)),
                              force=force, offline=offline)
        log["catalogs"].append(one)
        print(f"[{prefix}] {one['status']} series={one['series']} "
              f"obs={one['observations']} {one.get('error') or ''}", flush=True)
        _write_json(Path(cfg.data_dir) / "run.json", log)
    log["finished_utc"] = datetime.now(timezone.utc).isoformat()
    log["failed"] = sum(1 for c in log["catalogs"] if c["status"] == "failed")
    _write_json(Path(cfg.data_dir) / "run.json", log)
    return log


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Download every BLS LABSTAT catalog and write one JSON file per series.")
    p.add_argument("--only", default=None,
                   help="Comma-separated prefixes for tests (WS,CU). Default: all catalogs.")
    p.add_argument("--force", action="store_true")
    p.add_argument("--offline", action="store_true")
    p.add_argument("--data-dir", default=None)
    p.add_argument("--source-root", default=None)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = AppConfig()
    if args.data_dir:
        cfg.data_dir = Path(args.data_dir)
    if args.source_root:
        cfg.source_root = args.source_root
    only = [p.strip().upper() for p in args.only.split(",") if p.strip()] if args.only else None
    if args.offline and not only:
        print("--offline requires --only", file=sys.stderr)
        return 2
    log = run_all(cfg, only=only, force=args.force, offline=args.offline)
    print(json.dumps({k: log[k] for k in ("prefixes", "failed", "finished_utc") if k in log},
                     indent=2))
    return 1 if log.get("failed") else 0
