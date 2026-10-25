"""Robust bulk downloader.

Guarantees:
- streamed bodies to ``<name>.part``; atomic rename only after size verification
- SHA-256 of every completed file; a truncated/failed file is never "complete"
- retry with exponential backoff + jitter on 429/5xx/timeouts/resets,
  honoring ``Retry-After``; other 4xx are permanent
- conditional GET (ETag / Last-Modified) -> 304 handled as skip
- Range resume only when the server proves it with ``206`` + validator match;
  a ``200`` answer to a Range request restarts the file (observed live on
  download.bls.gov, which advertises ranges but ignores them)
"""
from __future__ import annotations

import hashlib
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx

from .config import AppConfig
from .manifest import FileRecord, Manifest

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


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
        return None  # HTTP-date form: fall back to backoff


class Downloader:
    def __init__(self, cfg: AppConfig, transport=None, sleep=time.sleep,
                 client: httpx.Client | None = None):
        self.cfg = cfg
        self.sleep = sleep
        self.transport = transport
        self._client = client
        self._last_request_at = 0.0

    # -- internals ---------------------------------------------------------
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
        # Accept-Encoding: identity is load-bearing: the BLS CDN has been
        # observed labeling plain bodies as gzip, which makes validating
        # decoders fail mid-stream (httpx DecodingError, live 2026-10-19).
        return httpx.Client(headers={"User-Agent": self.cfg.user_agent,
                                     "Accept-Encoding": "identity"},
                            timeout=timeout, transport=self.transport,
                            follow_redirects=True)

    # -- public API --------------------------------------------------------
    def download_one(self, record: FileRecord, dest_dir: Path,
                     prior: FileRecord | None = None) -> FileRecord:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / record.relative_path
        part = dest.with_name(dest.name + ".part")
        client = self._client_or_new()
        headers: dict[str, str] = {}
        # Verified local copy + validators -> conditional request / local skip.
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
                dest.unlink()  # corrupt local copy: never trust it
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
            except Exception as exc:  # noqa: BLE001 - one file must never kill the batch
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
                    append = True  # server proved range support
                elif resume_from > 0:
                    resume_from = 0  # Range ignored (observed on BLS): restart file
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
                part.unlink(missing_ok=True)  # decoded bytes != wire offsets; never resume them
            raise _RetryableError(f"{type(exc).__name__}: {exc}") from exc
        part.replace(dest)  # atomic on same filesystem
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
                    fut.result()  # download_one records failures; never raises
        failed_required = [r for r in records if r.download_status == "failed" and r.required]
        if failed_required:
            names = ", ".join(r.relative_path for r in failed_required)
            raise DownloadError(f"Required file(s) failed to download: {names}")
        return manifest


class _RetryableError(Exception):
    def __init__(self, msg: str, retry_after: float | None = None):
        super().__init__(msg)
        self.retry_after = retry_after


class _PermanentError(Exception):
    pass


class DownloadError(RuntimeError):
    pass


def _to_http_date(listing_or_http: str) -> str:
    """Listing dates look like '7/29/2026 10:00 AM'; HTTP dates pass through."""
    from email.utils import format_datetime
    from datetime import datetime, timezone

    for fmt in ("%m/%d/%Y %I:%M %p", "%a, %d %b %Y %H:%M:%S %Z"):
        try:
            dt = datetime.strptime(listing_or_http, fmt)
            return format_datetime(dt.replace(tzinfo=timezone.utc), usegmt=True)
        except ValueError:
            continue
    return listing_or_http
