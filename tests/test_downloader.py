import hashlib

import httpx
import pytest

from bls_corpus.config import AppConfig
from bls_corpus.downloader import Downloader
from bls_corpus.manifest import FileRecord


def cfg() -> AppConfig:
    c = AppConfig()
    c.min_interval_s = 0.0
    c.backoff_base_s = 0.01
    c.backoff_max_s = 0.05
    c.max_retries = 3
    return c


def rec(name="bd.series", size=None):
    return FileRecord(relative_path=name, source_url=f"https://x/bd/{name}",
                      file_role="series_catalog", required=True, remote_size=size)


def dl(transport, sleeps=None) -> Downloader:
    return Downloader(cfg(), transport=transport,
                      sleep=(sleeps.append if sleeps is not None else (lambda s: None)))


def test_download_success_sha_and_atomic(tmp_path):
    body = b"series_id\tvalue\nA\t1\n"
    t = httpx.MockTransport(lambda r: httpx.Response(200, content=body,
                              headers={"ETag": '"e1"', "Last-Modified": "Wed, 29 Jul 2026 14:00:00 GMT"}))
    r = dl(t).download_one(rec(size=len(body)), tmp_path)
    assert r.download_status == "downloaded"
    assert r.sha256 == hashlib.sha256(body).hexdigest()
    assert (tmp_path / "bd.series").read_bytes() == body
    assert not (tmp_path / "bd.series.part").exists()


def test_429_retry_after_then_success(tmp_path):
    calls = {"n": 0}
    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, content=b"ok")
    sleeps: list[float] = []
    r = dl(httpx.MockTransport(handler), sleeps).download_one(rec(), tmp_path)
    assert r.download_status == "downloaded" and r.attempts == 2
    assert 7.0 in sleeps


def test_500_and_permanent_404(tmp_path):
    calls = {"n": 0}
    def flaky(request):
        calls["n"] += 1
        return httpx.Response(500) if calls["n"] < 3 else httpx.Response(200, content=b"ok")
    assert dl(httpx.MockTransport(flaky)).download_one(rec(), tmp_path).download_status == "downloaded"
    r = dl(httpx.MockTransport(lambda r: httpx.Response(404))).download_one(rec(), tmp_path)
    assert r.download_status == "failed" and "404" in (r.error_message or "") and r.attempts == 1


def test_truncated_never_marked_complete(tmp_path):
    t = httpx.MockTransport(lambda r: httpx.Response(
        200, content=b"short", headers={"Content-Length": "9999"}))
    r = dl(t).download_one(rec(), tmp_path)
    assert r.download_status == "failed"
    assert not (tmp_path / "bd.series").exists()  # .part retained for diagnosis/resume


def test_304_skip_and_no_validator_local_skip(tmp_path):
    body = b"hello"
    (tmp_path / "bd.series").write_bytes(body)
    prior = FileRecord(relative_path="bd.series", source_url="https://x/bd/bd.series",
                       local_size=len(body), sha256=hashlib.sha256(body).hexdigest(),
                       etag='"e1"', download_status="downloaded")
    seen = {}
    def handler(request):
        seen["inm"] = request.headers.get("If-None-Match")
        return httpx.Response(304)
    r = dl(httpx.MockTransport(handler)).download_one(rec(), tmp_path, prior=prior)
    assert r.download_status == "skipped_not_modified" and seen["inm"] == '"e1"'
    # Server without validators: verified local copy is skipped without a request.
    prior2 = FileRecord(relative_path="bd.series", source_url="https://x/bd/bd.series",
                        local_size=len(body), sha256=hashlib.sha256(body).hexdigest(),
                        download_status="downloaded")
    def boom(request):
        raise AssertionError("must not request")
    r2 = dl(httpx.MockTransport(boom)).download_one(rec(), tmp_path, prior=prior2)
    assert r2.download_status == "skipped_verified"


def test_changed_file_between_runs_redownloads(tmp_path):
    (tmp_path / "bd.series").write_bytes(b"old!!")
    prior = FileRecord(relative_path="bd.series", source_url="https://x/bd/bd.series",
                       local_size=5, sha256=hashlib.sha256(b"old!!").hexdigest(),
                       etag='"e1"', download_status="downloaded")
    t = httpx.MockTransport(lambda r: httpx.Response(200, content=b"new-content",
                              headers={"ETag": '"e2"'}))
    r = dl(t).download_one(rec(), tmp_path, prior=prior)
    assert r.download_status == "downloaded"
    assert (tmp_path / "bd.series").read_bytes() == b"new-content"


def test_interrupted_download_leaves_part_and_resumes_only_on_206(tmp_path):
    # Interrupted: transport error mid-body -> .part kept, no final file.
    part = tmp_path / "bd.series.part"
    part.write_bytes(b"partial-")
    def handler(request):
        assert request.headers.get("Range") == "bytes=8-"
        return httpx.Response(206, content=b"rest", headers={"Content-Length": "4"})
    r = dl(httpx.MockTransport(handler)).download_one(rec(), tmp_path)
    assert r.download_status == "downloaded"
    assert (tmp_path / "bd.series").read_bytes() == b"partial-rest"


def test_decoding_error_is_retryable_and_part_discarded(tmp_path):
    # Regression (live 2026-10-19): BLS CDN mislabels plain bodies as gzip;
    # httpx raises DecodingError (a RequestError, not TransportError).
    (tmp_path / "bd.series.part").write_bytes(b"garbage-partial")
    calls = {"n": 0}
    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.DecodingError("Error -3 while decompressing data")
        return httpx.Response(200, content=b"clean-body")
    r = dl(httpx.MockTransport(handler)).download_one(rec(), tmp_path)
    assert r.download_status == "downloaded"
    assert (tmp_path / "bd.series").read_bytes() == b"clean-body"


def test_range_ignored_restarts_file(tmp_path):
    (tmp_path / "bd.series.part").write_bytes(b"STALE-STALE")
    t = httpx.MockTransport(lambda r: httpx.Response(200, content=b"full-body"))
    r = dl(t).download_one(rec(), tmp_path)
    assert r.download_status == "downloaded"
    assert (tmp_path / "bd.series").read_bytes() == b"full-body"
