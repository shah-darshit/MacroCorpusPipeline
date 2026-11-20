import hashlib
import threading
import time

import httpx
import pytest

from bls_corpus.pipeline import AppConfig, Downloader, FileRecord, Manifest, RequestPacer


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
    assert any(s == pytest.approx(7.0, abs=0.1) for s in sleeps)


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


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def test_pacer_spaces_requests_and_backs_off_on_throttle():
    clock = FakeClock()
    p = RequestPacer(0.5, 4.0, sleep=clock.sleep, clock=clock.now)
    starts = []
    for _ in range(3):
        p.wait()
        starts.append(clock.t)
    assert starts == [0.0, 0.5, 1.0]
    p.throttled(20.0)
    p.wait()
    assert clock.t == pytest.approx(21.0) and p.interval == 1.0
    for _ in range(3):
        p.throttled(0.0)
    assert p.interval == 4.0  # capped at max_interval_s
    for _ in range(50):
        p.succeeded()
    assert p.interval == 0.5  # eases back to the base, never below it


def test_pacer_is_shared_safely_across_threads():
    p = RequestPacer(0.05)
    times = []
    lock = threading.Lock()

    def worker():
        for _ in range(4):
            p.wait()
            with lock:
                times.append(time.monotonic())

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    times.sort()
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert len(times) == 12 and min(gaps) >= 0.04


def test_one_throttled_file_holds_back_every_downloader(tmp_path):
    clock = FakeClock()
    c = cfg()
    c.max_retries = 0
    c.pacer = RequestPacer(0.5, 10.0, sleep=clock.sleep, clock=clock.now)
    sent = {}

    def handler(request):
        sent[request.url.path] = clock.t
        if request.url.path.endswith("bd.series"):
            return httpx.Response(429, headers={"Retry-After": "60"})
        return httpx.Response(200, content=b"ok")

    t = httpx.MockTransport(handler)
    first = Downloader(c, transport=t, sleep=clock.sleep).download_one(rec(), tmp_path)
    second = Downloader(c, transport=t, sleep=clock.sleep).download_one(
        rec("bd.footnote"), tmp_path)
    assert first.download_status == "failed" and second.download_status == "downloaded"
    assert sent["/bd/bd.footnote"] >= 60.0


def test_download_all_with_concurrency_keeps_spacing(tmp_path):
    c = cfg()
    c.concurrency = 3
    c.pacer = RequestPacer(0.05)
    times = []
    lock = threading.Lock()

    def handler(request):
        with lock:
            times.append(time.monotonic())
        return httpx.Response(200, content=b"ok")

    names = [f"bd.map{i}" for i in range(6)]
    m = Manifest(program_id="BD", source_url="https://x/bd/", created_utc="now",
                 files=[rec(n) for n in names])
    Downloader(c, transport=httpx.MockTransport(handler)).download_all(m, tmp_path)
    times.sort()
    assert len(times) == 6
    assert min(b - a for a, b in zip(times, times[1:])) >= 0.04
