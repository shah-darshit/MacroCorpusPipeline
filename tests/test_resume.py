"""Resume / snapshot-reuse behaviour across pipeline reruns."""
import hashlib

import httpx

from bls_corpus.config import AppConfig
from bls_corpus.downloader import Downloader, sha256_file
from bls_corpus.manifest import FileRecord


def _cfg() -> AppConfig:
    c = AppConfig()
    c.min_interval_s = 0.0
    c.backoff_base_s = 0.01
    c.backoff_max_s = 0.05
    return c


def test_resume_after_interruption_completes_without_final_file(tmp_path):
    body = b"complete-body-after-resume"
    # Simulate interruption: only a .part exists, no final file, no record.
    (tmp_path / "bd.series.part").write_bytes(body[:10])
    def handler(request):
        if request.headers.get("Range") == "bytes=10-":
            return httpx.Response(206, content=body[10:])
        return httpx.Response(200, content=body)
    rec = FileRecord(relative_path="bd.series", source_url="https://x/bd/bd.series",
                     file_role="series_catalog", required=True)
    r = Downloader(_cfg(), transport=httpx.MockTransport(handler),
                   sleep=lambda s: None).download_one(rec, tmp_path)
    assert r.download_status == "downloaded"
    assert (tmp_path / "bd.series").read_bytes() == body
    assert r.sha256 == hashlib.sha256(body).hexdigest()
    assert sha256_file(tmp_path / "bd.series") == r.sha256


def test_corrupt_completed_file_is_redownloaded_not_trusted(tmp_path):
    (tmp_path / "bd.series").write_bytes(b"corrupt!")
    prior = FileRecord(relative_path="bd.series", source_url="https://x/bd/bd.series",
                       local_size=8, sha256=hashlib.sha256(b"original").hexdigest(),
                       etag='"e1"', download_status="downloaded")
    rec = FileRecord(relative_path="bd.series", source_url="https://x/bd/bd.series",
                     file_role="series_catalog", required=True)
    t = httpx.MockTransport(lambda r: httpx.Response(200, content=b"original"))
    r = Downloader(_cfg(), transport=t, sleep=lambda s: None).download_one(rec, tmp_path, prior=prior)
    assert r.download_status == "downloaded"
    assert (tmp_path / "bd.series").read_bytes() == b"original"
