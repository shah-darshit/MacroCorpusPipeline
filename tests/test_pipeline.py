import json
from pathlib import Path

from bls_corpus.pipeline import AppConfig, load_series, process_program, run_all

FIX = Path(__file__).parent / "fixtures" / "bd"


def test_offline_process_program_writes_series_files(tmp_path):
    raw = tmp_path / "data" / "bd" / "raw"
    raw.mkdir(parents=True)
    for p in FIX.iterdir():
        (raw / p.name).write_bytes(p.read_bytes())
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    cfg.program = "BD"
    result = process_program(cfg, offline=True)
    assert result["status"] == "ok"
    assert result["series"] == 40
    out = tmp_path / "data" / "bd"
    assert list((out / "series").glob("part-*.jsonl"))
    row = load_series(out, "BDS0000000000000000110001LQ5")
    assert row["series_id"].startswith("BD")
    assert "snapshot_id" not in row


def test_run_all_only_offline(tmp_path):
    raw = tmp_path / "data" / "bd" / "raw"
    raw.mkdir(parents=True)
    for p in FIX.iterdir():
        (raw / p.name).write_bytes(p.read_bytes())
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    log = run_all(cfg, only=["BD"], force=True, offline=True, prefixes=["bd"])
    assert log["catalogs"][0]["status"] == "ok"
    run_path = tmp_path / "data" / "run.json"
    assert run_path.exists()
    saved = json.loads(run_path.read_text())
    assert saved["catalogs"][0]["program"] == "BD"


def _fake_results(monkeypatch, throttled: set[str]):
    import bls_corpus.pipeline as pl

    def fake(cfg, force=False, offline=False):
        bad = cfg.program.lower() in throttled
        return {"program": cfg.program, "status": "failed" if bad else "ok", "series": 0,
                "observations": 0, "error": "HTTP 429" if bad else None,
                **({"throttled": True} if bad else {})}
    monkeypatch.setattr(pl, "process_program", fake)


def test_run_all_stops_after_consecutive_throttles(tmp_path, monkeypatch):
    _fake_results(monkeypatch, {"bb", "cc", "dd", "ee"})
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    cfg.throttled_catalog_retries = 0
    cfg.max_consecutive_throttled = 3
    waits = []
    log = run_all(cfg, prefixes=["aa", "bb", "cc", "dd", "ee", "ff"], sleep=waits.append)
    assert [c["status"] for c in log["catalogs"]] == [
        "ok", "failed", "failed", "failed", "not_reached", "not_reached"]
    assert waits == [300.0, 600.0]
    assert log["not_reached"] == 2 and "stopped_early" in log


def test_run_all_cooldown_resets_after_a_success(tmp_path, monkeypatch):
    _fake_results(monkeypatch, {"aa", "bb", "dd", "ee"})
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    cfg.throttled_catalog_retries = 0
    waits = []
    log = run_all(cfg, prefixes=["aa", "bb", "cc", "dd", "ee"], sleep=waits.append)
    assert "stopped_early" not in log and log["not_reached"] == 0
    assert waits == [300.0, 600.0, 300.0]  # no pause after the final catalog


def test_unattended_defaults_wait_out_a_long_block(tmp_path, monkeypatch):
    prefixes = ["aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh"]
    _fake_results(monkeypatch, set(prefixes[:7]))
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    cfg.throttled_catalog_retries = 0
    waits = []
    log = run_all(cfg, prefixes=prefixes, sleep=waits.append)
    assert "stopped_early" not in log and log["catalogs"][-1]["status"] == "ok"
    assert waits == [300.0, 600.0, 1200.0, 1800.0, 1800.0, 1800.0, 1800.0]


def test_zero_throttle_limit_never_stops(tmp_path, monkeypatch):
    prefixes = [f"a{c}" for c in "abcdefghijkl"]
    _fake_results(monkeypatch, set(prefixes))
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    cfg.throttled_catalog_retries = 0
    cfg.max_consecutive_throttled = 0
    waits = []
    log = run_all(cfg, prefixes=prefixes, sleep=waits.append)
    assert "stopped_early" not in log and log["not_reached"] == 0
    assert len(waits) == len(prefixes) - 1 and max(waits) == cfg.throttle_cooldown_max_s


def test_run_all_shares_one_pacer_across_catalogs(tmp_path, monkeypatch):
    import bls_corpus.pipeline as pl
    seen = []

    def fake(cfg, force=False, offline=False):
        seen.append(cfg.pacer)
        return {"program": cfg.program, "status": "ok", "series": 0, "observations": 0}
    monkeypatch.setattr(pl, "process_program", fake)
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    run_all(cfg, prefixes=["aa", "bb", "cc"])
    assert seen[0] is not None and all(p is seen[0] for p in seen)


def test_cli_pacing_flags(tmp_path, monkeypatch):
    import bls_corpus.pipeline as pl
    captured = {}

    def fake_run_all(cfg, **kwargs):
        captured["cfg"] = cfg
        return {"prefixes": [], "failed": 0}
    monkeypatch.setattr(pl, "run_all", fake_run_all)
    assert pl.main(["--only", "bd", "--data-dir", str(tmp_path), "--concurrency", "2",
                    "--min-interval", "1.5", "--max-throttled", "0"]) == 0
    cfg = captured["cfg"]
    assert (cfg.concurrency, cfg.min_interval_s, cfg.max_consecutive_throttled) == (2, 1.5, 0)
    assert pl.main(["--concurrency", "0"]) == 2
    assert pl.main(["--min-interval", "-1"]) == 2


def _scripted(monkeypatch, script: dict[str, list[bool]]):
    """script maps a prefix to whether each successive attempt is throttled."""
    import bls_corpus.pipeline as pl
    calls = []

    def fake(cfg, force=False, offline=False):
        name = cfg.program.lower()
        calls.append(name)
        plan = script.get(name, [])
        n = calls.count(name) - 1
        bad = plan[n] if n < len(plan) else (plan[-1] if plan else False)
        return {"program": cfg.program, "status": "failed" if bad else "ok", "series": 0,
                "observations": 0, "error": "HTTP 429" if bad else None,
                **({"throttled": True} if bad else {})}
    monkeypatch.setattr(pl, "process_program", fake)
    return calls


def test_throttled_catalog_is_retried_before_the_next_one(tmp_path, monkeypatch):
    calls = _scripted(monkeypatch, {"bb": [True, False]})
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    waits = []
    log = run_all(cfg, prefixes=["aa", "bb", "cc"], sleep=waits.append)
    assert calls == ["aa", "bb", "bb", "cc"]
    assert [c["status"] for c in log["catalogs"]] == ["ok", "ok", "ok"]
    assert log["catalogs"][1]["attempts"] == 2 and log["failed"] == 0
    assert waits == [300.0]


def test_still_throttled_catalog_gets_a_last_try_at_the_end(tmp_path, monkeypatch):
    calls = _scripted(monkeypatch, {"bb": [True, True, False]})
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    waits = []
    log = run_all(cfg, prefixes=["aa", "bb", "cc"], sleep=waits.append)
    assert calls == ["aa", "bb", "bb", "cc", "bb"]
    assert log["catalogs"][1]["status"] == "ok" and log["catalogs"][1]["attempts"] == 3
    assert waits == [300.0, 600.0]  # cc succeeded, so no pause before the last try


def test_stop_limit_counts_retries(tmp_path, monkeypatch):
    calls = _scripted(monkeypatch, {"aa": [True], "bb": [True], "cc": [True]})
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    cfg.max_consecutive_throttled = 3
    waits = []
    log = run_all(cfg, prefixes=["aa", "bb", "cc"], sleep=waits.append)
    assert calls == ["aa", "aa", "bb"]
    assert [c["status"] for c in log["catalogs"]] == ["failed", "failed", "not_reached"]
    assert waits == [300.0, 600.0] and "stopped_early" in log and log["not_reached"] == 1


def test_non_throttle_failure_is_not_retried(tmp_path, monkeypatch):
    import bls_corpus.pipeline as pl
    calls = []

    def fake(cfg, force=False, offline=False):
        calls.append(cfg.program.lower())
        return {"program": cfg.program, "status": "failed", "series": 0, "observations": 0,
                "error": "RuntimeError: validation failed"}
    monkeypatch.setattr(pl, "process_program", fake)
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    waits = []
    run_all(cfg, prefixes=["aa", "bb"], sleep=waits.append)
    assert calls == ["aa", "bb"] and waits == []


def test_retry_resumes_catalog_and_refetches_only_blocked_file(tmp_path, monkeypatch):
    """End to end: one mapping file is refused on the first pass; the retry completes BD."""
    import httpx
    import bls_corpus.pipeline as pl
    files = {p.name: p.read_bytes() for p in FIX.iterdir()}
    listing = "<pre>" + "".join(
        f' 7/29/2026 10:00 AM {len(body):>12} <A HREF="/pub/time.series/bd/{name}">{name}</A><br>\n'
        for name, body in files.items()) + "</pre>"
    full_bodies = []
    blocked = {"bd.industry": 1}  # refuse this file once

    def handler(request):
        name = request.url.path.rsplit("/", 1)[-1]
        if not name:
            return httpx.Response(200, text=listing)
        if blocked.get(name):
            blocked[name] -= 1
            return httpx.Response(429)
        if "If-Modified-Since" in request.headers or "If-None-Match" in request.headers:
            return httpx.Response(304)
        full_bodies.append(name)
        return httpx.Response(200, content=files[name],
                              headers={"Last-Modified": "Wed, 29 Jul 2026 10:00:00 GMT"})

    real_client = httpx.Client
    monkeypatch.setattr(pl.httpx, "Client", lambda **kw: real_client(
        **{**kw, "transport": httpx.MockTransport(handler)}))
    cfg = AppConfig(max_retries=0, backoff_base_s=0.0,
                    pacer=pl.RequestPacer(0.0, sleep=lambda s: None))
    cfg.data_dir = tmp_path / "data"
    waits = []
    log = run_all(cfg, prefixes=["bd"], sleep=waits.append)
    one = log["catalogs"][0]
    assert (one["status"], one["attempts"], one["series"]) == ("ok", 2, 40)
    assert waits == [300.0] and log["failed"] == 0
    # Every file body crossed the wire exactly once; the retry only fetched the blocked one.
    assert sorted(full_bodies) == sorted(files)
