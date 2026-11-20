import json
from pathlib import Path

from bls_corpus.pipeline import AppConfig, process_program, run_all

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
    first = tmp_path / "data" / "bd" / "series" / "BDS0000000000000000110001LQ5.json"
    assert first.exists()
    row = json.loads(first.read_text())
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
    waits = []
    log = run_all(cfg, prefixes=["aa", "bb", "cc", "dd", "ee"], sleep=waits.append)
    assert "stopped_early" not in log and log["not_reached"] == 0
    assert waits == [300.0, 600.0, 300.0]  # no pause after the final catalog


def test_unattended_defaults_wait_out_a_long_block(tmp_path, monkeypatch):
    prefixes = ["aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh"]
    _fake_results(monkeypatch, set(prefixes[:7]))
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    waits = []
    log = run_all(cfg, prefixes=prefixes, sleep=waits.append)
    assert "stopped_early" not in log and log["catalogs"][-1]["status"] == "ok"
    assert waits == [300.0, 600.0, 1200.0, 1800.0, 1800.0, 1800.0, 1800.0]


def test_zero_throttle_limit_never_stops(tmp_path, monkeypatch):
    prefixes = [f"a{c}" for c in "abcdefghijkl"]
    _fake_results(monkeypatch, set(prefixes))
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
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
