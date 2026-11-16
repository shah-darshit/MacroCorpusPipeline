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
    waits = []
    log = run_all(cfg, prefixes=["aa", "bb", "cc", "dd", "ee", "ff"], sleep=waits.append)
    assert [c["status"] for c in log["catalogs"]] == [
        "ok", "failed", "failed", "failed", "not_reached", "not_reached"]
    assert waits == [cfg.throttle_cooldown_s] * 2
    assert log["not_reached"] == 2 and "stopped_early" in log


def test_run_all_cooldown_resets_after_a_success(tmp_path, monkeypatch):
    _fake_results(monkeypatch, {"aa", "bb", "dd", "ee"})
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    waits = []
    log = run_all(cfg, prefixes=["aa", "bb", "cc", "dd", "ee"], sleep=waits.append)
    assert "stopped_early" not in log and log["not_reached"] == 0
    assert len(waits) == 3  # no pause after the final catalog
