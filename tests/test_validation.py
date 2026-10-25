"""Synthetic end-to-end (offline): raw fixtures -> parse -> validate -> rerun."""
import json
from pathlib import Path

import httpx

from bls_corpus import cli
from bls_corpus.config import AppConfig
from bls_corpus.discovery import parse_listing
from bls_corpus.downloader import Downloader
from bls_corpus.manifest import Manifest, new_manifest

FIX = Path(__file__).parent / "fixtures" / "bd"


def _manifest_for_fixtures(cfg: AppConfig) -> Manifest:
    names = sorted(p.name for p in FIX.iterdir())
    lines = "".join(
        f' 7/29/2026 10:00 AM  {1000 + i} <A HREF="/pub/time.series/bd/{n}">{n}</A><br>\n'
        for i, n in enumerate(names))
    disc = parse_listing(f"<pre>{lines}</pre>", "BD", "https://fixture/bd/")
    # Fixture listing sizes differ from real bytes; downloader size-vs-listing
    # check only fires without Content-Length, and parse uses local files.
    manifest = new_manifest(disc, "bd-test-snapshot", {})
    raw = cfg.raw_root / "bd-test-snapshot"
    raw.mkdir(parents=True, exist_ok=True)
    for p in FIX.iterdir():
        (raw / p.name).write_bytes(p.read_bytes())
    for rec in manifest.files:
        rec.download_status = "downloaded"
        rec.local_size = (raw / rec.relative_path).stat().st_size
    return manifest


def _run_parse_validate(cfg: AppConfig, manifest: Manifest):
    mpath = cfg.manifest_root / f"{manifest.snapshot_id}.json"
    manifest.save(mpath)
    class A:  # minimal argparse-like namespace
        program = "BD"; data_dir = None; concurrency = None; source_root = None
        snapshot = manifest.snapshot_id; offline = True; sample_size = 5
    assert cli.cmd_parse(cfg, A()) == 0
    return cli.cmd_validate(cfg, A())


def test_end_to_end_synthetic_and_idempotent_rerun(tmp_path):
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    cfg.min_interval_s = 0
    manifest = _manifest_for_fixtures(cfg)
    out = cfg.processed_root / manifest.snapshot_id

    assert _run_parse_validate(cfg, manifest) == 0
    import pyarrow.parquet as pq
    series = pq.read_table(out / "series_catalog.parquet").to_pylist()
    obs = pq.read_table(out / "observations.parquet").to_pylist()
    assert len(series) == 40
    assert len(obs) > 0
    assert all(o["series_id"] and o["period"] for o in obs)
    report = json.loads((out / f"validation-{manifest.snapshot_id}.json").read_text())
    assert report["series_in_catalog"] == 40
    assert report["files_failed"] == 0
    first_count = len(obs)

    # Idempotent re-parse: deterministic paths, no duplicated observations.
    class A:
        program = "BD"; data_dir = None; concurrency = None; source_root = None
        snapshot = manifest.snapshot_id; offline = True; sample_size = 5
    assert cli.cmd_parse(cfg, A()) == 0
    assert pq.read_table(out / "observations.parquet").num_rows == first_count


def test_missing_metadata_file_reported_not_fatal(tmp_path):
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    manifest = _manifest_for_fixtures(cfg)
    raw = cfg.raw_root / manifest.snapshot_id
    (raw / "bd.industry").unlink()
    for rec in manifest.files:
        if rec.relative_path == "bd.industry":
            rec.download_status = "failed"
    mpath = cfg.manifest_root / f"{manifest.snapshot_id}.json"
    manifest.save(mpath)
    class A:
        program = "BD"; data_dir = None; concurrency = None; source_root = None
        snapshot = manifest.snapshot_id; offline = True; sample_size = 5
    assert cli.cmd_parse(cfg, A()) == 0  # missing mapping degrades, does not crash
    rec = manifest.record_for("bd.industry")
    assert rec is not None  # failed file stays in the inventory
