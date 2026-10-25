"""Command line interface.

  bls-corpus discover --program BD
  bls-corpus download --program BD [--retry-failed]
  bls-corpus parse    --program BD [--offline]
  bls-corpus validate --program BD
  bls-corpus run      --program BD [--offline]
  bls-corpus validate-api --program BD --sample-size 25   (opt-in)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path

from . import __version__, engine as engine_mod, validate as validate_mod
from .config import AppConfig, load_config
from .discovery import discover_remote, snapshot_id_for
from .downloader import Downloader
from .manifest import Manifest, latest_manifest, new_manifest


def _apply_overrides(cfg: AppConfig, args) -> AppConfig:
    if getattr(args, "program", None):
        cfg.program = args.program.upper()
    if getattr(args, "data_dir", None):
        cfg.data_dir = Path(args.data_dir)
    if getattr(args, "concurrency", None):
        cfg.concurrency = args.concurrency
    if getattr(args, "source_root", None):
        cfg.source_root = args.source_root
    return cfg


def _manifest_path(cfg: AppConfig, snapshot_id: str) -> Path:
    return cfg.manifest_root / f"{snapshot_id}.json"


def cmd_discover(cfg: AppConfig, args) -> int:
    disc = discover_remote(cfg.program, cfg.program_url, cfg.user_agent,
                           timeout_s=cfg.read_timeout_s)
    snapshot = args.snapshot or snapshot_id_for(disc)
    print(f"program={disc.program} source={disc.source_url} snapshot={snapshot}")
    for f in disc.files:
        print(f"  {f.name:28} role={f.file_role:16} required={f.required} "
              f"size={f.remote_size} modified={f.listing_last_modified}")
    if disc.required_missing:
        print(f"ERROR missing required: {disc.required_missing}", file=sys.stderr)
        return 2
    if args.output:
        out = Path(args.output)
        out.mkdir(parents=True, exist_ok=True)
        (out / f"discovery-{snapshot}.json").write_text(
            json.dumps(disc.to_dict(), indent=2), encoding="utf-8")
        print(f"inventory written: {out / f'discovery-{snapshot}.json'}")
    return 0


def cmd_download(cfg: AppConfig, args) -> int:
    started = time.perf_counter()
    disc = discover_remote(cfg.program, cfg.program_url, cfg.user_agent,
                           timeout_s=cfg.read_timeout_s)
    snapshot = args.snapshot or snapshot_id_for(disc)
    manifest_path = _manifest_path(cfg, snapshot)
    prior = Manifest.load(manifest_path) if manifest_path.exists() else None
    # Reuse identical snapshot manifest if present (idempotent reruns).
    manifest = prior if prior and prior.snapshot_id == snapshot else new_manifest(
        disc, snapshot, config_summary={"concurrency": cfg.concurrency,
                                        "version": __version__})
    raw_dir = cfg.raw_root / snapshot
    only: set[str] | None = None
    if getattr(args, "retry_failed", False) and prior:
        only = {r.relative_path for r in prior.files if r.download_status == "failed"}
        if not only:
            print("no previously failed files to retry")
            return 0
    Downloader(cfg).download_all(manifest, raw_dir, prior=prior, only=only)
    manifest.save(manifest_path)
    counts = manifest.counts()
    print(f"snapshot={snapshot} raw_dir={raw_dir} manifest={manifest_path}")
    print(f"download counts={counts} elapsed_s={time.perf_counter() - started:.1f}")
    return 0


def _load_manifest_for(cfg: AppConfig, args) -> Manifest:
    if getattr(args, "snapshot", None):
        return Manifest.load(_manifest_path(cfg, args.snapshot))
    path = latest_manifest(cfg.manifest_root, cfg.program)
    if path is None:
        raise SystemExit("no local manifest found; run `discover`/`download` first (or drop --offline)")
    return Manifest.load(path)


def cmd_parse(cfg: AppConfig, args) -> int:
    started = time.perf_counter()
    manifest = _load_manifest_for(cfg, args)
    snapshot = manifest.snapshot_id
    raw_dir = cfg.raw_root / snapshot
    out_dir = cfg.processed_root / snapshot
    stats, schema = engine_mod.process_snapshot(raw_dir, out_dir, snapshot, manifest.program_id)
    present = {p.name for p in raw_dir.iterdir() if p.is_file()}
    for rec in manifest.files:
        name = rec.relative_path
        if name in schema.documentation:
            rec.parse_status = "not_applicable"
        elif name not in present:
            rec.parse_status = "failed"
            rec.error_message = "file missing locally"
        else:
            rec.parse_status = "parsed"
    manifest.save(_manifest_path(cfg, snapshot))
    print(f"snapshot={snapshot} series={stats.series} observations={stats.observations} "
          f"rows_in={stats.rows_in} dup_identical={stats.duplicate_identical} "
          f"dup_conflicts={stats.duplicate_conflicts} malformed={stats.malformed_rows} "
          f"elapsed_s={time.perf_counter() - started:.1f} out={out_dir}")
    return 0


def cmd_validate(cfg: AppConfig, args) -> int:
    manifest = _load_manifest_for(cfg, args)
    snapshot = manifest.snapshot_id
    out_dir = cfg.processed_root / snapshot
    stats_path = out_dir / "parse_stats.json"
    stats = json.loads(stats_path.read_text()) if stats_path.exists() else {}
    rep = validate_mod.validate_snapshot(manifest, cfg.raw_root / snapshot, out_dir, stats)
    jp, mp = validate_mod.save_report(rep, out_dir)
    print(rep.to_markdown())
    print(f"reports: {jp} {mp}")
    return 1 if (rep.files_failed or rep.checksum_failures) else 0


def cmd_validate_api(cfg: AppConfig, args) -> int:
    from .api_validation import compare_with_api

    manifest = _load_manifest_for(cfg, args)
    out_dir = cfg.processed_root / manifest.snapshot_id
    result = compare_with_api(out_dir / "series_catalog.parquet",
                              out_dir / "observations.parquet", cfg,
                              sample_size=args.sample_size)
    print(json.dumps(asdict(result), indent=2))
    return 0


def cmd_run(cfg: AppConfig, args) -> int:
    if not args.offline:
        rc = cmd_download(cfg, args)
        if rc:
            return rc
    rc = cmd_parse(cfg, args)
    if rc:
        return rc
    return cmd_validate(cfg, args)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="bls-corpus", description=__doc__)
    p.add_argument("--config", default=None)
    p.add_argument("--program", default=None, help="LABSTAT prefix, e.g. BD")
    p.add_argument("--data-dir", default=None)
    p.add_argument("--source-root", default=None)
    p.add_argument("--snapshot", default=None, help="override snapshot id")
    sub = p.add_subparsers(dest="command", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--program", default=None, help="LABSTAT prefix, e.g. BD")
    common.add_argument("--data-dir", default=None)
    common.add_argument("--source-root", default=None)
    common.add_argument("--snapshot", default=None, help="override snapshot id")
    d = sub.add_parser("discover", parents=[common]); d.add_argument("--output", default=None)
    dl = sub.add_parser("download", parents=[common])
    dl.add_argument("--concurrency", type=int, default=None)
    dl.add_argument("--retry-failed", action="store_true")
    pa = sub.add_parser("parse", parents=[common]); pa.add_argument("--offline", action="store_true")
    sub.add_parser("validate", parents=[common])
    r = sub.add_parser("run", parents=[common]); r.add_argument("--offline", action="store_true")
    r.add_argument("--concurrency", type=int, default=None)
    va = sub.add_parser("validate-api", parents=[common]); va.add_argument("--sample-size", type=int, default=25)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = _apply_overrides(load_config(args.config), args)
    handlers = {"discover": cmd_discover, "download": cmd_download, "parse": cmd_parse,
                "validate": cmd_validate, "run": cmd_run, "validate-api": cmd_validate_api}
    return handlers[args.command](cfg, args)


if __name__ == "__main__":
    raise SystemExit(main())
