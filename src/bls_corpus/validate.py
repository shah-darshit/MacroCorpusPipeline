"""Dataset-level validation over a manifest + processed Parquet outputs."""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pyarrow.parquet as pq

from .manifest import Manifest
from .normalize import split_footnotes


@dataclass
class ValidationReport:
    snapshot_id: str
    program_id: str
    files_discovered: int = 0
    files_downloaded: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    unrecognized_files: list[str] = field(default_factory=list)
    series_in_catalog: int = 0
    series_with_observations: int = 0
    catalog_series_without_observations: int = 0
    observation_series_not_in_catalog: list[str] = field(default_factory=list)
    observations: int = 0
    duplicate_identical: int = 0
    duplicate_conflicts: int = 0
    malformed_rows: int = 0
    non_numeric_values: int = 0
    missing_values: int = 0
    unknown_periods: int = 0
    unresolved_footnote_codes: list[str] = field(default_factory=list)
    footnote_codes_used: list[str] = field(default_factory=list)
    empty_files: list[str] = field(default_factory=list)
    checksum_failures: list[str] = field(default_factory=list)
    parse_stats: dict = field(default_factory=dict)
    notes: list[str] = field(default_factory=lambda: [
        "An empty error list is not proof of completeness; coverage is assessed "
        "against the discovered inventory and catalog/observation cross-checks."
    ])

    def to_dict(self) -> dict:
        return asdict(self)

    def to_markdown(self) -> str:
        d = self.to_dict()
        lines = [f"# Validation report — {self.program_id} snapshot `{self.snapshot_id}`", ""]
        for k, v in d.items():
            if k in ("snapshot_id", "program_id"):
                continue
            lines.append(f"- **{k}**: {v}")
        return "\n".join(lines) + "\n"


def validate_snapshot(manifest: Manifest, raw_dir: Path, processed_dir: Path,
                      parse_stats: dict | None = None) -> ValidationReport:
    from .downloader import sha256_file

    rep = ValidationReport(snapshot_id=manifest.snapshot_id, program_id=manifest.program_id,
                           parse_stats=parse_stats or {})
    statuses = Counter(r.download_status for r in manifest.files)
    rep.files_discovered = len(manifest.files)
    rep.files_downloaded = statuses.get("downloaded", 0)
    rep.files_skipped = statuses.get("skipped_not_modified", 0) + statuses.get("skipped_verified", 0)
    rep.files_failed = statuses.get("failed", 0)
    rep.unrecognized_files = [r.relative_path for r in manifest.files if r.file_role == "unclassified"]
    for r in manifest.files:
        local = raw_dir / r.relative_path
        if r.download_status in ("downloaded", "skipped_not_modified", "skipped_verified"):
            if not local.exists() or local.stat().st_size == 0:
                rep.empty_files.append(r.relative_path)
            elif r.sha256 and sha256_file(local) != r.sha256:
                rep.checksum_failures.append(r.relative_path)

    catalog_path = processed_dir / "series_catalog.parquet"
    obs_path = processed_dir / "observations.parquet"
    foot_path = processed_dir / "footnotes.parquet"
    defined_footnotes: set[str] = set()
    if foot_path.exists():
        defined_footnotes = set(pq.read_table(foot_path, columns=["footnote_code"])
                                .column("footnote_code").to_pylist())
    catalog_ids: set[str] = set()
    if catalog_path.exists():
        cat = pq.read_table(catalog_path, columns=["series_id"])
        catalog_ids = set(cat.column("series_id").to_pylist())
        rep.series_in_catalog = len(catalog_ids)
    obs_ids: set[str] = set()
    used_footnotes: set[str] = set()
    if obs_path.exists():
        pf = pq.ParquetFile(obs_path)
        rep.observations = pf.metadata.num_rows
        for batch in pf.iter_batches(columns=["series_id", "footnote_codes"], batch_size=262144):
            obs_ids.update(batch.column("series_id").to_pylist())
            for fc in batch.column("footnote_codes").to_pylist():
                used_footnotes.update(split_footnotes(fc))
    rep.series_with_observations = len(obs_ids & catalog_ids) if catalog_ids else len(obs_ids)
    rep.catalog_series_without_observations = len(catalog_ids - obs_ids)
    rep.observation_series_not_in_catalog = sorted(obs_ids - catalog_ids)[:100]
    stats = parse_stats or {}
    rep.duplicate_identical = int(stats.get("duplicate_identical", 0))
    rep.duplicate_conflicts = int(stats.get("duplicate_conflicts", 0))
    rep.malformed_rows = int(stats.get("malformed_rows", 0))
    rep.non_numeric_values = int(stats.get("non_numeric_values", 0))
    rep.missing_values = int(stats.get("missing_values", 0))
    rep.unknown_periods = int(stats.get("unknown_periods", 0))
    rep.footnote_codes_used = sorted(used_footnotes)
    rep.unresolved_footnote_codes = sorted(used_footnotes - defined_footnotes)
    return rep


def save_report(rep: ValidationReport, out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    jp = out_dir / f"validation-{rep.snapshot_id}.json"
    mp = out_dir / f"validation-{rep.snapshot_id}.md"
    jp.write_text(json.dumps(rep.to_dict(), indent=2), encoding="utf-8")
    mp.write_text(rep.to_markdown(), encoding="utf-8")
    return jp, mp
