"""Chunked parsers for LABSTAT flat files (BD adapter first).

Files are tab-delimited with a header row; fields are whitespace-padded and
identifiers carry leading zeros, so every field is read as a string, stripped
of padding, and never passed through a numeric-looking type inference.
"""
from __future__ import annotations

import csv
import json
import sqlite3
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from . import normalize, storage

csv.field_size_limit(10 * 1024 * 1024)

# BD series-catalog code columns carried verbatim into metadata_json and,
# where a documented interpretation exists, into dedicated fields.
BD_CODE_COLUMNS = [
    "seasonal", "msa_code", "state_code", "county_code", "industry_code",
    "unitanalysis_code", "dataelement_code", "sizeclass_code",
    "dataclass_code", "ratelevel_code", "periodicity_code", "ownership_code",
]


@dataclass
class ParseStats:
    rows_in: int = 0
    rows_written: int = 0
    malformed_rows: int = 0
    malformed_samples: list[str] = field(default_factory=list)
    non_numeric_values: int = 0
    missing_values: int = 0
    unknown_periods: int = 0
    duplicate_identical: int = 0
    duplicate_conflicts: int = 0
    duplicate_conflict_samples: list[dict] = field(default_factory=list)
    unknown_period_samples: list[str] = field(default_factory=list)

    def note_malformed(self, where: str) -> None:
        self.malformed_rows += 1
        if len(self.malformed_samples) < 20:
            self.malformed_samples.append(where)


def _iter_tsv(path: Path):
    """Yield (row_number_1_based_incl_header, dict) with padded fields stripped."""
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh, delimiter="\t")
        try:
            header = next(reader)
        except StopIteration:
            return
        keys = [h.strip() for h in header]
        for row_no, raw in enumerate(reader, start=2):
            if not raw or all(not c.strip() for c in raw):
                continue
            yield row_no, keys, [c.strip() for c in raw], raw


def read_small_table(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    materialized = list(_iter_tsv(path))
    if not materialized:
        return [], []
    keys = materialized[0][1]
    out = [{k: (vals[i] if i < len(vals) else "") for i, k in enumerate(keys)}
           for _, _, vals, _raw in materialized]
    return keys, out


def parse_series_catalog(path: Path, snapshot_id: str, program: str,
                         seasonal_mapping: dict[str, str] | None = None) -> list[dict]:
    # Catalog (34k rows for BD) fits comfortably; raw tuple kept so the
    # padded series_id survives verbatim in series_id_raw.
    parsed: list[tuple[dict, str]] = []
    for _row_no, keys, vals, raw_vals in _iter_tsv(path):
        r = {k: (vals[i] if i < len(vals) else "") for i, k in enumerate(keys)}
        sid_idx = keys.index("series_id") if "series_id" in keys else 0
        parsed.append((r, raw_vals[sid_idx] if sid_idx < len(raw_vals) else ""))
    out = []
    for r, sid_raw in parsed:
        freq, freq_method = normalize.frequency_from_periodicity(r.get("periodicity_code"))
        status, status_method = normalize.seasonal_status(r.get("seasonal"), seasonal_mapping)
        meta = {c: r.get(c, "") for c in BD_CODE_COLUMNS if c in r}
        meta["begin_year"], meta["begin_period"] = r.get("begin_year", ""), r.get("begin_period", "")
        meta["end_year"], meta["end_period"] = r.get("end_year", ""), r.get("end_period", "")
        meta["footnote_codes"] = r.get("footnote_codes", "")
        out.append({
            "snapshot_id": snapshot_id, "program_id": program,
            "series_id": sid_raw.strip(), "series_id_raw": sid_raw,
            "series_title": r.get("series_title") or None,
            "seasonal_code": r.get("seasonal") or None,
            "seasonal_status": status, "seasonal_status_method": status_method,
            # BD supplies ratelevel/dataelement codes, not a physical unit:
            # measure/unit stay null (documented) rather than fabricated.
            "measure": None, "unit": None,
            "frequency": freq, "frequency_method": freq_method,
            "first_period": None, "last_period": None,  # filled from observations
            "observation_count": 0,
            "source_catalog_file": path.name,
            "metadata_json": json.dumps(meta, sort_keys=True),
        })
    return out


def parse_footnotes(path: Path, snapshot_id: str, program: str) -> list[dict]:
    _, rows = read_small_table(path)
    return [{"snapshot_id": snapshot_id, "program_id": program,
             "footnote_code": r.get("footnote_code", ""), "footnote_text": r.get("footnote_text", ""),
             "source_file": path.name} for r in rows]


def parse_reference(path: Path, table_name: str, snapshot_id: str, program: str) -> list[dict]:
    keys, rows = read_small_table(path)
    if len(keys) < 2:
        return []
    code_key, label_key = keys[0], keys[1]
    out = []
    for r in rows:
        extra = {k: v for k, v in r.items() if k not in (code_key, label_key)}
        out.append({"snapshot_id": snapshot_id, "program_id": program,
                    "table_name": table_name, "code": r.get(code_key, ""),
                    "label": r.get(label_key, ""),
                    "extra_json": json.dumps(extra, sort_keys=True) if extra else None,
                    "source_file": path.name})
    return out


def data_file_precedence(name: str) -> int:
    """AllItems (complete history) outranks Current (overlapping subset)."""
    if "AllItems" in name or "AllData" in name:
        return 0
    if "Current" in name:
        return 1
    return 2


class ObservationParser:
    """Streams observation files into Parquet, deduplicating on
    (series_id, year, period) with an on-disk SQLite key store so memory
    does not scale with corpus size."""

    def __init__(self, snapshot_id: str, program: str, chunk_rows: int = 65536):
        self.snapshot_id, self.program, self.chunk_rows = snapshot_id, program, chunk_rows
        self.stats = ParseStats()
        self.series_stats: dict[str, dict] = {}
        self._tmpdir = tempfile.TemporaryDirectory(prefix="bls-dedupe-")
        self._db = sqlite3.connect(str(Path(self._tmpdir.name) / "keys.db"))
        self._db.execute("CREATE TABLE seen (k TEXT PRIMARY KEY, value_raw TEXT, footnote TEXT, source_file TEXT)")

    def close(self) -> None:
        self._db.close()
        self._tmpdir.cleanup()

    def _seen(self, key: str, value_raw: str, footnote: str, source: str) -> str:
        cur = self._db.execute("SELECT value_raw, footnote, source_file FROM seen WHERE k=?", (key,))
        row = cur.fetchone()
        if row is None:
            self._db.execute("INSERT INTO seen VALUES (?,?,?,?)", (key, value_raw, footnote, source))
            return "new"
        if row[0] == value_raw and row[1] == footnote:
            return "identical"
        if len(self.stats.duplicate_conflict_samples) < 20:
            self.stats.duplicate_conflict_samples.append({
                "key": key, "kept": {"value": row[0], "footnote": row[1], "file": row[2]},
                "discarded": {"value": value_raw, "footnote": footnote, "file": source}})
        return "conflict"

    def parse_files(self, paths: list[Path], out_path: Path) -> tuple[ParseStats, dict]:
        ordered = sorted(paths, key=lambda p: (data_file_precedence(p.name), p.name))
        writer = storage.BatchWriter(out_path, storage.OBSERVATIONS_SCHEMA)
        try:
            for path in ordered:
                self._parse_one(path, writer)
                self._db.commit()
        finally:
            writer.close()
        return self.stats, self.series_stats

    def _parse_one(self, path: Path, writer: storage.BatchWriter) -> None:
        batch: list[dict] = []
        for row_no, keys, vals, _raw_vals in _iter_tsv(path):
            self.stats.rows_in += 1
            if len(vals) != len(keys):
                self.stats.note_malformed(f"{path.name}:{row_no} cols={len(vals)} expected={len(keys)}")
                continue
            r = dict(zip(keys, vals))
            sid = r.get("series_id", "").strip()
            try:
                year = int(r.get("year", ""))
            except ValueError:
                self.stats.note_malformed(f"{path.name}:{row_no} bad year {r.get('year')!r}")
                continue
            period = r.get("period", "").strip()
            value_raw = r.get("value", "")
            if not sid or not period:
                self.stats.note_malformed(f"{path.name}:{row_no} empty series_id/period")
                continue
            verdict = self._seen(f"{sid}|{year}|{period}", value_raw, r.get("footnote_codes", ""), path.name)
            if verdict == "identical":
                self.stats.duplicate_identical += 1
                continue
            if verdict == "conflict":
                self.stats.duplicate_conflicts += 1
                continue
            value_num, value_status = normalize.parse_numeric(value_raw)
            if value_status == "non_numeric":
                self.stats.non_numeric_values += 1
            elif value_status != "numeric":
                self.stats.missing_values += 1
            start, end, kind = normalize.period_dates(year, period)
            if kind in ("unknown_period", "invalid_date"):
                self.stats.unknown_periods += 1
                if len(self.stats.unknown_period_samples) < 20:
                    self.stats.unknown_period_samples.append(period)
            batch.append({
                "snapshot_id": self.snapshot_id, "program_id": self.program,
                "series_id": sid, "year": year, "period": period,
                "period_start": start, "period_end": end, "period_kind": kind,
                "value_raw": value_raw, "value_numeric": value_num,
                "value_status": value_status,
                "footnote_codes": r.get("footnote_codes", "") or None,
                "source_file": path.name, "source_row": row_no,
            })
            st = self.series_stats.setdefault(sid, {"count": 0, "first": None, "last": None,
                                                    "periods": set()})
            st["count"] += 1
            st["periods"].add(period)
            label = f"{year}-{period}"
            st["first"] = label if st["first"] is None or label < st["first"] else st["first"]
            st["last"] = label if st["last"] is None or label > st["last"] else st["last"]
            if len(batch) >= self.chunk_rows:
                writer.write(batch)
                self.stats.rows_written += len(batch)
                batch = []
        if batch:
            writer.write(batch)
            self.stats.rows_written += len(batch)
