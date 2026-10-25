"""Explicit-schema Parquet/CSV storage with atomic writes."""
from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SERIES_SCHEMA = pa.schema([
    ("snapshot_id", pa.string()), ("program_id", pa.string()),
    ("series_id", pa.string()), ("series_id_raw", pa.string()),
    ("series_title", pa.string()),
    ("seasonal_code", pa.string()), ("seasonal_status", pa.string()),
    ("seasonal_status_method", pa.string()),
    ("measure", pa.string()), ("unit", pa.string()),
    ("frequency", pa.string()), ("frequency_method", pa.string()),
    ("first_period", pa.string()), ("last_period", pa.string()),
    ("observation_count", pa.int64()),
    ("source_catalog_file", pa.string()), ("metadata_json", pa.string()),
])

OBSERVATIONS_SCHEMA = pa.schema([
    ("snapshot_id", pa.string()), ("program_id", pa.string()),
    ("series_id", pa.string()),
    ("year", pa.int32()), ("period", pa.string()),
    ("period_start", pa.string()), ("period_end", pa.string()),
    ("period_kind", pa.string()),
    ("value_raw", pa.string()), ("value_numeric", pa.float64()),
    ("value_status", pa.string()),
    ("footnote_codes", pa.string()),
    ("source_file", pa.string()), ("source_row", pa.int64()),
])

FOOTNOTES_SCHEMA = pa.schema([
    ("snapshot_id", pa.string()), ("program_id", pa.string()),
    ("footnote_code", pa.string()), ("footnote_text", pa.string()),
    ("source_file", pa.string()),
])

REFERENCE_SCHEMA = pa.schema([
    ("snapshot_id", pa.string()), ("program_id", pa.string()),
    ("table_name", pa.string()), ("code", pa.string()),
    ("label", pa.string()), ("extra_json", pa.string()),
    ("source_file", pa.string()),
])


class BatchWriter:
    """Incremental Parquet writer: one temp file, atomic rename on close."""

    def __init__(self, path: Path, schema: pa.Schema):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        self.schema = schema
        self._writer = pq.ParquetWriter(self.tmp, schema, compression="snappy")
        self.rows = 0

    def write(self, rows: list[dict]) -> None:
        if not rows:
            return
        cols = {name: [r.get(name) for r in rows] for name in self.schema.names}
        self._writer.write_table(pa.table(cols, schema=self.schema))
        self.rows += len(rows)

    def close(self) -> Path:
        self._writer.close()
        os.replace(self.tmp, self.path)
        return self.path

    def abort(self) -> None:
        try:
            self._writer.close()
        except Exception:
            pass
        self.tmp.unlink(missing_ok=True)


def write_records(path: Path, rows: list[dict], schema: pa.Schema) -> Path:
    w = BatchWriter(path, schema)
    try:
        w.write(rows)
    finally:
        w.close()
    return path


def write_csv_copy(parquet_path: Path, csv_path: Path) -> Path:
    import pyarrow.csv as pacsv

    table = pq.read_table(parquet_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = csv_path.with_suffix(csv_path.suffix + ".tmp")
    pacsv.write_csv(table, tmp)
    os.replace(tmp, csv_path)
    return csv_path
