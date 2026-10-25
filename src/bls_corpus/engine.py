"""Generic LABSTAT processor.

Reads raw TSV files with DuckDB (one pass, no Python row loops), writes
Parquet plus a model-ready JSON pair: dataset.json and series.jsonl.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from .schema import ProgramSchema, infer_schema, required_missing


@dataclass
class ProcessResult:
    snapshot_id: str
    program: str
    series: int = 0
    observations: int = 0
    rows_in: int = 0
    duplicate_identical: int = 0
    duplicate_conflicts: int = 0
    malformed_rows: int = 0
    missing_values: int = 0
    non_numeric_values: int = 0
    unknown_periods: int = 0
    elapsed_s: float = 0.0
    extras: dict = field(default_factory=dict)

    def parse_stats(self) -> dict:
        return {
            "rows_in": self.rows_in, "rows_written": self.observations,
            "malformed_rows": self.malformed_rows, "malformed_samples": [],
            "non_numeric_values": self.non_numeric_values,
            "missing_values": self.missing_values,
            "unknown_periods": self.unknown_periods,
            "duplicate_identical": self.duplicate_identical,
            "duplicate_conflicts": self.duplicate_conflicts,
            "duplicate_conflict_samples": [], "unknown_period_samples": [],
        }


def _ident(name: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)
    if not cleaned or cleaned[0].isdigit():
        cleaned = "c_" + cleaned
    return cleaned


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _lines_sql(path: Path, encoding: str) -> str:
    return (
        f"read_csv({_sql_str(str(path))}, columns={{'line':'VARCHAR'}}, "
        f"delim='\\x1f', quote='', escape='', header=false, auto_detect=false, "
        f"encoding={_sql_str(encoding)}, store_rejects=true, ignore_errors=true) "
        f"WITH ORDINALITY AS t(line, rec)"
    )


def _split_sql(path: Path, encoding: str, skip_header: bool = True) -> str:
    where = "rec > 1 AND " if skip_header else ""
    return (
        f"SELECT rec, string_split(line, chr(9)) AS f FROM {_lines_sql(path, encoding)} "
        f"WHERE {where}line IS NOT NULL AND trim(line) <> ''"
    )


class Engine:
    def __init__(self, raw_dir: Path, out_dir: Path, snapshot_id: str, program: str):
        self.raw_dir = Path(raw_dir)
        self.out_dir = Path(out_dir)
        self.snapshot_id = snapshot_id
        self.program = program.upper()
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.out_dir / "work.duckdb"
        if self.db_path.exists():
            self.db_path.unlink()
        self.con = duckdb.connect(str(self.db_path))
        self.con.execute("SET preserve_insertion_order=true")
        spill = self.out_dir / "spill"
        spill.mkdir(exist_ok=True)
        self.con.execute(f"SET temp_directory={_sql_str(str(spill))}")
        self.con.execute("""
            CREATE MACRO period_start(y, p) AS (
              CASE
                WHEN p IS NULL OR y IS NULL THEN NULL
                WHEN regexp_matches(p, '^M(0[1-9]|1[0-2])$')
                  THEN make_date(y, CAST(substr(p, 2, 2) AS INTEGER), 1)
                WHEN p IN ('M13', 'Q05', 'S03', 'A01', 'ANN', 'A') THEN make_date(y, 1, 1)
                WHEN regexp_matches(p, '^Q0[1-4]$')
                  THEN make_date(y, (CAST(substr(p, 3, 1) AS INTEGER) - 1) * 3 + 1, 1)
                WHEN p = 'S01' THEN make_date(y, 1, 1)
                WHEN p = 'S02' THEN make_date(y, 7, 1)
                ELSE NULL
              END
            );
            CREATE MACRO period_finish(y, p) AS (
              CASE
                WHEN p IS NULL OR y IS NULL THEN NULL
                WHEN regexp_matches(p, '^M(0[1-9]|1[0-2])$')
                  THEN last_day(make_date(y, CAST(substr(p, 2, 2) AS INTEGER), 1))
                WHEN p IN ('M13', 'Q05', 'S03', 'A01', 'ANN', 'A') THEN make_date(y, 12, 31)
                WHEN regexp_matches(p, '^Q0[1-4]$')
                  THEN last_day(make_date(y, CAST(substr(p, 3, 1) AS INTEGER) * 3, 1))
                WHEN p = 'S01' THEN make_date(y, 6, 30)
                WHEN p = 'S02' THEN make_date(y, 12, 31)
                ELSE NULL
              END
            );
            CREATE MACRO period_kind(p) AS (
              CASE
                WHEN p IS NULL THEN 'unknown_period'
                WHEN regexp_matches(p, '^M(0[1-9]|1[0-2])$') THEN 'monthly'
                WHEN p IN ('M13', 'Q05', 'S03') THEN 'annual_average'
                WHEN regexp_matches(p, '^Q0[1-4]$') THEN 'quarterly'
                WHEN p IN ('S01', 'S02') THEN 'semiannual'
                WHEN p IN ('A01', 'ANN', 'A') THEN 'annual'
                ELSE 'unknown_period'
              END
            );
        """)

    def close(self) -> None:
        self.con.close()

    def process(self, schema: ProgramSchema | None = None) -> ProcessResult:
        import time

        started = time.perf_counter()
        schema = schema or infer_schema(self.raw_dir, self.program)
        missing = required_missing(schema)
        if missing:
            raise RuntimeError(f"{self.program}: missing required pieces: {', '.join(missing)}")
        self._load_catalog(schema)
        self._load_mappings(schema)
        self._load_footnotes(schema)
        self._load_observations(schema)
        self._enrich_catalog(schema)
        stats = self._stats()
        self._write_parquet()
        stats.elapsed_s = time.perf_counter() - started
        (self.out_dir / "schema.json").write_text(
            json.dumps(schema.to_dict(), indent=2), encoding="utf-8")
        (self.out_dir / "parse_stats.json").write_text(
            json.dumps(stats.parse_stats(), indent=2), encoding="utf-8")
        return stats

    def export_json(self, schema: ProgramSchema) -> tuple[Path, Path]:
        card_path = self.out_dir / "dataset.json"
        series_path = self.out_dir / "series.jsonl"
        card = self._dataset_card(schema)
        tmp = card_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(card, indent=2, default=str), encoding="utf-8")
        os.replace(tmp, card_path)
        self._write_series_jsonl(schema, series_path)
        return card_path, series_path

    # -- loaders -----------------------------------------------------------
    def _load_catalog(self, schema: ProgramSchema) -> None:
        path = self.raw_dir / schema.catalog_path
        n = len(schema.catalog_columns)
        selects = ", ".join(
            f"trim(f[{i}]) AS {_ident(col)}"
            for i, col in enumerate(schema.catalog_columns, start=1)
        )
        self.con.execute(f"""
            CREATE TABLE catalog_raw AS
            SELECT rec, {selects}, len(f) AS n_fields,
                   len(list_filter(f[{n}+1:], lambda x: trim(x) <> '')) AS extra
            FROM ({_split_sql(path, schema.encoding)})
        """)

    def _load_mappings(self, schema: ProgramSchema) -> None:
        self.con.execute(
            "CREATE TABLE mappings (table_name VARCHAR, code VARCHAR, label VARCHAR, source_file VARCHAR)")
        for m in schema.mappings:
            path = self.raw_dir / m.path
            code_i = m.columns.index(m.code_col) + 1
            label_expr = (f"trim(f[{m.columns.index(m.label_col)+1}])"
                          if m.label_col in m.columns else "NULL")
            self.con.execute(f"""
                INSERT INTO mappings
                SELECT {_sql_str(m.table)}, trim(f[{code_i}]), {label_expr}, {_sql_str(m.path)}
                FROM ({_split_sql(path, schema.encoding)})
                WHERE len(f) >= 1
            """)

    def _load_footnotes(self, schema: ProgramSchema) -> None:
        self.con.execute(
            "CREATE TABLE footnotes (footnote_code VARCHAR, footnote_text VARCHAR, source_file VARCHAR)")
        if not schema.footnote_path:
            return
        path = self.raw_dir / schema.footnote_path
        from .schema import read_header
        header, _ = read_header(path)
        code_i = 1
        text_i = 2 if len(header) > 1 else 1
        self.con.execute(f"""
            INSERT INTO footnotes
            SELECT trim(f[{code_i}]), trim(f[{text_i}]), {_sql_str(schema.footnote_path)}
            FROM ({_split_sql(path, schema.encoding)})
        """)

    def _load_observations(self, schema: ProgramSchema) -> None:
        cols = {c.lower(): i + 1 for i, c in enumerate(schema.data_columns)}
        sid, year, period = cols["series_id"], cols["year"], cols["period"]
        value, n = cols["value"], len(schema.data_columns)
        fn = cols.get("footnote_codes")
        fn_expr = f"nullif(trim(f[{fn}]), '')" if fn else "NULL"
        parts = []
        for item in schema.data_files:
            path = self.raw_dir / item["path"]
            parts.append(f"""
                SELECT trim(f[{sid}]) AS series_id,
                       TRY_CAST(trim(f[{year}]) AS INTEGER) AS year,
                       trim(f[{period}]) AS period,
                       trim(f[{value}]) AS value_raw,
                       {fn_expr} AS footnote_codes,
                       {int(item['rank'])} AS file_rank,
                       {_sql_str(item['path'])} AS source_file,
                       rec AS source_row,
                       len(f) AS n_fields,
                       len(list_filter(f[{n}+1:], lambda x: trim(x) <> '')) AS extra
                FROM ({_split_sql(path, schema.encoding)})
            """)
        self.con.execute("CREATE TABLE obs_raw AS " + " UNION ALL ".join(parts))
        self.con.execute("""
            CREATE TABLE observations AS
            SELECT series_id, year, period,
                   CAST(period_start(year, period) AS VARCHAR) AS period_start,
                   CAST(period_finish(year, period) AS VARCHAR) AS period_end,
                   period_kind(period) AS period_kind,
                   value_raw,
                   TRY_CAST(replace(value_raw, ',', '') AS DOUBLE) AS value_numeric,
                   CASE
                     WHEN value_raw IS NULL OR trim(value_raw) IN ('', '-', '--', '.', 'NA', 'N/A', '(NA)', 'null')
                       THEN 'missing_token'
                     WHEN TRY_CAST(replace(value_raw, ',', '') AS DOUBLE) IS NULL THEN 'non_numeric'
                     ELSE 'numeric'
                   END AS value_status,
                   footnote_codes, source_file, source_row
            FROM obs_raw
            WHERE year IS NOT NULL AND series_id <> '' AND period <> ''
              AND n_fields >= 4 AND extra = 0
            QUALIFY row_number() OVER (
                PARTITION BY series_id, year, period
                ORDER BY file_rank, source_file, source_row
            ) = 1
        """)

    def _enrich_catalog(self, schema: ProgramSchema) -> None:
        sid = _ident("series_id")
        title = _ident(schema.title_column) if schema.title_column else "NULL"
        title_sql = title if schema.title_column else "NULL"
        seasonal = _ident(schema.seasonal_column) if schema.seasonal_column else None
        periodicity = _ident(schema.periodicity_column) if schema.periodicity_column else None
        seasonal_sql = f"nullif(c.{seasonal}, '')" if seasonal else "NULL"
        periodicity_sql = f"nullif(c.{periodicity}, '')" if periodicity else "NULL"
        skip = {"series_id", schema.title_column, "begin_year", "begin_period",
                "end_year", "end_period", "footnote_codes"}
        meta_cols = [c for c in schema.catalog_columns if c not in skip]
        if meta_cols:
            pairs = ", ".join(f"{_sql_str(c)}, c.{_ident(c)}" for c in meta_cols)
            meta_sql = f"json_object({pairs})"
        else:
            meta_sql = "'{}'"
        self.con.execute(f"""
            CREATE TABLE series_stats AS
            SELECT series_id,
                   min(year || '-' || period) AS first_period,
                   max(year || '-' || period) AS last_period,
                   count(*)::BIGINT AS observation_count
            FROM observations GROUP BY 1
        """)
        self.con.execute(f"""
            CREATE TABLE catalog AS
            SELECT {_sql_str(self.snapshot_id)} AS snapshot_id,
                   {_sql_str(self.program)} AS program_id,
                   c.{sid} AS series_id,
                   c.{sid} AS series_id_raw,
                   {title_sql if schema.title_column else 'NULL'} AS series_title,
                   {seasonal_sql} AS seasonal_code,
                   {periodicity_sql} AS periodicity_code,
                   s.first_period, s.last_period,
                   coalesce(s.observation_count, 0) AS observation_count,
                   {_sql_str(schema.catalog_path)} AS source_catalog_file,
                   {meta_sql} AS metadata_json
            FROM catalog_raw c
            LEFT JOIN series_stats s ON s.series_id = c.{sid}
            WHERE c.{sid} IS NOT NULL AND c.{sid} <> ''
              AND c.n_fields >= {len(schema.catalog_columns)} AND c.extra = 0
        """)

    def _stats(self) -> ProcessResult:
        rows_in = self.con.execute("SELECT count(*) FROM obs_raw").fetchone()[0]
        kept = self.con.execute("SELECT count(*) FROM observations").fetchone()[0]
        malformed = self.con.execute(
            "SELECT count(*) FROM obs_raw WHERE year IS NULL OR series_id = '' OR period = '' "
            "OR n_fields < 4 OR extra > 0").fetchone()[0]
        keys = self.con.execute("""
            SELECT count(*), sum(n) - count(*),
                   count(*) FILTER (WHERE nd > 1)
            FROM (
              SELECT series_id, year, period, count(*) n,
                     count(DISTINCT coalesce(value_raw,'') || '|' || coalesce(footnote_codes,'')) nd
              FROM obs_raw
              WHERE year IS NOT NULL AND series_id <> '' AND period <> ''
                AND n_fields >= 4 AND extra = 0
              GROUP BY 1,2,3
            )
        """).fetchone()
        missing, nonnum, unknown = self.con.execute("""
            SELECT count(*) FILTER (WHERE value_status = 'missing_token'),
                   count(*) FILTER (WHERE value_status = 'non_numeric'),
                   count(*) FILTER (WHERE period_kind IN ('unknown_period', 'invalid_date'))
            FROM observations
        """).fetchone()
        series = self.con.execute("SELECT count(*) FROM catalog").fetchone()[0]
        return ProcessResult(
            snapshot_id=self.snapshot_id, program=self.program,
            series=series, observations=kept, rows_in=rows_in,
            duplicate_identical=int(keys[1] or 0),
            duplicate_conflicts=int(keys[2] or 0),
            malformed_rows=malformed, missing_values=missing,
            non_numeric_values=nonnum, unknown_periods=unknown,
        )

    def _write_parquet(self) -> None:
        sid = self.snapshot_id
        prog = self.program
        seasonal_join = """
            LEFT JOIN mappings sm
              ON sm.table_name IN ('seasonal') AND sm.code = c.seasonal_code
        """
        self.con.execute(f"""
            COPY (
              SELECT snapshot_id, program_id, series_id, series_id_raw, series_title,
                     seasonal_code,
                     coalesce(sm.label,
                              CASE seasonal_code
                                WHEN 'S' THEN 'Seasonally adjusted'
                                WHEN 'U' THEN 'Not seasonally adjusted'
                              END) AS seasonal_status,
                     CASE
                       WHEN sm.label IS NOT NULL THEN 'mapping_file'
                       WHEN seasonal_code IN ('S','U') THEN 'documented_code_convention'
                       WHEN seasonal_code IS NULL THEN 'missing'
                       ELSE 'unknown_code'
                     END AS seasonal_status_method,
                     CAST(NULL AS VARCHAR) AS measure,
                     CAST(NULL AS VARCHAR) AS unit,
                     CASE lower(coalesce(periodicity_code,''))
                       WHEN 'q' THEN 'quarterly' WHEN 'm' THEN 'monthly'
                       WHEN 'a' THEN 'annual' WHEN 'r' THEN 'monthly'
                       WHEN 'sa' THEN 'semiannual' WHEN 'w' THEN 'weekly'
                     END AS frequency,
                     CASE WHEN periodicity_code IS NULL OR periodicity_code = ''
                          THEN 'unknown_no_periodicity_code'
                          ELSE 'catalog_periodicity_code' END AS frequency_method,
                     first_period, last_period, observation_count,
                     source_catalog_file, metadata_json
              FROM catalog c
              {seasonal_join}
            ) TO {_sql_str(str(self.out_dir / "series_catalog.parquet"))} (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        self.con.execute(f"""
            COPY (
              SELECT {_sql_str(sid)} AS snapshot_id, {_sql_str(prog)} AS program_id,
                     series_id, year, period, period_start, period_end, period_kind,
                     value_raw, value_numeric, value_status, footnote_codes,
                     source_file, source_row
              FROM observations
            ) TO {_sql_str(str(self.out_dir / "observations.parquet"))} (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        self.con.execute(f"""
            COPY (
              SELECT {_sql_str(sid)} AS snapshot_id, {_sql_str(prog)} AS program_id,
                     footnote_code, footnote_text, source_file FROM footnotes
            ) TO {_sql_str(str(self.out_dir / "footnotes.parquet"))} (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        self.con.execute(f"""
            COPY (
              SELECT {_sql_str(sid)} AS snapshot_id, {_sql_str(prog)} AS program_id,
                     table_name, code, label, CAST(NULL AS VARCHAR) AS extra_json,
                     source_file FROM mappings
            ) TO {_sql_str(str(self.out_dir / "reference_metadata.parquet"))} (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)

    def _dataset_card(self, schema: ProgramSchema) -> dict:
        stats = json.loads((self.out_dir / "parse_stats.json").read_text())
        dicts = {}
        for m in schema.mappings:
            rows = self.con.execute(
                "SELECT code, label FROM mappings WHERE table_name = ?",
                [m.table]).fetchall()
            dicts[m.table] = {code: label for code, label in rows}
        footnotes = {c: t for c, t in self.con.execute(
            "SELECT footnote_code, footnote_text FROM footnotes").fetchall()}
        coverage = self.con.execute("""
            SELECT (SELECT count(*) FROM catalog),
                   (SELECT count(*) FROM observations),
                   (SELECT count(*) FROM catalog WHERE observation_count = 0),
                   (SELECT min(year) FROM observations),
                   (SELECT max(year) FROM observations)
        """).fetchone()
        return {
            "snapshot_id": self.snapshot_id,
            "program_id": self.program,
            "schema": schema.to_dict(),
            "dictionaries": dicts,
            "footnotes": footnotes,
            "coverage": {
                "series": coverage[0], "observations": coverage[1],
                "catalog_without_observations": coverage[2],
                "year_min": coverage[3], "year_max": coverage[4],
            },
            "validation": stats,
            "outputs": {
                "dataset_json": "dataset.json",
                "series_jsonl": "series.jsonl",
                "series_catalog_parquet": "series_catalog.parquet",
                "observations_parquet": "observations.parquet",
            },
        }

    def _write_series_jsonl(self, schema: ProgramSchema, dest: Path) -> None:
        dim_links = [(m, col) for m in schema.mappings for col in m.links_to]
        dim_sql = "[]::JSON"
        if dim_links:
            parts = []
            for m, col in dim_links:
                alias = _ident(col)
                parts.append(
                    f"json_object({_sql_str(m.table)}, "
                    f"json_object('code', json_extract_string(c.metadata_json, {_sql_str('$.' + col)}), "
                    f"'label', (SELECT label FROM mappings WHERE table_name = {_sql_str(m.table)} "
                    f"AND code = json_extract_string(c.metadata_json, {_sql_str('$.' + col)}) LIMIT 1)))"
                )
            dim_sql = "json_merge_patch(" + ", ".join(parts) + ")" if len(parts) > 1 else parts[0]
            if len(parts) > 2:
                # json_merge_patch is pairwise; fold
                expr = parts[0]
                for p in parts[1:]:
                    expr = f"json_merge_patch({expr}, {p})"
                dim_sql = expr
        tmp = dest.with_suffix(".jsonl.tmp")
        self.con.execute(f"""
            COPY (
              SELECT
                c.snapshot_id,
                c.program_id,
                c.series_id,
                c.series_title AS title,
                c.seasonal_code,
                CASE lower(coalesce(c.periodicity_code,''))
                    WHEN 'q' THEN 'quarterly' WHEN 'm' THEN 'monthly'
                    WHEN 'a' THEN 'annual' WHEN 'r' THEN 'monthly'
                END AS frequency,
                c.first_period,
                c.last_period,
                c.observation_count,
                {dim_sql} AS dimensions,
                c.metadata_json::JSON AS metadata,
                (
                    SELECT to_json(list(json_object(
                        'year', o.year, 'period', o.period,
                        'start', o.period_start, 'end', o.period_end,
                        'value', o.value_numeric, 'value_raw', o.value_raw,
                        'footnotes', o.footnote_codes
                    ) ORDER BY o.year, o.period))
                    FROM observations o WHERE o.series_id = c.series_id
                ) AS observations
              FROM catalog c
            ) TO {_sql_str(str(tmp))} (FORMAT JSON)
        """)
        os.replace(tmp, dest)


def process_snapshot(raw_dir: Path, out_dir: Path, snapshot_id: str,
                     program: str) -> tuple[ProcessResult, ProgramSchema]:
    schema = infer_schema(raw_dir, program)
    engine = Engine(raw_dir, out_dir, snapshot_id, program)
    try:
        stats = engine.process(schema)
        engine.export_json(schema)
        return stats, schema
    finally:
        engine.close()
        engine.db_path.unlink(missing_ok=True)
        spill = Path(out_dir) / "spill"
        if spill.exists():
            for p in spill.iterdir():
                p.unlink()
            spill.rmdir()
