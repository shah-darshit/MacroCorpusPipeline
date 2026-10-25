from pathlib import Path

import pyarrow.parquet as pq

from bls_corpus import parser as pm

FIX = Path(__file__).parent / "fixtures" / "bd"


def read_obs(path: Path):
    return pq.read_table(path).to_pylist()


def test_real_fixtures_catalog_preserves_ids_titles_codes():
    rows = pm.parse_series_catalog(FIX / "bd.series", "snap1", "BD")
    assert len(rows) == 40
    first = rows[0]
    assert first["series_id"] == "BDS0000000000000000110001LQ5"
    assert first["series_id_raw"].endswith("  ")  # padding preserved in raw
    assert "gross job gains" in first["series_title"]
    assert first["seasonal_code"] == "S"
    assert first["frequency"] == "quarterly"
    assert first["unit"] is None  # never fabricated


def test_reference_and_footnotes_real_fixtures():
    fn = pm.parse_footnotes(FIX / "bd.footnote", "s", "BD")
    assert {r["footnote_code"] for r in fn} == {"1", "2"}
    ref = pm.parse_reference(FIX / "bd.state", "state", "s", "BD")
    by_code = {r["code"]: r["label"] for r in ref}
    assert by_code["06"] == "California" and by_code["00"] == "U.S. totals"  # leading zeros kept


def write_obs(tmp_path, name, lines):
    p = tmp_path / name
    p.write_text("series_id\t year\tperiod\tvalue\tfootnote_codes\n".replace(" ", "") + "\n".join(lines) + "\n")
    return p


def test_observations_dedupe_overlap_malformed_nonnumeric_unknown(tmp_path):
    a = write_obs(tmp_path, "bd.data.1.AllItems", [
        "SID0001  \t2020\tQ01\t        100\t1",
        "SID0001  \t2020\tQ02\t        6.5\t",
        "SID0001  \t2020\tQ05\t        200\t",       # annual average
        "SID0002  \t2020\tQ01\tsuppressed\t",          # non-numeric preserved
        "SID0002  \t2020\tX99\t        5\t",           # unknown period
        "SID0003\t2020\tQ01",                          # malformed: too few cols
    ])
    b = write_obs(tmp_path, "bd.data.0.Current", [
        "SID0001  \t2020\tQ01\t        100\t1",        # identical duplicate -> deduped
        "SID0001  \t2020\tQ02\t        9.9\t",         # conflict -> AllItems (6.5) wins
        "SID0001  \t2020\tQ03\t        300\t2",
    ])
    op = pm.ObservationParser("snap1", "BD", chunk_rows=2)  # force multi-chunk
    try:
        stats, per_series = op.parse_files([b, a], tmp_path / "obs.parquet")
    finally:
        op.close()
    rows = read_obs(tmp_path / "obs.parquet")
    assert stats.rows_written == 6
    assert stats.duplicate_identical == 1
    assert stats.duplicate_conflicts == 1
    assert stats.malformed_rows == 1
    assert stats.non_numeric_values == 1
    assert stats.unknown_periods == 1
    by_key = {(r["series_id"], r["year"], r["period"]): r for r in rows}
    assert by_key[("SID0001", 2020, "Q02")]["value_numeric"] == 6.5
    assert by_key[("SID0001", 2020, "Q05")]["period_kind"] == "annual_average"
    assert by_key[("SID0002", 2020, "Q01")]["value_raw"] == "suppressed"
    assert by_key[("SID0002", 2020, "Q01")]["value_numeric"] is None
    assert by_key[("SID0001", 2020, "Q01")]["footnote_codes"] == "1"
    assert per_series["SID0001"]["count"] == 4


def test_leading_zero_series_and_multi_chunk_round_trip(tmp_path):
    lines = [f"000123{i:02d}  \t2021\tQ0{(i % 4) + 1}\t{i}\t" for i in range(10)]
    a = write_obs(tmp_path, "bd.data.1.AllItems", lines)
    op = pm.ObservationParser("s", "BD", chunk_rows=3)
    try:
        stats, _ = op.parse_files([a], tmp_path / "o.parquet")
    finally:
        op.close()
    rows = read_obs(tmp_path / "o.parquet")
    # Same (series, period) reused across i groups -> duplicates must be reported, not dropped silently
    assert stats.rows_in == 10
    assert stats.duplicate_identical + stats.duplicate_conflicts == 10 - stats.rows_written
    assert all(r["series_id"].startswith("000123") for r in rows)
