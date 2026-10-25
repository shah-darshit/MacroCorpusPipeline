import json
from pathlib import Path

import pyarrow.parquet as pq

from bls_corpus.engine import process_snapshot

FIX = Path(__file__).parent / "fixtures" / "bd"


def test_engine_on_bd_fixtures(tmp_path):
    stats, schema = process_snapshot(FIX, tmp_path, "snap-bd", "BD")
    assert schema.catalog_path == "bd.series"
    assert stats.series == 40
    assert stats.observations > 0
    assert stats.malformed_rows == 0
    catalog = pq.read_table(tmp_path / "series_catalog.parquet").to_pylist()
    assert catalog[0]["series_id"] == "BDS0000000000000000110001LQ5"
    assert catalog[0]["seasonal_code"] == "S"
    card = json.loads((tmp_path / "dataset.json").read_text())
    assert card["program_id"] == "BD"
    assert "industry" in card["dictionaries"]
    assert card["footnotes"]["1"]
    lines = (tmp_path / "series.jsonl").read_text().splitlines()
    assert len(lines) == 40
    first = json.loads(lines[0])
    assert first["series_id"].startswith("BD")
    assert "observations" in first
    assert first["dimensions"]["industry"]["code"] == "000000"


def test_engine_on_other_program_layout(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "ap.series").write_text(
        "series_id\tarea_code\titem_code\tseries_title\tbegin_year\tbegin_period\tend_year\tend_period\n"
        "APU0000701111\t0000\t701111\tFlour, per lb\t1980\tM01\t2026\tM09\n")
    (raw / "ap.data.0.Current").write_text(
        "series_id\tyear\tperiod\tvalue\tfootnote_codes\n"
        "APU0000701111\t1995\tM01\t0.238\t\n"
        "APU0000701111\t1995\tM02\t-\t\n")
    (raw / "ap.area").write_text("area_code\tarea_name\n0000\tU.S. city average\n")
    (raw / "ap.item").write_text("item_code\titem_name\n701111\tFlour, white\n")
    out = tmp_path / "out"
    stats, schema = process_snapshot(raw, out, "snap-ap", "AP")
    assert stats.series == 1
    assert stats.observations == 2
    assert stats.missing_values == 1
    card = json.loads((out / "dataset.json").read_text())
    assert card["dictionaries"]["area"]["0000"] == "U.S. city average"
    row = json.loads((out / "series.jsonl").read_text().splitlines()[0])
    assert row["title"] == "Flour, per lb"
    assert row["dimensions"]["item"]["label"] == "Flour, white"
    assert row["observations"][0]["period"] == "M01"
    assert row["observations"][0]["start"] == "1995-01-01"
    assert row["observations"][1]["value"] is None
