import json
from pathlib import Path

from bls_corpus.pipeline import process_catalog

FIX = Path(__file__).parent / "fixtures" / "bd"


def test_engine_on_bd_fixtures(tmp_path):
    stats, schema = process_catalog(FIX, tmp_path, "BD")
    assert schema.catalog_path == "bd.series"
    assert stats.series == 40
    assert stats.observations > 0
    assert stats.malformed_rows == 0
    first_id = "BDS0000000000000000110001LQ5"
    row = json.loads((tmp_path / "series" / f"{first_id}.json").read_text())
    assert row["series_id"] == first_id
    assert row["seasonal"] == "S"
    assert "observations" in row
    assert row["observations"]
    card = json.loads((tmp_path / "catalog.json").read_text())
    assert card["program"] == "BD"
    assert any(j["catalog_column"] == "industry_code" for j in card["joins"])
    index = json.loads((tmp_path / "index.json").read_text())
    assert first_id in index
    assert index[first_id]["observation_count"] > 0
    assert "snapshot_id" not in row


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
    stats, schema = process_catalog(raw, out, "AP")
    assert stats.series == 1
    assert stats.observations == 2
    assert stats.missing_values == 1
    row = json.loads((out / "series" / "APU0000701111.json").read_text())
    assert row["series_title"] == "Flour, per lb"
    assert row["area_name"] == "U.S. city average"
    assert row["item_name"] == "Flour, white"
    assert row["observations"][0]["period"] == "M01"
    assert row["observations"][0]["value"] == "0.238"
    assert row["observations"][1]["value"] == "-"
    assert "snapshot_id" not in row


def test_cu_style_labels_match_sample_shape(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "cu.series").write_text(
        "series_id\tarea_code\titem_code\tseasonal\tperiodicity_code\tbase_code\t"
        "base_period\tseries_title\tfootnote_codes\tbegin_year\tbegin_period\tend_year\tend_period\n"
        "CUSR0000SA0\t0000\tSA0\tS\tR\tS\t1982-84=100\t"
        "All items in U.S. city average, all urban consumers, seasonally adjusted\t\t"
        "1947\tM01\t2026\tM09\n")
    (raw / "cu.data.0.Current").write_text(
        "series_id\tyear\tperiod\tvalue\tfootnote_codes\n"
        "CUSR0000SA0\t1997\tM01\t159.40\t\n")
    (raw / "cu.area").write_text("area_code\tarea_name\n0000\tU.S. city average\n")
    (raw / "cu.item").write_text("item_code\titem_name\nSA0\tAll items\n")
    (raw / "cu.base").write_text("base_code\tbase_name\nS\tCurrent\n")
    out = tmp_path / "out"
    process_catalog(raw, out, "CU")
    row = json.loads((out / "series" / "CUSR0000SA0.json").read_text())
    assert row["area_name"] == "U.S. city average"
    assert row["item_name"] == "All items"
    assert row["base_period"] == "1982-84=100"
    assert row["series_title"].startswith("All items in U.S. city average")


def test_ws_series_file(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "ws.series").write_text(
        "series_id\tmeasure_code\tseasonal\tseries_title\tbegin_year\tbegin_period\tend_year\tend_period\n"
        "WSU001\t001\tU\tDays of idleness\t1947\tM13\t2026\tM09\n")
    (raw / "ws.data.1.AllData").write_text(
        "series_id\tyear\tperiod\tvalue\tfootnote_codes\n"
        "WSU001\t1947\tM13\t25720\t\n")
    (raw / "ws.measure").write_text("measure_code\tmeasure_text\n001\tDays of idleness\n")
    (raw / "ws.seasonal").write_text("seasonal_code\tseasonal_text\nU\tNot Seasonally Adjusted\n")
    out = tmp_path / "out"
    process_catalog(raw, out, "WS")
    row = json.loads((out / "series" / "WSU001.json").read_text())
    assert row["measure_text"] == "Days of idleness"
    assert row["observations"][0]["value"] == "25720"


def test_pc_product_names_use_industry_and_product(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "pc.series").write_text(
        "series_id\tindustry_code\tproduct_code\n"
        "PCU001A\t001\t01\n"
        "PCU002A\t002\t01\n"
        "PCU003A\t003\t01\n")
    (raw / "pc.product").write_text(
        "industry_code\tproduct_code\tproduct_name\n"
        "001\t01\tFirst industry product\n"
        "002\t01\tSecond industry product\n")
    (raw / "pc.data.0.Current").write_text(
        "series_id\tyear\tperiod\tvalue\tfootnote_codes\n"
        "PCU001A\t2026\tM01\t100\t\n"
        "PCU002A\t2026\tM01\t200\t\n"
        "PCU003A\t2026\tM01\t300\t\n")
    out = tmp_path / "out"
    process_catalog(raw, out, "PC")
    rows = [json.loads((out / "series" / f"PCU00{i}A.json").read_text())
            for i in (1, 2, 3)]
    assert [r["product_name"] for r in rows] == [
        "First industry product", "Second industry product", None]
    assert all(r["product_code"] == "01" for r in rows)
    assert [r["observations"][0]["value"] for r in rows] == ["100", "200", "300"]
    card = json.loads((out / "catalog.json").read_text())
    join = next(j for j in card["joins"] if j["file"] == "pc.product")
    assert join["context_column"] == "industry_code"
    assert join["matched"] == 2
    assert join["unmatched"] == 1
