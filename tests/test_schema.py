from pathlib import Path

from bls_corpus.schema import infer_schema, required_missing

FIX = Path(__file__).parent / "fixtures" / "bd"


def test_infer_bd_fixture_layout():
    schema = infer_schema(FIX, "BD")
    assert schema.catalog_path == "bd.series"
    assert "series_id" in schema.catalog_columns
    assert schema.title_column == "series_title"
    assert schema.seasonal_column == "seasonal"
    assert {d["path"] for d in schema.data_files} == {"bd.data.0.Current", "bd.data.1.AllItems"}
    assert schema.data_files[0]["path"] == "bd.data.1.AllItems"  # AllItems first
    assert schema.footnote_path == "bd.footnote"
    by_table = {m.table: m for m in schema.mappings}
    assert "industry" in by_table
    assert "industry_code" in by_table["industry"].links_to
    assert "state_code" in by_table["state"].links_to
    assert required_missing(schema) == []


def test_non_bd_layout_from_headers(tmp_path):
    (tmp_path / "ws.series").write_text(
        "series_id\tmeasure_code\tseasonal\tseries_title\tbegin_year\tbegin_period\tend_year\tend_period\n"
        "WSU001\t001\tU\tDays idle\t1947\tM13\t2026\tM09\n")
    (tmp_path / "ws.data.1.AllData").write_text(
        "series_id\tyear\tperiod\tvalue\tfootnote_codes\nWSU001\t1947\tM13\t25720\t\n")
    (tmp_path / "ws.measure").write_text("measure_code\tmeasure_text\n001\tDays of idleness\n")
    (tmp_path / "ws.footnote").write_text("footnote_code\tfootnote_text\n1\tNot available\n")
    (tmp_path / "ws.txt").write_text("Work Stoppages\n")
    schema = infer_schema(tmp_path, "WS")
    assert schema.title_column == "series_title"
    assert schema.data_files[0]["path"] == "ws.data.1.AllData"
    assert any(m.table == "measure" and m.links_to == ["measure_code"] for m in schema.mappings)
    assert schema.documentation == ["ws.txt"]
    assert required_missing(schema) == []


def test_label_is_not_always_the_second_column(tmp_path):
    (tmp_path / "ce.series").write_text(
        "series_id\tindustry_code\tdata_type_code\tseries_title\n"
        "CES0000000001\t00000000\t01\tAll employees\n")
    (tmp_path / "ce.data.0.Current").write_text(
        "series_id\tyear\tperiod\tvalue\tfootnote_codes\n"
        "CES0000000001\t1939\tM01\t29923\t\n")
    (tmp_path / "ce.industry").write_text(
        "industry_code\tnaics_code\tpublishing_status\tindustry_name\tdisplay_level\n"
        "00000000\t00\tB\tTotal nonfarm\t0\n")
    (tmp_path / "ce.datatype").write_text(
        "data_type_code\tdata_type_text\n01\tALL EMPLOYEES, THOUSANDS\n")
    schema = infer_schema(tmp_path, "CE")
    by_table = {m.table: m for m in schema.mappings}
    assert by_table["industry"].label_col == "industry_name"
    assert by_table["industry"].links_to == ["industry_code"]
    assert by_table["datatype"].label_col == "data_type_text"
    assert by_table["datatype"].links_to == ["data_type_code"]


def test_crosswalk_is_not_joined_on_its_first_column(tmp_path):
    (tmp_path / "fa.series").write_text(
        "series_id\tcase_code\tcategory_code\nFAU00X00000080N00\t0\t00X\n")
    (tmp_path / "fa.data.0.Current").write_text(
        "series_id\tyear\tperiod\tvalue\tfootnote_codes\n"
        "FAU00X00000080N00\t2023\tA01\t5283\t\n")
    (tmp_path / "fa.category").write_text(
        "case_code\tcategory_code\tcategory_text\n0\t00X\tTotal fatalities\n")
    (tmp_path / "fa.category2").write_text(
        "category_code\tcategory_text\n00X\tTotal fatalities\n")
    (tmp_path / "fa.aspect").write_text(
        "series_id\tyear\tperiod\taspect_type\tvalue\nFAU00X00000080N00\t2023\tA01\tR\t1\n")
    schema = infer_schema(tmp_path, "FA")
    by_table = {m.table: m for m in schema.mappings}
    assert by_table["category"].links_to == []
    assert by_table["category2"].links_to == ["category_code"]
    assert by_table["category2"].label_col == "category_text"
    assert schema.auxiliary == ["fa.aspect"]
