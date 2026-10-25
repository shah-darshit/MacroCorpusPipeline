from bls_corpus.normalize import (frequency_from_periodicity, parse_numeric,
                                  period_dates, seasonal_status, split_footnotes)


def test_parse_numeric_preserves_status():
    assert parse_numeric("        9004") == (9004.0, "numeric")
    assert parse_numeric("6.5") == (6.5, "numeric")
    assert parse_numeric("")[1] == "missing_token"
    assert parse_numeric("-")[1] == "missing_token"
    assert parse_numeric("(NA)")[1] == "missing_token"
    assert parse_numeric("suppressed")[1] == "non_numeric"


def test_period_dates_quarterly_monthly_annual_average():
    assert period_dates(2020, "Q02") == ("2020-04-01", "2020-06-30", "quarterly")
    assert period_dates(2020, "Q04") == ("2020-10-01", "2020-12-31", "quarterly")
    assert period_dates(2020, "M13") == ("2020-01-01", "2020-12-31", "annual_average")
    assert period_dates(2020, "Q05") == ("2020-01-01", "2020-12-31", "annual_average")
    assert period_dates(2021, "M02") == ("2021-02-01", "2021-02-28", "monthly")


def test_unknown_period_not_invented():
    assert period_dates(2020, "X99") == (None, None, "unknown_period")


def test_frequency_and_seasonal_not_title_inferred():
    assert frequency_from_periodicity("Q") == ("quarterly", "catalog_periodicity_code")
    assert frequency_from_periodicity("")[0] is None
    assert seasonal_status("S")[0] == "Seasonally adjusted"
    assert seasonal_status("U")[0] == "Not seasonally adjusted"
    assert seasonal_status("Z")[0] is None


def test_split_multiple_footnotes():
    assert split_footnotes("1,2") == ["1", "2"]
    assert split_footnotes("") == []
