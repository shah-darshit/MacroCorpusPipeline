"""Pure normalization helpers. No fabricated metadata: unsupported
interpretations yield ``None`` plus an explicit method/status string.
"""
from __future__ import annotations

import re
from datetime import date

_NUM_RE = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$")
MISSING_TOKENS = {"", "-", "--", ".", "NA", "N/A", "(NA)", "null"}


def parse_numeric(raw: str | None) -> tuple[float | None, str]:
    """Return (value, status). Raw string is always preserved by callers."""
    if raw is None:
        return None, "missing_empty"
    token = raw.strip()
    if token in MISSING_TOKENS:
        return None, "missing_token"
    cleaned = token.replace(",", "")
    if not _NUM_RE.match(cleaned):
        return None, "non_numeric"
    try:
        return float(cleaned), "numeric"
    except ValueError:
        return None, "non_numeric"


def period_dates(year: int, period: str) -> tuple[str | None, str | None, str]:
    """Map a BLS period code to (period_start, period_end, kind).

    Q05/M13/S03-style codes are annual averages: the year bounds are
    defensible, an exact day is not invented beyond Jan 1 / Dec 31 of that
    year, and kind flags the value as an average, not a point-in-time obs.
    Unknown codes -> (None, None, 'unknown_period') and are reported upstream.
    """
    p = (period or "").strip().upper()
    try:
        if re.fullmatch(r"M(0[1-9]|1[0-2])", p):
            m = int(p[1:])
            start = date(year, m, 1)
            end = date(year + (m == 12), (m % 12) + 1, 1).toordinal() - 1
            return start.isoformat(), date.fromordinal(end).isoformat(), "monthly"
        if p == "M13":
            return f"{year}-01-01", f"{year}-12-31", "annual_average"
        if re.fullmatch(r"Q0[1-4]", p):
            q = int(p[2])
            sm = 3 * (q - 1) + 1
            start = date(year, sm, 1)
            em = sm + 2
            next_month = date(year + (em == 12), (em % 12) + 1, 1)
            end_ord = next_month.toordinal() - 1
            return start.isoformat(), date.fromordinal(end_ord).isoformat(), "quarterly"
        if p == "Q05":
            return f"{year}-01-01", f"{year}-12-31", "annual_average"
        if re.fullmatch(r"S0[12]", p):
            half = int(p[2])
            sm, em = (1, 6) if half == 1 else (7, 12)
            return f"{year}-{sm:02d}-01", f"{year}-{em:02d}-{'30' if em == 6 else '31'}", "semiannual"
        if p == "S03":
            return f"{year}-01-01", f"{year}-12-31", "annual_average"
        if p in ("A01", "ANN", "A"):
            return f"{year}-01-01", f"{year}-12-31", "annual"
    except ValueError:
        return None, None, "invalid_date"
    return None, None, "unknown_period"


_PERIODICITY_FREQ = {"Q": "quarterly", "M": "monthly", "A": "annual",
                     "SA": "semiannual", "W": "weekly"}


def frequency_from_periodicity(periodicity_code: str | None) -> tuple[str | None, str]:
    if periodicity_code is None or not str(periodicity_code).strip():
        return None, "unknown_no_periodicity_code"
    freq = _PERIODICITY_FREQ.get(str(periodicity_code).strip().upper())
    if freq:
        return freq, "catalog_periodicity_code"
    return None, f"unknown_periodicity_code:{periodicity_code}"


def seasonal_status(seasonal_code: str | None,
                    mapping: dict[str, str] | None = None) -> tuple[str | None, str]:
    if seasonal_code is None or not str(seasonal_code).strip():
        return None, "missing"
    code = str(seasonal_code).strip()
    if mapping and code in mapping:
        return mapping[code], "mapping_file"
    documented = {"S": "Seasonally adjusted", "U": "Not seasonally adjusted"}
    if code in documented:
        return documented[code], "documented_code_convention_bd_txt"
    return None, f"unknown_code:{code}"


def split_footnotes(raw: str | None) -> list[str]:
    if raw is None:
        return []
    return [t for t in re.split(r"[,\s]+", str(raw).strip()) if t]
