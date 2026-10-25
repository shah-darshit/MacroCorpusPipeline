"""Infer a LABSTAT program layout from the files on disk.

No per-program adapters. Roles and column links come from filenames and
the first header row of each file.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from pathlib import Path


def sniff_encoding(path: Path, nbytes: int = 65536) -> str:
    sample = path.read_bytes()[:nbytes]
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def read_header(path: Path) -> tuple[list[str], str]:
    enc = sniff_encoding(path)
    text = path.read_bytes()[:8192].decode(enc, errors="replace")
    first = text.splitlines()[0] if text.strip() else ""
    cols = [c.strip() for c in first.split("\t") if c.strip() or True]
    # drop a trailing empty cell produced by a trailing tab
    while cols and cols[-1] == "":
        cols.pop()
    return [c.strip() for c in cols], enc


def _suffix(name: str, program: str) -> str:
    prefix = program.lower() + "."
    return name[len(prefix):] if name.lower().startswith(prefix) else name


def _data_rank(name: str) -> int:
    n = name.lower()
    if any(tag in n for tag in ("allitems", "alldata", "allcesseries")):
        return 0
    if "current" in n:
        return 2
    return 1


def _looks_like_observations(cols: list[str]) -> bool:
    lower = {c.lower() for c in cols}
    return {"series_id", "year", "period", "value"} <= lower


@dataclass
class MappingSpec:
    table: str
    path: str
    columns: list[str]
    code_col: str
    label_col: str | None
    links_to: list[str]


@dataclass
class ProgramSchema:
    program: str
    encoding: str
    catalog_path: str | None
    catalog_columns: list[str]
    title_column: str | None
    seasonal_column: str | None
    periodicity_column: str | None
    data_files: list[dict]  # {path, rank}
    data_columns: list[str]
    footnote_path: str | None
    mappings: list[MappingSpec] = field(default_factory=list)
    documentation: list[str] = field(default_factory=list)
    auxiliary: list[str] = field(default_factory=list)
    unclassified: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


def infer_schema(raw_dir: Path, program: str,
                 names: list[str] | None = None) -> ProgramSchema:
    raw_dir = Path(raw_dir)
    program = program.upper()
    files = sorted(names or [p.name for p in raw_dir.iterdir() if p.is_file()])
    encodings: list[str] = []
    catalog_path = None
    catalog_cols: list[str] = []
    data_files: list[dict] = []
    data_cols: list[str] = []
    footnote_path = None
    mapping_files: list[tuple[str, Path, list[str]]] = []
    documentation: list[str] = []
    auxiliary: list[str] = []
    unclassified: list[str] = []

    for name in files:
        path = raw_dir / name
        if not path.is_file() or name.endswith(".part"):
            continue
        low = name.lower()
        if low.endswith(".txt") or low.endswith(".contacts"):
            documentation.append(name)
            continue
        cols, enc = read_header(path)
        encodings.append(enc)
        if low.endswith(".series"):
            catalog_path, catalog_cols = name, cols
            continue
        if ".data." in low:
            data_files.append({"path": name, "rank": _data_rank(name)})
            if not data_cols:
                data_cols = cols
            continue
        if low.endswith(".footnote"):
            footnote_path = name
            continue
        if _looks_like_observations(cols):
            auxiliary.append(name)
            continue
        if len(cols) >= 2:
            mapping_files.append((_suffix(name, program), path, cols))
            continue
        unclassified.append(name)

    title_col = _first(catalog_cols, ("series_title", "series_name"))
    seasonal_col = _first(catalog_cols, ("seasonal", "seasonal_code"))
    periodicity_col = _first(catalog_cols, ("periodicity_code", "periodicity"))
    mappings = [_link_mapping(table, path.name, cols, catalog_cols)
                for table, path, cols in mapping_files]
    encoding = "latin-1" if "latin-1" in encodings else "utf-8"
    data_files.sort(key=lambda d: (d["rank"], d["path"]))
    return ProgramSchema(
        program=program, encoding=encoding,
        catalog_path=catalog_path, catalog_columns=catalog_cols,
        title_column=title_col, seasonal_column=seasonal_col,
        periodicity_column=periodicity_col,
        data_files=data_files, data_columns=data_cols,
        footnote_path=footnote_path, mappings=mappings,
        documentation=documentation, auxiliary=auxiliary,
        unclassified=unclassified,
    )


def required_missing(schema: ProgramSchema) -> list[str]:
    missing = []
    if not schema.catalog_path:
        missing.append("series catalog (*.series)")
    if not schema.data_files:
        missing.append("observation file (*.data.*)")
    else:
        have = {c.lower() for c in schema.data_columns}
        for col in ("series_id", "year", "period", "value"):
            if col not in have:
                missing.append(f"observation column '{col}'")
    return missing


def _first(cols: list[str], names: tuple[str, ...]) -> str | None:
    lower = {c.lower(): c for c in cols}
    for n in names:
        if n in lower:
            return lower[n]
    return None


def _stem(name: str) -> str:
    return re.sub(r"(_code|_text|_name)$", "", name.lower())


def _label_column(cols: list[str]) -> str | None:
    """Prefer a name column even when it is not the second field.

    CES industry files are ``industry_code, naics_code, ..., industry_name``.
    The second field is another code, not the label.
    """
    for name in cols[1:]:
        if name.lower().endswith(("_name", "_text", "_title")):
            return name
    return cols[1] if len(cols) > 1 else None


def _link_mapping(table: str, filename: str, cols: list[str],
                  catalog_cols: list[str]) -> MappingSpec:
    code_col = cols[0]
    label_col = _label_column(cols)
    links: list[str] = []
    # A crosswalk such as fa.category (case_code, category_code, category_text)
    # names two different things. Leave it unlinked so the later category file
    # supplies the category label.
    if label_col and _stem(code_col) == _stem(label_col):
        code_l = code_col.lower()
        stem = _stem(code_col)
        for c in catalog_cols:
            cl = c.lower()
            if cl == code_l or cl == stem or cl == stem + "_code":
                links.append(c)
    return MappingSpec(table=table, path=filename, columns=cols,
                       code_col=code_col, label_col=label_col, links_to=links)
