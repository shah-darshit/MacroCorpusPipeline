"""Download one BLS catalog, join code labels, and keep a few sample observations.

Does not download full observation files. Usage:

    python scripts/sample_catalog.py --program CU
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import re
from pathlib import Path

import httpx

ROOT = "https://download.bls.gov/pub/time.series"
UA = (
    "Mozilla/5.0 (Macintosh; BLS-Corpus-Research/1.0; +https://www.bls.gov/data/) "
    "bls-corpus/0.1"
)
LINE_RE = re.compile(
    r"(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s*[AP]M)\s+"
    r"(&lt;dir&gt;|\d+)\s+"
    r'<A HREF="[^"]+">([^<]+)</A>',
    re.IGNORECASE,
)
SAMPLE_SERIES = 5
SAMPLE_OBS = 24
MAX_DATA_LINES = 20000


def listing(client: httpx.Client, program: str) -> list[tuple[str, int | None]]:
    url = f"{ROOT}/{program.lower()}/"
    text = client.get(url).raise_for_status().text
    found = []
    for match in LINE_RE.finditer(text):
        name = match.group(3).strip()
        if name.startswith("[To"):
            continue
        size = None if match.group(2) == "&lt;dir&gt;" else int(match.group(2))
        if size is not None:
            found.append((name, size))
    return found


def download(client: httpx.Client, program: str, name: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"{ROOT}/{program.lower()}/{name}"
    with client.stream("GET", url) as response:
        response.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".part")
        with tmp.open("wb") as handle:
            for chunk in response.iter_bytes():
                handle.write(chunk)
        tmp.replace(dest)


def read_table(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("latin-1")
    rows = list(csv.reader(io.StringIO(text), delimiter="\t"))
    if not rows:
        return [], []
    header = [cell.strip() for cell in rows[0]]
    while header and header[-1] == "":
        header.pop()
    records = []
    for raw_row in rows[1:]:
        if not any(cell.strip() for cell in raw_row):
            continue
        values = [cell.strip() for cell in raw_row]
        records.append({name: values[i] if i < len(values) else "" for i, name in enumerate(header)})
    return header, records


def label_column(columns: list[str]) -> str | None:
    for name in columns[1:]:
        if name.lower().endswith(("_name", "_text", "_title")):
            return name
    return columns[1] if len(columns) > 1 else None


def _stem(name: str) -> str:
    return re.sub(r"(_code|_text|_name)$", "", name.lower())


def catalog_column_for(code_column: str, catalog_columns: list[str]) -> str | None:
    code = code_column.lower()
    stem = re.sub(r"(_code|_text|_name)$", "", code)
    by_lower = {name.lower(): name for name in catalog_columns}
    for candidate in (code, stem, stem + "_code"):
        if candidate in by_lower:
            return by_lower[candidate]
    return None


def sample_observations(client: httpx.Client, program: str, data_name: str) -> tuple[dict[str, list[dict]], int]:
    """Read only the start of one data file. Stop after a few series."""
    url = f"{ROOT}/{program.lower()}/{data_name}"
    collected: dict[str, list[dict]] = {}
    lines_read = 0
    header: list[str] | None = None
    with client.stream("GET", url) as response:
        response.raise_for_status()
        buffer = ""
        for chunk in response.iter_bytes():
            buffer += chunk.decode("latin-1")
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                lines_read += 1
                line = line.rstrip("\r")
                if header is None:
                    header = [cell.strip() for cell in line.split("\t")]
                    continue
                if not line.strip() or header is None:
                    continue
                cells = [cell.strip() for cell in line.split("\t")]
                row = {name: cells[i] if i < len(cells) else "" for i, name in enumerate(header)}
                series_id = row.get("series_id", "")
                if not series_id:
                    continue
                if series_id not in collected:
                    if len(collected) >= SAMPLE_SERIES:
                        return collected, lines_read
                    collected[series_id] = []
                if len(collected[series_id]) < SAMPLE_OBS:
                    collected[series_id].append({
                        "year": row.get("year", ""),
                        "period": row.get("period", ""),
                        "value": row.get("value", ""),
                        "footnote_codes": row.get("footnote_codes", ""),
                    })
                if lines_read >= MAX_DATA_LINES:
                    return collected, lines_read
    return collected, lines_read


def choose_data_file(names: list[str]) -> str | None:
    data_files = [name for name in names if ".data." in name.lower()]
    if not data_files:
        return None
    current = [name for name in data_files if "current" in name.lower()]
    return sorted(current or data_files)[0]


def run(program: str, out_root: Path) -> dict:
    program = program.upper()
    out = out_root / program.lower()
    raw_dir = out / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    with httpx.Client(headers={"User-Agent": UA, "Accept-Encoding": "identity"},
                      timeout=120, follow_redirects=True) as client:
        files = listing(client, program)
        names = [name for name, _size in files]
        for name, _size in files:
            lower = name.lower()
            if ".data." in lower or lower.endswith(".txt") or lower.endswith(".contacts"):
                continue
            download(client, program, name, raw_dir / name)
        data_name = choose_data_file(names)
        observations, lines_read = ({}, 0)
        if data_name:
            observations, lines_read = sample_observations(client, program, data_name)

    series_name = next((name for name in names if name.lower().endswith(".series")), None)
    if series_name is None:
        raise SystemExit(f"{program}: no series catalog in the directory listing")
    catalog_columns, series_rows = read_table(raw_dir / series_name)

    lookups = []
    for path in sorted(raw_dir.iterdir()):
        if path.name.lower().endswith(".series") or path.name.lower().endswith(".footnote"):
            continue
        columns, rows = read_table(path)
        if len(columns) < 2 or not rows:
            continue
        lower = {name.lower() for name in columns}
        if {"series_id", "year", "period"} <= lower:
            continue
        code_column = columns[0]
        label = label_column(columns)
        if not label:
            continue
        # Skip crosswalks such as fa.category (case_code, category_code, category_text).
        # A lookup is joined only when the code and the label name the same thing.
        if _stem(code_column) != _stem(label):
            continue
        linked = catalog_column_for(code_column, catalog_columns)
        if not linked:
            continue
        lookups.append({
            "file": path.name,
            "catalog_column": linked,
            "code_column": code_column,
            "label_column": label,
            "by_code": {row.get(code_column, ""): row.get(label, "") for row in rows if row.get(code_column, "")},
        })

    join_stats = []
    for lookup in lookups:
        matched = unmatched = blank = 0
        examples = []
        column = lookup["catalog_column"]
        for row in series_rows:
            code = row.get(column, "")
            if not code:
                blank += 1
                row[lookup["label_column"]] = ""
                continue
            label = lookup["by_code"].get(code)
            if label is None:
                unmatched += 1
                row[lookup["label_column"]] = ""
                if len(examples) < 5:
                    examples.append(code)
            else:
                matched += 1
                row[lookup["label_column"]] = label
        join_stats.append({
            "file": lookup["file"],
            "catalog_column": column,
            "label_column": lookup["label_column"],
            "matched": matched,
            "unmatched": unmatched,
            "blank": blank,
            "unmatched_examples": examples,
        })

    samples = []
    by_id = {row.get("series_id", ""): row for row in series_rows}
    for series_id, obs in observations.items():
        meta = dict(by_id.get(series_id, {"series_id": series_id}))
        meta["observations"] = obs
        samples.append(meta)

    summary = {
        "program": program,
        "series_count": len(series_rows),
        "catalog_fields": catalog_columns,
        "joins": join_stats,
        "data_file_sampled": data_name,
        "data_lines_read": lines_read,
        "sample_series": len(samples),
        "sample_observations": sum(len(item["observations"]) for item in samples),
        "skipped": "full observation files were not downloaded",
    }
    _write(out / "metadata.json", series_rows)
    _write(out / "samples.json", samples)
    _write(out / "summary.json", summary)
    return summary


def _write(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--program", required=True, help="LABSTAT prefix, for example CU")
    parser.add_argument("--out", default="samples")
    args = parser.parse_args()
    summary = run(args.program, Path(args.out))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
