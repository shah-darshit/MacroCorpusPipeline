# MacroCorpus pipeline

Downloads every survey from the official [BLS LABSTAT](https://download.bls.gov/pub/time.series/) flat files and writes sharded JSONL for series info and observations, plus small name-lookup files. The public BLS API is not required.

The repository does not contain downloaded surveys. `python run.py` creates `data/` on your machine.

## 1. What you need

- Python 3.11 or newer (`python3 --version`).
- Git.
- A network connection to `download.bls.gov`. The program sends a descriptive User-Agent. A bare download client is rejected.

On macOS the system `python3` is sometimes older than 3.11. Install a newer Python first.

The full LABSTAT tree is large: tens of catalogs, some with multi-hundred-megabyte history files, and a few (such as fatal injuries) with hundreds of thousands of series files.

## 2. Clone and install

```bash
git clone https://github.com/shah-darshit/MacroCorpusPipeline.git
cd MacroCorpusPipeline
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e ".[dev]"
```

On Windows PowerShell, activate with `.venv\Scripts\Activate.ps1`.

Leave the virtualenv active. In a new terminal, run `source .venv/bin/activate` again from the project folder.

## 3. Download every catalog

```bash
python run.py
```

That is the only command a new user needs. It lists the LABSTAT root, then for each two-letter survey folder downloads the files and writes the output. One catalog failing is recorded and the next catalog still runs. A later `python run.py` skips a catalog whose remote listing has not changed. Pass `--force` to rebuild anyway.

If BLS throttles a catalog (HTTP 429), the run waits, then tries that catalog again; files that already downloaded are not fetched twice. Catalogs still blocked get one more try at the end of the run. `--catalog-retries 0` turns this off.

DuckDB memory defaults to `auto` (about 60% of the job or VM allocation, at least 1GB). On a large VM use an explicit budget, for example `python run.py --memory-limit 16GB`.

`--only WS,CU` is for tests and debugging. It is not required to process a catalog.

When it finishes, look here:

| Path | What it is |
|---|---|
| `data/run.json` | Which prefixes were found, which finished, which failed |
| `data/cu/raw/` | Original BLS files for that catalog |
| `data/cu/catalog.json` | Joins, coverage, shard list, file checksums |
| `data/cu/series/part-00000.jsonl` | One JSON line per series (codes and observation count) |
| `data/cu/observations/part-00000.jsonl` | One JSON line per observation |
| `data/cu/labels/area.json` | Name lookup for one mapping file (codes → label) |

Series info and observations stay in separate files, sorted by `series_id`. Name tables are written once under `labels/` instead of repeating on every series. To rebuild the old one-record view in Python:

```python
from bls_corpus.pipeline import load_series
row = load_series("data/cu", "CUSR0000SA0")
# row has codes, names (area_name, ...), and observations
```

There is no snapshot id in the path or in the series body. A rerun overwrites `data/<prefix>/`.

## 4. Tests

No download and no API key:

```bash
python -m pytest
```

The tests use a small saved slice of the BD files in `tests/fixtures/bd/` and mocked HTTP.

## How a survey is read

The pipeline reads the header row of each file. A complete data file (`AllItems`, `AllData`, or `AllCESSeries`) is preferred over a `Current` file when both contain the same series, year, and period. Codes stay text. Seasonal status and frequency come from the catalog and the name tables, not from the series title. Folders that are not surveys (`compressed`, `sdmx`, `esbr`) and prefixes with no LABSTAT directory (`PF`, `PI`, `LF`) are skipped.

## More detail

[docs/architecture.md](docs/architecture.md) — how one run moves from the directory listing to the output files
