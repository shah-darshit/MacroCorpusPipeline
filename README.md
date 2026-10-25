# MacroCorpus pipeline

Downloads one BLS survey from the official [LABSTAT flat files](https://download.bls.gov/pub/time.series/) and turns it into JSON and Parquet. The survey is chosen with a two-letter prefix such as `WS`, `CU`, or `BD`. The same command works for every prefix. The public BLS API is not required.

Nothing in this repository is a downloaded survey. The first run creates a `data/` folder on your machine.

## 1. What you need

- Python 3.11 or newer. Check with `python3 --version`.
- Git.
- A network connection to `download.bls.gov`. The project sends a descriptive User-Agent from `config/default.toml`. A bare download client is rejected by the BLS site.

On macOS, the system `python3` is sometimes older than 3.11. Install a newer Python first, then use that interpreter in the steps below.

## 2. Clone and install

```bash
git clone https://github.com/shah-darshit/MacroCorpusPipeline.git
cd MacroCorpusPipeline
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e ".[dev]"
```

On Windows PowerShell, activate with `.venv\Scripts\Activate.ps1` instead of `source`.

Check that the command is available:

```bash
bls-corpus --help
```

Leave the virtualenv active for the commands below. If you open a new terminal, run `source .venv/bin/activate` again from the project folder.

## 3. First full run

Start with work stoppages. That survey has 6 series, so the download and the build finish quickly and you can see the output shape before running a large survey.

```bash
bls-corpus run --program WS
```

That one command does three things: download the files, build the tables, and write a check report.

When it finishes, look here:

| Path | What it is |
|---|---|
| `data/raw/ws/<snapshot_id>/` | The original BLS files, saved as downloaded |
| `data/processed/<snapshot_id>/dataset.json` | One card for the survey: layout, code names, footnotes, counts |
| `data/processed/<snapshot_id>/series.jsonl` | One JSON object per series, including each value |
| `data/processed/<snapshot_id>/series_catalog.parquet` | One row per series |
| `data/processed/<snapshot_id>/observations.parquet` | One row per series, year, and period |
| `data/manifests/<snapshot_id>.json` | Receipt: file sizes and SHA-256 |
| `data/processed/<snapshot_id>/validation-<snapshot_id>.md` | Short check report |

`<snapshot_id>` looks like `ws-20261009T100000Z-b16fb1b4`. The exact id changes when BLS updates the folder. Open `dataset.json` in any text editor. `series.jsonl` is one JSON object per line, not one pretty-printed file.

Business Employment Dynamics is the large example: about 34,000 series and about 4.6 million values. Expect a few hundred megabytes of download.

```bash
bls-corpus run --program BD
```

Any other prefix is the same command, for example `bls-corpus run --program CU`. The prefix is the folder name on the LABSTAT site, in upper case.

## 4. Run the stages separately

Use these when you want to stop between download and processing.

```bash
bls-corpus discover --program WS
bls-corpus download --program WS
bls-corpus parse --program WS
bls-corpus validate --program WS
```

`discover` only prints the file list. `download` fills `data/raw/`. `parse` reads those local files and writes `data/processed/`. `validate` writes the check report.

After a successful download, rebuild without touching the network:

```bash
bls-corpus run --program WS --offline
```

If a file failed partway through:

```bash
bls-corpus download --program WS --retry-failed
```

## 5. Catalog sample, without the full history

Use this to see how a survey’s codes connect, without saving every observation.

```bash
python scripts/sample_catalog.py --program CU
```

It writes:

| Path | What it is |
|---|---|
| `samples/cu/metadata.json` | Every series, with names attached. No history. |
| `samples/cu/samples.json` | Five series and a short run of values |
| `samples/cu/summary.json` | Series count and whether every code found a name |
| `samples/cu/raw/` | The series list and the name tables only |

Replace `CU` with another prefix to sample that survey. Counts from a previous local run are written up in [samples/README.md](samples/README.md). Those output files are created on your machine and are not in the git repository.

## 6. Tests

No download and no API key:

```bash
python -m pytest
```

The tests use a small saved slice of the BD files in `tests/fixtures/bd/` and mocked HTTP.

## 7. Optional API spot-check

This compares a handful of values with api.bls.gov. It does not replace the files you downloaded. Skip it for a normal run.

```bash
bls-corpus validate-api --program BD --sample-size 25
```

A key is optional. If you have one, set `BLS_API_KEY` in the environment before that command. Do not put the key in a file that you commit.

## How a survey is read

The pipeline reads the header row of each file. It does not have a separate program for each survey. A complete data file (`AllItems`, `AllData`, or `AllCESSeries`) is preferred over a `Current` file when both contain the same series, year, and period. Codes stay text, so leading zeros are kept. Seasonal status and frequency come from the catalog and the name tables, not from the series title.

Settings that you can change without editing Python are in `config/default.toml`: the default prefix, timeouts, and how many downloads run at once. `--program` overrides the default prefix.

## More detail

- [docs/architecture.md](docs/architecture.md) — the path from the BLS folder listing to the output files
- [docs/data_dictionary.md](docs/data_dictionary.md) — column meanings
- [docs/source_inventory.md](docs/source_inventory.md) — how the download site behaves
- [docs/validation_report.md](docs/validation_report.md) — measured coverage for the BD snapshot
