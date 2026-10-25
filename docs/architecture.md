# Architecture

One machine, one survey at a time. No database server. `bls-corpus run --program XX` downloads that survey’s folder from `https://download.bls.gov/pub/time.series/xx/`, then builds the outputs from the files it saved.

```text
config/default.toml and --program
        │
        ▼
  discovery ──▶ manifest ──▶ downloader ──▶ data/raw/<prefix>/<snapshot_id>/
                                                  │
                                                  ▼
                                            schema.py reads headers
                                                  │
                                                  ▼
                                            engine.py (DuckDB)
                                                  │
                                                  ▼
                                            data/processed/<snapshot_id>/
                                            parquet, dataset.json, series.jsonl
                                                  │
                                                  ▼
                                            validate.py
```

`validate-api` is separate. It compares a few values with api.bls.gov and does not replace the downloaded numbers.

## Modules

| Module | Job |
|---|---|
| `cli.py` | `discover`, `download`, `parse`, `validate`, `run`, `validate-api` |
| `config.py` | Reads `config/default.toml`. CLI flags override it. |
| `discovery.py` | Reads the folder listing and labels each file: series list, observations, footnotes, name table, or unknown. |
| `manifest.py` | Receipt for one snapshot: sizes, checksums, download status. |
| `downloader.py` | The only bulk download. Writes a temp file, then renames it. Retries network failures. Skips a file the site says has not changed. |
| `schema.py` | Decides each file’s job from its name and header row. Joins a name table only when the code and the label name the same thing. |
| `engine.py` | The processor `parse` and `run` call. Loads the tables in DuckDB, drops duplicate observations, writes Parquet and JSON. |
| `validate.py` | Checks the finished tables against the receipt. |
| `api_validation.py` | Optional API comparison. |
| `parser.py`, `normalize.py`, `storage.py` | The earlier reader. `parse` does not call it. Its tests still do. |

`scripts/sample_catalog.py` is not part of this chain. It only downloads the series list and name tables and reads the start of one data file. See `samples/README.md`.

## Snapshot id

`<prefix>-<newest listing time in UTC>-<first 8 hex chars of the listing>`. A finished snapshot directory is not overwritten. A later listing with a new time or new contents gets a new id. Files are downloaded one after another, so the folder is a record of that crawl, not a single instant on the BLS server. Each file’s size, last-modified time, and SHA-256 are on the receipt.

## Another survey

Pass `--program` with the two-letter folder name. No per-survey module is required. The header row supplies the columns. A name table is attached when its code column matches a catalog column and its label column names that same thing. A file that itself has `series_id`, `year`, and `period` is not used as a name table.
