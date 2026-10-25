# Data dictionary

Outputs land in `data/processed/<snapshot_id>/`. The column set is the same for every survey. Counts below are the measured BD snapshot (`bd-20260729T100000Z-e302ea2f`, 34,464 series). Codes are text. Leading zeros are kept. Padding spaces around a field are removed. `null` means the source did not supply a value.

## `series_catalog.parquet` — one row per series

| Column | Type | Source / derivation |
|---|---|---|
| `snapshot_id` | string | Snapshot identifier, e.g. `bd-20260729T100000Z-e302ea2f` |
| `program_id` | string | LABSTAT abbreviation, such as `BD` |
| `series_id` | string | Catalog `series_id`, spaces stripped |
| `series_id_raw` | string | Same stripped id as `series_id` |
| `series_title` | string | Catalog title. Empty when the catalog has no title column (JOLTS does not). |
| `seasonal_code` | string | Raw seasonal code, often `S` or `U`. Null when the catalog has no seasonal column. |
| `seasonal_status` | string | Label from the survey’s seasonal name table. If that table has no row, `S` and `U` use the documented labels. Never read from the title. |
| `seasonal_status_method` | string | `mapping_file`, `documented_code_convention`, `missing`, or `unknown_code` |
| `measure` / `unit` | string | Null. Level, rate, and unit codes stay in `metadata_json` and in `series.jsonl` dimensions. |
| `frequency` | string | From `periodicity_code` when that column exists: `Q` quarterly, `M` monthly, `A` annual, `R` monthly, `SA` semiannual, `W` weekly |
| `frequency_method` | string | `catalog_periodicity_code`, or `unknown_no_periodicity_code` |
| `first_period` / `last_period` | string | From the kept observations, as `YYYY-PPPP` (BD example: `1992-Q03`) |
| `observation_count` | int64 | Deduplicated observation rows for the series |
| `source_catalog_file` | string | The `*.series` filename |
| `metadata_json` | string | The catalog’s code columns for this survey, as JSON |

## `observations.parquet` — one row per (series, year, period) (4,598,544 rows)

| Column | Type | Source / derivation |
|---|---|---|
| `snapshot_id`, `program_id`, `series_id` | string | As above |
| `year` | int32 | Source `year` verbatim |
| `period` | string | Source period code verbatim (`Q01`–`Q04` observed; `Q05` supported = annual average) |
| `period_start` / `period_end` | string (ISO date) | Calendar quarter bounds for `Q01`–`Q04`; year bounds + `period_kind=annual_average` for `Q05/M13` — no fabricated exact dates |
| `period_kind` | string | `quarterly` \| `monthly` \| `annual_average` \| `annual` \| `semiannual` \| `unknown_period` |
| `value_raw` | string | Source `value` verbatim (whitespace-padded in file, stripped here, digits unchanged) |
| `value_numeric` | float64 (null) | Parsed numeric value; null when missing/non-numeric |
| `value_status` | string | `numeric` \| `missing_token` \| `non_numeric` |
| `footnote_codes` | string | Raw footnote code(s), comma/space separated preserved |
| `source_file` | string | Data file that won precedence. A complete file (`AllItems`, `AllData`, `AllCESSeries`) outranks a `Current` file. |
| `source_row` | int64 | 1-based row number in the source file (header = row 1) |

Logical key: `(snapshot_id, series_id, year, period)` — verified unique **after**
the documented partition-overlap deduplication (see validation report).

## `footnotes.parquet`

`snapshot_id, program_id, footnote_code, footnote_text, source_file` — from that survey’s `*.footnote` file.
BD has two codes: `1` total-private-includes-unclassified, `2` administrative event.

## `reference_metadata.parquet`

Long-form code-to-label tables: `snapshot_id, program_id, table_name, code, label,
extra_json, source_file`. One row per code in each joined name table. BD’s tables are
industry (91), state (54), sizeclass (32), dataclass (8), dataelement (2),
periodicity (2), ratelevel (2), seasonal (2), county (1), msa (1), ownership (1),
unitanalysis (1). `extra_json` is present and empty; only the code and the label are loaded.

## File manifest (`data/manifests/<snapshot_id>.json`)

Per file: `relative_path, source_url, file_role, required, remote_size, local_size,
last_modified, etag, sha256, download_status, parse_status, attempts, error_message`,
plus run ID, UTC timestamp, downloader version, git commit, config summary, and the
non-atomicity note. Statuses: `downloaded | skipped_not_modified | skipped_verified |
failed | pending`.

## JSON

`dataset.json` is one object for the survey: the inferred layout, each name table as a code-to-label map, footnotes, and coverage counts.

`series.jsonl` is one object per series. It repeats the title, seasonal code, frequency, and period range, then `dimensions` (each joined code with its label), `metadata` (the catalog codes), and `observations` (year, period, start, end, numeric value, raw value, footnotes).
