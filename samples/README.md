# Catalog samples

`scripts/sample_catalog.py` reads one BLS LABSTAT catalog, joins code labels from the lookup files, and keeps a few observations from the head of one data file. Full observation histories are not downloaded.

The JSON files and `raw/` tables in each folder below are produced by that command and are not committed. Run the command to recreate them.

Each program lands in `samples/<prefix>/`:

| File | Contents |
|---|---|
| `metadata.json` | Every series in the catalog, with codes and joined labels. No observations. |
| `samples.json` | The first 5 series found in the sampled data file, up to 24 observations each. |
| `summary.json` | Series count, join match rates, and which data file was sampled. |
| `raw/` | Catalog and lookup files only. `*.data.*` files are streamed and not saved. |

Codes stay strings, including leading zeros. A blank code is counted as blank. An unmatched code is a code that is present but missing from its lookup table. These runs had no unmatched codes.

## Commands

```bash
.venv/bin/python scripts/sample_catalog.py --program CU
.venv/bin/python scripts/sample_catalog.py --program CE
.venv/bin/python scripts/sample_catalog.py --program BD
.venv/bin/python scripts/sample_catalog.py --program AP
.venv/bin/python scripts/sample_catalog.py --program JT
.venv/bin/python scripts/sample_catalog.py --program WS
.venv/bin/python scripts/sample_catalog.py --program FA
```

## Counts

| Program | Catalog | Series | Joins | Data file sampled | Sample |
|---|---|---:|---|---|---|
| CU | CPI | 8,104 | area, base, item, periodicity, seasonal (8,104 / 8,104) | `cu.data.0.Current` | `CUSR0000SA0` and 4 others, 120 observations |
| CE | CES | 22,049 | datatype, industry, seasonal, supersector (22,049 / 22,049) | `ce.data.01a.CurrentSeasAE` | `CES0000000001` and 4 others, 120 observations |
| BD | Business Employment Dynamics | 34,464 | 12 lookups, all 34,464 / 34,464 | `bd.data.0.Current` | `BDS0000000000000000110001LQ5` and 4 others, 120 observations |
| AP | Average prices | 1,483 | area, item (1,483 / 1,483) | `ap.data.0.Current` | `APU0000701111` and 4 others, 120 observations |
| JT | JOLTS | 2,060 | area, dataelement, industry, ratelevel, seasonal, sizeclass, state (2,060 / 2,060) | `jt.data.0.Current` | `JTS000000000000000HIL` and 4 others, 120 observations |
| WS | Work stoppages | 6 | measure, seasonal (6 / 6) | `ws.data.1.AllData` | `WSU001` and 4 others, 120 observations |
| FA | Census of Fatal Occupational Injuries | 492,217 | area, case, category, datatype all matched; see blanks below | `fa.data.0.Current` | `FAU00X00000080N00` and 4 others, 10 observations |

CU was checked against the CPI reference: `CUSR0000SA0` has area name `U.S. city average` and item name `All items`. The live catalog end period is later than the reference copy. CE headline `CES0000000001` joins to supersector Total nonfarm, industry Total nonfarm, and datatype ALL EMPLOYEES, THOUSANDS.

## What was skipped

- Full `*.data.*` files. The script streams one file (preferring a name that contains `Current`) until it has 5 series or 20,000 lines, then stops.
- `*.txt` and `*.contacts` files.
- Tables that are themselves observation-like (`series_id`, `year`, and `period`), such as `cu.aspect`.
- Crosswalks whose code column and label column name different fields. `fa.category` starts with `case_code` and was not used. `fa.category2` supplies `category_text` from `category_code`.

FA sample observations stop at 10 because `fa.data.0.Current` only had a few rows before the fifth series. JT has no `series_title` column, so those sample titles are empty. Many FA series leave event, industry, occupation, or source blank in the catalog:

| Lookup | Matched | Blank |
|---|---:|---:|
| `fa.event` | 43,163 | 449,054 |
| `fa.industry` | 207,765 | 284,452 |
| `fa.occupation` | 138,740 | 353,477 |
| `fa.source` | 102,549 | 389,668 |
