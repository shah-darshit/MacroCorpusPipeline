# BLS LABSTAT coverage audit

Assessment date: 2026-11-08. Repository: `MacroCorpusPipeline`; branch `main`; inspected commit `5c1ee2d`. No scraper changes, full downloads, output deletion, staging, commits, or pushes were performed.

Follow-up: a subsequent full raw-table analysis is documented at the end of this report. It independently establishes the CA/CB singleton counts and hashes every downloaded observation file, superseding the initial audit’s sampling limits for those checks. Physical series-file counts remain unverified.

## Findings at a glance

Discovery matches all **66 current two-letter LABSTAT directories**, but the saved run records only **5 successes, 1 failure, and 60 datasets without results**. Successful status is not a correctness certificate: BG/BP lose observation qualifiers and most descriptive labels. CA's 52.4 million singleton series are unusual but consistent with the source's current biennial snapshot; they are not established scraper fragmentation or history truncation.

The five successful records report **52,475,041 series and 57,427,518 observations**. These are scraper-reported totals, not independently verified totals of physical output files or a denominator for observation coverage. No exact observation-coverage percentage is claimed.

Repository disk usage: **97 GiB allocated, rounded** (`du -sh .` returned `97G` on 2026-11-08). This single completed traversal includes downloaded data, hidden files, `.git`, and `.venv`. NFS traversal was slow; no second exhaustive traversal was performed. This is allocated filesystem usage, not an exact logical-byte total.

## Scope and evidence

Read `README.md`, `docs/architecture.md`, `run.py`, `pyproject.toml`, the entire `src/bls_corpus/pipeline.py`, and discovery/downloader/resume/schema/engine/pipeline tests. No applicable `AGENTS.md` was found in the repository or ancestor paths; `.agents` and `.codex` were empty. Baseline verification: all **26 existing tests passed** (`TMPDIR=/tmp PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -p no:cacheprovider`, 1.69 seconds), using mocks/local fixtures without network downloads. These tests do not certify the production corpus or cover the observed legacy-format failures. The stated scope is every survey in the BLS LABSTAT flat-file tree, with one JSON file per native BLS series, preserving full available history and joining labels. It is not all BLS datasets, APIs, microdata, or historical website tables outside that tree.

Inspected `data/run.json`, all five existing `catalog.json` and `download.json` files, all four manageable indexes, CB's raw-directory inventory, local survey documentation/mappings, and representative series outputs. `audit/check_local.py` is an independent standard-library reader, not a scraper rerun; its saved results are `audit/local_checks.json`.

Independent provider checks used the official [LABSTAT root](https://download.bls.gov/pub/time.series/) and the six attempted survey listings: [AP](https://download.bls.gov/pub/time.series/ap/), [BD](https://download.bls.gov/pub/time.series/bd/), [BG](https://download.bls.gov/pub/time.series/bg/), [BP](https://download.bls.gov/pub/time.series/bp/), [CA](https://download.bls.gov/pub/time.series/ca/), [CB](https://download.bls.gov/pub/time.series/cb/). Names and displayed byte sizes match all recorded files for the five successes: 12 AP, 18 BD, 11 BG, 12 BP, 26 CA. Local `stat` sizes also match their receipts/listing sizes. Recomputed SHA-256 for each raw file under 10 MB matches its catalog receipt. Large raw files were not rehashed.

Official pages were inspected through the web tool on the assessment date; its cached retrieval ages ranged from today to several weeks. This is independent listing/documentation evidence, not a fresh binary-content verification. A direct VM request for the root returned HTTP 403. Attempts to fetch BG binary files through the web tool were rejected as unsupported content types; CB/TU documentation also failed decoding. No new raw observation downloads were performed. Listing size agreement cannot prove equality of contents or freshness.

The [LABSTAT overview](https://download.bls.gov/pub/time.series/overview.txt) explains the flat-file relationships, but is older than the current root (for example it lists NL, absent from the current tree). Use the actual current root for discovery counts, not the overview's historical list.

## Discovery and completion

The current root has 66 two-letter directories plus `compressed`, `esbr`, and `sdmx`. The selected `data/run.json` prefix list equals those 66 survey directories: no confirmed current root discovery omissions or extra selections. The code explicitly skips PF/PI/LF; none appears in the current root, so this is not a confirmed present coverage gap. The two-letter naming restriction, IIS-specific HTML regex, ignored link hrefs, and lack of recursion remain future discovery risks. Content unique to the three excluded infrastructure trees has not been reconciled.

Family groups below are analytical groupings for this audit, not additional provider datasets. Each prefix appears exactly once. “OK” means reported successful, with the qualifications below; “absent” means no saved result and no local dataset directory.

| Family | Selected/current directories | OK | Failed | Absent / not reached | Prefixes |
|---|---:|---:|---:|---:|---|
| Prices | 13 | 1 | 0 | 12 | ap cu cw su mu mw li ei pc pd nd wd wp |
| Employment and labor force | 14 | 1 | 0 | 13 | bd ce ee sm sa fm gg gp jl jt la ln lu ml |
| Compensation, earnings and working conditions | 15 | 2 | 0 | 13 | bg bp cc ci cm eb ec le nb nc nw oe or wm ws |
| Workplace injuries and illnesses | 15 | 1 | 1 | 13 | ca cb cd cf ch cs fa fi fw hc hs ii is sh si |
| Productivity and international comparisons | 4 | 0 | 0 | 4 | in ip mp pr |
| Spending and time use | 2 | 0 | 0 | 2 | cx tu |
| Other programs | 3 | 0 | 0 | 3 | ep kv oa |
| **Total** | **66** | **5** | **1** | **60** | |

Discovery is 66/66 current root survey directories; recorded success is 5/66 (about 7.6%) at the **dataset-status level only**. The saved run began 2026-11-04 21:08 UTC and was last written locally on November 6 after CB. It has neither `finished_utc` nor a final `failed` count. All 60 prefixes after CB in the saved selection have no results or directories. They were selected but apparently never reached; do not describe them as request failures or discovery omissions. The reason the overall run stopped is unverified. `run_all` intends to continue after a handled failure, so CB's handled error alone does not explain termination.

| Dataset | Recorded state | Series | Observations | Reported years | Assessment |
|---|---|---:|---:|---|---|
| AP | ok | 1,483 | 356,553 | 1973–2026 | All local catalog series checked against raw values |
| BD | ok | 34,416 | 4,626,528 | 1992–2026 | Index reconciled; 101 series histories checked |
| BG | ok | 25 | 784 | 1983–1995 | Values verified; qualifiers and labels lost |
| BP | ok | 95 | 4,631 | 1983–1995 | Values verified; qualifiers and labels lost |
| CA | ok | 52,439,022 | 52,439,022 | 2024 | Only first 32 series checked; source snapshot, not independently recounted |
| CB | failed | 0 recorded | 0 recorded | not processed | Large observation files present but catalog/mappings missing |

## Confirmed coverage failures

1. **Most selected datasets were never reached.** This omits their entire measurements, geographic levels, demographic/industry dimensions, and histories from the current output. Examples include CU/CW inflation, CE/SM employment, LA local unemployment, LN labor force, JT vacancies, and all productivity and time-use programs. They are absent because of incomplete execution, not because a confirmed variable/geography filter excluded them. There is no fixed date, measurement, or geography filter in the observation loader.
2. **Historical injury coverage remains absent across related datasets.** CA alone cannot represent older injury histories. BLS explicitly separates current and historical classifications/reference periods on its [discontinued database page](https://www.bls.gov/data/archived.htm) and [injury-data page](https://www.bls.gov/iif/nonfatal-injuries-and-illnesses-tables.htm). CB was attempted and failed; other selected historical injury prefixes were not reached. Do not concatenate those histories without classification and comparability checks.

## Confirmed processing problems

### CB download failure and lost diagnostics

`data/run.json` names 14 required failures: `cb.datatype`, `cb.event`, `cb.footnote`, `cb.gender`, `cb.hour`, `cb.los`, `cb.medical`, `cb.nature`, `cb.occupation`, `cb.ownership`, `cb.pob`, `cb.race`, `cb.series`, `cb.special`. The [official CB listing](https://download.bls.gov/pub/time.series/cb/) contains every one; this is not a nonexistent dataset.

The raw directory contains 12 completed files, including both observation files (2,233,698,411 bytes each), but no `cb.series`, `download.json`, catalog, or index. Fetching was attempted; parsing/export never completed. The code saves the manifest only **after** `download_all` returns, and that function raises on required-file failure. Consequently per-file status codes, attempts, and errors were not persisted for this failure. The run log establishes failed fetching but not its HTTP cause. Current audit HTTP 403 is not proof that the original CB failures were 403.

### BG/BP observation qualifiers silently discarded

Both raw data headers include `cont_break`, `status`, and `footnote_exists`. `_load_observations` retains only the conventional `footnote_codes` column, if present; export always emits year, period, value, and footnote codes. The other fields disappear despite `ok` status.

Independent raw counts: BG has **101 provisional (`P`) observations**, 683 final (`F`), and **34 footnote indicators (`Y`)**. BP has 4,631 final observations and **227 footnote indicators**. Both have only `N` continuity-break markers in the inspected full raw data. Thus provisional status and positive footnote indicators are demonstrably lost; actual continuity-break loss is a general implementation risk, not observed here. Exported blank `footnote_codes` does not mean the source had no footnote. The official [BG specification](https://download.bls.gov/pub/time.series/bg/bg.txt) and [BP specification](https://download.bls.gov/pub/time.series/bp/bp.txt) document these fields and their meaning.

### BG/BP mapping parsing does not handle their actual files

Local `bg.measure` starts with a prose heading and dashed separator, followed by space-aligned codes and wrapped labels. `bp.measure` has a prose heading before a space-aligned header. Several other mapping files have similar layouts. `infer_schema` assumes the first line is a tab-separated header and ignores one-column layouts. BG consequently has **zero mapping joins**, and BP only its periodicity join, although raw mappings were downloaded. Measurement, unit, alteration, sector/industry, and contract descriptions are missing from series output. This is enrichment loss, not proof that the associated numeric measurements were omitted. Native measurement codes remain present (12 BG and 28 BP distinct measure codes).

### CA descriptive/context loss, with source inconsistencies

`ca.category` uses a composite `(case_code, category_code)` relation; `_link_mapping` only joins when its first code column stem matches the label stem. It is not joined at all. `CAU00X00000013000.json` retains `category_code=00X` and `case_code=3` but lacks the available category label “All industry.” This is a confirmed omission of source metadata, not a wrong native series key. Other CA mappings are largely joined according to the receipt, but only sampled outputs were independently inspected.

`ca.medical` was downloaded but the actual local `ca.series` header lacks `medical_code`, even though the official CA prose specification mentions it. This is a source/schema discrepancy requiring provider clarification; it is **not confirmed** that the scraper dropped a medical column present in the raw catalog.

## Checks of successful outputs, history, keys, and duplicates

- **Independent local value comparison:** read all AP/BD/BG/BP raw observation files once (no traversal of millions of files), then compared complete raw-derived histories with all AP (1,483), BG (25), BP (95), and 101 evenly spaced BD catalog entries. Checked 1,704 output files and 375,519 observations in those four datasets, plus 32 CA files/observations. No missing sampled files, wrong filename/body IDs, duplicate output year-period keys, value/footnote mismatches, or sampled duplicate conflicts were found. This validates local transformation, not provider-level observation completeness.
- **Small catalogs and indexes:** AP/BD/BG/BP native catalog IDs are unique. Index keys exactly equal catalog keys and indexed observation totals agree with the receipts. Catalog dimensions include all 74 AP area codes and 160 item codes; BD has 54 state codes, 91 industry codes, 32 size-class codes, 8 data-class codes, 2 data-element codes, 2 rate/level codes, 2 periodicity codes, and both seasonal states. These counts come from the full local catalogs, not an independent universe of all BLS combinations. The 54 state codes include aggregate/special codes; this is not a claim of 54 states. BD county/MSA mappings contain only the aggregate code; no confirmed county/MSA omission was established within BD's supplied catalog.
- **Endpoints:** every AP output and all 101 sampled BD histories match their catalog start/end year and period. BG/BP observations preserve the raw file's endpoints, but the catalogs disagree: all 25 BG and 95 BP comparisons flag endpoints, mostly because Q05 annual averages sort after the advertised last quarter. Twelve BG catalog entries even begin at Q13 while actual observations use Q05. These are inherited catalog/data inconsistencies and mixed-frequency issues, not evidence of scraper truncation. Do not treat lexical period comparisons as an expected-observation calendar.
- **Old endpoints:** BG/BP raw data and official directory files are historical, dated 1995/1996. A 1995 ending year is not sufficient evidence of truncation. Their raw values, including blank values, all survive in the checked outputs.
- **Deduplication:** receipts report AP 213,718, BD 3,613,680, and CA 52,439,022 duplicate removals; BG/BP report zero. Overlapping current/history files explain the observed AP duplicates and sampled BD overlaps. CA's current/all-data receipts have identical sizes and SHA-256; source listing sizes also agree. Full CA equality was not rehashed. No harmful duplicate removal was confirmed in checked histories.
- **Misleading duplicate statistic:** `_stats` calls `sum(n)-count(keys)` “duplicate_identical” even when conflicting duplicates exist; `duplicate_conflicts` counts conflicting keys, not discarded rows. Conflict selection is rank, filename, then source-row order, not revision recency. Current receipts report zero conflicts, but no complete independent BD/CA conflict census was done.
- **Keys and fragmentation:** observations group by native `series_id`, deduplicate by `(series_id, year, period)`, and export one file per catalog ID. There is no generated geography/date suffix splitting the checked histories. Native IDs distinguish measurements/dimensions; no confirmed series-key collision was found. Duplicate catalog IDs would overwrite the same output path, and observations lacking a catalog row would be counted in stats but omitted by the catalog-left-join export. These are implementation risks; full raw-orphan reconciliation was not done for BD/CA.
- **CA singleton interpretation:** BLS's [CA documentation](https://download.bls.gov/pub/time.series/ca/ca.txt) says year 2024 represents the biennial reference period 2023–2024 and A01 represents two years. Local catalog/observations sampled match that format and value (first ID has 54.9). The 52,439,022 singleton count remains receipt-based; 32 consecutive initial IDs are a narrow sample, not statistical validation. A singleton snapshot is not confirmed accidental fragmentation. Generic engine period macros would incorrectly describe it as a one-year annual period if those internal dates were used; the final JSON preserves the native year/period but exports no explicit biennial context.

## Suspicions and implementation risks requiring targeted verification

1. **Partial downloads may be accepted:** `_attempt` compares received bytes to HTTP Content-Length and checks listing size only when Content-Length is absent. A shorter but internally consistent HTTP response can be marked downloaded even when it disagrees with the listing. A resumed 206 response is appended without validating Content-Range start/total or stable representation; an interrupted partial file may belong to an older version. No observed successful local file has a receipt/listing/stat size mismatch, so this is a confirmed code weakness, not a confirmed corrupted download.
2. **Silent skips after local damage:** `_unchanged` trusts the catalog's remote-listing hash and skips the whole program before checking output existence or checksums. A deleted/truncated output, earlier partial export, or same-listing content change can remain “skipped.” Only actual output validation should establish completion.
3. **Auxiliary measurements not exported:** files with observation-like columns but names outside `.data.*` become `auxiliary` and never enter observations. The official [TU listing](https://download.bls.gov/pub/time.series/tu/) includes `tu.aspect` (114,271,376 bytes), beyond its standard value files. The schema test explicitly expects an aspect-style file to be auxiliary. TU was not reached, so actual loss of its auxiliary measurements is not demonstrated locally; inspect its schema and intended uncertainty/aspect output before claiming full coverage.
4. **Parsing assumptions:** one encoding and the first data file's column positions are used for every data file; tab headers and minimum four fields are assumed. DuckDB uses `ignore_errors=true`/`store_rejects=true`, but reject tables are not reconciled in reported malformed counts. Rows rejected before `obs_raw` may be absent from the stated row denominator. Unknown periods do not fail completion. Current receipts say zero malformed/unknown periods, which does not establish no pre-loader rejection.
5. **Export safety and accounting:** export removes old series files before writing replacements, writes individual JSON nonatomically, and only later writes catalog/index. An interruption can leave partial output. Duplicate catalog IDs, invalid catalog rows exported by the less strict `catalog_flat` query, and raw observations missing catalog entries can make “series”/“observations” differ from unique saved content. DuckDB and other unexpected exceptions are not all explicitly caught by `process_program`; a crash or external kill may leave no final run record.
6. **Resource and provenance scaling:** `sniff_encoding` and `read_header` call `read_bytes()` before slicing, loading an entire file into memory. CA's catalog alone is 4.51 GB. Index generation also builds an in-memory dictionary and a 6,607,316,774-byte JSON index. CA took 148,599 seconds (about 41.3 hours) in the run log. Per-observation source file/row and interpreted missing/status fields exist internally but are not exported; footnote definitions are loaded but not attached. These increase resource use and limit later diagnosis. They do not prove a particular run crashed from memory exhaustion.

## Limits and next steps

No full rerun, exhaustive physical series recount, huge CA index parse, large-file rehash, complete BD/CA independent distinct-key count, or provider-wide observation denominator was attempted. The only necessary exhaustive filesystem walk is the requested total disk-usage measurement. Most programs have no local data to inspect. Official listing names/sizes and prose provide independent evidence, but do not independently validate every measurement, geographic combination, date, suppression, numeric value, or source revision. Missing tokens are retained as raw strings (AP 457; BD 3,288; BG 341; BP 84 according to receipts); absence of numeric values is not automatically a collection failure.

Prioritized work for the supplying group:

1. Persist per-file receipts even on failure and durable per-program started/failed/finished states; recover the CB HTTP diagnostics and explain the run interruption. Resume only with verified existing downloads, then finish the 60 not-reached programs in manageable batches.
2. Before scaling, preserve BG/BP status/continuity/footnote fields, support their legacy mapping formats, implement composite category joins, and retain dataset-specific biennial semantics. Clarify CA medical metadata with BLS.
3. Validate listing size and HTTP range semantics; validate existing outputs before whole-program skip; make export completion atomic. Gate `ok` on catalog IDs, unique saved IDs, raw/orphan counts, source-file inventory, rejected rows, and conflicts.
4. Add independent checks of full BD/CA keys and histories using streaming/columnar queries, not millions of JSON opens. Compare source-declared series dates with actual periods while distinguishing annual averages, suppressed values, and classification changes. Inspect auxiliary files such as TU aspect separately.
5. Replace millions of singleton JSON files and the monolithic CA index with a partitioned/columnar design after agreeing on downstream compatibility. Keep the current raw outputs until integrity and migration are verified.

## Small handoff and Git instructions

`.gitignore` preserves existing useful rules and now excludes raw data, every index, series directories, failed-program remnants, temporary files, caches, and credential locations. It explicitly permits **only** `data/run.json` and the ten reviewed catalog/download receipts. These contain public BLS URLs, hashes, byte counts, joins, counts, and the CB failure summary; no credentials were found in the selected contents. The run log is 2,217 bytes; catalogs are 3,092–9,844 bytes and download receipts 5,776–13,403 bytes. The selected pre-existing evidence totals 70,470 bytes. The 6.61 GB CA index, 7.66 MB BD index, and all raw/series data remain excluded. The audit summary is about 67 KB and its reproduction script about 5 KB.

Share exactly these 18 files (the original 15-file audit handoff plus three statistics files; existing scraper source/documentation is already in the repository):

```text
.gitignore
coverage_report.md
audit/check_local.py
audit/local_checks.json
scripts/data_statistics.py
audit/data_statistics.json
audit/data_statistics.csv
data/run.json
data/ap/catalog.json
data/ap/download.json
data/bd/catalog.json
data/bd/download.json
data/bg/catalog.json
data/bg/download.json
data/bp/catalog.json
data/bp/download.json
data/ca/catalog.json
data/ca/download.json
```

The inspected branch is `main`, with remote `origin` pointing to the supplying group's GitHub repository. No new branch is assumed. If `main` is your chosen branch, the following stages only the handoff paths (no `git add .`, no force-add). If you choose another existing branch, switch to it before staging and check the diff again.

```bash
git branch --show-current
git add -- .gitignore coverage_report.md audit/check_local.py audit/local_checks.json scripts/data_statistics.py audit/data_statistics.json audit/data_statistics.csv data/run.json data/ap/catalog.json data/ap/download.json data/bd/catalog.json data/bd/download.json data/bg/catalog.json data/bg/download.json data/bp/catalog.json data/bp/download.json data/ca/catalog.json data/ca/download.json
git diff --cached --stat
git diff --cached --check
git commit -m "Document BLS coverage audit and small reproducibility evidence"
git push -u origin HEAD
```

`HEAD` pushes the branch you actually chose under its current name. Review the staged file list before committing. These commands have not been executed.


## Full raw-data statistics follow-up (2026-11-08)

Ran `scripts/data_statistics.py` with installed DuckDB 2.0.0, four threads, and an 8 GB database memory limit, using temporary spill files under `/tmp`. Runtime was **261.55 seconds (about 4.4 minutes)**. Results are `audit/data_statistics.csv` (summary) and `audit/data_statistics.json` (histograms, thresholds, dates, hashes, storage inventory, and output samples). Reproduce with:

```bash
.venv/bin/python scripts/data_statistics.py
```

This analysis scans every downloaded standard raw observation file, independently computes SHA-256, skips byte-identical copies, and deduplicates overlapping rows by native series/year/period. CA's two observation files are byte-identical; so are CB's. Full raw-derived observation-count histograms match all five completed output indexes. The large CA index was scanned fully for its observation-count fields using the scraper's known line layout, without loading its entire JSON object or validating the complete JSON grammar. No malformed rows were found by this reader. A synthetic calculation check covered duplicated files, overlapping keys, invalid years, missing values, singletons, and index/output reconciliation.

| Dataset | Raw-derived series | Singletons | Median observations | Years | Export state |
|---|---:|---:|---:|---|---|
| AP | 1,483 | 2 | 210 | 1973–2026 | Completed |
| BD | 34,416 | 0 | 135 | 1992–2026 | Completed |
| BG | 25 | 0 | 24 | 1983–1995 | Completed |
| BP | 95 | 0 | 37 | 1983–1995 | Completed |
| CA | 52,439,022 | 52,439,022 | 1 | 2024 | Completed index; physical files sampled |
| CB | 53,142,968 | 53,142,968 | 1 | 2022 | Raw observations only; no completed export |

Across downloaded raw data there are **105,618,009 distinct dataset/series records** and **110,570,486 deduplicated observations**. The CB records are not additional completed output series. Among the five completed datasets, **99.9314% of indexed series are singletons**, dominated by CA. Excluding singleton series leaves **36,017 series**. Requiring at least 24 finite numeric observations leaves **35,933 series**. These are descriptive thresholds, not validated forecasting eligibility: annual averages may coexist with quarterly/monthly observations, and gap/comparability checks remain necessary. No filters were applied.

Bounded output checks read 256 files each from AP, BD, and CA, all 25 BG files, and all 95 BP files: **888 files**, with no missing sampled files, JSON parse failures, ID mismatches, or observation-count mismatches. CB's 256 selected paths were absent as expected because it was never exported. Sample IDs were chosen deterministically by DuckDB hash. This is not an exhaustive physical-file inventory or independent provider-content verification.

CA's sampled average JSON size is **1,266.24 bytes**. Extrapolating from 256 files gives **66.4 GB logical JSON contents**, or about **80.5 GB allocated space** from sampled filesystem blocks. Both are estimates, not measured totals or guaranteed reclaimable space. Separately measured logical sizes are **6.607 GB for CA's index** and **10.281 GB for all CA raw files**. The raw total includes both byte-identical observation copies and the series catalog. Total repository allocated usage remains the earlier completed `du -sh .` result of **97 GiB rounded**. No second exhaustive filesystem walk was made.

The follow-up does not establish a provider-wide observation denominator, inspect all individual JSON contents, fully parse the CA source series catalog, resolve conflicting duplicates, or test continuity across historical classification changes. Large observation-file hashes and full raw distinct-key counts are now checked; the initial audit's corresponding unverified statements describe the earlier, smaller assessment.
