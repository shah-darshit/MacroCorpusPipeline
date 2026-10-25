# Source Inventory — BLS LABSTAT (Phase 0, live-verified)

**Probe date:** 2026-10-19 (UTC) · **Method:** direct HTTPS requests to
`download.bls.gov` with a descriptive research User-Agent; small files downloaded
in full, large files stream-sampled (first ~20 KB) and header-inspected.
All statements below are from observed bytes/headers, not filename inference.

## 1. Access behaviour (critical)

| Request | Result |
|---|---|
| `curl` default UA → `https://download.bls.gov/pub/time.series/` | **403 Akamai "Access Denied … bot activity"** |
| Descriptive UA (`Mozilla/5.0 … BLS-Corpus-Research/1.0 … bls-corpus/0.1`) | **200**, full IIS directory listing |
| `https://download.bls.gov/robots.txt` | 404 (IIS error page) — no robots policy published at that path; BLS bot policy is enforced at the CDN edge instead. Conservative pacing + descriptive UA is the compliance mechanism. |
| `https://www.bls.gov/data/`, `https://www.bls.gov/developers/` | 200 |
| `https://api.bls.gov/publicAPI/v2/...` (no key) | 200 — API reachable for optional spot-checks only |

**Implication:** every request from this pipeline sends a configurable, descriptive
`User-Agent`; a bare-library UA risks a hard 403 that must surface as a permanent error.

## 2. Directory structure

- Root: `https://download.bls.gov/pub/time.series/` — one lower-case two-letter
  directory per survey (`ap/ bd/ cu/ …`, ~65 dirs) plus `overview.txt` (5 929 B),
  `compressed/`, `sdmx/`, `esbr/`.
- Listing format: classic IIS HTML — `<pre>` lines of
  `M/D/YYYY  H:MM AM|PM  <size>|<dir>  <A HREF="/pub/time.series/bd/bd.series">bd.series</A>`.
  Sizes and modification dates **are** exposed in the listing; per-file `HEAD` adds
  `Content-Length`, `Last-Modified`, `ETag`, `Accept-Ranges: bytes`.
- Survey abbreviation for Business Employment Dynamics is **`BD`** (overview.txt §
  "LIST OF DATABASES": `BD Business Employment Dynamics`). There is **no `bed/`
  directory** (404 verified). Series IDs begin `BD` + seasonal char (`BDS…`, `BDU…`).

## 3. The BD directory — complete discovered inventory (18 files)

Remote state at probe: most files `Last-Modified: Wed, 29 Jul 2026 14:00:00 GMT`.

| File | Bytes | Role (this pipeline) | Required |
|---|---:|---|---|
| `bd.data.1.AllItems` | 253 477 712 | observations — complete history (1992Q3→) | **yes** |
| `bd.data.0.Current` | 197 581 980 | observations — overlapping subset (2000Q1→ observed) | **yes** |
| `bd.series` | 5 611 545 | series catalog (34 464 series, counted live) | **yes** |
| `bd.footnote` | 153 | footnote definitions (2 codes) | yes |
| `bd.industry` | 4 452 | mapping: `industry_code → name` (+display_level/selectable/sort_sequence) | yes |
| `bd.state` | 771 | mapping: `state_code → name` (FIPS, leading zeros) | yes |
| `bd.sizeclass` | 570 | mapping: `sizeclass_code → name` | yes |
| `bd.dataclass` | 267 | mapping: `dataclass_code → name` (+extra cols) | yes |
| `bd.contacts` | 209 | documentation (plain text, **no header row**) | no |
| `bd.txt` | 17 479 | documentation (database definition, §1–§7) | no |
| `bd.county` | 39 | mapping (single row: `000 National` — BD publishes no county data) | yes |
| `bd.dataelement` | 77 | mapping (`1 Employment`, `2 Number of Establishments`) | yes |
| `bd.msa` | 35 | mapping (single row `00000 National`) | yes |
| `bd.ownership` | 49 | mapping (`5 Private Sector`) | yes |
| `bd.periodicity` | 58 | mapping (`Q Quarterly`, `A Annual`) | yes |
| `bd.ratelevel` | 48 | mapping (`L Level`, `R Rate`) | yes |
| `bd.seasonal` | 79 | mapping (`S Seasonally adjusted`, `U Not seasonally adjusted`) | yes |
| `bd.unitanalysis` | 54 | mapping (`1 Establishment`) | yes |

Total raw payload ≈ **456.7 MB**. No file in the directory was unrecognized at probe
time; the classifier still records any future unknown file as `unclassified`.

## 4. Observed schemas (from actual bytes)

Common traits: **ASCII, tab-delimited, CRLF line endings, header row present**,
string fields whitespace-padded to fixed lengths (e.g. header cell
`'series_id                     '`). Parsers must strip padding per-field and never
split naively on whitespace (titles contain spaces).

### 4.1 `bd.series` — 19 columns, 34 464 data rows (live count)

```text
series_id(30, padded; 28-char ID + 2 spaces)  seasonal(S|U)  msa_code  state_code
county_code  industry_code  unitanalysis_code  dataelement_code  sizeclass_code
dataclass_code  ratelevel_code(L|R)  periodicity_code(Q|A)  ownership_code
series_title  footnote_codes  begin_year  begin_period  end_year  end_period
```

Observed: seasonal `S`=16 404 / `U`=18 060; `footnote_codes` empty for 29 624 series,
`1` for 4 840; begin 1992/Q03 onward. Series-ID decomposition is documented in
`bd.txt` §4 (positions map 1:1 onto catalog code columns).

### 4.2 `bd.data.*` — 5 columns

```text
series_id(padded)  year(YYYY)  period(Q01–Q05)  value(right-padded numeric)  footnote_codes
```

Sample: `BDS0000000000000000110001LQ5  \t2000\tQ01\t        9004\t1`.
`bd.data.1.AllItems` starts 1992Q3; `bd.data.0.Current` starts 2000Q1 in observed
heads — **the partitions overlap**, so `(series_id, year, period)` is *not* unique
across the raw corpus. Values observed are integers (levels, rounded to thousands
per titles) and rates; non-numeric/suppressed representations were **not** observed
in sampled heads — the parser nonetheless preserves any non-numeric token in
`value_raw` with `value_numeric = null` and reports it (defensive, per task §D).

### 4.3 Mapping files — 2–5 columns

`code, name` plus, for `dataclass`/`industry`, `display_level, selectable,
sort_sequence`. Codes are zero-padded strings (`06`, `000000`, `01`) — must never
be coerced to integers. `bd.footnote`: `footnote_code, footnote_text`
(`1` = "Total private includes unclassified sector…", `2` = administrative event).

### 4.4 Documentation files

`bd.txt` (7 sections: definition, file list, series/data/mapping formats, data-element
dictionary) and `bd.contacts` (no header; contact e-mail/phone). Both preserved raw;
neither is parsed as a table.

## 5. HTTP metadata quality

| Signal | Observed | Trust |
|---|---|---|
| `Content-Length` | Present & consistent on HEAD | Used for truncation check |
| `Last-Modified` | Present, per-file | Used for snapshot ID + conditional GET |
| `ETag` | Present, but **identical (`"070948b621fdd1:0"`) for every file sharing a Last-Modified** | Recorded; **not** a content hash — never trusted alone |
| `Accept-Ranges: bytes` | Advertised, but a `Range: 0-1999` probe returned **200 + full 5.6 MB body** | Range resume only after verified `206`; otherwise restart the file |
| Directory listing dates/sizes | Present, match HEAD values | Cross-checked in discovery |

## 6. Program selection

**Chosen: `BD` (Business Employment Dynamics).** Justification: (a) explicitly requested;
(b) representative standard LABSTAT layout (single series catalog, partitioned data
files, full mapping-file suite, footnotes) so the adapter pattern generalizes;
(c) quarterly frequency exercises non-monthly period semantics (`Q05` annual averages);
(d) 34 464 series / ~451 MB observations is large enough to prove chunked processing,
small enough for a complete auditable snapshot in one run; (e) documentation
(`bd.txt`) fully specifies every field — no schema guessing.
`CU` (CPI) remains the natural second adapter (monthly `M01–M13`, area/item mappings).

## 7. Exception programs named in the task (verified)

| Prefix | LABSTAT dir? | Finding |
|---|---|---|
| `PF` | **404** | No bulk directory under that prefix; alternate source must be investigated before any claim |
| `PI` | **404** | Same |
| `LF` | **404** | Same |
| `KV` | **200 — exists** | Full bulk set present (`kv.series`, `kv.data.1.AllData`, 12 mapping files…). The "API-only exception" assumption is **stale for KV** |

## 8. Known source limitations

1. **Non-atomic snapshots:** files may update mid-crawl; mitigated by recording
   per-file validators + SHA-256, never eliminated.
2. Overlapping data partitions (see §4.2) require the documented precedence rule.
3. `bd.txt` field examples are stale in places (end_year example 2021 vs live 2025) —
   live bytes win over prose.
4. Annual (`periodicity_code=A`) series are published as quarterly-spaced points
   (e.g. March-to-March) per `bd.periodicity` text; only `period=Q05` rows are
   annual averages. The two must not be conflated.
