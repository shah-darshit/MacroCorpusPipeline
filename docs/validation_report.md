# Validation Report — BD (Business Employment Dynamics), live snapshot

**Snapshot:** `bd-20260729T100000Z-e302ea2f` · **Source:** https://download.bls.gov/pub/time.series/bd/
**Runs:** initial run, idempotent rerun, and API spot-check executed 2026-10-19 (UTC).
Machine-readable twin: `data/processed/bd-20260729T100000Z-e302ea2f/validation-bd-20260729T100000Z-e302ea2f.json`
(regenerated on every `validate`; numbers below are copied from it).

## Coverage

| Check | Result |
|---|---|
| Files discovered in directory listing | 18 |
| Downloaded / skipped (rerun, 304) / failed | 18 / 18 / 0 |
| Unrecognized files | 0 |
| SHA-256 recorded for every file; checksum failures | yes; 0 |
| Series in catalog | 34,464 |
| Series with ≥1 observation | 34,464 (100%) |
| Catalog series without observations | 0 |
| Observation series absent from catalog | 0 |
| Observations written | 4,598,544 |
| Year / period coverage | 1992–2025; Q01–Q04 (no Q05 annual-average rows present in this vintage) |
| Frequency split (catalog periodicity) | quarterly 32,808; annual 1,656 |
| Seasonal split | S 16,404; U 18,060 |

## Deduplication evidence (partition overlap)

`bd.data.1.AllItems` (1992Q3→) and `bd.data.0.Current` (2000Q1→) overlap by design:

| | Rows |
|---|---|
| Rows read (both files) | 8,182,800 |
| Identical duplicates removed | 3,584,256 |
| **Conflicting duplicates (same key, different value/footnote)** | **0** |
| Rows written | 4,598,544 |

Rule applied: AllItems outranks Current; any conflict would keep the AllItems
value and be sampled in this report. Zero conflicts in this vintage means the
two partitions are consistent extracts of the same underlying data.

## Data-quality findings

| Finding | Count | Handling |
|---|---|---|
| Malformed rows (wrong column count, bad year, empty key) | 0 | counted + sampled when present |
| Non-numeric values | 0 | preserved in `value_raw`, `value_numeric=null` |
| Missing/empty values | 3,288 | `value_status=missing_token`, retained as rows |
| Unknown period codes | 0 | dates left null + reported when present |
| Unresolved footnote references | 0 | codes used {1, 2}, both defined in `bd.footnote` |
| Empty files | 0 | — |

## API spot-check (opt-in, bulk remains source of truth)

`bls-corpus validate-api --program BD --sample-size 5` (deterministic seed):
**5 series, 60 overlapping observations compared, 60 matches, 0 discrepancies,
0 errors.** Note: this session ran unregistered (a secret added in the Cloud
Agent dashboard applies to new sessions, and none was present in this VM's
environment); BLS unregistered limits were respected with 0.5 s pacing.

## Idempotency / recovery evidence

- Rerun: `download counts={'skipped_not_modified': 18}` in 4.3 s — conditional
  GETs returned 304 for every file; zero bytes re-downloaded.
- Re-parse reproduced exactly 4,598,544 observations (deterministic atomic
  outputs; no duplicate accumulation).
- Interrupted-download, corrupt-file, changed-file, 429/`Retry-After`, 500,
  truncated-body, and 206-vs-200 Range behaviours are covered by offline tests
  (27 tests, all passing, `python3 -m pytest`).

## Limitations

1. The snapshot is a sequential crawl, not an atomic upstream transaction
   (mitigation: per-file validators + hashes in the manifest).
2. Server `ETag` is timestamp-derived and identical across co-updated files;
   it is recorded but never trusted as a content hash — SHA-256 is authoritative.
3. The CDN advertises `Accept-Ranges` but ignored a Range probe (200 + full
   body), and mislabels some plain bodies as gzip; the downloader forces
   `Accept-Encoding: identity` and only resumes on a proven `206`.
4. `measure`/`unit` are null for BD: the source documents Level/Rate and
   Employment/Establishments codes (preserved in `metadata_json`), not units.
5. Annual-frequency series (1,656) publish quarterly-spaced points per
   `bd.periodicity`; only `Q05` rows (none in this vintage) are annual averages.
6. An empty error list is not proof of completeness; completeness here rests on
   the discovered-inventory reconciliation above.
