#!/usr/bin/env python3
"""Summarize existing BLS data with DuckDB. No downloads or changes to data/.

Run: .venv/bin/python scripts/data_statistics.py
Reads all raw observation files; uses bounded memory and /tmp spill space.
Writes compact JSON/CSV statistics under audit/; never traverses series directories.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import statistics
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import duckdb


def sql(value):
    return "'" + str(value).replace("'", "''") + "'"


def dict_rows(con, query):
    cur = con.execute(query)
    names = [x[0] for x in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def header(path):
    with path.open(encoding='utf-8-sig', newline='') as f:
        fields = [x.strip() for x in next(csv.reader(f, delimiter='\t'))]
    while fields and not fields[-1]:
        fields.pop()
    return fields


def sha256(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def index_stats(con, path):
    if not path.exists():
        return None
    if path.stat().st_size < 20_000_000:
        records = json.loads(path.read_text())
        hist = Counter(int(v['observation_count']) for v in records.values())
        method = 'full JSON parse'
    else:
        # The scraper writes one observation_count field per index entry.
        # Read every line without materializing the enormous top-level JSON object.
        # This validates counts in that known layout, not the whole JSON grammar.
        source = (f"read_csv({sql(path)}, header=false, columns={{'line':'VARCHAR'}}, "
                  "delim='\\x1f', quote='', escape='', auto_detect=false, strict_mode=true)")
        counts = dict_rows(con, f"""
            SELECT CAST(regexp_extract(line, '"observation_count": ([0-9]+)', 1) AS BIGINT) n,
                   count(*) series FROM {source}
            WHERE regexp_matches(line, '^    "observation_count": [0-9]+[,]?$')
            GROUP BY 1 ORDER BY 1
        """)
        hist = Counter({r['n']: r['series'] for r in counts})
        method = 'full line scan of scraper index layout; not full JSON validation'
    n = sum(hist.values())
    return {'method': method, 'series': n,
            'observations': sum(k * v for k, v in hist.items()),
            'zero_observation_series': hist[0], 'singleton_series': hist[1],
            'observation_count_histogram': [{'observations': k, 'series': v}
                                             for k, v in sorted(hist.items())]}


def analyze(con, directory, run_status, sample_size):
    started = time.monotonic()
    program = directory.name.upper()
    print(f'[{program}] inventory and hash raw observation files', flush=True)
    raw_files = sorted((directory / 'raw').glob(f'{directory.name}.data.*'))
    raw_inventory, unique_files, seen = [], [], {}
    for path in raw_files:
        digest = sha256(path)
        duplicate_of = seen.get(digest)
        raw_inventory.append({'name': path.name, 'bytes': path.stat().st_size,
                              'sha256': digest, 'byte_identical_to': duplicate_of})
        if duplicate_of is None:
            seen[digest] = path.name
            unique_files.append(path)
    rank = lambda p: 0 if any(x in p.name.lower() for x in ('alldata','allitems','allcesseries')) else 2 if 'current' in p.name.lower() else 1
    parts = []
    for path in sorted(unique_files, key=lambda p: (rank(p), p.name)):
        fields = header(path)
        positions = {c.lower(): i + 1 for i, c in enumerate(fields)}
        for required in ('series_id','year','period','value'):
            if required not in positions:
                raise ValueError(f'{path}: missing {required}')
        get = lambda name: f'trim(f[{positions[name]}])'
        fn = get('footnote_codes') if 'footnote_codes' in positions else "''"
        extra = [c for c in fields if c.lower() not in ('series_id','year','period','value','footnote_codes')]
        source = (f"read_csv({sql(path)}, header=false, columns={{'line':'VARCHAR'}}, "
                  "delim='\\x1f', quote='', escape='', auto_detect=false, strict_mode=true)")
        parts.append(f"""
            SELECT {get('series_id')} sid, try_cast({get('year')} AS INTEGER) y,
                   {get('period')} p, {get('value')} value,
                   {fn} footnote, {rank(path)} file_rank, {sql(path.name)} source_file,
                   rec source_row, len(f) n_fields,
                   len(list_filter(f[{len(fields)}+1:], lambda x: trim(x) <> '')) extra_fields,
                   {sql(json.dumps(extra))} additional_source_columns
            FROM (SELECT rec, string_split(line, chr(9)) f
                  FROM {source} WITH ORDINALITY t(line,rec) WHERE rec > 1)
        """)
    if not parts:
        return {'program': program, 'run_status': run_status, 'error': 'no raw observation files'}
    con.execute('CREATE OR REPLACE VIEW raw_obs AS ' + ' UNION ALL '.join(parts))
    print(f'[{program}] scan, validate, and deduplicate {len(unique_files)} distinct raw files', flush=True)
    con.execute("""CREATE OR REPLACE TABLE clean AS
        SELECT sid,y,p,coalesce(value,'') value,coalesce(footnote,'') footnote,
               count(*) OVER (PARTITION BY sid,y,p) key_rows
        FROM raw_obs
        WHERE sid IS NOT NULL AND sid<>'' AND y IS NOT NULL AND p IS NOT NULL AND p<>''
          AND n_fields>=4 AND extra_fields=0
        QUALIFY row_number() OVER (
            PARTITION BY sid,y,p ORDER BY file_rank,source_file,source_row)=1
    """)
    # Separate raw aggregation exposes invalid records rather than silently ignoring them.
    raw_quality = dict_rows(con, """SELECT count(*) rows_read,
        count(*) FILTER (WHERE sid IS NULL OR sid='' OR y IS NULL OR p IS NULL OR p=''
                          OR n_fields<4 OR extra_fields>0) invalid_rows
        FROM raw_obs""")[0]
    print(f'[{program}] aggregate per-series histories', flush=True)
    con.execute("""CREATE OR REPLACE TABLE series_stats AS
        SELECT sid,count(*) n_observations,count(DISTINCT y) n_years,
               min(y) first_year,max(y) last_year,
               sum(CASE WHEN try_cast(replace(value,',','') AS DOUBLE) IS NOT NULL
                         AND isfinite(try_cast(replace(value,',','') AS DOUBLE)) THEN 1 ELSE 0 END) numeric_observations,
               sum(CASE WHEN value IN ('','-','--','.','NA','N/A','(NA)','null') THEN 1 ELSE 0 END) missing_tokens
        FROM clean GROUP BY sid""")
    summary = dict_rows(con, """SELECT count(*) series,sum(n_observations) observations,
        count(*) FILTER(WHERE n_observations=1) singleton_series,
        count(*) FILTER(WHERE n_years=1) single_year_series,
        count(*) FILTER(WHERE numeric_observations=0) series_without_numeric_values,
        count(*) FILTER(WHERE numeric_observations=1) series_with_one_numeric_value,
        min(n_observations) min_observations,median(n_observations) median_observations,
        avg(n_observations) mean_observations,max(n_observations) max_observations,
        quantile_disc(n_observations,0.1) p10_observations,
        quantile_disc(n_observations,0.9) p90_observations,
        min(first_year) first_year,max(last_year) last_year,
        median(last_year-first_year+1) median_year_span,
        sum(numeric_observations) numeric_observations,sum(missing_tokens) missing_tokens
        FROM series_stats""")[0]
    thresholds = dict_rows(con, """SELECT t.minimum_observations,
        count(*) FILTER(WHERE s.n_observations>=t.minimum_observations) series,
        count(*) FILTER(WHERE s.numeric_observations>=t.minimum_observations) series_with_enough_numeric_values
        FROM series_stats s CROSS JOIN (VALUES (2),(12),(24),(60),(120)) t(minimum_observations)
        GROUP BY 1 ORDER BY 1""")
    lengths = dict_rows(con, 'SELECT n_observations observations,count(*) series FROM series_stats GROUP BY 1 ORDER BY 1')
    years = dict_rows(con, 'SELECT y AS "year",count(*) observations FROM clean GROUP BY 1 ORDER BY 1')
    periods = dict_rows(con, 'SELECT p period,count(*) observations FROM clean GROUP BY 1 ORDER BY 1')
    sample_ids = [r[0] for r in con.execute(f'SELECT sid FROM series_stats ORDER BY hash(sid),sid LIMIT {sample_size}').fetchall()]
    sample = {'selection': 'lowest DuckDB hash(series_id), deterministic for this DuckDB version',
              'requested': len(sample_ids), 'found': 0, 'missing': 0, 'invalid_json': 0,
              'count_mismatches': 0, 'logical_bytes': 0, 'allocated_bytes': 0}
    count_map = dict(con.execute(f'SELECT sid,n_observations FROM series_stats WHERE sid IN ({",".join(sql(s) for s in sample_ids)})').fetchall())
    sizes = []
    for sid in sample_ids:
        path = directory / 'series' / f'{sid}.json'
        if not path.exists():
            sample['missing'] += 1
            continue
        stat = path.stat()
        sample['found'] += 1
        sample['logical_bytes'] += stat.st_size
        sample['allocated_bytes'] += stat.st_blocks * 512
        sizes.append(stat.st_size)
        try:
            record = json.loads(path.read_text())
            if record.get('series_id') != sid or len(record['observations']) != count_map[sid]:
                sample['count_mismatches'] += 1
        except (ValueError, KeyError, TypeError):
            sample['invalid_json'] += 1
    if sizes:
        sample['mean_file_bytes'] = statistics.mean(sizes)
        sample['median_file_bytes'] = statistics.median(sizes)
        sample['estimated_all_series_logical_bytes'] = statistics.mean(sizes) * summary['series']
        sample['estimated_all_series_allocated_bytes'] = sample['allocated_bytes'] / len(sizes) * summary['series']
        sample['estimate_warning'] = 'Sample extrapolation, not measured total; no confidence interval; physical output count unverified.'
    print(f'[{program}] scan output index', flush=True)
    index = index_stats(con, directory / 'index.json')
    catalog = json.loads((directory/'catalog.json').read_text()) if (directory/'catalog.json').exists() else None
    inventory = [p for p in directory.iterdir() if p.is_file()]
    raw_paths = [p for p in (directory/'raw').iterdir() if p.is_file()]
    result = {'program': program, 'run_status': run_status, 'summary': summary,
              'raw_quality': raw_quality, 'raw_files': raw_inventory,
              'byte_identical_files_omitted_from_analysis': len(raw_files)-len(unique_files),
              'duplicate_rows_removed_after_identical_file_elimination': con.execute('SELECT sum(key_rows-1) FROM clean').fetchone()[0],
              'observation_count_histogram': lengths, 'length_thresholds': thresholds,
              'year_counts': years, 'period_counts': periods, 'output_index': index,
              'catalog_coverage_record': catalog.get('coverage') if catalog else None,
              'output_sample': sample,
              'storage': {'raw_logical_bytes': sum(p.stat().st_size for p in raw_paths),
                          'index_logical_bytes': (directory/'index.json').stat().st_size if (directory/'index.json').exists() else 0,
                          'top_level_file_logical_bytes': sum(p.stat().st_size for p in inventory)},
              'elapsed_s': round(time.monotonic()-started,2)}
    con.execute('DROP TABLE clean')
    con.execute('DROP TABLE series_stats')
    print(f'[{program}] done: {summary["series"]:,} raw-derived series; {summary["singleton_series"]:,} singletons', flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, default=Path('data'))
    parser.add_argument('--output-dir', type=Path, default=Path('audit'))
    parser.add_argument('--memory-limit', default='8GB')
    parser.add_argument('--threads', type=int, default=4)
    parser.add_argument('--sample-size', type=int, default=256)
    args = parser.parse_args()
    if args.sample_size < 1 or args.threads < 1:
        parser.error('sample-size and threads must be positive')
    args.output_dir.mkdir(parents=True,exist_ok=True)
    started = time.monotonic()
    run = json.loads((args.data_dir/'run.json').read_text())
    status = {r['program']:r['status'] for r in run['catalogs']}
    report = {'created_utc':datetime.now(timezone.utc).isoformat(),'duckdb_version':duckdb.__version__,
              'method': 'Full local raw-table scans, SHA-256 exact-file deduplication, deduplication by native series/year/period, full index count scans, bounded output samples.',
              'limitations': ['Local downloaded data only; no provider denominator or new requests.',
                              'Output indexes are inventories, not proof every physical series file exists.',
                              'No full traversal or exact measurement of individual series-file storage.',
                              'History span includes gaps; numeric counts include annual averages and are not frequency-specific training eligibility.',
                              'Conflicting raw values use scraper file-priority ordering; conflict counts and continuity gaps are not assessed.'],
              'datasets': []}
    with tempfile.TemporaryDirectory(prefix='bls-stats-',dir='/tmp') as temp:
        con=duckdb.connect()
        con.execute(f'SET memory_limit={sql(args.memory_limit)}')
        con.execute(f'SET threads={args.threads}')
        con.execute(f'SET temp_directory={sql(temp)}')
        con.execute('SET preserve_insertion_order=false')
        try:
            for directory in sorted(args.data_dir.iterdir()):
                if directory.is_dir() and (directory/'raw').is_dir():
                    report['datasets'].append(analyze(con,directory,status.get(directory.name.upper(),'unrecorded'),args.sample_size))
                    (args.output_dir/'data_statistics.json').write_text(json.dumps(report,indent=2))
        finally:
            con.close()
    report['elapsed_s']=round(time.monotonic()-started,2)
    rows=[]
    for d in report['datasets']:
        row={'program':d['program'],'run_status':d['run_status'],**d.get('summary',{}),**d.get('storage',{})}
        row['output_index_series']=d['output_index']['series'] if d.get('output_index') else None
        row['singleton_percent']=100*row['singleton_series']/row['series'] if row.get('series') else None
        rows.append(row)
    fields=list(dict.fromkeys(k for row in rows for k in row))
    with (args.output_dir/'data_statistics.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(rows)
    (args.output_dir/'data_statistics.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(rows,indent=2))

if __name__=='__main__':
    main()
