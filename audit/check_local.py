"""Read existing BLS flat files and bounded series samples; no network or scraper calls.
Run from repository root: python3 audit/check_local.py > audit/local_checks.json
CA is limited to the first 32 rows per raw file; other raw observation files are read once.
"""
import csv,json,hashlib,itertools,collections,os
from pathlib import Path
D=Path('data'); out={}
def table(p):
 with p.open(encoding='utf-8-sig',newline='') as f:
  r=csv.reader(f,delimiter='\t'); h=[x.strip() for x in next(r)]
  while h and not h[-1]: h.pop()
  for a in r:
   if not a: continue
   yield dict(zip(h,[x.strip() for x in a]))
for prog in ['ap','bd','bg','bp','ca']:
 p=D/prog; card=json.loads((p/'catalog.json').read_text()); man=json.loads((p/'download.json').read_text())
 receipt=[]
 for r in man['files']:
  f=p/'raw'/r['relative_path']; size=f.stat().st_size if f.exists() else None
  receipt.append({'name':r['relative_path'],'listing_size':r['remote_size'],'record_size':r['local_size'],'actual_size':size,'status':r['download_status']})
 rows=list(table(p/'raw'/f'{prog}.series')) if prog!='ca' else list(itertools.islice(table(p/'raw'/'ca.series'),32))
 ids=[r['series_id'] for r in rows]; sampled=ids if prog!='bd' else ids[::max(1,len(ids)//100)]
 wanted=set(sampled); expected={s:{} for s in sampled}; conflicts=0; rawrows=0; orphan=set(); periods=collections.Counter(); flags={}; dims={k:len(set(r[k] for r in rows)) for k in rows[0] if k.endswith('_code') or k=='seasonal'}
 # Read AP/BD/BG/BP flat files once; bounded first 32 rows only for CA.
 files=sorted((p/'raw').glob(f'{prog}.data.*'),key=lambda f:(0 if any(t in f.name.lower() for t in ['allitems','alldata','allcesseries']) else 2 if 'current' in f.name.lower() else 1,f.name))
 for f in files:
  stream=table(f)
  if prog=='ca': stream=itertools.islice(stream,32)
  for r in stream:
   rawrows+=1; sid=r['series_id']; periods[r['period']]+=1
   for k in ['cont_break','status','footnote_exists']:
    if k in r: flags.setdefault(k,collections.Counter())[r[k]]+=1
   if sid in wanted:
    key=(r['year'],r['period']); val=(r['value'],r.get('footnote_codes',''))
    if key in expected[sid] and expected[sid][key]!=val: conflicts+=1
    expected[sid].setdefault(key,val)
 errors=[]; endpoint=[]; missing=[]; total=0; examples=[]
 for sid in sampled:
  f=p/'series'/f'{sid}.json'
  if not f.exists(): missing.append(sid); continue
  rec=json.loads(f.read_text()); obs=rec['observations']; total+=len(obs)
  got={(o['year'],o['period']):(o['value'],o.get('footnote_codes','')) for o in obs}
  if rec['series_id']!=sid or got!=expected[sid] or len(got)!=len(obs): errors.append(sid)
  if obs:
   first=min(got); last=max(got)
   if first!=(rec['begin_year'],rec['begin_period']) or last!=(rec['end_year'],rec['end_period']): endpoint.append({'id':sid,'catalog':[rec['begin_year'],rec['begin_period'],rec['end_year'],rec['end_period']],'observed':[first,last]})
  if len(examples)<2: examples.append({'id':sid,'observations':len(obs),'first':obs[0] if obs else None,'last':obs[-1] if obs else None})
 # independently reconcile full small indexes to catalog without directory traversal
 index=None
 if prog!='ca':
  idx=json.loads((p/'index.json').read_text()); index={'entries':len(idx),'observations':sum(x['observation_count'] for x in idx.values()),'missing_catalog_ids':len(set(ids)-idx.keys()),'extra_ids':len(idx.keys()-set(ids))}
 hashes={}
 for f in (p/'raw').iterdir():
  if f.is_file() and f.stat().st_size<10_000_000:
   hashes[f.name]=hashlib.sha256(f.read_bytes()).hexdigest()
 out[prog]={'coverage_record':card['coverage'],'validation_record':card['validation'],'receipts':receipt,'catalog_rows_checked':len(rows),'catalog_duplicate_ids':len(ids)-len(set(ids)),'dimension_distinct_codes':dims,'raw_rows_read':rawrows,'periods':dict(periods),'extra_observation_fields':{k:dict(v) for k,v in flags.items()},'output_files_checked':len(sampled),'output_observations_checked':total,'missing_output_ids':missing,'raw_output_mismatches':errors,'sample_duplicate_conflicts':conflicts,'endpoint_mismatches':endpoint,'index_check':index,'examples':examples,'small_raw_sha256':hashes}
print(json.dumps(out,indent=2))
