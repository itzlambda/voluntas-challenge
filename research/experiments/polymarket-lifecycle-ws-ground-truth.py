"""Post-window checks; run only after capture ends to preserve the global REST rate cap."""
import datetime as dt
import json
import sys
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path
creation_only='--creation-only' in sys.argv
run=Path(sys.argv[1]);meta=json.loads((run/'meta.json').read_text())
if not meta.get('observation_ended_at'):raise SystemExit('Capture must finish before post-window checks')
def stamp(s):return dt.datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
start=stamp(meta['started_at']);end=stamp(meta['observation_ended_at']);markets={};event_ids=set()
for line in (run/'rest.jsonl').open():
    r=json.loads(line)
    if r.get('status')==200:
        for m in json.loads(r['raw']).get('markets',[]):markets[str(m['id'])]=m
for file in run.glob('socket-*.jsonl'):
    for line in file.open():
        r=json.loads(line)
        if r.get('kind')!='received':continue
        try:p=json.loads(r['raw'])
        except ValueError:continue
        for x in p if isinstance(p,list) else [p]:
            if x.get('event_type')=='new_market':event_ids.add(str(x['id']))
last=0;seq=0
def get(base,params,kind,file):
    global last,seq
    time.sleep(max(0,last+.65-time.monotonic()));last=time.monotonic();seq+=1
    url=base+'?'+urllib.parse.urlencode(params+[('_research_poll',str(time.time_ns()))]);r=dict(kind=kind,sent_at=dt.datetime.now(dt.timezone.utc).isoformat(),url=url)
    try:
        response=subprocess.run(['curl','--silent','--show-error','--max-time','25','--write-out','\n%{http_code}',url],capture_output=True,text=True,check=True)
        body,status=response.stdout.rsplit('\n',1)
        r.update(status=int(status),raw=body)
    except Exception as e:r['error']=str(e)
    r['received_at']=dt.datetime.now(dt.timezone.utc).isoformat()
    file.write(json.dumps(r)+'\n');file.flush()
    return json.loads(r['raw']) if r.get('status')==200 else None
# Recheck all event IDs, including previously missing or initially ineligible records.
with (run/'rest.jsonl').open('a') as f:
    if not creation_only:
        todo=sorted(event_ids,key=int)
        for n in range(0,len(todo),100):
            p=get('https://gamma-api.polymarket.com/markets/keyset',[('closed','any'),('version','v1'),('version','v2'),('limit','100')]+[('id',i) for i in todo[n:n+100]],'post-targeted',f)
            if p:
                for m in p.get('markets',[]):markets[str(m['id'])]=m
        for i in sorted(event_ids-set(markets),key=int):
            p=get('https://gamma-api.polymarket.com/markets/'+i,[],'post-direct',f)
            if p and p.get('id'):markets[str(p['id'])]=p
    # Independent createdAt ordering checks the ID-based poll's observed denominator.
    for version in ['v1','v2']:
        for closed_status in ['false','true']:
            cursor=None
            for page in range(100):
                params=[('version',version),('closed',closed_status),('order','createdAt,id'),('ascending','false'),('limit','100')]
                if cursor:params.append(('after_cursor',cursor))
                p=get('https://gamma-api.polymarket.com/markets/keyset',params,'post-created-'+version+'-'+closed_status,f)
                if not p:break
                a=p.get('markets',[])
                for m in a:markets[str(m['id'])]=m
                cursor=p.get('next_cursor')
                if not a or not cursor or all(stamp(m['createdAt'])<start-120 for m in a):break
if creation_only:
    print(json.dumps(dict(creation_check_requests=seq)))
    sys.exit(0)
closed=[m for m in markets.values() if m.get('version')=='v1' and m.get('closedTime') and start<=stamp(m['closedTime'])<=end]
with (run/'tokens.jsonl').open('a') as f:
    for n in range(0,len(closed),10):
        conditions=','.join(m['conditionId'] for m in closed[n:n+10])
        get('https://data-api.polymarket.com/v2/tokens',[('condition',conditions)],'tokens-resolved',f)
with (run/'resolutions.jsonl').open('a') as f:
    for n in range(0,len(closed),10):
        conditions=','.join(m['conditionId'] for m in closed[n:n+10])
        get('https://data-api.polymarket.com/v2/resolutions',[('condition',conditions)],'resolution-state',f)
print(json.dumps(dict(event_ids=len(event_ids),closed_markets=len(closed),post_requests=seq)))
