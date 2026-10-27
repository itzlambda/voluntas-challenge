"""Analyze raw frames without treating a post-subscribe book as an acknowledgement."""
import collections, json, pathlib, sys, statistics
from decimal import Decimal
root=pathlib.Path(sys.argv[1]); meta=json.loads((root/'metadata.json').read_text())
connections=collections.defaultdict(list)
for directory in [root]+[pathlib.Path(p) for p in sys.argv[3:]]:
 for line in (directory/'frames.jsonl').open():
  r=json.loads(line)
  if r['connection']!='rest': connections[str(directory.name)+'/'+r['connection']].append(r)
def events(r):
 if r['kind']!='received': return []
 try: x=json.loads(r['raw'])
 except (ValueError,TypeError): return []
 return x if isinstance(x,list) else [x]
def assets(e):
 return [c['asset_id'] for c in e.get('price_changes',[])] if e.get('event_type')=='price_change' else [e.get('asset_id')]
def latency(values):
 if not values:return None
 v=sorted(values);return {'min':v[0],'median':statistics.median(v),'p95':v[min(len(v)-1,int(len(v)*.95))],'max':v[-1]}
trials=[]; heart=[]; candidate=[]
counts=collections.Counter(); sides={s:collections.Counter() for s in ['bids','asks']}; rollback=[]; samples=[]; timestamps={}; pair=collections.Counter();bulk_pair=collections.Counter();book_assets=set();conditions=set()
for conn,rows in connections.items():
 start=next((r for r in rows if r['kind']=='trial_start'),None)
 if start:
  sub=next(r for r in rows if r.get('action')=='subscribe');unsub=next((r for r in rows if r.get('action')=='unsubscribe'),None);asset=start['asset']
  books=[r for r in rows if r['mono_ms']>=sub['mono_ms'] and any(e.get('event_type')=='book' and e.get('asset_id')==asset for e in events(r))]
  first=books[0]['mono_ms'] if books else float('inf')
  between=[(r,e) for r in rows if unsub and unsub['mono_ms']<r['mono_ms']<first for e in events(r) if asset in assets(e)]
  trials.append({**{k:start[k] for k in ['group','test','trial','asset','prior_price_changes']},'connection':conn,'fresh_book_observed':bool(books),'first_book_latency_ms':first-sub['mono_ms'] if books else None,'intervening_events':dict(collections.Counter(e.get('event_type') for r,e in between)),'before_subscribe_events':sum(r['mono_ms']<sub['mono_ms'] for r,e in between)})
 if '/no-ping-' in conn:
  opened=next(r for r in rows if r['kind']=='opened');end=next((r for r in rows if r['kind']=='observation_end'),None);close=next((r for r in rows if r['kind']=='closed'),None)
  heart.append({'connection':conn,'observation_ms':(close or end or rows[-1])['mono_ms']-opened['mono_ms'],'remote_or_transport_closed':bool(close and (not end or close['mono_ms']<end['mono_ms'])),'close_code':close['code'] if close else None,'received_frames':sum(r['kind']=='received' for r in rows)})
 if '/empty-candidate-' in conn:
  books=[e for r in rows for e in events(r) if e.get('event_type')=='book'];candidate.append({'connection':conn,'books':len(books),'empty_both':sum(not e['bids'] and not e['asks'] for e in books),'empty_one_or_both':sum(not e['bids'] or not e['asks'] for e in books),'asset_ids':list(dict.fromkeys(e['asset_id'] for e in books))})
 for r in rows:
  es=events(r);bs=[e for e in es if e.get('event_type')=='book']
  if bs:
   for market,number in collections.Counter(e['market'] for e in bs).items():pair[str(number)]+=1
   if any(tag in conn for tag in ['/bulk-', '/activity-screen-']):
    for number in collections.Counter(e['market'] for e in bs).values():bulk_pair[str(number)]+=1
  for e in es:
   typ=e.get('event_type','unknown');counts[typ]+=1
   if typ=='price_change':
    for asset in assets(e): timestamps[(conn,asset)]=max(timestamps.get((conn,asset),0),int(e['timestamp']))
   if typ!='book':continue
   asset=e['asset_id'];book_assets.add(asset);conditions.add(e['market']);ts=int(e['timestamp']);prior=timestamps.get((conn,asset))
   if prior is not None and ts<prior:
    if len(rollback)<20:rollback.append({'connection':conn,'asset_id':asset,'book_timestamp':ts,'prior_price_change_timestamp':prior,'received_at':r['received_at']})
    counts['book_older_than_prior_price_change']+=1
   if len(samples)<5:samples.append({'asset_id':asset,'market':e['market'],'timestamp':e['timestamp']})
   for side in sides:
    levels=e[side];ps=[Decimal(x['price']) for x in levels];c=sides[side];c['books']+=1;c['levels']+=len(ps)
    c['ascending']+=ps==sorted(ps);c['descending']+=ps==sorted(ps,reverse=True);c['fewer_than_two_levels']+=len(ps)<2
    c['duplicate_prices']+=len(ps)!=len(set(ps));c['zero_sizes']+=sum(Decimal(x['size'])==0 for x in levels)
    for field in ['price','size']:c[field+'_over_6_decimals']+=sum(len(str(x[field]).split('.')[1])>6 if '.' in str(x[field]) else False for x in levels)
groups={}
for group in ['busy','quiet']:
 for case in ['new','duplicate','back_to_back','gap_1s','flow']:
  t=[x for x in trials if x['group']==group and x['test']==case];groups[group+'/'+case]={'trials':len(t),'with_book':sum(x['fresh_book_observed'] for x in t),'latency_ms':latency([x['first_book_latency_ms'] for x in t if x['fresh_book_observed']]),'with_intervening_events':sum(bool(x['intervening_events']) for x in t),'with_prior_updates':sum(x['prior_price_changes']>0 for x in t)}
result={'captures':[{'directory':str(d),'started_at':m['started_at'],'finished_at':m.get('finished_at'),'eligible_markets':m['eligible_count']} for d in [root]+[pathlib.Path(p) for p in sys.argv[3:]] for m in [json.loads((d/'metadata.json').read_text())]],'started_at':meta['started_at'],'finished_at':meta.get('finished_at'),'raw_directories':[str(root)]+sys.argv[3:],'eligible_markets':meta['eligible_count'],'groups':groups,'trials':trials,'heartbeat':heart,'candidate_metadata':[{'id':m['id'],'acceptingOrders':m['acceptingOrders'],'enableOrderBook':m['enableOrderBook'],'asset_ids':json.loads(m['clobTokenIds'])} for m in meta['candidates']],'candidate_trials':candidate,'event_counts':dict(counts),'book_format':{s:dict(c) for s,c in sides.items()},'book_assets':len(book_assets),'book_markets':len(conditions),'books_per_market_in_same_frame':dict(pair),'both_outcomes_subscribed_books_per_market_in_same_frame':dict(bulk_pair),'rollback_samples':rollback,'sample_books':samples,'market_groups':{g:[{'id':m['id'],'volume24hr':m.get('volume24hr'),'asset_ids':json.loads(m['clobTokenIds'])} for m in meta[g]] for g in ['busy','quiet']},'connection_errors':[{'connection':c,**r} for c,rs in connections.items() for r in rs if r['kind']=='error'],'closes':[{'connection':c,'code':r['code'],'reason':r['reason']} for c,rs in connections.items() for r in rs if r['kind']=='closed']}
out=pathlib.Path(sys.argv[2]);out.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:result[k] for k in ['groups','heartbeat','event_counts','book_format','book_assets','book_markets','books_per_market_in_same_frame']},indent=2))
