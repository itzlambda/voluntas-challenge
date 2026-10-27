#!/usr/bin/env python3
"""Bounded, public-only resolution-tail measurement; Python standard library only."""
import argparse
import collections
import datetime as dt
import json
import pathlib
import re
import time
import urllib.error
import urllib.parse
import urllib.request

FIELDS = 'id slug conditionId questionID closed acceptingOrders acceptingOrdersTimestamp closedTime endDate umaEndDate umaResolutionStatus umaResolutionStatuses outcomePrices automaticallyResolved resolvedBy enableOrderBook version createdAt updatedAt customLiveness fallbackResolutionType'.split()
BASE = 'https://gamma-api.polymarket.com'
UTC = dt.timezone.utc

def timestamp(value):
    if not value:
        return None
    # Gamma sometimes appends a second timezone suffix, e.g. +00:00:00.
    value = re.sub(r'\+00:00:00$', '+00:00', value).replace('Z', '+00:00')
    try:
        return dt.datetime.fromisoformat(value).replace(tzinfo=UTC) if dt.datetime.fromisoformat(value).tzinfo is None else dt.datetime.fromisoformat(value)
    except ValueError:
        for fmt in ('%B %d, %Y', '%b %d, %Y'):
            try:
                return dt.datetime.strptime(value, fmt).replace(tzinfo=UTC)
            except ValueError:
                pass
    return None

class Client:
    def __init__(self):
        self.last = 0
        self.log = []
    def get(self, base, params):
        params = dict(params, _tail_probe=time.time_ns())
        url = base + '?' + urllib.parse.urlencode(params)
        for attempt in range(4):
            time.sleep(max(0, .4 - (time.monotonic() - self.last)))
            self.last = time.monotonic()
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Accept':'application/json'}), timeout=40) as response:
                    data = json.load(response)
                return data
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                code = getattr(exc, 'code', None)
                self.log.append({'endpoint': base, 'params': {k:v for k,v in params.items() if k != '_tail_probe'}, 'status': code, 'error': str(exc)})
                if code not in (429, 500, 502, 503, 504) and code is not None:
                    return None
                if attempt < 3:
                    retry = getattr(exc, 'headers', {}).get('Retry-After', '0')
                    time.sleep(max(2 ** (attempt + 1), float(retry) if str(retry).isdigit() else 0))
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pages', type=int, default=100, help='Maximum pages per broad sampling stratum (100 rows/page).')
    parser.add_argument('--historical-check', action='store_true', help='Add high-volume 2024-2026 historical resolution strata.')
    parser.add_argument('--tail-recheck', action='store_true', help='Recheck saved tail evidence and complete open proposal/dispute cohorts.')
    parser.add_argument('--enrich-only', action='store_true', help='Reverify candidates saved by a previous run, plus optional supplied snapshot.')
    parser.add_argument('--snapshot', help='Optional existing Gamma checkpoint with rows for independent cross-checking.')
    parser.add_argument('--output', default='research/experiments/polymarket-resolution-tail-results.json')
    args = parser.parse_args()
    client = Client()
    if args.historical_check:
        result = json.loads(pathlib.Path(args.output).read_text())
        historical_check(result, client)
        pathlib.Path(args.output).write_text(json.dumps(result,indent=2)+'\n')
        return
    if args.tail_recheck:
        result = json.loads(pathlib.Path(args.output).read_text())
        result['prior_verified_recent_sample'] = {k:v for k,v in result['verified_tail'].items() if k not in ('candidate_audit','resolution_api_errors')}
        rows = [x['market'] for x in result['verified_tail']['candidate_audit']] + [x['market'] for x in result['verified_tail']['top_20_actual_resolution_lags']] + result['no_status_or_price_resolution_evidence']['oldest_20']
        for status in ('proposed','disputed'):
            cursor = None
            for page in range(args.pages):
                params = {'version':'v1','closed':'false','uma_resolution_status':status,'limit':100}
                if cursor:
                    params['after_cursor'] = cursor
                data = client.get(BASE+'/markets/keyset', params)
                if data is None:
                    break
                rows += [{k:m[k] for k in FIELDS if k in m} for m in data['markets']]
                cursor = data.get('next_cursor')
                if not cursor:
                    break
        points = []
        for market_id in ('3358951','3358952','3358953','3358954','3358955','3358956','3358957'):
            m = client.get(BASE+'/markets/'+market_id,{})
            if m:
                points.append({k:m[k] for k in FIELDS if k in m})
        result['nonaccepting_open_point_checks'] = points
        rows += points
        enrich(result, rows, client, verify_open=True)
        pathlib.Path(args.output).write_text(json.dumps(result,indent=2)+'\n')
        return
    if args.enrich_only:
        result = json.loads(pathlib.Path(args.output).read_text())
        rows = result['nonresolved_status_candidates']['all_candidates'] + result['resolved_uma_end_proxy']['top_20'] + result['no_status_or_price_resolution_evidence']['oldest_20']
        if args.snapshot:
            snapshot = json.loads(pathlib.Path(args.snapshot).read_text())
            candidates = snapshot['rows']
            stride = max(1, len(candidates)//2000)
            rows += candidates[::stride][:2000]
            result['verification_snapshot_source'] = {'path':args.snapshot,'rows':len(candidates),'stride':stride,'selected':len(candidates[::stride][:2000])}
        enrich(result, rows, client)
        pathlib.Path(args.output).write_text(json.dumps(result,indent=2)+'\n')
        return
    started = dt.datetime.now(UTC)
    markets, strata = {}, []
    def walk(name, params, pages):
        cursor = None
        ids = set()
        complete = False
        for page in range(pages):
            query = dict(version='v1', limit=100, **params)
            if cursor:
                query['after_cursor'] = cursor
            data = client.get(BASE + '/markets/keyset', query)
            if data is None:
                break
            rows = data.get('markets', [])
            for m in rows:
                ids.add(m['id'])
                markets[m['id']] = {k:m[k] for k in FIELDS if k in m}
            cursor = data.get('next_cursor')
            if not cursor:
                complete = True
                break
            if (page + 1) % 25 == 0:
                print(name, page + 1, 'pages', len(ids), 'markets', flush=True)
        strata.append({'name':name, 'params':params, 'unique_markets':len(ids), 'complete':complete, 'pages_requested':page+1})
        print(name, len(ids), 'complete', complete, flush=True)
        return ids
    # Ascending closedTime produces 500 on this deployment; retain evidence then
    # use ascending ID and endDate as independent deliberate historical samples.
    client.get(BASE + '/markets/keyset', {'version':'v1','closed':'true','limit':1,'order':'closedTime','ascending':'true'})
    for status in ('disputed', 'proposed', 'challenged', 'reproposed', 'posed', 'initialized', 'arbitration'):
        walk('closed_status_' + status, {'closed':'true','uma_resolution_status':status}, args.pages)
    walk('recent_closures', {'closed':'true','order':'closedTime','ascending':'false'}, args.pages)
    walk('oldest_ids', {'closed':'true','order':'id','ascending':'true'}, args.pages)
    walk('oldest_end_dates', {'closed':'true','order':'endDate','ascending':'true'}, args.pages)
    walk('old_open_ids', {'closed':'false','order':'id','ascending':'true'}, min(args.pages, 30))
    walk('open_disputed', {'closed':'false','uma_resolution_status':'disputed'}, args.pages)
    walk('open_proposed', {'closed':'false','uma_resolution_status':'proposed'}, args.pages)
    now = dt.datetime.now(UTC)
    rows = list(markets.values())
    closed = [m for m in rows if m.get('closed') is True]
    def age(m):
        t = timestamp(m.get('closedTime'))
        return (now-t).total_seconds()/86400 if t else None
    def prices(m):
        try:
            return [float(x) for x in json.loads(m.get('outcomePrices','[]'))]
        except (ValueError, TypeError):
            return []
    def terminal(m):
        p = prices(m)
        return len(p)==2 and (p in ([0,1],[1,0],[.5,.5]))
    def annotate(m):
        return dict(m, age_since_closed_days=age(m), terminal_looking_prices=terminal(m))
    unresolved_status = [m for m in closed if m.get('umaResolutionStatus') not in (None, '', 'resolved')]
    no_resolution_evidence = [m for m in closed if m.get('umaResolutionStatus') != 'resolved' and not terminal(m)]
    def oldest(seq):
        return [annotate(m) for m in sorted((m for m in seq if age(m) is not None), key=age, reverse=True)[:20]]
    def counts(seq):
        return {str(d):sum(age(m) is not None and age(m)>d for m in seq) for d in (1,7,30,90)}
    proxy = []
    for m in closed:
        c,u=timestamp(m.get('closedTime')),timestamp(m.get('umaEndDate'))
        if m.get('umaResolutionStatus') == 'resolved' and c and u:
            proxy.append(dict(m, uma_end_minus_closed_days=(u-c).total_seconds()/86400))
    api_checks = []
    candidates = oldest(unresolved_status) + sorted(proxy,key=lambda m:m['uma_end_minus_closed_days'],reverse=True)[:20]
    conditions = list(dict.fromkeys(m['conditionId'] for m in candidates if m.get('conditionId')))
    for host in ('https://clob.polymarket.com','https://data-api.polymarket.com'):
        for route in ('resolutions','tokens'):
            # One representative batch: do not hammer a host that denies access.
            data = client.get(host+'/v2/'+route, {'condition':','.join(conditions[:20])})
            api_checks.append({'endpoint':host+'/v2/'+route,'conditions':conditions[:20],'response':data})
    result = {
        'measurement_started_utc':started.isoformat(), 'measurement_finished_utc':now.isoformat(),
        'method':{'max_requests_per_second':2.5,'cache_busting':True,'strata':strata,'unique_markets':len(rows),'unique_closed_markets':len(closed),'v1_only':all(m.get('version')=='v1' for m in rows)},
        'classification_warning':'Gamma status/price candidates are NOT chain-verified unresolved markets; umaEndDate is NOT an established actual-resolution timestamp. This is not a catalog census or a close-to-WebSocket latency measurement.',
        'closed_status_counts':dict(collections.Counter(str(m.get('umaResolutionStatus')) for m in closed)),
        'nonresolved_status_candidates':{'count':len(unresolved_status),'older_than_days':counts(unresolved_status),'oldest_20':oldest(unresolved_status),'all_candidates':[annotate(m) for m in unresolved_status]},
        'no_status_or_price_resolution_evidence':{'count':len(no_resolution_evidence),'older_than_days':counts(no_resolution_evidence),'oldest_20':oldest(no_resolution_evidence)},
        'resolved_uma_end_proxy':{'count':len(proxy),'positive_count':sum(m['uma_end_minus_closed_days']>0 for m in proxy),'negative_count':sum(m['uma_end_minus_closed_days']<0 for m in proxy),'zero_count':sum(m['uma_end_minus_closed_days']==0 for m in proxy),'top_20':sorted(proxy,key=lambda m:m['uma_end_minus_closed_days'],reverse=True)[:20]},
        'flag_mismatches':{'closed_accepting_true_count':sum(m.get('acceptingOrders') is True for m in closed),'closed_accepting_true_oldest_20':oldest([m for m in closed if m.get('acceptingOrders') is True]),'open_accepting_false_count':sum(m.get('closed') is False and m.get('acceptingOrders') is False for m in rows),'open_accepting_false_examples':sorted([m for m in rows if m.get('closed') is False and m.get('acceptingOrders') is False],key=lambda m:m.get('createdAt',''))[:20]},
        'resolution_api_checks':api_checks,'request_errors':client.log,
        'established_actual_resolution_top_20':None,'established_closed_unresolved_counts':None,'steady_state_closed_unresolved_estimate':None,
    }
    result['nonaccepting_open_point_checks'] = []
    for market_id in ('3358951','3358952','3358953','3358954','3358955','3358956','3358957'):
        point = client.get(BASE+'/markets/'+market_id, {})
        if point:
            result['nonaccepting_open_point_checks'].append({k:point[k] for k in FIELDS if k in point})
    enrich(result, rows, client)
    historical_check(result, client)
    pathlib.Path(args.output).write_text(json.dumps(result,indent=2)+'\n')
    print('Saved',args.output,'unique',len(rows),'candidate counts',counts(unresolved_status),flush=True)

def enrich(result, rows, client, verify_open=False):
    """Verify every non-resolved-status candidate and a deterministic broad sample."""
    by_id = {m['id']:m for m in rows}
    rows = list(by_id.values())
    candidates = [m for m in rows if (m.get('closed') is True or verify_open) and m.get('umaResolutionStatus') != 'resolved']
    resolved = sorted([m for m in rows if m.get('closed') is True and m.get('umaResolutionStatus') == 'resolved'], key=lambda m:int(m['id']))
    stride = max(1,len(resolved)//2000)
    selected = {m['id']:m for m in candidates + resolved[::stride][:2000] + result['resolved_uma_end_proxy']['top_20']}
    conditions = list(dict.fromkeys(m['conditionId'] for m in selected.values() if m.get('conditionId')))
    resolutions, tokens = {}, {}
    for index in range(0,len(conditions),20):
        batch=conditions[index:index+20]
        for route, dest in [('resolutions',resolutions)]:
            data=client.get('https://data-api.polymarket.com/v2/'+route, {'condition':','.join(batch)})
            if data is None:
                continue
            for cid in batch:
                dest[cid]=[]
            for raw in data.get('data',[]):
                cid=raw.get('condition_id')
                if cid in dest:
                    fields = ('status','last_update_timestamp','resolved_at','was_disputed','extended_review','expected_settlement_time','settlement_time_basis','price','resolution_source') if route=='resolutions' else ('module','resolved','closed','final_price','outcome_index')
                    dest[cid].append({k:raw[k] for k in fields if k in raw})
        for token_index in range(0,len(batch),10):
            token_batch=batch[token_index:token_index+10]
            data=client.get('https://data-api.polymarket.com/v2/tokens',{'condition':','.join(token_batch)})
            if data is not None:
                for cid in token_batch:
                    tokens[cid]=[]
                for raw in data.get('data',[]):
                    cid=raw.get('condition_id')
                    if cid in tokens:
                        tokens[cid].append({k:raw[k] for k in ('module','resolved','closed','final_price','outcome_index') if k in raw})
        if index%500==0:
            print('Verified',index+len(batch),'of',len(conditions),'conditions',flush=True)
    reference=dt.datetime.now(UTC)
    actual, pending, audit = [],[],[]
    classifications=collections.Counter()
    for m in selected.values():
        cid=m.get('conditionId')
        res=resolutions.get(cid,[])
        tok=[t for t in tokens.get(cid,[]) if t.get('module')=='v1_ctf']
        state='unknown'
        if tok and all(t.get('resolved') is True for t in tok):
            state='token_resolved'
        elif tok and all(t.get('resolved') is False for t in tok):
            state='token_unresolved'
        elif any(r.get('status')=='resolved' for r in res):
            state='lifecycle_resolved_tokens_unavailable'
        classifications[state]+=1
        c=timestamp(m.get('closedTime'))
        entry={'market':{k:m[k] for k in FIELDS if k in m},'classification':state,'resolution':res,'tokens':tok,'age_since_closed_days':(reference-c).total_seconds()/86400 if c else None}
        if m.get('umaResolutionStatus')!='resolved':
            audit.append(entry)
        if state=='token_unresolved' and m.get('closed') is True:
            pending.append(entry)
        for r in res:
            if r.get('status')!='resolved' or c is None or m.get('closed') is not True:
                continue
            t=timestamp(r.get('resolved_at'))
            source='resolved_at'
            if t is None:
                raw=r.get('last_update_timestamp')
                t=dt.datetime.fromtimestamp(float(raw),UTC) if raw and re.fullmatch(r'[0-9]+(?:\.[0-9]+)?',str(raw)) else timestamp(raw)
                source='resolved lifecycle last_update_timestamp'
            if t:
                actual.append(dict(entry,resolution_time_utc=t.isoformat(),resolution_time_source=source,close_to_resolution_days=(t-c).total_seconds()/86400))
    pending.sort(key=lambda x:x['age_since_closed_days'] if x['age_since_closed_days'] is not None else -1,reverse=True)
    actual.sort(key=lambda x:x['close_to_resolution_days'],reverse=True)
    result['verified_tail']={'verified_at_utc':reference.isoformat(),'selected_markets':len(selected),'conditions_requested':len(conditions),'resolved_sampling_stride':stride,'classification_counts':dict(classifications),'candidate_audit':audit,'closed_token_unresolved_count':len(pending),'closed_token_unresolved_older_than_days':{str(d):sum(x['age_since_closed_days'] is not None and x['age_since_closed_days']>d for x in pending) for d in (1,7,30,90)},'oldest_closed_token_unresolved_20':pending[:20],'unresolved_missing_closed_time':sum(x['age_since_closed_days'] is None for x in pending),'actual_resolution_timestamp_pairs':len(actual),'negative_lags':sum(x['close_to_resolution_days']<0 for x in actual),'top_20_actual_resolution_lags':actual[:20],'resolution_api_errors':client.log}
    open_unresolved = [x for x in audit if x['market'].get('closed') is False and x['classification']=='token_unresolved']
    for x in open_unresolved:
        end = timestamp(x['market'].get('endDate'))
        x['days_past_end_date'] = (reference-end).total_seconds()/86400 if end else None
    open_unresolved.sort(key=lambda x:x.get('days_past_end_date') if x.get('days_past_end_date') is not None else -1, reverse=True)
    result['verified_tail']['open_token_unresolved_count'] = len(open_unresolved)
    result['verified_tail']['open_nonaccepting_token_unresolved_count'] = sum(x['market'].get('acceptingOrders') is False for x in open_unresolved)
    result['verified_tail']['oldest_end_dates_open_token_unresolved_20'] = open_unresolved[:20]
    result['established_actual_resolution_top_20']=actual[:20]
    result['established_closed_unresolved_counts']={'scope':'verified selected sample only; not a catalog census','counts':result['verified_tail']['closed_token_unresolved_older_than_days']}

def historical_check(result, client):
    import copy
    rows, strata = [], []
    for year in (2024,2025,2026):
        params = {'version':'v1','closed':'true','limit':100,'order':'volumeNum','ascending':'false','uma_resolution_status':'resolved','end_date_min':str(year)+'-01-01T00:00:00Z','end_date_max':str(year)+'-12-31T23:59:59Z'}
        data=client.get(BASE+'/markets/keyset',params)
        if data is not None:
            cohort=[{k:m[k] for k in FIELDS if k in m} for m in data['markets']]
            rows+=cohort
            strata.append({'year':year,'params':params,'markets':len(cohort)})
    temp=copy.deepcopy(result)
    temp['resolved_uma_end_proxy']['top_20']=[]
    enrich(temp,rows,client)
    result['historical_high_volume_verification']={'strata':strata,'verification':temp['verified_tail']}
    prior=result.get('prior_verified_recent_sample',{}).get('top_20_actual_resolution_lags',[])
    combined=result['verified_tail']['top_20_actual_resolution_lags']+temp['verified_tail']['top_20_actual_resolution_lags']+prior
    combined={x['market']['id']:x for x in combined}
    result['established_actual_resolution_top_20']=sorted(combined.values(),key=lambda x:x['close_to_resolution_days'],reverse=True)[:20]

if __name__ == '__main__':
    main()
