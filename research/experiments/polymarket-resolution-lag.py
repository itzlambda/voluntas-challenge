#!/usr/bin/env python3
"""Public API measurement only. Run from repository root with python3 -I.

Compact checkpoints contain only selected market fields, never full API dumps.
Re-running with --resume reuses those non-atomic observations (not a fresh census).
"""
import argparse
import collections
import concurrent.futures
import datetime as dt
import hashlib
import gzip
import fcntl
import json
import math
from pathlib import Path
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = Path('research/experiments')
GAMMA = 'https://gamma-api.polymarket.com/markets/keyset'
DATA = 'https://data-api.polymarket.com/v2/'
FLAGS = {'active': True, 'closed': False, 'archived': False,
         'acceptingOrders': True, 'enableOrderBook': True}
FIELDS = ('id', 'conditionId', 'slug', 'version', 'active', 'closed', 'archived',
          'acceptingOrders', 'enableOrderBook', 'closedTime', 'createdAt',
          'startDate', 'endDate', 'updatedAt', 'acceptingOrdersTimestamp',
          'umaResolutionStatus', 'umaEndDate', 'outcomePrices', 'sportsMarketType',
          'gameStartTime', 'resolutionSource', 'automaticallyResolved', 'fallbackResolutionType')

def now():
    return dt.datetime.now(dt.timezone.utc)

def timestamp(value):
    if value is None or value == '':
        return None
    try:
        if str(value).isdigit():
            return float(value)
        return dt.datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp()
    except ValueError:
        return None

def compact(row):
    out = {k: row.get(k) for k in FIELDS}
    out['questionID'] = row.get('questionID')
    out['negRiskRequestID'] = row.get('negRiskRequestID')
    out['negRisk'] = row.get('negRisk')
    out['has_v2_position_ids'] = bool(row.get('positionIds'))
    out['v1_token_id'] = (json.loads(row.get('clobTokenIds') or '[]') or [None])[0]
    out['has_resolvedBy'] = bool(row.get('resolvedBy'))
    out['event_series'] = sorted({s.get('slug', '') for e in row.get('events', [])
                                  for s in e.get('series', [])})
    out['event_tags'] = sorted({t.get('slug', '') for e in row.get('events', [])
                                for t in e.get('tags', [])})
    return out

def category(row):
    slug = row.get('slug') or ''
    if 'updown' in slug or '-up-or-down-' in slug:
        return 'crypto_up_down'
    if row.get('sportsMarketType') or row.get('gameStartTime'):
        return 'sports'
    start = timestamp(row.get('startDate'))
    end = timestamp(row.get('endDate'))
    if start and end and end-start >= 30*86400:
        return 'other_scheduled_span_30d_plus'
    return 'other'

def distribution(values):
    a = sorted(values)
    if not a:
        return {'n': 0}
    def q(p):
        x = (len(a)-1)*p
        lo = math.floor(x)
        hi = math.ceil(x)
        return a[lo]+(a[hi]-a[lo])*(x-lo)
    return {'n':len(a), 'min':a[0], 'p10':q(.1), 'p50':q(.5),
            'p90':q(.9), 'p99':q(.99), 'max':a[-1], 'mean':sum(a)/len(a)}

def save(path, value):
    temp = path.with_suffix(path.suffix+'.tmp')
    temp.write_text(json.dumps(value, indent=2, sort_keys=True)+'\n')
    temp.replace(path)

class Client:
    def __init__(self, run):
        self.run = run
        self.lock = threading.Lock()
        self.next_request = 0
        self.counts = collections.Counter()
        self.errors = []
        self.headers = []

    def get(self, url, params=()):
        url += '?' + urllib.parse.urlencode(params)
        for attempt in range(6):
            with self.lock:
                wait = max(0, self.next_request-time.monotonic())
                time.sleep(wait)
                self.next_request = time.monotonic()+.27
                self.counts['attempts'] += 1
            with (BASE/'polymarket-resolution-lag-rate.lock').open('a+') as rate:
                fcntl.flock(rate,fcntl.LOCK_EX)
                rate.seek(0)
                last = float(rate.read() or 0)
                time.sleep(max(0,.27-(time.monotonic()-last)))
                rate.seek(0); rate.truncate(); rate.write(str(time.monotonic())); rate.flush()
                fcntl.flock(rate,fcntl.LOCK_UN)
            try:
                req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0',
                                                           'Accept':'application/json', 'Accept-Encoding':'gzip'})
                with urllib.request.urlopen(req, timeout=45) as response:
                    payload = response.read()
                    if response.headers.get('Content-Encoding') == 'gzip':
                        payload = gzip.decompress(payload)
                    data = json.loads(payload)
                    with self.lock:
                        self.counts['http_200'] += 1
                        if len(self.headers) < 15:
                            self.headers.append({k: response.headers.get(k) for k in
                                                 ('Date','Age','X-Cache','Cache-Control')})
                    return data
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
                code = getattr(exc, 'code', None)
                retry = code in (429,500,502,503,504) or code is None
                with self.lock:
                    self.counts[str(code or 'network_error')] += 1
                    self.errors.append({'endpoint':url.split('?')[0], 'code':code,
                                        'attempt':attempt+1})
                if not retry or attempt == 5:
                    raise
                time.sleep(max(float(exc.headers.get('Retry-After', 0)) if code else 0,
                               min(30, 2**attempt)))
        raise RuntimeError('unreachable')

    def scan(self, name, filters, cutoff=None, resume=False, id_stop=None):
        path = BASE / ('polymarket-resolution-lag-'+name+'-checkpoint.json')
        if resume and path.exists():
            state = json.loads(path.read_text())
            if state['complete']:
                print(name, 'reused', len(state['rows']), flush=True)
                return state
        else:
            state = {'started_at':now().isoformat(), 'filters':filters,
                     'cutoff':cutoff, 'rows':[], 'cursor':None, 'pages':0,
                     'duplicates':0, 'complete':False, 'ordering_inversions':0}
        state['cutoff'] = cutoff
        state['id_stop'] = id_stop
        seen = {r['id'] for r in state['rows']}
        cursors = set()
        previous = None
        while True:
            params = [('version','v1'),('limit','100'),('_resolution_run',self.run)] + filters
            if state['cursor']:
                params.append(('after_cursor',state['cursor']))
            page = self.get(GAMMA, params)
            rows = page['markets']
            for raw in rows:
                assert raw.get('version') == 'v1', 'unexpected protocol'
                row = compact(raw)
                if row['id'] in seen:
                    state['duplicates'] += 1
                    continue
                seen.add(row['id'])
                value = timestamp(row.get('closedTime'))
                if cutoff and value is not None:
                    if previous is not None and value > previous:
                        state['ordering_inversions'] += 1
                    previous = value
                state['rows'].append(row)
            state['pages'] += 1
            cursor = page.get('next_cursor')
            passed = cutoff and any(timestamp(r.get('closedTime')) is not None and
                                    timestamp(r['closedTime']) < cutoff for r in rows)
            id_boundary = id_stop is not None and any(
                int(r['id']) >= id_stop if name == 'open_low' else int(r['id']) <= id_stop
                for r in rows)
            if not cursor or passed or id_boundary:
                state['complete'] = True
                state['termination'] = 'id_boundary_crossed' if id_boundary else 'cutoff_crossed' if passed else 'cursor_exhausted'
                state['completed_at'] = now().isoformat()
                save(path,state)
                print(name, state['pages'], len(state['rows']), state['termination'], flush=True)
                return state
            assert cursor not in cursors, 'cursor cycle'
            cursors.add(cursor)
            state['cursor'] = cursor
            if state['pages'] % 25 == 0:
                save(path,state)
                print(name,state['pages'],len(state['rows']),rows[-1].get('closedTime'),flush=True)

def batches(rows, size):
    for i in range(0,len(rows),size):
        yield rows[i:i+size]

def enrich(client, rows, path, resume, token=False):
    cache = json.loads(path.read_text()) if resume and path.exists() else {}
    todo = [r for r in rows if r.get('conditionId') and r['conditionId'] not in cache]
    for i, batch in enumerate(batches(todo,10 if token else 20)):
        ids = [r['conditionId'] for r in batch]
        data = client.get(DATA+('tokens' if token else 'resolutions'), [('condition',','.join(ids))])['data']
        for cid in ids:
            cache[cid] = []
        for raw in data:
            cid = raw.get('condition_id')
            if cid not in cache:
                continue
            fields = ('module','resolved','closed','final_price','outcome_index') if token else (
                'status','last_update_timestamp','resolved_at','was_disputed','extended_review',
                'payouts','price','reporter','resolution_source','expected_settlement_time')
            cache[cid].append({k:raw.get(k) for k in fields if k in raw})
        if i%25 == 0:
            save(path,cache)
            print('tokens' if token else 'resolutions',i+1,'batches',flush=True)
    save(path,cache)
    return cache

def analyze(states, resolution, tokens, reference, client, config):
    recent = [r for r in states['recent']['rows'] if
              timestamp(r.get('closedTime')) is not None and
              reference-config.days*86400 <= timestamp(r['closedTime']) <= reference]
    opened = states['open']['rows']
    failures = collections.Counter()
    combinations = collections.Counter()
    nontrading = []
    for row in opened:
        bad = [k for k,v in FLAGS.items() if row.get(k) is not v]
        failures.update(bad)
        if bad:
            nontrading.append(row)
            combinations[','.join(bad)] += 1
    candidates = {r['id']:r for key in ('proposed','disputed') for r in states[key]['rows']}
    candidates.update({r['id']:r for r in recent if r.get('umaResolutionStatus') != 'resolved'})
    candidates.update({r['id']:r for r in nontrading})
    lag = collections.defaultdict(list)
    end_lag = collections.defaultdict(list)
    resolution_states = collections.Counter()
    signs = collections.Counter()
    time_sources = collections.Counter()
    samples = []
    all_observations = []
    for r in recent:
        for x in resolution.get(r['conditionId'],[]):
            resolution_states[x.get('status')] += 1
            if x.get('status') != 'resolved':
                continue
            source = 'resolved_at' if timestamp(x.get('resolved_at')) is not None else 'last_update_timestamp'
            resolved = timestamp(x.get(source))
            if resolved is None:
                continue
            time_sources[source] += 1
            diff = resolved-timestamp(r['closedTime'])
            lag['all'].append(diff)
            lag[category(r)].append(diff)
            all_observations.append({'id':r['id'],'category':category(r),'closedTime':r['closedTime'],
                                     'resolution_timestamp':x.get(source),'time_source':source,
                                     'signed_seconds':diff})
            signs['negative' if diff < 0 else 'positive' if diff > 0 else 'zero'] += 1
            end = timestamp(r.get('endDate'))
            if end is not None:
                end_lag['all'].append((resolved-end)/3600)
                end_lag[category(r)].append((resolved-end)/3600)
            if len(samples)<20 or diff>60:
                samples.append({'id':r['id'],'category':category(r),'closedTime':r['closedTime'],
                                'resolution':x,'signed_seconds':diff})
            break
    audit = []
    classification = collections.Counter()
    for r in candidates.values():
        tok = [x for x in tokens.get(r['conditionId'],[]) if x.get('module') == 'v1_ctf']
        res = resolution.get(r['conditionId'],[])
        if tok and all(x.get('resolved') is True for x in tok):
            status = 'onchain_resolved'
        elif tok and all(x.get('resolved') is False for x in tok):
            status = 'onchain_unresolved'
        elif any(x.get('status') == 'resolved' for x in res):
            status = 'resolution_api_resolved_tokens_unknown'
        else:
            status = 'unknown'
        classification[status] += 1
        close = timestamp(r.get('closedTime'))
        audit.append({'market':r,'classification':status,'tokens':tok,'resolution':res,
                      'closed_age_days':(reference-close)/86400 if close else None})
    audit.sort(key=lambda x:x.get('closed_age_days') or -1,reverse=True)
    tradable = len(opened)-len(nontrading)
    rates = {}
    for hours in (1,6,24,72,168):
        if hours > config.days*24:
            continue
        rows = [r for r in recent if timestamp(r['closedTime']) >= reference-hours*3600]
        rates[str(hours)+'h'] = {'count':len(rows),'per_hour':len(rows)/hours,
                                'per_day':len(rows)*24/hours,
                                'categories':dict(collections.Counter(category(r) for r in rows))}
    population = collections.Counter(category(r) for r in recent)
    weighted = [(x, population[k]/len(v)) for k,v in lag.items() if k != 'all' for x in v]
    weight = sum(w for x,w in weighted)
    weighted_positive = sum(max(0,x)*w for x,w in weighted)/weight if weight else None
    weighted_mean = sum(x*w for x,w in weighted)/weight if weight else None
    ordered = sorted(weighted)
    weighted_quantiles = {}
    for label,p in [('p10',.1),('p50',.5),('p90',.9),('p99',.99)]:
        running = 0
        for x,w in ordered:
            running += w
            if running >= p*weight:
                weighted_quantiles[label] = x
                break
    result = {'measurement_started_at':dt.datetime.fromtimestamp(reference,dt.timezone.utc).isoformat(),
              'measurement_completed_at':now().isoformat(), 'config':vars(config),
              'request_counts':dict(client.counts),'request_errors':client.errors,
              'request_count_scope':'Current process only; resumed checkpoints and exploratory probes are excluded. Scan page counts include retained earlier pages.',
              'sample_response_headers':client.headers,
              'scope_warning':'Full open census and full proposed/disputed cohorts; closed cohort bounded to recent days. Gamma missing/other statuses outside this window are NOT exhaustively enumerated.',
              'scan_summary':{k:{a:b for a,b in s.items() if a not in ('rows','cursor')} |
                                 {'row_count':len(s['rows'])} for k,s in states.items()},
              'open':{'count':len(opened),'tradable':tradable,'nontradable':len(nontrading),
                      'failed_flags':dict(failures),'failed_combinations':dict(combinations)},
              'recent_closed':{'count':len(recent),'gamma_statuses':dict(collections.Counter(
                  str(r.get('umaResolutionStatus')) for r in recent)),
                  'categories':dict(collections.Counter(category(r) for r in recent)),
                  'missing_closed_time_in_scan':sum(not r.get('closedTime') for r in states['recent']['rows'])},
              'closure_rates':rates,
              'lag_resolution_minus_gamma_closed_seconds':{k:distribution(v) for k,v in lag.items()},
              'population_weighted_signed_lag_seconds':dict(weighted_quantiles, mean=weighted_mean,
                  mean_positive_part=weighted_positive, represented_population=weight),
              'lag_sign_counts':dict(signs),'resolution_time_sources':dict(time_sources),
              'sample_resolution_statuses':dict(resolution_states),
              'schedule_end_to_resolution_hours_NOT_trading_stop':{k:distribution(v) for k,v in end_lag.items()},
              'candidate_classifications':dict(classification), 'candidate_audit':audit,
              'lag_samples':samples[:50], 'lag_observations':all_observations,
              'candidate_unresolved_percent_of_tradable':classification['onchain_unresolved']/tradable*100,
              'little_law':{'note':'Positive part of signed closedTime lag only; not an independently established trading-stop lag. Historical unresolved outliers excluded.',
                            'sample_mean_positive_lag_seconds':weighted_positive,
                            'estimate_recent_rate_times_mean_positive_lag':rates['24h']['per_hour']/3600*weighted_positive}}
    return result

def v1_audit(client, closed_rows, reference, sample_size, resume):
    """Use UMA question selectors to avoid condition lookups preferring V2."""
    groups = collections.defaultdict(list)
    for r in closed_rows:
        close = timestamp(r.get('closedTime'))
        if close is not None and reference-86400 <= close <= reference:
            groups[category(r)].append(r)
    selected = []
    for rows in groups.values():
        rows.sort(key=lambda r:hashlib.sha256(r['id'].encode()).digest())
        selected.extend(rows[:sample_size])
    path = BASE/'polymarket-resolution-lag-v1-metadata-checkpoint.json'
    metadata = json.loads(path.read_text()) if resume and path.exists() else {}
    missing = [r for r in selected if r['id'] not in metadata]
    for batch in batches(missing,100):
        params = [('version','v1'),('closed','any'),('limit','100'),('_resolution_run',client.run)]
        params += [('id',r['id']) for r in batch]
        raw = client.get(GAMMA,params)['markets']
        metadata.update({r['id']:compact(r) for r in raw})
        save(path,metadata)
    assert all(r['id'] in metadata for r in selected), 'metadata lookup miss'
    rows = [metadata[r['id']] for r in selected]
    condition = enrich(client,rows,BASE/'polymarket-resolution-lag-resolution-checkpoint.json',True)
    token_path = BASE/'polymarket-resolution-lag-token-checkpoint.json'
    token = json.loads(token_path.read_text()) if resume and token_path.exists() else {}
    missing = [r for r in rows if r.get('conditionId') not in token and r.get('v1_token_id')]
    for batch in batches(missing,50):
        data = client.get(DATA+'tokens',[('token_id',','.join(r['v1_token_id'] for r in batch))])['data']
        for r in batch:
            token[r['conditionId']] = []
        for raw in data:
            if raw.get('module') == 'v1_ctf':
                token[raw['condition_id']].append({k:raw.get(k) for k in
                    ('module','resolved','closed','final_price','outcome_index')})
        save(token_path,token)
    questions = {}
    for r in rows:
        q = r.get('negRiskRequestID') if r.get('negRisk') else r.get('questionID')
        if category(r) == 'crypto_up_down' and not r.get('has_v2_position_ids'):
            continue
        if q:
            questions[r['id']] = q
    question_path = BASE/'polymarket-resolution-lag-v1-questions-checkpoint.json'
    cache = json.loads(question_path.read_text()) if resume and question_path.exists() else {}
    todo = sorted(set(questions.values())-set(cache))
    def fetch(q):
        data = client.get(DATA+'resolutions',[('question_id',q)])['data']
        return q,[{k:x.get(k) for k in ('status','last_update_timestamp','resolved_at',
                                      'price','was_disputed','extended_review')} for x in data]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        pending = [pool.submit(fetch,q) for q in todo]
        for i,future in enumerate(concurrent.futures.as_completed(pending)):
            q,data = future.result()
            cache[q] = data
            if i%50 == 0:
                save(question_path,cache)
                print('v1 questions',i+1,'/',len(todo),flush=True)
    save(question_path,cache)
    fallback_rows = [r for r in rows if not r.get('has_v2_position_ids') and
                     not any(x.get('status') == 'resolved' for x in cache.get(questions.get(r['id']),[]))]
    for batch in batches([r for r in fallback_rows if len(token.get(r['conditionId'],[])) < 2],10):
        data = client.get(DATA+'tokens',[('condition',','.join(r['conditionId'] for r in batch))])['data']
        for r in batch:
            token[r['conditionId']] = []
        for x in data:
            if x.get('condition_id') in token:
                token[x['condition_id']].append({k:x.get(k) for k in
                    ('module','resolved','closed','final_price','outcome_index')})
        save(token_path,token)
    observations = []
    for r in rows:
        family = token.get(r['conditionId'],[])
        only_v1_family = len(family) >= 2 and all(t.get('module') == 'v1_ctf' for t in family)
        tok = [t for t in family if t.get('module') == 'v1_ctf']
        q = questions.get(r['id'])
        terminal = next((x for x in cache.get(q,[]) if x.get('status') == 'resolved'),None)
        source = None
        if terminal:
            source = 'v1_uma_question_latest_terminal_lifecycle'
        elif not r.get('has_v2_position_ids') and only_v1_family:
            terminal = next((x for x in condition.get(r['conditionId'],[]) if x.get('status') == 'resolved'),None)
            if terminal:
                source = 'condition_terminal_verified_v1_only_token_family'
        value = timestamp(terminal.get('resolved_at') or terminal.get('last_update_timestamp')) if terminal else None
        cr = next((x for x in condition.get(r['conditionId'],[]) if x.get('status')=='resolved'),{})
        ct = timestamp(cr.get('resolved_at') or cr.get('last_update_timestamp'))
        observations.append({'id':r['id'],'category':category(r),'closedTime':r['closedTime'],
            'has_v2_position_ids':r.get('has_v2_position_ids'),
            'token_module_families':sorted(set(t.get('module') for t in family)), 'timestamp_source':source,
            'resolution_timestamp':terminal.get('resolved_at') or terminal.get('last_update_timestamp') if terminal else None,
            'signed_seconds':value-timestamp(r['closedTime']) if value is not None else None,
            'condition_lookup_signed_seconds':ct-timestamp(r['closedTime']) if ct is not None else None,
            'v1_token_resolved':all(t.get('resolved') is True for t in tok) if tok else None,
            'question_statuses':[x.get('status') for x in cache.get(q,[])],
            'question_was_disputed':any(x.get('was_disputed') for x in cache.get(q,[]))})
    output = {'completed_at':now().isoformat(), 'cohort_reference_at':dt.datetime.fromtimestamp(reference,dt.timezone.utc).isoformat(), 'cohort':'Closures in the 24 hours before the main scan reference time',
              'population_by_category':{k:len(v) for k,v in groups.items()},
              'selected':len(rows),'observations':observations,'requests':dict(client.counts),
              'request_errors':client.errors}
    save(BASE/'polymarket-resolution-lag-v1-audit.json',output)
    return output


def fresh_backlog(client, hours=6, resume=False):
    path = BASE/'polymarket-resolution-lag-fresh-backlog.json'
    if resume and path.exists():
        return json.loads(path.read_text())
    reference = now().timestamp()
    client.run = str(time.time_ns())
    state = client.scan('backlog', [('closed','true'),('order','closedTime,id'),
                                   ('ascending','false')],reference-hours*3600,False)
    rows = [r for r in state['rows'] if timestamp(r.get('closedTime')) is not None and
            reference-hours*3600 <= timestamp(r['closedTime']) <= reference]
    status = {}
    missing = [r for r in rows if not r.get('v1_token_id')]
    for i,batch in enumerate(batches([r for r in rows if r.get('v1_token_id')],50)):
        response = client.get(DATA+'tokens',[('token_id',','.join(r['v1_token_id'] for r in batch))])['data']
        for x in response:
            if x.get('module') == 'v1_ctf':
                status[x['condition_id']] = {k:x.get(k) for k in ('resolved','closed','final_price')}
        if i%50 == 0:
            print('backlog token batches',i+1,flush=True)
    counts = collections.Counter()
    unresolved = []
    for r in rows:
        x = status.get(r['conditionId'],{})
        label = 'resolved' if x.get('resolved') is True else 'unresolved' if x.get('resolved') is False else 'unknown'
        counts[label] += 1
        if label != 'resolved':
            unresolved.append({'market':r,'token':x,'classification':label})
    output = {'started_at':dt.datetime.fromtimestamp(reference,dt.timezone.utc).isoformat(),
              'completed_at':now().isoformat(),'hours':hours,'closed_markets':len(rows),
              'token_classifications':dict(counts),'missing_gamma_token_id':len(missing),
              'unresolved_or_unknown':unresolved,'requests':dict(client.counts),
              'request_errors':client.errors,
              'scope':'All closedTime records in the bounded window, one explicitly v1 CTF token per market. Moving scan, not an atomic snapshot; no claim about older closed markets.'}
    save(path,output)
    return output


def finalize_protocol_summary(result):
    audit = result['v1_protocol_audit']
    values = collections.defaultdict(list)
    for x in audit['observations']:
        if x['signed_seconds'] is not None:
            values[x['category']].append(x['signed_seconds'])
    population = audit['population_by_category']
    weighted = [(x,population[k]/len(v)) for k,v in values.items() for x in v]
    total_weight = sum(w for x,w in weighted)
    mean_positive = sum(max(0,x)*w for x,w in weighted)/total_weight if total_weight else None
    all_values = [x for v in values.values() for x in v]
    result['v1_lag_resolution_minus_gamma_closed_seconds'] = {
        'by_category':{k:distribution(v) for k,v in values.items()},
        'all_stratified_sample_unweighted':distribution(all_values),
        'population_weighted_mean_signed':sum(x*w for x,w in weighted)/total_weight if total_weight else None,
        'population_weighted_mean_positive_part':mean_positive,
        'signs':dict(collections.Counter('positive' if x>0 else 'negative' if x<0 else 'zero' for x in all_values)),
        'timestamp_sources':dict(collections.Counter(x['timestamp_source'] for x in audit['observations'])),
        'missing_timestamps':sum(x['signed_seconds'] is None for x in audit['observations']),
        'tokens':dict(collections.Counter(str(x['v1_token_resolved']) for x in audit['observations'])),
        'condition_lookup_later_than_v1_by_over_1s':sum(
            x['condition_lookup_signed_seconds'] is not None and x['signed_seconds'] is not None and
            x['condition_lookup_signed_seconds']-x['signed_seconds']>1 for x in audit['observations']),
        'warning':'Signed timestamp offsets, not proof of an earlier acceptingOrders transition. UMA question terminal lifecycle is an oracle-resolution proxy; condition fallback requires complete v1-only token families and no Gamma V2 position IDs.'}
    result['little_law'] = {
        'closure_rate_per_hour_24h':result['closure_rates']['24h']['per_hour'],
        'population_weighted_mean_positive_lag_seconds':mean_positive,
        'estimated_closed_waiting_backlog':result['closure_rates']['24h']['per_hour']*mean_positive/3600 if mean_positive is not None else None,
        'warning':'Uses max(0, resolution_timestamp - closedTime). Not a bound on actual stop-to-resolution lag, old inactive markets, or missed WebSocket events.'}
    audit_rows = result['candidate_audit']
    open_rows = [x for x in audit_rows if x['market'].get('closed') is False]
    classifications = collections.Counter(x['classification'] for x in open_rows)
    lower = classifications['onchain_unresolved']
    upper = lower+classifications['unknown']
    denominator = result['open']['tradable']
    result['backlog_summary'] = {
        'open_failed_predicate_classifications':dict(classifications),
        'open_failed_predicate_unresolved_lower':lower,
        'open_failed_predicate_unresolved_upper_including_unknown':upper,
        'lower_percent_of_live_tradable':lower/denominator*100,
        'upper_percent_of_live_tradable':upper/denominator*100,
        'lower_percent_of_254000':lower/254000*100,
        'upper_percent_of_254000':upper/254000*100,
        'unresolved_book_enabled':sum(x['classification']=='onchain_unresolved' and x['market'].get('enableOrderBook') is True for x in open_rows),
        'unresolved_with_activation_timestamp':sum(x['classification']=='onchain_unresolved' and bool(x['market'].get('acceptingOrdersTimestamp')) for x in open_rows),
        'closed_proposed_disputed_classifications':dict(collections.Counter(x['classification'] for x in audit_rows if x['market'].get('closed') is True)),
        'global_historical_closed_backlog':'NOT established: missing/other Gamma resolution statuses older than the recent window were not exhaustively scanned.',
        'bootstrap_note':'All currently nontradable open exceptions fail startup admission. They are not automatically a backlog held by a collector started now.'}
    for x in open_rows:
        end = timestamp(x['market'].get('endDate'))
        created = timestamp(x['market'].get('createdAt'))
        ref = timestamp(result['measurement_started_at'])
        x['scheduled_end_age_days_NOT_closure_age'] = (ref-end)/86400 if end else None
        x['created_age_days_NOT_closure_age'] = (ref-created)/86400 if created else None
    diagnostic_names = ('lag_resolution_minus_gamma_closed_seconds','lag_sign_counts',
        'resolution_time_sources','sample_resolution_statuses','population_weighted_signed_lag_seconds',
        'schedule_end_to_resolution_hours_NOT_trading_stop','lag_samples','lag_observations')
    result['condition_lookup_diagnostics_NOT_v1_lags'] = {k:result.pop(k) for k in diagnostic_names if k in result}
    result['condition_lookup_diagnostics_NOT_v1_lags']['warning'] = 'Condition selectors may prefer V2 lifecycle for a carried-over v1 market. Do not interpret these offsets as v1 resolution lag.'
    result['measurement_completed_at'] = now().isoformat()
    result['request_counts'] = dict(result.get('request_counts',{}))


def clob_probe(client, result):
    rows = [x['market'] for x in result['candidate_audit']
            if x['classification'] == 'onchain_unresolved' and x['market']['enableOrderBook']]
    rows += [x['market'] for x in result['candidate_audit']
             if x['classification'] == 'onchain_unresolved' and not x['market']['enableOrderBook']][:1]
    observations = []
    for row in rows:
        url = 'https://clob.polymarket.com/markets/'+row['conditionId']
        try:
            raw = client.get(url)
            observations.append({'id':row['id'],'status':200,'state':{k:raw.get(k) for k in
                ('closed','active','archived','accepting_orders','enable_order_book','accepting_order_timestamp')}})
        except urllib.error.HTTPError as exc:
            observations.append({'id':row['id'],'status':exc.code})
    return {'observed_at':now().isoformat(),'observations':observations}

def observe(slug, seconds):
    """Observe Gamma and on-chain state; timestamps bound visibility, not WS delivery."""
    client = Client(str(time.time_ns()))
    observations = []
    deadline = time.monotonic()+seconds
    while time.monotonic() < deadline:
        started = now().isoformat()
        rows = client.get(GAMMA, [('version','v1'),('closed','any'),('slug',slug),
                                  ('_resolution_observation',str(time.time_ns()))])['markets']
        assert len(rows) == 1
        row = compact(rows[0])
        tok = client.get(DATA+'tokens',[('condition',row['conditionId'])])['data']
        res = client.get(DATA+'resolutions',[('condition',row['conditionId'])])['data']
        observations.append({'started_at':started,'completed_at':now().isoformat(),
                             'market':row,'tokens':[{k:t.get(k) for k in
                             ('module','resolved','final_price','closed')} for t in tok],
                             'resolutions':[{k:r.get(k) for k in
                             ('status','resolved_at','last_update_timestamp')} for r in res]})
        save(BASE/'polymarket-resolution-lag-observation.json',
             {'slug':slug,'observations':observations,'requests':dict(client.counts),
              'errors':client.errors})
        print(started, row['closed'], row['acceptingOrders'],
              [t.get('resolved') for t in tok], flush=True)
        time.sleep(min(20,max(0,deadline-time.monotonic())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--days',type=int,default=2)
    parser.add_argument('--sample-per-category',type=int,default=500)
    parser.add_argument('--resume',action='store_true')
    parser.add_argument('--open-pivot',type=int,default=5850000)
    parser.add_argument('--v1-audit-only',action='store_true')
    parser.add_argument('--backlog-only',action='store_true')
    parser.add_argument('--observe-slug')
    parser.add_argument('--observe-seconds',type=int,default=600)
    config = parser.parse_args()
    if config.observe_slug:
        observe(config.observe_slug,config.observe_seconds)
        return
    meta = BASE/'polymarket-resolution-lag-run.json'
    if config.resume and meta.exists():
        run = json.loads(meta.read_text())
    else:
        run = {'reference':now().timestamp(),'run':str(time.time_ns())}
        save(meta,run)
    reference = run['reference']
    client = Client(run['run'])
    if config.backlog_only:
        fresh_backlog(client,6,config.resume)
        return
    if config.v1_audit_only:
        recent = json.loads((BASE/'polymarket-resolution-lag-recent-checkpoint.json').read_text())
        assert any(timestamp(r.get('closedTime')) is not None and timestamp(r['closedTime']) < reference-86400 for r in recent['rows']), '24h cohort incomplete'
        v1_audit(client,recent['rows'],reference,config.sample_per_category,config.resume)
        return
    jobs = {'recent':([('closed','true'),('order','closedTime,id'),('ascending','false')],reference-config.days*86400),
            'open_low':([('closed','false'),('order','id'),('ascending','true')],None),
            'open_high':([('closed','false'),('order','id'),('ascending','false')],None),
            'proposed':([('closed','true'),('order','closedTime,id'),('ascending','false'),('uma_resolution_status','proposed')],None),
            'disputed':([('closed','true'),('order','closedTime,id'),('ascending','false'),('uma_resolution_status','disputed')],None)}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = {k:pool.submit(client.scan,k,filters,cutoff,config.resume,
                                          config.open_pivot if k.startswith('open_') else None) for k,(filters,cutoff) in jobs.items()}
        states = {k:v.result() for k,v in futures.items()}
    merged = {r['id']:r for k in ('open_low','open_high') for r in states[k]['rows']
              if (int(r['id']) <= config.open_pivot if k == 'open_low' else int(r['id']) > config.open_pivot)}
    states['open'] = {'rows':list(merged.values()),'complete':True,
                      'method':'Union of ascending IDs <= pivot and descending IDs > pivot; boundary pages consumed in full.',
                      'pivot':config.open_pivot}
    groups = collections.defaultdict(list)
    for r in states['recent']['rows']:
        close = timestamp(r.get('closedTime'))
        if close and reference-config.days*86400 <= close <= reference:
            groups[category(r)].append(r)
    selected = {}
    for rows in groups.values():
        rows.sort(key=lambda r:hashlib.sha256(r['id'].encode()).digest())
        selected.update({r['id']:r for r in rows[:config.sample_per_category]})
    candidates = {r['id']:r for k in ('proposed','disputed') for r in states[k]['rows']}
    candidates.update({r['id']:r for r in states['recent']['rows'] if r.get('umaResolutionStatus') != 'resolved'})
    candidates.update({r['id']:r for r in states['open']['rows'] if any(r.get(k) is not v for k,v in FLAGS.items())})
    selected.update(candidates)
    rows = list(selected.values())
    res = enrich(client,rows,BASE/'polymarket-resolution-lag-resolution-checkpoint.json',config.resume)
    # Verify every candidate, plus a deterministic sample of 100 resolved rows/category.
    token_rows = dict(candidates)
    for group in groups.values():
        token_rows.update({r['id']:r for r in group[:100]})
    tok = enrich(client,list(token_rows.values()),BASE/'polymarket-resolution-lag-token-checkpoint.json',config.resume,True)
    result = analyze(states,res,tok,reference,client,config)
    observation = BASE/'polymarket-resolution-lag-observation.json'
    if observation.exists():
        result['live_observation'] = json.loads(observation.read_text())
    result['v1_protocol_audit'] = v1_audit(client,states['recent']['rows'],reference,config.sample_per_category,config.resume)
    result['fresh_backlog'] = fresh_backlog(client,6,config.resume)
    result['clob_probes'] = clob_probe(client,result)
    result['resolution_sample_market_ids'] = sorted(selected)
    result['token_sample_market_ids'] = sorted(token_rows)
    finalize_protocol_summary(result)
    result['request_counts'] = dict(client.counts)
    result['request_errors'] = client.errors
    save(BASE/'polymarket-resolution-lag-results.json',result)
    print(json.dumps({k:result[k] for k in ('open','recent_closed','closure_rates','v1_lag_resolution_minus_gamma_closed_seconds','candidate_classifications','little_law')},indent=2),flush=True)

if __name__ == '__main__':
    main()
