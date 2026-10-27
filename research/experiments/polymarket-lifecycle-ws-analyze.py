"""Offline analysis of raw lifecycle frames and Gamma responses; no network calls."""
import bisect, collections, datetime as dt, json, math, sys
from pathlib import Path
run=Path(sys.argv[1]); output=Path(sys.argv[2]) if len(sys.argv)>2 else None
meta=json.loads((run/'meta.json').read_text())
def stamp(s):
    if not s:return None
    try:return dt.datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
    except ValueError:return None
def rows(file):
    with file.open() as f:
        for line in f:
            if line.strip():yield json.loads(line)
start=stamp(meta['started_at']); end=stamp(meta.get('observation_ended_at')) or dt.datetime.now(dt.timezone.utc).timestamp()
def inside(t):return t is not None and start<=t<=end
def stats(v):
    v=sorted(v)
    return dict(n=len(v),min=v[0] if v else None,p50=v[int((len(v)-1)*.5)] if v else None,p95=v[int((len(v)-1)*.95)] if v else None,p99=v[int((len(v)-1)*.99)] if v else None,max=v[-1] if v else None)
def reasons(m):
    a=[]
    for k,want in [('active',True),('closed',False),('archived',False),('enableOrderBook',True),('acceptingOrders',True)]:
        if m.get(k,False)!=want:a.append(k+'='+str(m.get(k)))
    try:
        tokens=m.get('positionIds',[]) if m.get('version')=='v2' else json.loads(m.get('clobTokenIds','[]'))
        if len(tokens)<2:a.append('missing_tokens')
    except (ValueError,TypeError):a.append('invalid_tokens')
    return a
markets={}; eligible=set(); eligible_post=set(); ever_eligible_all=set(); ever_eligible_in_capture=set(); listed=set(); first_gamma={}; closure={}; requests=[]; errors=[]; token_states={}; versions=collections.defaultdict(set); observations=collections.defaultdict(list); post_requests=[]; request_kinds=collections.Counter()
for r in rows(run/'rest.jsonl'):
    if 'sent_at' in r:
        requests.append(stamp(r['sent_at']));request_kinds[r['kind']]+=1
    if r.get('status')!=200:
        errors.append({k:v for k,v in r.items() if k not in ['raw','url']});continue
    p=json.loads(r['raw']); t=stamp(r['received_at'])
    for m in p.get('markets',([p] if p.get('id') else [])):
        i=str(m['id']); markets[i]=m; versions[i].add(m.get('version','unknown')); first_gamma.setdefault(i,t)
        if not observations[i]:observations[i].append((t,m))
        if not reasons(m):
            ever_eligible_all.add(i)
            if t<=end:ever_eligible_in_capture.add(i)
        if m.get('version')=='v1' and inside(stamp(m.get('createdAt'))) and not reasons(m):
            eligible_post.add(i)
            if t<=end:eligible.add(i)
        if r['kind']=='listing-v1-false':listed.add(i)
        if m.get('version')=='v1' and inside(stamp(m.get('closedTime'))):closure[i]=m
if (run/'tokens.jsonl').exists():
    for r in rows(run/'tokens.jsonl'):
        post_requests.append({k:v for k,v in r.items() if k not in ['raw','url','headers']})
        if r.get('status')==200:
            for x in json.loads(r['raw']).get('data',[]):token_states[x['token_id']]=x
resolution_states={}
if (run/'resolutions.jsonl').exists():
    for r in rows(run/'resolutions.jsonl'):
        post_requests.append({k:v for k,v in r.items() if k not in ['raw','url','headers']})
        if r.get('status')==200:
            for x in json.loads(r['raw']).get('data',[]):resolution_states[x['condition_id']]=x
sockets=[]; events=collections.defaultdict(list); union_times={}; all_events=[]; all_pongs=[]; all_received=[]
for f in sorted(run.glob('socket-*.jsonl')):
    counts=collections.Counter(); ids_by_type=collections.defaultdict(set); times=[]; pongs=[]; pings=[]; disconnects=[]; fingerprints=collections.Counter(); received=0
    for r in rows(f):
        t=stamp(r['received_at'])
        if r['kind'] in ['closed','error','opened']:disconnects.append({k:v for k,v in r.items() if k!='mono_ms'})
        if r['kind']=='sent' and r.get('raw')=='PING':pings.append(t)
        if r['kind']!='received':continue
        received+=1;all_received.append(t)
        if r['raw']=='PONG':pongs.append(t);all_pongs.append(t);continue
        try:p=json.loads(r['raw'])
        except ValueError:counts['non_json']+=1;continue
        for x in p if isinstance(p,list) else [p]:
            kind=x.get('event_type','unknown');counts[kind]+=1;i=str(x.get('id',x.get('market','unknown')));ids_by_type[kind].add(i)
            fingerprints[json.dumps(x,sort_keys=True)]+=1
            if kind in ['new_market','market_resolved']:
                events[(kind,i)].append((t,f.stem,x));all_events.append((t,kind,i,f.stem));times.append(t);union_times[(kind,i)]=min(union_times.get((kind,i),t),t)
    gaps=[dict(start=dt.datetime.fromtimestamp(a,dt.timezone.utc).isoformat(),end=dt.datetime.fromtimestamp(b,dt.timezone.utc).isoformat(),seconds=b-a) for a,b in zip([start]+times,times+[end]) if b-a>=10]
    down=[];last_close=None
    for c in disconnects:
        if c['kind']=='closed' and not c['expected']:last_close=c
        if c['kind']=='opened' and last_close:
            down.append(dict(closed_at=last_close['received_at'],opened_at=c['received_at'],seconds=stamp(c['received_at'])-stamp(last_close['received_at'])))
            last_close=None
    sockets.append(dict(unexpected_disconnects=sum(c['kind']=='closed' and not c['expected'] for c in disconnects),reconnect_intervals=down,socket=f.stem,received_frames=received,counts=dict(counts),unique={k:len(v) for k,v in ids_by_type.items()},duplicates={k:counts[k]-len(v) for k,v in ids_by_type.items()},lifecycle_gaps_ge_10s=gaps,pong_gaps_ge_20s=[dict(start=dt.datetime.fromtimestamp(a,dt.timezone.utc).isoformat(),end=dt.datetime.fromtimestamp(b,dt.timezone.utc).isoformat(),seconds=b-a) for a,b in zip(pongs,pongs[1:]) if b-a>=20],max_sent_ping_gap_seconds=max([b-a for a,b in zip(pings,pings[1:])],default=None),max_pong_gap_seconds=max([b-a for a,b in zip(pongs,pongs[1:])],default=None),connection_events=disconnects,_sets=ids_by_type,_fingerprints=fingerprints))
new={i for kind,i in events if kind=='new_market'}; resolved={i for kind,i in events if kind=='market_resolved'}
precision=collections.Counter(); failures=collections.Counter(); event_latency=[]; missing=[]
for i in new:
    m=markets.get(i)
    if not m:precision['unknown']+=1;missing.append(i);continue
    precision[m.get('version','unknown')]+=1
    why=reasons(m);precision['eligible' if not why else 'ineligible']+=1;failures.update(why)
    c=stamp(m.get('createdAt'))
    if c is not None:event_latency.append(union_times[('new_market',i)]-c)
resolved_gamma={i for i,m in closure.items() if m.get('umaResolutionStatus')=='resolved'}
resolved_tokens=set()
for i,m in closure.items():
    tokens=json.loads(m.get('clobTokenIds','[]'))
    if tokens and all(token_states.get(t,{}).get('resolved') is True for t in tokens):resolved_tokens.add(i)
latency_eligible=[union_times[('new_market',i)]-stamp(markets[i]['createdAt']) for i in eligible&new]
creation_order=sorted([(int(i),stamp(m.get('createdAt'))) for i,m in markets.items() if stamp(m.get('createdAt')) is not None])
inversions=[dict(lower_id=str(a[0]),higher_id=str(b[0]),lower_id_created_at=markets[str(a[0])]['createdAt'],higher_id_created_at=markets[str(b[0])]['createdAt'],seconds=a[1]-b[1]) for a,b in zip(creation_order,creation_order[1:]) if a[1]>b[1]]
shared=sorted(set(t for t,k,i,s in all_events)); shared_gaps=[dict(start=dt.datetime.fromtimestamp(a,dt.timezone.utc).isoformat(),end=dt.datetime.fromtimestamp(b,dt.timezone.utc).isoformat(),seconds=b-a) for a,b in zip([start]+shared,shared+[end]) if b-a>=10]
sets_equal=all(s['_sets']==sockets[0]['_sets'] for s in sockets)
payloads_equal=all(s['_fingerprints']==sockets[0]['_fingerprints'] for s in sockets)
for s in sockets:del s['_sets'];del s['_fingerprints']
summary=dict(run=str(run),meta=meta,complete='observation_ended_at' in meta,duration_seconds=end-start,rest=dict(requests=len(requests),errors=errors,min_request_spacing_seconds=min([b-a for a,b in zip(requests,requests[1:])],default=None)),new_market=dict(unique_ids=len(new),precision_counts=dict(precision),ineligibility_reasons=dict(failures),unknown_ids=sorted(missing,key=int),coverage_created_eligible_v1=None),sockets=sockets)
# Coverage still needs internal per-socket sets, reconstructed cheaply from indexed events.
def cov(den,kind):
    observed={i for k,i in events if k==kind};return dict(denominator=len(den),any_socket_count=len(den&observed),any_socket_fraction=len(den&observed)/len(den) if den else None,per_socket=[dict(socket=s['socket'],missing_ids=sorted([i for i in den if not any(sock==s['socket'] for _,sock,_ in events.get((kind,i),[]))],key=int),count=sum(any(sock==s['socket'] for _,sock,_ in events.get((kind,i),[])) for i in den),fraction=sum(any(sock==s['socket'] for _,sock,_ in events.get((kind,i),[])) for i in den)/len(den) if den else None) for s in sockets],missed_by_all=sorted(den-observed,key=int))
summary['new_market'].update(coverage_created_eligible_v1=cov(eligible,'new_market'),created_at_to_receipt_seconds=stats(event_latency),eligible_v1_created_at_to_receipt_seconds=stats(latency_eligible),sample_ids=sorted(new,key=int)[:20],classification_basis='latest Gamma observation; eligible denominator uses any eligible observation of v1 creations inside window',version_fraction={v:precision[v]/len(new) if new else None for v in ['v1','v2','unknown']})
actual_resolved_inside={i for i,m in closure.items() if inside(stamp(resolution_states.get(m['conditionId'],{}).get('resolved_at')))}
summary.update(market_resolved=dict(resolution_timestamp_inside_window_coverage=cov(actual_resolved_inside,'market_resolved'),resolution_state_statuses=dict(collections.Counter(x.get('status','unknown') for x in resolution_states.values())),unique_events=len(resolved),closed_v1=len(closure),uma_resolved_coverage=cov(resolved_gamma,'market_resolved'),tokens_resolved_coverage=cov(resolved_tokens,'market_resolved'),closed_time_to_receipt_seconds=stats([union_times[('market_resolved',i)]-stamp(closure[i]['closedTime']) for i in resolved&set(closure)]),closed_sample_ids=sorted(closure,key=int)[:20],uma_resolved_sample_ids=sorted(resolved_gamma,key=int)[:20],outcome_prices_at_extremes=sum(sorted(map(float,json.loads(m.get('outcomePrices','[]'))))==[0.0,1.0] for m in closure.values())),agreement=dict(shared_received_frame_gaps_ge_20s=[dict(start=dt.datetime.fromtimestamp(a,dt.timezone.utc).isoformat(),end=dt.datetime.fromtimestamp(b,dt.timezone.utc).isoformat(),seconds=b-a) for a,b in zip(sorted(all_received),sorted(all_received)[1:]) if b-a>=20],shared_pong_gaps_ge_20s=[dict(start=dt.datetime.fromtimestamp(a,dt.timezone.utc).isoformat(),end=dt.datetime.fromtimestamp(b,dt.timezone.utc).isoformat(),seconds=b-a) for a,b in zip(sorted(all_pongs),sorted(all_pongs)[1:]) if b-a>=20],event_sets_equal=sets_equal,full_payload_multiplicities_equal=payloads_equal,shared_lifecycle_gaps_ge_10s=shared_gaps),id_monotonicity=dict(observed_markets=len(creation_order),adjacent_creation_inversions=len(inversions),samples=inversions[:20]))
v1_creation_order=[x for x in creation_order if markets[str(x[0])].get('version')=='v1']
v1_inversions=[dict(lower_id=str(a[0]),higher_id=str(b[0]),seconds=a[1]-b[1]) for a,b in zip(v1_creation_order,v1_creation_order[1:]) if a[1]>b[1]]
summary['id_monotonicity']['v1_only']=dict(observed_markets=len(v1_creation_order),adjacent_creation_inversions=len(v1_inversions),largest_inversion_seconds=max((x['seconds'] for x in v1_inversions),default=None),samples=v1_inversions[:20])
summary['id_monotonicity']['event_first_arrival_below_prior_max']={}
for sock in [s['socket'] for s in sockets]:
    seen=set();maximum=0;backwards=[]
    for t,k,i,s in sorted(all_events):
        if s!=sock or k!='new_market' or i in seen:continue
        seen.add(i)
        if int(i)<maximum:backwards.append(dict(id=i,prior_max_id=str(maximum)))
        maximum=max(maximum,int(i))
    summary['id_monotonicity']['event_first_arrival_below_prior_max'][sock]=dict(count=len(backwards),samples=backwards[:20])
summary['new_market']['coverage_v1_created_first_60_minutes_eligible_during_capture']=cov({i for i in eligible if stamp(markets[i]['createdAt'])<=start+3600},'new_market')
summary['new_market']['coverage_v1_created_eligible_confirmed_including_postchecks']=cov(eligible_post,'new_market')
summary['new_market']['postcheck_only_eligible_ids']=sorted(eligible_post-eligible,key=int)
summary['new_market']['per_socket_created_at_latency_seconds']={sock:stats([min(t for t,s,x in events[('new_market',i)] if s==sock)-stamp(markets[i]['createdAt']) for i in new if i in markets and stamp(markets[i].get('createdAt')) is not None and any(s==sock for t,s,x in events[('new_market',i)])]) for sock in [s['socket'] for s in sockets]}
summary['new_market']['repeated_ids_per_socket']={sock:{i:sum(s==sock for t,s,x in events[('new_market',i)]) for i in new if sum(s==sock for t,s,x in events[('new_market',i)])>1} for sock in [s['socket'] for s in sockets]}
arrival_bins=collections.Counter(int((union_times[('new_market',i)]-start)//60) for i in new)
summary['new_market']['distinct_ids_per_minute']=dict(sorted(arrival_bins.items()))
summary['new_market']['average_distinct_ids_per_second']=len(new)/(end-start) if end>start else None
summary['new_market']['eligible_fraction']=precision['eligible']/len(new) if new else None
summary['new_market']['ineligible_fraction']=precision['ineligible']/len(new) if new else None
summary['new_market']['version_eligibility_counts']=dict(collections.Counter(markets[i].get('version','unknown')+(' eligible' if not reasons(markets[i]) else ' ineligible') for i in new if i in markets))
summary['new_market']['created_v1_records_in_window']=sum(m.get('version')=='v1' and inside(stamp(m.get('createdAt'))) for m in markets.values())
summary['new_market']['event_active_field_by_unique_id']=dict(collections.Counter(str(events[('new_market',i)][0][2].get('active')) for i in new))
summary['new_market']['event_active_false_but_gamma_eligible_ids']=sorted([i for i in new if events[('new_market',i)][0][2].get('active') is False and i in markets and not reasons(markets[i])],key=int)
summary['new_market']['event_timestamp_to_receipt_seconds']=stats([t-float(x['timestamp'])/1000 for (k,i),items in events.items() if k=='new_market' for t,s,x in items if str(x.get('timestamp','')).isdigit()])
summary['new_market']['ever_observed_eligible_during_capture_count']=len(new&ever_eligible_in_capture)
summary['new_market']['ever_observed_eligible_including_postchecks_count']=len(new&ever_eligible_all)
summary['new_market']['first_gamma_ineligibility_reasons']=dict(collections.Counter(reason for i in new if observations[i] for reason in reasons(observations[i][0][1])))
summary['new_market']['latest_ineligible_sample_ids']=sorted([i for i in new if i in markets and reasons(markets[i])],key=int)[:20]
summary['new_market']['first_gamma_eligibility_counts']=dict(collections.Counter('eligible' if not reasons(observations[i][0][1]) else 'ineligible' for i in new if observations[i]))
summary['market_resolved']['resolution_timestamp_availability']=dict(inside_window=len(actual_resolved_inside),missing=sum(not resolution_states.get(m['conditionId'],{}).get('resolved_at') for m in closure.values()))
summary['market_resolved']['resolution_timestamp_samples']=[dict(id=i,closed_time=closure[i]['closedTime'],resolved_at=resolution_states[closure[i]['conditionId']]['resolved_at']) for i in sorted(actual_resolved_inside,key=int)[:10]]
summary['market_resolved']['resolved_first_60_minutes_coverage']=cov({i for i in actual_resolved_inside if stamp(resolution_states[closure[i]['conditionId']]['resolved_at'])<=start+3600},'market_resolved')
summary['rest'].update(request_kinds=dict(request_kinds),live_requests=sum(v for k,v in request_kinds.items() if not k.startswith('post-')),live_errors=[r for r in errors if not r.get('kind','').startswith('post-')],post_gamma_errors=[r for r in errors if r.get('kind','').startswith('post-')],max_requests_in_rolling_one_second=max((j-bisect.bisect_right(requests,t-1,hi=j)+1 for j,t in enumerate(requests)),default=0))
summary['verification']=dict(post_window_data_requests=len(post_requests),errors=[r for r in post_requests if r.get('status')!=200],finished_at=max((r['received_at'] for r in post_requests),default=None),token_rows=len(token_states),resolution_state_conditions=len(resolution_states),closed_markets_with_both_tokens_found=sum(bool(json.loads(m.get('clobTokenIds','[]'))) and all(t in token_states for t in json.loads(m.get('clobTokenIds','[]'))) for m in closure.values()),closed_markets_with_resolution_state=sum(m['conditionId'] in resolution_states for m in closure.values()))
text=json.dumps(summary,indent=2)+'\n'
if output:output.write_text(text)
else:print(text)

