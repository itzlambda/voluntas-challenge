import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import dns from 'node:dns';
import net from 'node:net';
import dc from 'node:diagnostics_channel';
import {monitorEventLoopDelay} from 'node:perf_hooks';
import {createHash} from 'node:crypto';

// One distinct binary market / two assets per socket, all on one event loop.
const [output, maximumText='1024', holdText='60', scenario='standard', firstTargetText='2000', targetsText=''] = process.argv.slice(2);
const maximum=Number(maximumText), finalHold=Number(holdText);
const slow=scenario==='active-scale-slow',scale=scenario==='active-scale'||slow,active=scenario.startsWith('active'),resilient=scenario==='active-observe'||scale,maintained=scenario==='active-maintained'||resilient,firstTarget=Number(firstTargetText);
const explicitTargets=targetsText?targetsText.split(',').map(Number):[];
if(!['standard','active','active-maintained','active-observe','active-scale','active-scale-slow'].includes(scenario)||!Number.isInteger(firstTarget)||firstTarget<1||!output?.startsWith('recordings/subscription-capacity-connections-')||!Number.isInteger(maximum)||maximum<1||maximum>(scale?32000:active?4000:1024)||!Number.isInteger(finalHold)||finalHold<(active?300:30)||finalHold>(active?600:120)||scale&&firstTarget>maximum||explicitTargets.some((n,i)=>!Number.isInteger(n)||n<firstTarget||n>maximum||i>0&&n<=explicitTargets[i-1]))throw new Error('Usage: node experiments/polymarket-subscription-capacity-connections.mjs recordings/subscription-capacity-connections-NAME MAX HOLD_SECONDS SCENARIO FIRST_TARGET [TARGETS_COMMA_SEPARATED]');
const rampInterval=slow?50:scale?25:resilient?50:active?100:500,maxInFlight=slow?16:scale?32:resilient?16:8,transportBudget=scale?64:8,initializationTimeout=slow?60000:15000;
fs.mkdirSync(output,{recursive:false});fs.writeFileSync(path.join(output,'.gitignore'),'*\n');
const sourceBytes=fs.readFileSync(new URL(import.meta.url));
fs.writeFileSync(path.join(output,'harness.mjs'),sourceBytes);
dns.setDefaultResultOrder('ipv4first');net.setDefaultAutoSelectFamily(false);
const endpoint='wss://ws-subscriptions-clob.polymarket.com/ws/market';
const traceUrl='https://ws-subscriptions-clob.polymarket.com/cdn-cgi/trace';
const gamma='https://gamma-api.polymarket.com';
const origin=process.hrtime.bigint();const mono=()=>Number(process.hrtime.bigint()-origin)/1e6;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const save=(name,value)=>fs.writeFileSync(path.join(output,name),JSON.stringify(value,null,2)+'\n');
const events=fs.createWriteStream(path.join(output,'events.jsonl'),{flags:'wx'});
const frames=fs.createWriteStream(path.join(output,'frames.jsonl'),{flags:'wx'});
let stopReason=null,stopping=false,maximumQueuedBytes=0,bytes=0,peakRss=0,sampler,heartbeat,progress;
let retainedBytes=0,marketUpdates=0,changeEntries=0,callbackWorkMs=0,callbackMaxMs=0;
const connections=[],stages=[],localAddresses=new Set(),remoteAddresses=new Set();
const currentConnections=()=>connections.filter(c=>!c.retired);
const lifecyclePending=new Set();let lifecycleEpoch=0;
let transportRetirements=0,transportBackoffUntil=0;
const localsByWs=new Set(),remoteFamilies=new Set(),transportSockets=new Set();
const diagnosticErrors=[],handshakes=[];
let wsTransportCount=0;
function safeMessage(value){return String(value??'').replaceAll(process.cwd(),'[repository]').replaceAll(os.homedir(),'~').replace(/\b(?:\d{1,3}\.){3}\d{1,3}\b/g,'[address]');}
function errorFields(e){return {name:e?.name??null,code:e?.code??null,message:safeMessage(e?.message??e),cause:e?.cause?{name:e.cause.name??null,code:e.cause.code??null,message:safeMessage(e.cause.message)}:null};}
function write(stream,row){stream.write(JSON.stringify(row)+'\n');maximumQueuedBytes=Math.max(maximumQueuedBytes,stream.writableLength);if(stream.writableLength>16*1024*1024)stopReason??='client_output_queue_guard';}
const event=(kind,data={})=>write(events,{mono_ms:mono(),kind,...data});
events.on('error',()=>{stopReason??='event_output_error';});frames.on('error',()=>{stopReason??='frame_output_error';});
dc.channel('undici:client:connected').subscribe(({socket,connectParams})=>{
  if(connectParams.hostname!=='ws-subscriptions-clob.polymarket.com')return;
  localAddresses.add(socket.localAddress);remoteAddresses.add(socket.remoteAddress);transportSockets.add(socket);
  socket.once('close',()=>transportSockets.delete(socket));
});
dc.channel('undici:websocket:open').subscribe(message=>{
  // The diagnostic callback follows the public onopen callback.
  const c=connections.find(c=>c.socket===message.websocket);
  if(!c)return;
  wsTransportCount++;handshakes.push(message.handshakeResponse.status);
  const a=message.address;
  if(a?.address)localsByWs.add(a.address);
  if(a?.family)remoteFamilies.add(a.family);
  event('handshake',{connection:c.index,status:message.handshakeResponse.status});
});
dc.channel('undici:websocket:socket_error').subscribe(e=>{const detail=errorFields(e);diagnosticErrors.push(detail);event('transport_error',detail);});
dc.channel('undici:request:headers').subscribe(({request,response})=>{
  if(String(request.origin)!=='https://ws-subscriptions-clob.polymarket.com'||request.path!=='/ws/market')return;
  event('http_response',{status:response.statusCode});
  if(response.statusCode!==101)stopReason??=`upgrade_http_${response.statusCode}`;
});
process.on('SIGINT',()=>{stopReason??='interrupted';});process.on('SIGTERM',()=>{stopReason??='terminated';});
async function get(url){const r=await fetch(url,{signal:AbortSignal.timeout(15000)});if(!r.ok)throw new Error(`HTTP ${r.status}`);return r.json();}
async function trace(){try{const r=await fetch(traceUrl,{signal:AbortSignal.timeout(10000)});const body=await r.text();return {status:r.status,ip:/^ip=(.+)$/m.exec(body)?.[1]??null};}catch(e){return {status:null,error:errorFields(e),ip:null};}}
const array=v=>typeof v==='string'?JSON.parse(v):v;
function eligible(m){
  if(!(m.active&&!m.closed&&!m.archived&&m.acceptingOrders&&m.enableOrderBook))return null;
  const assets=array(m.version==='v2'?m.positionIds:m.clobTokenIds);
  if(!Array.isArray(assets)||assets.length!==2||new Set(assets).size!==2||array(m.outcomes)?.length!==2)return null;
  return {id:m.id,version:m.version,assets,volume24hr:m.volume24hr,active:m.active,closed:m.closed,archived:m.archived,acceptingOrders:m.acceptingOrders,enableOrderBook:m.enableOrderBook};
}
const startedAt=new Date().toISOString();const cpuStart=process.cpuUsage();
const loop=monitorEventLoopDelay({resolution:5});loop.enable();
const progressLoop=monitorEventLoopDelay({resolution:5});progressLoop.enable();
const protocol={claim:'Determine a tested lower bound or reproducible operational boundary for simultaneous market WebSockets from one observed egress; no assumption that a timeout is an IP connection cap.',endpoint,maximum_connections:maximum,one_market_per_socket:true,assets_per_socket:2,opens_per_second_maximum:2,initialization_timeout_seconds:15,stage_hold_seconds:15,final_hold_seconds:finalHold,ping_interval_seconds:10,custom_feature_enabled:false,initial_dump:true,address_family:'IPv4 first, automatic family selection disabled',success:'Both valid book snapshots on every requested socket, OPEN state, at least one PONG per socket, PONG age <=15 seconds, no unexpected close or error during hold',stop_policy:'Stop ramp on any failure or throttling; no replacement connections; graceful shutdown and no automatic retry',client_guards:{rss_bytes:768*1024*1024,queued_output_bytes:16*1024*1024,total_received_bytes:256*1024*1024,wall_seconds:1000},host:{logical_cores:os.availableParallelism(),load_average:os.loadavg()},script_sha256:createHash('sha256').update(sourceBytes).digest('hex')};
save('protocol.json',protocol);
if(resilient){protocol.opens_per_second_maximum=20;protocol.maximum_initializations_in_flight=16;protocol.transport_recovery={maximum_disconnects:8,backoff_seconds:5,allowed_close_code:1006};save('protocol.json',protocol);}
protocol.hold_policy=maintained?'Fixed wall-clock window, replace lifecycle retirements and report population dips':'Restart full hold after each lifecycle retirement';
save('protocol.json',protocol);
Object.assign(protocol,active?{maximum_connections:maximum,opens_per_second_maximum:10,maximum_initializations_in_flight:8,stage_hold_seconds:finalHold,market_selection:'All markets ranked by observed Gamma volume24hr descending, eligible binary only',lifecycle_policy:'Replace only server-resolved markets, revalidate metadata, and restart the full hold; retain every attempt',stop_policy:'Stop on non-lifecycle disconnect, error, throttling, failed lifecycle revalidation, or client guard',frame_recording:'All initial snapshot frames and PONGs, then at most one update frame per connection per ten seconds',client_guards:{rss_bytes:2*1024**3,queued_output_bytes:16*1024*1024,total_received_bytes:16*1024**3,retained_evidence_bytes:4*1024**3,wall_seconds:2400}}:{});
save('protocol.json',protocol);
let before=null,after=null,catalog=[];
if(resilient){Object.assign(protocol,{opens_per_second_maximum:20,maximum_initializations_in_flight:16,stop_policy:'Record and replace at most eight code-1006 disconnects with five-second backoff; stop on other non-lifecycle closes, overload, throttling, or client guards'});save('protocol.json',protocol);}
if(scale){Object.assign(protocol,{opens_per_second_maximum:1000/rampInterval,maximum_initializations_in_flight:maxInFlight,transport_recovery:{maximum_disconnects:transportBudget,backoff_seconds:5,allowed_close_code:1006},lifecycle_policy:'Retire unavailable/resolved markets with recorded evidence; replace and report every population dip without restarting the observation',stop_policy:'Stop on explicit overload/throttling, non-recoverable close/error, exhausted transport budget, or local resource guard',client_guards:{rss_bytes:Math.min(5*1024**3,Math.max(1024**3,os.freemem()-3*1024**3)),minimum_free_host_bytes:2*1024**3,queued_output_bytes:16*1024*1024,total_received_bytes:64*1024**3,retained_evidence_bytes:4*1024**3,wall_seconds:3600},planned_targets:[]});save('protocol.json',protocol);}
protocol.initialization_timeout_seconds=initializationTimeout/1000;save('protocol.json',protocol);
try{
  before=await trace();
  const pool=new Map();
  if(!active){const liquid=await get(`${gamma}/markets/keyset?limit=100&order=volume24hr&ascending=false&active=true&closed=false`);for(const m of liquid.markets.map(eligible).filter(Boolean).slice(0,5))pool.set(m.id,m);}
  let cursor=null,pages=0;
  const poolTarget=active?maximum+(scale?Math.max(500,Math.ceil(maximum*.1)):500):maximum;
  while(pool.size<poolTarget&&!stopReason){
    const url=new URL(`${gamma}/markets/keyset`);Object.entries({limit:'100',order:active?'volume24hr':'id',ascending:active?'false':'true',active:'true',closed:'false',...(cursor?{after_cursor:cursor}:{})}).forEach(([k,v])=>url.searchParams.set(k,v));
    const page=await get(url);pages++;
    for(const m of page.markets.map(eligible).filter(Boolean))if(pool.size<poolTarget)pool.set(m.id,m);
    if(!page.next_cursor||!page.markets.length)break;
    if(pages%10===0)console.log(JSON.stringify({phase:'discovery',pages,eligible_markets:pool.size}));
    if(cursor===page.next_cursor||pages>(scale?400:100))throw new Error('Discovery pagination guard');cursor=page.next_cursor;await sleep(100);
  }
  catalog=[...pool.values()];if(active)catalog.sort((a,b)=>Number(b.volume24hr)-Number(a.volume24hr));catalog=catalog.slice(0,poolTarget);
  save('catalog.json',{fetched_at:new Date().toISOString(),pages,selection:protocol.market_selection??'Five volume leaders followed by ascending ID',markets:catalog});
  if(catalog.length<maximum&&!scale||scale&&catalog.length<firstTarget)throw new Error('Eligible market pool below requested starting count');
  const runMaximum=Math.min(maximum,catalog.length);
  protocol.available_eligible_markets=catalog.length;protocol.effective_connection_guard=runMaximum;save('protocol.json',protocol);
  event('discovered',{markets:catalog.length,assets:catalog.length*2});
  sampler=setInterval(()=>{
    peakRss=Math.max(peakRss,process.memoryUsage().rss);
    if(peakRss>protocol.client_guards.rss_bytes)stopReason??='client_rss_guard';
    if(scale&&os.freemem()<protocol.client_guards.minimum_free_host_bytes)stopReason??='client_host_free_memory_guard';
    if(bytes>protocol.client_guards.total_received_bytes)stopReason??='client_received_bytes_guard';
    if(retainedBytes>(protocol.client_guards.retained_evidence_bytes??Infinity))stopReason??='client_retained_evidence_guard';
    if(mono()>protocol.client_guards.wall_seconds*1000)stopReason??='client_wall_time_guard';
  },500);
  let previousBytes=0,previousUpdates=0,previousSample=mono(),previousCpu=process.cpuUsage();
  progress=setInterval(()=>{const now=mono(),cpu=process.cpuUsage(previousCpu);const row={connections:currentConnections().length,attempts:connections.length,open:connections.filter(c=>c.socket.readyState===WebSocket.OPEN).length,lifecycle_replacements:connections.filter(c=>c.retired&&c.retirement_reason!=='transport_disconnect').length,transport_replacements:transportRetirements,bytes_per_second:(bytes-previousBytes)/((now-previousSample)/1000),market_updates_per_second:(marketUpdates-previousUpdates)/((now-previousSample)/1000),cpu_cores:(cpu.user+cpu.system)/((now-previousSample)*1000),event_loop_p99_ms:progressLoop.percentile(99)/1e6,event_loop_max_ms:progressLoop.max/1e6,stale_heartbeat_connections:connections.filter(c=>!c.retired&&c.complete_ms!==null&&c.lastPongMs!==null&&now-c.lastPongMs>15000).length,rss_bytes:process.memoryUsage().rss,host_free_bytes:os.freemem(),open_file_descriptors:fs.readdirSync('/proc/self/fd').length,stop_reason:stopReason};event('traffic_sample',row);console.log(JSON.stringify({phase:'progress',...row}));previousBytes=bytes;previousUpdates=marketUpdates;previousSample=now;previousCpu=process.cpuUsage();progressLoop.reset();},30000);
  heartbeat=setInterval(()=>{
    for(const c of connections)if(c.socket.readyState===WebSocket.OPEN&&mono()-c.lastPingMs>=10000){c.lastPingMs=mono();c.pings++;c.socket.send('PING');}
  },250);
  async function open(m){
    const created=mono();
    const c={index:connections.length+1,market:m.id,assets:m.assets,created_ms:created,opened_ms:null,complete_ms:null,books:new Set(),lastPingMs:0,lastPongMs:null,pings:0,pongs:0,pong_rtt_ms:[],frames:0,bytes:0,events:{},errors:[],closes:[],max_callback_ms:0,market_updates:0,price_change_entries:0,lastRetainedUpdateMs:-Infinity};
    const socket=new WebSocket(endpoint);c.socket=socket;connections.push(c);
    socket.onopen=()=>{c.opened_ms=mono();socket.send(JSON.stringify({type:'market',assets_ids:m.assets,initial_dump:true,custom_feature_enabled:false}));c.lastPingMs=mono();c.pings++;socket.send('PING');event('opened',{connection:c.index,market:m.id});};
    socket.onmessage=e=>{
      const receipt=mono();
      if(typeof e.data!=='string'){c.errors.push({kind:'binary'});stopReason??='unexpected_binary';return;}
      const raw=e.data;const n=Buffer.byteLength(raw);c.frames++;c.bytes+=n;bytes+=n;
      let retain=!active||raw==='PONG'||receipt-c.lastRetainedUpdateMs>=10000;
      if(raw==='PONG'){c.pongs++;c.lastPongMs=receipt;c.pong_rtt_ms.push(receipt-c.lastPingMs);}
      else try{
        const decoded=JSON.parse(raw);
        for(const item of Array.isArray(decoded)?decoded:[decoded]){
          const type=item.event_type??'unknown';c.events[type]=(c.events[type]??0)+1;
          if(type==='book'&&m.assets.includes(item.asset_id)&&Array.isArray(item.bids)&&Array.isArray(item.asks)){if(!c.books.has(item.asset_id))retain=true;else{c.market_updates++;marketUpdates++;}c.books.add(item.asset_id);}
          else if(type!=='new_market'&&type!=='unknown'){c.market_updates++;marketUpdates++;}
          if(type==='price_change'){const entries=(item.price_changes??[]).filter(x=>m.assets.includes(x.asset_id)).length;c.price_change_entries+=entries;changeEntries+=entries;}
          if(type==='error'||item.error){c.errors.push({kind:'server',payload:item});stopReason??='server_error';}
        }
        if(c.books.size===2&&c.complete_ms===null)c.complete_ms=receipt;
      }catch(e){c.errors.push({kind:'parse',...errorFields(e)});stopReason??='parse_error';}
      if(retain){write(frames,{mono_ms:receipt,connection:c.index,raw});retainedBytes+=n;if(raw!=='PONG')c.lastRetainedUpdateMs=receipt;}
      const work=mono()-receipt;c.max_callback_ms=Math.max(c.max_callback_ms,work);callbackWorkMs+=work;callbackMaxMs=Math.max(callbackMaxMs,work);
    };
    socket.onerror=e=>{const detail=errorFields(e.error??e);c.errors.push(detail);event('socket_error',{connection:c.index,...detail});if(!stopping&&!resilient)stopReason??='socket_error';};
    socket.onclose=e=>{
      const row={mono_ms:mono(),code:e.code,reason:safeMessage(e.reason),wasClean:e.wasClean,expected:stopping||!!c.retired};c.closes.push(row);event('closed',{connection:c.index,...row});
      if(stopping||c.retired)return;
      if(resilient&&e.code===1006&&!stopReason){c.retired=true;c.retirement_reason='transport_disconnect';transportRetirements++;lifecycleEpoch++;transportBackoffUntil=mono()+5000;event('transport_retired',{connection:c.index,market:m.id,disconnects:transportRetirements});if(transportRetirements>transportBudget)stopReason='transport_disconnect_budget_exceeded';return;}
      if(active&&e.code===1000&&e.reason==='all subscribed assets resolved'){
        c.retired=true;c.retirement_reason='stream_declared_resolved';lifecycleEpoch++;
        const p=get(`${gamma}/markets/${m.id}`).then(fresh=>{c.lifecycle_revalidation={active:fresh.active,closed:fresh.closed,archived:fresh.archived,acceptingOrders:fresh.acceptingOrders,agrees_with_stream:!!fresh.closed||!fresh.acceptingOrders};event('lifecycle_revalidated',{connection:c.index,market:m.id,...c.lifecycle_revalidation});}).catch(error=>{c.lifecycle_revalidation={error:errorFields(error)};event('lifecycle_revalidation_error',{connection:c.index,...c.lifecycle_revalidation});if(/429|throttl|rate.limit/i.test(error.message))stopReason??='metadata_throttling';}).finally(()=>lifecyclePending.delete(p));
        lifecyclePending.add(p);
      }else stopReason??=`unexpected_close_${e.code}`;
    };
    while(!stopReason&&!c.retired&&(c.complete_ms===null||!c.pongs)&&mono()-created<initializationTimeout)await sleep(25);
    if(!c.retired&&(c.complete_ms===null||!c.pongs)&&!stopReason){
      if(active&&c.pongs>0&&c.complete_ms===null){
        try{
          const fresh=await get(`${gamma}/markets/${m.id}`);
          c.lifecycle_revalidation={active:fresh.active,closed:fresh.closed,archived:fresh.archived,acceptingOrders:fresh.acceptingOrders,enableOrderBook:fresh.enableOrderBook};
          if(!eligible(fresh)){
            c.retired=true;c.retirement_reason='metadata_ineligible_before_initial_snapshot';lifecycleEpoch++;event('ineligible_market_retired',{connection:c.index,market:m.id,metadata:c.lifecycle_revalidation});socket.close(1000,'Retired ineligible market');
          }else{
            const checks=[];
            for(const asset of m.assets){const response=await fetch(`https://clob.polymarket.com/book?token_id=${asset}`,{signal:AbortSignal.timeout(15000)});const body=await response.json();checks.push({asset,http_status:response.status,error:body.error??null});if(response.status===429)stopReason??='book_api_throttling';}
            c.book_revalidation=checks;
            if(!stopReason&&checks.every(x=>x.http_status===404)){c.retired=true;c.retirement_reason='book_api_unavailable_before_initial_snapshot';lifecycleEpoch++;event('unavailable_books_retired',{connection:c.index,market:m.id,checks});socket.close(1000,'Retired unavailable books');}
            else stopReason??='initialization_timeout_inconclusive';
          }
        }catch(error){c.initialization_revalidation_error=errorFields(error);stopReason??=/429|throttl|rate.limit/i.test(error.message)?'metadata_throttling':'initialization_timeout_inconclusive';}
      }else stopReason='initialization_timeout_inconclusive';
    }
    if(stopReason&&c.books.size<2){try{const current=await get(`${gamma}/markets/${m.id}`);c.lifecycle_revalidation={eligible:!!eligible(current),assets:eligible(current)?.assets??null};}catch(e){c.lifecycle_revalidation={error:errorFields(e)};}}
    await sleep(Math.max(0,rampInterval-(mono()-created)));
  }
  const targets=[];if(scale&&explicitTargets.length){targets.push(...explicitTargets.filter(n=>n<=runMaximum));if(targets.at(-1)!==runMaximum)targets.push(runMaximum);}else if(scale){for(let n=firstTarget;n<runMaximum;n*=2)targets.push(n);targets.push(runMaximum);}else if(active){if(maximum>2000&&firstTarget===2000)targets.push(2000);targets.push(maximum);}else{for(let n=1;n<maximum;n*=2)targets.push(n);targets.push(maximum);}
  protocol.planned_targets=targets;save('protocol.json',protocol);
  async function populate(target){
    if(active){do{const pending=new Set();while(currentConnections().length<target&&!stopReason){if(pending.size>=maxInFlight)await Promise.race(pending);if(stopReason)break;if(mono()<transportBackoffUntil)await sleep(transportBackoffUntil-mono());if(!catalog[connections.length]){stopReason='lifecycle_reserve_exhausted';break;}const p=open(catalog[connections.length]).finally(()=>pending.delete(p));pending.add(p);await sleep(active?rampInterval:100);}await Promise.allSettled(pending);await Promise.allSettled(lifecyclePending);}while(!stopReason&&currentConnections().length<target);}
    else while(connections.length<target&&!stopReason)await open(catalog[connections.length]);
  }
  for(const target of targets){
    if(stopReason)break;
    const stageStart=mono();
    await populate(target);
    let holdStart,baselineBytes,cpu,baselineUpdates,baselineChanges,baselineWork,perMarketBaseline,holdIndices,epoch,holdRestarts=0;
    function beginHold(){holdStart=mono();baselineBytes=bytes;cpu=process.cpuUsage();baselineUpdates=marketUpdates;baselineChanges=changeEntries;baselineWork=callbackWorkMs;perMarketBaseline=new Map(currentConnections().map(c=>[c.index,c.market_updates]));holdIndices=currentConnections().map(c=>c.index);epoch=lifecycleEpoch;callbackMaxMs=0;loop.reset();event('hold_started',{target,connections:holdIndices.length,restart:holdRestarts});}
    beginHold();
    const duration=active?finalHold:(target===maximum?finalHold:15);
    while(!stopReason&&mono()-holdStart<duration*1000){if(lifecycleEpoch!==epoch){await populate(target);if(maintained){epoch=lifecycleEpoch;event('population_restored',{target,connections:currentConnections().length});}else{holdRestarts++;beginHold();}}await sleep(100);}
    const end=mono();const healthy=currentConnections().filter(c=>c.socket.readyState===WebSocket.OPEN&&c.complete_ms!==null&&c.pongs>0&&end-c.lastPongMs<=15000&&c.errors.length===0&&c.closes.length===0).length;
    const cpuUsed=process.cpuUsage(cpu);
    const stage={target,requested_markets:connections.length,requested_assets:connections.length*2,open:connections.filter(c=>c.socket.readyState===WebSocket.OPEN).length,complete_books:connections.reduce((n,c)=>n+c.books.size,0),healthy,stage_start_ms:stageStart,hold_start_ms:holdStart,end_ms:end,hold_seconds:(end-holdStart)/1000,passed:!stopReason&&healthy===target,stop_reason:stopReason,received_bytes:bytes-baselineBytes,bytes_per_second:(bytes-baselineBytes)/((end-holdStart)/1000),cpu_ms:{user:cpuUsed.user/1000,system:cpuUsed.system/1000},event_loop_ms:{p50:loop.percentile(50)/1e6,p99:loop.percentile(99)/1e6,max:loop.max/1e6},rss_bytes:process.memoryUsage().rss,open_file_descriptors:fs.readdirSync('/proc/self/fd').length};
    Object.assign(stage,{population_policy:maintained?'maintained':'fixed cohort',requested_markets:currentConnections().length,requested_assets:currentConnections().length*2,complete_books:currentConnections().reduce((n,c)=>n+c.books.size,0),connection_indices:holdIndices,hold_restarts:holdRestarts,market_updates:marketUpdates-baselineUpdates,market_updates_per_second:(marketUpdates-baselineUpdates)/stage.hold_seconds,price_change_entries:changeEntries-baselineChanges,active_markets:currentConnections().filter(c=>c.market_updates>(perMarketBaseline.get(c.index)??0)).length,per_market_updates:currentConnections().map(c=>({connection:c.index,market:c.market,updates:c.market_updates-(perMarketBaseline.get(c.index)??0)})),callback_work_ms:callbackWorkMs-baselineWork,maximum_callback_ms:callbackMaxMs});
    stages.push(stage);save('stages.json',stages);event('stage',stage);const {connection_indices,per_market_updates,...printedStage}=stage;console.log(JSON.stringify({phase:'stage',...printedStage}));
    if(!stage.passed)stopReason??='health_check_failed_inconclusive';
  }
  clearInterval(heartbeat);clearInterval(sampler);clearInterval(progress);
}catch(e){stopReason??='experiment_error';event('experiment_error',errorFields(e));}
finally{
  clearInterval(heartbeat);clearInterval(sampler);clearInterval(progress);
  stopping=true;
  await Promise.all(connections.map(c=>new Promise(resolve=>{
    if(c.socket.readyState===WebSocket.CLOSED)return resolve();const t=setTimeout(resolve,3000);c.socket.addEventListener('close',()=>{clearTimeout(t);resolve();},{once:true});c.socket.close(1000,'Experiment complete');
  })));
  const unclosed=connections.filter(c=>c.socket.readyState!==WebSocket.CLOSED).length;
  if(unclosed)for(const s of transportSockets)s.destroy();
  after=await trace();loop.disable();progressLoop.disable();
  const result={started_at:startedAt,finished_at:new Date().toISOString(),runtime:process.version,protocol,stop_reason:stopReason,conclusion:stopReason?'Observed operational failure; inspect evidence before claiming an IP cap':'Tested lower bound at chosen guard; no maximum established',maximum_requested:connections.length,maximum_passed:Math.max(0,...stages.filter(s=>s.passed).map(s=>s.target)),stages,egress:{source:traceUrl,before_status:before?.status??null,after_status:after?.status??null,public_ip_observed_before:!!before?.ip,public_ip_observed_after:!!after?.ip,public_ip_unchanged:before?.ip&&after?.ip?before.ip===after.ip:null,public_ip_family_before:net.isIP(before?.ip??''),public_ip_family_after:net.isIP(after?.ip??''),distinct_local_transport_addresses:localAddresses.size,distinct_local_websocket_addresses:localsByWs.size,distinct_remote_transport_addresses:remoteAddresses.size,websocket_remote_families:[...remoteFamilies],websocket_handshake_diagnostics:wsTransportCount,handshake_statuses:[...new Set(handshakes)],addresses_retained:false},errors:diagnosticErrors,bytes_received:bytes,peak_rss_bytes:peakRss,maximum_queued_output_bytes:maximumQueuedBytes,cpu_ms:(()=>{const c=process.cpuUsage(cpuStart);return {user:c.user/1000,system:c.system/1000};})(),sockets_not_closed_after_grace:unclosed,connections:connections.map(c=>{const {socket,books,...rest}=c;return {...rest,received_book_assets:[...books]};})};
  save('result.json',result);
  result.retained_payload_bytes=retainedBytes;result.total_market_updates=marketUpdates;result.total_price_change_entries=changeEntries;
  save('result.json',result);
  event('complete',{maximum_passed:result.maximum_passed,stop_reason:stopReason,remaining_websockets:connections.filter(c=>c.socket.readyState!==WebSocket.CLOSED).length});
  await Promise.all([events,frames].map(s=>new Promise(r=>s.end(r))));
  console.log(JSON.stringify({phase:'complete',maximum_passed:result.maximum_passed,maximum_requested:connections.length,stop_reason:stopReason,egress:result.egress,remaining_websockets:connections.filter(c=>c.socket.readyState!==WebSocket.CLOSED).length}));
}
