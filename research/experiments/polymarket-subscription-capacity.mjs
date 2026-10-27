import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { monitorEventLoopDelay } from 'node:perf_hooks';

// Public, receive-only experiment. Run from the repository root with Node >=24.
const [command, output, ...args] = process.argv.slice(2);
if (!['discover', 'trial', 'search', 'bisect', 'repeat'].includes(command) || !output?.startsWith('recordings/subscription-capacity')) {
  throw new Error('Usage: node experiments/polymarket-subscription-capacity.mjs discover|trial|search|bisect|repeat recordings/subscription-capacity-NAME [market-count|low|counts-comma-separated] [seconds|high]');
}
fs.mkdirSync(output, { recursive: true });
fs.writeFileSync(path.join(output, '.gitignore'), '*\n');
const json = (name, value) => fs.writeFileSync(path.join(output, name), JSON.stringify(value, null, 2) + '\n');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const endpoint = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
const gamma = 'https://gamma-api.polymarket.com';
const origin = process.hrtime.bigint();
const mono = () => Number(process.hrtime.bigint() - origin) / 1e6;
const sockets = new Set();
let interrupted = false;
process.on('SIGINT', () => { interrupted = true; for (const s of sockets) s.close(1000, 'Interrupted'); });
process.on('SIGTERM', () => { interrupted = true; for (const s of sockets) s.close(1000, 'Interrupted'); });
const array = v => typeof v === 'string' ? JSON.parse(v) : v;
function market(m) {
  if (!(m.active && !m.closed && !m.archived && m.acceptingOrders && m.enableOrderBook)) return null;
  const ids = array(m.version === 'v2' ? m.positionIds : m.clobTokenIds);
  if (!Array.isArray(ids) || ids.length !== 2 || new Set(ids).size !== 2 || array(m.outcomes)?.length !== 2) return null;
  return { id: m.id, version: m.version, assets: ids, volume24hr: m.volume24hr, endDate: m.endDate,
    active: m.active, closed: m.closed, archived: m.archived, acceptingOrders: m.acceptingOrders, enableOrderBook: m.enableOrderBook };
}
async function get(url) {
  const r = await fetch(url, { signal: AbortSignal.timeout(20000) });
  if (!r.ok) throw new Error(`HTTP ${r.status}: ${url.split('?')[0]}`);
  return r.json();
}
if (command === 'discover') {
  const limit = Math.min(20000, Number(args[0] ?? 20000));
  if(!Number.isInteger(limit)||limit<5)throw new Error('Discovery guard must be 5..20000 markets');
  const liquid = await get(`${gamma}/markets/keyset?limit=100&order=volume24hr&ascending=false&active=true&closed=false`);
  const probes = liquid.markets.map(market).filter(Boolean).slice(0, 5);
  const pool = new Map(probes.map(m => [m.id, m]));
  let cursor, pages = 0, exhausted = false;
  const discovery = fs.createWriteStream(path.join(output, 'discovery.jsonl'), { flags: 'wx' });
  while (pool.size < limit && !interrupted) {
    const url = new URL(`${gamma}/markets/keyset`);
    Object.entries({ limit: '100', order: 'id', ascending: 'true', active: 'true', closed: 'false', ...(cursor ? {after_cursor: cursor} : {}) }).forEach(([k,v]) => url.searchParams.set(k,v));
    const page = await get(url);
    pages++;
    const eligible = page.markets.map(market).filter(Boolean);
    discovery.write(JSON.stringify({ page: pages, fetched_at: new Date().toISOString(), count: page.markets.length, eligible }) + '\n');
    for (const m of eligible) if (pool.size < limit) pool.set(m.id, m);
    if (pages % 20 === 0) console.log(JSON.stringify({ phase: 'discovery', pages, markets: pool.size }));
    if (!page.next_cursor || !page.markets.length) { exhausted = true; break; }
    if (page.next_cursor === cursor || pages >= 600) throw new Error('Discovery cursor/page guard');
    cursor = page.next_cursor;
    await sleep(50);
  }
  await new Promise(r => discovery.end(r));
  const catalog = { fetched_at: new Date().toISOString(), limit, pages, exhausted, probe_market_ids: probes.map(m => m.id), markets: [...pool.values()] };
  json('catalog.json', catalog);
  console.log(JSON.stringify({ phase: 'catalog', markets: pool.size, assets: pool.size * 2, probes: catalog.probe_market_ids, exhausted }));
  process.exit(0);
}
const catalog = JSON.parse(fs.readFileSync(path.join(output, 'catalog.json')));
const probes = catalog.markets.filter(m => catalog.probe_market_ids.includes(m.id));
const probeAssets = new Set(probes.flatMap(m => m.assets));
const quantile = (a, p) => a.length ? [...a].sort((x,y)=>x-y)[Math.min(a.length-1,Math.floor(a.length*p))] : null;
async function trial(count, seconds = 10, purpose = 'manual') {
  if (!Number.isInteger(count)||count < probes.length || count > Math.min(20000,catalog.markets.length)) throw new Error('Count outside bounded catalog');
  if(!Number.isFinite(seconds)||seconds<1||seconds>120)throw new Error('Observation must be 1..120 seconds');
  const id = `${new Date().toISOString().replaceAll(/[:.]/g,'-')}-${count}`;
  const dir = path.join(output, id);
  fs.mkdirSync(dir);
  let profiler;
  if(purpose==='profile-excluded') {
    const {Session}=await import('node:inspector/promises');profiler=new Session();profiler.connect();
    await profiler.post('Profiler.enable');await profiler.post('Profiler.start');
  }
  const selected = catalog.markets.slice(0, count);
  fs.writeFileSync(path.join(dir, 'requested.json'), JSON.stringify(selected));
  const errors = [];
  const cpuStart = process.cpuUsage();
  const started = mono();
  let abortReason = null, maxRss = 0, maxQueued = 0, stopped = false, totalBytes = 0;
  const loop = monitorEventLoopDelay({resolution:1}); loop.enable();
  const steadyLoop = monitorEventLoopDelay({resolution:1});
  let steadyCpuStart=null,steadyCpu=null,steadyLoopMs=null,steadyActualStart=null,steadyActualEnd=null;
  const streams = [];
  const stats = [];
  const active = [];
  function feed(label, assets) {
    const requested = new Set(assets);
    const received = new Set();
    const stream = fs.createWriteStream(path.join(dir, `${label}.jsonl`), {flags:'wx'});
    streams.push(stream);
    const st = {label, requested_assets:assets.length, received_assets:0, sent_ms:null, complete_ms:null, frames:0, bytes:0, max_frame_bytes:0, types:{}, per_second:{}, closes:[], errors:[], callback_ms:[], parsing_ms:[]};
    stats.push(st);
    stream.on('error', e => {errors.push({label,kind:'output',message:e.message});abortReason='output_error';});
    const write = row => {stream.write(JSON.stringify(row)+'\n'); maxQueued=Math.max(maxQueued,stream.writableLength); if(stream.writableLength>32*1024*1024)abortReason='client_output_queue_guard';};
    const socket = new WebSocket(endpoint); sockets.add(socket); active.push(socket);
    socket.onopen = () => {
      st.sent_ms=mono();
      const subscription={type:'market',assets_ids:assets,initial_dump:true,custom_feature_enabled:true};
      write({kind:'sent',mono_ms:st.sent_ms,payload:subscription}); socket.send(JSON.stringify(subscription));
    };
    socket.onmessage = event => {
      const receipt=mono(); // First callback operation, before parsing or I/O.
      if(typeof event.data!=='string'){errors.push({label,kind:'binary'});abortReason='unexpected_binary';return;}
      const raw=event.data;
      const bytes=Buffer.byteLength(raw); st.frames++;st.bytes+=bytes;totalBytes+=bytes;st.max_frame_bytes=Math.max(st.max_frame_bytes,bytes);
      if(bytes>64*1024*1024){abortReason='client_frame_guard';return;}
      write({kind:'received',mono_ms:receipt,raw});
      const bucket=Math.floor((receipt-started)/1000);
      const b=st.per_second[bucket]??={bytes:0,frames:0,events:0,global_new_market:0};b.bytes+=bytes;b.frames++;
      if(raw!=='PONG') {
        try {
          const parseStart=mono();const decoded=JSON.parse(raw); st.parsing_ms.push(mono()-parseStart);
          for(const e of Array.isArray(decoded)?decoded:[decoded]) {
            const type=e.event_type??'unknown'; st.types[type]=(st.types[type]??0)+1;b.events++;
            if(type==='new_market')b.global_new_market++;
            if(type==='book'&&requested.has(e.asset_id)) {
              if(Array.isArray(e.bids)&&Array.isArray(e.asks))received.add(e.asset_id);
              else {errors.push({label,kind:'invalid_book_snapshot',payload:e});abortReason='invalid_book_snapshot';}
            }
            if(type==='error'||e.error){errors.push({label,kind:'server',payload:e});abortReason='server_error';}
          }
          st.received_assets=received.size;
          if(st.complete_ms===null&&received.size===requested.size)st.complete_ms=receipt;
        } catch(e){errors.push({label,kind:'parse',message:e.message,raw});abortReason='parse_error';}
      }
      const callbackMs=mono()-receipt;st.callback_ms.push(callbackMs);
      b.callback_total_ms=(b.callback_total_ms??0)+callbackMs;b.callback_max_ms=Math.max(b.callback_max_ms??0,callbackMs);
      if(totalBytes>768*1024*1024)abortReason='client_total_bytes_guard';
    };
    socket.onerror=e=>{const row={kind:'websocket',message:e.message||e.error?.message||'socket error',name:e.error?.name??null,code:e.error?.code??null,cause:e.error?.cause?{name:e.error.cause.name,message:e.error.cause.message,code:e.error.cause.code}:null};st.errors.push(row);write({mono_ms:mono(),...row});};
    socket.onclose=e=>{const row={mono_ms:mono(),code:e.code,reason:e.reason,wasClean:e.wasClean,expected:stopped};st.closes.push(row);write({kind:'close',...row});if(!stopped)abortReason='unexpected_close';sockets.delete(socket);};
    st._received=received;
  }
  // Both counts in each balanced round use the same opening order.
  const controlFirst=purpose.startsWith('balanced-')?Number(purpose.split('-')[1])%2===1:fs.readdirSync(output).filter(x=>/^20.*-\d+$/.test(x)).length%2;
  if(controlFirst) {
    feed('control',[...probeAssets]);feed('candidate',selected.flatMap(m=>m.assets));
  } else {feed('candidate',selected.flatMap(m=>m.assets));feed('control',[...probeAssets]);}
  const heartbeat=setInterval(()=>{for(const s of active)if(s.readyState===WebSocket.OPEN)s.send('PING');},10000);
  let windowStart=null,windowEnd=null;
  while(!interrupted&&!abortReason) {
    const now=mono();maxRss=Math.max(maxRss,process.memoryUsage().rss);
    if(maxRss>1024**3){abortReason='client_rss_guard';break;}
    if(windowStart===null&&stats.every(s=>s.complete_ms!==null)){windowStart=Math.max(...stats.map(s=>s.complete_ms))+5000;windowEnd=windowStart+seconds*1000;}
    if(windowStart!==null&&now>=windowStart&&steadyCpuStart===null){steadyActualStart=now;steadyCpuStart=process.cpuUsage();steadyLoop.enable();}
    if(windowEnd!==null&&now>=windowEnd&&steadyCpu===null){steadyActualEnd=now;steadyCpu=process.cpuUsage(steadyCpuStart);steadyLoop.disable();steadyLoopMs={p50:steadyLoop.percentile(50)/1e6,p99:steadyLoop.percentile(99)/1e6,max:steadyLoop.max/1e6};}
    if(windowEnd!==null&&now>windowEnd+3000)break;
    if(windowStart===null&&now-started>30000){abortReason='initialization_timeout_inconclusive';break;}
    await sleep(100);
  }
  stopped=true;clearInterval(heartbeat);
  const ended=mono();
  await Promise.all(active.map(s=>new Promise(resolve=>{if(s.readyState===WebSocket.CLOSED)return resolve();const t=setTimeout(resolve,3000);s.addEventListener('close',()=>{clearTimeout(t);resolve();},{once:true});s.close(1000,'Trial complete');})));
  loop.disable();
  steadyLoop.disable();
  await Promise.all(streams.map(s=>new Promise(r=>s.end(r))));
  if(profiler) {
    const {profile}=await profiler.post('Profiler.stop');profiler.disconnect();
    for(const node of profile.nodes)node.callFrame.url=node.callFrame.url.replace(`file://${process.cwd()}/`,'').replace(process.cwd()+'/','').replace(os.homedir(),'~');
    fs.writeFileSync(path.join(dir,'profile.json'),JSON.stringify(profile));
  }
  const missing=selected.flatMap(m=>m.assets).filter(a=>!stats.find(s=>s.label==='candidate')._received.has(a));
  const revalidation=[];
  if(missing.length)for(const m of selected.filter(m=>m.assets.some(a=>missing.includes(a))).slice(0,100)) {
    try {const fresh=await get(`${gamma}/markets/${m.id}`);revalidation.push({id:m.id,eligible_now:!!market(fresh),current_assets:market(fresh)?.assets??null});}catch(e){revalidation.push({id:m.id,error:e.message});}
    await sleep(30);
  }
  const cpu=process.cpuUsage(cpuStart);
  const result={id,purpose,started_at:new Date(Date.now()-(mono()-started)).toISOString(),runtime:process.version,cores:os.availableParallelism(),load_average:os.loadavg(),endpoint,markets:count,assets:count*2,probe_markets:probes.length,probe_assets:probeAssets.size,seconds,window_start_ms:windowStart,window_end_ms:windowEnd,started_ms:started,ended_ms:ended,abort_reason:abortReason,interrupted,accepted:stats.every(s=>s.complete_ms!==null),missing_assets:missing,revalidation,errors,max_rss_bytes:maxRss,max_output_queue_bytes:maxQueued,cpu_ms:{user:cpu.user/1000,system:cpu.system/1000},event_loop_ms:{p50:loop.percentile(50)/1e6,p99:loop.percentile(99)/1e6,max:loop.max/1e6},stats:stats.map(s=>{const {_received,callback_ms,parsing_ms,...rest}=s;return {...rest,initialization_ms:s.complete_ms===null?null:s.complete_ms-s.sent_ms,callback_ms:{p50:quantile(callback_ms,.5),p99:quantile(callback_ms,.99),max:Math.max(0,...callback_ms)},parse_ms:{p99:quantile(parsing_ms,.99),max:Math.max(0,...parsing_ms)}};})};
  result.steady_event_loop_ms=steadyLoopMs;
  result.steady_cpu_ms=steadyCpu?{user:steadyCpu.user/1000,system:steadyCpu.system/1000}:null;
  result.steady_actual_interval_ms=[steadyActualStart,steadyActualEnd];
  fs.writeFileSync(path.join(dir,'result.json'),JSON.stringify(result,null,2)+'\n');
  fs.appendFileSync(path.join(output,'trials.jsonl'),JSON.stringify(result)+'\n');
  console.log(JSON.stringify({phase:'trial',id,markets:count,assets:count*2,accepted:result.accepted,abort_reason:abortReason,received:result.stats.map(s=>[s.label,s.received_assets]),init_ms:result.stats.map(s=>[s.label,s.initialization_ms]),missing:missing.length,bytes:totalBytes}));
  await sleep(abortReason?5000:2000);
  const throttle=JSON.stringify({errors:result.errors,feeds:result.stats.map(s=>({errors:s.errors,closes:s.closes}))});
  if(/429|throttl|rate.limit|too many/i.test(throttle)) {
    json('throttling-stop.json',{trial:id,evidence:JSON.parse(throttle)});
    interrupted=true;
  }
  return result;
}
if(command==='trial')await trial(Number(args[0]),Number(args[1]??30),args[2]??'manual');
else if(command==='repeat') {
  const counts=args[0].split(',').map(Number);
  const rounds=Number(args[2]??5);
  if(!Number.isInteger(rounds)||rounds<1||rounds>5||counts.length>2)throw new Error('Repeat guard: at most five rounds of two counts');
  for(let round=0;round<rounds&&!interrupted;round++)for(const count of counts){if(interrupted)break;await trial(count,Number(args[1]??20),`balanced-${round+1}`);}
}
else if(command==='bisect') {
  let low=Number(args[0]),high=Number(args[1]);
  if(!Number.isInteger(low)||!Number.isInteger(high)||low<probes.length||high<=low||high>Math.min(20000,catalog.markets.length))throw new Error('Invalid bounded delivery bracket');
  const tested=[];
  // This searches survival of a bounded observation, independently of acceptance.
  while(high-low>80&&!interrupted){
    const mid=Math.floor((low+high)/2);const r=await trial(mid,5,'delivery-binary');
    tested.push({id:r.id,count:mid,accepted:r.accepted,abort_reason:r.abort_reason});
    if(r.accepted&&!r.abort_reason)low=mid;else high=mid;
  }
  json('delivery-search.json',{low,high,resolution_markets:80,tested,conclusion:'Observed short-window survival bracket, not a monotonic exchange cap; repeat endpoints and inspect server/client causes'});
}
else {
  let low=0,high=null;
  const guard=Math.min(Number(args[0]??20000),catalog.markets.length);
  if(!Number.isInteger(guard)||guard<probes.length||guard>20000)throw new Error('Invalid acceptance guard');
  for(let count=Math.min(10,guard);!interrupted;count=Math.min(count*2,guard)) {
    const r=await trial(count,5,'exponential');
    if(!r.accepted){high=count;break;}low=count;if(count===guard)break;
  }
  // A timeout is an operational snapshot boundary only; never call it a server cap.
  if(high!==null&&!interrupted) {
    const repeat=await trial(high,5,'boundary-repeat');
    if(repeat.accepted) {json('search.json',{low,high,conclusion:'nonmonotonic_or_transient; no reliable bracket'});}
    else {
      while(high-low>1&&!interrupted){const mid=Math.floor((low+high)/2);const r=await trial(mid,5,'binary');if(r.accepted)low=mid;else high=mid;}
      json('search.json',{low,high,conclusion:'operational snapshot-coverage bracket, inspect failure causes before attributing to server'});
    }
  } else json('search.json',{low,high,guard,conclusion:'tested lower bound; no upper rejection bracket found'});
}
if(sockets.size){for(const s of sockets)s.close(1000,'Experiment complete');await sleep(1000);}
