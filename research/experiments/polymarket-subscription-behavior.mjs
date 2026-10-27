import fs from 'node:fs';
import path from 'node:path';
const out = process.argv[2];
if (!out) throw Error('Usage: node research/experiments/polymarket-subscription-behavior.mjs research/recordings/RUN');
fs.mkdirSync(out, {recursive:false});
fs.writeFileSync(path.join(out,'.gitignore'), '*\n!.gitignore\n');
const sleep = ms => new Promise(r=>setTimeout(r,ms));
const origin=performance.now();
const log=fs.createWriteStream(path.join(out,'frames.jsonl'),{flags:'wx'});
const record=(connection,kind,data={})=>log.write(JSON.stringify({connection,kind,received_at:new Date().toISOString(),mono_ms:performance.now()-origin,...data})+'\n');
const markets=[];
for (const order of ['id','volume24hr']) {
 for(let page=0,cursor='';page<6;page++) {
  const url=`https://gamma-api.polymarket.com/markets/keyset?version=v1&closed=false&limit=100&order=${order}&ascending=false${cursor?'&after_cursor='+encodeURIComponent(cursor):''}`;
  const r=await fetch(url); if(!r.ok) throw Error('Gamma HTTP '+r.status);
  const body=await r.json(); record('rest','response',{url,body});
  const rows=Array.isArray(body)?body:body.markets??body.data??[];
  markets.push(...rows); cursor=body.next_cursor??body.nextCursor??'';
  await sleep(650); if(!cursor) break;
 }
}
const unique=[...new Map(markets.map(m=>[m.id,m])).values()].filter(m=>m.version!=='v2'&&m.active&&!m.closed&&!m.archived&&m.enableOrderBook);
const ids=m=>JSON.parse(m.clobTokenIds??'[]');
const eligible=unique.filter(m=>m.acceptingOrders&&ids(m).length);
if(eligible.length<12) throw Error('Insufficient eligible markets: '+eligible.length);
const byVolume=[...eligible].sort((a,b)=>Number(b.volume24hr??0)-Number(a.volume24hr??0));
const busy=byVolume.slice(0,6),quiet=byVolume.slice(-6);
const candidates=unique.filter(m=>!m.acceptingOrders&&ids(m).length).slice(0,10);
const metadata={started_at:new Date().toISOString(),endpoint:'wss://ws-subscriptions-clob.polymarket.com/ws/market',runtime:process.version,busy,quiet,candidates,eligible_count:eligible.length};
fs.writeFileSync(path.join(out,'metadata.json'),JSON.stringify(metadata,null,2));
let serial=0;const sockets=[];
async function connect(name,assets,heartbeat=true){
 const nameId=name+'-'+(++serial),ws=new WebSocket(metadata.endpoint);sockets.push(ws);
 const state={ws,id:nameId,events:[],send(raw,extra={}){if(ws.readyState!==1) return;record(nameId,'sent',{raw:typeof raw==='string'?raw:JSON.stringify(raw),...extra});ws.send(typeof raw==='string'?raw:JSON.stringify(raw));}};
 ws.onmessage=e=>{record(nameId,'received',{raw:e.data});let parsed;try{parsed=JSON.parse(e.data)}catch{return}for(const event of Array.isArray(parsed)?parsed:[parsed])state.events.push({event,time:performance.now()});};
 ws.onclose=e=>record(nameId,'closed',{code:e.code,reason:e.reason});ws.onerror=()=>record(nameId,'error');
 await new Promise((resolve,reject)=>{const t=setTimeout(()=>reject(Error('Open timeout')),20000);ws.onopen=()=>{clearTimeout(t);record(nameId,'opened');resolve()}});
 state.send({type:'market',assets_ids:assets,initial_dump:true});
 const timer=heartbeat?setInterval(()=>state.send('PING'),10000):null;
 ws.addEventListener('close',()=>clearInterval(timer));
 state.close=()=>{clearInterval(timer);if(ws.readyState===1)ws.close(1000,'Experiment complete')};return state;
}
if(process.argv[3]==='--flow-only'){
 const feed=await connect('activity-screen',eligible.flatMap(ids).slice(0,1800));await sleep(5000);
 const activity=new Map();for(const {event} of feed.events)for(const c of event.price_changes??[])activity.set(c.asset_id,(activity.get(c.asset_id)??0)+1);
 const asset=[...activity].sort((a,b)=>b[1]-a[1])[0]?.[0];if(!asset)throw Error('No flowing asset');
 for(let trial=5;trial<10;trial++){
  const s=await connect('busy-flow-extra',[asset]);const start=performance.now();while(performance.now()-start<30000&&!s.events.some(x=>x.event.event_type==='price_change'))await sleep(25);
  record(s.id,'trial_start',{group:'busy',test:'flow',trial,asset,prior_price_changes:s.events.filter(x=>x.event.event_type==='price_change').length});
  s.send({operation:'unsubscribe',assets_ids:[asset]},{action:'unsubscribe',asset});s.send({operation:'subscribe',assets_ids:[asset]},{action:'subscribe',asset});await sleep(2500);record(s.id,'trial_end');s.close();
 }
 feed.close();await sleep(1500);metadata.finished_at=new Date().toISOString();fs.writeFileSync(path.join(out,'metadata.json'),JSON.stringify(metadata,null,2));await new Promise(r=>log.end(r));process.exit(0);
}
if(process.argv[3]==='--format-only'){
 const feeds=await Promise.all([0,1].map(()=>connect('bulk-format',eligible.flatMap(ids).slice(0,1800))));
 await sleep(30000);feeds.forEach(s=>s.close());await sleep(1500);metadata.finished_at=new Date().toISOString();fs.writeFileSync(path.join(out,'metadata.json'),JSON.stringify(metadata,null,2));await new Promise(r=>log.end(r));process.exit(0);
}
const heartbeats=await Promise.all([0,1,2].map(i=>connect('no-ping',ids(quiet[i]),false)));
const heartbeatDone=(async()=>{await sleep(300000);heartbeats.forEach(s=>{record(s.id,'observation_end',{limit_ms:300000});s.close()})})();
const bulk=await connect('bulk',eligible.flatMap(ids).slice(0,1800));
async function trials(group,selection){
 for(const test of ['new','duplicate','back_to_back','gap_1s','flow']){
  for(let trial=0;trial<5;trial++){
   const asset=ids(selection[trial])[0],anchor=ids(selection[(trial+1)%selection.length])[0];
   const s=await connect(group+'-'+test,[test==='new'?anchor:asset]);await sleep(1500);
   if(test==='flow'){
    const start=performance.now();while(performance.now()-start<15000&&!s.events.some(x=>x.event.event_type==='price_change'))await sleep(100);
   }
   const changes=s.events.filter(x=>x.event.event_type==='price_change').length;
   record(s.id,'trial_start',{group,test,trial,asset,prior_price_changes:changes});
   if(['back_to_back','gap_1s','flow'].includes(test))s.send({operation:'unsubscribe',assets_ids:[asset]},{action:'unsubscribe',asset});
   if(test==='gap_1s')await sleep(1000);
   s.send({operation:'subscribe',assets_ids:[asset]},{action:'subscribe',asset});
   await sleep(2500);record(s.id,'trial_end');s.close();
  }
 }
}
await Promise.all([trials('busy',busy),trials('quiet',quiet),(async()=>{
 for(let trial=0;trial<5;trial++){
  const selection=candidates.length?candidates:eligible.slice(0,10);
  const s=await connect('empty-candidate',selection.flatMap(ids));record(s.id,'candidate_trial',{trial,accepting_orders:candidates.length===0});await sleep(3500);s.close();
 }
})()]);
await heartbeatDone;bulk.close();await sleep(1500);
metadata.finished_at=new Date().toISOString();fs.writeFileSync(path.join(out,'metadata.json'),JSON.stringify(metadata,null,2));
await new Promise(r=>log.end(r));console.log(JSON.stringify({out,eligible:eligible.length,candidates:candidates.length}));
