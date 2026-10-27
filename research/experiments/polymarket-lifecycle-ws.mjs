import fs from 'node:fs';
import path from 'node:path';
import {setTimeout as sleep} from 'node:timers/promises';
const output=process.argv[2], seconds=Number(process.argv[3]??3900);
if(!output || !Number.isFinite(seconds) || seconds<3600) throw Error('Usage: node experiments/polymarket-lifecycle-ws.mjs recordings/RUN [SECONDS>=3600]');
fs.mkdirSync(output,{recursive:false});
fs.writeFileSync(path.join(output,'.gitignore'),'*\n!.gitignore\n');
const files=new Map();
function log(file,row){if(!files.has(file)) files.set(file,fs.createWriteStream(path.join(output,file),{flags:'wx'})); files.get(file).write(JSON.stringify({received_at:new Date().toISOString(),mono_ms:performance.now(),...row})+'\n');}
const started=new Date(), end=+started+seconds*1000;
const meta={started_at:started.toISOString(),seconds,subscription:{type:'market',assets_ids:[],initial_dump:false,custom_feature_enabled:true},connections:3};
fs.writeFileSync(path.join(output,'meta.json'),JSON.stringify(meta,null,2));
let stopping=false, requests=0, lastRequest=0;
const ids=new Set(), looked=new Map(), missing=new Set();
const sockets=[];
function connect(i){const ws=new WebSocket('wss://ws-subscriptions-clob.polymarket.com/ws/market'); sockets[i]=ws;
 ws.onopen=()=>{log(`socket-${i}.jsonl`,{kind:'opened'});ws.send(JSON.stringify(meta.subscription));log(`socket-${i}.jsonl`,{kind:'sent',raw:JSON.stringify(meta.subscription)});};
 ws.onmessage=async e=>{const raw=typeof e.data==='string'?e.data:Buffer.from(await e.data.arrayBuffer()).toString('base64');log(`socket-${i}.jsonl`,{kind:'received',raw});try{const a=JSON.parse(raw);for(const x of Array.isArray(a)?a:[a]) if(x.event_type==='new_market'&&x.id) ids.add(String(x.id));}catch{}};
 ws.onerror=e=>log(`socket-${i}.jsonl`,{kind:'error',message:e.message??'WebSocket error'});
 ws.onclose=e=>{log(`socket-${i}.jsonl`,{kind:'closed',code:e.code,reason:e.reason,expected:stopping});if(!stopping)setTimeout(()=>connect(i),1000);};
}
for(let i=0;i<3;i++)connect(i);
const heartbeat=setInterval(()=>sockets.forEach((s,i)=>{if(s.readyState===1){s.send('PING');log(`socket-${i}.jsonl`,{kind:'sent',raw:'PING'});}}),10000);
async function get(base,params,kind){await sleep(Math.max(0,lastRequest+600-Date.now()));lastRequest=Date.now();const u=new URL(base);for(const [k,v]of params)u.searchParams.append(k,v);u.searchParams.set('_research_poll',`${Date.now()}-${requests++}`);const sent=new Date().toISOString();try{const r=await fetch(u,{signal:AbortSignal.timeout(20000)});const raw=await r.text();log('rest.jsonl',{kind,sent_at:sent,url:String(u),status:r.status,headers:Object.fromEntries(['date','age','cf-cache-status','cache-control'].map(k=>[k,r.headers.get(k)])),raw});return r.ok?JSON.parse(raw):null;}catch(e){log('rest.jsonl',{kind,sent_at:sent,url:String(u),error:e.message});return null;}}
async function listing(version,closed){let cursor,pages=0;do{const p=[['version',version],['closed',String(closed)],['order',closed?'closedTime,id':'id'],['ascending','false'],['limit','100']];if(cursor)p.push(['after_cursor',cursor]);const r=await get('https://gamma-api.polymarket.com/markets/keyset',p,`listing-${version}-${closed}`);if(!r)break;const rows=r.markets??[];cursor=r.next_cursor;pages++;if(!rows.length)break;const dates=rows.map(x=>Date.parse(closed?x.closedTime:x.createdAt));if(dates.every(x=>Number.isFinite(x)&&x<+started-120000))break;if(pages>=100){log('rest.jsonl',{kind:'pagination-limit',version,closed});break;}}while(cursor);}
async function lookup(){const todo=[...ids].filter(id=>!looked.has(id)||missing.has(id)&&Date.now()-looked.get(id)>60000);for(let n=0;n<todo.length;n+=100){const batch=todo.slice(n,n+100);const r=await get('https://gamma-api.polymarket.com/markets/keyset',[['closed','any'],['version','v1'],['version','v2'],['limit','100'],...batch.map(id=>['id',id])],'targeted');if(r){const found=new Set((r.markets??[]).map(x=>String(x.id)));for(const id of batch){looked.set(id,Date.now());if(found.has(id))missing.delete(id);else missing.add(id);}}}}
while(Date.now()<end){const cycle=Date.now();await listing('v1',false);await listing('v2',false);await listing('v1',true);await lookup();console.log(JSON.stringify({elapsed_s:Math.round((Date.now()-started)/1000),requests,event_ids:ids.size,looked:looked.size,missing:missing.size}));await sleep(Math.min(15000,Math.max(0,end-Date.now())));}
meta.observation_ended_at=new Date().toISOString();stopping=true;clearInterval(heartbeat);for(const s of sockets)s.close(1000,'Observation complete');await sleep(1000);
await listing('v1',false);await listing('v2',false);await listing('v1',true);await lookup();
meta.finished_at=new Date().toISOString();meta.requests=requests;meta.event_ids=ids.size;meta.missing_ids=[...missing];fs.writeFileSync(path.join(output,'meta.json'),JSON.stringify(meta,null,2));for(const f of files.values())f.end();console.log(JSON.stringify({done:true,...meta}));
