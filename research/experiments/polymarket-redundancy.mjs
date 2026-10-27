import fs from 'node:fs';
import path from 'node:path';
import { monitorEventLoopDelay } from 'node:perf_hooks';

// Observe redundant feeds without changing the collector or merging their data.
const output = process.argv[2];
const seconds = Number(process.argv[3] ?? 120);
if (!output || !Number.isFinite(seconds) || seconds < 10) {
  throw new Error('Usage: node experiments/polymarket-redundancy.mjs OUTPUT_DIRECTORY [SECONDS>=10]');
}
const marketId = '6104736';
const assetId = '42820435881084171095305173309280682294357335411274100735935831763831134276262';
const endpoint = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
const warmup = 10;
const grace = 10;
const connections = 10;
fs.mkdirSync(output, { recursive: false });
const response = await fetch(`https://gamma-api.polymarket.com/markets/${marketId}`, {
  signal: AbortSignal.timeout(15000),
});
if (!response.ok) throw new Error(`Gamma returned HTTP ${response.status}`);
const market = await response.json();
if (!market.active || market.closed || market.archived || !market.acceptingOrders || !market.enableOrderBook) {
  throw new Error('Selected market is no longer eligible');
}
const ids = market.version === 'v2' ? market.positionIds : JSON.parse(market.clobTokenIds);
if (ids[JSON.parse(market.outcomes).indexOf('Yes')] !== assetId) throw new Error('Yes asset identity changed');
const origin = process.hrtime.bigint();
const mono = () => Number(process.hrtime.bigint() - origin) / 1e6;
const files = [];
const sockets = [];
const status = [];
const errors = [];
let stopping = false;
let maximumQueuedBytes = 0;
function record(i, kind, extra = {}) {
  const row = { mono_ms: mono(), received_at: new Date().toISOString(), kind, ...extra };
  files[i].write(`${JSON.stringify(row)}\n`);
  maximumQueuedBytes = Math.max(maximumQueuedBytes, files[i].writableLength);
}
const subscription = JSON.stringify({ type: 'market', assets_ids: [assetId], initial_dump: true, custom_feature_enabled: true });
for (let i = 0; i < connections; i++) {
  files.push(fs.createWriteStream(path.join(output, `connection-${i + 1}.jsonl`), { flags: 'wx' }));
  files[i].on('error', e => errors.push({ connection: i + 1, type: 'output', reason: e.message }));
  status.push({ connection: i + 1, opened: false, frames: 0, bytes: 0, errors: 0, unexpected_closes: 0 });
  const socket = new WebSocket(endpoint);
  sockets.push(socket);
  socket.onmessage = event => {
    // Timestamp before JSON decoding; all decoding and comparison happen offline.
    if (typeof event.data !== 'string') {
      errors.push({ connection: i + 1, type: 'binary', reason: 'unexpected binary frame' });
      return;
    }
    record(i, 'received', { raw: event.data });
    status[i].frames++;
    status[i].bytes += Buffer.byteLength(event.data);
  };
  socket.onerror = event => {
    status[i].errors++;
    const reason = event.message || event.error?.message || 'socket error';
    errors.push({ connection: i + 1, type: 'websocket', reason });
    record(i, 'error', { reason });
  };
  socket.onclose = event => {
    if (!stopping) status[i].unexpected_closes++;
    record(i, 'closed', { code: event.code, reason: event.reason, expected: stopping });
  };
}
await Promise.all(sockets.map((socket, i) => new Promise((resolve, reject) => {
  const timeout = setTimeout(() => reject(new Error(`Connection ${i + 1} failed to open`)), 15000);
  socket.onopen = () => {
    clearTimeout(timeout);
    status[i].opened = true;
    record(i, 'opened');
    resolve();
  };
}))).catch(error => {
  for (const socket of sockets) socket.close();
  for (const file of files) file.end();
  throw error;
});
const startedAt = new Date().toISOString();
const startedMono = mono();
const cpuStart = process.cpuUsage();
const loop = monitorEventLoopDelay({ resolution: 1 });
loop.enable();
for (let i = 0; i < connections; i++) {
  record(i, 'sent', { raw: subscription });
  sockets[i].send(subscription);
}
console.log(JSON.stringify({ phase: 'subscribed', connections, market_id: marketId, question: market.question, warmup_seconds: warmup, observation_seconds: seconds, grace_seconds: grace }));
const heartbeat = setInterval(() => {
  sockets.forEach((socket, i) => {
    if (socket.readyState === WebSocket.OPEN) {
      record(i, 'sent', { raw: 'PING' });
      socket.send('PING');
    }
  });
}, 10000);
const progress = setInterval(() => console.log(JSON.stringify({ phase: 'observing', elapsed_seconds: Math.round((mono() - startedMono) / 1000), frames_per_connection: status.map(s => s.frames), errors: errors.length })), 30000);
let interrupted = false;
await new Promise(resolve => {
  const timer = setTimeout(resolve, (warmup + seconds + grace) * 1000);
  process.once('SIGINT', () => { interrupted = true; clearTimeout(timer); resolve(); });
});
clearInterval(heartbeat);
clearInterval(progress);
stopping = true;
const stoppedMono = mono();
const cpu = process.cpuUsage(cpuStart);
loop.disable();
await Promise.all(sockets.map(socket => new Promise(resolve => {
  if (socket.readyState === WebSocket.CLOSED) return resolve();
  const timeout = setTimeout(resolve, 2500);
  socket.addEventListener('close', () => { clearTimeout(timeout); resolve(); }, { once: true });
  socket.close(1000, 'Observation complete');
})));
await Promise.all(files.map(file => new Promise((resolve, reject) => { file.once('error', reject); file.end(resolve); })));
const metadata = {
  market: { id: marketId, question: market.question, condition_id: market.conditionId, yes_asset_id: assetId, slug: market.slug, event_slug: market.events?.[0]?.slug },
  endpoint, runtime: process.version, connections, started_at: startedAt,
  started_mono_ms: startedMono, stopped_mono_ms: stoppedMono,
  warmup_seconds: warmup, observation_seconds: seconds, grace_seconds: grace,
  interrupted, status, errors, maximum_queued_output_bytes: maximumQueuedBytes,
  cpu_ms: { user: cpu.user / 1000, system: cpu.system / 1000 },
  event_loop_delay_ms: { median: loop.percentile(50) / 1e6, p99: loop.percentile(99) / 1e6, max: loop.max / 1e6 },
};
fs.writeFileSync(path.join(output, 'metadata.json'), `${JSON.stringify(metadata, null, 2)}\n`, { flag: 'wx' });
console.log(JSON.stringify({ phase: 'complete', status, errors, event_loop_delay_ms: metadata.event_loop_delay_ms }));
if (interrupted || errors.length || status.some(s => s.errors || s.unexpected_closes)) process.exitCode = 1;
