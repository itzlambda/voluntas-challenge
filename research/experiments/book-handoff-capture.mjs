// Public market data only. Node >= 22; no packages, credentials, or orders.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {setTimeout as sleep} from 'node:timers/promises';
const out = process.argv[2];
if (!out) throw Error('Usage: node research/experiments/book-handoff-capture.mjs PRIVATE_OUTPUT [JSON_OPTIONS]');
const config = {joins: 20, baseline_seconds: 45, join_seconds: 40, control_gap_seconds: 10, tail_seconds: 35, pilot_seconds: 25, market_id: null, ...JSON.parse(process.argv[3] ?? '{}')};
for (const key of ['joins', 'baseline_seconds', 'join_seconds', 'control_gap_seconds', 'tail_seconds', 'pilot_seconds']) if (!Number.isInteger(config[key]) || config[key] < 1) throw Error('Expected positive integer configuration');
if (config.joins > 30 || config.pilot_seconds + config.baseline_seconds + config.joins * (config.join_seconds + config.control_gap_seconds + 3) + config.tail_seconds > 1180) throw Error('Requested schedule exceeds bounded capture budget');
fs.mkdirSync(out, {recursive: false, mode: 0o700});
const sourceHash = crypto.createHash('sha256').update(fs.readFileSync(fileURLToPath(import.meta.url))).digest('hex');
const fd = fs.openSync(path.join(out, 'frames.jsonl'), 'wx', 0o600);
const endpoint = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';
let index = 0, bytes = 0, stopping = false;
const live = new Set();
const started = Date.now();
const stamp = () => ({received_utc: new Date().toISOString(), received_unix_ms: Date.now(), received_mono_ns: process.hrtime.bigint().toString()});
function log(row, at = stamp()) {
  const line = JSON.stringify({index: ++index, ...at, ...row}) + '\n';
  bytes += Buffer.byteLength(line);
  if (bytes > 512 * 1024 * 1024) throw Error('512 MiB capture limit');
  fs.writeSync(fd, line);
}
async function connect(feed, assets) {
  const ws = new WebSocket(endpoint);
  live.add(ws);
  let frame = 0;
  const books = new Set(), counts = {};
  const opened = new Promise((resolve, reject) => {
    ws.addEventListener('open', () => {
      log({kind: 'open', feed});
      const raw = JSON.stringify({type: 'market', assets_ids: assets, initial_dump: true});
      ws.send(raw); log({kind: 'sent', feed, raw}); resolve();
    });
    ws.addEventListener('error', e => {log({kind: 'error', feed, message: e.message ?? 'WebSocket error'}); reject(Error('WebSocket error'));});
  });
  ws.addEventListener('message', e => {
    const at = stamp();
    log({kind: 'received', feed, frame: ++frame, payload_type: typeof e.data, raw: e.data}, at);
    if (typeof e.data !== 'string') return;
    try {
      const value = JSON.parse(e.data);
      for (const event of Array.isArray(value) ? value : [value]) {
        counts[event.event_type] = (counts[event.event_type] ?? 0) + 1;
        if (event.event_type === 'book') books.add(event.asset_id);
        if (event.event_type === 'price_change') for (const change of event.price_changes ?? []) counts[change.asset_id] = (counts[change.asset_id] ?? 0) + 1;
      }
    } catch {if (e.data !== 'PONG') log({kind: 'non_json', feed, frame});}
  });
  ws.addEventListener('close', e => {live.delete(ws); log({kind: 'close', feed, code: e.code, reason: e.reason, was_clean: e.wasClean});});
  const heartbeat = setInterval(() => {if (ws.readyState === WebSocket.OPEN) {ws.send('PING'); log({kind: 'sent', feed, raw: 'PING'});}}, 10000);
  try {await Promise.race([opened, sleep(12000).then(() => {throw Error('open timeout');})]);}
  catch (e) {clearInterval(heartbeat); ws.close(); throw e;}
  return {ws, books, counts, async close() {
    clearInterval(heartbeat); log({kind: 'close_requested', feed});
    ws.close(1000, 'bounded observation complete');
    for (let i = 0; i < 30 && live.has(ws); i++) await sleep(100);
    if (live.has(ws)) log({kind: 'close_timeout', feed});
  }};
}
let previous = process.hrtime.bigint(), maxLagMs = 0;
const timer = setInterval(() => {
  const now = process.hrtime.bigint();
  const lag_ms = Math.max(0, Number(now - previous) / 1e6 - 1000);
  previous = now; maxLagMs = Math.max(maxLagMs, lag_ms);
  if (lag_ms > 100) log({kind: 'collector_lag', lag_ms});
}, 1000);
const hardStop = setTimeout(() => {log({kind: 'hard_timeout'}); process.exit(2);}, 1200000);
process.on('SIGTERM', () => {stopping = true; for (const ws of live) ws.close(1000, 'terminated');});
let meta = {started_utc: new Date(started).toISOString(), source_sha256: sourceHash, endpoint,
  subscription: {type: 'market', initial_dump: true}, joins_planned: config.joins, baseline_seconds: config.baseline_seconds,
  join_seconds: config.join_seconds, control_gap_seconds: config.control_gap_seconds, tail_seconds: config.tail_seconds, pilot_seconds: config.pilot_seconds,
  transport: 'Node built-in WebSocket: application messages; wire fragmentation and automatic protocol ping/pong are not exposed'};
try {
  const url = config.market_id ? `https://gamma-api.polymarket.com/markets?id=${encodeURIComponent(config.market_id)}&closed=false&active=true` : 'https://gamma-api.polymarket.com/markets?closed=false&active=true&limit=40&order=volume24hr&ascending=false';
  const response = await fetch(url, {signal: AbortSignal.timeout(20000)});
  const raw = await response.text();
  fs.writeFileSync(path.join(out, 'gamma.json'), raw, {flag: 'wx', mode: 0o600});
  if (!response.ok) throw Error(`Gamma HTTP ${response.status}`);
  const markets = JSON.parse(raw).filter(m => m.active && !m.closed && m.acceptingOrders && Date.parse(m.endDate) > Date.now() + 1800000 && JSON.parse(m.clobTokenIds ?? '[]').length === 2).slice(0, 8);
  const assets = markets.flatMap(m => JSON.parse(m.clobTokenIds));
  if (!assets.length) throw Error('No current binary candidates');
  log({kind: 'pilot_start', candidates: markets.map(m => m.id)});
  const pilot = await connect('pilot', assets);
  await sleep(config.pilot_seconds * 1000);
  const scored = markets.map(m => ({market: m, assets: JSON.parse(m.clobTokenIds), updates: JSON.parse(m.clobTokenIds).reduce((n, a) => n + (pilot.counts[a] ?? 0), 0)})).filter(m => m.assets.every(a => pilot.books.has(a))).sort((a, b) => b.updates - a.updates);
  await pilot.close();
  if (!scored.length || scored[0].updates === 0) throw Error('Pilot did not establish live price changes');
  const selected = scored[0];
  meta = {...meta, gamma_url: url, gamma_sha256: crypto.createHash('sha256').update(raw).digest('hex'),
    selected: Object.fromEntries(['id', 'question', 'conditionId', 'endDate', 'active', 'closed', 'acceptingOrders'].map(k => [k, selected.market[k]])),
    asset_ids: selected.assets, pilot_updates: selected.updates,
    pilot_scores: scored.map(x => ({id: x.market.id, updates: x.updates})), main_started_utc: new Date().toISOString()};
  fs.writeFileSync(path.join(out, 'meta.json'), JSON.stringify(meta, null, 2), {mode: 0o600});
  console.log(JSON.stringify({kind: 'selected', ...meta}));
  const feedA = await connect('A', selected.assets);
  await sleep(config.baseline_seconds * 1000);
  if (!selected.assets.every(a => feedA.books.has(a))) throw Error('Feed A lacks both baseline books');
  for (let join = 1; join <= config.joins && !stopping; join++) {
    if (feedA.ws.readyState !== WebSocket.OPEN) throw Error('Established feed A disconnected; no continuity claim');
    log({kind: 'join_start', join});
    const feedB = await connect(`B${String(join).padStart(2, '0')}`, selected.assets);
    await sleep(config.join_seconds * 1000);
    log({kind: 'join_end', join, initial_books: feedB.books.size, counts: feedB.counts});
    await feedB.close();
    console.log(JSON.stringify({kind: 'progress', join, elapsed_seconds: (Date.now() - started) / 1000, bytes, feedA_open: feedA.ws.readyState === WebSocket.OPEN}));
    await sleep(config.control_gap_seconds * 1000);
  }
  await sleep(config.tail_seconds * 1000); await feedA.close(); meta.completed = !stopping;
} catch (e) {meta.completed = false; meta.error = e.message; log({kind: 'experiment_error', message: e.message}); process.exitCode = 1;}
finally {
  stopping = true;
  for (const ws of live) ws.close(1000, 'experiment ended');
  for (let i = 0; i < 30 && live.size; i++) await sleep(100);
  clearInterval(timer); clearTimeout(hardStop);
  meta = {...meta, finished_utc: new Date().toISOString(), elapsed_seconds: (Date.now() - started) / 1000, max_event_loop_lag_ms: maxLagMs, bytes, records: index, remaining_sockets: live.size};
  log({kind: 'finished', ...meta});
  fs.writeFileSync(path.join(out, 'meta.json'), JSON.stringify(meta, null, 2), {mode: 0o600});
  fs.closeSync(fd); console.log(JSON.stringify({kind: 'finished', ...meta})); process.exit(process.exitCode ?? 0);
}
