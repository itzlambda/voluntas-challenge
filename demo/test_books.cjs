const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const context = {TextDecoder, DataView, Uint8Array, URL, URLSearchParams, Map, Set, BigInt, Date, Number, JSON, Error};
vm.createContext(context);
vm.runInContext(fs.readFileSync(`${__dirname}/book-view.js`, 'utf8'), context);
const B = context.DemoBooks;
const {record, frame} = require('./book-fixture.cjs');
let passed = 0;
function test(name, fn) { fn(); passed++; console.log(`ok ${name}`); }
test('shared Rust encoder fixture decodes snapshots and deletion exactly', () => {
  const bytes = fs.readFileSync(`${__dirname}/fixtures/books.sbe`);
  const records = B.decodeFrame(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength));
  assert.equal(records.length, 3); assert.equal(records[0].bids[0][1], 12345678901234567n);
  const book = B.emptyBook(); B.apply(book, records[0]); B.apply(book, records[1]);
  assert.equal(book.bids.size, 0); assert.equal(records[2].symbol, '456');
  assert.equal(records[2].bids.length + records[2].asks.length, 0);
});
test('fixed point never rounds through Number', () => {
  assert.equal(B.fixed(12345678901234567n), '12345678901.234567'); assert.equal(B.fixed(1000000n), '1'); assert.equal(B.fixed(-1n), '-0.000001');
});
test('coalesced records decode in order with large exact quantities', () => {
  const records = B.decodeFrame(frame(record(1), record(2), record(1, '456', {bids: [], asks: []})));
  assert.equal(records.length, 3); assert.equal(records[0].bids[0][1], 12345678901234567n); assert.equal(records[2].symbol, '456');
});
test('updates wait for baselines, zero deletes levels, snapshots replace everything', () => {
  const book = B.emptyBook(), update = B.decodeFrame(frame(record(2, '123', {quantity: 10n})))[0];
  B.apply(book, update); assert.equal(book.bids.size, 0);
  B.apply(book, B.decodeFrame(frame(record(1)))[0]); B.apply(book, update); assert.equal(book.bids.get(450001n), 10n);
  B.apply(book, {...update, quantity: 0n}); assert.equal(book.bids.size, 0);
  B.apply(book, B.decodeFrame(frame(record(1, '123', {bids: [], asks: []})))[0]); assert.equal(book.asks.size, 0); assert.equal(book.ready, true);
});
test('malformed frames fail atomically and bounds are checked', () => {
  const snapshot = record(1);
  for (let end = 1; end < snapshot.length; end++) assert.throws(() => B.decodeFrame(frame(snapshot.subarray(0, end))));
  for (const [offset, value] of [[2, 9], [4, 8], [6, 99], [24, 0]]) { const b = Buffer.from(snapshot); b[offset] = value; assert.throws(() => B.decodeFrame(frame(b))); }
  assert.throws(() => B.decodeFrame(frame(record(2, '123', {side: 2}))));
  assert.throws(() => B.decodeFrame(frame(record(2, '123', {quantity: -1n}))));
  assert.throws(() => B.decodeFrame(frame(record(1, 'bad'))));
});
test('bids descend and asks ascend without numeric conversion', () => {
  const book = B.emptyBook(); book.bids = new Map([[1n, 1n], [3n, 1n], [2n, 1n]]); book.asks = book.bids;
  assert.equal(B.sortedLevels(book, 'bids').map(x => x[0]).join(), '3,2,1'); assert.equal(B.sortedLevels(book, 'asks').map(x => x[0]).join(), '1,2,3');
});
test('metadata parses Gamma strings and rejects unsafe icons or missing outcomes', () => {
  const raw = {conditionId: `0x${'a'.repeat(64)}`, outcomes: '["Yes","No"]', clobTokenIds: '["123","456"]', outcomePrices: '["0.4","0.6"]', icon: 'javascript:alert(1)'};
  const parsed = B.metadata(raw); assert.equal(parsed.tokens[1], '456'); assert.equal(parsed.icon, null);
  assert.equal(B.metadata({...raw, outcomes: '[]'}), null); assert.equal(B.metadata({...raw, clobTokenIds: '["123","123"]'}), null);
});
function env() {
  const sockets = [], timers = new Map(); let next = 0;
  class WebSocket {
    constructor(url) { this.url = url; sockets.push(this); }
    send(value) { this.sent = JSON.parse(value); }
    close() { this.closed = true; }
  }
  return {WebSocket, sockets, timers, setTimeout(fn) { const id = ++next; timers.set(id, fn); return id; }, clearTimeout(id) { timers.delete(id); }};
}
test('reconnect clears both baselines and resubscribes; stale messages cannot change books', () => {
  for (const code of [1006, 1013]) {
    const e = env(), feed = B.createFeed(e, 'wss://stream.example.com', ['123', '456'], () => {}), first = e.sockets[0];
    first.onopen(); assert.equal(first.sent.books.join(), '123,456');
    first.onmessage({data: frame(record(1), record(1, '456'))}); assert.equal(feed.state.status, 'live');
    first.onclose({code}); assert.equal(feed.state.books.get('123').ready, false);
    [...e.timers.values()][0](); const second = e.sockets[1]; second.onopen();
    first.onmessage({data: frame(record(1))}); assert.equal(feed.state.books.get('123').ready, false);
    second.onmessage({data: frame(record(2, '123', {quantity: 99n}))}); assert.equal(feed.state.books.get('123').bids.size, 0);
    second.onmessage({data: frame(record(1))}); assert.equal(feed.state.status, 'waiting');
    feed.stop(); assert.equal(second.closed, true);
  }
});
test('policy rejection does not retry and stop cancels pending reconnect', () => {
  const e = env(), feed = B.createFeed(e, 'wss://stream.example.com', ['123'], () => {});
  e.sockets[0].onclose({code: 1008}); assert.equal(feed.state.status, 'rejected'); assert.equal(e.timers.size, 0); feed.stop();
  const f = env(), another = B.createFeed(f, 'wss://stream.example.com', ['123'], () => {});
  f.sockets[0].onclose({code: 1013}); assert.equal(f.timers.size, 1); another.stop(); assert.equal(f.timers.size, 0);
});
test('snapshot freshness ages into stale even if updates continue', () => {
  const book = B.emptyBook(); book.ready = true; book.snapshotAt = 1000;
  const state = {status: 'live', books: new Map([['123', book]])};
  assert.equal(B.feedStatus(state, 136000), 'live');
  B.apply(book, {type: 'update', side: 0, price: 1n, quantity: 2n});
  assert.equal(B.feedStatus(state, 136001), 'stale');
  book.snapshotAt = 136001; assert.equal(B.feedStatus(state, 136001), 'live');
  state.status = 'reconnecting'; assert.equal(B.feedStatus(state, 999999), 'reconnecting');
});
test('decode errors immediately clear all baselines and stop after two attempts', () => {
  const e = env(), feed = B.createFeed(e, 'wss://stream.example.com', ['123'], () => {});
  const first = e.sockets[0]; first.onopen(); first.onmessage({data: frame(record(1))});
  const bad = record(1); bad.writeUInt16LE(99, 6);
  first.onmessage({data: frame(bad)}); assert.equal(feed.state.books.get('123').ready, false);
  first.onclose({code: 1000});
  const reconnect = [...e.timers.values()][0]; e.timers.clear(); reconnect();
  const second = e.sockets[1]; second.onopen(); second.onmessage({data: frame(bad)}); second.onclose({code: 1000});
  assert.equal(feed.state.status, 'incompatible'); assert.equal(e.timers.size, 0); feed.stop();
});
test('market pagination has no omissions and clamps first, last, and empty pages', () => {
  const entries = Array.from({length: 23}, (_, id) => id);
  const first = B.marketPage(entries, -1), second = B.marketPage(entries, 1), last = B.marketPage(entries, 999);
  assert.equal(B.MARKET_PAGE_SIZE, 5);
  assert.equal(first.items.length, 5); assert.equal(second.items.length, 5); assert.equal(last.items.length, 3);
  assert.equal(last.page, 4); assert.equal(last.pages, 5);
  const all = Array.from({length: last.pages}, (_, page) => B.marketPage(entries, page).items).flat();
  assert.equal(all.join(), entries.join());
  assert.equal(B.marketPage([], 4).page, 0); assert.equal(B.marketPage([], 4).items.length, 0);
});
test('ladder prices show exact cents and grouped exact shares', () => {
  assert.equal(B.cents(945000n), '94.5¢'); assert.equal(B.cents(10000n), '1¢'); assert.equal(B.cents(1000n), '0.1¢');
  assert.equal(B.shares(12345678901234567n), '12,345,678,901.234567'); assert.equal(B.shares(2500000n), '2.5'); assert.equal(B.shares(999000000n), '999');
});
test('midpoint chance and spread come from the best levels, without floats', () => {
  const book = B.emptyBook();
  book.bids = new Map([[940000n, 1n], [930000n, 1n]]); book.asks = new Map([[950000n, 1n], [960000n, 1n]]);
  assert.deepEqual({...B.midSpread(book)}, {chance: '94.5%', spread: '1¢'});
  book.asks = new Map([[941000n, 1n]]); assert.deepEqual({...B.midSpread(book)}, {chance: '94.1%', spread: '0.1¢'});
  book.asks = new Map([[960000n, 1n]]); assert.equal(B.midSpread(book).chance, '95%');
  book.asks = new Map(); assert.equal(B.midSpread(book), null);
});
test('quoted chance rounds like Polymarket and marks the extremes', () => {
  assert.equal(B.chance('0.62'), '62%'); assert.equal(B.chance('0.945'), '95%'); assert.equal(B.chance('0.004'), '<1%');
  assert.equal(B.chance('0.996'), '>99%'); assert.equal(B.chance('0'), '0%'); assert.equal(B.chance('1'), '100%');
  assert.equal(B.chance(null), ''); assert.equal(B.chance('x'), '');
});
test('collected markets follow the collector discovery rule', () => {
  const raw = {conditionId: `0x${'b'.repeat(64)}`, outcomes: '["Yes","No"]', clobTokenIds: '["1","2"]', active: true, closed: false, archived: false, acceptingOrders: true};
  assert.equal(B.collectable(raw), true);
  assert.equal(B.collectable({...raw, version: 'v1'}), true);
  for (const change of [{version: 'v2'}, {active: false}, {active: undefined}, {closed: true}, {archived: true}, {acceptingOrders: false}, {acceptingOrders: undefined}, {clobTokenIds: '["1"]'}]) {
    assert.equal(B.collectable({...raw, ...change}), false, JSON.stringify(change));
  }
  assert.equal(B.collectable(null), false);
});
test('metadata carries the group label, event title, and 24-hour volume when present', () => {
  const raw = {conditionId: `0x${'c'.repeat(64)}`, outcomes: '["Yes","No"]', clobTokenIds: '["1","2"]', groupItemTitle: ' 72,000 ', events: [{title: 'Bitcoin above ___?'}], volume24hr: 1234.5};
  const parsed = B.metadata(raw);
  assert.equal(parsed.label, '72,000'); assert.equal(parsed.event, 'Bitcoin above ___?'); assert.equal(parsed.volume24hr, 1234.5);
  const bare = B.metadata({...raw, groupItemTitle: '', events: 'x', volume24hr: -1});
  assert.equal(bare.label, null); assert.equal(bare.event, null); assert.equal(bare.volume24hr, null);
});
function clockEnv() { const e = env(); e.clock = 0; e.Date = {now: () => e.clock}; return e; }
test('a close keeps the last ready books for display until a new snapshot replaces them', () => {
  const e = env(), feed = B.createFeed(e, 'wss://stream.example.com', ['123', '456'], () => {}), first = e.sockets[0];
  first.onopen(); first.onmessage({data: frame(record(1), record(1, '456'))});
  first.onclose({code: 1006});
  assert.equal(feed.state.books.get('123').ready, false, 'the live baseline is still discarded');
  assert.equal(feed.state.previous.get('123').bids.get(450001n), 12345678901234567n);
  [...e.timers.values()][0](); const second = e.sockets[1]; second.onopen();
  second.onmessage({data: frame(record(1))});
  assert.equal(feed.state.previous.has('123'), false); assert.equal(feed.state.previous.has('456'), true);
  feed.stop();
  const f = env(), rejected = B.createFeed(f, 'wss://stream.example.com', ['123'], () => {});
  f.sockets[0].onopen(); f.sockets[0].onmessage({data: frame(record(1))}); f.sockets[0].onclose({code: 1008});
  assert.equal(rejected.state.previous.size, 0, 'a rejected market shows no old orders'); rejected.stop();
});
test('hidden tabs keep the socket; reconnects wait until visible', () => {
  let hidden = false;
  const e = env(), feed = B.createFeed(e, 'wss://stream.example.com', ['123'], () => {}, {paused: () => hidden}), first = e.sockets[0];
  first.onopen(); first.onmessage({data: frame(record(1))});
  hidden = true;
  first.onmessage({data: frame(record(2, '123', {quantity: 5n}))});
  assert.equal(feed.state.books.get('123').bids.get(450001n), 5n, 'records still apply while hidden');
  assert.equal(first.closed, undefined, 'hiding does not close the socket');
  first.onclose({code: 1006});
  assert.equal(e.timers.size, 0, 'no reconnect while hidden'); assert.equal(e.sockets.length, 1);
  feed.check(); assert.equal(e.sockets.length, 1, 'check does nothing while hidden');
  hidden = false; feed.check(); assert.equal(e.sockets.length, 2, 'shown again: reconnect now');
  feed.check(); assert.equal(e.sockets.length, 2, 'only once');
  feed.stop();
  // A backoff timer that fires while hidden also defers.
  const f = env(); let away = false;
  const other = B.createFeed(f, 'wss://stream.example.com', ['123'], () => {}, {paused: () => away});
  f.sockets[0].onclose({code: 1013}); away = true; [...f.timers.values()][0]();
  assert.equal(f.sockets.length, 1); away = false; other.check(); assert.equal(f.sockets.length, 2); other.stop();
});
test('a socket silent for 150 s after receiving data is replaced', () => {
  const e = clockEnv(), feed = B.createFeed(e, 'wss://stream.example.com', ['123'], () => {}), first = e.sockets[0];
  first.onopen(); e.clock = 1000; feed.check(); e.clock = 500000; feed.check();
  assert.equal(e.sockets.length, 1, 'no data yet: a market that is not collected does not loop');
  e.clock = 1000000; first.onmessage({data: frame(record(1))});
  e.clock = 1150000; feed.check(); assert.equal(e.sockets.length, 1, 'exactly 150 s is still alive');
  e.clock = 1150001; feed.check(); assert.equal(e.sockets.length, 2, 'silent socket replaced');
  assert.equal(first.closed, true); assert.equal(first.onmessage, null, 'the old socket can no longer change books');
  assert.equal(feed.state.previous.has('123'), true, 'the last book stays visible while resyncing');
  assert.equal(feed.state.status, 'connecting');
  feed.stop();
});
console.log(`${passed} book tests passed`);
