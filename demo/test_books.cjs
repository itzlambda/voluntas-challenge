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
console.log(`${passed} book tests passed`);
