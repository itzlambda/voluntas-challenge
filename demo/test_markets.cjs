const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const context = {TextDecoder, DataView, Uint8Array, URL, URLSearchParams, Map, Set, BigInt, Date, Number, JSON, Error};
vm.createContext(context);
for (const file of ['book-view.js', 'markets.js']) vm.runInContext(fs.readFileSync(`${__dirname}/${file}`, 'utf8'), context);
const B = context.DemoBooks, M = context.DemoMarkets;
let passed = 0;
function test(name, fn) { fn(); passed++; console.log(`ok ${name}`); }
const market = (n, extra = {}) => ({conditionId: `0x${n.toString(16).padStart(64, '0')}`, question: `Market ${n}`, outcomes: '["Yes","No"]',
  clobTokenIds: `["${n}1","${n}2"]`, outcomePrices: '["0.4","0.6"]', active: true, closed: false, archived: false, acceptingOrders: true, ...extra});

test('volumes are compact dollar amounts and missing volume shows nothing', () => {
  assert.equal(M.compactVolume(1870216), '$1.87M'); assert.equal(M.compactVolume(738438), '$738K');
  assert.equal(M.compactVolume(999), '$999'); assert.equal(M.compactVolume(0), '$0'); assert.equal(M.compactVolume(null), '');
});
test('top markets keep Gamma order, drop uncollected markets, and stop at ten', () => {
  const rows = [market(1, {acceptingOrders: false}), ...Array.from({length: 12}, (_, i) => market(i + 2)), market(20, {version: 'v2'})];
  const top = M.topMarkets(B, rows);
  assert.equal(top.length, 10); assert.equal(top[0].question, 'Market 2'); assert.equal(top[9].question, 'Market 11');
  assert.equal(M.topMarkets(B, {error: 'x'}), null);
});
test('search groups collected markets under their events in Polymarket order and counts the rest', () => {
  const body = {events: [
    {title: 'Bitcoin above ___?', markets: [market(1, {groupItemTitle: '70,000'}), market(2, {closed: true}), market(3, {groupItemTitle: '74,000'})]},
    {title: 'Only closed', markets: [market(4, {closed: true})]},
    {title: 'No markets'}, null]};
  const result = M.searchGroups(B, body);
  assert.equal(result.total, 2); assert.equal(result.hidden, 2); assert.equal(result.groups.length, 1);
  assert.deepEqual([...result.groups[0].markets.map(m => m.label)], ['70,000', '74,000']);
  assert.equal(result.groups[0].markets[0].event, 'Bitcoin above ___?');
  assert.equal(M.searchGroups(B, {pagination: {}}), null);
  assert.equal(M.searchGroups(B, {events: []}).total, 0);
});
test('a shared link names one valid condition ID', () => {
  const id = `0x${'A'.repeat(64)}`;
  assert.equal(M.marketParam(`?market=${id}`), id.toLowerCase());
  for (const search of ['', '?market=0x12', '?market=javascript:alert(1)', `?other=${id}`]) assert.equal(M.marketParam(search), null);
});
console.log(`${passed} market tests passed`);
