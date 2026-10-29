// Socket traffic panel tests: bin fitting, filtering and pages, stale and null handling, history requests, the synthetic demo.
// Run with: node demo/test_sockets.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const DEMO = __dirname;

function load(files) {
  const context = {console, Date, Math, JSON, Number, Promise, Object, Array, Set, Map, String, Boolean, Error, Symbol, URL, URLSearchParams};
  context.globalThis = context;
  vm.createContext(context);
  for (const file of files) vm.runInContext(fs.readFileSync(path.join(DEMO, file), 'utf8'), context, {filename: file});
  return context;
}

const passed = [];
async function test(name, fn) { await fn(); passed.push(name); }
const settle = () => new Promise(resolve => setImmediate(resolve));
const NOW = 1_800_000_000;

/** A scripted browser. route(url) returns {status, body}, a promise of one, or null for a network failure. */
function fakeEnv(route) {
  let now = NOW * 1000;
  let nextId = 1;
  const timers = new Map();
  const listeners = {document: {}, window: {}};
  const env = {
    fetches: [],
    Date: {now: () => now},
    document: {visibilityState: 'visible', addEventListener(type, fn) { (listeners.document[type] ||= []).push(fn); }},
    navigator: {onLine: true},
    window: {addEventListener(type, fn) { (listeners.window[type] ||= []).push(fn); }},
    AbortSignal: {timeout: () => undefined},
    setTimeout(fn, delay) { const id = nextId++; timers.set(id, {fn, at: now + delay}); return id; },
    clearTimeout(id) { timers.delete(id); },
    async fetch(url) {
      env.fetches.push(url);
      const answer = await route(url);
      if (!answer) throw new TypeError('Failed to fetch');
      return {ok: answer.status === undefined || answer.status < 400, status: answer.status || 200, json: async () => answer.body};
    },
    async advance(ms) {
      now += ms;
      for (const [id, timer] of [...timers]) if (timer.at <= now) { timers.delete(id); timer.fn(); }
      await settle();
    },
    fire(target, type) { for (const fn of listeners[target][type] || []) fn(); },
  };
  return env;
}

function deferred() {
  let resolve;
  const promise = new Promise(r => { resolve = r; });
  return {promise, resolve};
}

const socket = (id, rate, extra = {}) => ({id, pool: id % 2 ? 'replica' : 'primary', connected: true, markets: 256, events_per_second: rate, state: 'connected', ...extra});
const fleet = (sockets, extra = {}) => ({schema_version: 1, status: 'live', retained: false, query_at: NOW, values_at: NOW - 3, age_seconds: 3,
  rate_window_seconds: 60, scrape_interval_seconds: 15, sockets, ...extra});
const history = (id, window, extra = {}) => ({schema_version: 1, status: 'ok', socket_id: id, pool: 'primary', window, rate_window_seconds: 60,
  start: NOW - 3600, end: NOW, step_seconds: 15, timestamps: [NOW - 30, NOW - 15, NOW], events_per_second: [1, null, 3], restarts: [NOW - 20], ...extra});

// Deterministic normal-ish values without Math.random.
function spread(count, center, width, seed = 1) {
  const out = [];
  let x = seed;
  for (let i = 0; i < count; i += 1) {
    x = (x * 1103515245 + 12345) % 2147483648;
    const y = (x * 1103515245 + 12345) % 2147483648;
    x = y;
    out.push(center + width * ((x / 2147483648) + (y / 2147483648 % 0.5) - 0.75));
  }
  return out;
}

(async () => {
  const ctx = load(['status.js', 'sockets.js']);
  const K = ctx.DemoSockets;
  const regular = layout => layout.edges.length - 1;
  const totals = (rates, layout) => {
    const counts = {};
    for (const rate of rates) { const key = K.binOf(rate, layout); counts[key] = (counts[key] || 0) + 1; }
    return counts;
  };

  await test('concentrated rates fit 8 to 15 round bins with nothing outside', () => {
    const rates = spread(2000, 15, 2);
    const layout = K.fitLayout(rates);
    assert.ok(regular(layout) >= 8 && regular(layout) <= 15, `${regular(layout)} bins`);
    const counts = totals(rates, layout);
    assert.equal(counts.under || 0, 0);
    assert.equal(counts.over || 0, 0);
    const step = layout.edges[1] - layout.edges[0];
    for (let i = 1; i < layout.edges.length; i += 1) assert.ok(Math.abs(layout.edges[i] - layout.edges[i - 1] - step) < 1e-9, 'equal widths');
    assert.ok(Math.max(...Object.values(counts)) < 2000 * 0.5, 'no single bar holds most sockets');
  });

  await test('exact zeros have their own bin and never enter a regular bin', () => {
    const rates = [...Array(30).fill(0), ...spread(300, 4, 3)];
    const layout = K.fitLayout(rates);
    assert.equal(layout.edges[0], 0, 'range starts at zero when the data nearly reaches it');
    assert.equal(K.binOf(0, layout), 'zero');
    assert.equal(K.binOf(0.001, layout), '0', 'a tiny positive rate is not zero');
    assert.equal(totals(rates, layout).zero, 30);
    assert.deepEqual([K.binLabel({kind: 'zero', lo: 0, hi: 0}), K.binLabel({kind: 'regular', lo: 0, hi: 1}), K.binLabel({kind: 'regular', lo: 1, hi: 2})], ['0', '>0', '1'],
      'the first positive bar is labelled apart from zero');
    const hist = K.histogram(rates.map((rate, id) => socket(id, rate)), layout);
    assert.equal(hist.bins[0].key, 'zero');
    assert.ok(!hist.bins.some(bin => bin.key === 'under'), 'no underflow bar when the range starts at zero');
    assert.equal(hist.bins.reduce((t, bin) => t + bin.count, 0), 330);
    const allZero = K.fitLayout([0, 0, 0]);
    assert.ok(regular(allZero) >= 8 && allZero.edges.every(Number.isFinite), 'all-zero fleets still get a usable range');
  });

  await test('identical rates fall in one regular bin with readable neighbours', () => {
    for (const value of [7.5, 0.0167, 12000]) {
      const layout = K.fitLayout(Array(500).fill(value));
      assert.ok(layout.edges.every(Number.isFinite));
      assert.ok(regular(layout) >= 5 && regular(layout) <= 15, `${value}: ${regular(layout)} bins`);
      const counts = totals(Array(500).fill(value), layout);
      assert.deepEqual(Object.keys(counts).length, 1, `${value} lands in one bin`);
      assert.ok(!('under' in counts) && !('over' in counts));
    }
  });

  await test('outliers go to the overflow bin instead of stretching the range', () => {
    const rates = [...spread(1990, 15, 2), 400, 520, 610, 700, 760, 800, 840, 870, 890, 900];
    const layout = K.fitLayout(rates);
    assert.ok(layout.edges[layout.edges.length - 1] < 40, `upper edge ${layout.edges.at(-1)}`);
    assert.ok(regular(layout) >= 8 && regular(layout) <= 15);
    assert.equal(totals(rates, layout).over, 10);
    const hist = K.histogram(rates.map((rate, id) => socket(id, rate)), layout);
    assert.equal(hist.drift, false, 'ten outliers in two thousand do not ask for a refit');
    assert.equal(K.binLabel(hist.bins.at(-1)), `≥${layout.edges.at(-1)}`);
    // A low cluster far below the main mass goes to underflow when the range does not start at zero.
    const low = K.fitLayout([...spread(500, 100, 4), 0.5, 0.6]);
    assert.ok(low.edges[0] > 1);
    assert.equal(K.binOf(0.5, low), 'under');
  });

  await test('unknown states and null rates are counted apart, never as zero', () => {
    const sockets = [
      socket(1, 0), socket(2, 5),
      socket(3, null), // connected but no fresh rate
      socket(4, null, {state: 'warming_up'}),
      socket(5, null, {state: 'disconnected', connected: false}),
      socket(6, null, {state: 'empty', markets: 0}),
      socket(7, 3, {markets: 0}), // a rate on a socket with no markets is not in the distribution
    ];
    const parsed = K.parseFleet(fleet(sockets)).sockets;
    assert.deepEqual([...parsed.map(K.category)], ['rated', 'rated', 'no_rate', 'warming_up', 'disconnected', 'empty', 'empty']);
    assert.equal(parsed[2].rate, null);
    const hist = K.histogram(parsed, K.fitLayout([0, 5]));
    assert.deepEqual({...hist.categories}, {rated: 2, no_rate: 1, warming_up: 1, disconnected: 1, empty: 2});
    assert.equal(hist.bins.find(bin => bin.key === 'zero').count, 1, 'only the real zero');
    const rows = K.selectRows(parsed, null, {sort: 'quietest'});
    assert.deepEqual([...rows.map(s => s.id)], [1, 7, 2, 3, 4, 5, 6], 'sockets without a rate sort last, even quietest first');
    assert.equal(K.rateText(null), null);
    assert.equal(K.rateText(0), '0');
  });

  await test('fleet and history bodies are validated', () => {
    for (const bad of [null, {}, {schema_version: 2, status: 'live', sockets: []}, {schema_version: 1, status: 'ok', sockets: []}]) assert.equal(K.parseFleet(bad), null);
    const mixed = K.parseFleet(fleet([socket(1, 2), {id: -1, pool: 'primary', state: 'connected'}, {id: 70000, pool: 'primary', state: 'connected'},
      {id: 2, pool: 'other', state: 'connected'}, {id: 3, pool: 'primary', state: 'weird'}, socket(1, 9), socket(4, -3), socket(5, Infinity)]));
    assert.deepEqual([...mixed.sockets.map(s => s.id)], [1, 4, 5], 'malformed and duplicate rows are dropped');
    assert.equal(mixed.sockets[1].rate, null, 'negative rates are not data');
    assert.equal(mixed.sockets[2].rate, null, 'non-finite rates are not data');
    assert.equal(K.parseFleet({schema_version: 1, status: 'unavailable'}).sockets, null, 'missing arrays mean no fleet, not an empty fleet');
    assert.equal(K.parseHistory(history(4, '1h'), 5, '1h'), null, 'another socket is rejected');
    assert.equal(K.parseHistory(history(4, '6h'), 4, '1h'), null, 'another window is rejected');
    const error = K.parseHistory({schema_version: 1, status: 'unavailable', socket_id: 4, window: '1h'}, 4, '1h');
    assert.equal(error.timestamps, null);
    assert.deepEqual([...error.restarts], []);
    assert.equal(K.parseHistory(history(4, '1h', {events_per_second: [1, 2]}), 4, '1h').timestamps, null, 'mismatched arrays are not drawn');
    assert.deepEqual([...K.gaps([1, null, null, 2, null])].map(r => [...r]), [[1, 2], [4, 4]]);
  });

  await test('a bar filters the list, which pages in 25 rows', () => {
    const sockets = K.parseFleet(fleet([...Array(200).keys()].map(id => socket(id, id < 60 ? 10 + id / 100 : 20 + id / 10)))).sockets;
    const layout = {edges: [0, 5, 10, 15, 20, 25, 30, 35, 40]};
    const filter = {kind: 'bin', key: K.binOf(10.3, layout)};
    const rows = K.selectRows(sockets, layout, {filter, sort: 'busiest'});
    assert.equal(rows.length, 60);
    assert.ok(rows.every(s => s.rate >= 10 && s.rate < 15));
    assert.equal(rows[0].id, 59, 'busiest first');
    const pages = [0, 1, 2, 9].map(page => K.paginate(rows, page));
    assert.deepEqual(pages.map(p => p.items.length), [25, 25, 10, 10], 'the last page is clamped');
    assert.equal(pages[3].page, 2);
    assert.equal(pages[1].start, 25);
    assert.equal(K.selectRows(sockets, layout, {filter, sort: 'quietest'})[0].id, 0);
    assert.deepEqual([...K.selectRows(sockets, layout, {query: '#19', sort: 'id'}).map(s => s.id)], [19, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]);
    assert.equal(K.selectRows(sockets, layout, {query: 'abc'}).length, 0, 'non-numeric searches match nothing');
    assert.ok(K.selectRows(sockets, layout, {pool: 'replica'}).every(s => s.pool === 'replica'));
    assert.equal(K.selectRows(sockets, layout, {filter: {kind: 'state', key: 'disconnected'}}).length, 0);
  });

  await test('controller: bins stay fixed across refreshes until Fit range', async () => {
    let rates = spread(400, 15, 2);
    const env = fakeEnv(url => url === '/api/demo/sockets' ? {body: fleet(rates.map((rate, id) => socket(id, rate)))} : null);
    const c = K.createController({env});
    c.start();
    await settle();
    const first = [...c.view().layout.edges];
    c.setFilter({kind: 'bin', key: '3'});
    rates = spread(400, 40, 3, 7);
    await env.advance(5000);
    const v = c.view();
    assert.deepEqual([...v.layout.edges], first, 'boundaries are stable');
    assert.equal(v.filter.key, '3', 'the selected bar survives the refresh');
    assert.equal(v.hist.drift, true, 'the panel suggests refitting');
    c.fit();
    assert.notDeepEqual([...c.view().layout.edges], first);
    assert.equal(c.view().filter, null, 'refitting clears a bar filter that no longer exists');
    assert.equal(c.view().hist.drift, false);
    assert.ok(env.fetches.every(url => url === '/api/demo/sockets'), 'no history without a selection');
  });

  await test('controller: failures keep the last fleet with a growing age and honest state', async () => {
    const answers = [{body: fleet([socket(1, 4), socket(2, null)])}, null, {body: {schema_version: 1, status: 'unavailable'}},
      {body: fleet([socket(1, 4)], {status: 'stale', retained: true, age_seconds: 90})}];
    const env = fakeEnv(() => answers.shift());
    const c = K.createController({env});
    c.start();
    await settle();
    assert.equal(c.view().fleetState, 'live');
    assert.match(K.fleetLine(c.view()), /^Updated 3 s ago$/);
    await env.advance(5000);
    let v = c.view();
    assert.equal(v.fleetState, 'error');
    assert.equal(v.fleet.sockets.length, 2, 'the last fleet stays');
    assert.match(K.fleetLine(v), /^Could not refresh, showing values from 8 s ago/);
    await env.advance(30000);
    v = c.view();
    assert.equal(v.fleetState, 'unavailable');
    assert.equal(v.fleet.sockets.length, 2, 'a body without sockets keeps the last fleet');
    assert.match(K.fleetLine(v), /^Unavailable, showing values from/);
    await env.advance(30000);
    v = c.view();
    assert.equal(v.fleetState, 'stale');
    assert.match(K.fleetLine(v), /^Stale, values from 2 min ago$/);
    assert.equal(K.fleetLine({fleetState: 'unavailable', fleet: null, age: null}), 'Socket telemetry unavailable');
  });

  await test('controller: history loads for the selected socket only, and late answers are dropped', async () => {
    const pending = {};
    const env = fakeEnv(url => {
      if (url === '/api/demo/sockets') return {body: fleet([...Array(2000).keys()].map(id => socket(id, 10 + (id % 7))))};
      const match = url.match(/socket=(\d+)&window=(\w+)/);
      const gate = deferred();
      pending[`${match[1]}/${match[2]}`] = gate;
      return gate.promise;
    });
    const c = K.createController({env});
    c.start();
    await settle();
    assert.deepEqual(env.fetches, ['/api/demo/sockets']);
    c.select(7);
    await settle();
    c.select(8);
    await settle();
    assert.deepEqual(env.fetches.slice(1), ['/api/demo/socket-history?socket=7&window=1h', '/api/demo/socket-history?socket=8&window=1h']);
    pending['8/1h'].resolve({body: history(8, '1h')});
    await settle();
    pending['7/1h'].resolve({body: history(7, '1h', {events_per_second: [99, 99, 99]})});
    await settle();
    let v = c.view();
    assert.equal(v.history.socket, 8);
    assert.deepEqual([...v.history.values], [1, null, 3], 'nulls stay nulls');
    assert.deepEqual([...v.history.restarts], [NOW - 20]);
    c.setWindow('24h');
    assert.equal(c.view().history, null, 'the previous window is not shown under the new one');
    await settle();
    assert.equal(env.fetches.at(-1), '/api/demo/socket-history?socket=8&window=24h');
    pending['8/24h'].resolve({body: {schema_version: 1, status: 'unavailable', socket_id: 8, window: '24h'}});
    await settle();
    v = c.view();
    assert.equal(v.historyState, 'unavailable');
    assert.equal(v.history, null);
    c.setWindow('2d');
    assert.equal(c.view().window, '24h', 'unknown windows are ignored');
    const historyCalls = env.fetches.filter(url => url.includes('socket-history'));
    assert.equal(new Set(historyCalls.map(url => url.match(/socket=(\d+)/)[1])).size, 2, 'never more sockets than were selected');
  });

  await test('controller: history refreshes no faster than 15 s; pause and hidden tabs stop polling', async () => {
    const env = fakeEnv(url => url === '/api/demo/sockets' ? {body: fleet([socket(1, 2)])} : {body: history(1, '1h')});
    const c = K.createController({env});
    c.start();
    await settle();
    c.select(1);
    await settle();
    await env.advance(5000);
    const counts = () => [env.fetches.filter(u => u.endsWith('sockets')).length, env.fetches.filter(u => u.includes('history')).length];
    assert.deepEqual(counts(), [2, 1]);
    await env.advance(5000);
    await env.advance(5000);
    assert.deepEqual(counts(), [4, 2], 'history every 15 s, fleet every 5 s');
    c.setPaused(true);
    await env.advance(5000);
    await env.advance(5000);
    assert.deepEqual(counts(), [4, 2].map((n, i) => i === 0 ? n : counts()[1]), 'no fleet requests while paused');
    assert.match(K.fleetLine(c.view()), /^Paused, showing values from/);
    c.setPaused(false);
    await settle();
    assert.equal(counts()[0], 5, 'resuming fetches at once');
    env.document.visibilityState = 'hidden';
    env.fire('document', 'visibilitychange');
    const before = env.fetches.length;
    await env.advance(60000);
    assert.equal(env.fetches.length, before, 'nothing is fetched while hidden');
    env.document.visibilityState = 'visible';
    env.fire('document', 'visibilitychange');
    await settle();
    assert.ok(env.fetches.length > before, 'visible again polls at once');
  });

  await test('controller: filters reset the page; selection and window survive refreshes', async () => {
    const env = fakeEnv(url => url === '/api/demo/sockets' ? {body: fleet([...Array(120).keys()].map(id => socket(id, id + 1)))} : {body: history(3, '6h')});
    const c = K.createController({env});
    c.start();
    await settle();
    c.setPage(3);
    assert.equal(c.view().page.page, 3);
    c.setPage(99);
    assert.equal(c.view().page.page, 4, 'clamped to the last page');
    c.setSort('quietest');
    assert.equal(c.view().page.page, 0);
    assert.equal(c.view().page.items[0].id, 0);
    c.setWindow('6h');
    c.select(3);
    await env.advance(5000);
    const v = c.view();
    assert.equal(v.selected, 3);
    assert.equal(v.window, '6h');
    assert.equal(v.selectedSocket.id, 3);
    c.setFilter({kind: 'state', key: 'empty'});
    c.setFilter({kind: 'state', key: 'empty'});
    assert.equal(c.view().filter, null, 'selecting the same filter again clears it');
  });

  // Synthetic demo fixture.
  const fixtureContext = load(['socket-demo.js']);
  const F = fixtureContext.DemoSocketFixture;
  const json = async response => ({status: response.status, body: await response.json()});

  await test('synthetic demo: deterministic fleet with every state, zeros and outliers', async () => {
    const a = F.create({now: () => NOW * 1000});
    const b = F.create({now: () => NOW * 1000});
    const one = await json(await a.fetch('/api/demo/sockets'));
    const two = await json(await b.fetch('/api/demo/sockets'));
    assert.deepEqual(one, two, 'the same seed gives the same fleet');
    const parsed = K.parseFleet(one.body);
    assert.equal(parsed.sockets.length, 2000);
    const hist = K.histogram(parsed.sockets, K.fitLayout(parsed.sockets.filter(s => K.category(s) === 'rated').map(s => s.rate)));
    for (const key of K.CATEGORIES) assert.ok(hist.categories[key] > 0, `has ${key}`);
    assert.ok(hist.bins.find(bin => bin.key === 'zero').count >= 40);
    assert.ok(hist.bins.find(bin => bin.key === 'over').count >= 5, 'outliers are visible');
    const regularBins = hist.bins.filter(bin => bin.kind === 'regular');
    assert.ok(regularBins.length >= 8 && regularBins.length <= 15);
    const later = F.create({now: () => (NOW + 15) * 1000}).fleet();
    assert.notDeepEqual(later.sockets.map(s => s.events_per_second), one.body.sockets.map(s => s.events_per_second), 'rates move between samples');
  });

  await test('synthetic demo: history with gaps, a restart and the contract limits', async () => {
    const fixture = F.create({now: () => NOW * 1000});
    const rated = fixture.fleet().sockets.find(s => s.state === 'connected' && s.events_per_second > 1);
    for (const window of K.WINDOWS) {
      const {status, body} = await json(await fixture.fetch(`/api/demo/socket-history?socket=${rated.id}&window=${window}`));
      assert.equal(status, 200);
      const h = K.parseHistory(body, rated.id, window);
      assert.ok(h && h.timestamps.length > 0 && h.timestamps.length <= 1000, `${window}: ${h.timestamps.length} points`);
      assert.ok(h.timestamps.every((t, i) => i === 0 || t > h.timestamps[i - 1]));
      if (window !== '15m') {
        assert.equal(h.restarts.length, 1, `${window} shows the restart`);
        assert.ok(h.values.some(v => v === null) && h.values.some(v => v > 0), `${window} has gaps and data`);
      }
    }
    const current = fixture.fleet();
    for (const sample of current.sockets.filter(s => s.events_per_second !== null && s.events_per_second > 0).slice(0, 50)) {
      const h = (await json(await fixture.fetch(`/api/demo/socket-history?socket=${sample.id}&window=1h`))).body;
      assert.equal(h.timestamps.at(-1), current.values_at);
      assert.equal(h.events_per_second.at(-1), sample.events_per_second, `socket ${sample.id}: the current rate is the latest history point`);
      const before = h.events_per_second.filter((v, i) => v !== null && h.timestamps[i] < h.restarts[0]);
      const after = h.events_per_second.filter((v, i) => v !== null && h.timestamps[i] >= h.restarts[0]);
      const mean = values => values.reduce((t, v) => t + v, 0) / values.length;
      assert.ok(mean(before) > mean(after), 'the earlier market set was busier');
    }
    for (const bad of ['socket=x&window=1h', 'socket=1&window=2d', 'socket=70000&window=1h', 'socket=1&window=toString']) {
      assert.equal((await fixture.fetch(`/api/demo/socket-history?${bad}`)).status, 400, bad);
    }
    const empty = fixture.fleet().sockets.find(s => s.state === 'empty');
    const h = (await json(await fixture.fetch(`/api/demo/socket-history?socket=${empty.id}&window=1h`))).body;
    assert.ok(h.events_per_second.every(v => v === null), 'a socket without markets has no rate, not zero');
  });

  await test('synthetic demo drives the controller end to end: bar, 25 rows, one history', async () => {
    let fixture;
    const env = fakeEnv(url => fixture.fetch(url).then(json));
    fixture = F.create({now: () => env.Date.now()});
    const c = K.createController({env});
    c.start();
    await settle();
    let v = c.view();
    const busiest = v.hist.bins.filter(bin => bin.kind === 'regular').sort((a, b) => b.count - a.count)[0];
    assert.ok(busiest.count > 25);
    c.setFilter({kind: 'bin', key: busiest.key});
    v = c.view();
    assert.equal(v.page.items.length, 25);
    assert.equal(v.page.total, busiest.count);
    c.select(v.page.items[0].id);
    await settle();
    v = c.view();
    assert.equal(v.historyState, 'ok');
    assert.equal(v.history.socket, v.page.items[0].id);
    assert.equal(env.fetches.filter(url => url.includes('socket-history')).length, 1);
  });

  await test('page markup: separate panel, external scripts, demo fixture only on request', () => {
    const raw = fs.readFileSync(path.join(DEMO, 'live.html'), 'utf8');
    const panel = raw.match(/<section class="panel socket-panel"[\s\S]*?<\/section>/)[0];
    assert.doesNotMatch(panel, /data-chart|data-plot|data-range=/, 'the generic chart loop does not pick up the panel');
    assert.deepEqual([...panel.matchAll(/data-socket-window="([^"]+)" aria-pressed="(\w+)"/g)].map(m => `${m[1]}:${m[2]}`), ['15m:false', '1h:true', '6h:false', '24h:false']);
    assert.match(raw, /<script src="\/sockets.js" defer><\/script>/);
    assert.doesNotMatch(raw, /socket-demo\.js/, 'the fixture is not loaded by default');
    assert.doesNotMatch(panel, /\sstyle="/);
    const visible = panel.replace(/<[^>]+>/g, ' ');
    assert.doesNotMatch(visible, / · | — |→|»/);
    assert.doesNotMatch(visible, /per market\b(?! rates)/i);
    const source = fs.readFileSync(path.join(DEMO, 'sockets.js'), 'utf8');
    assert.doesNotMatch(source, /innerHTML|setAttribute\('style'/, 'socket data never becomes markup; no style attributes under the CSP');
    for (const file of ['sockets.js', 'socket-demo.js']) {
      const text = fs.readFileSync(path.join(DEMO, file), 'utf8');
      assert.doesNotMatch(text, /\/home\/|\/Users\/|\b(?:\d{1,3}\.){3}\d{1,3}\b|localhost|admin@/, `${file}: no paths, hosts or IPs`);
    }
  });

  console.log(`Socket panel tests passed (${passed.length}): ${passed.join('; ')}.`);
})().catch(error => { console.error(error); process.exit(1); });
