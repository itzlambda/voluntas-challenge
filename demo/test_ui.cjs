// UI logic tests: connection states, polling, rendering rules, chart transforms, diagram content and copy.
// Run with: node demo/test_ui.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const DEMO = __dirname;
const REPO = path.dirname(DEMO);

function load(files, extra = {}) {
  const context = {console, Date, Math, JSON, Number, Promise, Object, Array, Set, Map, String, Boolean, Error, Symbol, ...extra};
  context.globalThis = context;
  vm.createContext(context);
  for (const file of files) vm.runInContext(fs.readFileSync(path.join(DEMO, file), 'utf8'), context, {filename: file});
  return context;
}

const passed = [];
async function test(name, fn) { await fn(); passed.push(name); }

const NOW = 1_800_000_000;
function stats(status, extra = {}) {
  return {
    schema_version: 1, status, query_at: NOW, values_at: NOW - 1, age_seconds: 1, retained: false,
    last_successful_scrape_at: NOW - 1, component_published_at: NOW - 1,
    markets: {desired: 258591, synced: 258589, ratio: 0.99999},
    books: {desired: 517182, synced: 517178},
    connections: {orderbook: {expected: 2022, connected: 2022}, market_discovery: {expected: 3, connected: 3}},
    workers: 4, resilience: {standby_ready_markets: 258573, publisher_switches_total: 191},
    events_per_second: 32857, book_updates_per_second: 32800,
    internal_latency: {p50_ms: 0.025, p99_ms: 1, p50_above_60_seconds: false, p99_above_60_seconds: false},
    exchange_age: {p50_ms: 10, p99_ms: 50},
    ...extra,
  };
}

/** A scripted browser: fetch answers come from a queue; timers run only when the test advances them. */
function fakeEnv() {
  let now = NOW * 1000;
  let nextId = 1;
  const timers = new Map();
  const listeners = {document: {}, window: {}};
  const env = {
    responses: [], fetches: [], delays: [],
    Date: {now: () => now},
    document: {visibilityState: 'visible', addEventListener(type, fn) { (listeners.document[type] ||= []).push(fn); }},
    navigator: {onLine: true},
    window: {addEventListener(type, fn) { (listeners.window[type] ||= []).push(fn); }},
    AbortSignal: {timeout: () => undefined},
    setTimeout(fn, delay) { const id = nextId++; timers.set(id, {fn, at: now + delay, delay}); env.delays.push(delay); return id; },
    clearTimeout(id) { timers.delete(id); },
    setInterval() { return 0; },
    async fetch(url) {
      env.fetches.push(url);
      const next = env.responses.shift();
      if (!next || next.error) throw new TypeError('Failed to fetch');
      return {ok: next.status === undefined || next.status < 400, status: next.status || 200, json: async () => next.body};
    },
    async advance(ms) {
      now += ms;
      for (const [id, timer] of [...timers]) if (timer.at <= now) { timers.delete(id); timer.fn(); }
      await settle();
    },
    fire(target, type) { for (const fn of listeners[target][type] || []) fn(); },
    pending: () => timers.size,
  };
  return env;
}
const settle = () => new Promise(resolve => setImmediate(resolve));

(async () => {
  const ctx = load(['status.js']);
  const S = ctx.DemoStatus;

  await test('formatting', () => {
    assert.equal(S.duration(0.025), '25 µs');
    assert.equal(S.duration(1.09), '1.09 ms');
    assert.equal(S.duration(61000), '61 s');
    assert.equal(S.duration(null), null);
    assert.equal(S.latency({internal_latency: {p99_above_60_seconds: true, p99_ms: null}}, 'internal_latency', 99), '> 60 s');
    assert.equal(S.ago(42), '42 s ago');
    assert.equal(S.ago(7200), '2 h ago');
  });

  await test('API bodies are validated before they can show as live', () => {
    assert.equal(S.validStats(stats('live')), true);
    for (const bad of [null, {}, {schema_version: 2, status: 'live'}, {schema_version: 1, status: 'unknown'}]) assert.equal(Boolean(S.validStats(bad)), false);
  });

  await test('routine reconnections are silent; unavailable states have notices', () => {
    const cases = {
      live: [stats('live'), /^Live, updated 1 s ago$/, null],
      starting: [stats('starting'), /^Starting$/, /first market scan has not finished/],
      degraded: [stats('degraded', {connections: {orderbook: {expected: 2022, connected: 2019}, market_discovery: {expected: 3, connected: 3}}}), /^Degraded/, null],
      stale: [stats('stale', {retained: true, age_seconds: 60}), /^Stale, last updated 1 min ago$/, /stopped updating 1 min ago/],
      stopped: [stats('stopped', {retained: true, age_seconds: 7200}), /^Not running, last data 2 h ago$/, /not running. Values are from its last run, 2 h ago/],
      metrics_unavailable: [stats('metrics_unavailable', {retained: true}), /^Metrics unavailable$/, /Prometheus is not responding\. Showing values from/],
      api_unavailable: [stats('live'), /^Demo API unavailable$/, /demo API is not responding/],
      offline: [stats('live'), /^Offline$/, /You're offline/],
    };
    const lines = new Set();
    for (const [state, [body, pill, line]] of Object.entries(cases)) {
      const snap = S.snapshot({state, stats: body, receivedAt: NOW * 1000, retryAt: null}, NOW * 1000);
      assert.match(S.pill(snap), pill, state);
      const notice = S.notice(snap);
      if (line === null) assert.equal(notice, null); else { assert.match(notice.text, line, state); lines.add(notice.text); }
    }
    assert.equal(lines.size, 6, 'starting and unavailable states retain distinct notices');
    const never = S.snapshot({state: 'stopped', stats: stats('stopped', {values_at: null, age_seconds: null, markets: {}}), receivedAt: 0}, 0);
    assert.equal(never.retained, false);
    assert.match(S.notice(never).text, /no data from it in the last 7 days/);
    assert.equal(S.pill(never), 'Not running');
  });

  await test('polling: live, API failure with backoff, recovery', async () => {
    const env = fakeEnv();
    const status = S.createStatus(env);
    const seen = [];
    status.subscribe(snap => seen.push(snap));
    env.responses.push({body: stats('live')});
    status.start();
    await settle();
    assert.equal(seen.at(-1).state, 'live');
    assert.equal(env.delays.at(-1), 1000);
    for (let i = 0; i < 4; i += 1) {
      env.responses.push({error: true});
      await env.advance(env.delays.at(-1));
    }
    const snap = seen.at(-1);
    assert.equal(snap.state, 'api_unavailable');
    assert.equal(snap.retained, true, 'last good values stay visible');
    assert.ok(snap.age > 1, 'their age keeps growing');
    const backoff = env.delays.slice(-4);
    assert.ok(backoff.every((delay, i) => i === 0 || delay > backoff[i - 1]), `backoff grows: ${backoff}`);
    assert.ok(backoff.every(delay => delay <= 36000));
    env.responses.push({status: 502, body: {}});
    await env.advance(env.delays.at(-1));
    assert.equal(seen.at(-1).state, 'api_unavailable', 'HTTP errors are API failures');
    env.responses.push({body: {schema_version: 1, status: 'bogus'}});
    await env.advance(env.delays.at(-1));
    assert.equal(seen.at(-1).state, 'api_unavailable', 'unknown bodies never show as live');
    env.responses.push({body: stats('live')});
    await env.advance(env.delays.at(-1));
    assert.equal(seen.at(-1).state, 'live');
    assert.equal(env.delays.at(-1), 1000, 'recovery returns to the one-second interval');
  });

  await test('polling pauses while hidden and resumes at once when visible', async () => {
    const env = fakeEnv();
    const status = S.createStatus(env);
    env.responses.push({body: stats('live')});
    status.start();
    await settle();
    env.document.visibilityState = 'hidden';
    env.fire('document', 'visibilitychange');
    assert.equal(env.pending(), 0, 'no timer while hidden');
    const before = env.fetches.length;
    await env.advance(60000);
    assert.equal(env.fetches.length, before, 'no fetch while hidden');
    env.document.visibilityState = 'visible';
    env.responses.push({body: stats('live')});
    env.fire('document', 'visibilitychange');
    await settle();
    assert.equal(env.fetches.length, before + 1, 'fetches immediately when visible');
  });

  await test('offline is detected without fetching and recovers on the online event', async () => {
    const env = fakeEnv();
    const status = S.createStatus(env);
    const seen = [];
    status.subscribe(snap => seen.push(snap));
    env.responses.push({body: stats('live')});
    status.start();
    await settle();
    env.navigator.onLine = false;
    env.fire('window', 'offline');
    await settle();
    const fetches = env.fetches.length;
    assert.equal(seen.at(-1).state, 'offline');
    assert.equal(seen.at(-1).retained, true);
    await env.advance(5000);
    assert.equal(env.fetches.length, fetches, 'no requests while offline');
    env.navigator.onLine = true;
    env.responses.push({body: stats('live')});
    env.fire('window', 'online');
    await settle();
    assert.equal(seen.at(-1).state, 'live');
  });

  await test('Prometheus outage on a restarted server keeps the browser\'s last values and backs off', async () => {
    const env = fakeEnv();
    const status = S.createStatus(env);
    const seen = [];
    status.subscribe(snap => seen.push(snap));
    env.responses.push({body: stats('live')});
    status.start();
    await settle();
    env.responses.push({body: {schema_version: 1, status: 'metrics_unavailable', retained: false, values_at: null}});
    await env.advance(1000);
    assert.equal(seen.at(-1).state, 'metrics_unavailable');
    assert.equal(seen.at(-1).retained, true);
    assert.equal(seen.at(-1).stats.markets.synced, 258589);
    assert.ok(env.delays.at(-1) > 1000, 'backs off while Prometheus is down');
  });

  await test('stopped polls every five seconds', async () => {
    const env = fakeEnv();
    const status = S.createStatus(env);
    env.responses.push({body: stats('stopped', {retained: true})});
    status.start();
    await settle();
    assert.equal(env.delays.at(-1), 5000);
  });

  // Overview rendering.
  const elements = new Map();
  const element = () => ({innerHTML: '', textContent: '', classList: {values: new Set(), toggle(name, on) { if (on) this.values.add(name); else this.values.delete(name); }, contains(name) { return this.values.has(name); }}});
  const byId = id => { if (!elements.has(id)) elements.set(id, element()); return elements.get(id); };
  const ov = load(['status.js', 'overview.js']);

  await test('overview figures show values and retained state, never zeros for missing data', () => {
    const render = snap => ov.DemoStatus.renderOverview(snap, byId);
    render(ov.DemoStatus.snapshot({state: 'live', stats: stats('live'), receivedAt: 0}, 0));
    assert.match(byId('fig-markets').innerHTML, /258,589 <span class="of">of 258,591<\/span>/);
    assert.equal(byId('fig-markets-note').textContent, 'Two outcome books per market. Polymarket sometimes lists markets as active before their order books are available. This likely explains small gaps between the expected and actual book counts.');
    assert.match(byId('fig-latency').innerHTML, /p50<\/span> [^<]+<\/span> <span class="pair"><span class="of">p99<\/span> 1 ms<\/span>$/);
    assert.equal(byId('fig-standby-note').textContent, '', 'publisher switches stay on the Live page');
    assert.equal(byId('fig-markets-gap').textContent, 'Polymarket never sends an order book for some markets. Hover or tap the title for details.');
    assert.match(byId('fig-exchange').innerHTML, /p50<\/span> 10 ms<\/span> <span class="pair"><span class="of">p99<\/span> 50 ms<\/span>$/);
    render(ov.DemoStatus.snapshot({state: 'live', stats: stats('live', {markets: {desired: 10, synced: 10}}), receivedAt: 0}, 0));
    assert.equal(byId('fig-markets-note').textContent, 'Two outcome books per market.');
    assert.equal(byId('fig-markets-gap').textContent, '', 'no pointer when every market has a book');
    render(ov.DemoStatus.snapshot({state: 'stopped', stats: stats('stopped', {retained: true}), receivedAt: 0}, 0));
    assert.equal(byId('figures').classList.contains('retained'), true);
    render(ov.DemoStatus.snapshot({state: 'stopped', stats: {schema_version: 1, status: 'stopped'}, receivedAt: 0}, 0));
    for (const id of ['fig-markets', 'fig-updates', 'fig-exchange', 'fig-latency', 'fig-standby']) {
      assert.match(byId(id).innerHTML, /No data/, id);
      assert.doesNotMatch(byId(id).innerHTML, /\b0\b|—|-{1,2}$/, id);
    }
    render(ov.DemoStatus.snapshot({state: 'api_unavailable', stats: null, receivedAt: 0}, 0));
    assert.match(byId('fig-markets').innerHTML, /No data yet/);
  });

  // Chart transforms.
  const charts = load(['status.js', 'charts.js']).DemoCharts;
  await test('chart transforms: stacks, gaps and legend values', () => {
    assert.deepEqual([...charts.stack([[1, null, 2], [3, null, 0]]).map(layer => [...layer])], [[1, null, 2], [4, null, 2]]);
    assert.deepEqual([...charts.gaps([[1, null, null, 2, null], [null, null, null, 1, null]]).map(range => [...range])], [[1, 2], [4, 4]]);
    assert.deepEqual([...charts.diff([10, null, 5], [8, 1, 7])], [2, null, 0]);
    const history = {series: {internal_p50: [0.02], internal_p99: [1], writer_blocked: [0, null], writer_busy: [0.75, null]},
      bars: {series: {'session_ends:remote_close': [null, null], 'session_ends:io_error': [2, 3], book_mismatches: [0, 4], older_full_books_skipped: [null, null]}}};
    const defs0 = charts.definitions(history);
    const legend = id => [...charts.legendValues(id, defs0[id], stats('live'))];
    assert.equal(legend('disconnects').length, defs0.disconnects.series.length, 'one value per legend entry');
    assert.deepEqual(legend('disconnects').slice(0, 2), [null, '5'], 'bars total the range; no data in range is not zero');
    assert.deepEqual(legend('checks'), ['4', null, null]);
    assert.deepEqual(legend('gaps'), ['2', '18']);
    assert.deepEqual(legend('writer_busy'), ['75%']);
    assert.deepEqual(legend('writer'), ['0 s/s'], 'a measured zero stays zero');
    assert.deepEqual([...charts.legendValues('internal', defs0.internal, {})], ['20 µs', '1 ms'], 'history fills in when live stats are missing');
    assert.ok(['checks', 'disconnects', 'switches'].every(id => defs0[id].series.every(item => item.tip)), 'technical legend labels explain themselves');
    assert.match(charts.chartNote('connections', history, stats('live')), /^Open: [\d,]+ of [\d,]+ order-book, \d+ of \d+ discovery\.$/);
    assert.equal(charts.chartNote('connections', history, {}), '');
    assert.equal(charts.chartNote('gaps', history, stats('live', {markets: {desired: 11, synced: 10}})), 'Polymarket never sends an order book for some markets. Hover or tap the title for details.');
    assert.equal(charts.chartNote('gaps', history, {}), '');
    const defs = charts.definitions({series: {'worker_busy:1': [0.2], 'worker_busy:0': [0.1]}, bars: {series: {}}});
    assert.deepEqual([...defs.workers.series.map(s => s.label)], ['W0', 'W1']);
    assert.deepEqual([...defs.writer_busy.series[0].values], [], 'older collectors leave writer busy empty');
    assert.deepEqual([...charts.legendValues('writer_busy', defs.writer_busy, stats('live'))], [null]);
    const idle = charts.definitions({series: {writer_busy: [0]}});
    assert.deepEqual([...charts.legendValues('writer_busy', idle.writer_busy, stats('live'))], ['0%'], 'measured idle differs from missing data');
  });

  await test('deployment marker stays unobtrusive and reveals details only near its line', () => {
    const at = Date.UTC(2026, 9, 27, 13, 8) / 1000;
    const history = {start: at - 60, end: at + 60};
    const calls = [];
    const context = new Proxy({}, {get: (_, name) => (...args) => {
      calls.push([name, ...args]);
      if (name === 'measureText') return {width: 100};
    }});
    const plot = {ctx: context, bbox: {left: 100, top: 20, width: 600, height: 360}, valToPos: () => 680};
    charts.drawDeploymentMarker(plot, history, 2);
    assert.ok(calls.some(call => call[0] === 'scale' && call[1] === 2));
    assert.ok(calls.some(call => call[0] === 'stroke'));
    assert.ok(!calls.some(call => ['fillText', 'fillRect'].includes(call[0])), 'no permanent label covers the data');
    assert.equal(charts.chartNote('updates', history, {}), '', 'deployment details do not occupy space below the chart');
    plot.valToPos = (_value, _axis, canvas) => canvas ? 680 : 290;
    plot.cursor = {left: 294, top: 100};
    assert.ok(charts.deploymentHovered(plot, history, 2));
    plot.cursor.left = 300;
    assert.ok(!charts.deploymentHovered(plot, history, 2));
    plot.cursor = {left: 290, top: -1};
    assert.ok(!charts.deploymentHovered(plot, history, 2), 'leaving the plot hides the tooltip');
    plot.cursor.top = 181;
    assert.ok(!charts.deploymentHovered(plot, history, 2), 'the tooltip stays within the plot height');
    calls.length = 0;
    charts.drawDeploymentMarker(plot, {start: at + 1, end: at + 900}, 2);
    assert.equal(calls.length, 0, 'no marker on unrelated history');
    assert.equal(charts.chartNote('updates', {start: at + 1, end: at + 900}, {}), '');
    assert.equal(charts.chartNote('updates', null, {}), '');
  });

  // Diagram content.
  const diagram = load(['diagram.js']).DemoDiagram;
  await test('architecture content: seven steps, real files, routing example', () => {
    assert.equal(diagram.STEPS.length, 7);
    for (const item of [...Object.values(diagram.COMPONENTS), ...diagram.STEPS]) {
      for (const file of item.code) assert.ok(fs.existsSync(path.join(REPO, file)), `missing ${file}`);
    }
    const condition = '0x233e4370d16889f614f62a3280887b31e0c381ec3a3e0bb362aefdf0703f6406';
    assert.equal(BigInt(`0x${condition.slice(-16)}`) % 4n, 2n);
    assert.match(diagram.componentHtml('route').body, /6 mod 4 = 2/);
  });

  await test('pages: copy rules, privacy, no inline scripts or styles', () => {
    const pages = ['index.html', 'live.html'];
    const visible = file => fs.readFileSync(path.join(DEMO, file), 'utf8').replace(/<script[\s\S]*?<\/script>/g, '').replace(/<[^>]+>/g, ' ');
    for (const file of [...pages, 'status.js', 'overview.js', 'charts.js', 'chart-help.js', 'diagram.js', 'site.css']) {
      // SVG path data is coordinates, not addresses.
      const text = fs.readFileSync(path.join(DEMO, file), 'utf8').replace(/ d="[^"]*"/g, '');
      assert.doesNotMatch(text, /\/home\/|\/Users\/|\b(?:\d{1,3}\.){3}\d{1,3}\b|localhost|admin@/, `${file}: no paths, hosts or IPs`);
      assert.doesNotMatch(text, /#(?:ff69b4|ffc0cb|e91e63|ff1493|db7093)/i, `${file}: no pink`);
    }
    for (const file of pages) {
      const raw = fs.readFileSync(path.join(DEMO, file), 'utf8');
      assert.doesNotMatch(raw, /<script>|<script(?![^>]*src=)[^>]*>/, `${file}: scripts are external`);
      assert.doesNotMatch(raw, /\sstyle="/, `${file}: no inline styles under the CSP`);
      const text = visible(file);
      assert.doesNotMatch(text, / · | — |→|»/, `${file}: no dot joins, dash fragments or arrows`);
      assert.match(raw, /<nav class="site-nav"[\s\S]*Overview[\s\S]*Live/);
      assert.doesNotMatch(raw, /\/tradeoffs/, `${file}: Trade-offs is removed until its sources are rewritten`);
      const footer = raw.match(/<footer[\s\S]*?<\/footer>/)[0];
      for (const href of ['https://github.com/itzlambda/voluntas-challenge', 'https://x.com/itzlambda']) {
        assert.ok(footer.includes(`href="${href}" target="_blank" rel="noopener noreferrer" aria-label="`), `${file}: ${href} opens in a new tab with a name`);
      }
      assert.equal((footer.match(/<a /g) || []).length, 2, `${file}: footer has only the source and Twitter icons`);
      if (file === 'live.html') {
        assert.deepEqual([...raw.matchAll(/data-range="([^"]+)"/g)].map(match => match[1]),
          ['15m', '1h', '12h', '24h', 'all']);
        const sections = raw.match(/<section class="panel chart[\s\S]*?<\/section>/g);
        assert.equal(sections.length, 11);
        assert.doesNotMatch(raw, /How to read these numbers/, 'definitions live at chart titles');
        assert.match(raw, /src="\/chart-help.js"/, 'tooltip interactions are loaded');
        for (const section of sections) {
          assert.doesNotMatch(section, /data-values/, 'headers carry only the title');
          const helpId = section.match(/aria-describedby="([^"]+)"/)[1];
          assert.ok(section.includes(`id="${helpId}" role="tooltip" popover="auto"`), 'each title describes its tooltip');
          assert.match(section, /data-plot[^\n]*<\/div>\n\s*<div class="legend" data-legend><\/div>/, 'legend sits below the plot');
        }
      }
    }
  });

  console.log(`Demo UI tests passed (${passed.length}): ${passed.join('; ')}.`);
})().catch(error => { console.error(error); process.exit(1); });
