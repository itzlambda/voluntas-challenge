// Deployment banner and restart marker tests: metadata parsing, polling, banner state, chart markers and page markup.
// Run with: node demo/test_deployment.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const DEMO = __dirname;

function load(files) {
  const context = {console, Date, Math, JSON, Number, Promise, Object, Array, Set, Map, String, Boolean, Error, Symbol, URL};
  context.globalThis = context;
  vm.createContext(context);
  for (const file of files) vm.runInContext(fs.readFileSync(path.join(DEMO, file), 'utf8'), context, {filename: file});
  return context;
}

const passed = [];
async function test(name, fn) { await fn(); passed.push(name); }

const NOW = 1_800_000_000;
const STAGE = Date.UTC(2026, 9, 29, 10, 0, 5) / 1000;
const STREAM = Date.UTC(2026, 9, 29, 11, 30, 17) / 1000;
const body = (extra = {}) => ({schema_version: 1, in_progress: false, message: 'A new deployment is in progress.', started_at: null, markers: [], ...extra});

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
    setTimeout(fn, delay) { const id = nextId++; timers.set(id, {fn, at: now + delay}); env.delays.push(delay); return id; },
    clearTimeout(id) { timers.delete(id); },
    async fetch(url) {
      env.fetches.push(url);
      const next = env.responses.shift();
      if (!next || next.error) throw new TypeError('Failed to fetch');
      return {ok: !next.status || next.status < 400, status: next.status || 200, json: async () => next.body};
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

function fakeBanner() {
  const text = {textContent: ''};
  const link = {hidden: true, textContent: '', attributes: {}, set href(value) { this.attributes.href = value; },
    removeAttribute(name) { delete this.attributes[name]; }};
  const element = {hidden: true, querySelector: selector => selector === '.deployment-banner-link' ? link : text, text, link};
  return {element, doc: {getElementById: id => id === 'deployment-banner' ? element : null}};
}

(async () => {
  const ctx = load(['status.js', 'deployment.js']);
  const D = ctx.DemoDeployment;
  const S = ctx.DemoStatus;

  await test('metadata parsing rejects invalid bodies instead of guessing a state', () => {
    for (const bad of [null, [], 'x', {}, body({schema_version: 2}), body({in_progress: 'true'}), body({in_progress: 1}),
      body({markers: {}}), body({markers: 'none'})]) {
      assert.equal(D.parse(bad), null, JSON.stringify(bad));
    }
    const ok = D.parse(body({in_progress: true, started_at: STAGE}));
    assert.equal(ok.inProgress, true);
    assert.equal(ok.startedAt, STAGE);
    assert.equal(D.parse(body({in_progress: true, message: '   '})).message, D.DEFAULT_MESSAGE, 'blank message falls back to the neutral default');
    assert.equal(D.parse(body({started_at: 'soon'})).startedAt, null);
    assert.equal(D.parse(body({started_at: STAGE * 1000})).startedAt, null, 'milliseconds are not seconds');
    assert.equal(D.parse(body({message: 'x'.repeat(1000)})).message.length, 300);
    const {markers} = D.parse({schema_version: 1, in_progress: false});
    assert.deepEqual([...markers], [], 'markers are optional');
  });

  await test('markers: valid, sorted, deduplicated, malformed entries dropped', () => {
    const parsed = D.parse(body({markers: [
      {at: STREAM, label: 'Stream activation', details: 'Collector restarted with stream output.'},
      {at: STAGE, label: 'File telemetry stage', details: 'Collector restarted with new telemetry.'},
      {at: STAGE, label: 'Duplicate', details: 'same instant'},
      {at: 0, label: 'Zero'}, {at: 'x', label: 'Text'}, {at: STAGE + 5}, {at: STAGE + 6, label: ''}, null, 7,
      {at: STAGE + 9, label: 'No details'},
    ]})).markers;
    assert.deepEqual([...parsed.map(m => m.label)], ['File telemetry stage', 'No details', 'Stream activation']);
    assert.equal(parsed[1].details, '');
    assert.equal(parsed[1].url, null, 'links are optional');
    const linked = D.parse(body({markers: [{at: STAGE, label: 'Exchange outage', url: 'https://status.example.com/'},
      {at: STREAM, label: 'Bad link', url: 'javascript:alert(1)'}]})).markers;
    assert.equal(linked[0].url, 'https://status.example.com/');
    assert.equal(linked[1].url, null, 'a bad link drops the link, not the marker');
  });

  await test('polling: immediate fetch, normal interval, failures keep the banner and back off', async () => {
    const env = fakeEnv();
    const deployment = D.createDeployment(env, S);
    const {element, doc} = fakeBanner();
    D.bindBanner(deployment, doc, null);
    const states = [];
    deployment.subscribe(state => states.push(state));
    env.responses.push({body: body({in_progress: true})});
    deployment.start();
    await settle();
    assert.deepEqual(env.fetches, ['/deployment.json']);
    assert.equal(element.hidden, false);
    assert.equal(element.text.textContent, 'A new deployment is in progress.');
    assert.equal(env.delays.at(-1), D.INTERVAL);
    assert.ok(D.INTERVAL >= 5000 && D.INTERVAL <= 10000);

    // Unchanged metadata does not notify again.
    env.responses.push({body: body({in_progress: true})});
    await env.advance(D.INTERVAL);
    assert.equal(states.length, 1);

    // Network error, server error, invalid body, missing file: banner stays, delays grow.
    env.responses.push({error: true}, {status: 502}, {body: {schema_version: 1, in_progress: 'maybe'}}, {status: 404});
    for (let i = 0; i < 4; i += 1) await env.advance(60000);
    assert.equal(deployment.failures, 4);
    assert.equal(element.hidden, false, 'banner stays visible through transient failures');
    assert.ok(env.delays.at(-1) > D.INTERVAL);

    env.responses.push({body: body({in_progress: false, markers: [{at: STAGE, label: 'Stage', details: 'Restarted.'}]})});
    await env.advance(60000);
    assert.equal(deployment.failures, 0);
    assert.equal(element.hidden, true);
    assert.equal(element.text.textContent, '');
    assert.equal(states.length, 2);
    assert.equal(states[1].markers.length, 1);
    assert.equal(env.delays.at(-1), D.INTERVAL);
  });

  await test('banner starts hidden and never appears from failures alone', async () => {
    const env = fakeEnv();
    const deployment = D.createDeployment(env, S);
    const {element, doc} = fakeBanner();
    D.bindBanner(deployment, doc, null);
    env.responses.push({status: 404});
    deployment.start();
    await settle();
    assert.equal(element.hidden, true, 'an older server without the file shows no banner');
    assert.equal(deployment.state, null);
  });

  await test('polling pauses while hidden and resumes at once when visible', async () => {
    const env = fakeEnv();
    const deployment = D.createDeployment(env, S);
    env.responses.push({body: body()});
    deployment.start();
    await settle();
    env.document.visibilityState = 'hidden';
    env.fire('document', 'visibilitychange');
    assert.equal(env.pending(), 0);
    await env.advance(120000);
    assert.equal(env.fetches.length, 1);
    env.document.visibilityState = 'visible';
    env.responses.push({body: body()});
    env.fire('document', 'visibilitychange');
    await settle();
    assert.equal(env.fetches.length, 2);
  });

  await test('banner text uses textContent and adds the start time', () => {
    const {element, doc} = fakeBanner();
    const listeners = [];
    const fake = {subscribe: fn => listeners.push(fn)};
    D.bindBanner(fake, doc, at => `T${at}`);
    listeners[0](D.parse(body({in_progress: true, message: '<img src=x onerror=alert(1)>', started_at: STAGE})));
    assert.equal(element.text.textContent, `<img src=x onerror=alert(1)> Started T${STAGE}.`);
    assert.equal(element.innerHTML, undefined, 'never written as markup');
  });

  await test('banner link: https only, rendered as text with an href, hidden without a banner', () => {
    const ok = D.parse(body({link: {url: 'https://status.example.com/incidents', label: ' Status '}})).link;
    assert.deepEqual({...ok}, {url: 'https://status.example.com/incidents', label: 'Status'});
    for (const bad of [{url: 'javascript:alert(1)', label: 'x'}, {url: 'http://status.example.com/', label: 'x'},
      {url: '/relative', label: 'x'}, {url: 'https://status.example.com/', label: ' '}, {url: 7, label: 'x'}, 'https://x.example/']) {
      assert.equal(D.parse(body({link: bad})).link, null, JSON.stringify(bad));
    }
    assert.equal(D.parse(body()).link, null, 'links are optional');
    const {element, doc} = fakeBanner();
    const listeners = [];
    D.bindBanner({subscribe: fn => listeners.push(fn)}, doc, () => null);
    listeners[0](D.parse(body({in_progress: true, message: 'Upstream incident.', link: {url: 'https://status.example.com/', label: '<b>Status</b>'}})));
    assert.equal(element.link.hidden, false);
    assert.equal(element.link.textContent, '<b>Status</b>', 'label is text, never markup');
    assert.equal(element.link.attributes.href, 'https://status.example.com/');
    listeners[0](D.parse(body({in_progress: false, link: {url: 'https://status.example.com/', label: 'Status'}})));
    assert.equal(element.link.hidden, true, 'no link without the banner');
    assert.equal(element.link.attributes.href, undefined);
  });

  const charts = load(['status.js', 'deployment.js', 'charts.js']).DemoCharts;
  const historical = Date.UTC(2026, 9, 27, 13, 8) / 1000;

  await test('chart markers: historical marker kept, metadata markers added, duplicates dropped', () => {
    const list = charts.deploymentMarkers([{at: STREAM, label: 'Stream', details: 'b'}, {at: STAGE, label: 'Stage', details: 'a'},
      {at: STAGE, label: 'Dup', details: ''}, {at: historical, label: 'Same as historical', details: ''}]);
    assert.deepEqual([...list.map(m => m.label)], ['New deployment', 'Stage', 'Stream']);
    assert.equal(list[0].approx, true);
    assert.equal(list[1].approx, false);
    assert.deepEqual([...charts.deploymentMarkers().map(m => m.at)], [historical]);
  });

  await test('chart markers: each line drawn in range, hover picks the nearest and explains it', () => {
    const markers = charts.deploymentMarkers([{at: STAGE, label: 'File telemetry stage', details: 'Collector restarted.'},
      {at: STREAM, label: 'Stream activation', details: ''}]);
    const history = {start: STAGE - 60, end: STREAM + 60};
    const calls = [];
    const context = new Proxy({}, {get: (_, name) => (...args) => { calls.push([name, ...args]); }});
    const pos = at => at === STAGE ? 200 : at === STREAM ? 500 : -1000;
    const plot = {ctx: context, bbox: {left: 100, top: 20, width: 600, height: 360}, valToPos: (at, _axis, canvas) => canvas ? pos(at) * 2 : pos(at)};
    charts.drawDeploymentMarker(plot, history, 2, markers);
    assert.equal(calls.filter(call => call[0] === 'stroke').length, 2, 'historical marker outside the range is not drawn');
    assert.ok(!calls.some(call => ['fillText', 'fillRect'].includes(call[0])), 'no permanent labels');
    plot.cursor = {left: 203, top: 50};
    assert.equal(charts.deploymentHovered(plot, history, 2, markers).label, 'File telemetry stage');
    plot.cursor.left = 497;
    assert.equal(charts.deploymentHovered(plot, history, 2, markers).label, 'Stream activation');
    plot.cursor.left = 350;
    assert.equal(charts.deploymentHovered(plot, history, 2, markers), null);
    const text = charts.deploymentText(markers[1]);
    assert.match(text, /^File telemetry stage · .+: Collector restarted\.$/);
    assert.doesNotMatch(text, /approx/, 'metadata markers are exact');
    assert.match(charts.deploymentText(markers[0]), /\(approx\.\): service restarted/);
    assert.doesNotMatch(charts.deploymentText(markers[2]), /: $/, 'no dangling colon without details');
    assert.doesNotMatch(text, /Click/, 'no click hint without a link');
    const [, linked] = charts.deploymentMarkers([{at: STAGE, label: 'Exchange outage', details: 'Trading stopped.', url: 'https://status.example.com/'}]);
    assert.equal(linked.url, 'https://status.example.com/');
    assert.match(charts.deploymentText(linked), /Trading stopped\. Click for details\.$/);
  });

  await test('rollout windows: valid, sorted, non-overlapping, malformed entries dropped', () => {
    assert.equal(D.parse(body({windows: {}})), null, 'windows must be an array');
    assert.deepEqual([...D.parse(body()).windows], [], 'windows are optional');
    const parsed = D.parse(body({windows: [
      {start: STREAM, end: STREAM + 600, label: '  Second  ', details: 'b'},
      {start: STAGE, end: STREAM, label: 'First', details: ' a '},
      {start: STAGE + 60, end: STAGE + 120, label: 'Overlaps first', details: ''},
      {start: STAGE, end: STAGE, label: 'Empty'}, {start: STAGE, end: STAGE - 1, label: 'Reversed'},
      {start: STAGE * 1000, end: STAGE * 1000 + 1, label: 'Milliseconds'}, {start: STAGE, end: STREAM}, null,
    ]})).windows;
    assert.deepEqual(JSON.parse(JSON.stringify(parsed)), [
      {start: STAGE, end: STREAM, label: 'First', details: 'a'}, {start: STREAM, end: STREAM + 600, label: 'Second', details: 'b'}]);
  });

  await test('rollout windows: shaded with edge lines in range, hover anywhere inside explains it', () => {
    const windows = [{start: STAGE, end: STREAM, label: 'Switched to stream output', details: 'Ignore the band itself.'}];
    const calls = [];
    const context = new Proxy({}, {get: (_, name) => (...args) => { calls.push([name, ...args]); }});
    const pos = at => at === STAGE ? 200 : at === STREAM ? 500 : at < STAGE ? 50 : 650;
    const plot = {ctx: context, bbox: {left: 100, top: 20, width: 1200, height: 360}, valToPos: (at, _axis, canvas) => canvas ? pos(at) * 2 : pos(at)};
    charts.drawRolloutWindows(plot, {start: STAGE - 60, end: STREAM + 60}, 2, windows);
    assert.deepEqual(calls.filter(call => call[0] === 'fillRect').map(call => call.slice(1)), [[200, 10, 300, 180]]);
    assert.equal(calls.filter(call => call[0] === 'stroke').length, 2, 'start and end lines');
    assert.ok(!calls.some(call => call[0] === 'fillText'), 'no permanent label covers the data');
    calls.length = 0;
    charts.drawRolloutWindows(plot, {start: STAGE + 60, end: STREAM + 60}, 2, windows);
    assert.equal(calls.filter(call => call[0] === 'stroke').length, 1, 'only the edge inside the range is drawn');
    assert.equal(calls.filter(call => call[0] === 'fillRect').length, 1, 'the window is still shaded');
    calls.length = 0;
    charts.drawRolloutWindows(plot, {start: STREAM + 1, end: STREAM + 900}, 2, windows);
    assert.equal(calls.length, 0, 'nothing drawn for unrelated history');
    const history = {start: STAGE - 60, end: STREAM + 60};
    plot.cursor = {left: 350, top: 50};
    assert.equal(charts.rolloutHovered(plot, history, 2, windows).label, windows[0].label);
    plot.cursor.left = 520;
    assert.equal(charts.rolloutHovered(plot, history, 2, windows), null);
    plot.cursor = {left: 350, top: -1};
    assert.equal(charts.rolloutHovered(plot, history, 2, windows), null, 'leaving the plot hides the tooltip');
    assert.equal(charts.rolloutText(windows[0]), 'Switched to stream output. Ignore the band itself.', 'no timestamps: the band shows when');
    assert.equal(charts.rolloutText({...windows[0], details: ''}), 'Switched to stream output');
  });

  await test('deployed metadata: one rollout window replaces the individual restart markers', () => {
    const state = D.parse(JSON.parse(fs.readFileSync(path.join(DEMO, 'deployment.json'), 'utf8')));
    assert.ok(state);
    assert.deepEqual([...state.markers.map(marker => marker.label)], ['Polymarket outage']);
    assert.equal(state.markers[0].at, Date.UTC(2026, 9, 29, 17, 56, 50) / 1000);
    assert.equal(state.markers[0].url, 'https://status.polymarket.com/');
    assert.equal(state.windows.length, 1);
    assert.equal(state.windows[0].start, Date.UTC(2026, 9, 29, 12, 40, 37) / 1000);
    assert.equal(state.windows[0].end, Date.UTC(2026, 9, 29, 14, 8, 48) / 1000);
    assert.equal(state.windows[0].details, JSON.parse(fs.readFileSync(path.join(DEMO, 'deployment.json'), 'utf8')).windows[0].details, 'details are not truncated');
  });

  await test('pages: hidden status banner first, metadata script before charts, no inline markup', () => {
    for (const file of ['index.html', 'live.html']) {
      const raw = fs.readFileSync(path.join(DEMO, file), 'utf8');
      const banner = raw.match(/<div class="deployment-banner"[^>]*>/);
      assert.ok(banner, `${file}: banner present`);
      assert.match(banner[0], /id="deployment-banner"/);
      assert.match(banner[0], /role="status"/);
      assert.match(banner[0], /\shidden>/, `${file}: hidden by default`);
      assert.ok(raw.indexOf('deployment-banner') < raw.indexOf('<header'), `${file}: banner above the header`);
      const scripts = [...raw.matchAll(/<script src="([^"]+)"/g)].map(match => match[1]);
      assert.ok(scripts.indexOf('/deployment.js') > scripts.indexOf('/status.js'), `${file}: loads after status.js`);
      if (scripts.includes('/charts.js')) assert.ok(scripts.indexOf('/deployment.js') < scripts.indexOf('/charts.js'));
      assert.match(raw, /<script src="\/deployment.js" defer><\/script>/);
    }
    const source = fs.readFileSync(path.join(DEMO, 'deployment.js'), 'utf8');
    assert.doesNotMatch(source, /innerHTML|insertAdjacentHTML|eval\(|https?:\/\//, 'same-origin, text only');
    assert.doesNotMatch(source, /\/home\/|\/Users\/|\b(?:\d{1,3}\.){3}\d{1,3}\b|localhost|@/, 'no paths, hosts or addresses');
    const charts = fs.readFileSync(path.join(DEMO, 'charts.js'), 'utf8');
    assert.doesNotMatch(charts, /markerTip\.innerHTML/);
  });

  console.log(`Deployment tests passed (${passed.length}): ${passed.join('; ')}.`);
})().catch(error => { console.error(error); process.exit(1); });
