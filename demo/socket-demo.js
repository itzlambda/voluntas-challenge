'use strict';
// Synthetic socket telemetry for /live?socket-demo=1. Loaded only with that flag and answered in the
// browser, so it never reaches the demo API. Values come from a fixed seed and are not measurements.
(function (root) {
  const SOCKETS = 2000;
  const SEED = 0x5eed;
  const SAMPLE_SECONDS = 15;
  const WINDOWS = {'15m': 900, '1h': 3600, '6h': 21600, '24h': 86400};

  // mulberry32: a small deterministic generator.
  function random(seed) {
    let a = seed >>> 0;
    return () => {
      a = (a + 0x6d2b79f5) >>> 0;
      let t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }
  const mix = (...parts) => parts.reduce((h, part) => Math.imul(h ^ (part >>> 0), 0x9e3779b1) >>> 0, SEED);
  const noise = (...parts) => random(mix(...parts))() * 2 - 1;
  function gaussian(next) {
    const u = Math.max(next(), 1e-12);
    return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * next());
  }
  const round2 = value => Math.round(value * 100) / 100;

  /** Fixed per-socket profiles: most rates cluster near 14/s, with outliers, zeros and every non-rated state. */
  function profiles() {
    const next = random(SEED);
    const order = [...Array(SOCKETS).keys()];
    for (let i = order.length - 1; i > 0; i -= 1) {
      const j = Math.floor(next() * (i + 1));
      [order[i], order[j]] = [order[j], order[i]];
    }
    const kinds = new Map();
    const take = (kind, count) => { for (let i = 0; i < count; i += 1) kinds.set(order[kinds.size], kind); };
    take('outlier', 7);
    take('zero', 40);
    take('quiet', 30);
    take('empty', 25);
    take('disconnected', 12);
    take('warming_up', 8);
    take('no_rate', 3);
    return [...Array(SOCKETS).keys()].map(id => {
      const kind = kinds.get(id) || 'normal';
      const base = kind === 'outlier' ? 250 + next() * 650
        : kind === 'quiet' ? 0.2 + next() * 2
          : kind === 'zero' ? 0 : Math.exp(Math.log(14) + 0.22 * gaussian(next));
      return {
        id, pool: id % 2 ? 'replica' : 'primary', kind, base,
        markets: kind === 'empty' ? 0 : id >= SOCKETS - 30 ? 40 + Math.floor(next() * 200) : 256,
      };
    });
  }

  function create(options) {
    const now = options.now;
    const all = profiles();

    // Rate at time t, shared by the fleet and history so the current value matches the latest point.
    function rateAt(profile, t) {
      return round2(Math.max(0, profile.base * (1 + 0.18 * Math.sin(t / 1700 + profile.id) + 0.06 * noise(profile.id, Math.floor(t / SAMPLE_SECONDS)))));
    }

    function socket(profile, at) {
      const state = {empty: 'empty', disconnected: 'disconnected', warming_up: 'warming_up'}[profile.kind] || 'connected';
      const rated = state === 'connected' && profile.kind !== 'no_rate';
      return {
        id: profile.id, pool: profile.pool, connected: state === 'connected' || state === 'warming_up',
        markets: profile.markets, state,
        events_per_second: rated ? rateAt(profile, at) : null,
      };
    }

    function fleet() {
      const seconds = now() / 1000;
      const tick = Math.floor(seconds / SAMPLE_SECONDS);
      const valuesAt = tick * SAMPLE_SECONDS;
      return {
        schema_version: 1, status: 'live', retained: false, query_at: Math.floor(seconds), values_at: valuesAt,
        age_seconds: Math.max(0, seconds - valuesAt), rate_window_seconds: 60, scrape_interval_seconds: SAMPLE_SECONDS,
        sockets: all.map(profile => socket(profile, valuesAt)),
      };
    }

    /** One socket's history with a gap, a restart that changes its level, and nulls during rate warmup. */
    function history(id, window) {
      const span = WINDOWS[window];
      const step = Math.max(SAMPLE_SECONDS, Math.ceil(span / 360 / SAMPLE_SECONDS) * SAMPLE_SECONDS);
      const end = Math.floor(now() / 1000 / step) * step;
      const start = end - span;
      const profile = all[id];
      // Events sit at fixed wall-clock offsets: the restart 30 to 40 minutes ago, so the default 1h window
      // shows it, a short gap after it, and a longer gap only the 6h and 24h windows reach.
      const restartAt = Math.floor(now() / 1000 / 600) * 600 - 1800;
      const gaps = [[restartAt + 600, restartAt + 780], [restartAt - 9000, restartAt - 8400]];
      const timestamps = [];
      const values = [];
      for (let t = start + step; t <= end; t += step) {
        timestamps.push(t);
        let value = null;
        const afterRestart = t >= restartAt;
        const warming = afterRestart && t < restartAt + 75;
        const inGap = gaps.some(([from, to]) => t >= from && t < to);
        const disconnectedTail = profile && profile.kind === 'disconnected' && t > end - 600;
        const rated = profile && !['empty', 'warming_up', 'no_rate'].includes(profile.kind) && !inGap && !warming && !disconnectedTail;
        if (rated) {
          // Before the restart this ID carried a busier market set.
          value = afterRestart ? rateAt(profile, t) : round2(rateAt(profile, t) * 1.4);
        }
        values.push(value);
      }
      return {
        schema_version: 1, status: 'ok', socket_id: id, pool: profile ? profile.pool : 'primary', window,
        rate_window_seconds: 60, start, end, step_seconds: step, timestamps, events_per_second: values,
        restarts: restartAt > start && restartAt <= end ? [restartAt] : [],
      };
    }

    function respond(status, body) {
      return Promise.resolve({ok: status < 400, status, json: async () => JSON.parse(JSON.stringify(body))});
    }

    function fetch(url) {
      const parsed = new URL(url, 'http://demo.invalid');
      if (parsed.pathname === '/api/demo/sockets') return respond(200, fleet());
      if (parsed.pathname === '/api/demo/socket-history') {
        const raw = parsed.searchParams.get('socket');
        const window = parsed.searchParams.get('window');
        const id = /^\d{1,5}$/.test(raw || '') ? Number(raw) : NaN;
        if (!(id <= 65535) || !Object.prototype.hasOwnProperty.call(WINDOWS, window)) return respond(400, {schema_version: 1, status: 'unavailable'});
        return respond(200, history(id, window));
      }
      return respond(404, {});
    }

    return {fetch, fleet, history};
  }

  root.DemoSocketFixture = {create, SOCKETS};
})(typeof window !== 'undefined' ? window : globalThis);
