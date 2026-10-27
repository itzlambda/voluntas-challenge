'use strict';
// Shared connection state, polling and formatting. Pages subscribe with DemoStatus.subscribe.
(function (root) {
  const SERVER_STATES = ['starting', 'live', 'degraded', 'stale', 'stopped', 'metrics_unavailable'];
  const RETAINED_STATES = new Set(['stale', 'stopped', 'metrics_unavailable', 'api_unavailable', 'offline']);

  const isNumber = value => typeof value === 'number' && Number.isFinite(value);
  const count = value => Math.round(value).toLocaleString('en-US');
  const sig = (value, digits = 3) => Number(value.toPrecision(digits)).toLocaleString('en-US', {maximumFractionDigits: 6});

  function duration(ms) {
    if (!isNumber(ms)) return null;
    if (ms < 1) return `${sig(ms * 1000)} µs`;
    if (ms < 1000) return `${sig(ms)} ms`;
    return `${sig(ms / 1000)} s`;
  }

  function latency(stats, key, quantile) {
    const value = stats && stats[key];
    if (!value) return null;
    if (value[`p${quantile}_above_60_seconds`]) return '> 60 s';
    return duration(value[`p${quantile}_ms`]);
  }

  function ago(seconds) {
    if (!isNumber(seconds)) return null;
    const s = Math.max(0, Math.round(seconds));
    if (s < 60) return `${s} s ago`;
    if (s < 3600) return `${Math.round(s / 60)} min ago`;
    if (s < 86400) return `${Math.round(s / 3600)} h ago`;
    return `${Math.round(s / 86400)} d ago`;
  }

  function clock(epochSeconds) {
    if (!isNumber(epochSeconds)) return null;
    const date = new Date(epochSeconds * 1000);
    const sameDay = new Date().toDateString() === date.toDateString();
    const time = date.toLocaleTimeString(undefined, {hour: '2-digit', minute: '2-digit'});
    return sameDay ? time : `${date.toLocaleDateString(undefined, {month: 'short', day: 'numeric'})} ${time}`;
  }

  /** Validates an API body; anything unexpected is treated as an API failure, never as live data. */
  function validStats(body) {
    return body && typeof body === 'object' && body.schema_version === 1 && SERVER_STATES.includes(body.status);
  }

  function hasValues(stats) {
    return Boolean(stats && isNumber(stats.values_at));
  }

  /**
   * Combines the latest server response (or failure) with the last good values.
   * snapshot: {state, stats, age, retained, retryIn}
   */
  function snapshot(view, now) {
    const {state, stats, receivedAt, retryAt} = view;
    const elapsed = receivedAt ? (now - receivedAt) / 1000 : 0;
    const age = stats && isNumber(stats.age_seconds) ? stats.age_seconds + elapsed : null;
    const retained = RETAINED_STATES.has(state) && hasValues(stats);
    return {
      state, stats, age, retained,
      retryIn: retryAt ? Math.max(0, Math.ceil((retryAt - now) / 1000)) : null,
    };
  }

  /** Always-visible pointer to the title tooltip that explains markets without a book. */
  function booklessNote(stats) {
    const markets = (stats || {}).markets || {};
    if (!isNumber(markets.desired) || !isNumber(markets.synced) || markets.desired <= markets.synced) return '';
    return 'Polymarket never sends an order book for some markets. Hover or tap the title for details.';
  }

  function sockets(stats, kind) {
    const value = stats && stats.connections && stats.connections[kind];
    return value && isNumber(value.expected) && isNumber(value.connected) ? value : null;
  }

  /** The header label: a state and, when values exist, how old they are. */
  function pill(snap) {
    const {state, age} = snap;
    const updated = ago(age);
    switch (state) {
      case 'live': return updated ? `Live, updated ${updated}` : 'Live';
      case 'degraded': return updated ? `Degraded, updated ${updated}` : 'Degraded';
      case 'starting': return 'Starting';
      case 'stale': return updated ? `Stale, last updated ${updated}` : 'Stale';
      case 'stopped': return snap.retained && updated ? `Not running, last data ${updated}` : 'Not running';
      case 'metrics_unavailable': return 'Metrics unavailable';
      case 'api_unavailable': return 'Demo API unavailable';
      case 'offline': return 'Offline';
      default: return 'Connecting';
    }
  }

  function retainedSuffix(snap) {
    const at = snap.stats && clock(snap.stats.values_at);
    return snap.retained && at ? ` Showing values from ${at}.` : '';
  }

  /** Notices for startup and unavailable data; routine reconnections stay in the metrics. */
  function notice(snap) {
    const {state, stats} = snap;
    const retry = isNumber(snap.retryIn) && snap.retryIn > 0 ? ` Retrying in ${snap.retryIn} s.` : ' Retrying.';
    switch (state) {
      case 'live':
      case 'degraded':
      case null:
      case undefined:
        return null;
      case 'starting':
        // Only what the metrics report: the collector is up but has no successful discovery scan or publication yet.
        return {text: stats && isNumber(stats.component_published_at)
          ? 'Starting. The first market scan has not finished yet.'
          : "Starting. Waiting for the collector's first metrics."};
      case 'stale':
        return {text: `Metrics stopped updating ${ago(snap.age) || ''}. Values are retained and update when publication resumes.`.replace('  ', ' ')};
      case 'stopped':
        return snap.retained
          ? {text: `The collector is not running. Values are from its last run, ${ago(snap.age)}. Live values return when it restarts.`}
          : {text: 'The collector is not running, and Prometheus has no data from it in the last 7 days. Live values appear when it starts.'};
      case 'metrics_unavailable':
        return {text: `Prometheus is not responding.${retainedSuffix(snap)}${retry}`};
      case 'api_unavailable':
        return {text: `The demo API is not responding.${retainedSuffix(snap)}${retry}`};
      case 'offline':
        return {text: `You're offline.${retainedSuffix(snap)} Updates resume when the connection returns.`};
      default:
        return null;
    }
  }

  /** Exponential backoff with jitter: about 2, 4, 8, 16, then 30 s. */
  function backoff(failures, random = Math.random) {
    const base = Math.min(30000, 1000 * 2 ** failures);
    return Math.round(base * (0.8 + 0.4 * random()));
  }

  /**
   * Polls a JSON URL while the page is visible and the browser is online.
   * env supplies fetch, timers, document, navigator and window so tests can drive it.
   */
  function createPoller(options) {
    const env = options.env || root;
    const interval = options.interval || 1000;
    let timer = null;
    let failures = 0;
    let running = false;
    let inFlight = false;
    let stopped = false;

    const visible = () => !env.document || env.document.visibilityState !== 'hidden';
    const online = () => !env.navigator || env.navigator.onLine !== false;

    function schedule(delay) {
      env.clearTimeout(timer);
      timer = null;
      if (stopped || !visible()) return;
      timer = env.setTimeout(poll, delay);
      if (options.onSchedule) options.onSchedule(env.Date.now() + delay);
    }

    async function poll() {
      timer = null;
      if (stopped || inFlight || !visible()) return;
      if (!online()) {
        options.onResult({ok: false, offline: true});
        schedule(5000);
        return;
      }
      inFlight = true;
      let next = false;
      try {
        const response = await env.fetch(options.url(), {cache: 'no-store', signal: env.AbortSignal && env.AbortSignal.timeout ? env.AbortSignal.timeout(8000) : undefined});
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const body = await response.json();
        next = options.onResult({ok: true, body});
      } catch (error) {
        next = options.onResult({ok: false, offline: !online(), error});
      } finally {
        inFlight = false;
      }
      // onResult returns true for the normal interval, a number for a custom delay, or false to back off.
      failures = next === false ? failures + 1 : 0;
      schedule(next === false ? backoff(failures, options.random) : next === true || next === undefined ? interval : next);
    }

    function now() { env.clearTimeout(timer); timer = null; poll(); }

    function start() {
      if (running) return;
      running = true;
      if (env.document && env.document.addEventListener) {
        env.document.addEventListener('visibilitychange', () => { if (visible()) now(); else { env.clearTimeout(timer); timer = null; } });
      }
      if (env.window && env.window.addEventListener) {
        env.window.addEventListener('online', now);
        env.window.addEventListener('offline', now);
      }
      poll();
    }

    return {start, now, stop() { stopped = true; env.clearTimeout(timer); }, get failures() { return failures; }};
  }

  /** The page-wide status: latest stats with fallback to the last good values. */
  function createStatus(env = root) {
    const listeners = new Set();
    const view = {state: null, stats: null, receivedAt: null, retryAt: null};
    let lastGood = null;
    let lastGoodAt = 0;
    const aged = () => lastGood && {...lastGood, age_seconds: (lastGood.age_seconds || 0) + (env.Date.now() - lastGoodAt) / 1000};

    function emit() {
      const snap = snapshot(view, env.Date.now());
      for (const listener of listeners) listener(snap);
    }

    const poller = createPoller({
      env,
      url: () => '/api/demo/stats',
      onSchedule(at) { view.retryAt = ['api_unavailable', 'offline', 'metrics_unavailable'].includes(view.state) ? at : null; },
      onResult(result) {
        const now = env.Date.now();
        if (result.ok && validStats(result.body)) {
          const body = result.body;
          view.state = body.status;
          if (hasValues(body)) {
            lastGood = body;
            lastGoodAt = now;
            view.stats = body;
          } else {
            // A response without values (for example a restarted server while Prometheus is down) keeps the browser's last good values.
            view.stats = aged() || body;
          }
          view.receivedAt = now;
          emit();
          if (body.status === 'metrics_unavailable') return false;
          return body.status === 'stopped' ? 5000 : true;
        }
        view.state = result.offline ? 'offline' : 'api_unavailable';
        view.stats = aged();
        view.receivedAt = now;
        emit();
        return false;
      },
    });

    return {
      subscribe(listener) { listeners.add(listener); if (view.state) listener(snapshot(view, env.Date.now())); return () => listeners.delete(listener); },
      start() {
        poller.start();
        // Ages keep counting between polls and during backoff.
        env.setInterval(() => { if (view.state && (!env.document || env.document.visibilityState !== 'hidden')) emit(); }, 1000);
      },
      refresh: () => poller.now(),
      get snapshot() { return snapshot(view, env.Date.now()); },
    };
  }

  /** Binds the shared header status and the page notice. */
  function bindHeader(status, doc) {
    const pillElement = doc.getElementById('status');
    const noticeElement = doc.getElementById('notice');
    status.subscribe(snap => {
      if (pillElement) {
        pillElement.dataset.state = snap.state || '';
        pillElement.querySelector('.status-text').textContent = pill(snap);
      }
      if (!noticeElement) return;
      const message = notice(snap);
      noticeElement.hidden = !message;
      if (!message) return;
      noticeElement.querySelector('.notice-text').textContent = message.text;
    });
  }

  root.DemoStatus = {
    isNumber, count, duration, latency, ago, clock, sig, validStats, snapshot, pill, notice, backoff,
    createPoller, createStatus, bindHeader, sockets, booklessNote, RETAINED_STATES,
  };

  if (root.document && root.document.getElementById && root.document.getElementById('status')) {
    const status = createStatus(root);
    root.DemoStatus.page = status;
    bindHeader(status, root.document);
    // Page scripts subscribe before polling starts.
    root.addEventListener('DOMContentLoaded', () => status.start());
  }
})(typeof window !== 'undefined' ? window : globalThis);
