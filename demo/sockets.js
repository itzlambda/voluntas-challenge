'use strict';
// Per-socket traffic on the live page: a fleet distribution, a filtered socket list and one socket's history.
// Kept apart from charts.js so the panel never joins the generic chart loop. Pure helpers are exported for tests.
(function (root) {
  const S = root.DemoStatus;
  const isNum = S.isNumber;
  const PAGE_SIZE = 25;
  const WINDOWS = ['15m', '1h', '6h', '24h'];
  const DEFAULT_WINDOW = '1h';
  // Socket samples arrive every 15 s, so history never refreshes faster than that.
  const HISTORY_REFRESH = {'15m': 15000, '1h': 15000, '6h': 60000, '24h': 120000};
  const FLEET_INTERVAL = 5000;
  const STATES = ['connected', 'disconnected', 'empty', 'warming_up'];
  const FLEET_STATUSES = ['live', 'stale', 'unavailable'];
  const HISTORY_STATUSES = ['ok', 'stale', 'unavailable'];
  const POOLS = ['primary', 'replica'];
  const SORTS = ['busiest', 'quietest', 'id'];
  // Categories outside the distribution are counted on their own, never folded into zero.
  const CATEGORIES = ['rated', 'no_rate', 'warming_up', 'disconnected', 'empty'];
  const CATEGORY_LABELS = {rated: 'With a rate', no_rate: 'Rate unavailable', warming_up: 'Warming up', disconnected: 'Disconnected', empty: 'No markets'};
  const STATE_LABELS = {connected: 'Connected', disconnected: 'Disconnected', empty: 'No markets', warming_up: 'Warming up'};
  const NICE = [1, 2, 2.5, 5];
  const TARGET_BINS = 12;
  const MIN_BINS = 8;
  const MAX_BINS = 15;
  // A one-minute rate moves in steps of 1/60, so finer bins would only show sampling noise.
  const MIN_STEP = 0.01;
  // Refitting is suggested once this share of nonzero rates falls outside the fitted range.
  const DRIFT_SHARE = 0.1;
  const C = {green: '#2abc89', light: '#c9c9c9', axis: '#8b9094', grid: '#2f3336', gap: 'rgba(139, 144, 148, 0.07)'};

  // Parsing. Anything malformed counts as a failed response, never as live data.
  function parseSocket(raw) {
    if (!raw || !Number.isInteger(raw.id) || raw.id < 0 || raw.id > 65535 || !POOLS.includes(raw.pool) || !STATES.includes(raw.state)) return null;
    const rate = raw.events_per_second;
    return {
      id: raw.id, pool: raw.pool, state: raw.state, connected: raw.connected === true,
      markets: isNum(raw.markets) && raw.markets >= 0 ? raw.markets : null,
      rate: isNum(rate) && rate >= 0 ? rate : null,
    };
  }

  /** {status, retained, values_at, age_seconds, sockets|null}; sockets is null when the body carries none. */
  function parseFleet(body) {
    if (!body || typeof body !== 'object' || body.schema_version !== 1 || !FLEET_STATUSES.includes(body.status)) return null;
    let sockets = null;
    if (Array.isArray(body.sockets)) {
      const seen = new Set();
      sockets = [];
      for (const raw of body.sockets) {
        const socket = parseSocket(raw);
        if (socket && !seen.has(socket.id)) { seen.add(socket.id); sockets.push(socket); }
      }
    }
    return {
      status: body.status, retained: body.retained === true,
      values_at: isNum(body.values_at) ? body.values_at : null,
      age_seconds: isNum(body.age_seconds) ? body.age_seconds : null,
      rate_window_seconds: isNum(body.rate_window_seconds) ? body.rate_window_seconds : 60,
      sockets,
    };
  }

  /** History for the requested socket and window only; error bodies may omit everything else. */
  function parseHistory(body, socket, window) {
    if (!body || typeof body !== 'object' || body.schema_version !== 1 || !HISTORY_STATUSES.includes(body.status)) return null;
    if (body.socket_id !== undefined && body.socket_id !== socket) return null;
    if (body.window !== undefined && body.window !== window) return null;
    const t = body.timestamps;
    const v = body.events_per_second;
    const series = Array.isArray(t) && Array.isArray(v) && t.length === v.length && t.every(isNum);
    return {
      status: body.status, socket, window,
      pool: POOLS.includes(body.pool) ? body.pool : null,
      start: isNum(body.start) ? body.start : null, end: isNum(body.end) ? body.end : null,
      step_seconds: isNum(body.step_seconds) ? body.step_seconds : null,
      timestamps: series ? t : null,
      values: series ? v.map(value => isNum(value) ? value : null) : null,
      restarts: Array.isArray(body.restarts) ? body.restarts.filter(isNum) : [],
    };
  }

  /** Only connected sockets with markets and a finite rate join the distribution. */
  function category(socket) {
    if (socket.state === 'empty' || socket.markets === 0) return 'empty';
    if (socket.state === 'disconnected' || !socket.connected) return 'disconnected';
    if (socket.state === 'warming_up') return 'warming_up';
    return socket.rate === null || socket.markets === null ? 'no_rate' : 'rated';
  }

  function quantile(sorted, p) {
    const at = (sorted.length - 1) * p;
    const low = Math.floor(at);
    return sorted[low] + (sorted[Math.min(low + 1, sorted.length - 1)] - sorted[low]) * (at - low);
  }

  const tidy = value => Number(value.toPrecision(12));

  /** Round-number edges covering [lo, hi], with a bin count as close to the target as the steps allow. */
  function niceEdges(lo, hi) {
    const exponent = Math.floor(Math.log10(Math.max(hi - lo, MIN_STEP) / TARGET_BINS));
    let best = null;
    for (let e = exponent - 1; e <= exponent + 1; e += 1) {
      for (const m of NICE) {
        const step = tidy(m * 10 ** e);
        if (step < MIN_STEP) continue;
        const first = Math.floor(lo / step + 1e-9);
        // The largest regular value must sit inside the last bin, not on its open upper edge.
        const last = Math.floor(hi / step + 1e-9) + 1;
        const n = last - first;
        const score = (n < MIN_BINS || n > MAX_BINS ? 100 : 0) + Math.abs(n - TARGET_BINS);
        if (!best || score < best.score) best = {score, step, first, n};
      }
    }
    const edges = [];
    for (let i = 0; i <= best.n; i += 1) edges.push(tidy((best.first + i) * best.step));
    return edges;
  }

  /**
   * Fits bin edges to the nonzero rates. Values beyond wide quartile fences go to the underflow and
   * overflow bins so a few outliers cannot squeeze everyone else into one bar. Zero has its own bin.
   */
  function fitLayout(rates) {
    const positive = rates.filter(rate => isNum(rate) && rate > 0).sort((a, b) => a - b);
    if (!positive.length) return {edges: niceEdges(0, 1)};
    const q1 = quantile(positive, 0.25);
    const q3 = quantile(positive, 0.75);
    const spread = Math.max(q3 - q1, q3 * 0.1, MIN_STEP);
    const inliers = positive.filter(rate => rate >= q1 - 3 * spread && rate <= q3 + 3 * spread);
    let lo = inliers[0];
    let hi = inliers[inliers.length - 1];
    if (hi - lo < MIN_STEP * MIN_BINS) {
      const half = Math.max(hi * 0.1, MIN_STEP * MIN_BINS / 2);
      lo = Math.max(0, lo - half);
      hi += half;
    }
    // Start at zero when the data nearly reaches it; an axis from zero is easier to read.
    if (lo <= (hi - lo) * 0.5) lo = 0;
    return {edges: niceEdges(lo, hi)};
  }

  /** Bin key for a rated socket: 'zero', 'under', 'over' or the regular bin index as a string. */
  function binOf(rate, layout) {
    if (rate === 0) return 'zero';
    const edges = layout.edges;
    if (rate < edges[0]) return 'under';
    if (rate >= edges[edges.length - 1]) return 'over';
    for (let i = 0; i < edges.length - 1; i += 1) if (rate < edges[i + 1]) return String(i);
    return 'over';
  }

  function bins(layout) {
    const edges = layout.edges;
    const out = [{key: 'zero', kind: 'zero', lo: 0, hi: 0}];
    if (edges[0] > 0) out.push({key: 'under', kind: 'under', lo: 0, hi: edges[0]});
    for (let i = 0; i < edges.length - 1; i += 1) out.push({key: String(i), kind: 'regular', lo: edges[i], hi: edges[i + 1]});
    out.push({key: 'over', kind: 'over', lo: edges[edges.length - 1], hi: Infinity});
    return out;
  }

  /** Bin counts, state counts and whether the fitted range still fits. */
  function histogram(sockets, layout) {
    const categories = Object.fromEntries(CATEGORIES.map(key => [key, 0]));
    const counts = {};
    let positive = 0;
    for (const socket of sockets || []) {
      const kind = category(socket);
      categories[kind] += 1;
      if (kind !== 'rated' || !layout) continue;
      const key = binOf(socket.rate, layout);
      counts[key] = (counts[key] || 0) + 1;
      if (socket.rate > 0) positive += 1;
    }
    const list = layout ? bins(layout).map(bin => ({...bin, count: counts[bin.key] || 0})) : [];
    const outside = (counts.under || 0) + (counts.over || 0);
    return {
      bins: list, categories, max: list.reduce((max, bin) => Math.max(max, bin.count), 0),
      drift: positive > 0 && outside / positive > DRIFT_SHARE,
    };
  }

  function matches(socket, filter, layout) {
    if (!filter) return true;
    if (filter.kind === 'state') return category(socket) === filter.key;
    return category(socket) === 'rated' && Boolean(layout) && binOf(socket.rate, layout) === filter.key;
  }

  /** Normalized ID search: digits, optionally after '#'. Returns null for a query that cannot match. */
  function idQuery(query) {
    const text = String(query || '').trim().replace(/^#/, '');
    if (!text) return '';
    return /^\d{1,5}$/.test(text) ? text : null;
  }

  /** Filters and sorts; sockets without a rate always sort after those with one. */
  function selectRows(sockets, layout, options) {
    const {filter = null, pool = '', query = '', sort = 'busiest'} = options || {};
    const id = idQuery(query);
    if (id === null) return [];
    const rows = (sockets || []).filter(socket => matches(socket, filter, layout)
      && (!pool || socket.pool === pool) && (!id || String(socket.id).startsWith(id)));
    const direction = sort === 'quietest' ? 1 : -1;
    return rows.sort((a, b) => {
      if (sort === 'id') return a.id - b.id;
      if ((a.rate === null) !== (b.rate === null)) return a.rate === null ? 1 : -1;
      return (a.rate === null ? 0 : direction * (a.rate - b.rate)) || a.id - b.id;
    });
  }

  function paginate(rows, page) {
    const pages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
    const current = Math.min(Math.max(0, page | 0), pages - 1);
    const start = current * PAGE_SIZE;
    return {page: current, pages, start, total: rows.length, items: rows.slice(start, start + PAGE_SIZE)};
  }

  /** [start, end] index ranges of missing values, drawn as shaded gaps. */
  function gaps(values) {
    const ranges = [];
    let start = null;
    (values || []).forEach((value, i) => {
      if (!isNum(value) && start === null) start = i;
      if (isNum(value) && start !== null) { ranges.push([start, i - 1]); start = null; }
    });
    if (start !== null) ranges.push([start, values.length - 1]);
    return ranges;
  }

  const edge = value => S.sig(value, 3);
  const rateText = rate => !isNum(rate) ? null : rate === 0 ? '0' : S.sig(rate, rate < 0.1 ? 2 : 3);

  function binLabel(bin) {
    if (bin.kind === 'zero') return '0';
    if (bin.kind === 'under') return `<${edge(bin.hi)}`;
    if (bin.kind === 'over') return `≥${edge(bin.lo)}`;
    // The first positive bin starts above zero; exact zero has its own bar.
    return bin.lo === 0 ? '>0' : edge(bin.lo);
  }

  function binDescription(bin) {
    if (bin.kind === 'zero') return 'exactly zero events per second';
    if (bin.kind === 'under') return `above 0 and below ${edge(bin.hi)} events per second, under the fitted range`;
    if (bin.kind === 'over') return `${edge(bin.lo)} events per second or more, above the fitted range`;
    return bin.lo === 0 ? `above 0 and below ${edge(bin.hi)} events per second` : `${edge(bin.lo)} to below ${edge(bin.hi)} events per second`;
  }

  function sockets(count) { return `${S.count(count)} socket${count === 1 ? '' : 's'}`; }

  /** The panel's own freshness line. The page header stays with status.js. */
  function fleetLine(view) {
    const age = S.ago(view.age);
    const from = age ? `values from ${age}` : 'values of unknown age';
    if (view.paused && view.fleet) return `Paused, showing ${from}`;
    switch (view.fleetState) {
      case 'live': return age ? `Updated ${age}` : 'Updated';
      case 'stale': return view.fleet ? `Stale, ${from}` : 'Stale';
      case 'unavailable': return view.fleet ? `Unavailable, showing ${from}` : 'Socket telemetry unavailable';
      case 'error': return view.fleet ? `Could not refresh, showing ${from}. Retrying.` : 'Socket list unavailable. Retrying.';
      case 'offline': return view.fleet ? `Offline, showing ${from}` : 'Offline';
      default: return 'Loading sockets';
    }
  }

  /** State, polling and selection, independent of the DOM so tests can drive it. */
  function createController(options) {
    const env = options.env;
    const listeners = new Set();
    const st = {
      fleet: null, fleetAt: null, fleetState: 'loading', layout: null, fittedAt: null,
      filter: null, pool: '', query: '', sort: 'busiest', page: 0, paused: false,
      selected: null, window: DEFAULT_WINDOW, history: null, historyState: 'idle',
    };
    let fleetPoller = null;
    let historyPoller = null;
    let historyGeneration = 0;
    let memo = null;

    const emit = () => { for (const listener of listeners) listener(); };
    const ratedRates = () => (st.fleet ? st.fleet.sockets : []).filter(s => category(s) === 'rated').map(s => s.rate);

    function fit() {
      const rates = ratedRates();
      if (!rates.length) return false;
      st.layout = fitLayout(rates);
      st.fittedAt = env.Date.now();
      if (st.filter && st.filter.kind === 'bin') st.filter = null;
      st.page = 0;
      return true;
    }

    function onFleet(result) {
      if (st.paused) return true;
      const fleet = result.ok ? parseFleet(result.body) : null;
      if (!fleet) {
        st.fleetState = result.offline ? 'offline' : 'error';
        emit();
        return false;
      }
      // A body without sockets keeps the last good fleet and its age.
      if (fleet.sockets) {
        st.fleet = fleet;
        st.fleetAt = env.Date.now();
        if (!st.layout) fit();
      }
      st.fleetState = fleet.status;
      emit();
      return fleet.status === 'live' ? true : fleet.status === 'stale' ? 15000 : false;
    }

    function startFleet() {
      if (fleetPoller) fleetPoller.stop();
      fleetPoller = S.createPoller({env, interval: FLEET_INTERVAL, url: () => '/api/demo/sockets', onResult: onFleet});
      fleetPoller.start();
    }

    function loadHistory() {
      if (historyPoller) historyPoller.stop();
      historyPoller = null;
      const mine = ++historyGeneration;
      st.history = null;
      if (st.selected === null) { st.historyState = 'idle'; return; }
      const socket = st.selected;
      const window = st.window;
      st.historyState = 'loading';
      historyPoller = S.createPoller({
        env, interval: HISTORY_REFRESH[window],
        url: () => `/api/demo/socket-history?socket=${socket}&window=${window}`,
        onResult(result) {
          // A late answer for an earlier socket or window is dropped.
          if (mine !== historyGeneration) return true;
          const history = result.ok ? parseHistory(result.body, socket, window) : null;
          if (history) {
            if (history.timestamps) st.history = history;
            st.historyState = history.status;
          } else {
            st.historyState = result.offline ? 'offline' : 'error';
          }
          emit();
          return history ? (history.status === 'ok' ? true : history.status === 'stale' ? HISTORY_REFRESH[window] : false) : false;
        },
      });
      historyPoller.start();
    }

    function view() {
      const sockets = st.fleet ? st.fleet.sockets : [];
      if (!memo || memo.fleet !== st.fleet || memo.layout !== st.layout) memo = {fleet: st.fleet, layout: st.layout, hist: histogram(sockets, st.layout)};
      const rows = selectRows(sockets, st.layout, st);
      const page = paginate(rows, st.page);
      st.page = page.page;
      const age = st.fleet && isNum(st.fleet.age_seconds) ? st.fleet.age_seconds + (env.Date.now() - st.fleetAt) / 1000 : null;
      const selectedSocket = st.selected === null ? null : sockets.find(socket => socket.id === st.selected) || null;
      return {...st, hist: memo.hist, page, age, selectedSocket, queryValid: idQuery(st.query) !== null};
    }

    const reset = () => { st.page = 0; emit(); };
    return {
      subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); },
      start() { startFleet(); },
      view,
      setFilter(filter) {
        const same = filter && st.filter && filter.kind === st.filter.kind && filter.key === st.filter.key;
        st.filter = !filter || same ? null : filter;
        reset();
      },
      setPool(pool) { st.pool = POOLS.includes(pool) ? pool : ''; reset(); },
      setQuery(query) { st.query = String(query || ''); reset(); },
      setSort(sort) { st.sort = SORTS.includes(sort) ? sort : 'busiest'; reset(); },
      setPage(page) { st.page = page; emit(); },
      fit() { fit(); emit(); },
      setPaused(paused) {
        if (paused === st.paused) return;
        st.paused = paused;
        if (paused) { if (fleetPoller) fleetPoller.stop(); fleetPoller = null; } else startFleet();
        emit();
      },
      select(id) {
        if (!Number.isInteger(id) || id === st.selected) return;
        st.selected = id;
        loadHistory();
        emit();
      },
      setWindow(window) {
        if (!WINDOWS.includes(window) || window === st.window) return;
        st.window = window;
        loadHistory();
        emit();
      },
    };
  }

  // Rendering.
  function mount(doc, win, uPlot, controller, demo) {
    const panel = doc.getElementById('sockets');
    const q = name => panel.querySelector(`[data-socket-${name}]`);
    const el = {
      status: q('status'), demo: q('demo'), states: q('states'), histogram: q('histogram'), fit: q('fit'), fitNote: q('fit-note'),
      search: q('search'), pool: q('pool'), sort: q('sort'), pause: q('pause'), filterNote: q('filter'), rows: q('rows'),
      prev: q('prev'), next: q('next'), pageText: q('page'), title: q('title'), meta: q('meta'), plot: q('plot'),
      readout: q('readout'), historyNote: q('history-note'), windows: [...panel.querySelectorAll('[data-socket-window]')],
    };
    let plot = null;
    let history = null;
    el.demo.hidden = !demo;

    function make(tag, className, text) {
      const node = doc.createElement(tag);
      if (className) node.className = className;
      if (text !== undefined) node.textContent = text;
      return node;
    }

    function keepFocus(container, attribute, build) {
      const active = doc.activeElement;
      const key = active && container.contains(active) ? active.getAttribute(attribute) : null;
      build();
      if (key !== null) {
        const next = container.querySelector(`[${attribute}="${key}"]`);
        if (next) next.focus();
      }
    }

    function renderStates(v) {
      keepFocus(el.states, 'data-key', () => {
        el.states.replaceChildren(...CATEGORIES.map(key => {
          const button = make('button', 'state-chip');
          button.type = 'button';
          button.dataset.key = key;
          const pressed = Boolean(v.filter && v.filter.kind === 'state' && v.filter.key === key);
          button.setAttribute('aria-pressed', String(pressed));
          button.append(make('span', 'name', CATEGORY_LABELS[key]), make('b', '', v.fleet ? S.count(v.hist.categories[key]) : 'no data'));
          button.disabled = !v.fleet;
          return button;
        }));
      });
    }

    function renderHistogram(v) {
      const list = v.hist.bins;
      if (!list.length) {
        el.histogram.replaceChildren(make('p', 'plot-empty', v.fleet ? 'No connected socket with markets has a rate yet' : 'No socket data yet'));
        return;
      }
      const pressedKey = v.filter && v.filter.kind === 'bin' ? v.filter.key : null;
      const focusKey = pressedKey || (list.find(bin => bin.count > 0) || list[0]).key;
      keepFocus(el.histogram, 'data-key', () => {
        el.histogram.replaceChildren(...list.map(bin => {
          const button = make('button', `bar ${bin.kind}`);
          button.type = 'button';
          button.dataset.key = bin.key;
          button.tabIndex = bin.key === focusKey ? 0 : -1;
          button.setAttribute('aria-pressed', String(bin.key === pressedKey));
          button.setAttribute('aria-label', `${sockets(bin.count)} at ${binDescription(bin)}`);
          const track = make('span', 'bar-track');
          const fill = make('span', 'bar-fill');
          // CSSOM writes are allowed under the page's CSP; style attributes are not.
          fill.style.height = bin.count ? `max(2px, ${(100 * bin.count / v.hist.max).toFixed(2)}%)` : '0';
          track.append(fill);
          button.append(make('span', 'bar-count', bin.count ? S.count(bin.count) : ''), track, make('span', 'bar-label', binLabel(bin)));
          return button;
        }));
      });
    }

    function renderFit(v) {
      el.fit.disabled = !v.fleet || !v.hist.categories.rated;
      const at = v.fittedAt ? new Date(v.fittedAt).toLocaleTimeString(undefined, {hour: '2-digit', minute: '2-digit', hourCycle: 'h23'}) : null;
      el.fitNote.textContent = v.hist.drift
        ? 'Over a tenth of nonzero rates are now outside the fitted range.'
        : at ? `Bins fitted at ${at} and kept fixed between refreshes.` : '';
      el.fitNote.classList.toggle('attention', v.hist.drift);
    }

    function filterText(v) {
      if (!v.queryValid) return 'Search takes a numeric socket ID.';
      const parts = [];
      if (v.filter && v.filter.kind === 'bin') {
        const bin = v.hist.bins.find(item => item.key === v.filter.key);
        if (bin) parts.push(`rate ${binDescription(bin)}`);
      }
      if (v.filter && v.filter.kind === 'state') parts.push(CATEGORY_LABELS[v.filter.key].toLowerCase());
      if (v.pool) parts.push(`${v.pool} pool`);
      if (idQuery(v.query)) parts.push(`ID starting ${idQuery(v.query)}`);
      return parts.length ? `Showing sockets with ${parts.join(', ')}.` : '';
    }

    function statusCell(socket) {
      const kind = category(socket);
      if (kind === 'no_rate') return 'Connected, rate unavailable';
      return STATE_LABELS[socket.state] || socket.state;
    }

    function renderRows(v) {
      const items = v.page.items;
      keepFocus(el.rows, 'data-id', () => {
        if (!items.length) {
          const row = make('tr');
          const cell = make('td', 'empty-row', v.fleet ? 'No sockets match.' : 'No socket data yet.');
          cell.colSpan = 5;
          row.append(cell);
          el.rows.replaceChildren(row);
          return;
        }
        el.rows.replaceChildren(...items.map(socket => {
          const row = make('tr');
          row.dataset.row = String(socket.id);
          const selected = socket.id === v.selected;
          if (selected) row.className = 'selected';
          const button = make('button', 'socket-id mono', String(socket.id));
          button.type = 'button';
          button.dataset.id = String(socket.id);
          button.setAttribute('aria-pressed', String(selected));
          button.setAttribute('aria-label', `Socket ${socket.id}, show history`);
          const id = make('td');
          id.append(button);
          const rate = rateText(socket.rate);
          row.append(id, make('td', socket.pool, socket.pool === 'primary' ? 'Primary' : 'Replica'), make('td', '', statusCell(socket)),
            make('td', 'num', socket.markets === null ? 'unknown' : S.count(socket.markets)),
            make('td', `num rate${rate === null ? ' unavailable' : ''}`, rate === null ? 'no rate' : rate));
          return row;
        }));
      });
      const {page} = v;
      el.pageText.textContent = page.total ? `${S.count(page.start + 1)} to ${S.count(page.start + items.length)} of ${S.count(page.total)}` : '0 sockets';
      el.prev.disabled = page.page === 0;
      el.next.disabled = page.page >= page.pages - 1;
    }

    // History chart.
    const timeAxis = (u, values) => values.map(value => new Date(value * 1000).toLocaleTimeString(undefined, {hour: '2-digit', minute: '2-digit', hourCycle: 'h23'}));
    const ratio = () => uPlot.pxRatio || win.devicePixelRatio || 1;

    function drawGaps(u) {
      if (!history) return;
      const step = history.step_seconds || 0;
      const x = u.data[0];
      const {ctx, bbox} = u;
      ctx.save();
      ctx.fillStyle = C.gap;
      for (const [a, b] of gaps(history.values)) {
        const left = Math.max(bbox.left, u.valToPos(x[a] - step / 2, 'x', true));
        const right = Math.min(bbox.left + bbox.width, u.valToPos(x[b] + step / 2, 'x', true));
        if (right > left) ctx.fillRect(left, bbox.top, right - left, bbox.height);
      }
      ctx.restore();
    }

    function drawRestarts(u) {
      if (!history || !history.restarts.length) return;
      const {ctx, bbox} = u;
      ctx.save();
      ctx.strokeStyle = C.light;
      ctx.lineWidth = ratio();
      ctx.setLineDash([4 * ratio(), 4 * ratio()]);
      for (const at of history.restarts) {
        const x = Math.round(u.valToPos(at, 'x', true));
        if (x < bbox.left || x > bbox.left + bbox.width) continue;
        ctx.beginPath();
        ctx.moveTo(x, bbox.top);
        ctx.lineTo(x, bbox.top + bbox.height);
        ctx.stroke();
      }
      ctx.restore();
    }

    function readout(u) {
      const i = u.cursor.idx;
      if (!history || !isNum(i)) { el.readout.textContent = ''; return; }
      const at = new Date(history.timestamps[i] * 1000).toLocaleTimeString(undefined, {hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23'});
      const value = rateText(history.values[i]);
      el.readout.textContent = `${at}: ${value === null ? 'no rate' : `${value} events/s`}`;
    }

    function plotOptions() {
      return {
        width: Math.max(240, el.plot.clientWidth), height: 220,
        legend: {show: false},
        cursor: {drag: {x: false, y: false}, points: {size: 5}},
        scales: {
          x: {time: true, range: (u, min, max) => history && isNum(history.start) && isNum(history.end) ? [history.start, history.end] : [min, max]},
          y: {range: (u, min, max) => [0, isNum(max) && max > 0 ? max * 1.15 : 1]},
        },
        axes: [
          {stroke: C.axis, grid: {stroke: C.grid, width: 1}, ticks: {stroke: C.grid, width: 1}, font: '12px system-ui, sans-serif', values: timeAxis, space: 70},
          {stroke: C.axis, grid: {stroke: C.grid, width: 1}, ticks: {stroke: C.grid, width: 1}, font: '12px system-ui, sans-serif', size: 56,
            values: (u, values) => values.map(value => S.sig(value, 3))},
        ],
        series: [{}, {label: 'Events per second', stroke: C.green, width: 1.5, spanGaps: false, points: {show: false}}],
        hooks: {drawAxes: [drawGaps], draw: [drawRestarts], setCursor: [readout]},
      };
    }

    function historyNote(v) {
      const state = {
        loading: 'Loading history.', error: 'History could not be loaded. Retrying.', offline: 'History paused while offline.',
        unavailable: 'History is unavailable from Prometheus. Retrying.', stale: 'History could not be refreshed.',
      }[v.historyState];
      const parts = [];
      if (state && v.history && v.historyState !== 'loading') parts.push(`${state} Showing the last load.`);
      else if (state) parts.push(state);
      if (v.history) {
        const restarts = v.history.restarts.length;
        parts.push('Gaps mean no rate: missing samples, stale telemetry, disconnected, no markets or rate warmup.');
        if (restarts) parts.push(`${restarts === 1 ? 'One collector restart' : `${restarts} collector restarts`} in range (dashed). After a restart this ID may carry different markets.`);
      }
      return parts.join(' ');
    }

    function renderHistory(v) {
      for (const button of el.windows) button.setAttribute('aria-pressed', String(button.dataset.socketWindow === v.window));
      if (v.selected === null) {
        el.title.textContent = 'No socket selected';
        el.meta.textContent = 'Select a socket in the list to load its events per second.';
      } else {
        el.title.textContent = `Socket ${v.selected}`;
        const s = v.selectedSocket;
        const rate = s && rateText(s.rate);
        el.meta.textContent = !s ? 'Not in the current socket list.'
          : `${s.pool === 'primary' ? 'Primary' : 'Replica'} pool, ${statusCell(s).toLowerCase()}, ${s.markets === null ? 'unknown' : S.count(s.markets)} markets, ${rate === null ? 'no current rate' : `${rate} events/s now`}.`;
      }
      history = v.history;
      let empty = el.plot.querySelector('.plot-empty');
      if (!empty) { empty = make('p', 'plot-empty'); el.plot.append(empty); }
      const has = Boolean(history && history.values.some(isNum));
      empty.hidden = has;
      empty.textContent = v.selected === null ? 'No socket selected'
        : !history ? (v.historyState === 'loading' ? 'Loading history' : 'History unavailable')
        : has ? '' : 'No rate in this range';
      el.plot.setAttribute('aria-label', v.selected === null ? 'No socket selected'
        : `Events per second for socket ${v.selected} over ${v.window}, with gaps where no rate exists and dashed lines at collector restarts`);
      if (history) {
        const data = [history.timestamps, history.values];
        if (plot) plot.setData(data); else plot = new uPlot(plotOptions(), data, el.plot);
      } else if (plot) {
        plot.destroy();
        plot = null;
        el.readout.textContent = '';
      }
      el.historyNote.textContent = v.selected === null ? '' : historyNote(v);
    }

    function renderStatus() {
      const v = controller.view();
      el.status.textContent = `${demo ? 'Synthetic demo. ' : ''}${fleetLine(v)}`;
      panel.classList.toggle('retained', Boolean(v.fleet) && (v.paused || v.fleetState !== 'live'));
    }

    function render() {
      const v = controller.view();
      renderStatus();
      renderStates(v);
      renderHistogram(v);
      renderFit(v);
      el.pause.setAttribute('aria-pressed', String(v.paused));
      el.pause.textContent = v.paused ? 'Resume updates' : 'Pause updates';
      el.filterNote.textContent = filterText(v);
      renderRows(v);
      renderHistory(v);
    }

    el.states.addEventListener('click', event => {
      const button = event.target.closest('button[data-key]');
      if (button) controller.setFilter({kind: 'state', key: button.dataset.key});
    });
    el.histogram.addEventListener('click', event => {
      const button = event.target.closest('button[data-key]');
      if (button) controller.setFilter({kind: 'bin', key: button.dataset.key});
    });
    // One tab stop for the bars; arrow keys move between them.
    el.histogram.addEventListener('keydown', event => {
      const buttons = [...el.histogram.querySelectorAll('button')];
      const index = buttons.indexOf(doc.activeElement);
      if (index < 0) return;
      const next = {ArrowRight: index + 1, ArrowLeft: index - 1, Home: 0, End: buttons.length - 1}[event.key];
      if (next === undefined) return;
      event.preventDefault();
      const target = buttons[Math.max(0, Math.min(buttons.length - 1, next))];
      for (const button of buttons) button.tabIndex = button === target ? 0 : -1;
      target.focus();
    });
    el.fit.addEventListener('click', () => controller.fit());
    el.search.addEventListener('input', () => controller.setQuery(el.search.value));
    el.pool.addEventListener('change', () => controller.setPool(el.pool.value));
    el.sort.addEventListener('change', () => controller.setSort(el.sort.value));
    el.pause.addEventListener('click', () => controller.setPaused(!controller.view().paused));
    el.prev.addEventListener('click', () => controller.setPage(controller.view().page.page - 1));
    el.next.addEventListener('click', () => controller.setPage(controller.view().page.page + 1));
    el.rows.addEventListener('click', event => {
      const row = event.target.closest('tr[data-row]');
      if (row) controller.select(Number(row.dataset.row));
    });
    for (const button of el.windows) button.addEventListener('click', () => controller.setWindow(button.dataset.socketWindow));

    controller.subscribe(render);
    // Ages keep counting between polls.
    win.setInterval(() => { if (doc.visibilityState !== 'hidden') renderStatus(); }, 1000);
    if (win.ResizeObserver) {
      new win.ResizeObserver(() => { if (plot) plot.setSize({width: Math.max(240, el.plot.clientWidth), height: 220}); }).observe(el.plot);
    }
    render();
  }

  function browserEnv(fetch) {
    return {
      fetch, Date: root.Date, document: root.document, navigator: root.navigator, window: root, AbortSignal: root.AbortSignal,
      setTimeout: (fn, ms) => root.setTimeout(fn, ms), clearTimeout: id => root.clearTimeout(id),
    };
  }

  function boot() {
    const doc = root.document;
    if (!doc.getElementById('sockets') || !root.uPlot) return;
    const start = (fetch, demo) => {
      const controller = createController({env: browserEnv(fetch)});
      mount(doc, root, root.uPlot, controller, demo);
      controller.start();
      root.DemoSockets.page = controller;
    };
    if (new URLSearchParams(root.location.search).get('socket-demo') !== '1') {
      start((url, init) => root.fetch(url, init), false);
      return;
    }
    // The synthetic fixture loads only with the explicit flag, so the normal page never carries mock data.
    const script = doc.createElement('script');
    script.src = '/socket-demo.js';
    script.addEventListener('load', () => start(root.DemoSocketFixture.create({now: () => root.Date.now()}).fetch, true));
    script.addEventListener('error', () => {
      doc.querySelector('[data-socket-status]').textContent = 'The synthetic demo could not be loaded.';
    });
    doc.head.append(script);
  }

  root.DemoSockets = {
    PAGE_SIZE, WINDOWS, DEFAULT_WINDOW, HISTORY_REFRESH, CATEGORIES,
    parseFleet, parseHistory, category, fitLayout, binOf, bins, histogram, selectRows, paginate, gaps,
    idQuery, binLabel, binDescription, rateText, fleetLine, createController,
  };
  if (root.document && root.document.getElementById && S && S.page) boot();
})(typeof window !== 'undefined' ? window : globalThis);
