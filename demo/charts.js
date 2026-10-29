'use strict';
// Live page charts. uPlot draws; the transforms below are pure so tests can check them.
(function (root) {
  const S = root.DemoStatus;
  const C = {
    green: '#2abc89', blue: '#5ba9e7', light: '#c9c9c9', grey: '#8b9094', dark: '#767d82', miss: '#f05d4d',
    axis: '#8b9094', grid: '#2f3336', gap: 'rgba(139, 144, 148, 0.07)',
    rollout: '#d9a441', rolloutFill: 'rgba(217, 164, 65, 0.09)',
    incident: '#f05d4d', incidentFill: 'rgba(240, 93, 77, 0.10)',
  };
  const REASON_COLORS = {remote_close: C.green, io_error: C.blue, silence: C.light, decode_error: C.grey,
    protocol_error: C.dark, sequence_gap: C.dark};
  const REASON_TIPS = {
    remote_close: 'Polymarket sent a close frame or ended the connection.',
    io_error: 'A transport read, write or send operation failed.',
    silence: 'No frame arrived within the connection silence limit, so the collector replaced the socket.',
    decode_error: 'A recognized book message could not be decoded, so the affected copy was marked unsynchronized.',
    protocol_error: 'The WebSocket session ended because of a protocol error.',
    sequence_gap: 'The exchange protocol reported missing messages, so the affected copy was marked unsynchronized.',
  };
  const END_REASONS = ['remote_close', 'io_error', 'silence', 'decode_error', 'protocol_error'];
  const SWITCH_REASONS = ['remote_close', 'io_error', 'silence', 'decode_error', 'sequence_gap'];
  const WORKER_COLORS = [C.green, C.blue, C.light, C.grey, C.dark];
  // Approximate restart time, estimated from the reported dip just before the screenshot.
  const DEPLOYMENT_AT = Date.UTC(2026, 9, 27, 13, 8) / 1000;
  const HISTORICAL_MARKER = {at: DEPLOYMENT_AT, label: 'New deployment', details: 'service restarted to bring in other changes.', approx: true};
  const RANGES = {'15m': 5000, '1h': 15000, '12h': 60000, '24h': 120000, all: 120000};
  // Legend swatches use classes because the page's CSP forbids inline styles.
  const SWATCH = {[C.green]: 'c-green', [C.blue]: 'c-blue', [C.light]: 'c-light', [C.grey]: 'c-grey', [C.dark]: 'c-dark', [C.miss]: 'c-miss'};

  // Pure transforms.
  const isNum = S.isNumber;
  const diff = (a, b) => (a || []).map((value, i) => isNum(value) && isNum(b && b[i]) ? Math.max(0, value - b[i]) : null);

  /** Cumulative stacks, bottom first. A bucket with no data stays null in every layer. */
  function stack(layers) {
    const totals = [];
    return layers.map(layer => layer.map((value, i) => {
      if (!isNum(value)) return null;
      totals[i] = (totals[i] || 0) + value;
      return totals[i];
    }));
  }

  /** [start, end] index ranges where every series is null. */
  function gaps(arrays) {
    const length = arrays.length ? arrays[0].length : 0;
    const ranges = [];
    let start = null;
    for (let i = 0; i < length; i += 1) {
      const empty = arrays.every(values => !isNum(values[i]));
      if (empty && start === null) start = i;
      if (!empty && start !== null) { ranges.push([start, i - 1]); start = null; }
    }
    if (start !== null) ranges.push([start, length - 1]);
    return ranges;
  }

  const sum = values => (values || []).reduce((total, value) => total + (isNum(value) ? value : 0), 0);
  const hasData = arrays => arrays.some(values => (values || []).some(isNum));
  const latest = values => { for (let i = (values || []).length - 1; i >= 0; i -= 1) if (isNum(values[i])) return values[i]; return null; };
  const compact = value => !isNum(value) ? '' : Math.abs(value) >= 1e6 ? `${S.sig(value / 1e6, 3)}M` : Math.abs(value) >= 1e3 ? `${S.sig(value / 1e3, 3)}k` : S.sig(value, 3);
  const percent = value => isNum(value) ? `${Math.round(value * 100)}%` : '';
  const percentAxis = value => isNum(value) ? `${S.sig(value * 100, 2)}%` : '';
  const rounded = value => isNum(value) ? S.count(value) : '';

  /** Series definitions per chart: [label, color, values, options]. */
  function definitions(history) {
    const s = history.series || {};
    const b = (history.bars && history.bars.series) || {};
    const workers = Object.keys(s).filter(key => key.startsWith('worker_busy:')).sort((x, y) => Number(x.split(':')[1]) - Number(y.split(':')[1]));
    const reasons = (prefix, list) => list.map(reason => ({label: reason, mono: true, color: REASON_COLORS[reason], tip: REASON_TIPS[reason], values: b[`${prefix}:${reason}`] || []}));
    return {
      internal: {log: true, min: 0.01, top: 1000, format: S.duration, series: [
        {label: 'p50', color: C.green, values: s.internal_p50 || []},
        {label: 'p99', color: C.blue, values: s.internal_p99 || []},
      ]},
      updates: {format: rounded, axis: compact, series: [
        {label: 'Book updates', color: C.green, values: s.book_updates_per_second || []},
        {label: 'Messages received', color: C.grey, values: s.events_per_second || []},
      ]},
      gaps: {format: rounded, axis: compact, series: [
        {label: 'Without a current book', color: C.green, values: diff(s.markets_desired, s.markets_synced)},
        {label: 'Without a ready standby', color: C.blue, values: diff(s.markets_desired, s.markets_standby_ready)},
      ]},
      disconnects: {bars: true, format: rounded, axis: compact, series: reasons('session_ends', END_REASONS)},
      switches: {bars: true, format: rounded, axis: compact, series: reasons('publisher_switches', SWITCH_REASONS)},
      workers: {format: percent, axis: percentAxis, min0: 0.1, series: workers.map((key, i) => ({
        label: `W${key.split(':')[1]}`, mono: true, color: WORKER_COLORS[i % WORKER_COLORS.length], values: s[key]}))},
      writer_busy: {format: percent, axis: percentAxis, min0: 0.1, series: [
        {label: 'Writer busy', color: C.green, values: s.writer_busy || []},
      ]},
      writer: {format: value => isNum(value) ? `${S.sig(value, 2)} ms waiting / second` : '', axis: value => `${S.sig(value, 2)} ms`, min0: 100, series: [
        {label: 'Combined worker wait', color: C.green, values: (s.writer_blocked || []).map(value => isNum(value) ? value * 1000 : null)},
      ]},
      exchange: {log: true, min: 1, top: 1000, format: S.duration, series: [
        {label: 'p50', color: C.green, values: s.exchange_p50 || []},
        {label: 'p99', color: C.blue, values: s.exchange_p99 || []},
      ]},
      connections: {format: rounded, axis: compact, min0: 4, series: [
        {label: 'Order-book', color: C.green, values: diff(s['connections:orderbook:expected'], s['connections:orderbook:connected'])},
        {label: 'Discovery', color: C.blue, values: diff(s['connections:market_discovery:expected'], s['connections:market_discovery:connected'])},
      ]},
      checks: {bars: true, format: rounded, axis: compact, series: [
        {label: 'Mismatched', color: C.green, values: b.book_mismatches || [],
          tip: 'A full book from Polymarket that differs from the book built from its own price changes. Every one we traced was Polymarket sending a full book out of order with its updates, not data lost by the collector.'},
        {label: 'Skipped as older', color: C.blue, values: b.older_full_books_skipped || [],
          tip: 'A full book older than price changes already applied. Polymarket sent it late, so the collector ignores it.'},
        {label: 'Decode errors', color: C.grey, values: b.decode_errors || [],
          tip: 'Messages from Polymarket that could not be decoded.'},
      ]},
    };
  }

  /**
   * One value per series, shown in the legend when the cursor is off the chart.
   * Lines show the current value, from live stats where available; bars show the total in range.
   */
  function legendValues(id, def, stats) {
    const st = stats || {};
    const markets = st.markets || {};
    const standby = (st.resilience || {}).standby_ready_markets;
    const fromHistory = def.series.map(item => {
      if (def.bars) return hasData([item.values]) ? def.format(sum(item.values)) : null;
      const value = latest(item.values);
      return isNum(value) ? def.format(value) : null;
    });
    const count = value => isNum(value) ? S.count(value) : null;
    const notOpen = kind => { const sockets = S.sockets(st, kind); return sockets ? S.count(Math.max(0, sockets.expected - sockets.connected)) : null; };
    const live = {
      internal: () => [S.latency(st, 'internal_latency', 50), S.latency(st, 'internal_latency', 99)],
      exchange: () => [S.latency(st, 'exchange_age', 50), S.latency(st, 'exchange_age', 99)],
      updates: () => [count(st.book_updates_per_second), count(st.events_per_second)],
      gaps: () => [isNum(markets.synced) ? count(markets.desired - markets.synced) : null,
        isNum(standby) ? count(markets.desired - standby) : null],
      connections: () => [notOpen('orderbook'), notOpen('market_discovery')],
    }[id];
    const current = live ? live() : [];
    return fromHistory.map((value, i) => current[i] || value);
  }

  /** The historical marker plus exact-time markers from deployment metadata, sorted, one per instant. */
  function deploymentMarkers(extra) {
    const seen = new Set([DEPLOYMENT_AT]);
    const out = [HISTORICAL_MARKER];
    for (const marker of extra || []) {
      if (!marker || !isNum(marker.at) || seen.has(marker.at)) continue;
      seen.add(marker.at);
      out.push({at: marker.at, label: String(marker.label || 'Deployment'), details: String(marker.details || ''), approx: false, url: marker.url || null});
    }
    return out.sort((a, b) => a.at - b.at);
  }

  const inRange = (history, at) => history && isNum(history.start) && isNum(history.end) && history.start <= at && at <= history.end;

  function deploymentInRange(history, markers = [HISTORICAL_MARKER]) {
    return markers.some(marker => inRange(history, marker.at));
  }

  function drawDeploymentMarker(u, history, ratio, markers = [HISTORICAL_MARKER]) {
    const visible = markers.filter(marker => inRange(history, marker.at));
    if (!visible.length) return;
    const {ctx, bbox} = u;
    const top = bbox.top / ratio;
    const bottom = (bbox.top + bbox.height) / ratio;
    ctx.save();
    ctx.scale(ratio, ratio);
    ctx.strokeStyle = C.light;
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 4]);
    for (const marker of visible) {
      const x = u.valToPos(marker.at, 'x', true) / ratio;
      ctx.beginPath();
      ctx.moveTo(x, top);
      ctx.lineTo(x, bottom);
      ctx.stroke();
    }
    ctx.restore();
  }

  /** The marker nearest the cursor within 6 px, or null. */
  function deploymentHovered(u, history, ratio, markers = [HISTORICAL_MARKER]) {
    if (!isNum(u.cursor.left) || !isNum(u.cursor.top) || u.cursor.top < 0 || u.cursor.top > u.bbox.height / ratio) return null;
    let best = null;
    let bestDistance = 6;
    for (const marker of markers) {
      if (!inRange(history, marker.at)) continue;
      const distance = Math.abs(u.cursor.left - u.valToPos(marker.at, 'x'));
      if (distance <= bestDistance) { best = marker; bestDistance = distance; }
    }
    return best;
  }

  function deploymentText(marker = HISTORICAL_MARKER) {
    const when = new Date(marker.at * 1000).toLocaleString(undefined, {month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
      ...(marker.approx ? {} : {second: '2-digit'}), timeZoneName: 'short'});
    const details = marker.details ? `: ${marker.details}` : '';
    return `${marker.label} · ${when}${marker.approx ? ' (approx.)' : ''}${details}${marker.url ? ' Click for details.' : ''}`;
  }

  const overlaps = (history, item) => history && isNum(history.start) && isNum(history.end) && item.start <= history.end && history.start <= item.end;

  /** Shades each window in range (amber for rollouts, red for incidents) and draws solid lines at the edges that fall inside the range. */
  function drawRolloutWindows(u, history, ratio, windows = []) {
    const visible = windows.filter(item => overlaps(history, item));
    if (!visible.length) return;
    const {ctx, bbox} = u;
    const left = bbox.left / ratio;
    const right = (bbox.left + bbox.width) / ratio;
    const top = bbox.top / ratio;
    const bottom = (bbox.top + bbox.height) / ratio;
    ctx.save();
    ctx.scale(ratio, ratio);
    ctx.lineWidth = 1;
    for (const item of visible) {
      const incident = item.kind === 'incident';
      ctx.fillStyle = incident ? C.incidentFill : C.rolloutFill;
      ctx.strokeStyle = incident ? C.incident : C.rollout;
      const a = Math.max(left, u.valToPos(item.start, 'x', true) / ratio);
      const b = Math.min(right, u.valToPos(item.end, 'x', true) / ratio);
      if (b > a) ctx.fillRect(a, top, b - a, bottom - top);
      for (const at of [item.start, item.end]) {
        if (!inRange(history, at)) continue;
        const x = u.valToPos(at, 'x', true) / ratio;
        ctx.beginPath();
        ctx.moveTo(x, top);
        ctx.lineTo(x, bottom);
        ctx.stroke();
      }
    }
    ctx.restore();
  }

  /** The window under the cursor, or null. */
  function rolloutHovered(u, history, ratio, windows = []) {
    if (!isNum(u.cursor.left) || !isNum(u.cursor.top) || u.cursor.top < 0 || u.cursor.top > u.bbox.height / ratio) return null;
    return windows.find(item => overlaps(history, item)
      && u.valToPos(item.start, 'x') <= u.cursor.left && u.cursor.left <= u.valToPos(item.end, 'x')) || null;
  }

  // The band already shows when it happened, so the text carries only the context.
  const rolloutText = item => `${item.details ? `${item.label}. ${item.details}` : item.label}${item.url ? ' Click for details.' : ''}`;

  function chartNote(id, history, stats, markers = [], windows = []) {
    if (id === 'writer_busy') {
      return windows.some(item => /stream/i.test(item.label))
        ? 'File output before the shaded band, stream output after. They do different work, so do not compare across it.' : '';
    }
    if (id === 'updates') return '';
    if (id === 'connections') {
      const ob = S.sockets(stats || {}, 'orderbook');
      const md = S.sockets(stats || {}, 'market_discovery');
      const open = ob && md ? `Open: ${S.count(ob.connected)} of ${S.count(ob.expected)} order-book, ${S.count(md.connected)} of ${S.count(md.expected)} discovery. ` : '';
      return open.trim();
    }
    if (id === 'gaps') return S.booklessNote(stats);
    const b = (history && history.bars && history.bars.series) || {};
    if (id === 'disconnects' && hasData([b['session_ends:emptied'] || []])) {
      const discovery = Object.keys(b).filter(key => key.startsWith('discovery_session_ends:'));
      return `Planned endings in range: ${S.count(sum(b['session_ends:emptied']))} emptied (last market removed), ${S.count(sum(b['session_ends:shutdown']))} shutdown (collector stopped). Discovery socket session ends: ${S.count(discovery.reduce((t, k) => t + sum(b[k]), 0))}.`;
    }
    return null;
  }

  // Rendering.
  function init(doc, win, uPlot, status) {
    const plots = new Map();
    const view = {range: '15m', history: null, stats: null, retained: false, state: null, markers: deploymentMarkers(), windows: []};
    const sections = [...doc.querySelectorAll('[data-chart]')];
    const historyStatus = doc.getElementById('history-status');
    let generation = 0;
    let poller = null;

    function xRange() {
      const h = view.history;
      return h && isNum(h.start) ? [h.start, h.end] : [Date.now() / 1000 - 900, Date.now() / 1000];
    }

    const timeAxis = (u, values) => values.map(value => new Date(value * 1000).toLocaleTimeString(undefined, {hour: '2-digit', minute: '2-digit', hourCycle: 'h23'}));
    const decades = (min, max) => { const out = []; for (let e = Math.floor(Math.log10(min)); 10 ** e <= max * 1.0001; e += 1) out.push(10 ** e); return out; };

    function axisFormat(def) {
      return (u, values) => values.map(value => def.log ? S.duration(value) : (def.axis || def.format)(value));
    }

    function options(id, def, element) {
      let markerTip;
      const big = element.closest('.chart').classList.contains('big');
      const series = def.series.map(item => {
        const base = {label: item.label, stroke: item.color, width: 1.5, spanGaps: false, points: {show: false}};
        if (def.bars) {
          return {...base, width: 0, fill: item.color, paths: uPlot.paths.bars({size: [0.82, 64], align: 0})};
        }
        return base;
      });
      if (def.bars) series.reverse();
      return {
        width: Math.max(240, element.clientWidth), height: big ? 280 : 180,
        legend: {show: false},
        cursor: {sync: {key: 'live'}, drag: {x: false, y: false}, points: {size: 5}},
        scales: {
          x: {time: true, range: () => xRange()},
          y: def.log
            ? {distr: 3, log: 10, range: (u, min, max) => [def.min, Math.max(def.top, isNum(max) ? 10 ** Math.ceil(Math.log10(max * 1.5)) : 0)]}
            : {range: (u, min, max) => [0, def.max || Math.max(def.min0 || 1, isNum(max) ? max * 1.15 : 1)]},
        },
        axes: [
          {stroke: C.axis, grid: {stroke: C.grid, width: 1}, ticks: {stroke: C.grid, width: 1}, font: '12px system-ui, sans-serif', values: timeAxis, space: 70},
          {stroke: C.axis, grid: {stroke: C.grid, width: 1}, ticks: {stroke: C.grid, width: 1}, font: '12px system-ui, sans-serif', size: 62, values: axisFormat(def),
            ...(def.log ? {splits: (u, i, min, max) => decades(min, max)} : {})},
        ],
        series: [{}, ...series],
        hooks: {
          ready: [u => {
            markerTip = doc.createElement('div');
            markerTip.className = 'deployment-marker-tip';
            markerTip.setAttribute('role', 'tooltip');
            markerTip.hidden = true;
            u.over.append(markerTip);
            // A marker or window with a link opens it, for example an exchange status page.
            u.over.addEventListener('click', () => {
              const ratio = uPlot.pxRatio || win.devicePixelRatio || 1;
              const marker = id === 'updates' && deploymentHovered(u, view.history, ratio, view.markers);
              const target = marker || rolloutHovered(u, view.history, ratio, view.windows);
              if (target && target.url) win.open(target.url, '_blank', 'noopener,noreferrer');
            });
          }],
          setData: [() => { if (markerTip) markerTip.hidden = true; }],
          drawAxes: [u => shadeGaps(u, id)],
          draw: [u => {
            const ratio = uPlot.pxRatio || win.devicePixelRatio || 1;
            drawRolloutWindows(u, view.history, ratio, view.windows);
            if (id === 'updates') drawDeploymentMarker(u, view.history, ratio, view.markers);
          }],
          setCursor: [u => {
            renderLegend(id, u.cursor.idx);
            if (!markerTip) return;
            const ratio = uPlot.pxRatio || win.devicePixelRatio || 1;
            const hovering = u.over.matches(':hover');
            const marker = hovering && id === 'updates' && deploymentHovered(u, view.history, ratio, view.markers);
            const rollout = hovering && !marker && rolloutHovered(u, view.history, ratio, view.windows);
            u.over.classList.toggle('marker-link', Boolean((marker || rollout || {}).url));
            markerTip.hidden = !marker && !rollout;
            if (!marker && !rollout) return;
            // Metadata text is untrusted: textContent only.
            const text = marker ? deploymentText(marker) : rolloutText(rollout);
            if (markerTip.textContent !== text) markerTip.textContent = text;
            markerTip.style.left = `${Math.max(4, Math.min(u.cursor.left + 12, u.over.clientWidth - markerTip.offsetWidth - 4))}px`;
          }],
        },
      };
    }

    function xValues(id, def) {
      const h = view.history;
      if (def.bars) {
        const bars = h.bars || {timestamps: [], bucket_seconds: 0};
        return bars.timestamps.map(t => t + bars.bucket_seconds / 2);
      }
      return h.timestamps;
    }

    function data(id, def) {
      const x = xValues(id, def);
      if (def.bars) return [x, ...stack(def.series.map(item => item.values)).reverse()];
      return [x, ...def.series.map(item => item.values)];
    }

    function shadeGaps(u, id) {
      const def = current[id];
      if (!def || !view.history) return;
      const x = u.data[0];
      const ranges = gaps(def.series.map(item => item.values));
      const {ctx} = u;
      ctx.save();
      ctx.fillStyle = C.gap;
      const step = def.bars ? (view.history.bars || {}).bucket_seconds || 0 : view.history.step_seconds || 0;
      for (const [a, b] of ranges) {
        const left = u.valToPos(x[a] - step / 2, 'x', true);
        const right = u.valToPos(x[b] + step / 2, 'x', true);
        ctx.fillRect(Math.max(u.bbox.left, left), u.bbox.top, Math.min(u.bbox.left + u.bbox.width, right) - Math.max(u.bbox.left, left), u.bbox.height);
      }
      ctx.restore();
    }


    let current = {};

    const hover = {};

    function renderLegend(id, index) {
      const section = sections.find(element => element.dataset.chart === id);
      const def = current[id];
      if (!section || !def) return;
      hover[id] = isNum(index) ? index : null;
      const values = hover[id] === null
        ? legendValues(id, def, view.stats)
        : def.series.map(item => isNum(item.values[hover[id]]) ? def.format(item.values[hover[id]]) : null);
      const legend = section.querySelector('[data-legend]');
      const signature = def.series.map(item => item.label).join('|');
      if (legend.dataset.signature !== signature) {
        legend.dataset.signature = signature;
        legend.innerHTML = def.series.map((item, i) => {
          const swatch = `<i class="${SWATCH[item.color] || 'c-grey'}"></i>`;
          const label = item.mono ? `<code>${item.label}</code>` : item.label;
          if (!item.tip) return `<span class="item">${swatch}<span class="name">${label}</span><b></b></span>`;
          const tipId = `tip-${id}-${i}`;
          return `<span class="item has-tip" tabindex="0" aria-describedby="${tipId}">${swatch}<span class="name">${label}</span><b></b>`
            + `<span class="tip" role="tooltip" id="${tipId}">${item.tip}</span></span>`;
        }).join('');
      }
      legend.querySelectorAll('b').forEach((element, i) => {
        element.textContent = values[i] === null ? 'no data' : values[i];
        element.classList.toggle('unavailable', values[i] === null);
      });
    }

    function renderHeaders() {
      for (const section of sections) {
        const id = section.dataset.chart;
        if (hover[id] === null || hover[id] === undefined) renderLegend(id, null);
        section.classList.toggle('retained', view.retained);
        const note = chartNote(id, view.history, view.stats, view.markers, view.windows);
        if (note !== null) section.querySelector('[data-note]').textContent = note;
      }
    }

    function emptyMessage() {
      if (!view.history || !view.history.timestamps) {
        return {api_unavailable: 'History unavailable while the demo API is not responding', offline: 'History unavailable while offline',
          metrics_unavailable: 'History unavailable while Prometheus is not responding'}[view.state] || 'Loading history';
      }
      return view.state === 'stopped' ? 'No data: the collector was not running in this range' : 'No data in this range';
    }

    function renderCharts() {
      const h = view.history;
      current = h && h.timestamps ? definitions(h) : {};
      for (const section of sections) {
        const id = section.dataset.chart;
        const element = section.querySelector('[data-plot]');
        const def = current[id];
        let empty = element.querySelector('.plot-empty');
        if (!empty) {
          empty = doc.createElement('p');
          empty.className = 'plot-empty';
          element.append(empty);
        }
        const show = def && hasData(def.series.map(item => item.values));
        empty.hidden = show;
        empty.textContent = show ? '' : emptyMessage();
        if (!def) continue;
        const signature = `${def.series.length}`;
        const existing = plots.get(id);
        if (existing && existing.signature === signature) {
          existing.plot.setData(data(id, def));
        } else {
          if (existing) existing.plot.destroy();
          plots.set(id, {signature, plot: new uPlot(options(id, def, element), data(id, def), element)});
        }
        renderLegend(id, null);
      }
      renderHeaders();
    }

    function describeHistory() {
      const h = view.history;
      if (!h || !h.timestamps) return emptyMessage();
      if (h.status === 'stale') return 'History could not be refreshed; showing the last successful load.';
      return '';
    }

    function load(range) {
      view.range = range;
      for (const button of doc.querySelectorAll('[data-range]')) button.setAttribute('aria-pressed', String(button.dataset.range === range));
      if (poller) poller.stop();
      const mine = ++generation;
      poller = S.createPoller({
        env: win,
        interval: RANGES[range],
        url: () => `/api/demo/history?window=${range}`,
        onResult(result) {
          if (mine !== generation) return true;
          const ok = result.ok && result.body && result.body.schema_version === 1 && result.body.window === range;
          if (ok && result.body.timestamps) {
            view.history = result.body;
          } else if (ok && !view.history) {
            view.history = null;
          }
          renderCharts();
          historyStatus.textContent = !ok && view.history
            ? 'History could not be refreshed; charts show the last successful load. Retrying.'
            : ok ? describeHistory() : emptyMessage();
          return ok && result.body.status === 'ok';
        },
      });
      poller.start();
    }

    for (const button of doc.querySelectorAll('[data-range]')) {
      button.addEventListener('click', () => {
        if (button.dataset.range === view.range) return;
        view.history = null;
        for (const {plot} of plots.values()) plot.destroy();
        plots.clear();
        load(button.dataset.range);
      });
    }

    status.subscribe(snap => {
      const changed = snap.state !== view.state;
      view.stats = snap.stats;
      view.retained = snap.retained;
      view.state = snap.state;
      renderHeaders();
      if (changed && poller) {
        renderCharts();
        if (snap.state === 'live' || snap.state === 'degraded') poller.now();
      }
    });

    const deployments = root.DemoDeployment && root.DemoDeployment.page;
    if (deployments) {
      deployments.subscribe(state => {
        const next = deploymentMarkers(state.markers);
        const windows = state.windows || [];
        if (JSON.stringify(next) === JSON.stringify(view.markers) && JSON.stringify(windows) === JSON.stringify(view.windows)) return;
        view.markers = next;
        view.windows = windows;
        renderHeaders();
        for (const {plot} of plots.values()) plot.redraw(false, false);
      });
    }

    if (win.ResizeObserver) {
      new win.ResizeObserver(() => {
        for (const [id, {plot}] of plots) {
          const element = sections.find(section => section.dataset.chart === id).querySelector('[data-plot]');
          plot.setSize({width: Math.max(240, element.clientWidth), height: element.closest('.chart').classList.contains('big') ? 280 : 180});
        }
      }).observe(doc.getElementById('charts'));
    }
    load('15m');
    return view;
  }

  root.DemoCharts = {stack, gaps, diff, definitions, legendValues, chartNote, deploymentMarkers, deploymentInRange, drawDeploymentMarker, deploymentHovered, deploymentText,
    drawRolloutWindows, rolloutHovered, rolloutText};
  if (root.document && root.document.getElementById && root.document.getElementById('charts') && root.uPlot && S.page) {
    root.DemoCharts.page = init(root.document, root, root.uPlot, S.page);
  }
})(typeof window !== 'undefined' ? window : globalThis);
