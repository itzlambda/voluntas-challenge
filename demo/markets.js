'use strict';
// The Markets page: Polymarket's busiest markets and search, each with its live book from the stream.
(function (root) {
  const TOP_LIMIT = 10;
  // Gamma may return markets the collector skips; ask for extra so ten remain.
  const TOP_REQUEST = 20;
  const SEARCH_DELAY = 300;
  const MIN_QUERY = 2;
  const PER_EVENT = 5;
  const CONDITION = /^0x[0-9a-fA-F]{64}$/;

  function compactVolume(value) {
    if (value === null || value === undefined) return '';
    if (value >= 1e6) return `$${(value / 1e6).toFixed(2)}M`;
    if (value >= 1e3) return `$${Math.round(value / 1e3)}K`;
    return `$${Math.round(value)}`;
  }

  // Top markets in Gamma's order, keeping only those the collector streams.
  function topMarkets(B, rows) {
    if (!Array.isArray(rows)) return null;
    return rows.filter(B.collectable).map(B.metadata).slice(0, TOP_LIMIT);
  }

  // Search returns events with their markets. Keep the collected ones in Polymarket's order.
  function searchGroups(B, body) {
    if (!body || !Array.isArray(body.events)) return null;
    let hidden = 0;
    const groups = [];
    for (const event of body.events) {
      if (!event || !Array.isArray(event.markets)) continue;
      const title = typeof event.title === 'string' ? event.title : '';
      const markets = [];
      for (const raw of event.markets) {
        if (!B.collectable(raw)) { hidden += 1; continue; }
        markets.push(B.metadata({...raw, events: [{title}]}));
      }
      if (markets.length) groups.push({event: title, markets});
    }
    return {groups, hidden, total: groups.reduce((sum, group) => sum + group.markets.length, 0)};
  }

  function marketParam(search) {
    const value = new URLSearchParams(search).get('market');
    return value && CONDITION.test(value) ? value.toLowerCase() : null;
  }

  function mount(doc, env, B) {
    const page = doc.getElementById('markets');
    const q = name => page.querySelector(`[data-markets-${name}]`);
    const input = q('search'), clearButton = q('clear'), heading = q('heading'), status = q('status'), list = q('list');
    const detail = page.querySelector('[data-book-detail]'), emptyText = detail.querySelector('[data-book-empty]');
    const make = (tag, cls, text) => { const node = doc.createElement(tag); if (cls) node.className = cls; if (text !== undefined) node.textContent = text; return node; };
    let stream = null, streamReady = null, top = null, topAt = null, topProblem = null, searchAbort = null, searchTimer = null, generation = 0;
    const panel = B.bookPanel(doc, env, detail, {streamOrigin: () => stream});

    function setStatus(text, problem) { status.textContent = text; status.classList.toggle('lookup-problem', Boolean(problem)); }
    function paintSelection() {
      const id = panel.selected()?.id;
      for (const button of list.querySelectorAll('.market-row')) button.setAttribute('aria-pressed', String(button.dataset.id === id));
    }
    async function select(market) {
      try { await streamReady; } catch (_) { /* Reported below. */ }
      if (stream === null) {
        panel.clear(); emptyText.textContent = 'Live order books are not enabled on this deployment.'; return;
      }
      panel.select(market); paintSelection();
      const url = new URL(env.location.href); url.searchParams.set('market', market.id);
      env.history.replaceState(null, '', `${url.pathname}${url.search}`);
    }
    function button(market, options) {
      const row = make('button', 'market-row'); row.type = 'button'; row.dataset.id = market.id;
      row.replaceChildren(...B.marketRow(doc, market, {...options, volume: compactVolume(market.volume24hr)}));
      row.title = market.question;
      row.setAttribute('aria-pressed', String(panel.selected()?.id === market.id));
      row.addEventListener('click', () => select(market));
      return row;
    }
    function showTop() {
      heading.textContent = 'Top 10 by 24h volume';
      if (topProblem) { setStatus(B.LOOKUP_PROBLEMS[topProblem], true); list.replaceChildren(); return; }
      if (!top) { setStatus('Loading markets…'); list.replaceChildren(); return; }
      setStatus(top.length ? `As of ${new Date(topAt).toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'})}` : 'No open markets found.');
      list.replaceChildren(...top.map((market, i) => button(market, {rank: i + 1})));
    }
    function showResults(query, result) {
      heading.textContent = `Results for “${query}”`;
      const nodes = [];
      for (const group of result.groups) {
        const block = make('div', 'market-group');
        block.append(make('p', 'market-group-title', group.event));
        const rows = group.markets.map(market => button(market, {label: group.markets.length > 1 && market.label ? market.label : null}));
        block.append(...rows.slice(0, PER_EVENT));
        if (rows.length > PER_EVENT) {
          const more = make('button', 'market-more', `Show ${rows.length - PER_EVENT} more`); more.type = 'button';
          more.addEventListener('click', () => { more.replaceWith(...rows.slice(PER_EVENT)); });
          block.append(more);
        }
        nodes.push(block);
      }
      const count = result.total === 1 ? '1 open market' : `${result.total} open markets`;
      const hidden = result.hidden ? ` ${result.hidden} closed or unsupported ${result.hidden === 1 ? 'market' : 'markets'} hidden.` : '';
      setStatus(result.total ? `${count}.${hidden}` : `No open markets match.${hidden}`);
      list.replaceChildren(...nodes);
    }
    async function search(query) {
      const mine = ++generation;
      if (searchAbort) searchAbort.abort();
      searchAbort = new env.AbortController();
      heading.textContent = `Results for “${query}”`; setStatus('Searching…'); list.setAttribute('aria-busy', 'true');
      const params = new URLSearchParams({q: query, limit_per_type: '10', events_status: 'active'});
      try {
        const result = searchGroups(B, (await B.fetchJson(env, `${B.GAMMA}/public-search?${params}`, searchAbort.signal)).body);
        if (mine !== generation) return;
        if (!result) { const error = new Error('Invalid search'); error.kind = 'failed'; throw error; }
        showResults(query, result);
      } catch (error) {
        if (mine !== generation) return;
        setStatus(B.LOOKUP_PROBLEMS[error.kind || 'failed'], true); list.replaceChildren();
      } finally {
        if (mine === generation) list.removeAttribute('aria-busy');
      }
    }
    function onInput() {
      const query = input.value.trim();
      clearButton.hidden = !input.value;
      env.clearTimeout(searchTimer);
      if (query.length < MIN_QUERY) { ++generation; if (searchAbort) searchAbort.abort(); list.removeAttribute('aria-busy'); showTop(); return; }
      searchTimer = env.setTimeout(() => search(query), SEARCH_DELAY);
    }
    input.addEventListener('input', onInput);
    input.addEventListener('keydown', event => { if (event.key === 'Escape' && input.value) { input.value = ''; onInput(); } });
    clearButton.addEventListener('click', () => { input.value = ''; onInput(); input.focus(); });

    async function loadTop() {
      const params = new URLSearchParams({limit: String(TOP_REQUEST), closed: 'false', active: 'true', order: 'volume24hr', ascending: 'false'});
      const abort = new env.AbortController();
      try {
        top = topMarkets(B, (await B.fetchJson(env, `${B.GAMMA}/markets?${params}`, abort.signal)).body);
        if (!top) { const error = new Error('Invalid markets'); error.kind = 'failed'; throw error; }
        topAt = env.Date.now();
      } catch (error) { topProblem = error.kind || 'failed'; }
      if (input.value.trim().length < MIN_QUERY) showTop();
    }
    // A shared link opens its market directly. Closed or uncollected markets explain why there is no book.
    async function loadLinked(id) {
      const params = new URLSearchParams({limit: '1', closed: 'any', condition_ids: id});
      params.append('version', 'v1'); params.append('version', 'v2');
      try {
        const rows = (await B.fetchJson(env, `${B.GAMMA}/markets?${params}`, new env.AbortController().signal)).body;
        const raw = Array.isArray(rows) ? rows.find(row => (row.conditionId || '').toLowerCase() === id) : null;
        if (raw && B.collectable(raw)) { await select(B.metadata(raw)); return; }
        emptyText.textContent = raw ? 'This market is closed or not collected, so it has no live book.' : 'This market was not found on Polymarket.';
      } catch (error) {
        emptyText.textContent = B.LOOKUP_PROBLEMS[error.kind || 'failed'];
      }
    }
    streamReady = B.fetchJson(env, '/api/demo/config', new env.AbortController().signal).then(response => {
      const origin = response.body?.stream_origin;
      stream = typeof origin === 'string' && /^wss:\/\/[A-Za-z0-9.-]+(?::\d{1,5})?$/.test(origin) ? origin : null;
      if (stream === null) emptyText.textContent = 'Live order books are not enabled on this deployment.';
    }, () => { stream = null; emptyText.textContent = 'Live order books could not be configured. Reload the page to retry.'; });
    showTop();
    loadTop();
    const linked = marketParam(env.location.search);
    if (linked) loadLinked(linked);
    return {select, search: onInput};
  }

  root.DemoMarkets = {TOP_LIMIT, PER_EVENT, compactVolume, topMarkets, searchGroups, marketParam, mount};
  if (root.document && root.document.getElementById && root.document.getElementById('markets') && root.DemoBooks) {
    root.DemoMarkets.page = mount(root.document, root, root.DemoBooks);
  }
})(typeof window !== 'undefined' ? window : globalThis);
