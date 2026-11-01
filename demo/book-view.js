'use strict';
// Browser order books use the same fixed-point SBE records as the file reader.
(function (root) {
  const SCALE = 1000000n;
  const MARKET_PAGE_SIZE = 20;
  const volumeFormat = new Intl.NumberFormat('en-US', {style: 'currency', currency: 'USD'});
  const GAMMA = 'https://gamma-api.polymarket.com';
  const ICON_ORIGIN = 'https://polymarket-upload.s3.us-east-2.amazonaws.com';
  const textDecoder = new TextDecoder('utf-8', {fatal: true});

  function fixed(value) {
    const negative = value < 0n;
    const abs = negative ? -value : value;
    const fraction = (abs % SCALE).toString().padStart(6, '0').replace(/0+$/, '');
    return `${negative ? '-' : ''}${abs / SCALE}${fraction ? `.${fraction}` : ''}`;
  }

  function decodeFrame(buffer) {
    const view = new DataView(buffer);
    let at = 0;
    const records = [];
    function need(bytes) { if (at + bytes > view.byteLength) throw new Error('Truncated book data'); }
    function symbol() {
      need(4);
      const length = view.getUint32(at, true); at += 4;
      if (length > 100) throw new Error('Invalid market data');
      need(length);
      const value = textDecoder.decode(new Uint8Array(buffer, at, length)); at += length;
      if (!/^\d+$/.test(value)) throw new Error('Invalid market data');
      return value;
    }
    function levels() {
      need(4);
      const block = view.getUint16(at, true), count = view.getUint16(at + 2, true); at += 4;
      if (block !== 16) throw new Error('Unsupported book data');
      need(count * block);
      const result = [];
      for (let i = 0; i < count; i += 1) {
        const price = view.getBigInt64(at, true), quantity = view.getBigInt64(at + 8, true); at += block;
        if (price < 0n || quantity < 0n) throw new Error('Invalid market data');
        result.push([price, quantity]);
      }
      return result;
    }
    while (at < view.byteLength) {
      need(8);
      const block = view.getUint16(at, true), template = view.getUint16(at + 2, true);
      if (view.getUint16(at + 4, true) !== 1 || view.getUint16(at + 6, true) !== 2) throw new Error('Unsupported book data');
      at += 8;
      need(block);
      if (template === 1 && block === 16) {
        at += block;
        const bids = levels(), asks = levels();
        records.push({type: 'snapshot', bids, asks, symbol: symbol()});
      } else if (template === 2 && block === 33) {
        const price = view.getBigInt64(at + 16, true), quantity = view.getBigInt64(at + 24, true), side = view.getUint8(at + 32);
        if (price < 0n || quantity < 0n || side > 1) throw new Error('Invalid market data');
        at += block;
        records.push({type: 'update', price, quantity, side, symbol: symbol()});
      } else throw new Error('Unsupported book data');
    }
    return records;
  }

  function emptyBook() { return {ready: false, snapshotAt: null, bids: new Map(), asks: new Map()}; }
  function apply(book, record) {
    if (record.type === 'snapshot') {
      book.bids = new Map(record.bids.filter(([, qty]) => qty !== 0n));
      book.asks = new Map(record.asks.filter(([, qty]) => qty !== 0n));
      book.ready = true;
    } else if (book.ready) {
      const side = record.side === 0 ? book.bids : book.asks;
      if (record.quantity === 0n) side.delete(record.price); else side.set(record.price, record.quantity);
    }
  }
  function sortedLevels(book, side) {
    return [...book[side]].sort(([a], [b]) => a === b ? 0 : (a < b ? -1 : 1) * (side === 'bids' ? -1 : 1));
  }

  function array(value) {
    try { const result = typeof value === 'string' ? JSON.parse(value) : value; return Array.isArray(result) ? result : []; }
    catch (_) { return []; }
  }
  function volume24hr(value) {
    if (typeof value !== 'number' && !(typeof value === 'string' && /^\d+(?:\.\d+)?$/.test(value))) return null;
    const amount = Number(value);
    return Number.isFinite(amount) && amount >= 0 ? amount : null;
  }
  function rankMarkets(entries) {
    return entries.slice().sort((a, b) => {
      const av = a.market?.volume24hr ?? null, bv = b.market?.volume24hr ?? null;
      if (av !== null && bv === null) return -1;
      if (av === null && bv !== null) return 1;
      if (av !== bv) return bv - av;
      if (Boolean(a.market) !== Boolean(b.market)) return a.market ? -1 : 1;
      return a.id.localeCompare(b.id);
    });
  }
  function marketPage(entries, requestedPage) {
    const pages = Math.max(1, Math.ceil(entries.length / MARKET_PAGE_SIZE));
    const page = Math.max(0, Math.min(pages - 1, requestedPage));
    const start = page * MARKET_PAGE_SIZE;
    return {page, pages, start, total: entries.length, items: entries.slice(start, start + MARKET_PAGE_SIZE)};
  }
  function metadata(raw) {
    if (!raw || !/^0x[0-9a-fA-F]{64}$/.test(raw.conditionId || '')) return null;
    const outcomes = array(raw.outcomes), tokens = array(raw.clobTokenIds), prices = array(raw.outcomePrices);
    if (outcomes.length !== 2 || !outcomes.every(s => typeof s === 'string') || tokens.length !== 2 || !tokens.every(s => typeof s === 'string' && /^\d{1,100}$/.test(s)) || tokens[0] === tokens[1]) return null;
    let icon = null;
    try { const url = new URL(raw.icon); if (url.origin === ICON_ORIGIN) icon = url.href; } catch (_) { /* Optional image. */ }
    const end = typeof raw.endDate === 'string' && Number.isFinite(Date.parse(raw.endDate)) ? raw.endDate : null;
    return {id: raw.conditionId.toLowerCase(), question: typeof raw.question === 'string' ? raw.question : raw.conditionId,
      outcomes, tokens, volume24hr: volume24hr(raw.volume24hr), prices: prices.map(p => typeof p === 'string' && /^\d+(\.\d+)?$/.test(p) ? p : null), icon, end};
  }

  // Every new session discards its baselines. A late callback from an older session is ignored.
  function createFeed(env, url, tokens, changed) {
    let socket = null, timer = null, stopped = false, failures = 0, decodeFailures = 0;
    const state = {status: 'connecting', books: new Map()};
    function reset() { state.books = new Map(tokens.map(token => [token, emptyBook()])); }
    function connect() {
      if (stopped) return;
      reset(); state.status = 'connecting'; changed(state);
      const current = new env.WebSocket(url); socket = current; current.binaryType = 'arraybuffer';
      current.onopen = () => {
        if (stopped || socket !== current) return;
        state.status = 'waiting'; changed(state);
        current.send(JSON.stringify({op: 'subscribe', books: tokens}));
      };
      current.onmessage = event => {
        if (stopped || socket !== current) return;
        try {
          const records = decodeFrame(event.data);
          for (const record of records) {
            const book = state.books.get(record.symbol);
            if (book) {
              apply(book, record);
              if (record.type === 'snapshot') book.snapshotAt = (env.Date || Date).now();
            }
          }
          state.status = [...state.books.values()].every(book => book.ready) ? 'live' : 'waiting';
          if (state.status === 'live') { failures = 0; decodeFailures = 0; }
          changed(state);
        } catch (_) {
          reset(); decodeFailures += 1;
          state.status = decodeFailures >= 2 ? 'incompatible' : 'invalid'; changed(state);
          current.onmessage = null; current.close();
        }
      };
      current.onclose = event => {
        if (stopped || socket !== current) return;
        reset();
        if (decodeFailures >= 2) { state.status = 'incompatible'; changed(state); return; }
        if ([1008, 1003].includes(event.code)) { state.status = 'rejected'; changed(state); return; }
        state.status = 'reconnecting'; changed(state);
        timer = env.setTimeout(connect, Math.min(30000, 1000 * 2 ** Math.min(failures++, 5)));
      };
    }
    reset(); connect();
    return {state, stop() { stopped = true; env.clearTimeout(timer); if (socket) { socket.onmessage = null; socket.close(); } }};
  }

  function feedStatus(state, now) {
    if (state.status !== 'live') return state.status;
    return [...state.books.values()].some(book => book.snapshotAt === null || now - book.snapshotAt > 135000) ? 'stale' : 'live';
  }

  function mount(doc, env, dialog, demo) {
    const list = dialog.querySelector('[data-market-list]'), note = dialog.querySelector('[data-market-status]');
    const detail = dialog.querySelector('[data-book-detail]'), title = dialog.querySelector('[data-book-title]');
    const bookStatus = dialog.querySelector('[data-book-status]'), outcomes = dialog.querySelector('[data-book-outcomes]');
    const retry = dialog.querySelector('[data-market-retry]');
    let generation = 0, abort = null, feed = null, renderTimer = null, demoTimer = null, freshnessTimer = null, selectedMarket = null, currentId = null, stream = null;
    const make = (tag, cls, text) => { const node = doc.createElement(tag); if (cls) node.className = cls; if (text !== undefined) node.textContent = text; return node; };
    let marketEntries = [], page = 0, loading = false;
    const pager = make('div', 'pager'), previous = make('button', 'control', 'Previous'), next = make('button', 'control', 'Next');
    const pageText = make('p', 'small muted');
    previous.type = next.type = 'button';
    previous.dataset.marketPrevious = ''; next.dataset.marketNext = ''; pageText.dataset.marketPage = '';
    pageText.setAttribute('aria-live', 'polite');
    pager.append(previous, pageText, next); list.after(pager); pager.hidden = true;
    function renderMarketPage() {
      const slice = marketPage(marketEntries, page); page = slice.page;
      list.replaceChildren(...slice.items.map(entry => entry.button)); list.scrollTop = 0;
      pager.hidden = slice.total <= MARKET_PAGE_SIZE;
      previous.disabled = loading || page === 0; next.disabled = loading || page === slice.pages - 1;
      pageText.textContent = slice.total ? `${slice.start + 1} to ${slice.start + slice.items.length} of ${slice.total} markets` : '';
    }
    previous.addEventListener('click', () => { page -= 1; renderMarketPage(); });
    next.addEventListener('click', () => { page += 1; renderMarketPage(); });
    function stopBook() {
      if (feed) feed.stop(); feed = null;
      env.clearTimeout(renderTimer); renderTimer = null;
      env.clearInterval(demoTimer); demoTimer = null;
      env.clearInterval(freshnessTimer); freshnessTimer = null;
    }
    function close() { ++generation; if (abort) abort.abort(); stopBook(); selectedMarket = null; }
    function setText(node, text) { if (node.textContent !== text) node.textContent = text; }
    async function request(url, signal) {
      const controller = new env.AbortController();
      const cancel = () => controller.abort();
      if (signal.aborted) cancel(); else signal.addEventListener('abort', cancel, {once: true});
      const timeout = env.setTimeout(cancel, 15000);
      try {
        const response = await env.fetch(url, {signal: controller.signal});
        return {ok: response.ok, status: response.status, body: response.ok ? await response.json() : null};
      } finally {
        env.clearTimeout(timeout); signal.removeEventListener('abort', cancel);
      }
    }
    async function json(url, signal) {
      const response = await request(url, signal);
      if (!response.ok) throw new Error('Request failed');
      return response.body;
    }
    function freshness(market, state) {
      const now = env.Date.now();
      const messages = {connecting: 'Connecting to live order books…', waiting: 'Waiting for a snapshot. This may take up to 60 seconds.',
        live: demo ? 'Synthetic live order books' : 'Live order books', stale: 'Snapshots are delayed. Showing the last known orders.',
        reconnecting: 'Connection interrupted. Reconnecting and waiting for fresh snapshots…',
        rejected: 'Live viewing is unavailable for this market. Select it again to retry.', invalid: 'Book data could not be read. Reconnecting…',
        incompatible: 'Book data is not compatible with this page. Reload the page before trying again.'};
      setText(bookStatus, messages[feedStatus(state, now)]);
      market.tokens.forEach((token, i) => {
        const age = outcomes.children[i]?.querySelector('[data-snapshot-age]'), book = state.books.get(token);
        if (age) setText(age, book?.snapshotAt === null || !book ? '' : `Last snapshot ${Math.max(0, Math.floor((now - book.snapshotAt) / 1000))}s ago`);
      });
    }
    function renderLevels(body, levels) {
      const key = levels.map(([p, q]) => `${p}:${q}`).join(',');
      if (body.dataset.levels === key) return;
      body.dataset.levels = key;
      if (!levels.length) { const row = make('tr'), cell = make('td', 'muted', 'No orders'); cell.colSpan = 2; row.append(cell); body.replaceChildren(row); return; }
      if (body.firstElementChild?.firstElementChild?.colSpan === 2) body.replaceChildren();
      while (body.children.length > levels.length) body.lastElementChild.remove();
      levels.forEach(([price, qty], i) => {
        let row = body.children[i];
        if (!row) { row = make('tr'); row.append(make('td'), make('td')); body.append(row); }
        setText(row.children[0], fixed(price)); setText(row.children[1], fixed(qty));
      });
    }
    function renderBooks(market, state) {
      market.tokens.forEach((token, i) => {
        const book = state.books.get(token);
        let section = outcomes.children[i];
        if (!section) {
          section = make('section', 'outcome-book');
          section.append(make('h4', '', market.outcomes[i]));
          const age = make('p', 'small muted'); age.dataset.snapshotAge = ''; age.setAttribute('aria-live', 'off');
          section.append(age); outcomes.append(section);
        }
        if (!book || !book.ready) {
          section.querySelector('.book-sides')?.remove();
          if (!section.querySelector('.book-waiting')) section.append(make('p', 'muted book-waiting', 'Waiting for a snapshot…'));
          return;
        }
        section.querySelector('.book-waiting')?.remove();
        let sides = section.querySelector('.book-sides');
        if (!sides) {
          sides = make('div', 'book-sides');
          for (const side of ['bids', 'asks']) {
            const table = make('table', `depth-table ${side}`);
            table.append(make('caption', '', side === 'bids' ? 'Bids' : 'Asks'));
            const head = make('thead'), row = make('tr');
            for (const label of ['Price (USDC)', 'Shares']) { const th = make('th', '', label); th.scope = 'col'; row.append(th); }
            head.append(row); table.append(head, make('tbody')); sides.append(table);
          }
          section.append(sides);
        }
        for (const side of ['bids', 'asks']) renderLevels(sides.querySelector(`.${side} tbody`), sortedLevels(book, side).slice(0, 20));
      });
      freshness(market, state);
    }
    function select(market) {
      stopBook(); selectedMarket = market; detail.hidden = false; title.textContent = market.question;
      outcomes.replaceChildren(); setText(bookStatus, 'Connecting to live order books…');
      detail.scrollIntoView({block: 'nearest'});
      if (demo) {
        const state = {status: 'live', books: new Map(market.tokens.map(token => [token, emptyBook()]))};
        let tick = 0;
        const update = () => {
          market.tokens.forEach((token, i) => apply(state.books.get(token), {type: 'snapshot', bids: [[450000n + BigInt(i * 50000), 10000000n + BigInt(tick) * 1000000n]], asks: [[550000n + BigInt(i * 50000), 15000000n]]}));
          for (const book of state.books.values()) book.snapshotAt = env.Date.now();
          tick += 1; renderBooks(market, state);
        };
        update(); demoTimer = env.setInterval(update, 1500); return;
      }
      const mine = generation;
      try {
        feed = createFeed(env, stream, market.tokens, state => {
          if (mine !== generation || renderTimer !== null) return;
          renderTimer = env.setTimeout(() => { renderTimer = null; if (mine === generation) renderBooks(market, state); }, 100);
        });
        freshnessTimer = env.setInterval(() => { if (feed) freshness(market, feed.state); }, 1000);
      } catch (_) { bookStatus.textContent = 'Unable to connect. Select the market again to retry.'; }
    }
    function fill(button, market) {
      button.replaceChildren(); button.removeAttribute('aria-busy');
      if (market.icon) { const img = make('img'); img.src = market.icon; img.alt = ''; img.loading = 'lazy'; img.referrerPolicy = 'no-referrer'; button.append(img); }
      const copy = make('span', 'market-copy'); copy.append(make('strong', '', market.question));
      const prices = market.outcomes.map((outcome, i) => `${outcome}${market.prices[i] ? ` ${market.prices[i]} USDC` : ''}`).join(' · ');
      copy.append(make('span', 'small muted', prices));
      copy.append(make('span', 'small muted market-volume', market.volume24hr === null ? '24h volume unavailable' : `24h volume ${volumeFormat.format(market.volume24hr)}`));
      if (market.end) copy.append(make('span', 'small muted', `Ends ${new Date(market.end).toLocaleDateString()}`));
      button.append(copy); button.disabled = false; button.addEventListener('click', () => select(market));
    }
    async function open(id) {
      close(); const mine = generation; currentId = id; abort = new env.AbortController();
      const signal = abort.signal;
      detail.hidden = true; marketEntries = []; page = 0; loading = true; renderMarketPage();
      list.setAttribute('aria-busy', 'true'); retry.hidden = true; note.textContent = 'Loading markets…';
      try {
        let ids;
        if (demo) ids = [1, 2, 3].map(n => `0x${(id * 10 + n).toString(16).padStart(64, '0')}`);
        else {
          const config = await json('/api/demo/config', signal);
          if (mine !== generation) return;
          if (config.stream_origin === null) { note.textContent = 'Live order books are not enabled on this deployment.'; return; }
          if (typeof config.stream_origin !== 'string' || !/^wss:\/\/[A-Za-z0-9.-]+(?::\d{1,5})?$/.test(config.stream_origin)) throw new Error('Stream unavailable');
          stream = config.stream_origin;
          const response = await request(`${stream.replace('wss:', 'https:')}/sockets/${id}/markets`, signal);
          if (mine !== generation) return;
          if (response.status === 404) { note.textContent = 'No markets published for this socket yet.'; retry.hidden = false; return; }
          if (!response.ok) throw new Error('Directory unavailable');
          const body = response.body; ids = body.markets;
          if (!Array.isArray(ids) || ids.length > 512 || !ids.every(value => typeof value === 'string' && /^0x[0-9a-fA-F]{64}$/.test(value))) throw new Error('Invalid directory');
          ids = [...new Set(ids.map(id => id.toLowerCase()))];
        }
        if (mine !== generation) return;
        note.textContent = ids.length ? `Loading details and 24h volume for ${ids.length} markets…` : 'No markets on this socket.';
        const buttons = new Map(ids.map(id => {
          const button = make('button', 'market-row'); button.type = 'button'; button.disabled = true; button.setAttribute('aria-busy', 'true');
          button.append(make('span', 'market-spinner'), make('span', 'market-copy mono', id)); return [id, button];
        }));
        marketEntries = ids.map(id => ({id, button: buttons.get(id), market: null}));
        const entriesById = new Map(marketEntries.map(entry => [entry.id, entry]));
        renderMarketPage();
        if (demo) {
          ids.forEach((id, i) => {
            const market = {id, question: ['Will this sample market settle Yes?', 'A second sample market', 'Another market on this socket'][i],
              outcomes: ['Yes', 'No'], prices: ['0.45', '0.55'], tokens: [`${i + 1}1`, `${i + 1}2`], volume24hr: [12000, 88000, 0][i], icon: null, end: null};
            entriesById.get(id).market = market; fill(buttons.get(id), market);
          }); return;
        }
        for (let start = 0; start < ids.length; start += 40) {
          const batch = ids.slice(start, start + 40), params = new URLSearchParams({limit: String(Math.min(40, ids.length - start)), closed: 'any'});
          params.append('version', 'v1'); params.append('version', 'v2');
          for (const id of batch) params.append('condition_ids', id);
          try {
            const rows = await json(`${GAMMA}/markets?${params}`, signal);
            if (mine !== generation) return;
            if (!Array.isArray(rows)) throw new Error('Invalid metadata');
            const found = new Map(rows.map(metadata).filter(Boolean).map(market => [market.id, market]));
            for (const id of batch) {
              const market = found.get(id), button = buttons.get(id);
              if (market) { entriesById.get(id).market = market; fill(button, market); button.disabled = true; }
              else { button.removeAttribute('aria-busy'); button.textContent = `${id} — Details unavailable`; }
            }
          } catch (error) {
            if (mine !== generation) return;
            for (const id of batch) { const button = buttons.get(id); button.removeAttribute('aria-busy'); button.textContent = `${id} — Details unavailable`; }
          }
        }
        note.textContent = ids.length ? `${ids.length} markets, highest 24h volume first. Select a market to watch its order books.` : 'No markets on this socket.';
        retry.hidden = false;
      } catch (_) {
        if (mine !== generation) return;
        note.textContent = 'Markets could not be loaded. Try again.'; retry.hidden = false;
      } finally {
        if (mine === generation) {
          loading = false; marketEntries = rankMarkets(marketEntries);
          for (const entry of marketEntries) if (entry.market) entry.button.disabled = false;
          renderMarketPage(); list.removeAttribute('aria-busy');
          if (demo && marketEntries.length) note.textContent = `${marketEntries.length} markets · Synthetic demo. Highest 24h volume first.`;
        }
      }
    }
    doc.addEventListener('visibilitychange', () => {
      if (!selectedMarket) return;
      if (doc.visibilityState === 'hidden') { stopBook(); outcomes.replaceChildren(); setText(bookStatus, 'Live viewing is paused while this tab is hidden.'); }
      else select(selectedMarket);
    });
    retry.addEventListener('click', () => open(currentId));
    return {open, close};
  }
  root.DemoBooks = {fixed, decodeFrame, emptyBook, apply, sortedLevels, metadata, volume24hr, rankMarkets, marketPage, MARKET_PAGE_SIZE, createFeed, feedStatus, mount};
})(typeof window !== 'undefined' ? window : globalThis);
