'use strict';
// Browser order books use the same fixed-point SBE records as the file reader.
(function (root) {
  const SCALE = 1000000n;
  const MARKET_PAGE_SIZE = 10;
  const LADDER_DEPTH = 10;
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

  // Display helpers stay in fixed point: cents and shares are exact decimal strings.
  function cents(price) { return `${fixed(price * 100n)}¢`; }
  function shares(quantity) {
    const [whole, fraction] = fixed(quantity).split('.');
    return whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',') + (fraction ? `.${fraction}` : '');
  }
  // Midpoint as a whole-tenth percentage and the spread in cents, or null when a side is empty.
  function midSpread(book) {
    const bid = sortedLevels(book, 'bids')[0], ask = sortedLevels(book, 'asks')[0];
    if (!bid || !ask) return null;
    const tenths = ((bid[0] + ask[0]) * 500n + 500000n) / 1000000n;
    return {chance: `${tenths / 10n}${tenths % 10n ? `.${tenths % 10n}` : ''}%`, spread: cents(ask[0] - bid[0])};
  }
  // Gamma's quoted price as a chance, the way Polymarket lists markets.
  function chance(price) {
    if (price === null || price === undefined) return '';
    const value = Number(price);
    if (!Number.isFinite(value)) return '';
    if (value > 0 && value < 0.01) return '<1%';
    if (value < 1 && value > 0.99) return '>99%';
    return `${Math.round(value * 100)}%`;
  }

  function array(value) {
    try { const result = typeof value === 'string' ? JSON.parse(value) : value; return Array.isArray(result) ? result : []; }
    catch (_) { return []; }
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
      outcomes, tokens, prices: prices.map(p => typeof p === 'string' && /^\d+(\.\d+)?$/.test(p) ? p : null), icon, end};
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
    const retry = dialog.querySelector('[data-market-retry]'), empty = dialog.querySelector('[data-book-empty]');
    const body = dialog.querySelector('[data-book-body]'), icon = dialog.querySelector('[data-book-icon]');
    const title = dialog.querySelector('[data-book-title]'), toggle = dialog.querySelector('[data-book-outcomes]');
    const bookStatus = dialog.querySelector('[data-book-status]'), ladder = dialog.querySelector('[data-book-ladder]');
    const meta = dialog.querySelector('[data-book-meta]');
    const make = (tag, cls, text) => { const node = doc.createElement(tag); if (cls) node.className = cls; if (text !== undefined) node.textContent = text; return node; };
    let generation = 0, abort = null, feed = null, renderTimer = null, demoTimer = null, freshnessTimer = null;
    let selectedMarket = null, side = 0, view = null, currentId = null, stream = null;
    let ids = [], page = 0, buttons = new Map(), details = new Map(), pending = new Set();
    const pager = make('div', 'pager'), previous = make('button', 'control', 'Previous'), next = make('button', 'control', 'Next');
    const pageText = make('p', 'small muted');
    previous.type = next.type = 'button';
    previous.dataset.marketPrevious = ''; next.dataset.marketNext = ''; pageText.dataset.marketPage = '';
    pageText.setAttribute('aria-live', 'polite');
    pager.append(previous, pageText, next); list.after(pager); pager.hidden = true;

    function setText(node, text) { if (node.textContent !== text) node.textContent = text; }
    function stopBook() {
      if (feed) feed.stop(); feed = null; view = null;
      env.clearTimeout(renderTimer); renderTimer = null;
      env.clearInterval(demoTimer); demoTimer = null;
      env.clearInterval(freshnessTimer); freshnessTimer = null;
    }
    function showEmpty() { body.hidden = true; empty.hidden = false; }
    function close() { ++generation; if (abort) abort.abort(); stopBook(); selectedMarket = null; showEmpty(); }
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

    // Market rows. Details load one page at a time and are kept while the popup stays on this socket.
    function row(id) {
      let button = buttons.get(id);
      if (button) return button;
      button = make('button', 'market-row'); button.type = 'button'; button.dataset.id = id;
      button.addEventListener('click', () => { const market = details.get(id); if (market) select(market); });
      buttons.set(id, button); paint(id); return button;
    }
    function paint(id) {
      const button = buttons.get(id);
      if (!button) return;
      const market = details.get(id);
      button.setAttribute('aria-pressed', String(selectedMarket?.id === id));
      if (market) {
        const picture = market.icon ? make('img') : make('span', 'icon-blank');
        if (market.icon) { picture.src = market.icon; picture.alt = ''; picture.loading = 'lazy'; picture.referrerPolicy = 'no-referrer'; }
        const quote = chance(market.prices[0]);
        button.replaceChildren(picture, make('span', 'market-q', market.question), make('span', 'market-price', quote ? `${market.outcomes[0]} ${quote}` : ''));
        button.title = market.question; button.disabled = false; button.removeAttribute('aria-busy');
      } else if (details.has(id)) {
        button.replaceChildren(make('span', 'icon-blank'), make('span', 'market-q id', id), make('span', 'market-price', 'Details unavailable'));
        button.disabled = true; button.removeAttribute('aria-busy');
      } else {
        const spinner = make('span', 'market-spinner'); spinner.setAttribute('aria-hidden', 'true');
        button.replaceChildren(spinner, make('span', 'market-q id', id));
        button.disabled = true; button.setAttribute('aria-busy', 'true');
      }
    }
    function renderPage() {
      const slice = marketPage(ids, page); page = slice.page;
      list.replaceChildren(...slice.items.map(row));
      pager.hidden = slice.total <= MARKET_PAGE_SIZE;
      previous.disabled = page === 0; next.disabled = page === slice.pages - 1;
      setText(pageText, slice.total ? `${slice.start + 1} to ${slice.start + slice.items.length} of ${slice.total}` : '');
      loadDetails(slice.items);
    }
    previous.addEventListener('click', () => { page -= 1; renderPage(); });
    next.addEventListener('click', () => { page += 1; renderPage(); });
    function syntheticMarket(id) {
      const n = ids.indexOf(id);
      const questions = ['Will this sample market settle Yes?', 'Sample team A vs. sample team B', 'Will the sample index close higher this week?', 'Another market on this socket'];
      const yes = 0.15 + ((n * 37) % 70) / 100;
      return {id, question: `${questions[n % questions.length]}${n >= questions.length ? ` (${n + 1})` : ''}`, outcomes: n % 4 === 1 ? ['Team A', 'Team B'] : ['Yes', 'No'],
        prices: [yes.toFixed(2), (1 - yes).toFixed(2)], tokens: [`${n + 1}1`, `${n + 1}2`], icon: null, end: null};
    }
    async function loadDetails(items) {
      const missing = items.filter(id => !details.has(id) && !pending.has(id));
      if (!missing.length) return;
      const mine = generation, signal = abort.signal;
      missing.forEach(id => pending.add(id));
      let found = new Map();
      try {
        if (demo) {
          await new Promise(resolve => env.setTimeout(resolve, 400));
          found = new Map(missing.map(id => [id, syntheticMarket(id)]));
        } else {
          const params = new URLSearchParams({limit: String(missing.length), closed: 'any'});
          params.append('version', 'v1'); params.append('version', 'v2');
          for (const id of missing) params.append('condition_ids', id);
          const rows = await json(`${GAMMA}/markets?${params}`, signal);
          if (!Array.isArray(rows)) throw new Error('Invalid metadata');
          found = new Map(rows.map(metadata).filter(Boolean).map(market => [market.id, market]));
        }
      } catch (_) { /* Rows fall back to their IDs; Refresh retries. */ }
      if (mine !== generation) return;
      for (const id of missing) { pending.delete(id); details.set(id, found.get(id) || null); paint(id); }
      list.removeAttribute('aria-busy');
    }
    async function open(id) {
      close(); const mine = generation; currentId = id; abort = new env.AbortController();
      ids = []; page = 0; buttons = new Map(); details = new Map(); pending = new Set();
      renderPage(); retry.hidden = true; list.setAttribute('aria-busy', 'true'); note.textContent = 'Loading markets…';
      try {
        let assigned;
        if (demo) assigned = Array.from({length: 23}, (_, n) => `0x${(id * 100 + n + 1).toString(16).padStart(64, '0')}`);
        else {
          const config = await json('/api/demo/config', abort.signal);
          if (mine !== generation) return;
          if (config.stream_origin === null) { note.textContent = 'Live order books are not enabled on this deployment.'; list.removeAttribute('aria-busy'); return; }
          if (typeof config.stream_origin !== 'string' || !/^wss:\/\/[A-Za-z0-9.-]+(?::\d{1,5})?$/.test(config.stream_origin)) throw new Error('Stream unavailable');
          stream = config.stream_origin;
          const response = await request(`${stream.replace('wss:', 'https:')}/sockets/${id}/markets`, abort.signal);
          if (mine !== generation) return;
          if (response.status === 404) { note.textContent = 'No markets published for this socket yet.'; retry.hidden = false; list.removeAttribute('aria-busy'); return; }
          if (!response.ok) throw new Error('Directory unavailable');
          assigned = response.body.markets;
          if (!Array.isArray(assigned) || assigned.length > 512 || !assigned.every(value => typeof value === 'string' && /^0x[0-9a-fA-F]{64}$/.test(value))) throw new Error('Invalid directory');
          assigned = [...new Set(assigned.map(value => value.toLowerCase()))];
        }
        if (mine !== generation) return;
        ids = assigned; retry.hidden = false;
        note.textContent = ids.length ? `${ids.length} markets${demo ? ' · synthetic demo' : ''}. Select one to watch its live order book.` : 'No markets on this socket.';
        if (!ids.length) list.removeAttribute('aria-busy');
        renderPage();
      } catch (_) {
        if (mine !== generation) return;
        note.textContent = 'Markets could not be loaded. Try again.'; retry.hidden = false; list.removeAttribute('aria-busy');
      }
    }

    // The order book: one outcome at a time, asks above the midpoint and bids below.
    function freshness() {
      if (!selectedMarket || !view) return;
      const now = env.Date.now();
      const status = feedStatus(view, now);
      const messages = {connecting: 'Connecting…', waiting: 'Waiting for the first snapshot. This can take up to 60 seconds.',
        live: demo ? 'Live (synthetic)' : 'Live', stale: 'Snapshots are delayed. Showing the last known orders.',
        reconnecting: 'Connection interrupted. Reconnecting…',
        rejected: 'Live viewing is unavailable for this market. Select it again to retry.', invalid: 'Book data could not be read. Reconnecting…',
        incompatible: 'Book data is not compatible with this page. Reload the page before trying again.'};
      setText(bookStatus, messages[status]); bookStatus.classList.toggle('live', status === 'live');
      const book = view.books.get(selectedMarket.tokens[side]);
      const age = book?.snapshotAt === null || !book ? '' : `Last snapshot ${Math.max(0, Math.floor((now - book.snapshotAt) / 1000))}s ago`;
      const end = selectedMarket.end ? `Ends ${new Date(selectedMarket.end).toLocaleDateString()}` : '';
      setText(meta, [end, age].filter(Boolean).join(' · '));
    }
    function levelRows(group, levels, kind, max) {
      if (!levels.length) {
        if (group.dataset.empty !== '1') { group.replaceChildren(make('div', 'ladder-row none', kind === 'ask' ? 'No asks' : 'No bids')); group.dataset.empty = '1'; }
        return;
      }
      if (group.dataset.empty === '1') { group.replaceChildren(); delete group.dataset.empty; }
      while (group.children.length > levels.length) group.lastElementChild.remove();
      levels.forEach(([price, quantity], i) => {
        let line = group.children[i];
        const fresh = !line;
        if (fresh) { line = make('div', `ladder-row ${kind}`); line.setAttribute('role', 'row'); for (let c = 0; c < 2; c++) { const cell = make('span'); cell.setAttribute('role', 'cell'); line.append(cell); } group.append(line); }
        const priceText = cents(price), sizeText = shares(quantity);
        const changed = !fresh && line.children[0].textContent === priceText && line.children[1].textContent !== sizeText;
        setText(line.children[0], priceText); setText(line.children[1], sizeText);
        line.style.setProperty('--depth', `${Number(quantity * 1000n / max) / 10}%`);
        if (changed) { line.classList.remove('flash'); void line.offsetWidth; line.classList.add('flash'); }
      });
    }
    function renderBook() {
      if (!selectedMarket || !view) return;
      const book = view.books.get(selectedMarket.tokens[side]);
      if (!book || !book.ready) {
        if (!ladder.querySelector('.ladder-wait')) {
          const wait = make('div', 'ladder-wait'), spinner = make('span', 'market-spinner'); spinner.setAttribute('aria-hidden', 'true');
          wait.append(spinner, 'Waiting for the first snapshot…'); ladder.replaceChildren(wait);
        }
        freshness(); return;
      }
      if (!ladder.querySelector('[data-asks]')) {
        const head = make('div', 'ladder-row head'); head.setAttribute('aria-hidden', 'true'); head.append(make('span', '', 'Price'), make('span', '', 'Shares'));
        const asks = make('div'), mid = make('div', 'ladder-mid'), bids = make('div');
        asks.dataset.asks = ''; bids.dataset.bids = ''; mid.dataset.mid = '';
        asks.setAttribute('role', 'table'); bids.setAttribute('role', 'table');
        asks.setAttribute('aria-label', `${selectedMarket.outcomes[side]} asks`); bids.setAttribute('aria-label', `${selectedMarket.outcomes[side]} bids`);
        ladder.replaceChildren(head, asks, mid, bids);
      }
      const asks = sortedLevels(book, 'asks').slice(0, LADDER_DEPTH), bids = sortedLevels(book, 'bids').slice(0, LADDER_DEPTH);
      const max = [...asks, ...bids].reduce((most, [, quantity]) => quantity > most ? quantity : most, 1n);
      levelRows(ladder.querySelector('[data-asks]'), asks.reverse(), 'ask', max);
      levelRows(ladder.querySelector('[data-bids]'), bids, 'bid', max);
      const quote = midSpread(book);
      setText(ladder.querySelector('[data-mid]'), quote ? `${quote.chance} chance · spread ${quote.spread}` : 'No spread while a side is empty');
      freshness();
    }
    function chooseSide(index) {
      side = index;
      for (const [i, button] of [...toggle.children].entries()) button.setAttribute('aria-pressed', String(i === side));
      ladder.replaceChildren(); renderBook();
    }
    function select(market) {
      stopBook(); const previous = selectedMarket; selectedMarket = market; side = 0;
      if (previous) paint(previous.id); paint(market.id);
      empty.hidden = true; body.hidden = false;
      icon.replaceChildren(); if (market.icon) { const img = make('img'); img.src = market.icon; img.alt = ''; img.referrerPolicy = 'no-referrer'; icon.append(img); }
      title.textContent = market.question;
      toggle.replaceChildren(...market.outcomes.map((outcome, i) => {
        const button = make('button', '', outcome); button.type = 'button'; button.setAttribute('aria-pressed', String(i === 0));
        button.addEventListener('click', () => chooseSide(i)); return button;
      }));
      ladder.replaceChildren(); setText(meta, ''); setText(bookStatus, 'Connecting…'); bookStatus.classList.remove('live');
      body.scrollIntoView({block: 'nearest'});
      if (demo) {
        view = {status: 'live', books: new Map(market.tokens.map(token => [token, emptyBook()]))};
        let tick = 0;
        const update = () => {
          const mid = BigInt(Math.round(Number(market.prices[0]) * 100)) * 10000n;
          market.tokens.forEach((token, i) => {
            const centre = i === 0 ? mid : 1000000n - mid, level = n => 2000000n + BigInt(((n * 7919 + tick * (n + 3) * 131) % 4800) * 1000000);
            apply(view.books.get(token), {type: 'snapshot',
              bids: Array.from({length: 8}, (_, n) => [centre - 10000n * BigInt(n + 1), level(n)]).filter(([p]) => p > 0n),
              asks: Array.from({length: 8}, (_, n) => [centre + 10000n * BigInt(n + 1), level(n + 8)]).filter(([p]) => p < 1000000n)});
            view.books.get(token).snapshotAt = env.Date.now();
          });
          tick += 1; renderBook();
        };
        update(); demoTimer = env.setInterval(update, 1500); return;
      }
      const mine = generation;
      try {
        feed = createFeed(env, stream, market.tokens, state => {
          view = state;
          if (mine !== generation || renderTimer !== null) return;
          renderTimer = env.setTimeout(() => { renderTimer = null; if (mine === generation) renderBook(); }, 100);
        });
        view = feed.state;
        freshnessTimer = env.setInterval(freshness, 1000);
      } catch (_) { bookStatus.textContent = 'Unable to connect. Select the market again to retry.'; }
    }
    doc.addEventListener('visibilitychange', () => {
      if (!selectedMarket) return;
      if (doc.visibilityState === 'hidden') {
        stopBook(); ladder.replaceChildren(); setText(meta, ''); bookStatus.classList.remove('live');
        setText(bookStatus, 'Live viewing is paused while this tab is hidden.');
      } else select(selectedMarket);
    });
    retry.addEventListener('click', () => open(currentId));
    return {open, close};
  }
  root.DemoBooks = {fixed, cents, shares, midSpread, chance, decodeFrame, emptyBook, apply, sortedLevels, metadata, marketPage, MARKET_PAGE_SIZE, createFeed, feedStatus, mount};
})(typeof window !== 'undefined' ? window : globalThis);
