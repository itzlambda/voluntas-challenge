'use strict';
// Overview figures and the live counts inside the compact diagram.
(function (root) {
  const S = root.DemoStatus;

  /** Renders the four figures from a status snapshot into elements found by id. */
  function renderOverview(snap, byId) {
    const stats = snap.stats || {};
    const empty = snap.state === 'stopped' && !snap.retained ? 'No data' : 'No data yet';
    const set = (id, html) => {
      byId(id).innerHTML = html === null ? `<span class="unavailable">${empty}</span>` : html;
    };
    const note = (id, text) => { byId(id).textContent = text || ''; };
    const markets = stats.markets || {};
    const standby = stats.resilience || {};

    set('fig-markets', S.isNumber(markets.synced) && S.isNumber(markets.desired)
      ? `${S.count(markets.synced)} <span class="of">of ${S.count(markets.desired)}</span>` : null);
    // Traced: Polymarket lists the remaining markets as open but serves no book for them.
    const bookless = S.isNumber(markets.synced) && S.isNumber(markets.desired) && markets.desired > markets.synced;
    note('fig-markets-note', S.isNumber(markets.desired)
      ? `Two outcome books per market.${bookless ? ' Polymarket sometimes lists markets as active before their order books are available. This likely explains small gaps between the expected and actual book counts.' : ''}` : '');

    note('fig-markets-gap', S.booklessNote(stats));

    set('fig-updates', S.isNumber(stats.book_updates_per_second) ? S.count(stats.book_updates_per_second) : null);
    note('fig-updates-note', S.isNumber(stats.book_updates_per_second)
      ? 'Book updates per second, both pools' : '');

    const percentiles = name => {
      const p50 = S.latency(stats, name, 50);
      const p99 = S.latency(stats, name, 99);
      return p50 && p99 ? `<span class="pair"><span class="of">p50</span> ${p50}</span> <span class="pair"><span class="of">p99</span> ${p99}</span>` : null;
    };
    set('fig-exchange', percentiles('exchange_age'));
    set('fig-latency', percentiles('internal_latency'));

    set('fig-standby', S.isNumber(standby.standby_ready_markets)
      ? `${S.count(standby.standby_ready_markets)}${S.isNumber(markets.synced) ? ` <span class="of">of ${S.count(markets.synced)}</span>` : ''}` : null);

    byId('figures').classList.toggle('retained', snap.retained);
    renderCounts(snap, root.document);
  }

  /** Socket and worker counts come from metrics; the static text is the configured value. */
  function renderCounts(snap, doc) {
    const stats = snap.stats || {};
    if (!doc || !doc.querySelectorAll) return;
    const discovery = S.sockets(stats, 'market_discovery');
    for (const element of doc.querySelectorAll('[data-live="discovery_sockets"]')) {
      if (discovery) element.textContent = S.count(discovery.expected);
    }
    for (const element of doc.querySelectorAll('[data-live="worker_range"]')) {
      if (S.isNumber(stats.workers) && stats.workers > 0) element.textContent = stats.workers === 1 ? 'W0' : `W0-W${stats.workers - 1}`;
    }
    const caption = doc.getElementById('flow-sockets');
    const orderbook = S.sockets(stats, 'orderbook');
    if (caption) {
      caption.textContent = orderbook && orderbook.expected > 0
        ? `Every market is subscribed in both pools. ${S.count(orderbook.connected)} of ${S.count(orderbook.expected)} order-book sockets open${snap.retained ? ' when last updated' : ''}.`
        : 'Every market is subscribed in both pools.';
    }
  }

  S.renderOverview = renderOverview;
  S.renderCounts = renderCounts;
  if (S.page && root.document.getElementById('figures')) {
    S.page.subscribe(snap => renderOverview(snap, id => root.document.getElementById(id)));
  }
})(typeof window !== 'undefined' ? window : globalThis);
