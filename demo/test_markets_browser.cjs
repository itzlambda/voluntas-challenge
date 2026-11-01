// Run with Playwright available: node demo/test_markets_browser.cjs http://127.0.0.1:PORT
// The demo instance must use --stream-origin wss://stream.example.com; Gamma and the stream are intercepted.
const assert = require('node:assert/strict');
const {chromium} = require('playwright');
const {record} = require('./book-fixture.cjs');
const base = process.argv[2];
const cid = n => `0x${n.toString(16).padStart(64, '0')}`;
const market = (n, extra = {}) => ({conditionId: cid(n), question: `Top market ${n}`, outcomes: '["Yes","No"]', clobTokenIds: `["${n}1","${n}2"]`,
  outcomePrices: '["0.25","0.75"]', active: true, closed: false, archived: false, acceptingOrders: true, volume24hr: 2000000 - n * 1000, ...extra});
(async () => {
  const browser = await chromium.launch({headless: true});
  try {
    const page = await browser.newPage();
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    let gammaMode = 'ok', streamOrigin = 'wss://stream.example.com';
    const searches = [], subscriptions = [], sockets = [];
    await page.route('**/api/demo/config', route => route.fulfill({json: {stream_origin: streamOrigin}}));
    await page.route('**/api/demo/stats', route => route.abort());
    await page.route('https://gamma-api.polymarket.com/**', route => {
      if (gammaMode === 'dns') return route.abort('namenotresolved');
      const url = new URL(route.request().url()), cors = {'Access-Control-Allow-Origin': '*'};
      if (url.pathname === '/markets' && url.searchParams.get('order') === 'volume24hr') {
        assert.equal(url.searchParams.get('closed'), 'false'); assert.equal(url.searchParams.get('ascending'), 'false');
        // The first row is not collected, so the page needs the extra rows it asked for.
        return route.fulfill({headers: cors, json: [market(99, {acceptingOrders: false}), ...Array.from({length: 14}, (_, i) => market(i + 1))]});
      }
      if (url.pathname === '/markets') {
        const id = url.searchParams.get('condition_ids');
        const rows = id === cid(7) ? [market(7)] : id === cid(8) ? [market(8, {closed: true})] : [];
        return route.fulfill({headers: cors, json: rows});
      }
      if (url.pathname === '/public-search') {
        searches.push(url.searchParams.get('q'));
        const many = Array.from({length: 7}, (_, i) => market(200 + i, {question: `Will Bitcoin be above ${70 + i}k?`, groupItemTitle: `${70 + i},000`}));
        return route.fulfill({headers: cors, json: {events: [
          {title: 'Bitcoin above ___?', markets: [...many, market(300, {closed: true})]},
          {title: 'Bitcoin single', markets: [market(400, {question: 'Will Bitcoin hit $1M?'})]}]}});
      }
      return route.fulfill({status: 404, headers: cors, body: ''});
    });
    await page.routeWebSocket('wss://stream.example.com/**', socket => {
      sockets.push(socket);
      socket.onMessage(message => subscriptions.push(JSON.parse(message).books.join()));
    });

    await page.goto(`${base}/markets`);
    await page.waitForFunction(() => document.querySelectorAll('[data-markets-list] .market-row').length === 10);
    assert.match(await page.locator('[data-markets-status]').innerText(), /^As of /);
    assert.match(await page.locator('.market-row').first().innerText(), /^1\s*Top market 1\s*Yes 25%\s*\$2.00M$/);
    assert.equal(await page.getByText('Select a market to watch its live order book.').isVisible(), true);
    await page.locator('.market-row').nth(1).click();
    await page.getByText('Waiting for the first snapshot. This can take up to 60 seconds.', {exact: true}).waitFor();
    assert.equal(new URL(page.url()).searchParams.get('market'), cid(2), 'the address names the selected market');
    assert.equal(await page.locator('.market-row[aria-pressed="true"]').count(), 1);
    await page.waitForFunction(() => true);
    await page.waitForTimeout(100);
    assert.deepEqual(subscriptions, ['21,22']);
    sockets[0].send(Buffer.concat([record(1, '21'), record(1, '22')]));
    await page.locator('[data-book-status]').filter({hasText: /^Live$/}).waitFor();
    assert.equal(await page.locator('.ladder-row.ask').count(), 1);

    // Typing waits for a pause and needs two characters; search groups markets under events.
    await page.fill('[data-markets-search]', 'b');
    await page.waitForTimeout(500);
    assert.equal(searches.length, 0);
    await page.locator('[data-markets-search]').pressSequentially('itcoin');
    await page.waitForFunction(() => document.querySelectorAll('.market-group').length === 2);
    assert.deepEqual(searches, ['bitcoin'], 'one request after typing stops');
    assert.equal(await page.locator('[data-markets-heading]').innerText(), 'Results for “bitcoin”');
    assert.equal(await page.locator('[data-markets-status]').innerText(), '8 open markets. 1 closed or unsupported market hidden.');
    assert.equal(await page.locator('.market-group').first().locator('.market-row').count(), 5);
    assert.equal(await page.locator('.market-group').first().locator('.market-q').first().innerText(), '70,000', 'grouped rows use the short label');
    assert.equal(await page.locator('.market-group').nth(1).locator('.market-q').innerText(), 'Will Bitcoin hit $1M?', 'a single market keeps its question');
    await page.getByRole('button', {name: 'Show 2 more'}).click();
    assert.equal(await page.locator('.market-group').first().locator('.market-row').count(), 7);
    await page.locator('.market-group').nth(1).locator('.market-row').click();
    await page.getByText('Waiting for the first snapshot. This can take up to 60 seconds.', {exact: true}).waitFor();
    assert.equal(await page.locator('[data-book-title]').innerText(), 'Will Bitcoin hit $1M?');
    assert.equal(await page.locator('[data-book-event]').innerText(), 'Bitcoin single');
    await page.waitForTimeout(100);
    assert.equal(subscriptions.at(-1), '4001,4002');
    await page.getByRole('button', {name: 'Clear search'}).click();
    await page.waitForFunction(() => document.querySelectorAll('[data-markets-list] .market-row').length === 10);
    assert.equal(await page.locator('[data-markets-heading]').innerText(), 'Top 10 by 24h volume');
    await page.fill('[data-markets-search]', 'xy');
    await page.locator('[data-markets-search]').press('Escape');
    assert.equal(await page.locator('[data-markets-search]').inputValue(), '');

    // A shared link opens its book; a closed one explains why there is none.
    await page.goto(`${base}/markets?market=${cid(7)}`);
    await page.getByText('Waiting for the first snapshot. This can take up to 60 seconds.', {exact: true}).waitFor();
    assert.equal(await page.locator('[data-book-title]').innerText(), 'Top market 7');
    await page.goto(`${base}/markets?market=${cid(8)}`);
    await page.getByText('This market is closed or not collected, so it has no live book.', {exact: true}).waitFor();
    await page.goto(`${base}/markets?market=${cid(9)}`);
    await page.getByText('This market was not found on Polymarket.', {exact: true}).waitFor();

    // Blocked Polymarket and a deployment without a stream.
    gammaMode = 'dns';
    await page.goto(`${base}/markets`);
    await page.locator('[data-markets-status].lookup-problem').filter({hasText: 'Polymarket may be blocked in your region or on your network.'}).waitFor();
    await page.fill('[data-markets-search]', 'bitcoin');
    await page.locator('[data-markets-status].lookup-problem').filter({hasText: 'may be blocked'}).waitFor();
    gammaMode = 'ok'; streamOrigin = null;
    await page.goto(`${base}/markets`);
    await page.waitForFunction(() => document.querySelectorAll('[data-markets-list] .market-row').length === 10);
    await page.locator('.market-row').first().click();
    await page.getByText('Live order books are not enabled on this deployment.', {exact: true}).waitFor();

    await page.setViewportSize({width: 390, height: 844});
    streamOrigin = 'wss://stream.example.com';
    await page.goto(`${base}/markets`);
    await page.waitForFunction(() => document.querySelectorAll('[data-markets-list] .market-row').length === 10);
    await page.locator('.market-row').first().click();
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1), 'no sideways scrolling on phones');
    assert.deepEqual(errors, []);
    console.log('Markets browser checks passed: top 10 with uncollected markets skipped, book selection and address, debounced grouped search, more rows, clear and Escape, shared links, blocked Polymarket, no stream, phone width.');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
