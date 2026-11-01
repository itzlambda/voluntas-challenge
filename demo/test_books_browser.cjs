// Run with Playwright available: node demo/test_books_browser.cjs http://127.0.0.1:PORT
const assert = require('node:assert/strict');
const {chromium} = require('playwright');
const {record} = require('./book-fixture.cjs');
const base = process.argv[2];
const id = `0x${'a'.repeat(64)}`;
(async () => {
  const browser = await chromium.launch({headless: true});
  try {
    const page = await browser.newPage();
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(`${base}/live?socket-demo=1`);
    await page.locator('.socket-id').first().click();
    await page.locator('[data-market-list] button:enabled').first().waitFor();
    assert.equal(await page.locator('dialog').evaluate(d => d.open), true);
    assert.equal(await page.locator('[data-socket-plot] .uplot').count(), 1);
    await page.locator('[data-market-list] button:enabled').first().click();
    await page.getByText('Synthetic live order books', {exact: true}).waitFor();
    assert.equal(await page.locator('.depth-table').count(), 4);
    await page.keyboard.press('Escape');
    assert.equal(await page.locator('dialog').evaluate(d => d.open), false);
    await page.setViewportSize({width: 390, height: 844});
    await page.locator('.socket-id').first().click();
    await page.locator('[data-market-list] button:enabled').first().click();
    assert.ok(await page.locator('dialog').evaluate(d => d.scrollWidth <= d.clientWidth + 1));
    await page.getByRole('button', {name: 'Close', exact: true}).click();

    let connections = 0, directoryRequests = 0, closed = 0;
    const sockets = [];
    await page.route('**/api/demo/config', route => route.fulfill({json: {stream_origin: 'wss://stream.example.com'}}));
    await page.route('https://stream.example.com/sockets/*/markets', route => {
      directoryRequests++;
      return route.fulfill({headers: {'Access-Control-Allow-Origin': '*'}, json: {markets: [id]}});
    });
    await page.route('https://gamma-api.polymarket.com/markets?**', route => route.fulfill({headers: {'Access-Control-Allow-Origin': '*'}, json: [{conditionId: id, question: 'Live wire test market', outcomes: '["Yes","No"]', outcomePrices: '["0.45","0.55"]', clobTokenIds: '["123","456"]'}]}));
    await page.routeWebSocket('wss://stream.example.com/**', socket => {
      connections++; sockets.push(socket);
      socket.onClose(() => closed++);
      socket.onMessage(message => {
        assert.deepEqual(JSON.parse(message), {op: 'subscribe', books: ['123', '456']});
        // A pre-snapshot update must remain invisible.
        socket.send(record(2, '123', {price: 123n, quantity: 999n}));
      });
    });
    await page.route('**/api/demo/sockets', route => route.fulfill({json: {schema_version: 1, status: 'live', retained: false, values_at: Date.now() / 1000, age_seconds: 0, sockets: [{id: 0, pool: 'primary', connected: true, markets: 1, state: 'connected', events_per_second: 2}]}}));
    await page.goto(`${base}/live`);
    await page.locator('.socket-id').first().click();
    await page.getByRole('button', {name: /Live wire test market/}).click();
    await page.getByText('Waiting for a snapshot. This may take up to 60 seconds.', {exact: true}).waitFor();
    assert.equal(await page.locator('.depth-table').count(), 0);
    sockets[0].send(Buffer.concat([record(1), record(1, '456')]));
    await page.locator('[data-book-status]').filter({hasText: /^Live order books$/}).waitFor();
    assert.equal(await page.locator('.depth-table').count(), 4);
    assert.equal(await page.getByText('12345678901.234567', {exact: true}).count(), 2);
    sockets[0].send(record(2, '123', {quantity: 0n}));
    await page.waitForFunction(() => document.querySelector('.depth-table.bids tbody').textContent === 'No orders');
    sockets[0].close({code: 1013, reason: 'test reconnect'});
    await page.waitForFunction(() => document.querySelector('[data-book-status]').textContent.includes('Waiting for a snapshot'));
    assert.equal(connections, 2);
    assert.equal(await page.locator('.depth-table').count(), 0);
    sockets[1].send(Buffer.concat([record(1), record(1, '456')]));
    await page.locator('[data-book-status]').filter({hasText: /^Live order books$/}).waitFor();
    await page.keyboard.press('Escape');
    await page.waitForTimeout(200);
    assert.ok(closed >= 1);
    await page.locator('.socket-id').first().click();
    await page.getByRole('button', {name: /Live wire test market/}).waitFor();
    assert.equal(directoryRequests, 2, 'reopening fetches current assignments');
    await page.keyboard.press('Escape');
    assert.deepEqual(errors, []);
    console.log('Browser checks passed: synthetic popup/history/books, mobile overflow, Gamma lookup, exact SBE snapshots/updates, waiting state, reconnect reset, close cleanup, and assignment refresh.');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
