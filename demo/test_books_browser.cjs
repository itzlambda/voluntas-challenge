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
    assert.equal(await page.evaluate(() => document.activeElement.classList.contains('socket-id')), true, 'Escape restores focus');
    await page.locator('.socket-id').first().click();
    const bounds = await page.locator('dialog').boundingBox();
    await page.mouse.click(bounds.x + 4, bounds.y + 4);
    assert.equal(await page.locator('dialog').evaluate(d => d.open), true, 'inside padding does not dismiss');
    await page.mouse.move(bounds.x + 4, bounds.y + 4); await page.mouse.down();
    await page.mouse.move(2, 2); await page.mouse.up();
    assert.equal(await page.locator('dialog').evaluate(d => d.open), true, 'dragging out from inside does not dismiss');
    await page.mouse.click(2, 2);
    await page.waitForFunction(() => !document.querySelector('dialog').open);
    assert.equal(await page.evaluate(() => document.activeElement.classList.contains('socket-id')), true, 'outside dismissal restores focus');
    await page.setViewportSize({width: 390, height: 844});
    await page.locator('.socket-id').first().click();
    await page.locator('[data-market-list] button:enabled').first().click();
    assert.ok(await page.locator('dialog').evaluate(d => d.scrollWidth <= d.clientWidth + 1));
    await page.getByRole('button', {name: 'Close', exact: true}).click();
    assert.equal(await page.evaluate(() => document.activeElement.classList.contains('socket-id')), true, 'Close restores focus');

    let connections = 0, directoryRequests = 0, closed = 0;
    const sockets = [];
    const marketIds = [...Array.from({length: 44}, (_, i) => `0x${(i + 1).toString(16).padStart(64, '0')}`), id];
    await page.route('**/api/demo/config', route => route.fulfill({json: {stream_origin: 'wss://stream.example.com'}}));
    await page.route('https://stream.example.com/sockets/*/markets', route => {
      directoryRequests++;
      return route.fulfill({headers: {'Access-Control-Allow-Origin': '*'}, json: {markets: marketIds}});
    });
    await page.route('https://gamma-api.polymarket.com/markets?**', route => {
      const params = new URL(route.request().url()).searchParams, requested = params.getAll('condition_ids');
      assert.equal(Number(params.get('limit')), requested.length, 'Gamma limit covers the entire batch');
      assert.deepEqual(params.getAll('version'), ['v1', 'v2']);
      assert.equal(params.get('closed'), 'any');
      const rows = requested.slice(0, Number(params.get('limit') || 20)).map((conditionId, i) => ({conditionId,
        question: conditionId === id ? 'Live wire test market' : `Other test market ${i}`,
        outcomes: '["Yes","No"]', outcomePrices: '["0.45","0.55"]', clobTokenIds: '["123","456"]', volume24hr: marketIds.indexOf(conditionId) * 100}));
      return route.fulfill({headers: {'Access-Control-Allow-Origin': '*'}, json: rows});
    });
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
    await page.waitForFunction(() => document.querySelectorAll('[data-market-list] button:enabled').length === 20 && !document.querySelector('[data-market-list]').hasAttribute('aria-busy'));
    assert.match(await page.locator('[data-market-list] button').first().innerText(), /Live wire test market/);
    assert.equal(await page.locator('.market-volume').first().innerText(), '24h volume $4,400.00', 'highest volume from the last batch comes first');
    assert.equal(await page.locator('[data-market-page]').innerText(), '1 to 20 of 45 markets');
    await page.locator('[data-market-next]').click();
    assert.equal(await page.locator('[data-market-page]').innerText(), '21 to 40 of 45 markets');
    assert.equal(await page.locator('.market-volume').first().innerText(), '24h volume $2,400.00');
    await page.locator('[data-market-next]').click();
    assert.equal(await page.locator('[data-market-list] button').count(), 5);
    assert.equal(await page.locator('[data-market-next]').isDisabled(), true);
    await page.locator('[data-market-previous]').click(); await page.locator('[data-market-previous]').click();
    assert.equal(await page.locator('[data-market-previous]').isDisabled(), true);
    await page.getByRole('button', {name: /Live wire test market/}).click();
    await page.getByText('Waiting for a snapshot. This may take up to 60 seconds.', {exact: true}).waitFor();
    assert.equal(await page.locator('.depth-table').count(), 0);
    sockets[0].send(Buffer.concat([record(1), record(1, '456')]));
    await page.locator('[data-book-status]').filter({hasText: /^Live order books$/}).waitFor();
    assert.equal(await page.locator('.depth-table').count(), 4);
    await page.locator('[data-market-next]').click();
    assert.equal(connections, 1, 'paging leaves the selected stream running');
    assert.equal(await page.locator('[data-book-title]').innerText(), 'Live wire test market');
    await page.locator('[data-market-previous]').click();
    assert.equal(await page.getByText('12345678901.234567', {exact: true}).count(), 2);
    assert.ok(await page.locator('dialog').evaluate(d => d.scrollWidth <= d.clientWidth + 1), 'large exact quantities fit on mobile');
    await page.evaluate(() => {
      window.bookStatusMutations = 0;
      window.bookStatusObserver = new MutationObserver(list => window.bookStatusMutations += list.length);
      window.bookStatusObserver.observe(document.querySelector('[data-book-status]'), {childList: true, characterData: true, subtree: true});
      window.firstBookTable = document.querySelector('.depth-table');
    });
    sockets[0].send(record(1));
    await page.waitForTimeout(200);
    assert.equal(await page.evaluate(() => window.bookStatusMutations), 0, 'unchanged live status is not reannounced');
    assert.equal(await page.evaluate(() => window.firstBookTable === document.querySelector('.depth-table')), true, 'table nodes remain stable');
    await page.evaluate(() => window.bookStatusObserver.disconnect());
    sockets[0].send(record(2, '123', {quantity: 0n}));
    await page.waitForFunction(() => document.querySelector('.depth-table.bids tbody').textContent === 'No orders');
    sockets[0].close({code: 1013, reason: 'test reconnect'});
    await page.waitForFunction(() => document.querySelector('[data-book-status]').textContent.includes('Waiting for a snapshot'));
    assert.equal(connections, 2);
    assert.equal(await page.locator('.depth-table').count(), 0);
    sockets[1].send(Buffer.concat([record(1), record(1, '456')]));
    await page.locator('[data-book-status]').filter({hasText: /^Live order books$/}).waitFor();
    await page.mouse.click(2, 2);
    await page.waitForTimeout(200);
    assert.equal(await page.locator('dialog').evaluate(d => d.open), false, 'outside click closes live books');
    assert.ok(closed >= 1, 'outside dismissal closes the stream');
    await page.locator('.socket-id').first().click();
    await page.getByRole('button', {name: /Live wire test market/}).waitFor();
    assert.equal(directoryRequests, 2, 'reopening fetches current assignments');
    await page.keyboard.press('Escape');
    // Hidden tabs release the subscription and start fresh when visible again.
    await page.locator('.socket-id').first().click();
    await page.getByRole('button', {name: /Live wire test market/}).click();
    await page.waitForFunction(() => document.querySelector('[data-book-status]').textContent.includes('Waiting for a snapshot'));
    const beforeHidden = connections;
    await page.evaluate(() => {
      Object.defineProperty(document, 'visibilityState', {configurable: true, value: 'hidden'});
      document.dispatchEvent(new Event('visibilitychange'));
    });
    await page.getByText('Live viewing is paused while this tab is hidden.', {exact: true}).waitFor();
    await page.evaluate(() => {
      Object.defineProperty(document, 'visibilityState', {configurable: true, value: 'visible'});
      document.dispatchEvent(new Event('visibilitychange'));
    });
    await page.waitForFunction(() => document.querySelector('[data-book-status]').textContent.includes('Waiting for a snapshot'));
    assert.equal(connections, beforeHidden + 1);
    await page.keyboard.press('Escape');
    await page.route('**/api/demo/config', route => route.fulfill({json: {stream_origin: null}}));
    await page.locator('.socket-id').first().click();
    await page.getByText('Live order books are not enabled on this deployment.', {exact: true}).waitFor();
    await page.keyboard.press('Escape');
    await page.route('**/book-view.js', route => route.abort());
    await page.goto(`${base}/live`);
    await page.locator('.socket-id').first().click();
    await page.getByText('Market viewing could not be loaded. Reload the page to retry.', {exact: true}).waitFor();
    await page.keyboard.press('Escape');
    assert.deepEqual(errors, []);
    console.log('Browser checks passed: synthetic popup/history/books, mobile overflow, Gamma lookup, exact SBE snapshots/updates, waiting state, reconnect reset, close cleanup, and assignment refresh.');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
