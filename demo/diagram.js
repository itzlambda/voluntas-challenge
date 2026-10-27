'use strict';
// Architecture diagram: component details, the "Follow one message" step-through, and the phone sheet.
(function (root) {
  const REPO = 'https://github.com/itzlambda/voluntas-challenge/blob/main/';
  const code = paths => paths.map(path => `<a href="${REPO}${path}"><code>${path}</code></a>`).join(', ');

  const COMPONENTS = {
    gamma: {
      title: 'Gamma REST scan',
      lead: 'Lists every open protocol-v1 market, page by page and oldest IDs first. It runs at startup, every 15 minutes, and after all lifecycle sockets were down at the same time.',
      rows: [
        ['Owns', 'Nothing between passes. A pass buffers every page before anything changes.'],
        ['Receives', 'Gamma market pages, 100 rows each'],
        ['Sends', 'One complete candidate set to discovery. A failed pass changes nothing.'],
        ['Runs as', 'A Tokio task started by discovery. Gamma requests share a budget of 10 per second.'],
        ['Why', 'Lifecycle hints are fast but not complete. The scan is the source of truth and the removal path that does not depend on resolution events.'],
      ],
      code: ['src/exchange/polymarket/gamma.rs', 'src/core/discovery.rs'],
    },
    lifecycle: {
      title: 'Lifecycle sockets',
      lead: 'Three WebSocket connections to Polymarket\'s market channel, kept for early notice of new and resolved markets. They subscribe to no order books; they receive new_market and market_resolved announcements.',
      rows: [
        ['Owns', 'Its own WebSocket session'],
        ['Receives', 'New-market and resolution announcements'],
        ['Sends', 'Hints to discovery. A new market waits until Gamma confirms it accepts orders.'],
        ['Runs as', 'One Tokio task per socket, PING every 10 s'],
        ['Why three', 'In one 65-minute run the three sockets delivered 519, 510 and 518 of 519 new markets, and missed events were not replayed after a reconnect. Together they delivered all 519.'],
      ],
      code: ['src/exchange/polymarket/lifecycle.rs'],
    },
    discovery: {
      title: 'Discovery and registry',
      lead: 'Owns the desired market set: the markets the collector keeps subscribed.',
      rows: [
        ['Owns', 'The registry and the queue of hints waiting for Gamma confirmation'],
        ['Receives', 'Complete scans, lifecycle hints and resolutions, and reports of markets whose books never arrive'],
        ['Sends', 'Add and remove requests to the subscription manager'],
        ['Runs as', 'One Tokio task'],
        ['Why', 'One owner orders every addition and removal without locks. A complete scan reconciles anything the hints missed.'],
      ],
      code: ['src/core/discovery.rs', 'src/core/registry.rs'],
    },
    subscriptions: {
      title: 'Subscription manager',
      lead: 'Places each market on one socket in each pool, 256 markets per socket, and keeps those assignments for the market\'s lifetime.',
      rows: [
        ['Owns', 'Both connection assignments for every market and the free capacity in each pool'],
        ['Receives', 'Add and remove requests from discovery'],
        ['Sends', 'AddMarket to the market\'s worker before subscribing, RemoveMarket before unsubscribing, and subscription commands to sockets. It opens at most 20 sockets per second.'],
        ['Runs as', 'One Tokio task'],
        ['Why', 'A control message sent before the subscription is always ahead of that subscription\'s data on the worker\'s channel.'],
      ],
      code: ['src/core/subscriptions.rs'],
    },
    'pool-a': {
      title: 'Pool A',
      lead: 'The WebSocket connections to Polymarket that carry order books, 256 markets per socket. Every market is subscribed once in Pool A and once in Pool B. Neither pool is primary or standby; each worker picks the publishing copy per market.',
      rows: [
        ['Owns', 'Each socket\'s subscribed asset set, used to resubscribe and to check for missing first books. No book state.'],
        ['Receives', 'Full books and price changes'],
        ['Sends', 'Decoded market data and connection resets to the workers, in order'],
        ['Runs as', 'One Tokio task per socket that reads, decodes, routes and sends in sequence. PING every 10 s; 30 s with no frame ends the session.'],
        ['Timestamps', 'Receive time in UTC and a monotonic instant, taken right after the read and before decoding'],
        ['Why two pools', 'Pools only keep a market\'s two copies on different sockets. When a socket fails, its markets switch to their copy in the other pool. Replicas are placed independently, so one socket\'s neighbours land on different sockets in the other pool.'],
      ],
      code: ['src/core/book_connection.rs', 'src/core/connection.rs', 'src/core/subscriptions.rs'],
    },
    route: {
      title: 'Pick the market\'s worker',
      lead: 'Every event from a market must reach the same worker, and markets must spread evenly across workers. The worker is the low 64 bits of the market\'s condition ID, modulo the worker count, so the choice is deterministic and nothing is stored.',
      example: true,
      rows: [
        ['Runs as', 'A function called inside each socket task, and by the subscription manager for controls'],
        ['Why', 'Condition IDs are uniformly distributed hashes, so markets land on workers as if at random, yet a given market always lands on the same one. Both outcome books share the condition ID, so they reach the same worker too.'],
        ['Cost', 'Markets are balanced by count, not activity. Changing the worker count reassigns markets, so it happens only at restart.'],
      ],
      code: ['src/core/routing.rs'],
    },
    workers: {
      title: 'Book workers',
      lead: 'Each worker owns a fixed share of markets for the life of the process. It keeps two copies of every book, one per pool, and publishes one copy per market.',
      rows: [
        ['Owns', 'Its markets\' books, the publisher choice per market, and a 60-second snapshot timer per published book'],
        ['Receives', 'Market data, AddMarket, RemoveMarket, and connection resets'],
        ['Sends', 'One batch of output records per input or timer, to the writer'],
        ['Runs as', 'One Tokio task per worker on the shared runtime. The count defaults to the process’s available CPU threads and can be overridden with --workers. These are tasks, not dedicated OS threads.'],
        ['Copies', 'Each copy is registered as a pair: the market and the connection it arrives on. Data is matched by that pair, and a failed connection\'s markets are found through it. Every market has exactly two pairs today; matching by pair rather than by pool is what would allow more than one backup.'],
        ['Why', 'When a publishing socket fails, the worker writes the other copy\'s full books as replacement snapshots and continues from it. There is no automatic failback.'],
      ],
      code: ['src/core/worker.rs', 'src/core/order_book.rs'],
    },
    writer: {
      title: 'Writer thread',
      lead: 'Converts each output record into the supplied SBE types and encodes it with AlignedSbeWriter. The file grows in 64 MiB chunks.',
      rows: [
        ['Owns', 'The output file'],
        ['Receives', 'Record batches from every worker, through one bounded channel'],
        ['Sends', 'SBE bytes to the output'],
        ['Runs as', 'A plain OS thread, because file writes block'],
        ['Timestamps', 'Time inside the collector ends when a level update finishes encoding'],
        ['Why', 'I originally planned to move encoding to the book workers. That required changes to the supplied encoding path, so I kept it unchanged. Benchmark a single book worker before retaining the pool or parallelizing encoding.'],
      ],
      code: ['src/core/writer.rs'],
    },
    output: {
      title: 'books.sbe',
      lead: 'Snapshots and price-level updates in the challenge\'s SBE schema, readable with the supplied AlignedSbeReader.',
      rows: [
        ['Written by', 'The writer thread only'],
        ['Visible', 'In 64 MiB chunks, so file visibility is not latency'],
        ['Cost', 'A crash loses up to one in-memory chunk. There is no fsync durability promise.'],
      ],
      code: ['src/core/writer.rs', 'docs/ARCHITECTURE.md'],
    },
  };

  COMPONENTS['pool-b'] = {...COMPONENTS['pool-a'], title: 'Pool B'};

  const STEPS = [
    {title: 'A market is found', lit: ['gamma', 'lifecycle'], link: 1,
      text: 'A full Gamma scan lists every open market. A lifecycle socket can announce a new one sooner; Gamma then confirms it accepts orders.',
      time: 'None. No message has arrived yet.', code: ['src/exchange/polymarket/gamma.rs', 'src/exchange/polymarket/lifecycle.rs']},
    {title: 'It is registered', lit: ['discovery'], link: 2,
      text: 'Discovery adds the market to the desired set and asks the subscription manager to collect it.',
      time: 'None. No message has arrived yet.', code: ['src/core/discovery.rs', 'src/core/registry.rs']},
    {title: 'It is placed on two sockets', lit: ['subscriptions'], link: 3,
      text: 'The manager picks one socket in each pool. It sends AddMarket to the market\'s worker first, then subscribes both sockets.',
      time: 'None. No message has arrived yet.', code: ['src/core/subscriptions.rs']},
    {title: 'Both pools receive it', lit: ['pool-a', 'pool-b'], link: 4,
      text: 'The exchange sends the same price change on both sockets. Each socket takes its receive time right after the read, then decodes the frame.',
      time: 'Exchange time (in the message). Received at, in UTC, kept in the output. A monotonic instant that starts time inside the collector. Exchange age is recorded here.',
      code: ['src/core/book_connection.rs', 'src/core/connection.rs']},
    {title: 'It goes to the market\'s worker', lit: ['route'], link: 5, example: true,
      text: 'The socket reads the message\'s condition ID and hands it to the worker that owns that market, the same worker every time.',
      time: 'Exchange time and received at, from step 4', code: ['src/core/routing.rs']},
    {title: 'The worker applies it', lit: ['workers'], link: 6,
      text: 'The worker applies each copy to that source\'s book only. Only the publishing copy produces output records; the standby copy stays current privately.',
      time: 'No new timestamp. The records carry the receive time and monotonic instant.', code: ['src/core/worker.rs']},
    {title: 'The writer encodes it', lit: ['writer', 'output'], link: 7,
      text: 'The writer encodes each record into SBE. That ends time inside the collector. The bytes reach the file later, when the 64 MiB chunk is written.',
      time: 'Encoding complete: time inside the collector is measured here', code: ['src/core/writer.rs']},
  ];

  const EXAMPLE = `<p class="example">Condition ID <code>0x233e…703f6406</code>. Low 64 bits <code>0x62aefdf0703f6406</code>. With four workers this is the last hex digit, 6, and 6 mod 4 = 2, so both outcome books go to <code>W2</code>.</p>`;

  function rowsHtml(rows) {
    return `<dl>${rows.map(([term, text]) => `<dt>${term}</dt><dd>${text}</dd>`).join('')}</dl>`;
  }

  function componentHtml(id) {
    const c = COMPONENTS[id];
    return {
      title: c.title,
      body: `<p class="lead">${c.lead}</p>${c.example ? EXAMPLE : ''}${rowsHtml([...c.rows, ['In the code', code(c.code)]])}`,
    };
  }

  function stepHtml(index) {
    const s = STEPS[index];
    return {
      title: `${index + 1}. ${s.title}`,
      body: `<p class="lead">${s.text}</p>${s.example ? EXAMPLE : ''}${rowsHtml([['Timestamps', s.time], ['In the code', code(s.code)]])}`,
    };
  }

  function init(doc, win) {
    const detail = doc.getElementById('detail');
    if (!detail) return null;
    const title = doc.getElementById('detail-title');
    const body = doc.getElementById('detail-body');
    const nums = [...doc.querySelectorAll('#diagram .num[data-num]')];
    const boxes = [...doc.querySelectorAll('#diagram .box')];
    const sections = [...doc.querySelectorAll('#diagram .step-row')];
    const phone = win.matchMedia ? win.matchMedia('(max-width: 860px)') : {matches: false};
    const reduced = win.matchMedia ? win.matchMedia('(prefers-reduced-motion: reduce)') : {matches: false};
    let step = 0;

    function show(content) {
      title.textContent = content.title;
      body.innerHTML = content.body;
    }

    function openSheet(focus) {
      detail.classList.remove('closed');
      if (focus) detail.focus({preventScroll: true});
    }

    function light(link) {
      for (const section of sections) section.classList.toggle('selected', Number(section.dataset.step) === link);
      for (const box of boxes) {
        box.classList.remove('lit');
        box.setAttribute('aria-pressed', 'false');
      }
      for (const row of doc.querySelectorAll('.link-row')) {
        const on = Number(row.dataset.link) === link;
        row.classList.toggle('lit', on);
        for (const path of row.querySelectorAll('path')) {
          path.classList.remove('lit');
          if (on) path.classList.add('lit');
        }
      }
      for (const num of doc.querySelectorAll('.num[data-num]')) num.classList.toggle('on', Number(num.dataset.num) === link);
    }

    function setStep(index, {fromUser = false} = {}) {
      step = Math.max(0, Math.min(STEPS.length - 1, index));
      light(STEPS[step].link);
      show(stepHtml(step));
      for (const num of nums) num.setAttribute('aria-pressed', String(Number(num.dataset.num) === STEPS[step].link));
      if (fromUser) {
        win.history.replaceState(null, '', `#step-${step + 1}`);
        if (phone.matches) {
          openSheet(false);
          const target = boxes.find(box => box.dataset.id === STEPS[step].lit[0]);
          // Keep the highlighted box above the sheet.
          if (target) target.scrollIntoView({block: 'start', behavior: reduced.matches ? 'auto' : 'smooth'});
        }
      }
    }

    function showComponent(id, {focus = false} = {}) {
      if (!COMPONENTS[id]) return;
      for (const section of sections) section.classList.remove('selected');
      for (const box of boxes) {
        box.classList.remove('lit');
        box.setAttribute('aria-pressed', String(box.dataset.id === id));
      }
      for (const row of doc.querySelectorAll('.link-row')) row.classList.remove('lit');
      for (const path of doc.querySelectorAll('.link-row path')) path.classList.remove('lit');
      for (const num of nums) { num.classList.remove('on'); num.setAttribute('aria-pressed', 'false'); }
      show(componentHtml(id));
      win.history.replaceState(null, '', `#${id}`);
      openSheet(focus);
    }

    for (const num of nums) {
      const index = STEPS.findIndex(item => item.link === Number(num.dataset.num));
      if (index < 0) continue;
      num.setAttribute('aria-label', `Step ${index + 1} of ${STEPS.length}: ${STEPS[index].title}`);
      num.addEventListener('click', () => setStep(index, {fromUser: true}));
    }
    for (const section of sections) {
      section.addEventListener('click', event => {
        if (event.target.closest('button, a')) return;
        section.querySelector('.num[data-num]').click();
      });
    }
    for (const box of boxes) box.addEventListener('click', () => showComponent(box.dataset.id, {focus: phone.matches}));
    doc.getElementById('detail-close').addEventListener('click', () => {
      detail.classList.add('closed');
      const pressed = boxes.find(box => box.getAttribute('aria-pressed') === 'true');
      (pressed || nums.find(num => num.getAttribute('aria-pressed') === 'true') || nums[0]).focus();
    });
    doc.addEventListener('keydown', event => {
      if (event.key === 'Escape' && phone.matches && !detail.classList.contains('closed')) doc.getElementById('detail-close').click();
    });

    // Start closed on phones; the sheet opens when a step or box is chosen.
    if (phone.matches) detail.classList.add('closed');
    const hash = win.location.hash.slice(1);
    const stepMatch = /^step-(\d)$/.exec(hash);
    if (COMPONENTS[hash]) showComponent(hash);
    else setStep(stepMatch ? Number(stepMatch[1]) - 1 : 0);
    return {setStep, showComponent, get step() { return step; }};
  }

  root.DemoDiagram = {COMPONENTS, STEPS, componentHtml, stepHtml, init};
  if (root.document && root.document.getElementById && root.document.getElementById('diagram')) {
    root.DemoDiagram.page = init(root.document, root);
    const S = root.DemoStatus;
    if (S && S.page && S.renderCounts) {
      S.page.subscribe(snap => {
        S.renderCounts(snap, root.document);
        const note = root.document.getElementById('counts-note');
        const live = snap.stats && S.isNumber(snap.stats.workers);
        note.textContent = live
          ? `Workers are schematic. Socket counts come from the collector's metrics${snap.retained ? `, last updated ${S.ago(snap.age)}` : ''}.`
          : 'Workers are schematic. Socket counts show configured values until live metrics arrive.';
      });
    }
  }
})(typeof window !== 'undefined' ? window : globalThis);
