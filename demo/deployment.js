'use strict';
// Deployment metadata: a site-wide banner while a deployment or incident is in progress, restart markers and rollout windows for the charts.
// Charts subscribe with DemoDeployment.page.subscribe. Requires status.js for polling.
(function (root) {
  const METADATA_PATH = '/deployment.json';
  const INTERVAL = 7500;
  const DEFAULT_MESSAGE = 'A new deployment is in progress. Connections and charts may take a few minutes to recover.';
  const MAX_MESSAGE = 300;
  const MAX_LABEL = 80;
  const MAX_DETAILS = 300;
  const MAX_MARKERS = 50;
  const MAX_WINDOW_DETAILS = 600;
  const MAX_WINDOWS = 10;
  const MAX_URL = 300;
  // Plausible Unix seconds only: rejects zero, and millisecond values fail the upper bound.
  const MIN_AT = Date.UTC(2026, 0, 1) / 1000;
  const MAX_AT = Date.UTC(2100, 0, 1) / 1000;

  const isNumber = value => typeof value === 'number' && Number.isFinite(value);
  const isTime = value => isNumber(value) && value >= MIN_AT && value < MAX_AT;
  const clean = (value, max) => typeof value === 'string' ? value.replace(/\s+/g, ' ').trim().slice(0, max) : '';

  /** An absolute https URL, normalized, or null. */
  function httpsUrl(value) {
    if (typeof value !== 'string' || value.length > MAX_URL) return null;
    try {
      const url = new URL(value);
      return url.protocol === 'https:' ? url.href : null;
    } catch {
      return null;
    }
  }

  /** Valid markers, sorted by time, one per instant. Malformed entries are dropped individually. */
  function markers(list) {
    const seen = new Set();
    const out = [];
    for (const item of Array.isArray(list) ? list : []) {
      if (!item || typeof item !== 'object' || !isTime(item.at)) continue;
      const label = clean(item.label, MAX_LABEL);
      if (!label || seen.has(item.at)) continue;
      seen.add(item.at);
      out.push({at: item.at, label, details: clean(item.details, MAX_DETAILS), url: httpsUrl(item.url)});
    }
    return out.sort((a, b) => a.at - b.at).slice(-MAX_MARKERS);
  }

  /**
   * Valid windows, sorted by start. A rollout window brackets a period whose data should be ignored,
   * such as several restarts in one rollout; an incident window marks an external outage and may link to
   * its status page. Malformed or overlapping entries are dropped individually.
   */
  function windows(list) {
    const out = [];
    for (const item of Array.isArray(list) ? list : []) {
      if (!item || typeof item !== 'object' || !isTime(item.start) || !isTime(item.end) || item.start >= item.end) continue;
      const label = clean(item.label, MAX_LABEL);
      if (!label) continue;
      out.push({start: item.start, end: item.end, label, details: clean(item.details, MAX_WINDOW_DETAILS),
        kind: item.kind === 'incident' ? 'incident' : 'rollout', url: httpsUrl(item.url)});
    }
    out.sort((a, b) => a.start - b.start);
    const kept = [];
    for (const item of out) if (!kept.length || item.start >= kept[kept.length - 1].end) kept.push(item);
    return kept.slice(-MAX_WINDOWS);
  }

  /** An optional banner link. Only absolute https URLs are kept; anything else drops the link, not the banner. */
  function link(value) {
    if (!value || typeof value !== 'object') return null;
    const label = clean(value.label, MAX_LABEL);
    const url = httpsUrl(value.url);
    return label && url ? {url, label} : null;
  }

  /** Parses a metadata body. Returns null for anything that is not schema 1, so callers keep their last good state. */
  function parse(body) {
    if (!body || typeof body !== 'object' || Array.isArray(body) || body.schema_version !== 1) return null;
    if (typeof body.in_progress !== 'boolean') return null;
    if (body.markers !== undefined && !Array.isArray(body.markers)) return null;
    if (body.windows !== undefined && !Array.isArray(body.windows)) return null;
    return {
      inProgress: body.in_progress,
      message: clean(body.message, MAX_MESSAGE) || DEFAULT_MESSAGE,
      startedAt: isTime(body.started_at) ? body.started_at : null,
      markers: markers(body.markers),
      windows: windows(body.windows),
      link: link(body.link),
    };
  }

  const signature = state => JSON.stringify(state);

  /** Polls the metadata file while the page is visible; transient failures keep the last known state. */
  function createDeployment(env = root, S = root.DemoStatus) {
    const listeners = new Set();
    let state = null;

    const poller = S.createPoller({
      env,
      interval: INTERVAL,
      url: () => METADATA_PATH,
      onResult(result) {
        const next = result.ok ? parse(result.body) : null;
        if (!next) return false;
        if (!state || signature(next) !== signature(state)) {
          state = next;
          for (const listener of listeners) listener(state);
        }
        return true;
      },
    });

    return {
      subscribe(listener) { listeners.add(listener); if (state) listener(state); return () => listeners.delete(listener); },
      start: () => poller.start(),
      refresh: () => poller.now(),
      get state() { return state; },
      get failures() { return poller.failures; },
    };
  }

  function bannerText(state, clock) {
    const started = state.startedAt && clock ? clock(state.startedAt) : null;
    return started ? `${state.message} Started ${started}.` : state.message;
  }

  /** Shows the top banner only while the last valid metadata says a deployment or incident is in progress. */
  function bindBanner(deployment, doc, clock) {
    const element = doc.getElementById('deployment-banner');
    if (!element) return;
    const text = element.querySelector('.deployment-banner-text') || element;
    const anchor = element.querySelector('.deployment-banner-link');
    deployment.subscribe(state => {
      element.hidden = !state.inProgress;
      text.textContent = state.inProgress ? bannerText(state, clock) : '';
      if (!anchor) return;
      const shown = state.inProgress && state.link;
      anchor.hidden = !shown;
      anchor.textContent = shown ? state.link.label : '';
      if (shown) anchor.href = state.link.url;
      else anchor.removeAttribute('href');
    });
  }

  root.DemoDeployment = {parse, markers, windows, bannerText, createDeployment, bindBanner, DEFAULT_MESSAGE, INTERVAL, METADATA_PATH};

  if (root.document && root.document.getElementById && root.DemoStatus && root.DemoStatus.createPoller) {
    const deployment = createDeployment(root, root.DemoStatus);
    root.DemoDeployment.page = deployment;
    bindBanner(deployment, root.document, root.DemoStatus.clock);
    // Chart scripts subscribe before polling starts; subscribers also receive the current state.
    root.addEventListener('DOMContentLoaded', () => deployment.start());
  }
})(typeof window !== 'undefined' ? window : globalThis);
