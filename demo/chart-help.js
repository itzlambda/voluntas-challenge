'use strict';
// Keep chart definitions beside their title and within the viewport.
(function () {
  const heads = document.querySelectorAll('.chart-head, .figure');
  for (const head of heads) {
    const button = head.querySelector('.chart-help');
    const tip = head.querySelector('.chart-tooltip');
    if (!button || !tip) continue;
    let closeTimer;
    const isOpen = () => tip.matches(':popover-open');
    const cancelClose = () => clearTimeout(closeTimer);
    function position() {
      const rect = button.getBoundingClientRect();
      const margin = 16;
      const below = window.innerHeight - rect.bottom - margin - 6;
      const above = rect.top - margin - 6;
      const placeBelow = below >= Math.min(tip.scrollHeight, 320) || below >= above;
      tip.style.maxHeight = `${Math.max(80, placeBelow ? below : above)}px`;
      tip.style.left = `${Math.max(margin, Math.min(rect.left, window.innerWidth - tip.offsetWidth - margin))}px`;
      tip.style.top = `${placeBelow ? rect.bottom + 6 : Math.max(margin, rect.top - tip.offsetHeight - 6)}px`;
    }
    function show() {
      cancelClose();
      if (!isOpen()) tip.showPopover();
      button.setAttribute('aria-expanded', 'true');
      position();
    }
    function hide() {
      cancelClose();
      if (isOpen()) tip.hidePopover();
      button.setAttribute('aria-expanded', 'false');
    }
    function scheduleClose() {
      cancelClose();
      closeTimer = setTimeout(() => {
        if (!head.contains(document.activeElement) && !button.matches(':hover') && !tip.matches(':hover')) hide();
      }, 150);
    }
    button.addEventListener('pointerenter', event => { if (event.pointerType !== 'touch') show(); });
    button.addEventListener('pointerleave', scheduleClose);
    button.addEventListener('focus', show);
    // Focus may already have opened the tooltip on the first tap.
    button.addEventListener('click', show);
    head.addEventListener('focusout', scheduleClose);
    tip.addEventListener('pointerenter', cancelClose);
    tip.addEventListener('pointerleave', scheduleClose);
    tip.addEventListener('toggle', () => button.setAttribute('aria-expanded', String(isOpen())));
    head.addEventListener('keydown', event => {
      if (event.key === 'Escape') hide();
    });
    window.addEventListener('resize', () => { if (isOpen()) position(); });
    window.addEventListener('scroll', event => {
      if (!isOpen() || tip.contains(event.target)) return;
      const rect = button.getBoundingClientRect();
      if (rect.bottom < 0 || rect.top > window.innerHeight) hide();
      else position();
    }, true);
  }
})();
