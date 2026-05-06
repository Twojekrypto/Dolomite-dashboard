(function () {
  function ensureTip() {
    var tip = document.getElementById('unified-tooltip');
    if (!tip) {
      tip = document.createElement('div');
      tip.id = 'unified-tooltip';
      document.body.appendChild(tip);
    }
    var arrow = document.getElementById('unified-tooltip-arrow');
    if (!arrow) {
      arrow = document.createElement('div');
      arrow.id = 'unified-tooltip-arrow';
      document.body.appendChild(arrow);
    }
    return { tip: tip, arrow: arrow };
  }

  function cleanText(value) {
    return String(value || '').replace(/\s+/g, ' ').trim();
  }

  function tooltipText(target) {
    var addr = target.closest && target.closest('.addr-tooltip-wrap[data-full-addr]');
    if (addr) return { text: addr.getAttribute('data-full-addr'), target: addr, mono: true };

    var explicit = target.closest && target.closest('[data-tooltip]');
    if (explicit) return { text: explicit.getAttribute('data-tooltip'), target: explicit, mono: explicit.dataset.tooltipMono === 'true' };

    var tipAttr = target.closest && target.closest('[data-tip]');
    if (tipAttr) {
      var attrText = tipAttr.getAttribute('data-tip');
      if (attrText) return { text: attrText, target: tipAttr, mono: false };
      var attrBubble = tipAttr.querySelector('.tooltip-bubble');
      if (attrBubble) return { text: cleanText(attrBubble.textContent), target: tipAttr, mono: false };
    }

    var inline = target.closest && target.closest('.yield-help-icon, .breakdown-help-icon, .tooltip-icon, .tooltip-wrap');
    if (inline) {
      var bubble = inline.querySelector('.tooltip-bubble');
      if (bubble) return { text: cleanText(bubble.textContent), target: inline, mono: false };
    }

    return null;
  }

  function position(tip, arrow, target) {
    var rect = target.getBoundingClientRect();
    tip.style.display = 'block';
    var tw = tip.offsetWidth;
    var th = tip.offsetHeight || 28;
    var left = rect.left + rect.width / 2 - tw / 2;
    if (left < 4) left = 4;
    if (left + tw > window.innerWidth - 4) left = window.innerWidth - tw - 4;
    tip.style.left = left + 'px';

    var arrowLeft = Math.min(Math.max(rect.left + rect.width / 2 - 5, 8), window.innerWidth - 14);
    if (rect.top - th - 8 > 0) {
      tip.style.top = (rect.top - th - 6) + 'px';
      arrow.style.cssText = 'position:fixed;z-index:999999;width:0;height:0;border:5px solid transparent;border-top-color:rgba(201,162,39,.55);pointer-events:none;opacity:1;transition:opacity .15s ease;filter:drop-shadow(0 2px 6px rgba(0,0,0,.55));';
      arrow.style.left = arrowLeft + 'px';
      arrow.style.top = (rect.top - 6) + 'px';
    } else {
      tip.style.top = (rect.bottom + 6) + 'px';
      arrow.style.cssText = 'position:fixed;z-index:999999;width:0;height:0;border:5px solid transparent;border-bottom-color:rgba(201,162,39,.55);pointer-events:none;opacity:1;transition:opacity .15s ease;filter:drop-shadow(0 2px 6px rgba(0,0,0,.55));';
      arrow.style.left = arrowLeft + 'px';
      arrow.style.top = (rect.bottom - 4) + 'px';
    }
  }

  var activeTarget = null;
  var lastPointer = { x: 0, y: 0 };
  var hideTimer = null;

  function hide() {
    var els = ensureTip();
    els.tip.style.opacity = '0';
    els.tip.style.display = 'none';
    els.arrow.style.opacity = '0';
    activeTarget = null;
  }

  function show(data) {
    if (hideTimer) {
      clearTimeout(hideTimer);
      hideTimer = null;
    }
    activeTarget = data.target;
    var els = ensureTip();
    els.tip.textContent = data.text;
    els.tip.classList.toggle('is-mono', !!data.mono);
    els.tip.style.opacity = '1';
    position(els.tip, els.arrow, data.target);
  }

  function scheduleHide() {
    if (hideTimer) clearTimeout(hideTimer);
    hideTimer = setTimeout(function () {
      var el = document.elementFromPoint(lastPointer.x, lastPointer.y);
      if (activeTarget && el && activeTarget.contains(el)) return;
      hide();
    }, 80);
  }

  document.addEventListener('mouseover', function (event) {
    lastPointer = { x: event.clientX, y: event.clientY };
    var data = tooltipText(event.target);
    if (!data || !data.text) return;
    show(data);
  });

  document.addEventListener('mousemove', function (event) {
    lastPointer = { x: event.clientX, y: event.clientY };
    var data = tooltipText(event.target);
    if (!data || !data.text) return;
    if (data.target !== activeTarget) show(data);
  });

  document.addEventListener('mouseout', function (event) {
    lastPointer = { x: event.clientX, y: event.clientY };
    var data = tooltipText(event.target);
    if (!data) return;
    var related = event.relatedTarget;
    if (related && data.target.contains && data.target.contains(related)) return;
    scheduleHide();
  });

  window.addEventListener('scroll', hide, true);
  window.addEventListener('resize', hide);
})();
