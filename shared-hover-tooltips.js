(function () {
  if (window.__DOLO_ADDRESS_MATCH_LOADED) return;
  window.__DOLO_ADDRESS_MATCH_LOADED = true;

  var ACTION_TOOLTIP_SELECTOR = [
    '.copy-addr-icon',
    '.addr-copy',
    '.addr-debank',
    '.debank-icon',
    '.latest-tx',
    '.tx-ext-icon',
    '.tx-only-link',
    '.proto-copy',
    '.proto-explore',
    '.assets-ext-link',
    '.search-clear',
    '.dd-clear',
    '.trigger-clear',
    '.col-filter-clear',
    '.col-filter-btn',
    '.pg-btn',
    '.flow-pager-btn',
    '.token-ca-copy',
    '.asset-toggle',
    '.holder-toggle',
    '.ex-toggle',
    '.modal-close',
    '.supply-activity-type-clear',
    '.earn-dolo-proto-copy',
    '.earn-dolo-proto-explore',
    '[data-page]',
    '[data-flow-page]',
    '[data-latest-page]'
  ].join(',');

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

  function normalizeMatchAddress(value) {
    var address = cleanText(value).toLowerCase();
    return /^0x[a-f0-9]{40}$/.test(address) ? address : '';
  }

  function isDisplayedAddressTrigger(trigger) {
    var text = cleanText(trigger && trigger.textContent).toLowerCase();
    return /^0x[a-f0-9]{40}$/.test(text) || /^0x[a-f0-9]{4,}(?:\.{3}|…)[a-f0-9]{4}$/.test(text);
  }

  function isRenderedAddressTrigger(trigger) {
    if (!isDisplayedAddressTrigger(trigger)) return false;
    if (typeof trigger.getClientRects === 'function' && trigger.getClientRects().length === 0) return false;
    return true;
  }

  function addressMatchTrigger(target) {
    var trigger = target.closest && target.closest('.addr-tooltip-wrap[data-full-addr]');
    var table = trigger && trigger.closest('table[data-address-match-cells]');
    var address = trigger && normalizeMatchAddress(trigger.getAttribute('data-full-addr'));
    return table && address && isRenderedAddressTrigger(trigger) ? { mode: 'address', trigger: trigger, table: table, address: address } : null;
  }

  function rowAddressMatchData(target) {
    var row = target.closest && target.closest('tr');
    var table = row && row.closest('table[data-address-match-cells]');
    if (!row || !table || !row.parentElement || row.parentElement.tagName !== 'TBODY') return null;

    var addresses = [];
    row.querySelectorAll('.addr-tooltip-wrap[data-full-addr]').forEach(function (trigger) {
      var address = normalizeMatchAddress(trigger.getAttribute('data-full-addr'));
      if (!address || !isRenderedAddressTrigger(trigger) || addresses.indexOf(address) !== -1) return;
      addresses.push(address);
    });
    return addresses.length ? { mode: 'row', row: row, table: table, addresses: addresses } : null;
  }

  function matchDataForTarget(target) {
    return addressMatchTrigger(target) || rowAddressMatchData(target);
  }

  function isAddress(value) {
    return /^0x[a-f0-9]{40}$/i.test(cleanText(value));
  }

  function isActionTooltip(target, value) {
    var text = cleanText(value).toLowerCase();
    if (!text) return false;
    if (target.closest && target.closest(ACTION_TOOLTIP_SELECTOR)) return true;
    return /^(copy address|copy ca|view on|open in|open transaction|close|clear( search| filter| chain filter| category filter| hf filter| collateral filter| debt filter| collateral token filter| debt token filter| activity filter)?|first|previous|next|last|show asset details|hide asset details|show exercise history|hide exercise history|show position details|hide position details|sort by\b)/i.test(text);
  }

  function forEachMatch(root, selector, callback) {
    var scope = root && root.nodeType === 1 ? root : document;
    if (scope.matches && scope.matches(selector)) callback(scope);
    scope.querySelectorAll(selector).forEach(callback);
  }

  function normalizeTooltipAttributes(root) {
    forEachMatch(root, '[title]', function (el) {
      var text = el.getAttribute('title');
      if (isActionTooltip(el, text)) {
        el.removeAttribute('title');
        return;
      }
      if (!el.dataset.tooltip) el.dataset.tooltip = text;
      if (isAddress(text)) el.dataset.tooltipMono = 'true';
      el.removeAttribute('title');
    });

    forEachMatch(root, '[data-tooltip], [data-tip]', function (el) {
      var text = el.getAttribute('data-tooltip') || el.getAttribute('data-tip');
      if (!isActionTooltip(el, text)) return;
      el.removeAttribute('data-tooltip');
      el.removeAttribute('data-tip');
    });
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
  var activeAddressMatch = null;
  var lastPointer = { x: 0, y: 0 };
  var hideTimer = null;

  function clearAddressMatches() {
    if (!activeAddressMatch) return;
    activeAddressMatch.elements.forEach(function (element) {
      element.classList.remove('address-match-active', 'address-match-source', 'address-match-peer');
    });
    activeAddressMatch = null;
  }

  function showAddressMatches(data) {
    if (activeAddressMatch && activeAddressMatch.mode === 'address' && activeAddressMatch.table === data.table && activeAddressMatch.trigger === data.trigger && activeAddressMatch.address === data.address) return;
    clearAddressMatches();

    var elements = [];
    data.table.querySelectorAll('.addr-tooltip-wrap[data-full-addr]').forEach(function (trigger) {
      if (normalizeMatchAddress(trigger.getAttribute('data-full-addr')) !== data.address) return;
      if (!isRenderedAddressTrigger(trigger)) return;
      elements.push(trigger);
    });

    if (elements.length < 2) return;

    elements.forEach(function (element) {
      element.classList.add('address-match-active');
      element.classList.add(element === data.trigger ? 'address-match-source' : 'address-match-peer');
    });
    activeAddressMatch = { mode: 'address', table: data.table, trigger: data.trigger, address: data.address, elements: elements };
  }

  function showRowAddressMatches(data) {
    if (activeAddressMatch && activeAddressMatch.mode === 'row' && activeAddressMatch.row === data.row) return;
    clearAddressMatches();

    var candidatesByAddress = {};
    data.addresses.forEach(function (address) {
      candidatesByAddress[address] = [];
    });
    data.table.querySelectorAll('.addr-tooltip-wrap[data-full-addr]').forEach(function (trigger) {
      if (!isRenderedAddressTrigger(trigger)) return;
      var address = normalizeMatchAddress(trigger.getAttribute('data-full-addr'));
      if (!address || !candidatesByAddress[address]) return;
      candidatesByAddress[address].push(trigger);
    });

    var elements = [];
    Object.keys(candidatesByAddress).forEach(function (address) {
      var candidates = candidatesByAddress[address];
      var hasPeerRow = candidates.some(function (trigger) {
        return !data.row.contains(trigger);
      });
      if (!hasPeerRow) return;
      candidates.forEach(function (trigger) {
        if (elements.indexOf(trigger) === -1) elements.push(trigger);
      });
    });
    if (!elements.length) return;

    elements.forEach(function (element) {
      element.classList.add('address-match-active', 'address-match-peer');
    });
    activeAddressMatch = { mode: 'row', table: data.table, row: data.row, elements: elements };
  }

  function hideTooltip() {
    var els = ensureTip();
    els.tip.style.opacity = '0';
    els.tip.style.display = 'none';
    els.arrow.style.opacity = '0';
    activeTarget = null;
  }

  function hide() {
    hideTooltip();
    clearAddressMatches();
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

  function reconcileAddressMatchAfterViewportChange() {
    if (!activeAddressMatch) return;
    var elementAtPointer = document.elementFromPoint(lastPointer.x, lastPointer.y);
    if (elementAtPointer) showMatchForTarget(elementAtPointer);
    else clearAddressMatches();
  }

  var hasInlineTooltipSystem = !!window.__DOLO_INLINE_TOOLTIP_ACTIVE;

  if (!hasInlineTooltipSystem) {
    document.addEventListener('mouseover', function (event) {
      lastPointer = { x: event.clientX, y: event.clientY };
      var data = tooltipText(event.target);
      if (!data || !data.text) return;
      show(data);
    });

    document.addEventListener('mousemove', function (event) {
      lastPointer = { x: event.clientX, y: event.clientY };
      var data = tooltipText(event.target);
      if (!data || !data.text) {
        if (activeTarget) scheduleHide();
        return;
      }
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

    document.addEventListener('focusin', function (event) {
      var data = tooltipText(event.target);
      if (!data || !data.text) return;
      show(data);
    });

    document.addEventListener('focusout', function (event) {
      if (tooltipText(event.target)) scheduleHide();
    });

    normalizeTooltipAttributes(document);
    new MutationObserver(function (records) {
      records.forEach(function (record) {
        if (record.type === 'childList') {
          record.addedNodes.forEach(function (node) {
            if (node.nodeType === 1) normalizeTooltipAttributes(node);
          });
        } else if (record.type === 'attributes' && record.target && record.target.nodeType === 1) {
          normalizeTooltipAttributes(record.target);
        }
      });
    }).observe(document.documentElement, { childList: true, subtree: true, attributes: true, attributeFilter: ['title', 'data-tip', 'data-tooltip'] });

    var handleViewportChange = function () {
      hideTooltip();
      reconcileAddressMatchAfterViewportChange();
    };
    window.addEventListener('scroll', handleViewportChange, true);
    window.addEventListener('resize', handleViewportChange);
  } else {
    window.addEventListener('scroll', reconcileAddressMatchAfterViewportChange, true);
    window.addEventListener('resize', reconcileAddressMatchAfterViewportChange);
  }

  function showMatchForTarget(target) {
    var data = matchDataForTarget(target);
    if (!data) {
      clearAddressMatches();
    } else if (data.mode === 'address') {
      showAddressMatches(data);
    } else {
      showRowAddressMatches(data);
    }
  }

  function handleAddressMatchOver(event) {
    lastPointer = { x: event.clientX, y: event.clientY };
    showMatchForTarget(event.target);
  }

  function handleAddressMatchOut(event) {
    lastPointer = { x: event.clientX, y: event.clientY };
    if (event.relatedTarget) showMatchForTarget(event.relatedTarget);
    else clearAddressMatches();
  }

  document.addEventListener('pointerover', handleAddressMatchOver);
  document.addEventListener('mouseover', handleAddressMatchOver);
  document.addEventListener('pointerout', handleAddressMatchOut);
  document.addEventListener('mouseout', handleAddressMatchOut);
  document.addEventListener('pointerdown', function (event) {
    if (event.pointerType && event.pointerType !== 'mouse' && activeAddressMatch && activeAddressMatch.mode === 'row') {
      clearAddressMatches();
    }
  });

  document.addEventListener('focusin', function (event) {
    var addressData = addressMatchTrigger(event.target);
    if (addressData) showAddressMatches(addressData);
  });

  document.addEventListener('focusout', function (event) {
    if (!activeAddressMatch || !activeAddressMatch.trigger.contains(event.target)) return;
    if (event.relatedTarget && activeAddressMatch.trigger.contains(event.relatedTarget)) return;
    clearAddressMatches();
  });
  window.addEventListener('blur', clearAddressMatches);

  var initiallyHoveredAddress = document.querySelector('.addr-tooltip-wrap[data-full-addr]:hover');
  var initialAddressMatch = initiallyHoveredAddress && addressMatchTrigger(initiallyHoveredAddress);
  if (initialAddressMatch) showAddressMatches(initialAddressMatch);
})();
