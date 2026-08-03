(function(root, factory){
  const api = factory();
  if(typeof module === 'object' && module.exports) module.exports = api;
  if(!root || !root.document) return;

  let queued = false;
  const schedule = () => {
    if(queued) return;
    queued = true;
    (root.requestAnimationFrame || root.setTimeout)(() => {
      queued = false;
      api.applyAll(root.document);
    });
  };

  const init = () => {
    schedule();
    new root.MutationObserver(schedule).observe(root.document.body, {childList:true, subtree:true});
    root.addEventListener('resize', schedule, {passive:true});
  };

  if(root.document.readyState === 'loading') root.document.addEventListener('DOMContentLoaded', init, {once:true});
  else init();
})(typeof window !== 'undefined' ? window : null, function(){
  'use strict';

  const MOBILE_BREAKPOINT = 560;

  // Exact export saved from the local EARN layout editor on 2026-07-22.
  const STATIC_LAYOUTS = {
    supply: {
      order: ['token', 'price', 'supply', 'balance', 'yield', 'quality', 'details'],
      widths: {token:10.855785, price:6.418384, supply:10.935024, balance:32.26011, yield:10.988907, quality:7.117275, details:8.792393},
    },
    borrow: {
      order: ['health', 'emode', 'spacer', 'collateral', 'debt', 'pnl', 'details'],
      widths: {health:15.524564, emode:8.70206, spacer:19.172405, collateral:8.027274, debt:6.679278, pnl:6.190847, details:7.971473},
    },
    past: {
      order: ['token', 'spacer', 'yield', 'quality', 'details'],
      widths: {token:13.379165, spacer:52.817557, yield:11.105766, quality:7.103011, details:10},
    },
  };

  const TABLE_SELECTORS = {
    supply: '#earn-supply-section [data-earn-layout-table="supply"]',
    borrow: '#earn-lending-section [data-earn-layout-table="borrow"]',
    past: '#earn-past-section [data-earn-layout-table="past"]',
  };

  const MOBILE_ORDERS = {
    supply: ['token', 'quality', 'price', 'supply', 'balance', 'yield', 'details'],
    borrow: ['health', 'emode', 'collateral', 'debt', 'pnl', 'details'],
    past: ['token', 'quality', 'yield', 'details'],
  };

  function directColumn(parent, key){
    return [...(parent?.children || [])].find(child => child.dataset?.column === key) || null;
  }

  function ensureSpacer(documentLike, table, enabled){
    const parts = [
      [table.querySelector('colgroup'), 'col'],
      [table.tHead?.rows?.[0], 'th'],
      ...[...table.querySelectorAll('tbody tr[data-earn-layout-row]')].map(row => [row, 'td']),
    ];
    parts.forEach(([parent, tag]) => {
      if(!parent) return;
      const existing = directColumn(parent, 'spacer');
      if(!enabled){
        existing?.remove();
        return;
      }
      if(existing) return;
      const spacer = documentLike.createElement(tag);
      spacer.dataset.column = 'spacer';
      spacer.setAttribute('aria-hidden', 'true');
      parent.append(spacer);
    });
  }

  function reorderColumns(parent, order){
    if(!parent) return;
    const present = order.filter(key => directColumn(parent, key));
    const current = [...parent.children].map(child => child.dataset?.column).filter(Boolean);
    if(current.length === present.length && current.every((key, index) => key === present[index])) return;
    present.forEach(key => parent.append(directColumn(parent, key)));
  }

  function clearDesktopLayout(table){
    if(!table) return;
    table.classList.remove('earn-static-layout');
    table.style.removeProperty('width');
    table.querySelectorAll('colgroup > col').forEach(col => col.style.removeProperty('width'));
  }

  function applyMobileLayout(name, table, documentLike){
    const order = MOBILE_ORDERS[name];
    if(!table || !order || !documentLike) return;
    clearDesktopLayout(table);
    ensureSpacer(documentLike, table, false);
    const colgroup = table.querySelector('colgroup');
    reorderColumns(colgroup, order);
    reorderColumns(table.tHead?.rows?.[0], order);
    table.querySelectorAll('tbody tr[data-earn-layout-row]').forEach(row => reorderColumns(row, order));
    table.querySelectorAll('[data-earn-layout-detail] > td, .earn-table-spacer > td').forEach(cell => {
      cell.colSpan = order.length;
    });
  }

  function applyLayout(name, table, documentLike){
    const layout = STATIC_LAYOUTS[name];
    if(!table || !layout || !documentLike) return;
    if(name === 'supply') table.classList.remove('no-yield');
    table.classList.add('earn-static-layout');
    ensureSpacer(documentLike, table, layout.order.includes('spacer'));
    const totalWidth = layout.order.reduce((sum, key) => sum + layout.widths[key], 0);
    table.style.setProperty('width', `${Math.max(100, totalWidth)}%`, 'important');
    const colgroup = table.querySelector('colgroup');
    layout.order.forEach(key => {
      const col = directColumn(colgroup, key);
      if(col) col.style.setProperty('width', `${layout.widths[key]}%`, 'important');
    });
    reorderColumns(colgroup, layout.order);
    reorderColumns(table.tHead?.rows?.[0], layout.order);
    table.querySelectorAll('tbody tr[data-earn-layout-row]').forEach(row => reorderColumns(row, layout.order));
    table.querySelectorAll('[data-earn-layout-detail] > td, .earn-table-spacer > td').forEach(cell => {
      cell.colSpan = layout.order.length;
    });
  }

  function syncVerifiedMetadata(documentLike){
    documentLike.querySelectorAll('[data-earn-verified-meta]').forEach(meta => {
      const section = meta.closest('.earn-section-card');
      const hasRows = !!section?.querySelector('tbody tr[data-earn-layout-row]');
      if(!hasRows){
        delete meta.dataset.verifiedAt;
        if(meta.textContent !== 'Verified at · waiting') meta.textContent = 'Verified at · waiting';
        return;
      }
      if(!meta.dataset.verifiedAt) meta.dataset.verifiedAt = new Date().toISOString();
      const timestamp = new Date(meta.dataset.verifiedAt);
      const time = timestamp.toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit', timeZone:'UTC'});
      const label = `Verified at · ${time} UTC`;
      if(meta.textContent !== label) meta.textContent = label;
    });
  }

  function applyAll(documentLike){
    if(!documentLike) return;
    const viewportWidth = Number(documentLike.defaultView?.innerWidth || 0);
    const useMobileLayout = viewportWidth > 0 && viewportWidth <= MOBILE_BREAKPOINT;
    Object.entries(TABLE_SELECTORS).forEach(([name, selector]) => {
      const table = documentLike.querySelector(selector);
      if(useMobileLayout) applyMobileLayout(name, table, documentLike);
      else applyLayout(name, table, documentLike);
    });
    syncVerifiedMetadata(documentLike);
  }

  return {STATIC_LAYOUTS, MOBILE_BREAKPOINT, clearDesktopLayout, applyMobileLayout, syncVerifiedMetadata, applyLayout, applyAll};
});
