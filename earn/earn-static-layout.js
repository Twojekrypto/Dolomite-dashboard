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
  };

  if(root.document.readyState === 'loading') root.document.addEventListener('DOMContentLoaded', init, {once:true});
  else init();
})(typeof window !== 'undefined' ? window : null, function(){
  'use strict';

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

  function applyAll(documentLike){
    if(!documentLike) return;
    Object.entries(TABLE_SELECTORS).forEach(([name, selector]) => {
      applyLayout(name, documentLike.querySelector(selector), documentLike);
    });
  }

  return {STATIC_LAYOUTS, applyLayout, applyAll};
});
