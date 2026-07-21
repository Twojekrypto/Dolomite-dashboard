(function(root, factory){
  const api = factory(root);
  if(typeof module === "object" && module.exports) module.exports = api;
  if(root) root.EarnLayoutEditor = api;
})(typeof window !== "undefined" ? window : null, function(root){
  "use strict";

  const VERSION = 1;
  const SPACER = "spacer";
  const SPACER_WIDTH = 4;
  const RESIZE_SAFETY_FLOOR = 0.1;
  const LEGACY_SUPPLY_KEYS = ["token", "price", "supply", "balance", "yield", "details"];
  const LEGACY_BORROW_KEYS = ["health", "collateral", "debt", "pnl", "details"];
  const STORAGE_KEY = "dolomite:earn-layout-editor:v1";
  const EXPORT_NAME = "earn-layout-draft.json";
  const SCHEMAS = {
    supply: {
      keys:["token", "quality", "price", "supply", "balance", "yield", "details"],
      widths:{token:28, quality:11, price:9, supply:17, balance:16, yield:12, details:7},
    },
    borrow: {
      keys:["health", "emode", "collateral", "debt", "pnl", "details"],
      widths:{health:17, emode:11, collateral:23, debt:23, pnl:16, details:10},
    },
  };

  const clone = value => JSON.parse(JSON.stringify(value));
  const round = value => Number(Number(value).toFixed(6));
  const schemaFor = name => SCHEMAS[name] || null;
  const sumWidths = (order, widths) => order.reduce((sum, key) => sum + Number(widths[key] || 0), 0);

  function createDefaultLayout(name){
    const schema = schemaFor(name);
    if(!schema) return null;
    return {version:VERSION, order:schema.keys.slice(), widths:{...schema.widths}};
  }

  function normalizeLayout(name, value){
    const schema = schemaFor(name);
    if(!schema || !value || value.version !== VERSION || !Array.isArray(value.order) || !value.widths) return null;
    const order = value.order.slice();
    const hasSpacer = order.includes(SPACER);
    if(order.length !== schema.keys.length + (hasSpacer ? 1 : 0)) return null;
    if(new Set(order).size !== order.length) return null;
    if(schema.keys.some(key => !order.includes(key))) return null;
    if(order.some(key => !schema.keys.includes(key) && key !== SPACER)) return null;
    if(order.filter(key => key === SPACER).length > 1) return null;
    const widths = {};
    for(const key of order){
      const width = Number(value.widths[key]);
      if(!Number.isFinite(width) || width <= 0) return null;
      widths[key] = round(width);
    }
    const totalWidth = sumWidths(order, widths);
    if(!Number.isFinite(totalWidth) || totalWidth <= 0) return null;
    return {version:VERSION, order, widths};
  }

  function reorderLayout(name, layout, movedKey, targetKey, placeAfter){
    const current = normalizeLayout(name, layout);
    if(!current || movedKey === targetKey || !current.order.includes(movedKey) || !current.order.includes(targetKey)) return current || clone(layout);
    const order = current.order.filter(key => key !== movedKey);
    const targetIndex = order.indexOf(targetKey);
    order.splice(targetIndex + (placeAfter ? 1 : 0), 0, movedKey);
    return {...current, order};
  }

  function resizeLayout(name, layout, key, deltaPx, tableWidthPx){
    const schema = schemaFor(name);
    const current = normalizeLayout(name, layout);
    const tableWidth = Number(tableWidthPx);
    if(!schema || !current || !current.order.includes(key) || !(tableWidth > 0)) return current || clone(layout);
    const delta = Number(deltaPx) / tableWidth * 100;
    if(!Number.isFinite(delta)) return current;
    const widths = {...current.widths};
    // Columns may shrink freely; the small non-zero floor only keeps the saved layout valid.
    widths[key] = round(Math.max(RESIZE_SAFETY_FLOOR, widths[key] + delta));
    return {...current, widths};
  }

  function getLayoutTableWidth(name, layout){
    const valid = normalizeLayout(name, layout);
    return valid ? Math.max(100, sumWidths(valid.order, valid.widths)) : 100;
  }

  function addSpacer(name, layout){
    const current = normalizeLayout(name, layout);
    if(!schemaFor(name) || !current || current.order.includes(SPACER)) return current || clone(layout);
    const donor = current.order.reduce((largest, key) => current.widths[key] > current.widths[largest] ? key : largest, current.order[0]);
    if(current.widths[donor] <= SPACER_WIDTH) return current;
    return {
      version:VERSION,
      order:current.order.concat(SPACER),
      widths:{...current.widths, [donor]:round(current.widths[donor] - SPACER_WIDTH), [SPACER]:SPACER_WIDTH},
    };
  }

  function removeSpacer(name, layout){
    const current = normalizeLayout(name, layout);
    if(!schemaFor(name) || !current || !current.order.includes(SPACER)) return current || clone(layout);
    const spacerIndex = current.order.indexOf(SPACER);
    const recipient = current.order[spacerIndex + 1] || current.order[spacerIndex - 1];
    const order = current.order.filter(key => key !== SPACER);
    const widths = {...current.widths, [recipient]:round(current.widths[recipient] + current.widths[SPACER])};
    delete widths[SPACER];
    return {version:VERSION, order, widths};
  }

  function isLocalEditorEnabled(locationLike){
    if(!locationLike) return false;
    const loopbackHosts = new Set(["localhost", "127.0.0.1", "::1", "[::1]"]);
    if(!loopbackHosts.has(String(locationLike.hostname || "").toLowerCase())) return false;
    return new URLSearchParams(String(locationLike.search || "")).get("layoutEditor") === "1";
  }

  function createDefaultSavedLayouts(){
    return {version:VERSION, supply:createDefaultLayout("supply"), borrow:createDefaultLayout("borrow")};
  }

  function migrateSupplyLayout(value){
    if(!value || value.version !== VERSION || !Array.isArray(value.order) || !value.widths) return value;
    const legacyKeys = value.order.filter(key => key !== SPACER);
    if(legacyKeys.length !== LEGACY_SUPPLY_KEYS.length || LEGACY_SUPPLY_KEYS.some(key => !legacyKeys.includes(key))) return value;
    const order = value.order.slice();
    order.splice(order.indexOf("token") + 1, 0, "quality");
    return {...value, order, widths:{...value.widths, quality:11}};
  }

  function migrateBorrowLayout(value){
    if(!value || value.version !== VERSION || !Array.isArray(value.order) || !value.widths) return value;
    const legacyKeys = value.order.filter(key => key !== SPACER);
    if(legacyKeys.length !== LEGACY_BORROW_KEYS.length || LEGACY_BORROW_KEYS.some(key => !legacyKeys.includes(key))) return value;
    const order = value.order.slice();
    order.splice(order.indexOf("health") + 1, 0, "emode");
    return {...value, order, widths:{...value.widths, emode:11}};
  }

  function normalizeSavedLayouts(value){
    if(!value || value.version !== VERSION) return null;
    const supply = normalizeLayout("supply", migrateSupplyLayout(value.supply));
    const borrow = normalizeLayout("borrow", migrateBorrowLayout(value.borrow));
    if(!supply || !borrow) return null;
    return {version:VERSION, supply, borrow};
  }

  let editorLayouts = null;
  let editorInitialized = false;
  let reapplyQueued = false;

  function readStoredLayouts(){
    if(!root) return createDefaultSavedLayouts();
    try {
      return normalizeSavedLayouts(JSON.parse(root.localStorage.getItem(STORAGE_KEY))) || createDefaultSavedLayouts();
    } catch(error) {
      return createDefaultSavedLayouts();
    }
  }

  function persistLayouts(){
    const valid = normalizeSavedLayouts(editorLayouts);
    if(valid && root){
      editorLayouts = valid;
      root.localStorage.setItem(STORAGE_KEY, JSON.stringify(valid));
    }
    return valid;
  }

  function currentLayout(name){
    if(!editorLayouts) editorLayouts = readStoredLayouts();
    return editorLayouts[name];
  }

  function updateLayout(name, next){
    const valid = normalizeLayout(name, next);
    if(!valid) return;
    editorLayouts = {...(editorLayouts || readStoredLayouts()), [name]:valid};
    persistLayouts();
    reapply();
  }

  function directColumn(parent, key){
    return [...(parent?.children || [])].find(child => child.dataset?.column === key) || null;
  }

  function reorderColumns(parent, order){
    if(!parent) return;
    const present = order.filter(key => directColumn(parent, key));
    const current = [...parent.children].map(child => child.dataset?.column).filter(Boolean);
    if(current.length === present.length && current.every((key, index) => key === present[index])) return;
    present.forEach(key => parent.append(directColumn(parent, key)));
  }

  function ensureSpacer(table, enabled){
    const parts = [
      [table.querySelector('colgroup'), 'col'],
      [table.tHead?.rows?.[0], 'th'],
      ...[...table.querySelectorAll('tbody tr[data-earn-layout-row]')].map(row => [row, 'td']),
    ];
    parts.forEach(([parent, tag]) => {
      if(!parent) return;
      const existing = directColumn(parent, SPACER);
      if(!enabled){
        existing?.remove();
        return;
      }
      if(existing) return;
      const spacer = root.document.createElement(tag);
      spacer.dataset.column = SPACER;
      spacer.className = 'earn-layout-spacer';
      spacer.setAttribute('aria-hidden', 'true');
      if(tag === 'th') spacer.setAttribute('aria-label', 'Blank spacer');
      parent.append(spacer);
    });
  }

  function tableFor(name){
    if(!root?.document) return null;
    const selector = name === 'supply'
      ? '#earn-supply-section [data-earn-layout-table="supply"]'
      : '#earn-lending-section [data-earn-layout-table="borrow"]';
    return root.document.querySelector(selector);
  }

  function applyLayout(name, table, layout){
    const valid = normalizeLayout(name, layout);
    if(!table || !valid) return;
    if(name === 'supply') table.classList.remove('no-yield');
    ensureSpacer(table, valid.order.includes(SPACER));
    table.classList.add('earn-layout-editor-active');
    const colgroup = table.querySelector('colgroup');
    const header = table.tHead?.rows?.[0];
    table.style.setProperty('width', `${getLayoutTableWidth(name, valid)}%`, 'important');
    valid.order.forEach(key => {
      const col = directColumn(colgroup, key);
      if(col) col.style.setProperty('width', `${valid.widths[key]}%`, 'important');
    });
    reorderColumns(colgroup, valid.order);
    reorderColumns(header, valid.order);
    table.querySelectorAll('tbody tr[data-earn-layout-row]').forEach(row => reorderColumns(row, valid.order));
    table.querySelectorAll('[data-earn-layout-detail] > td, .earn-table-spacer > td').forEach(cell => {
      cell.colSpan = valid.order.length;
    });
    decorateHeaders(name, table, header ? header.querySelectorAll(':scope > [data-column]') : []);
  }

  function icon(name){
    const paths = {
      add:'<path d="M12 5v14M5 12h14"/>',
      remove:'<path d="M5 12h14"/>',
      reset:'<path d="M3 12a9 9 0 1 0 3-6.7"/><path d="M3 4v5h5"/>',
      save:'<path d="M5 3h12l4 4v14H5z"/><path d="M8 3v6h8V3M8 21v-7h8v7"/>',
      drag:'<path d="M9 5h.01M15 5h.01M9 12h.01M15 12h.01M9 19h.01M15 19h.01"/>',
    };
    return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths[name] || ''}</svg>`;
  }

  function createToolbar(name, section){
    let toolbar = section.querySelector(`:scope > [data-earn-layout-toolbar="${name}"]`);
    if(!toolbar){
      toolbar = root.document.createElement('div');
      toolbar.className = 'earn-layout-editor-toolbar';
      toolbar.dataset.earnLayoutToolbar = name;
      toolbar.innerHTML = `
        <button type="button" data-layout-action="add" aria-label="Add blank spacer" title="Add blank spacer">${icon('add')}</button>
        <button type="button" data-layout-action="remove" aria-label="Remove blank spacer" title="Remove blank spacer">${icon('remove')}</button>
        <button type="button" data-layout-action="reset" aria-label="Reset layout" title="Reset layout">${icon('reset')}</button>
        <button type="button" data-layout-action="save" aria-label="Save layout file" title="Save layout file">${icon('save')}</button>`;
      const shell = section.querySelector('.earn-section-table-shell');
      if(shell) section.insertBefore(toolbar, shell);
      else section.append(toolbar);
      toolbar.addEventListener('click', event => {
        const button = event.target.closest('[data-layout-action]');
        if(!button) return;
        const action = button.dataset.layoutAction;
        const layout = currentLayout(name);
        if(action === 'add') updateLayout(name, addSpacer(name, layout));
        if(action === 'remove') updateLayout(name, removeSpacer(name, layout));
        if(action === 'reset') updateLayout(name, createDefaultLayout(name));
        if(action === 'save') downloadLayouts();
      });
    }
    toolbar.querySelector('[data-layout-action="remove"]').disabled = !currentLayout(name).order.includes(SPACER);
  }

  function downloadLayouts(){
    const valid = persistLayouts();
    if(!valid || !root) return;
    const blob = new Blob([JSON.stringify(valid, null, 2)], {type:'application/json'});
    const href = root.URL.createObjectURL(blob);
    const link = root.document.createElement('a');
    link.href = href;
    link.download = EXPORT_NAME;
    link.click();
    root.URL.revokeObjectURL(href);
  }

  function decorateHeaders(name, table, headers){
    [...headers].forEach(header => {
      const key = header.dataset.column;
      if(!key) return;
      header.classList.add('earn-layout-editor-header');
      let drag = header.querySelector(':scope > .earn-layout-drag-handle');
      if(!drag){
        drag = root.document.createElement('button');
        drag.type = 'button';
        drag.className = 'earn-layout-drag-handle';
        drag.setAttribute('aria-label', `Move ${key === SPACER ? 'blank spacer' : key} column`);
        drag.title = 'Drag to move column';
        drag.innerHTML = icon('drag');
        header.append(drag);
        drag.addEventListener('click', event => event.stopPropagation());
        drag.addEventListener('pointerdown', event => {
          event.stopPropagation();
          startColumnDrag(event, name, table, key);
        });
      }
      let resize = header.querySelector(':scope > .earn-layout-resize-handle');
      if(!resize){
        resize = root.document.createElement('span');
        resize.className = 'earn-layout-resize-handle';
        resize.setAttribute('aria-hidden', 'true');
        header.append(resize);
        resize.addEventListener('pointerdown', event => {
          event.stopPropagation();
          startResize(event, name, table, key);
        });
      }
    });
  }

  function startColumnDrag(event, name, table, movedKey){
    event.preventDefault();
    const source = event.currentTarget;
    let targetHeader = null;
    const clearTarget = () => {
      targetHeader?.classList.remove('earn-layout-drop-target');
      targetHeader = null;
    };
    const move = moveEvent => {
      const next = root.document.elementFromPoint(moveEvent.clientX, moveEvent.clientY)
        ?.closest('.earn-layout-editor-header');
      if(!next || !table.contains(next) || next.dataset.column === movedKey){
        clearTarget();
        return;
      }
      if(next !== targetHeader){
        clearTarget();
        targetHeader = next;
        targetHeader.classList.add('earn-layout-drop-target');
      }
    };
    const stop = stopEvent => {
      root.removeEventListener('pointermove', move);
      root.removeEventListener('pointerup', stop);
      root.removeEventListener('pointercancel', stop);
      root.removeEventListener('blur', stop);
      root.document.removeEventListener('keydown', escape);
      source.releasePointerCapture?.(event.pointerId);
      if(targetHeader && stopEvent.type === 'pointerup'){
        const rect = targetHeader.getBoundingClientRect();
        updateLayout(name, reorderLayout(name, currentLayout(name), movedKey, targetHeader.dataset.column, stopEvent.clientX > rect.left + rect.width / 2));
      }
      clearTarget();
    };
    const escape = keyEvent => { if(keyEvent.key === 'Escape') stop({type:'cancel'}); };
    source.setPointerCapture?.(event.pointerId);
    root.addEventListener('pointermove', move);
    root.addEventListener('pointerup', stop, {once:true});
    root.addEventListener('pointercancel', stop, {once:true});
    root.addEventListener('blur', stop, {once:true});
    root.document.addEventListener('keydown', escape, {once:true});
  }

  function startResize(event, name, table, key){
    event.preventDefault();
    const source = event.currentTarget;
    const startX = event.clientX;
    const initial = currentLayout(name);
    const tableWidth = table.getBoundingClientRect().width;
    if(!(tableWidth > 0)) return;
    const move = moveEvent => updateLayout(name, resizeLayout(name, initial, key, moveEvent.clientX - startX, tableWidth));
    const stop = () => {
      root.removeEventListener('pointermove', move);
      root.removeEventListener('pointerup', stop);
      root.removeEventListener('pointercancel', stop);
      root.removeEventListener('blur', stop);
      root.document.removeEventListener('keydown', escape);
      source.releasePointerCapture?.(event.pointerId);
    };
    const escape = keyEvent => { if(keyEvent.key === 'Escape') stop(); };
    source.setPointerCapture?.(event.pointerId);
    root.addEventListener('pointermove', move);
    root.addEventListener('pointerup', stop, {once:true});
    root.addEventListener('pointercancel', stop, {once:true});
    root.addEventListener('blur', stop, {once:true});
    root.document.addEventListener('keydown', escape, {once:true});
  }

  function reapply(){
    if(!root?.document || !isLocalEditorEnabled(root.location)) return;
    const supplyTable = tableFor('supply');
    const borrowTable = tableFor('borrow');
    if(supplyTable){
      applyLayout('supply', supplyTable, currentLayout('supply'));
      createToolbar('supply', root.document.getElementById('earn-supply-section'));
    }
    if(borrowTable){
      applyLayout('borrow', borrowTable, currentLayout('borrow'));
      createToolbar('borrow', root.document.getElementById('earn-lending-section'));
    }
  }

  function scheduleReapply(){
    if(reapplyQueued || !root) return;
    reapplyQueued = true;
    (root.requestAnimationFrame || root.setTimeout)(() => {
      reapplyQueued = false;
      reapply();
    });
  }

  function initEarnLayoutEditor(){
    if(editorInitialized || !root?.document || !isLocalEditorEnabled(root.location)) return;
    editorInitialized = true;
    editorLayouts = readStoredLayouts();
    root.document.body.classList.add('earn-layout-editor-enabled');
    reapply();
    const observer = new MutationObserver(scheduleReapply);
    observer.observe(root.document.body, {childList:true, subtree:true});
  }

  const api = {
    VERSION, SPACER, SPACER_WIDTH, SCHEMAS, STORAGE_KEY, EXPORT_NAME,
    createDefaultLayout, normalizeLayout, reorderLayout, resizeLayout, getLayoutTableWidth, addSpacer, removeSpacer,
    isLocalEditorEnabled, createDefaultSavedLayouts, normalizeSavedLayouts,
    applyLayout, reapply, initEarnLayoutEditor,
  };
  if(root && isLocalEditorEnabled(root.location)) root.setTimeout(initEarnLayoutEditor, 0);
  return api;
});
