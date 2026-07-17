(function(root, factory){
  const api = factory(root);
  if(typeof module === "object" && module.exports) module.exports = api;
  if(root) root.DoloHolderLayoutEditor = api;
})(typeof window !== "undefined" ? window : null, function(root){
  "use strict";

  const VERSION = 1;
  const SPACER = "spacer";
  const SPACER_WIDTH = 4;
  const SCHEMAS = {
    distribution: {
      keys:["group", "balance", "wallets", "change", "details"],
      widths:{group:50.630228, balance:14.756621, wallets:10.347846, change:13.807863, details:10.457442},
      minimums:{group:24, balance:24, wallets:24, change:24, details:24, spacer:12},
    },
    details: {
      keys:["chain", "address", "dolo", "change"],
      widths:{chain:13.1423, address:57.0044, dolo:17.2978, change:12.5555},
      minimums:{chain:24, address:24, dolo:24, change:24, spacer:12},
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
    if(Math.abs(sumWidths(order, widths) - 100) > 0.0001) return null;
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

  function resizeLayout(name, layout, key, deltaPx, containerWidthPx){
    const schema = schemaFor(name);
    const current = normalizeLayout(name, layout);
    const width = Number(containerWidthPx);
    if(!schema || !current || !current.order.includes(key) || !(width > 0)) return current || clone(layout);
    const index = current.order.indexOf(key);
    const delta = Number(deltaPx) / width * 100;
    if(!Number.isFinite(delta)) return current;
    const minimum = column => (schema.minimums[column] || 12) / width * 100;
    const widths = {...current.widths};
    if(delta > 0){
      const donors = current.order.slice(index + 1).concat(current.order.slice(0, index));
      let remaining = Math.min(delta, donors.reduce((sum, donor) => sum + Math.max(0, widths[donor] - minimum(donor)), 0));
      const applied = remaining;
      donors.forEach(donor => {
        const available = Math.max(0, widths[donor] - minimum(donor));
        const taken = Math.min(available, remaining);
        widths[donor] = round(widths[donor] - taken);
        remaining = round(remaining - taken);
      });
      widths[key] = round(widths[key] + applied);
    } else if(delta < 0){
      const recipient = current.order[index + 1] || current.order[index - 1];
      if(!recipient) return current;
      const released = Math.min(-delta, Math.max(0, widths[key] - minimum(key)));
      widths[key] = round(widths[key] - released);
      widths[recipient] = round(widths[recipient] + released);
    }
    const roundingDelta = round(100 - sumWidths(current.order, widths));
    widths[key] = round(widths[key] + roundingDelta);
    return {...current, widths};
  }

  function addSpacer(name, layout){
    const schema = schemaFor(name);
    const current = normalizeLayout(name, layout);
    if(!schema || !current || current.order.includes(SPACER)) return current || clone(layout);
    const donor = current.order.reduce((best, key) => current.widths[key] > current.widths[best] ? key : best, current.order[0]);
    const donorMinimum = (schema.minimums[donor] || 20);
    if(current.widths[donor] * 10 <= donorMinimum + SPACER_WIDTH * 10) return current;
    const order = current.order.concat(SPACER);
    const widths = {...current.widths, [donor]:round(current.widths[donor] - SPACER_WIDTH), [SPACER]:SPACER_WIDTH};
    return {version:VERSION, order, widths};
  }

  function removeSpacer(name, layout){
    const current = normalizeLayout(name, layout);
    if(!current || !current.order.includes(SPACER)) return current || clone(layout);
    const spacerIndex = current.order.indexOf(SPACER);
    const recipient = current.order[spacerIndex + 1] || current.order[spacerIndex - 1];
    const order = current.order.filter(key => key !== SPACER);
    const widths = {...current.widths, [recipient]:round(current.widths[recipient] + current.widths[SPACER])};
    delete widths[SPACER];
    return {version:VERSION, order, widths};
  }

  const STORAGE_KEY = "dolomite:dolo-holder-layout-editor:v1";
  const EDITOR_QUERY = "layoutEditor";
  const EXPORT_NAME = "dolo-holder-layout-draft.json";
  let editorLayouts = null;
  let editorInitialized = false;
  let reapplyQueued = false;

  function createDefaultSavedLayouts(){
    return {
      version:VERSION,
      holderDistribution:createDefaultLayout("distribution"),
      holderDetails:createDefaultLayout("details"),
    };
  }

  function normalizeSavedLayouts(value){
    if(!value || value.version !== VERSION) return null;
    const holderDistribution = normalizeLayout("distribution", value.holderDistribution);
    const holderDetails = normalizeLayout("details", value.holderDetails);
    if(!holderDistribution || !holderDetails) return null;
    return {version:VERSION, holderDistribution, holderDetails};
  }

  function isEditorEnabled(){
    return !!root && new URLSearchParams(root.location.search).get(EDITOR_QUERY) === "1";
  }

  function readStoredLayouts(){
    if(!root) return createDefaultSavedLayouts();
    try {
      return normalizeSavedLayouts(JSON.parse(root.localStorage.getItem(STORAGE_KEY))) || createDefaultSavedLayouts();
    } catch(_) {
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

  function layoutKey(name){
    return name === "distribution" ? "holderDistribution" : "holderDetails";
  }

  function currentLayout(name){
    if(!editorLayouts) editorLayouts = readStoredLayouts();
    return editorLayouts[layoutKey(name)];
  }

  function updateLayout(name, next){
    const valid = normalizeLayout(name, next);
    if(!valid) return;
    editorLayouts = {...(editorLayouts || readStoredLayouts()), [layoutKey(name)]:valid};
    persistLayouts();
    reapply();
  }

  function directColumn(parent, key){
    return [...parent.children].find(child => child.dataset?.column === key) || null;
  }

  function reorderColumns(parent, order){
    order.forEach(key => {
      const cell = directColumn(parent, key);
      if(cell) parent.append(cell);
    });
  }

  function ensureDistributionSpacer(legend, enabled){
    const parents = [legend.querySelector(".holder-legend-head"), ...legend.querySelectorAll(".holder-legend-row")].filter(Boolean);
    parents.forEach(parent => {
      const existing = directColumn(parent, SPACER);
      if(!enabled){ existing?.remove(); return; }
      if(existing) return;
      const spacer = root.document.createElement(parent.classList.contains("holder-legend-head") ? "span" : "div");
      spacer.dataset.column = SPACER;
      spacer.className = "dolo-layout-spacer";
      spacer.setAttribute("aria-hidden", "true");
      parent.append(spacer);
    });
  }

  function applyDistributionLayout(legend, layout){
    const valid = normalizeLayout("distribution", layout);
    if(!legend || !valid) return;
    ensureDistributionSpacer(legend, valid.order.includes(SPACER));
    legend.classList.add("dolo-layout-editor-active");
    legend.style.setProperty("--holder-layout-columns", valid.order.map(key => `${valid.widths[key]}%`).join(" "));
    const parents = [legend.querySelector(".holder-legend-head"), ...legend.querySelectorAll(".holder-legend-row")].filter(Boolean);
    parents.forEach(parent => {
      valid.order.forEach((key, index) => {
        const cell = directColumn(parent, key);
        if(cell) cell.style.setProperty("--holder-layout-order", index);
      });
      reorderColumns(parent, valid.order);
    });
    decorateHeaders("distribution", legend, valid, legend.querySelectorAll(".holder-legend-head > [data-column]"));
  }

  function ensureDetailsSpacer(table, enabled){
    const colgroup = table.querySelector("colgroup");
    const header = table.tHead?.rows?.[0];
    const rows = [...table.querySelectorAll("tbody tr[data-wallet-search]")];
    if(colgroup){
      const existing = directColumn(colgroup, SPACER);
      if(!enabled) existing?.remove();
      else if(!existing){ const col = root.document.createElement("col"); col.dataset.column = SPACER; col.className = "dolo-layout-spacer"; colgroup.append(col); }
    }
    if(header){
      const existing = directColumn(header, SPACER);
      if(!enabled) existing?.remove();
      else if(!existing){ const th = root.document.createElement("th"); th.dataset.column = SPACER; th.className = "dolo-layout-spacer"; th.setAttribute("aria-label", "Blank spacer"); header.append(th); }
    }
    rows.forEach(row => {
      const existing = directColumn(row, SPACER);
      if(!enabled) existing?.remove();
      else if(!existing){ const td = root.document.createElement("td"); td.dataset.column = SPACER; td.className = "dolo-layout-spacer"; td.setAttribute("aria-hidden", "true"); row.append(td); }
    });
    const empty = table.querySelector("[data-holder-details-search-empty]");
    if(empty) empty.firstElementChild?.setAttribute("colspan", String(4 + (enabled ? 1 : 0)));
  }

  function applyDetailsLayout(table, layout){
    const valid = normalizeLayout("details", layout);
    if(!table || !valid) return;
    ensureDetailsSpacer(table, valid.order.includes(SPACER));
    table.classList.add("dolo-layout-editor-active");
    const colgroup = table.querySelector("colgroup");
    const header = table.tHead?.rows?.[0];
    if(colgroup){
      valid.order.forEach(key => {
        const col = directColumn(colgroup, key);
        if(col) col.style.width = `${valid.widths[key]}%`;
      });
      reorderColumns(colgroup, valid.order);
    }
    if(header) reorderColumns(header, valid.order);
    table.querySelectorAll("tbody tr[data-wallet-search]").forEach(row => reorderColumns(row, valid.order));
    decorateHeaders("details", table, valid, header ? header.querySelectorAll(":scope > [data-column]") : []);
  }

  function icon(name){
    const paths = {
      add:'<path d="M12 5v14M5 12h14"/>',
      remove:'<path d="M5 12h14"/>',
      reset:'<path d="M3 12a9 9 0 1 0 3-6.7"/><path d="M3 4v5h5"/>',
      save:'<path d="M5 3h12l4 4v14H5z"/><path d="M8 3v6h8V3M8 21v-7h8v7"/>',
      drag:'<path d="M9 5h.01M15 5h.01M9 12h.01M15 12h.01M9 19h.01M15 19h.01"/>',
    };
    return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths[name] || ""}</svg>`;
  }

  function createToolbar(name, host){
    let toolbar = host.querySelector(`:scope > [data-dolo-layout-toolbar="${name}"]`);
    if(!toolbar){
      toolbar = root.document.createElement("div");
      toolbar.className = "dolo-layout-editor-toolbar";
      toolbar.dataset.doloLayoutToolbar = name;
      toolbar.innerHTML = `
        <button type="button" data-layout-action="add" aria-label="Add blank spacer" title="Add blank spacer">${icon("add")}</button>
        <button type="button" data-layout-action="remove" aria-label="Remove blank spacer" title="Remove blank spacer">${icon("remove")}</button>
        <button type="button" data-layout-action="reset" aria-label="Reset layout" title="Reset layout">${icon("reset")}</button>
        <button type="button" data-layout-action="save" aria-label="Save both layouts" title="Save both layouts">${icon("save")}</button>`;
      host.append(toolbar);
      toolbar.addEventListener("click", event => {
        const button = event.target.closest("[data-layout-action]");
        if(!button) return;
        const action = button.dataset.layoutAction;
        const layout = currentLayout(name);
        if(action === "add") updateLayout(name, addSpacer(name, layout));
        if(action === "remove") updateLayout(name, removeSpacer(name, layout));
        if(action === "reset") updateLayout(name, createDefaultLayout(name));
        if(action === "save") downloadLayouts();
      });
    }
    toolbar.querySelector('[data-layout-action="remove"]').disabled = !currentLayout(name).order.includes(SPACER);
  }

  function downloadLayouts(){
    const valid = persistLayouts();
    if(!valid || !root) return;
    const blob = new Blob([JSON.stringify(valid, null, 2)], {type:"application/json"});
    const href = root.URL.createObjectURL(blob);
    const link = root.document.createElement("a");
    link.href = href;
    link.download = EXPORT_NAME;
    link.click();
    root.URL.revokeObjectURL(href);
  }

  function decorateHeaders(name, container, layout, headers){
    const directHeaderDrag = name === "distribution";
    [...headers].forEach(header => {
      const key = header.dataset.column;
      if(!key) return;
      header.classList.add("dolo-layout-editor-header");
      let drag = header.querySelector(":scope > .dolo-layout-drag-handle");
      if(directHeaderDrag){
        drag?.remove();
        if(!header.dataset.doloLayoutDirectDrag){
          header.dataset.doloLayoutDirectDrag = "1";
          header.addEventListener("pointerdown", event => {
            if(event.target.closest(".dolo-layout-resize-handle")) return;
            startColumnDrag(event, name, container, key);
          });
        }
      } else if(!drag){
        drag = root.document.createElement("button");
        drag.type = "button";
        drag.className = "dolo-layout-drag-handle";
        drag.setAttribute("aria-label", `Move ${key} column`);
        drag.title = "Drag to move column";
        drag.innerHTML = icon("drag");
        header.append(drag);
        drag.addEventListener("pointerdown", event => startColumnDrag(event, name, container, key));
      }
      let resize = header.querySelector(":scope > .dolo-layout-resize-handle");
      if(!resize){
        resize = root.document.createElement("span");
        resize.className = "dolo-layout-resize-handle";
        resize.setAttribute("aria-hidden", "true");
        header.append(resize);
        resize.addEventListener("pointerdown", event => startResize(event, name, container, key));
      }
    });
  }

  function startColumnDrag(event, name, container, movedKey){
    event.preventDefault();
    const source = event.currentTarget;
    let targetHeader = null;
    const clearTarget = () => {
      targetHeader?.classList.remove("dolo-layout-drop-target");
      targetHeader = null;
    };
    const move = moveEvent => {
      const next = root.document.elementFromPoint(moveEvent.clientX, moveEvent.clientY)
        ?.closest(".dolo-layout-editor-header");
      if(!next || !container.contains(next) || next.dataset.column === movedKey){
        clearTarget();
        return;
      }
      if(next !== targetHeader){
        clearTarget();
        targetHeader = next;
        targetHeader.classList.add("dolo-layout-drop-target");
      }
    };
    const stop = stopEvent => {
      root.removeEventListener("pointermove", move);
      root.removeEventListener("pointerup", stop);
      root.removeEventListener("pointercancel", stop);
      root.removeEventListener("blur", stop);
      root.document.removeEventListener("keydown", escape);
      source.releasePointerCapture?.(event.pointerId);
      if(targetHeader && stopEvent.type === "pointerup"){
        const rect = targetHeader.getBoundingClientRect();
        const placeAfter = stopEvent.clientX > rect.left + rect.width / 2;
        updateLayout(name, reorderLayout(name, currentLayout(name), movedKey, targetHeader.dataset.column, placeAfter));
      }
      clearTarget();
    };
    const escape = keyEvent => { if(keyEvent.key === "Escape") stop({type:"cancel"}); };
    source.setPointerCapture?.(event.pointerId);
    root.addEventListener("pointermove", move);
    root.addEventListener("pointerup", stop, {once:true});
    root.addEventListener("pointercancel", stop, {once:true});
    root.addEventListener("blur", stop, {once:true});
    root.document.addEventListener("keydown", escape, {once:true});
  }

  function startResize(event, name, container, key){
    event.preventDefault();
    const source = event.currentTarget;
    const startX = event.clientX;
    const initial = currentLayout(name);
    const target = name === "distribution" ? container : container;
    const width = target.getBoundingClientRect().width;
    if(!(width > 0)) return;
    const move = moveEvent => updateLayout(name, resizeLayout(name, initial, key, moveEvent.clientX - startX, width));
    const stop = () => {
      root.removeEventListener("pointermove", move);
      root.removeEventListener("pointerup", stop);
      root.removeEventListener("pointercancel", stop);
      root.removeEventListener("blur", stop);
      root.document.removeEventListener("keydown", escape);
      source.releasePointerCapture?.(event.pointerId);
    };
    const escape = keyEvent => { if(keyEvent.key === "Escape") stop(); };
    source.setPointerCapture?.(event.pointerId);
    root.addEventListener("pointermove", move);
    root.addEventListener("pointerup", stop, {once:true});
    root.addEventListener("pointercancel", stop, {once:true});
    root.addEventListener("blur", stop, {once:true});
    root.document.addEventListener("keydown", escape, {once:true});
  }

  function reapply(){
    if(!isEditorEnabled() || !root?.document) return;
    const distribution = root.document.getElementById("holderChartLegend");
    if(distribution){
      applyDistributionLayout(distribution, currentLayout("distribution"));
      const host = distribution.parentElement;
      if(host){
        let toolbarHost = host.querySelector(":scope > .dolo-layout-editor-distribution-host");
        if(!toolbarHost){ toolbarHost = root.document.createElement("div"); toolbarHost.className = "dolo-layout-editor-distribution-host"; host.insertBefore(toolbarHost, distribution); }
        createToolbar("distribution", toolbarHost);
      }
    }
    root.document.querySelectorAll("[data-holder-details-table]").forEach(table => {
      applyDetailsLayout(table, currentLayout("details"));
      const tools = table.closest(".holder-wallet-detail-shell")?.querySelector(".holder-wallet-panel-tools");
      if(tools) createToolbar("details", tools);
    });
  }

  function scheduleReapply(){
    if(reapplyQueued || !root) return;
    reapplyQueued = true;
    (root.requestAnimationFrame || root.setTimeout)(() => {
      reapplyQueued = false;
      reapply();
    });
  }

  function initDoloHolderLayoutEditor(){
    if(editorInitialized || !isEditorEnabled() || !root?.document) return;
    editorInitialized = true;
    editorLayouts = readStoredLayouts();
    root.document.body.classList.add("dolo-layout-editor-enabled");
    reapply();
    const observer = new MutationObserver(scheduleReapply);
    observer.observe(root.document.body, {childList:true, subtree:true});
  }

  const api = {
    VERSION, SPACER, SPACER_WIDTH, SCHEMAS, STORAGE_KEY, EXPORT_NAME,
    createDefaultLayout, normalizeLayout, reorderLayout, resizeLayout, addSpacer, removeSpacer,
    createDefaultSavedLayouts, normalizeSavedLayouts, isEditorEnabled,
    applyDistributionLayout, applyDetailsLayout, reapply, initDoloHolderLayoutEditor,
  };
  if(root && isEditorEnabled()) root.setTimeout(initDoloHolderLayoutEditor, 0);
  return api;
});
