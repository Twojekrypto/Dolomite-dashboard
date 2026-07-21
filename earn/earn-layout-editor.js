(function(root, factory){
  const api = factory();
  if(typeof module === "object" && module.exports) module.exports = api;
  if(root) root.EarnLayoutEditor = api;
})(typeof window !== "undefined" ? window : null, function(){
  "use strict";

  const VERSION = 1;
  const SPACER = "spacer";
  const SPACER_WIDTH = 4;
  const STORAGE_KEY = "dolomite:earn-layout-editor:v1";
  const EXPORT_NAME = "earn-layout-draft.json";
  const SCHEMAS = {
    supply: {
      keys:["token", "price", "supply", "balance", "yield", "details"],
      widths:{token:32, price:10, supply:20, balance:16, yield:14, details:8},
      minimums:{token:170, price:78, supply:130, balance:132, yield:130, details:80, spacer:16},
    },
    borrow: {
      keys:["health", "collateral", "debt", "pnl", "details"],
      widths:{health:20, collateral:25, debt:25, pnl:18, details:12},
      minimums:{health:112, collateral:150, debt:150, pnl:128, details:80, spacer:16},
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

  function resizeLayout(name, layout, key, deltaPx, tableWidthPx){
    const schema = schemaFor(name);
    const current = normalizeLayout(name, layout);
    const tableWidth = Number(tableWidthPx);
    if(!schema || !current || !current.order.includes(key) || !(tableWidth > 0)) return current || clone(layout);
    const delta = Number(deltaPx) / tableWidth * 100;
    if(!Number.isFinite(delta)) return current;
    const minimum = column => Math.ceil(((schema.minimums[column] || 16) / tableWidth * 100) * 1e6) / 1e6;
    const widths = {...current.widths};
    const index = current.order.indexOf(key);

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

    widths[key] = round(widths[key] + round(100 - sumWidths(current.order, widths)));
    return {...current, widths};
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

  function normalizeSavedLayouts(value){
    if(!value || value.version !== VERSION) return null;
    const supply = normalizeLayout("supply", value.supply);
    const borrow = normalizeLayout("borrow", value.borrow);
    if(!supply || !borrow) return null;
    return {version:VERSION, supply, borrow};
  }

  return {
    VERSION, SPACER, SPACER_WIDTH, SCHEMAS, STORAGE_KEY, EXPORT_NAME,
    createDefaultLayout, normalizeLayout, reorderLayout, resizeLayout, addSpacer, removeSpacer,
    isLocalEditorEnabled, createDefaultSavedLayouts, normalizeSavedLayouts,
  };
});
