(function(root, factory){
  const api = factory(root || {});
  if(typeof module === "object" && module.exports) module.exports = api;
  if(root) root.DoloWalletTableUX = api;
})(typeof window !== "undefined" ? window : globalThis, function(root){
  "use strict";

  const ADDRESS_RE = /^0x[a-fA-F0-9]{40}$/;
  const TX_RE = /^0x[a-fA-F0-9]{64}$/;
  const EXPLORERS = {
    ethereum:"https://etherscan.io/tx/",
    berachain:"https://berascan.com/tx/",
  };
  const INTERNAL_DEPOSIT_ACTIONS = new Set(["deposit", "route", "transfer"]);
  const ACTION_LABEL_TO_KEY = Object.freeze({
    deposit:"deposit",
    withdraw:"withdraw",
    withdrawal:"withdraw",
    borrow:"borrow",
    repay:"repay",
    transfer:"transfer",
    route:"route",
    trade:"trade",
    zap:"zap",
    claim:"claim",
    liquidation:"liquidation",
    "new lock":"create",
  });
  const EMPTY_STATE_MESSAGES = Object.freeze({
    history:"No transactions found",
    holders:"No veDOLO holders found",
    activity:"No activity found",
    positions:"No positions found",
    claimable:"No expired veDOLO positions found",
    assets:"No deposited assets found",
    default:"No results found",
  });
  const UX_VERSION = "20260820-table-ux-v1";
  const portalState = new WeakMap();
  let routeMutationInProgress = false;
  let enhancementFrame = 0;
  let installed = false;

  function escapeHtml(value){
    return String(value ?? "").replace(/[&<>"']/g, char => ({
      "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;",
    })[char]);
  }

  function shortAddress(address){
    const value = String(address || "");
    return ADDRESS_RE.test(value) ? `${value.slice(0, 6)}…${value.slice(-4)}` : value;
  }

  function formatTxDate(timestamp){
    const seconds = Number(timestamp);
    if(!Number.isInteger(seconds) || seconds <= 0) return "";
    return new Intl.DateTimeFormat("en-GB", {
      day:"2-digit", month:"short", year:"numeric", timeZone:"UTC",
    }).format(new Date(seconds * 1000)).replace(/^0/, "");
  }

  function formatTxDateValue(timestamp, dateValue){
    const exactTimestampDate = formatTxDate(timestamp);
    if(exactTimestampDate) return exactTimestampDate;
    if(!dateValue) return "";
    const parsed = new Date(dateValue);
    if(Number.isNaN(parsed.getTime())) return "";
    return new Intl.DateTimeFormat("en-GB", {
      day:"2-digit", month:"short", year:"numeric", timeZone:"UTC",
    }).format(parsed).replace(/^0/, "");
  }

  function txExplorerUrl(chain, txHash){
    const base = EXPLORERS[String(chain || "").toLowerCase()];
    return base && TX_RE.test(String(txHash || "")) ? `${base}${txHash}` : "";
  }

  function applyAddressOverrides(){
    const overrides = root.DOLO_ADDRESS_BEHAVIORAL_OVERRIDES || {};
    ["DOLO_ADDRESS_LABELS", "DOLO_ADDR_LABELS"].forEach(key => {
      const current = root[key];
      if(current && typeof current === "object") Object.assign(current, overrides);
      else root[key] = Object.assign({}, overrides);
    });
    return overrides;
  }

  function resolveIdentity(address, fallback, labels){
    const normalized = String(address || "").toLowerCase();
    const overrides = applyAddressOverrides();
    const override = overrides[normalized];
    if(override){
      return {
        address:normalized,
        known:true,
        label:override.label || "Wallet",
        type:override.type || fallback?.type || "",
        metadata:override,
      };
    }
    if(typeof root.resolveDoloWalletIdentity === "function"){
      return root.resolveDoloWalletIdentity(address, fallback, labels);
    }
    return {address:normalized, known:false, label:"", type:fallback?.type || "", metadata:{}};
  }

  function walletCellHtml(options = {}){
    const address = String(options.address || "").toLowerCase();
    const safeAddress = escapeHtml(address);
    const identity = resolveIdentity(address, options.fallback || {}, options.labels);
    const date = formatTxDateValue(options.txTimestamp, options.txDate);
    const txUrl = date ? txExplorerUrl(options.txChain, options.txHash) : "";
    const displayLabel = identity.known ? identity.label : "Wallet";
    const copyIcon = options.copyIcon || '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="9" y="9" width="11" height="11" rx="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
    const externalIcon = options.externalIcon || '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M14 3h7v7"></path><path d="M10 14 21 3"></path><path d="M21 14v5a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5"></path></svg>';
    const actions = `<span class="wallet-address-actions"><span class="wallet-address addr-mono addr-tooltip-wrap" data-full-addr="${safeAddress}">${escapeHtml(shortAddress(address))}</span><button class="addr-copy" type="button" data-copy="${safeAddress}" aria-label="Copy address">${copyIcon}</button><a class="addr-debank" href="https://debank.com/profile/${safeAddress}" target="_blank" rel="noopener" aria-label="View on DeBank" onclick="event.stopPropagation()"><img src="https://debank.com/favicon.ico" alt="DeBank"></a></span>`;
    const txMeta = txUrl
      ? `<a class="wallet-tx-meta" href="${escapeHtml(txUrl)}" target="_blank" rel="noopener" aria-label="Open transaction from ${escapeHtml(date)}" onclick="event.stopPropagation()"><span>${escapeHtml(date)}</span>${externalIcon}</a>`
      : "";
    const primaryTxMeta = options.txInPrimaryLine ? txMeta : "";
    const primary = `<div class="wallet-primary-line"><div class="wallet-primary">${escapeHtml(displayLabel)}</div>${primaryTxMeta}${options.badgesHtml || ""}</div>`;
    const addressLine = `<div class="wallet-secondary">${actions}</div>`;
    const secondary = options.txInPrimaryLine
      ? addressLine
      : options.txOnOwnLine && txMeta
      ? `${addressLine}<div class="wallet-secondary wallet-secondary-tx">${txMeta}</div>`
      : `<div class="wallet-secondary">${actions}${txMeta ? `<span class="wallet-meta-separator" aria-hidden="true">·</span>${txMeta}` : ""}</div>`;
    return `<div class="wallet-identity-cell" data-wallet-address="${safeAddress}">${primary}${secondary}</div>`;
  }

  function normalizeTxHash(value){
    const match = String(value || "").toLowerCase().match(/0x[a-f0-9]{64}/);
    return match ? match[0] : "";
  }

  function normalizedActionKey(value){
    const text = String(value || "").trim().toLowerCase().replace(/\s+/g, " ");
    if(ACTION_LABEL_TO_KEY[text]) return ACTION_LABEL_TO_KEY[text];
    if(text.includes("deposit")) return "deposit";
    if(text.includes("withdraw")) return "withdraw";
    if(text.includes("transfer")) return "transfer";
    if(text.includes("route")) return "route";
    if(text.includes("trade") || text.includes("swap")) return "trade";
    if(text.includes("borrow")) return "borrow";
    if(text.includes("repay")) return "repay";
    if(text.includes("claim")) return "claim";
    return text.replace(/[^a-z0-9]+/g, "-") || "activity";
  }

  function historyActionsForTransaction(txHash, actions){
    const normalizedHash = normalizeTxHash(txHash);
    const normalizedActions = (Array.isArray(actions) ? actions : []).map(normalizedActionKey).filter(Boolean);
    if(normalizedActions.includes("deposit")
      && normalizedActions.every(action => INTERNAL_DEPOSIT_ACTIONS.has(action))){
      return ["deposit"];
    }
    return normalizedActions;
  }

  function routeSelectionPlan(selectedKeys, clickedKey, allKeys){
    const all = Array.from(new Set((allKeys || []).map(String)));
    const selected = new Set((selectedKeys || []).map(String));
    const clicked = String(clickedKey || "");
    if(clicked === "all") return all;
    if(!all.includes(clicked)) return Array.from(selected);
    if(all.length && all.every(key => selected.has(key))) return [clicked];
    if(selected.has(clicked)) selected.delete(clicked); else selected.add(clicked);
    return all.filter(key => selected.has(key));
  }

  function emptyStateMessage(tableIdentity, currentText){
    const identity = String(tableIdentity || "").toLowerCase();
    const current = String(currentText || "").trim();
    if(/enter a wallet|wallet loaded|load a wallet/i.test(current)){
      return /history|transaction/.test(identity) ? "Enter a wallet to load transactions." : current;
    }
    if(/history|transaction/.test(identity)) return EMPTY_STATE_MESSAGES.history;
    if(/holder/.test(identity)) return EMPTY_STATE_MESSAGES.holders;
    if(/position-activity|activity|flow/.test(identity)) return EMPTY_STATE_MESSAGES.activity;
    if(/claimable|expired|expiry/.test(identity)) return EMPTY_STATE_MESSAGES.claimable;
    if(/deposit|asset/.test(identity)) return EMPTY_STATE_MESSAGES.assets;
    if(/position|borrow|supply/.test(identity)) return EMPTY_STATE_MESSAGES.positions;
    return EMPTY_STATE_MESSAGES.default;
  }

  function actionIconSvg(key){
    const icons = {
      deposit:'<path d="M12 3v12"/><path d="m7 10 5 5 5-5"/><path d="M5 21h14"/>',
      withdraw:'<path d="M12 21V9"/><path d="m7 14 5-5 5 5"/><path d="M5 3h14"/>',
      transfer:'<path d="M7 7h11"/><path d="m15 4 3 3-3 3"/><path d="M17 17H6"/><path d="m9 14-3 3 3 3"/>',
      borrow:'<path d="M5 12h14"/><path d="m14 7 5 5-5 5"/><circle cx="7" cy="12" r="3"/>',
      repay:'<path d="M19 12H5"/><path d="m10 7-5 5 5 5"/><circle cx="17" cy="12" r="3"/>',
      route:'<path d="M5 5h4a3 3 0 0 1 3 3v8a3 3 0 0 0 3 3h4"/><path d="m16 16 3 3-3 3"/>',
      trade:'<path d="M4 7h14"/><path d="m15 4 3 3-3 3"/><path d="M20 17H6"/><path d="m9 14-3 3 3 3"/>',
      claim:'<path d="M12 3v13"/><path d="m8 12 4 4 4-4"/><path d="M5 21h14"/>',
      create:'<rect x="5" y="10" width="14" height="11" rx="2"/><path d="M8 10V7a4 4 0 0 1 8 0v3"/><path d="M12 13v5"/><path d="M9.5 15.5h5"/>',
      activity:'<circle cx="12" cy="12" r="8"/><path d="M8 12h8"/>',
    };
    return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${icons[key] || icons.activity}</svg>`;
  }

  function chipActionKey(chip){
    const classKeys = ["deposit","withdraw","transfer","borrow","repay","route","trade","zap","claim","create"];
    for(const key of classKeys){
      if(chip.classList && chip.classList.contains(key)) return key === "zap" ? "route" : key;
    }
    return normalizedActionKey(chip.textContent);
  }

  function decorateActionChip(chip){
    if(!chip || chip.dataset.doloActionDecorated === UX_VERSION) return;
    const key = chipActionKey(chip);
    chip.dataset.doloActionDecorated = UX_VERSION;
    chip.dataset.actionKey = key;
    chip.classList.add("dolo-route-badge");
    if(!chip.querySelector(".dolo-route-icon")){
      const icon = root.document.createElement("span");
      icon.className = "dolo-route-icon";
      icon.innerHTML = actionIconSvg(key);
      chip.insertBefore(icon, chip.firstChild);
    }
  }

  function historyTxHash(row){
    const link = row && row.querySelector ? row.querySelector('.date-tx[href*="/tx/"]') : null;
    return normalizeTxHash(link && link.getAttribute("href"));
  }

  function normalizeHistoryRows(scope){
    const doc = root.document;
    if(!doc) return;
    (scope || doc).querySelectorAll(".history-table tbody tr").forEach(row => {
      const actionList = row.querySelector(".action-td .action-list");
      if(!actionList) return;
      const chips = Array.from(actionList.querySelectorAll(".action-chip"));
      const txHash = historyTxHash(row);
      const actions = chips.map(chipActionKey);
      const visibleActions = historyActionsForTransaction(txHash, actions);
      if(visibleActions.length === 1
        && visibleActions[0] === "deposit"
        && actions.length > 1){
        if(row.dataset.doloCollapsedAction !== "deposit"){
          actionList.innerHTML = '<span class="action-chip deposit dolo-route-badge" data-action-key="deposit" data-dolo-action-decorated="' + UX_VERSION + '"><span class="dolo-route-icon">' + actionIconSvg("deposit") + '</span><span>Deposit</span></span>';
          row.dataset.doloCollapsedAction = "deposit";
          row.dataset.doloInternalSteps = actions.join(",");
        }
      }else{
        chips.forEach(decorateActionChip);
      }
    });
  }

  function tableIdentity(table){
    if(!table) return "";
    const context = table.closest && table.closest("section,article,.card,.history-card,.table-card");
    return [table.id, table.className, context && context.id, context && context.className]
      .filter(Boolean).join(" ");
  }

  function normalizeEmptyStates(scope){
    const doc = root.document;
    if(!doc) return;
    const candidateSet = new Set((scope || doc).querySelectorAll("tr.table-empty-row, tr.empty-row, tr.no-results-row, tr[data-empty-state]"));
    (scope || doc).querySelectorAll("table tbody tr").forEach(row => {
      const cells = row.querySelectorAll(":scope > td, :scope > th");
      if(cells.length !== 1) return;
      const text = cells[0].textContent.replace(/\s+/g, " ").trim();
      const explicitCell = cells[0].matches(".pf-empty,.table-empty,.empty-cell,[data-empty-state]");
      if(explicitCell || (cells[0].colSpan > 1 && /^(no |enter |nothing |there (?:are|is) no |0 )/i.test(text))) candidateSet.add(row);
    });
    candidateSet.forEach(row => {
      if(row.classList.contains("table-spacer-row")) return;
      const cell = row.querySelector("td,th");
      if(!cell) return;
      if(cell.dataset.doloEmptyFixed === "true") return;
      const current = cell.textContent.replace(/\s+/g, " ").trim();
      if(!current || /loading|checking|fetching/i.test(current) || /error|failed|unable/i.test(current)) return;
      const table = row.closest("table");
      const message = emptyStateMessage(tableIdentity(table), current);
      row.classList.add("dolo-empty-state-row");
      cell.classList.add("dolo-empty-state-cell");
      if(cell.querySelector("a,button,.history-empty-note")){
        cell.querySelectorAll(".history-empty-note").forEach(note => note.classList.add("dolo-empty-state"));
        return;
      }
      if(cell.dataset.doloEmptyMessage !== message){
        cell.dataset.doloEmptyMessage = message;
        cell.innerHTML = `<div class="dolo-empty-state" role="status"><span>${escapeHtml(message)}</span></div>`;
      }
    });
  }

  function normalizeSearchNoResults(scope){
    const doc = root.document;
    if(!doc) return;
    const tableForBody = function(bodyId){
      const body = doc.getElementById(bodyId);
      return body ? body.closest("table") : null;
    };
    const configs = [
      {table:doc.getElementById("tbl-holders"), input:doc.getElementById("q-holders")},
      {table:doc.getElementById("dolo-lp-table"), input:doc.getElementById("dolo-lp-search")},
      {table:doc.getElementById("tbl-cb"), input:doc.getElementById("q-cb")},
      {table:doc.getElementById("tbl-ex"), input:doc.getElementById("q-ex")},
      {table:tableForBody("flows-acc-body"), input:doc.getElementById("q-flows")},
      {table:tableForBody("flows-out-body"), input:doc.getElementById("q-flows")}
    ];
    configs.forEach(function(config){
      if(!config.table || !config.input || !String(config.input.value || "").trim()) return;
      const body = config.table.tBodies && config.table.tBodies[0];
      const rows = body ? Array.from(body.rows) : [];
      const onlySpacerRows = rows.length > 0 && rows.every(function(row){
        return row.classList.contains("tbl-spacer-row") || row.classList.contains("flow-spacer-row") || row.classList.contains("dolo-lp-empty");
      });
      if(!onlySpacerRows) return;
      const headerCells = config.table.tHead && config.table.tHead.rows[0] ? config.table.tHead.rows[0].cells.length : 1;
      body.innerHTML = `<tr class="dolo-empty-state-row dolo-search-empty-row"><td colspan="${headerCells}" class="dolo-empty-state-cell" data-dolo-empty-fixed="true"><div class="dolo-empty-state" role="status"><span>No results found</span></div></td></tr>`;
    });
  }

  function normalizeTableCorners(scope){
    const doc = root.document;
    if(!doc) return;
    const selectors = ["#holders-table", "#position-activity-table", "#claimable-table"];
    (scope || doc).querySelectorAll(selectors.join(",")).forEach(table => {
      table.classList.add("dolo-rounded-table");
      const wrapper = table.closest(".tbl-wrap,.position-activity-scroll,.table-scroll");
      if(wrapper) wrapper.classList.add("dolo-rounded-table-wrap");
    });
  }

  function looksLikeNewLockRow(row){
    const actionCell = row.querySelector(".activity-action-cell");
    const badge = actionCell && actionCell.querySelector(".flow-source-tag,.tag");
    const cells = row.querySelectorAll("td");
    if(!badge || cells.length < 6) return false;
    if(/new lock/i.test(badge.textContent)) return true;
    const tooltip = `${badge.getAttribute("data-tooltip") || ""} ${badge.getAttribute("aria-label") || ""}`;
    const amount = cells[4].textContent.replace(/\s+/g, " ").trim();
    const lockEnd = cells[5].textContent.replace(/\s+/g, " ").trim();
    return /(unknown route|direct lock)/i.test(tooltip)
      && /dolo/i.test(amount)
      && !/^[-—]$/.test(amount)
      && lockEnd
      && !/^[-—]$/.test(lockEnd);
  }

  function normalizeNewLockRows(scope){
    const doc = root.document;
    if(!doc) return;
    (scope || doc).querySelectorAll("#position-activity-body tr.stable-data-row").forEach(row => {
      if(row.dataset.vedoloNewLock !== "true" && !looksLikeNewLockRow(row)) return;
      row.dataset.vedoloNewLock = "true";
      const badge = row.querySelector(".activity-action-cell .flow-source-tag,.activity-action-cell .tag");
      if(badge){
        if(badge.dataset.doloNewLockNormalized !== UX_VERSION){
          badge.textContent = "New Lock";
          badge.dataset.doloNewLockNormalized = UX_VERSION;
        }
        badge.classList.add("dolo-route-badge","dolo-new-lock-badge");
        badge.setAttribute("data-tooltip", "New veDOLO Lock: A new veDOLO position was created.");
        badge.setAttribute("aria-label", "New veDOLO Lock: A new veDOLO position was created.");
      }
      const cells = row.querySelectorAll("td");
      const lockCell = cells[5];
      if(lockCell && !lockCell.querySelector(".dolo-new-lock-label")){
        const date = lockCell.textContent.replace(/\s+/g, " ").trim();
        lockCell.classList.add("dolo-lock-end-cell");
        lockCell.innerHTML = `<span class="dolo-lock-end-date">${escapeHtml(date)}</span><span class="dolo-new-lock-label">NEW LOCK</span>`;
      }
    });
  }

  function routeKindFromElement(element){
    const text = `${element && element.textContent || ""} ${element && element.getAttribute && element.getAttribute("data-tooltip") || ""}`.toLowerCase();
    if(/new lock/.test(text)) return "create";
    if(/exercise|odolo/.test(text)) return "odolo";
    if(/pair/.test(text)) return "pair";
    if(/airdrop/.test(text)) return "airdrop";
    if(/direct lock|direct vedolo|\bdirect\b/.test(text)) return "direct";
    if(/extend/.test(text)) return "extend";
    if(/merge/.test(text)) return "merge";
    if(/split/.test(text)) return "split";
    if(/transfer/.test(text)) return "transfer";
    return "all";
  }

  function normalizePositionActivityBadges(scope){
    const doc = root.document;
    if(!doc) return;
    (scope || doc).querySelectorAll('#position-activity-table .activity-action-cell .flow-source-tag, #pf-exercises-section .pf-route-tag').forEach(badge => {
      const key = routeKindFromElement(badge);
      badge.classList.add("dolo-route-badge");
      if(!badge.querySelector('.dolo-route-icon')){
        const icon = doc.createElement("span");
        icon.className = "dolo-route-icon";
        const activity = root.VeDoloPositionActivity;
        icon.innerHTML = activity && typeof activity.routeIconSvg === "function"
          ? activity.routeIconSvg(key)
          : actionIconSvg(key === "create" || key === "direct" ? "create" : "route");
        badge.insertBefore(icon, badge.firstChild);
      }
    });
  }

  function normalizeRouteDropdownIcons(scope){
    const doc = root.document;
    if(!doc) return;
    const activity = root.VeDoloPositionActivity;
    if(!activity || typeof activity.routeIconHtml !== "function") return;
    (scope || doc).querySelectorAll('.pf-exercise-route-filter .dd-opt[data-route]').forEach(option => {
      const key = String(option.dataset.route || "all");
      const active = option.classList.contains("active");
      const signature = `${UX_VERSION}:${key}:${active ? "1" : "0"}`;
      const current = option.querySelector('.exercise-route-icon,.dd-ico,img');
      if(option.dataset.doloRouteIcon === signature && current && current.classList.contains("dolo-shared-route-icon")) return;
      const wrapper = doc.createElement("span");
      wrapper.innerHTML = activity.routeIconHtml(key, active, "exercise-route-icon dolo-shared-route-icon");
      const next = wrapper.firstElementChild;
      if(!next) return;
      if(current) current.replaceWith(next);
      else {
        const check = option.querySelector('.dd-opt-check');
        if(check) check.insertAdjacentElement("afterend", next);
        else option.insertBefore(next, option.firstChild);
      }
      option.dataset.doloRouteIcon = signature;
    });
  }

  function allRoutesOptionHtml(){
    const check = '<span class="dd-opt-check" aria-hidden="true"><svg viewBox="0 0 12 10" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m1 5 3 3 7-7"/></svg></span>';
    let icon = `<span class="dd-ico dolo-all-routes-icon">${actionIconSvg("route")}</span>`;
    const activity = root.VeDoloPositionActivity;
    if(activity && typeof activity.routeIconHtml === "function"){
      icon = activity.routeIconHtml("all", false, "exercise-route-icon");
    }
    return `<div class="dd-opt select-all dolo-all-routes-option" data-route="all" role="option">${check}${icon}<span class="dd-opt-name">All Routes</span></div>`;
  }

  function enhanceAllRoutes(scope){
    const doc = root.document;
    if(!doc) return;
    (scope || doc).querySelectorAll(".pf-exercise-route-filter").forEach(dd => {
      if(dd.dataset.routeModel === "native") return;
      const list = dd.querySelector(".dd-list");
      if(!list) return;
      let concrete = Array.from(list.querySelectorAll('.dd-opt[data-route]:not([data-route="all"])'));
      if(!concrete.length) return;
      let allOption = list.querySelector('.dd-opt[data-route="all"]');
      if(!allOption){
        list.insertAdjacentHTML("afterbegin", allRoutesOptionHtml());
        allOption = list.querySelector('.dd-opt[data-route="all"]');
      }
      concrete = Array.from(list.querySelectorAll('.dd-opt[data-route]:not([data-route="all"])'));
      const allSelected = concrete.length > 0 && concrete.every(option => option.classList.contains("active"));
      dd.classList.toggle("dolo-all-routes-mode", allSelected);
      if(allOption){
        allOption.classList.toggle("active", allSelected);
        allOption.setAttribute("aria-selected", allSelected ? "true" : "false");
      }
    });
  }

  function clickFreshRouteOption(host, key, predicate){
    const selector = `.pf-exercise-route-filter .dd-opt[data-route="${String(key).replace(/"/g, "")}"]`;
    const option = host.querySelector(selector);
    if(option && (!predicate || predicate(option))) option.click();
  }

  function handleRouteClick(event){
    if(routeMutationInProgress) return;
    const anyOption = event.target.closest && event.target.closest('.pf-dd .dd-opt');
    const anyDropdown = anyOption && anyOption.closest('.pf-dd');
    if(anyDropdown && anyDropdown.dataset.routeModel === "native") return;
    if(anyDropdown && portalState.has(anyDropdown)) restoreDropdown(anyDropdown);
    const option = event.target.closest && event.target.closest('.pf-exercise-route-filter .dd-opt[data-route]');
    if(!option) return;
    const dd = option.closest(".pf-exercise-route-filter");
    if(dd && dd.dataset.routeModel === "native") return;
    const host = dd && dd.parentElement;
    if(!dd || !host) return;
    const key = option.dataset.route;
    const concrete = Array.from(dd.querySelectorAll('.dd-opt[data-route]:not([data-route="all"])'));
    const keys = concrete.map(node => node.dataset.route).filter(Boolean);
    const allMode = dd.classList.contains("dolo-all-routes-mode");

    if(key === "all"){
      event.preventDefault();
      event.stopPropagation();
      event.stopImmediatePropagation();
      routeMutationInProgress = true;
      try{
        keys.forEach(routeKey => clickFreshRouteOption(host, routeKey, node => !node.classList.contains("active")));
      }finally{
        routeMutationInProgress = false;
        scheduleEnhancements();
      }
      return;
    }

    if(allMode){
      event.preventDefault();
      event.stopPropagation();
      event.stopImmediatePropagation();
      routeMutationInProgress = true;
      try{
        keys.filter(routeKey => routeKey !== key)
          .forEach(routeKey => clickFreshRouteOption(host, routeKey, node => node.classList.contains("active")));
      }finally{
        routeMutationInProgress = false;
        scheduleEnhancements();
      }
    }
  }

  function portalDropdown(dd){
    const doc = root.document;
    if(!doc || portalState.has(dd) || dd.classList.contains("dolo-dropdown-portal")) return;
    const rect = dd.getBoundingClientRect();
    const placeholder = doc.createElement("span");
    const computed = root.getComputedStyle ? root.getComputedStyle(dd) : null;
    placeholder.className = "dolo-dropdown-placeholder";
    placeholder.style.display = computed && computed.display === "inline-flex" ? "inline-block" : (computed && computed.display) || "block";
    placeholder.style.width = `${Math.max(rect.width, 1)}px`;
    placeholder.style.height = `${Math.max(rect.height, 1)}px`;
    const parent = dd.parentNode;
    const nextSibling = dd.nextSibling;
    parent.insertBefore(placeholder, dd);
    doc.body.appendChild(dd);
    portalState.set(dd, {placeholder, parent, nextSibling});
    dd.classList.add("dolo-dropdown-portal");
    positionPortaledDropdown(dd);
  }

  function documentViewportHeight(){
    return root.document && root.document.documentElement
      ? root.document.documentElement.clientHeight
      : 0;
  }

  function positionPortaledDropdown(dd){
    const state = portalState.get(dd);
    if(!state || !state.placeholder.isConnected) return;
    const rect = state.placeholder.getBoundingClientRect();
    dd.style.setProperty("position", "fixed", "important");
    dd.style.setProperty("left", `${Math.round(rect.left)}px`, "important");
    dd.style.setProperty("top", `${Math.round(rect.top)}px`, "important");
    dd.style.setProperty("width", `${Math.max(Math.round(rect.width), 1)}px`, "important");
    dd.style.setProperty("z-index", "10050", "important");
    const panel = dd.querySelector(".dd-panel");
    if(panel){
      const viewportHeight = Math.max(root.innerHeight || 0, documentViewportHeight());
      const below = Math.max(96, viewportHeight - rect.bottom - 12);
      const above = Math.max(96, rect.top - 12);
      const naturalHeight = Math.min(panel.scrollHeight || 0, 360);
      const openUp = naturalHeight > below && above > below;
      dd.classList.toggle("dolo-dropdown-up", openUp);
      panel.style.setProperty("max-height", `${Math.floor(Math.min(360, openUp ? above : below))}px`, "important");
    }
  }

  function restoreDropdown(dd){
    const state = portalState.get(dd);
    if(!state) return;
    dd.classList.remove("dolo-dropdown-portal","dolo-dropdown-up");
    ["position","left","top","width","z-index"].forEach(prop => dd.style.removeProperty(prop));
    const panel = dd.querySelector(".dd-panel");
    if(panel) panel.style.removeProperty("max-height");
    if(state.placeholder.parentNode){
      state.placeholder.parentNode.insertBefore(dd, state.placeholder);
      state.placeholder.remove();
    }else if(state.parent){
      state.parent.insertBefore(dd, state.nextSibling && state.nextSibling.parentNode === state.parent ? state.nextSibling : null);
    }
    portalState.delete(dd);
  }

  function syncDropdownPortals(scope){
    const doc = root.document;
    if(!doc) return;
    (scope || doc).querySelectorAll(".pf-dd").forEach(dd => {
      if(dd.dataset.doloDropdownMode === "static"){
        if(portalState.has(dd)) restoreDropdown(dd);
        return;
      }
      const panel = dd.querySelector(".dd-panel");
      if(panel && panel.classList.contains("show")) portalDropdown(dd);
      else if(portalState.has(dd)) restoreDropdown(dd);
    });
    doc.querySelectorAll("body > .pf-dd.dolo-dropdown-portal").forEach(dd => {
      const panel = dd.querySelector(".dd-panel");
      if(!panel || !panel.classList.contains("show")) restoreDropdown(dd);
    });
  }

  function runEnhancements(){
    enhancementFrame = 0;
    if(!root.document || !root.document.body) return;
    applyAddressOverrides();
    normalizeHistoryRows(root.document);
    normalizeSearchNoResults(root.document);
    normalizeEmptyStates(root.document);
    normalizeTableCorners(root.document);
    normalizeNewLockRows(root.document);
    normalizePositionActivityBadges(root.document);
    enhanceAllRoutes(root.document);
    normalizeRouteDropdownIcons(root.document);
    syncDropdownPortals(root.document);
  }

  function scheduleEnhancements(){
    if(enhancementFrame || !root.document) return;
    const raf = root.requestAnimationFrame || (callback => setTimeout(callback, 0));
    enhancementFrame = raf(runEnhancements);
  }

  function install(){
    if(installed || !root.document) return;
    installed = true;
    const start = () => {
      runEnhancements();
      root.document.addEventListener("click", handleRouteClick, true);
      const observer = new root.MutationObserver(scheduleEnhancements);
      observer.observe(root.document.body, {subtree:true, childList:true, attributes:true, attributeFilter:["class","style"]});
      root.addEventListener("resize", () => {
        root.document.querySelectorAll(".dolo-dropdown-portal").forEach(positionPortaledDropdown);
      }, {passive:true});
      root.addEventListener("scroll", () => {
        root.document.querySelectorAll(".dolo-dropdown-portal").forEach(positionPortaledDropdown);
      }, {passive:true, capture:true});
    };
    if(root.document.readyState === "loading") root.document.addEventListener("DOMContentLoaded", start, {once:true});
    else start();
  }

  if(root.document) install();

  return {
    escapeHtml,
    shortAddress,
    formatTxDate,
    formatTxDateValue,
    txExplorerUrl,
    walletCellHtml,
    normalizeTxHash,
    normalizedActionKey,
    historyActionsForTransaction,
    routeSelectionPlan,
    emptyStateMessage,
    actionIconSvg,
    applyAddressOverrides,
    install,
    runEnhancements,
    version:UX_VERSION,
  };
});
