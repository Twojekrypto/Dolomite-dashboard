(function () {
  "use strict";

  const HISTORY_VERSION = "history-20260708-archived-chains";
  const TAX_REPORT_SCOPE = "Dolomite protocol activity only";
  const TAX_EXTERNAL_COST_BASIS_INCLUDED = "no";
  const TAX_SCOPE_NOTES = "Excludes acquisition cost basis and activity before or after Dolomite.";
  const HISTORY_TABLE_COLSPAN = 7;
  const HISTORY_VISIBLE_PAGE_SIZE = 10;
  const PAGE_SIZE = 500;
  const MAX_PAGES = 40;
  const DEFAULT_GRAPH_TIMEOUT_MS = 25000;
  const DEFAULT_GRAPH_ATTEMPTS = 2;
  const HISTORY_GRAPH_OPTIONS = { timeoutMs: 8000, attempts: 1 };
  const HISTORY_CHAIN_CONCURRENCY = 3;
  const HISTORY_ENTITY_CONCURRENCY = 10;
  const HISTORY_CLASSIFICATION_RECEIPT_CONCURRENCY = 4;
  const HISTORY_BACKGROUND_GAS_CONCURRENCY = 2;
  const HISTORY_FINALIZE_BUDGET_MS = 20000;
  const START_YEAR = 2024;
  const DEFAULT_YEAR = String(Math.max(new Date().getUTCFullYear(), START_YEAR));
  const CHAIN_FILTER_ORDER = ["berachain", "arbitrum", "ethereum", "botanix", "mantle", "polygonzkevm", "xlayer"];
  const EARN_LEDGER_BASE = "data/earn-verified-ledger";
  const EARN_REWARDS_BASE = "data/earn-merkl-rewards";
  const EARN_SNAPSHOT_BASE = "data/earn-snapshots";
  const REWARD_CLAIM_EVENTS_URL = "data/reward-claim-events.json";
  const REWARD_CLAIM_EVENTS_BASE = "data/reward-claim-events";
  const ODOLO_CLAIM_EVENTS_URL = "data/odolo-claim-events.json";
  const REWARD_CLAIM_INDEX_CHAIN_KEYS = new Set(["berachain", "arbitrum", "mantle", "xlayer"]);
  const ODOLO_REWARDS_DISTRIBUTOR = "0x79e6e932bf6686a4d357d7821e6e08835ba8a026";
  const ODOLO_TOKEN_ADDRESS = "0x02e513b5b54ee216bf836ceb471507488fc89543";
  const NOTE_STORAGE_PREFIX = "dolomite-history-review-notes";
  const GAS_STORAGE_PREFIX = "dolomite-history-gas-v1";
  const GAS_STORAGE_TTL_MS = 30 * 24 * 60 * 60 * 1000;
  const RPC_GATEWAY_STORAGE_KEY = "dolomite-history-rpc-gateway";
  const FAST_GAS_STATUS = "skipped-fast";
  const BORROW_POSITION_LIFECYCLE_ACTIONS = new Set(["borrowPositionOpen", "borrowPositionClose"]);
  const BORROW_POSITION_OPEN_TOPIC = "0xfd9156bd20ce24a786c761efe71a3931de038c1f2620c1bb4720609bc742b58e";
  const BORROW_POSITION_CLOSE_TOPIC = "0x21281f8d59117d0399dc467dbdd321538ceffe3225e80e2bd4de6f1b3355cbc7";
  const OPEN_BORROW_POSITION_SELECTOR = "0xbb0a6fa5";
  const CLOSE_BORROW_POSITION_SELECTOR = "0x8fb8b6c7";

  const GRAPH_BASE = "https://subgraph.api.dolomite.io/api/public/1301d2d1-7a9d-4be4-9e9a-061cb8611549/subgraphs";
  const CHAINS = {
    ethereum: {
      name: "Ethereum",
      short: "ETH",
      icon: "https://icons.llamao.fi/icons/chains/rsz_ethereum.jpg",
      subgraph: "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-ethereum/latest/gn",
      explorerTx: "https://etherscan.io/tx/",
      explorerAddress: "https://etherscan.io/address/",
      nativeSymbol: "ETH",
      priceId: "coingecko:ethereum",
      llamaSlug: "ethereum",
      rpcs: ["https://eth.llamarpc.com/", "https://ethereum-rpc.publicnode.com/", "https://eth.drpc.org/"],
      rpcIdx: 0,
    },
    berachain: {
      name: "Berachain",
      short: "BERA",
      icon: "https://icons.llamao.fi/icons/chains/rsz_berachain.jpg",
      subgraph: "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-berachain-mainnet/latest/gn",
      explorerTx: "https://berascan.com/tx/",
      explorerAddress: "https://berascan.com/address/",
      nativeSymbol: "BERA",
      priceId: "coingecko:berachain-bera",
      llamaSlug: "berachain",
      rpcs: ["https://rpc.berachain.com/", "https://berachain-rpc.publicnode.com/", "https://berachain.drpc.org/"],
      rpcIdx: 0,
    },
    arbitrum: {
      name: "Arbitrum",
      short: "ARB",
      icon: "https://icons.llamao.fi/icons/chains/rsz_arbitrum.jpg",
      subgraph: "https://api.goldsky.com/api/public/project_clyuw4gvq4d5801tegx0aafpu/subgraphs/dolomite-arbitrum/latest/gn",
      explorerTx: "https://arbiscan.io/tx/",
      explorerAddress: "https://arbiscan.io/address/",
      nativeSymbol: "ETH",
      priceId: "coingecko:ethereum",
      llamaSlug: "arbitrum",
      rpcs: ["https://arb1.arbitrum.io/rpc", "https://arbitrum.drpc.org/", "https://arbitrum-one-rpc.publicnode.com/"],
      rpcIdx: 0,
    },
    mantle: {
      name: "Mantle",
      short: "MNT",
      icon: "https://icons.llamao.fi/icons/chains/rsz_mantle.jpg",
      subgraph: `${GRAPH_BASE}/dolomite-mantle/latest/gn`,
      explorerTx: "https://mantlescan.xyz/tx/",
      explorerAddress: "https://mantlescan.xyz/address/",
      nativeSymbol: "MNT",
      priceId: "coingecko:mantle",
      llamaSlug: "mantle",
      rpcs: ["https://rpc.mantle.xyz/", "https://mantle-rpc.publicnode.com/", "https://mantle.drpc.org/"],
      rpcIdx: 0,
    },
    botanix: {
      name: "Botanix",
      short: "BOT",
      historyLifecycle: "shuttingDown",
      historyLifecycleLabel: "Shutting down",
      icon: "https://icons.llamao.fi/icons/chains/rsz_botanix.jpg",
      subgraph: `${GRAPH_BASE}/dolomite-botanix/latest/gn`,
      explorerTx: "https://explorer.botanixlabs.dev/tx/",
      explorerAddress: "https://explorer.botanixlabs.dev/address/",
      nativeSymbol: "BTC",
      priceId: "coingecko:bitcoin",
      llamaSlug: "botanix",
      rpcs: ["https://rpc.botanixlabs.com", "https://rpc.ankr.com/botanix_mainnet"],
      rpcIdx: 0,
    },
    polygonzkevm: {
      name: "Polygon zkEVM",
      short: "zkEVM",
      historyLifecycle: "archived",
      historyLifecycleLabel: "Archived",
      icon: "https://icons.llamao.fi/icons/chains/rsz_polygon%20zkevm.jpg",
      subgraph: `${GRAPH_BASE}/dolomite-polygon-zkevm/latest/gn`,
      explorerTx: "https://zkevm.polygonscan.com/tx/",
      explorerAddress: "https://zkevm.polygonscan.com/address/",
      nativeSymbol: "ETH",
      priceId: "coingecko:ethereum",
      llamaSlug: "polygon_zkevm",
      rpcs: ["https://zkevm-rpc.com/", "https://polygon-zkevm.drpc.org/"],
      rpcIdx: 0,
    },
    xlayer: {
      name: "X Layer",
      short: "XLAY",
      icon: "https://icons.llamao.fi/icons/chains/rsz_x%20layer.jpg",
      subgraph: `${GRAPH_BASE}/dolomite-x-layer/latest/gn`,
      explorerTx: "https://www.oklink.com/xlayer/tx/",
      explorerAddress: "https://www.oklink.com/xlayer/address/",
      nativeSymbol: "OKB",
      priceId: "coingecko:okb",
      llamaSlug: "xlayer",
      rpcs: ["https://rpc.xlayer.tech/", "https://xlayer.drpc.org/"],
      rpcIdx: 0,
    },
  };

  const ACTION_LABELS = {
    deposit: "Deposit",
    withdraw: "Withdraw",
    borrow: "Borrow",
    repay: "Repay",
    openBorrow: "Open Borrow",
    closeBorrow: "Close Borrow",
    borrowPositionOpen: "Open Borrow",
    borrowPositionClose: "Close Borrow",
    addCollateral: "Add Collateral",
    withdrawCollateral: "Withdraw Collateral",
    transfer: "Transfer",
    trade: "Trade",
    liquidation: "Liquidation",
    vaporization: "Debt Settlement",
    zap: "Zap",
    asyncDeposit: "Delayed Deposit",
    asyncWithdrawal: "Delayed Withdraw",
    amm: "AMM / Liquidity",
    ammTrade: "AMM Trade",
    ammAddLiquidity: "Add Liquidity",
    ammRemoveLiquidity: "Remove Liquidity",
    vesting: "Pair / Claim veDOLO",
    vestingPair: "Pair oDOLO + DOLO",
    vestingClaim: "Claim veDOLO",
    vestingInternal: "Move veDOLO position",
    claim: "Claim",
    odoloClaim: "Claim oDOLO",
    rewardClaim: "Claim Rewards",
    rewardLevelUpdate: "Reward Level Update",
    classificationPending: "Checking classification...",
  };

  const ACTION_TABLE_LABELS = {
    deposit: "Deposit",
    withdraw: "Withdraw",
    borrow: "Borrow",
    repay: "Repay",
    openBorrow: "Open Borrow",
    closeBorrow: "Close Borrow",
    borrowPositionOpen: "Open Borrow",
    borrowPositionClose: "Close Borrow",
    addCollateral: "Add Collateral",
    withdrawCollateral: "Withdraw Collateral",
    transfer: "Transfer",
    trade: "Trade",
    liquidation: "Liquidation",
    vaporization: "Debt",
    zap: "Zap",
    asyncDeposit: "Delayed Dep",
    asyncWithdrawal: "Delayed Wd",
    amm: "AMM",
    ammTrade: "AMM Trade",
    ammAddLiquidity: "Add LP",
    ammRemoveLiquidity: "Remove LP",
    vesting: "Pair / Claim",
    vestingPair: "PAIR",
    vestingClaim: "CLAIM",
    vestingInternal: "MOVE",
    claim: "Claim",
    odoloClaim: "oDOLO Claim",
    rewardClaim: "Reward Claim",
    rewardLevelUpdate: "Reward Level",
    classificationPending: "Checking classification...",
  };

  const CLASSIFICATION_SOURCE_LABELS = {
    borrow_position_lifecycle: "Borrow position lifecycle",
    borrow_position_calldata: "Borrow position calldata",
    borrow_position_receipt: "Borrow position receipt",
    current_balance_replay: "Current balance replay",
    range_replay: "Range replay",
    semantic: "Semantic event",
  };

  const state = {
    address: "",
    year: DEFAULT_YEAR,
    dateFrom: "",
    dateTo: "",
    action: "all",
    selectedActions: new Set(),
    selectedChains: new Set(defaultChainKeys()),
    rows: [],
    filteredRows: [],
    expandedKey: "",
    visiblePage: 1,
    loading: false,
    runId: 0,
    gasChecked: 0,
    gasTotal: 0,
    loadingPhase: "idle",
    loadingStartedAt: 0,
    loadedChains: 0,
    chainTotal: 0,
    warnings: [],
    earn: emptyEarnState(),
    reviewNotes: {},
    filtersDirty: false,
    loadedScope: null,
  };

  const els = {};
  const priceCache = new Map();
  const rewardClaimEventsPromises = new Map();
  const gasCache = new Map();
  const interestIndexCache = new Map();
  let loadingTicker = 0;
  let reportMenuCloseTimer = 0;
  let historyDatePicker = { open: "from", monthTs: 0 };

  const TX_FIELDS = "transaction { id timestamp blockNumber }";
  const TOKEN_FIELDS = "token { id symbol decimals marketId }";
  const PAIR_TOKEN_FIELDS = "id symbol decimals marketId";

  function init() {
    cacheElements();
    buildYearOptions();
    buildActionDropdown();
    buildChainFilters();
    wireEvents();
    hydrateFromUrl();
    render();
  }

  function cacheElements() {
    els.form = document.getElementById("history-form");
    els.address = document.getElementById("history-address");
    els.clearAddress = document.getElementById("history-clear-address");
    els.year = document.getElementById("history-year");
    els.yearButton = document.getElementById("history-year-button");
    els.yearLabel = document.getElementById("history-year-label");
    els.yearCount = document.getElementById("history-year-count");
    els.yearMenu = document.getElementById("history-year-menu");
    els.dateRange = document.getElementById("history-date-range");
    els.dateFrom = document.getElementById("history-date-from");
    els.dateTo = document.getElementById("history-date-to");
    els.dateFromText = document.getElementById("history-date-from-text");
    els.dateToText = document.getElementById("history-date-to-text");
    els.dateFromBtn = document.getElementById("history-date-from-btn");
    els.dateToBtn = document.getElementById("history-date-to-btn");
    els.datePopover = document.getElementById("history-date-popover");
    els.dateCalGrid = document.getElementById("history-date-cal-grid");
    els.dateCalTitle = document.getElementById("history-date-cal-title");
    els.dateCalSubtitle = document.getElementById("history-date-cal-subtitle");
    els.dateCalRange = document.getElementById("history-date-cal-range");
    els.dateCalPrev = document.getElementById("history-date-cal-prev");
    els.dateCalNext = document.getElementById("history-date-cal-next");
    els.dateCalToday = document.getElementById("history-date-cal-today");
    els.action = document.getElementById("history-action");
    els.actionButton = document.getElementById("history-action-button");
    els.actionLabel = document.getElementById("history-action-label");
    els.actionCount = document.getElementById("history-action-count");
    els.actionMenu = document.getElementById("history-action-menu");
    els.networkButton = document.getElementById("history-network-button");
    els.networkIcon = document.getElementById("history-network-icon");
    els.networkLabel = document.getElementById("history-network-label");
    els.networkCount = document.getElementById("history-network-count");
    els.networkMenu = document.getElementById("history-network-menu");
    els.run = document.getElementById("history-run");
    els.body = document.getElementById("history-body");
    els.tableWrap = document.querySelector(".table-wrap");
    els.pagination = document.getElementById("history-pagination");
    els.status = document.getElementById("history-status");
    els.count = document.getElementById("history-count");
    els.scopeInfo = document.querySelector(".history-scope-info");
    els.taxExport = document.getElementById("history-tax-export");
    els.reportButton = document.getElementById("history-report-button");
    els.reportMenu = document.getElementById("history-report-menu");
    els.loadingPanel = document.getElementById("history-loading-panel");
    els.loadingTitle = document.getElementById("history-loading-title");
    els.loadingSub = document.getElementById("history-loading-sub");
    els.loadingEta = document.getElementById("history-loading-eta");
    els.loadingPercent = document.getElementById("history-loading-percent");
    els.loadingClock = document.getElementById("history-loading-clock");
    els.loadingBar = document.getElementById("history-loading-bar");
    els.loadingStepSubgraphs = document.getElementById("history-step-subgraphs");
    els.loadingStepReceipts = document.getElementById("history-step-receipts");
    els.loadingStepEvidence = document.getElementById("history-step-evidence");
    els.loadingStepReports = document.getElementById("history-step-reports");
    els.reportStatus = document.getElementById("history-report-status");
    els.reportDetail = document.getElementById("history-report-detail");
    els.reportProgress = document.getElementById("history-report-progress");
    els.reportProgressBar = document.getElementById("history-report-progress-bar");
    els.reportJson = document.getElementById("history-report-json");
    els.reportPrint = document.getElementById("history-report-print");
  }

  function buildYearOptions() {
    els.year.innerHTML = `<option value="custom">Custom range</option>`;
    state.year = "custom";
    els.year.value = state.year;
    els.yearMenu.innerHTML = dateDropdownPanelHtml();
    cacheDateRangeElements();
    ensureCustomRangeDefaults();
    syncYearDropdown();
    syncDateRangeControls();
  }

  function cacheDateRangeElements() {
    els.dateRange = document.getElementById("history-date-range");
    els.dateFrom = document.getElementById("history-date-from");
    els.dateTo = document.getElementById("history-date-to");
    els.dateFromText = document.getElementById("history-date-from-text");
    els.dateToText = document.getElementById("history-date-to-text");
    els.dateFromBtn = document.getElementById("history-date-from-btn");
    els.dateToBtn = document.getElementById("history-date-to-btn");
    els.datePopover = document.getElementById("history-date-popover");
    els.dateCalGrid = document.getElementById("history-date-cal-grid");
    els.dateCalTitle = document.getElementById("history-date-cal-title");
    els.dateCalSubtitle = document.getElementById("history-date-cal-subtitle");
    els.dateCalRange = document.getElementById("history-date-cal-range");
    els.dateCalPrev = document.getElementById("history-date-cal-prev");
    els.dateCalNext = document.getElementById("history-date-cal-next");
    els.dateCalToday = document.getElementById("history-date-cal-today");
  }

  function dateDropdownPanelHtml() {
    return `
      <div class="history-dd-panel-head history-date-panel-head">
        <span>Date</span>
        <small>Select exact range, then Load history</small>
      </div>
      <div class="history-date-range" id="history-date-range">
        <div class="history-date-range-head">
          <span>Custom range only</span>
          <small>Applied after Load history</small>
        </div>
        <div class="history-date-fields">
          <label class="history-date-field" for="history-date-from-text">
            <span>From</span>
            <div class="history-date-trigger" id="history-date-from-shell">
              <input class="history-date-value history-date-manual" id="history-date-from-text" type="text" inputmode="numeric" autocomplete="off" enterkeyhint="done" spellcheck="false" maxlength="10" pattern="\\d{1,2}(?:[.]|/|-)\\d{1,2}(?:[.]|/|-)\\d{4}" title="Format: dd.mm.yyyy" placeholder="dd.mm.yyyy" aria-label="History start date">
              <button class="history-date-icon-btn" id="history-date-from-btn" type="button" data-history-date-bound="from" aria-haspopup="dialog" aria-expanded="false" aria-label="Open start date calendar">${calendarIconHtml()}</button>
            </div>
            <input id="history-date-from" name="from" type="hidden">
          </label>
          <label class="history-date-field" for="history-date-to-text">
            <span>To</span>
            <div class="history-date-trigger" id="history-date-to-shell">
              <input class="history-date-value history-date-manual" id="history-date-to-text" type="text" inputmode="numeric" autocomplete="off" enterkeyhint="done" spellcheck="false" maxlength="10" pattern="\\d{1,2}(?:[.]|/|-)\\d{1,2}(?:[.]|/|-)\\d{4}" title="Format: dd.mm.yyyy" placeholder="dd.mm.yyyy" aria-label="History end date">
              <button class="history-date-icon-btn" id="history-date-to-btn" type="button" data-history-date-bound="to" aria-haspopup="dialog" aria-expanded="false" aria-label="Open end date calendar">${calendarIconHtml()}</button>
            </div>
            <input id="history-date-to" name="to" type="hidden">
          </label>
        </div>
        <div class="history-date-popover" id="history-date-popover" role="dialog" aria-label="Select history date" hidden>
          <div class="history-cal-head">
            <div class="history-cal-title">
              <strong id="history-date-cal-title">Month</strong>
              <span id="history-date-cal-subtitle">Select date</span>
            </div>
            <div class="history-cal-nav">
              <button class="history-cal-btn" id="history-date-cal-prev" type="button" aria-label="Previous month">${chevronLeftIconHtml()}</button>
              <button class="history-cal-btn" id="history-date-cal-next" type="button" aria-label="Next month">${chevronRightIconHtml()}</button>
            </div>
          </div>
          <div class="history-cal-weekdays" aria-hidden="true">
            <span>Mo</span><span>Tu</span><span>We</span><span>Th</span><span>Fr</span><span>Sa</span><span>Su</span>
          </div>
          <div class="history-cal-grid" id="history-date-cal-grid"></div>
          <div class="history-cal-foot">
            <span id="history-date-cal-range">Range</span>
            <button id="history-date-cal-today" type="button">Latest</button>
          </div>
        </div>
      </div>
    `;
  }

  function buildActionDropdown() {
    const options = Array.from(els.action.options).map(option => ({
      value: option.value,
      label: option.textContent || option.value,
    }));
    els.actionMenu.innerHTML = dropdownPanelHtml("Action", options.map(option => (
      dropdownOptionHtml("action", option.value, option.label, actionIconHtml(option.value), option.value === "all")
    )).join(""));
    syncActionDropdown();
  }

  function buildChainFilters() {
    const chainOptions = [
      dropdownOptionHtml("network", "all", "All Chains", globeIconHtml(), true),
      ...chainFilterKeys().map(key => {
        const chain = CHAINS[key];
        return dropdownOptionHtml("network", key, chainMenuLabel(key), `<img src="${escapeAttr(chain.icon)}" alt="" onerror="this.style.display='none'">`);
      }),
    ].join("");
    els.networkMenu.innerHTML = dropdownPanelHtml("Chain", chainOptions);
    syncNetworkDropdown();
  }

  function chainFilterKeys() {
    const preferred = CHAIN_FILTER_ORDER.filter(key => CHAINS[key]);
    const seen = new Set(preferred);
    return preferred.concat(Object.keys(CHAINS).filter(key => !seen.has(key)));
  }

  function defaultChainKeys() {
    const active = chainFilterKeys().filter(chainKey => !CHAINS[chainKey]?.historyLifecycle);
    return active.length ? active : chainFilterKeys();
  }

  function chainSelectionIsDefault() {
    return chainSetMatches(state.selectedChains, defaultChainKeys());
  }

  function chainSetMatches(set, keys) {
    if (!set || set.size !== keys.length) return false;
    return keys.every(chainKey => set.has(chainKey));
  }

  function chainMenuLabel(chainKey) {
    const chain = CHAINS[chainKey];
    if (!chain) return chainKey;
    return chain.historyLifecycleLabel ? `${chain.name} · ${chain.historyLifecycleLabel}` : chain.name;
  }

  function dropdownPanelHtml(title, optionsHtml) {
    const body = Array.isArray(optionsHtml) ? optionsHtml.join("") : optionsHtml;
    return `
      <div class="history-dd-panel-head">${escapeHtml(title)}</div>
      <div class="history-dd-list">${body}</div>
    `;
  }

  function dropdownOptionHtml(type, value, label, iconHtml, selectAll = false) {
    return `
      <button class="history-dd-opt ${selectAll ? "select-all" : ""}" type="button" data-history-${escapeAttr(type)}="${escapeAttr(value)}" role="option">
        <span class="history-dd-check">${checkIconHtml()}</span>
        <span class="history-dd-opt-ico">${iconHtml}</span>
        <span class="history-dd-name">${escapeHtml(label)}</span>
      </button>
    `;
  }

  function checkIconHtml() {
    return `<svg viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="2 6 5 9 10 3"/></svg>`;
  }

  function globeIconHtml() {
    return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>`;
  }

  function calendarIconHtml() {
    return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M8 2v4"/><path d="M16 2v4"/><rect x="3" y="5" width="18" height="16" rx="2"/><path d="M3 10h18"/></svg>`;
  }

  function chevronLeftIconHtml() {
    return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="m15 18-6-6 6-6"/></svg>`;
  }

  function chevronRightIconHtml() {
    return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="m9 18 6-6-6-6"/></svg>`;
  }

  function actionIconHtml(action) {
    if (action === "all") {
      return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3"/></svg>`;
    }
    return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 7h16"/><path d="M4 12h16"/><path d="M4 17h10"/></svg>`;
  }

  function normalizeActionFilter(value) {
    const action = String(value || "all");
    if (action === "trade" || action === "zap") return "swap";
    if (action === "odoloClaim" || action === "rewardClaim" || action === "vestingClaim") return "claim";
    return action;
  }

  function wireEvents() {
    els.form.addEventListener("submit", event => {
      event.preventDefault();
      lookup();
    });
    els.address.addEventListener("input", () => {
      syncAddressFieldValue(els.address.value);
      markHistoryFiltersDirty();
    });
    els.clearAddress.addEventListener("click", () => {
      syncAddressFieldValue("");
      markHistoryFiltersDirty();
      els.address.focus();
    });
    els.year.addEventListener("change", () => {
      state.year = els.year.value;
      ensureCustomRangeDefaults();
      syncYearDropdown();
      syncDateRangeControls();
      markHistoryFiltersDirty();
    });
    els.action.addEventListener("change", () => {
      setSelectedActionsFromValues([els.action.value]);
      syncActionDropdown();
      markHistoryFiltersDirty({ tryClientSide: true });
    });
    [els.dateFrom, els.dateTo].forEach(input => {
      const syncCustomDateInput = () => {
        if (state.year !== "custom") {
          state.year = "custom";
          els.year.value = state.year;
        }
        state.dateFrom = els.dateFrom.value;
        state.dateTo = els.dateTo.value;
        syncYearDropdown();
        syncDateRangeControls();
        markHistoryFiltersDirty();
      };
      input?.addEventListener("input", syncCustomDateInput);
      input?.addEventListener("change", syncCustomDateInput);
    });
    [els.dateFromText, els.dateToText].forEach(input => {
      const bound = input?.id === "history-date-from-text" ? "from" : "to";
      input?.addEventListener("focus", () => {
        openHistoryDatePicker(bound);
        input.select();
      });
      input?.addEventListener("input", () => {
        input.value = input.value.replace(/[^\d./-]/g, "").slice(0, 10);
        input.classList.remove("invalid");
        input.removeAttribute("aria-invalid");
      });
      input?.addEventListener("keydown", event => {
        if (event.key === "Enter") {
          event.preventDefault();
          if (applyManualHistoryDate(bound, { keepFocus: true })) input.blur();
        }
        if (event.key === "Escape") {
          event.preventDefault();
          input.classList.remove("invalid");
          input.removeAttribute("aria-invalid");
          updateHistoryDatePickerControls();
          closeHistoryDatePicker();
          input.blur();
        }
      });
      input?.addEventListener("blur", () => {
        if (input.classList.contains("invalid")) {
          input.classList.remove("invalid");
          input.removeAttribute("aria-invalid");
          updateHistoryDatePickerControls();
          return;
        }
        const current = bound === "from" ? state.dateFrom : state.dateTo;
        if (input.value.trim() !== formatManualHistoryDate(current) && !applyManualHistoryDate(bound)) {
          input.classList.remove("invalid");
          input.removeAttribute("aria-invalid");
        }
        updateHistoryDatePickerControls();
      });
    });
    [els.dateFromBtn, els.dateToBtn].forEach(button => {
      button?.addEventListener("click", event => {
        event.preventDefault();
        event.stopPropagation();
        const bound = button.dataset.historyDateBound || "from";
        if (historyDatePicker.open === bound && !els.datePopover?.hidden) closeHistoryDatePicker();
        else openHistoryDatePicker(bound);
      });
    });
    els.dateCalPrev?.addEventListener("click", event => {
      event.preventDefault();
      historyDatePicker.monthTs = addMonthsTs(historyDatePicker.monthTs, -1);
      renderHistoryDatePicker();
    });
    els.dateCalNext?.addEventListener("click", event => {
      event.preventDefault();
      historyDatePicker.monthTs = addMonthsTs(historyDatePicker.monthTs, 1);
      renderHistoryDatePicker();
    });
    els.dateCalToday?.addEventListener("click", event => {
      event.preventDefault();
      const latest = historyDateMaxTs();
      if (historyDatePicker.open === "from") setHistoryDateRange(inputDateFromTs(latest), state.dateTo, "from");
      else setHistoryDateRange(state.dateFrom, inputDateFromTs(latest), "to");
      closeHistoryDatePicker();
    });
    document.addEventListener("click", handleDocumentDropdownClick);
    document.addEventListener("keydown", event => {
      if (event.key === "Escape") {
        closeHistoryDropdowns();
        closeHistoryReportMenu();
        closeHistoryScopeInfo();
      }
    });
    els.scopeInfo?.addEventListener("click", toggleHistoryScopeInfo);
    els.yearButton.addEventListener("click", event => toggleHistoryDropdown(event, "year"));
    els.actionButton.addEventListener("click", event => toggleHistoryDropdown(event, "action"));
    els.networkButton.addEventListener("click", event => toggleHistoryDropdown(event, "network"));
    els.yearMenu.addEventListener("click", handleYearDropdownClick);
    els.actionMenu.addEventListener("click", handleActionDropdownClick);
    els.networkMenu.addEventListener("click", handleNetworkDropdownClick);
    els.reportButton?.addEventListener("click", toggleHistoryReportMenu);
    els.reportMenu?.addEventListener("mouseenter", openHistoryReportMenu);
    els.reportMenu?.addEventListener("mouseleave", scheduleHistoryReportMenuClose);
    els.reportMenu?.addEventListener("focusin", openHistoryReportMenu);
    els.reportMenu?.addEventListener("focusout", scheduleHistoryReportMenuClose);
    els.taxExport.addEventListener("click", exportHistoryReportCsv);
    els.reportJson.addEventListener("click", exportEvidenceJson);
    els.reportPrint.addEventListener("click", printAnnualStatement);
    els.reportMenu?.addEventListener("click", event => {
      if (event.target.closest(".report-format-btn")) closeHistoryReportMenu();
    });
    els.pagination?.addEventListener("click", event => {
      const button = event.target.closest("[data-history-page]");
      if (!button || button.disabled) return;
      goHistoryPage(Number(button.dataset.historyPage || 1));
    });
    els.body.addEventListener("click", event => {
      const row = event.target.closest("tr[data-row-key]");
      const detailButton = event.target.closest("[data-history-detail-toggle]");
      if (detailButton && row) {
        state.expandedKey = state.expandedKey === row.dataset.rowKey ? "" : row.dataset.rowKey;
        renderRows();
        return;
      }
      if (!row || event.target.closest("a,button")) return;
      state.expandedKey = state.expandedKey === row.dataset.rowKey ? "" : row.dataset.rowKey;
      renderRows();
    });
    const handleReviewNoteInput = event => {
      const field = event.target.closest("[data-review-note]");
      if (!field) return;
      state.reviewNotes[field.dataset.reviewNote] = field.value;
      saveReviewNotes();
    };
    els.body.addEventListener("input", handleReviewNoteInput);
    els.body.addEventListener("change", handleReviewNoteInput);
  }

  function toggleHistoryDropdown(event, key) {
    event.preventDefault();
    event.stopPropagation();
    const clear = event.target.closest("[data-history-clear]");
    if (clear) {
      clearHistoryFilter(clear.dataset.historyClear);
      return;
    }
    const button = dropdownButtonForKey(key);
    const panel = dropdownPanelForKey(key);
    if (!button || !panel || button.disabled) return;
    const wasOpen = panel.classList.contains("show");
    closeHistoryScopeInfo();
    closeHistoryReportMenu();
    closeHistoryDropdowns();
    if (!wasOpen) {
      panel.classList.add("show");
      button.classList.add("open");
      button.setAttribute("aria-expanded", "true");
    }
  }

  function clearHistoryFilter(key) {
    if (key === "year") {
      if (dateRangeIsDefault()) return;
      state.year = "custom";
      const defaults = defaultCustomRange();
      state.dateFrom = defaults.from;
      state.dateTo = defaults.to;
      els.year.value = state.year;
      els.dateFrom.value = state.dateFrom;
      els.dateTo.value = state.dateTo;
      syncYearDropdown();
      syncDateRangeControls();
      closeHistoryDropdowns();
      markHistoryFiltersDirty();
      return;
    }
    if (key === "action") {
      if (actionFilterAllSelected()) return;
      selectAllActions();
      syncActionDropdown();
      closeHistoryDropdowns();
      markHistoryFiltersDirty({ tryClientSide: true });
      return;
    }
    if (key === "network") {
      if (chainSelectionIsDefault()) return;
      state.selectedChains = new Set(defaultChainKeys());
      syncNetworkDropdown();
      closeHistoryDropdowns();
      markHistoryFiltersDirty({ tryClientSide: true });
    }
  }

  function handleDocumentDropdownClick(event) {
    if (event.target.closest(".history-dd")) return;
    if (event.target.closest(".history-report-menu")) return;
    if (event.target.closest(".history-scope-info")) return;
    closeHistoryDropdowns();
    closeHistoryReportMenu();
    closeHistoryScopeInfo();
  }

  function closeHistoryDropdowns() {
    closeHistoryDatePicker();
    [els.yearMenu, els.actionMenu, els.networkMenu].forEach(panel => panel?.classList.remove("show"));
    [els.yearButton, els.actionButton, els.networkButton].forEach(button => {
      button?.classList.remove("open");
      button?.setAttribute("aria-expanded", "false");
    });
  }

  function toggleHistoryReportMenu(event) {
    event.preventDefault();
    event.stopPropagation();
    const isOpen = els.reportMenu?.classList.contains("open");
    closeHistoryDropdowns();
    closeHistoryScopeInfo();
    closeHistoryReportMenu();
    if (!isOpen) {
      openHistoryReportMenu();
      els.reportMenu.classList.add("open");
    }
  }

  function clearHistoryReportMenuClose() {
    if (!reportMenuCloseTimer) return;
    window.clearTimeout(reportMenuCloseTimer);
    reportMenuCloseTimer = 0;
  }

  function openHistoryReportMenu() {
    if (!els.reportMenu) return;
    clearHistoryReportMenuClose();
    els.reportMenu.classList.add("hover-open");
    els.reportButton?.setAttribute("aria-expanded", "true");
  }

  function scheduleHistoryReportMenuClose() {
    clearHistoryReportMenuClose();
    reportMenuCloseTimer = window.setTimeout(() => {
      const active = document.activeElement;
      if (active && els.reportMenu?.contains(active)) {
        reportMenuCloseTimer = 0;
        return;
      }
      els.reportMenu?.classList.remove("hover-open");
      if (!els.reportMenu?.classList.contains("open")) {
        els.reportButton?.setAttribute("aria-expanded", "false");
      }
      reportMenuCloseTimer = 0;
    }, 180);
  }

  function closeHistoryReportMenu() {
    clearHistoryReportMenuClose();
    els.reportMenu?.classList.remove("open", "hover-open");
    els.reportButton?.setAttribute("aria-expanded", "false");
  }

  function toggleHistoryScopeInfo(event) {
    event.preventDefault();
    event.stopPropagation();
    const isOpen = els.scopeInfo?.classList.contains("open");
    closeHistoryDropdowns();
    closeHistoryReportMenu();
    closeHistoryScopeInfo();
    if (!isOpen) {
      els.scopeInfo.classList.add("open");
      els.scopeInfo.setAttribute("aria-expanded", "true");
      els.scopeInfo.querySelector(".history-scope-tooltip")?.setAttribute("aria-hidden", "false");
    }
  }

  function closeHistoryScopeInfo() {
    els.scopeInfo?.classList.remove("open");
    els.scopeInfo?.setAttribute("aria-expanded", "false");
    els.scopeInfo?.querySelector(".history-scope-tooltip")?.setAttribute("aria-hidden", "true");
  }

  function syncAddressFieldValue(value) {
    els.address.value = value;
    els.address.closest(".wallet-field")?.classList.toggle("has-value", !!els.address.value.trim());
  }

  function markHistoryFiltersDirty(options = {}) {
    if (state.loading) return;
    state.visiblePage = 1;
    state.expandedKey = "";
    if (options.tryClientSide && filtersCanReuseLoadedRows()) {
      state.filtersDirty = false;
      if (isAddress(state.address)) setUrlAddress(state.address);
      render();
      const count = state.filteredRows.length;
      const suffix = count === 1 ? "" : "s";
      setStatus(`Applied filters locally: ${count.toLocaleString()} transaction${suffix} match.`, "good");
      return;
    }
    const hasLoadedContext = !!state.address || state.rows.length > 0 || state.filteredRows.length > 0;
    state.filtersDirty = hasLoadedContext;
    syncControls();
    renderReportFiles();
    if (state.filtersDirty) {
      setStatus("Filters changed. Click Load history to apply the new selection.", "warn");
    }
  }

  function recordLoadedHistoryScope(address, chainKeys) {
    state.loadedScope = {
      address: normalizeAddress(address),
      year: state.year || "custom",
      dateFrom: state.dateFrom || "",
      dateTo: state.dateTo || "",
      action: actionFilterParam() || "all",
      chains: Array.from(chainKeys || selectedChainKeys()),
    };
  }

  function filtersCanReuseLoadedRows() {
    const scope = state.loadedScope;
    if (!scope || state.loading || !state.rows.length) return false;
    const currentAddress = normalizeAddress(state.address || els.address?.value || "");
    const currentFrom = els.dateFrom?.value || state.dateFrom || "";
    const currentTo = els.dateTo?.value || state.dateTo || "";
    if (!currentAddress || currentAddress !== normalizeAddress(scope.address)) return false;
    if ((state.year || "custom") !== (scope.year || "custom")) return false;
    if (currentFrom !== (scope.dateFrom || "") || currentTo !== (scope.dateTo || "")) return false;
    const loadedChains = new Set(scope.chains || []);
    if (!selectedChainKeys().every(chainKey => loadedChains.has(chainKey))) return false;

    const loadedAction = String(scope.action || "all");
    if (loadedAction === "all") return true;
    const loadedActions = new Set(loadedAction.split(",").map(normalizeActionFilter).filter(Boolean));
    return selectedActionKeys().every(action => loadedActions.has(action));
  }

  function ensureCustomRangeDefaults() {
    state.year = "custom";
    if (els.year) els.year.value = state.year;
    const defaults = defaultCustomRange();
    if (!state.dateFrom) state.dateFrom = defaults.from;
    if (!state.dateTo) state.dateTo = defaults.to;
    if (els.dateFrom) els.dateFrom.value = state.dateFrom;
    if (els.dateTo) els.dateTo.value = state.dateTo;
  }

  function defaultCustomRange(yearValue = DEFAULT_YEAR) {
    const today = new Date();
    const todayYear = Math.max(today.getUTCFullYear(), START_YEAR);
    const requestedYear = Math.max(START_YEAR, Math.min(todayYear, Number(yearValue) || todayYear));
    const todayInput = isoDateInput(Math.floor(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()) / 1000));
    return {
      from: `${requestedYear}-01-01`,
      to: requestedYear === today.getUTCFullYear() ? todayInput : `${requestedYear}-12-31`,
    };
  }

  function dateRangeIsDefault() {
    const defaults = defaultCustomRange();
    return state.year === "custom" && state.dateFrom === defaults.from && state.dateTo === defaults.to;
  }

  function syncDateRangeControls() {
    if (!els.dateRange) return;
    state.year = "custom";
    els.year.value = state.year;
    els.dateRange.hidden = false;
    els.yearMenu?.classList.add("has-custom-range");
    [els.dateFrom, els.dateTo, els.dateFromText, els.dateToText].forEach(input => {
      if (input) input.disabled = state.loading;
    });
    updateHistoryDatePickerControls();
    renderHistoryDatePicker();
  }

  function historyDateMinTs() {
    return Math.floor(Date.UTC(START_YEAR, 0, 1) / 1000);
  }

  function historyDateMaxTs() {
    const today = new Date();
    return Math.floor(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()) / 1000);
  }

  function inputDateFromTs(ts) {
    return new Date(ts * 1000).toISOString().slice(0, 10);
  }

  function historyDateInputTs(value) {
    if (!validDateInput(value)) return NaN;
    const [year, month, day] = String(value).split("-").map(Number);
    return Math.floor(Date.UTC(year, month - 1, day) / 1000);
  }

  function manualHistoryDateTs(value) {
    const raw = String(value || "").trim();
    const iso = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (iso) {
      const ts = historyDateInputTs(raw);
      return Number.isFinite(ts) ? ts : NaN;
    }
    const match = raw.match(/^(\d{1,2})[./-](\d{1,2})[./-](\d{4})$/);
    if (!match) return NaN;
    const day = Number(match[1]);
    const month = Number(match[2]);
    const year = Number(match[3]);
    if (!Number.isInteger(day) || !Number.isInteger(month) || !Number.isInteger(year)) return NaN;
    const ts = Math.floor(Date.UTC(year, month - 1, day) / 1000);
    const date = new Date(ts * 1000);
    if (date.getUTCFullYear() !== year || date.getUTCMonth() !== month - 1 || date.getUTCDate() !== day) return NaN;
    return ts;
  }

  function formatManualHistoryDate(value) {
    const ts = typeof value === "number" ? value : historyDateInputTs(value);
    if (!Number.isFinite(ts)) return "";
    const date = new Date(ts * 1000);
    const day = String(date.getUTCDate()).padStart(2, "0");
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    return `${day}.${month}.${date.getUTCFullYear()}`;
  }

  function formatHistoryCalendarDay(ts) {
    return new Date(ts * 1000).toLocaleDateString("en-US", { month: "short", day: "2-digit", year: "numeric", timeZone: "UTC" });
  }

  function historyMonthStartTs(ts) {
    const date = new Date(ts * 1000);
    return Math.floor(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1) / 1000);
  }

  function addMonthsTs(monthTs, delta) {
    const date = new Date(monthTs * 1000);
    return Math.floor(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + delta, 1) / 1000);
  }

  function sameHistoryDay(a, b) {
    return inputDateFromTs(a) === inputDateFromTs(b);
  }

  function clampHistoryTs(ts, minTs, maxTs) {
    return Math.max(minTs, Math.min(maxTs, ts));
  }

  function setHistoryDateRange(fromValue, toValue, changed) {
    const minTs = historyDateMinTs();
    const maxTs = historyDateMaxTs();
    let from = validDateInput(fromValue) ? historyDateInputTs(fromValue) : manualHistoryDateTs(fromValue);
    let to = validDateInput(toValue) ? historyDateInputTs(toValue) : manualHistoryDateTs(toValue);
    if (!Number.isFinite(from)) from = historyDateInputTs(state.dateFrom) || minTs;
    if (!Number.isFinite(to)) to = historyDateInputTs(state.dateTo) || maxTs;
    from = clampHistoryTs(from, minTs, maxTs);
    to = clampHistoryTs(to, minTs, maxTs);
    if (from > to) {
      if (changed === "from") to = from;
      else from = to;
    }
    state.year = "custom";
    state.dateFrom = inputDateFromTs(from);
    state.dateTo = inputDateFromTs(to);
    if (els.year) els.year.value = state.year;
    if (els.dateFrom) els.dateFrom.value = state.dateFrom;
    if (els.dateTo) els.dateTo.value = state.dateTo;
    syncYearDropdown();
    syncDateRangeControls();
    markHistoryFiltersDirty();
  }

  function markHistoryDateInvalid(input) {
    if (!input) return;
    input.classList.add("invalid");
    input.setAttribute("aria-invalid", "true");
    window.setTimeout(() => {
      input.classList.remove("invalid");
      input.removeAttribute("aria-invalid");
    }, 900);
  }

  function applyManualHistoryDate(bound, options = {}) {
    const input = bound === "from" ? els.dateFromText : els.dateToText;
    if (!input) return false;
    const typed = input.value.trim();
    const ts = manualHistoryDateTs(typed);
    if (!Number.isFinite(ts)) {
      markHistoryDateInvalid(input);
      if (options.keepFocus) {
        input.focus();
        input.select();
      }
      updateHistoryDatePickerControls();
      return false;
    }
    input.classList.remove("invalid");
    input.removeAttribute("aria-invalid");
    if (bound === "from") setHistoryDateRange(inputDateFromTs(ts), state.dateTo, "from");
    else setHistoryDateRange(state.dateFrom, inputDateFromTs(ts), "to");
    closeHistoryDatePicker();
    return true;
  }

  function updateHistoryDatePickerControls() {
    if (els.dateFrom && state.dateFrom) els.dateFrom.value = state.dateFrom;
    if (els.dateTo && state.dateTo) els.dateTo.value = state.dateTo;
    if (els.dateFromText && document.activeElement !== els.dateFromText) els.dateFromText.value = formatManualHistoryDate(state.dateFrom);
    if (els.dateToText && document.activeElement !== els.dateToText) els.dateToText.value = formatManualHistoryDate(state.dateTo);
    const updateButton = (button, open) => {
      if (!button) return;
      button.classList.toggle("open", open);
      button.closest(".history-date-trigger")?.classList.toggle("open", open);
      button.setAttribute("aria-expanded", open ? "true" : "false");
    };
    updateButton(els.dateFromBtn, historyDatePicker.open === "from" && !els.datePopover?.hidden);
    updateButton(els.dateToBtn, historyDatePicker.open === "to" && !els.datePopover?.hidden);
  }

  function closeHistoryDatePicker() {
    historyDatePicker.open = "";
    if (els.datePopover) els.datePopover.hidden = true;
    updateHistoryDatePickerControls();
  }

  function openHistoryDatePicker(bound) {
    if (!els.datePopover || state.loading) return;
    historyDatePicker.open = bound === "to" ? "to" : "from";
    const target = historyDatePicker.open === "from" ? state.dateFrom : state.dateTo;
    historyDatePicker.monthTs = historyMonthStartTs(historyDateInputTs(target) || historyDateMaxTs());
    els.datePopover.hidden = false;
    els.datePopover.dataset.side = historyDatePicker.open === "from" ? "left" : "right";
    updateHistoryDatePickerControls();
    renderHistoryDatePicker();
  }

  function renderHistoryDatePicker() {
    if (!els.datePopover || els.datePopover.hidden || !els.dateCalGrid) return;
    const minTs = historyDateMinTs();
    const maxTs = historyDateMaxTs();
    const fromTs = historyDateInputTs(state.dateFrom) || minTs;
    const toTs = historyDateInputTs(state.dateTo) || maxTs;
    if (!historyDatePicker.monthTs) historyDatePicker.monthTs = historyMonthStartTs(historyDatePicker.open === "to" ? toTs : fromTs);
    historyDatePicker.monthTs = historyMonthStartTs(clampHistoryTs(historyDatePicker.monthTs, historyMonthStartTs(minTs), historyMonthStartTs(maxTs)));
    const monthDate = new Date(historyDatePicker.monthTs * 1000);
    const year = monthDate.getUTCFullYear();
    const month = monthDate.getUTCMonth();
    const firstDay = Math.floor(Date.UTC(year, month, 1) / 1000);
    const startOffset = (new Date(firstDay * 1000).getUTCDay() + 6) % 7;
    const firstCell = firstDay - startOffset * 86400;
    if (els.dateCalTitle) {
      els.dateCalTitle.textContent = monthDate.toLocaleDateString("en-US", { month: "long", year: "numeric", timeZone: "UTC" });
    }
    if (els.dateCalSubtitle) els.dateCalSubtitle.textContent = historyDatePicker.open === "from" ? "Choose start date" : "Choose end date";
    if (els.dateCalRange) els.dateCalRange.textContent = `${formatHistoryCalendarDay(fromTs)} -> ${formatHistoryCalendarDay(toTs)}`;
    els.dateCalGrid.innerHTML = "";
    for (let i = 0; i < 42; i += 1) {
      const ts = firstCell + i * 86400;
      const date = new Date(ts * 1000);
      const dayMonth = date.getUTCMonth();
      const disabled = ts < minTs || ts > maxTs;
      const inRange = ts >= fromTs && ts <= toTs;
      const selected = sameHistoryDay(ts, fromTs) || sameHistoryDay(ts, toTs);
      const button = document.createElement("button");
      button.type = "button";
      button.className = "history-cal-day"
        + (dayMonth !== month ? " muted" : "")
        + (inRange ? " in-range" : "")
        + (selected ? " selected" : "");
      button.textContent = String(date.getUTCDate());
      button.disabled = disabled;
      button.dataset.historyDateValue = inputDateFromTs(ts);
      button.setAttribute("aria-label", formatHistoryCalendarDay(ts));
      button.addEventListener("click", event => {
        event.preventDefault();
        event.stopPropagation();
        if (historyDatePicker.open === "from") setHistoryDateRange(inputDateFromTs(ts), state.dateTo, "from");
        else setHistoryDateRange(state.dateFrom, inputDateFromTs(ts), "to");
        closeHistoryDatePicker();
      });
      els.dateCalGrid.appendChild(button);
    }
    const minMonth = historyMonthStartTs(minTs);
    const maxMonth = historyMonthStartTs(maxTs);
    if (els.dateCalPrev) els.dateCalPrev.disabled = historyDatePicker.monthTs <= minMonth;
    if (els.dateCalNext) els.dateCalNext.disabled = historyDatePicker.monthTs >= maxMonth;
  }

  function dropdownButtonForKey(key) {
    if (key === "year") return els.yearButton;
    if (key === "action") return els.actionButton;
    if (key === "network") return els.networkButton;
    return null;
  }

  function dropdownPanelForKey(key) {
    if (key === "year") return els.yearMenu;
    if (key === "action") return els.actionMenu;
    if (key === "network") return els.networkMenu;
    return null;
  }

  function handleYearDropdownClick(event) {
    event.stopPropagation();
  }

  function handleActionDropdownClick(event) {
    const option = event.target.closest("[data-history-action]");
    if (!option) return;
    event.preventDefault();
    event.stopPropagation();
    const action = normalizeActionFilter(option.dataset.historyAction);
    const allActions = actionFilterKeys();
    if (action === "all") {
      selectAllActions();
    } else if (actionFilterAllSelected()) {
      state.selectedActions = new Set([action]);
    } else if (state.selectedActions.has(action)) {
      state.selectedActions.delete(action);
      if (state.selectedActions.size === 0) selectAllActions();
    } else if (allActions.includes(action)) {
      state.selectedActions.add(action);
    }
    syncActionDropdown();
    markHistoryFiltersDirty({ tryClientSide: true });
  }

  function handleNetworkDropdownClick(event) {
    const option = event.target.closest("[data-history-network]");
    if (!option) return;
    event.preventDefault();
    event.stopPropagation();
    const chain = option.dataset.historyNetwork;
    const defaultChains = defaultChainKeys();
    const defaultSelected = chainSelectionIsDefault();
    if (chain === "all") {
      state.selectedChains = new Set(defaultChains);
    } else if (defaultSelected) {
      state.selectedChains = new Set([chain]);
    } else if (state.selectedChains.has(chain)) {
      state.selectedChains.delete(chain);
      if (state.selectedChains.size === 0) state.selectedChains = new Set(defaultChains);
    } else {
      state.selectedChains.add(chain);
    }
    syncNetworkDropdown();
    markHistoryFiltersDirty({ tryClientSide: true });
  }

  function hydrateFromUrl() {
    const params = new URLSearchParams(window.location.search);
    const address = normalizeAddress(params.get("address") || "");
    const year = String(params.get("year") || "");
    state.year = "custom";
    els.year.value = state.year;
    const fromParam = params.get("from") || "";
    const toParam = params.get("to") || "";
    if (validDateInput(fromParam) && validDateInput(toParam)) {
      state.dateFrom = fromParam;
      state.dateTo = toParam;
    } else if (year === "all") {
      state.dateFrom = `${START_YEAR}-01-01`;
      state.dateTo = defaultCustomRange().to;
    } else if (/^\d{4}$/.test(year)) {
      const legacyRange = defaultCustomRange(year);
      state.dateFrom = legacyRange.from;
      state.dateTo = legacyRange.to;
    } else {
      const defaults = defaultCustomRange();
      state.dateFrom = defaults.from;
      state.dateTo = defaults.to;
    }
    els.dateFrom.value = state.dateFrom;
    els.dateTo.value = state.dateTo;
    const actionParam = String(params.get("action") || "");
    if (actionParam) {
      setSelectedActionsFromValues(actionParam.split(","));
    }
    const chainParam = String(params.get("chains") || "");
    if (chainParam) {
      const requestedChains = chainParam.split(",").map(item => item.trim()).filter(key => CHAINS[key]);
      if (requestedChains.length) state.selectedChains = new Set(requestedChains);
    }
    syncYearDropdown();
    syncDateRangeControls();
    syncActionDropdown();
    syncNetworkDropdown();
    if (address) {
      syncAddressFieldValue(address);
      lookup();
    }
  }

  async function lookup() {
    const address = normalizeAddress(els.address.value);
    if (!isAddress(address)) {
      setStatus("Enter a valid 0x wallet address.", "warn");
      els.address.focus();
      return;
    }

    state.runId += 1;
    const runId = state.runId;
    state.loading = true;
    state.address = address;
    state.year = els.year.value;
    state.dateFrom = els.dateFrom.value;
    state.dateTo = els.dateTo.value;
    state.action = actionFilterParam() || "all";
    els.action.value = actionFilterAllSelected() ? "all" : (selectedActionKeys()[0] || "all");
    let bounds;
    try {
      bounds = getBounds(state.year);
    } catch (error) {
      state.loading = false;
      setStatus(error.message || String(error), "warn");
      if (state.year === "custom") (els.dateFrom.value ? els.dateTo : els.dateFrom)?.focus();
      return;
    }
    state.filtersDirty = false;
    state.reviewNotes = loadReviewNotes(address, state.year);
    state.rows = [];
    state.filteredRows = [];
    state.expandedKey = "";
    state.visiblePage = 1;
    state.gasChecked = 0;
    state.gasTotal = 0;
    state.loadingPhase = "subgraphs";
    state.loadingStartedAt = Date.now();
    state.loadedChains = 0;
    state.chainTotal = selectedChainKeys().length;
    state.warnings = [];
    state.earn = emptyEarnState("loading");
    state.loadedScope = null;
    render();
    setUrlAddress(address);

    try {
      const earnPromise = fetchEarnEnrichment(address, bounds, runId).then(earn => {
        if (runId !== state.runId) return null;
        state.earn = earn;
        render();
        return earn;
      });
      setStatus(`Scanning Dolomite subgraphs for ${bounds.label}...`);
      const chainKeys = selectedChainKeys();
      state.chainTotal = chainKeys.length;
      const [chainResults, balanceReplay] = await Promise.all([
        mapLimit(chainKeys, HISTORY_CHAIN_CONCURRENCY, async chainKey => {
          const result = await fetchChainHistory(chainKey, address, bounds);
          if (runId === state.runId) {
            state.loadedChains += 1;
            renderLoadingPanel();
          }
          return result;
        }),
        fetchBorrowReplayBalances(chainKeys, address, bounds),
      ]);
      if (runId !== state.runId) return;
      const allEvents = chainResults.flatMap(result => result.events || []);
      state.warnings = chainResults.flatMap(result => result.warnings || []).concat(balanceReplay.warnings || []);
      state.rows = groupEvents(allEvents, { currentBalances: balanceReplay.balances, currentBalanceReplay: balanceReplay.currentBalanceReplay });
      markReceiptClassificationPendingRows(state.rows);
      recordLoadedHistoryScope(address, chainKeys);
      state.loading = false;
      state.loadingPhase = "receipts";
      state.gasTotal = state.rows.length;
      render();

      if (!state.rows.length) {
        await earnPromise;
        if (runId !== state.runId) return;
        const earnEntries = earnTaxEntriesForCurrentView();
        state.loadingPhase = "done";
        if (earnEntries.length) {
          setStatus(`No Dolomite transactions found for ${bounds.label}, but candidate evidence has ${earnEntries.length.toLocaleString()} export row${earnEntries.length === 1 ? "" : "s"}.`, "good");
          render();
          return;
        }
        const warningText = compactDataWarningText();
        setStatus(`No Dolomite transactions found for the selected filters.${warningText}`, "warn");
        render();
        return;
      }

      setStatus(`Found ${state.rows.length.toLocaleString()} tx. Checking gas receipts, historical prices, and report evidence...`);
      const gasPromise = enrichGasForRows(state.rows, address, runId);
      const finalizeComplete = await waitForHistoryFinalize([gasPromise, earnPromise], HISTORY_FINALIZE_BUDGET_MS);
      if (runId !== state.runId) return;
      state.loadingPhase = finalizeComplete ? "done" : "receipts";
      const doneTone = !finalizeComplete ? "warn" : "good";
      const visibleRows = rowsMatchingCurrentFilters(state.rows);
      const visibleEvidenceEntries = earnTaxEntriesForCurrentView();
      setStatus(historyCompletionStatusMessage(state.rows.length, visibleRows.length, visibleEvidenceEntries.length, finalizeComplete), doneTone);
      render();
      if (!finalizeComplete) {
        Promise.allSettled([gasPromise, earnPromise]).then(() => {
          if (runId !== state.runId) return;
          state.loadingPhase = "done";
          const finalVisibleRows = rowsMatchingCurrentFilters(state.rows);
          const finalEvidenceEntries = earnTaxEntriesForCurrentView();
          const finalTone = "good";
          setStatus(historyCompletionStatusMessage(state.rows.length, finalVisibleRows.length, finalEvidenceEntries.length, true), finalTone);
          render();
        });
      }
    } catch (error) {
      if (runId !== state.runId) return;
      state.loading = false;
      state.loadingPhase = "error";
      if (state.earn?.status === "loading") {
        state.earn = {
          ...emptyEarnState("error"),
          warnings: [error.message || String(error)],
        };
      }
      setStatus(`History load failed: ${error.message || error}`, "warn");
      render();
    }
  }

  async function fetchChainHistory(chainKey, address, bounds) {
    const chain = CHAINS[chainKey];
    const timeFilter = `transaction_: { timestamp_gte: "${bounds.start}", timestamp_lte: "${bounds.end}" }`;
    const creationTimeFilter = `creationTransaction_: { timestamp_gte: "${bounds.start}", timestamp_lte: "${bounds.end}" }`;
    const executionTimeFilter = `executionTransaction_: { timestamp_gte: "${bounds.start}", timestamp_lte: "${bounds.end}" }`;
    const initiateTimeFilter = `initiateTransaction_: { timestamp_gte: "${bounds.start}", timestamp_lte: "${bounds.end}" }`;
    const fulfilmentTimeFilter = `fulfilmentTransaction_: { timestamp_gte: "${bounds.start}", timestamp_lte: "${bounds.end}" }`;
    const directTimeFilter = `timestamp_gte: "${bounds.start}", timestamp_lte: "${bounds.end}"`;
    const user = address.toLowerCase();
    const specs = [
      {
        entity: "deposits",
        where: `effectiveUser: "${user}", ${timeFilter}`,
        fields: `serialId ${TX_FIELDS} effectiveUser { id } ${TOKEN_FIELDS} amountDeltaWei amountUSDDeltaWei marginAccount { accountNumber }`,
        map: row => eventFromDeposit(chainKey, row),
      },
      {
        entity: "withdrawals",
        where: `effectiveUser: "${user}", ${timeFilter}`,
        fields: `serialId ${TX_FIELDS} effectiveUser { id } ${TOKEN_FIELDS} amountDeltaWei amountUSDDeltaWei marginAccount { accountNumber }`,
        map: row => eventFromWithdrawal(chainKey, row),
      },
      {
        entity: "transfers",
        where: `fromEffectiveUser: "${user}", ${timeFilter}`,
        fields: `serialId ${TX_FIELDS} fromEffectiveUser { id } toEffectiveUser { id } ${TOKEN_FIELDS} amountDeltaWei amountUSDDeltaWei isSelfTransfer isTransferForMarginPosition fromMarginAccount { accountNumber } toMarginAccount { accountNumber }`,
        map: row => eventFromTransfer(chainKey, row, "out"),
      },
      {
        entity: "transfers",
        where: `toEffectiveUser: "${user}", fromEffectiveUser_not: "${user}", ${timeFilter}`,
        fields: `serialId ${TX_FIELDS} fromEffectiveUser { id } toEffectiveUser { id } ${TOKEN_FIELDS} amountDeltaWei amountUSDDeltaWei isSelfTransfer isTransferForMarginPosition fromMarginAccount { accountNumber } toMarginAccount { accountNumber }`,
        map: row => eventFromTransfer(chainKey, row, "in"),
      },
      {
        entity: "trades",
        where: `takerEffectiveUser: "${user}", ${timeFilter}`,
        fields: `serialId ${TX_FIELDS} takerEffectiveUser { id } makerEffectiveUser { id } takerToken { ${PAIR_TOKEN_FIELDS} } makerToken { ${PAIR_TOKEN_FIELDS} } takerTokenDeltaWei makerTokenDeltaWei amountUSD takerAmountUSD makerAmountUSD takerMarginAccount { accountNumber } makerMarginAccount { accountNumber }`,
        map: row => eventFromTrade(chainKey, row, "taker"),
      },
      {
        entity: "trades",
        where: `makerEffectiveUser: "${user}", takerEffectiveUser_not: "${user}", ${timeFilter}`,
        fields: `serialId ${TX_FIELDS} takerEffectiveUser { id } makerEffectiveUser { id } takerToken { ${PAIR_TOKEN_FIELDS} } makerToken { ${PAIR_TOKEN_FIELDS} } takerTokenDeltaWei makerTokenDeltaWei amountUSD takerAmountUSD makerAmountUSD takerMarginAccount { accountNumber } makerMarginAccount { accountNumber }`,
        map: row => eventFromTrade(chainKey, row, "maker"),
      },
      {
        entity: "liquidations",
        where: `liquidEffectiveUser: "${user}", ${timeFilter}`,
        fields: `serialId ${TX_FIELDS} liquidEffectiveUser { id } solidEffectiveUser { id } heldToken { ${PAIR_TOKEN_FIELDS} } borrowedToken { ${PAIR_TOKEN_FIELDS} } heldTokenAmountDeltaWei borrowedTokenAmountDeltaWei heldTokenAmountUSD borrowedTokenAmountUSD liquidMarginAccount { accountNumber } solidMarginAccount { accountNumber }`,
        map: row => eventFromLiquidation(chainKey, row, "liquid"),
      },
      {
        entity: "liquidations",
        where: `solidEffectiveUser: "${user}", liquidEffectiveUser_not: "${user}", ${timeFilter}`,
        fields: `serialId ${TX_FIELDS} liquidEffectiveUser { id } solidEffectiveUser { id } heldToken { ${PAIR_TOKEN_FIELDS} } borrowedToken { ${PAIR_TOKEN_FIELDS} } heldTokenAmountDeltaWei borrowedTokenAmountDeltaWei heldTokenAmountUSD borrowedTokenAmountUSD liquidMarginAccount { accountNumber } solidMarginAccount { accountNumber }`,
        map: row => eventFromLiquidation(chainKey, row, "solid"),
      },
      {
        entity: "vaporizations",
        where: `vaporEffectiveUser: "${user}", ${timeFilter}`,
        fields: `serialId ${TX_FIELDS} vaporEffectiveUser { id } solidEffectiveUser { id } heldToken { ${PAIR_TOKEN_FIELDS} } borrowedToken { ${PAIR_TOKEN_FIELDS} } heldTokenAmountDeltaWei borrowedTokenAmountDeltaWei amountUSDVaporized solidMarginAccount { accountNumber } vaporMarginAccount { accountNumber }`,
        map: row => eventFromVaporization(chainKey, row, "vapor"),
      },
      {
        entity: "vaporizations",
        where: `solidEffectiveUser: "${user}", vaporEffectiveUser_not: "${user}", ${timeFilter}`,
        fields: `serialId ${TX_FIELDS} vaporEffectiveUser { id } solidEffectiveUser { id } heldToken { ${PAIR_TOKEN_FIELDS} } borrowedToken { ${PAIR_TOKEN_FIELDS} } heldTokenAmountDeltaWei borrowedTokenAmountDeltaWei amountUSDVaporized solidMarginAccount { accountNumber } vaporMarginAccount { accountNumber }`,
        map: row => eventFromVaporization(chainKey, row, "solid"),
      },
      {
        entity: "zaps",
        orderBy: "id",
        where: `effectiveUser: "${user}", ${timeFilter}`,
        fields: `id ${TX_FIELDS} effectiveUser { id } marginAccount { accountNumber } tokenPath { ${PAIR_TOKEN_FIELDS} } amountInToken amountOutToken amountInUSD amountOutUSD`,
        map: row => eventFromZap(chainKey, row),
      },
      {
        entity: "borrowPositions",
        orderBy: "id",
        where: `effectiveUser: "${user}", openTimestamp_gte: "${bounds.start}", openTimestamp_lte: "${bounds.end}"`,
        fields: `id effectiveUser { id } marginAccount { accountNumber } openTimestamp closeTimestamp status openTransaction { id timestamp blockNumber } closeTransaction { id timestamp blockNumber }`,
        map: row => eventFromBorrowPositionLifecycle(chainKey, row, "open"),
      },
      {
        entity: "borrowPositions",
        orderBy: "id",
        where: `effectiveUser: "${user}", closeTimestamp_gte: "${bounds.start}", closeTimestamp_lte: "${bounds.end}"`,
        fields: `id effectiveUser { id } marginAccount { accountNumber } openTimestamp closeTimestamp status openTransaction { id timestamp blockNumber } closeTransaction { id timestamp blockNumber }`,
        map: row => eventFromBorrowPositionLifecycle(chainKey, row, "close"),
      },
      {
        entity: "asyncDeposits",
        orderBy: "id",
        where: `effectiveUser: "${user}", ${creationTimeFilter}`,
        fields: `id key creationTransaction { id timestamp blockNumber } executionTransaction { id timestamp blockNumber } marginAccount { accountNumber } status inputToken { symbol } inputAmount outputToken { symbol } minOutputAmount outputAmount`,
        map: row => eventFromAsync(chainKey, row, "asyncDeposit", "created", "creationTransaction"),
      },
      {
        entity: "asyncDeposits",
        orderBy: "id",
        where: `effectiveUser: "${user}", ${executionTimeFilter}`,
        fields: `id key creationTransaction { id timestamp blockNumber } executionTransaction { id timestamp blockNumber } marginAccount { accountNumber } status inputToken { symbol } inputAmount outputToken { symbol } minOutputAmount outputAmount`,
        map: row => eventFromAsync(chainKey, row, "asyncDeposit", "executed", "executionTransaction"),
      },
      {
        entity: "asyncWithdrawals",
        orderBy: "id",
        where: `effectiveUser: "${user}", ${creationTimeFilter}`,
        fields: `id key creationTransaction { id timestamp blockNumber } executionTransaction { id timestamp blockNumber } marginAccount { accountNumber } status inputToken { symbol } inputAmount outputToken { symbol } minOutputAmount outputAmount isLiquidation`,
        map: row => eventFromAsync(chainKey, row, "asyncWithdrawal", "created", "creationTransaction"),
      },
      {
        entity: "asyncWithdrawals",
        orderBy: "id",
        where: `effectiveUser: "${user}", ${executionTimeFilter}`,
        fields: `id key creationTransaction { id timestamp blockNumber } executionTransaction { id timestamp blockNumber } marginAccount { accountNumber } status inputToken { symbol } inputAmount outputToken { symbol } minOutputAmount outputAmount isLiquidation`,
        map: row => eventFromAsync(chainKey, row, "asyncWithdrawal", "executed", "executionTransaction"),
      },
      {
        entity: "ammTrades",
        where: `sender: "${user}", ${directTimeFilter}`,
        fields: `serialId ${TX_FIELDS} timestamp pair { token0 { symbol } token1 { symbol } } sender from to amount0In amount1In amount0Out amount1Out amountUSD`,
        map: row => eventFromAmmTrade(chainKey, row, "sender"),
      },
      {
        entity: "ammTrades",
        where: `from: "${user}", sender_not: "${user}", ${directTimeFilter}`,
        fields: `serialId ${TX_FIELDS} timestamp pair { token0 { symbol } token1 { symbol } } sender from to amount0In amount1In amount0Out amount1Out amountUSD`,
        map: row => eventFromAmmTrade(chainKey, row, "from"),
      },
      {
        entity: "ammTrades",
        where: `to: "${user}", sender_not: "${user}", from_not: "${user}", ${directTimeFilter}`,
        fields: `serialId ${TX_FIELDS} timestamp pair { token0 { symbol } token1 { symbol } } sender from to amount0In amount1In amount0Out amount1Out amountUSD`,
        map: row => eventFromAmmTrade(chainKey, row, "to"),
      },
      {
        entity: "ammMints",
        where: `sender: "${user}", ${directTimeFilter}`,
        fields: `serialId ${TX_FIELDS} timestamp pair { token0 { symbol } token1 { symbol } } sender to amount0 amount1 liquidity amountUSD`,
        map: row => eventFromAmmLiquidity(chainKey, row, "mint", "sender"),
      },
      {
        entity: "ammMints",
        where: `to: "${user}", sender_not: "${user}", ${directTimeFilter}`,
        fields: `serialId ${TX_FIELDS} timestamp pair { token0 { symbol } token1 { symbol } } sender to amount0 amount1 liquidity amountUSD`,
        map: row => eventFromAmmLiquidity(chainKey, row, "mint", "to"),
      },
      {
        entity: "ammBurns",
        where: `sender: "${user}", ${directTimeFilter}`,
        fields: `serialId ${TX_FIELDS} timestamp pair { token0 { symbol } token1 { symbol } } sender to amount0 amount1 liquidity amountUSD`,
        map: row => eventFromAmmLiquidity(chainKey, row, "burn", "sender"),
      },
      {
        entity: "ammBurns",
        where: `to: "${user}", sender_not: "${user}", ${directTimeFilter}`,
        fields: `serialId ${TX_FIELDS} timestamp pair { token0 { symbol } token1 { symbol } } sender to amount0 amount1 liquidity amountUSD`,
        map: row => eventFromAmmLiquidity(chainKey, row, "burn", "to"),
      },
      {
        entity: "liquidityMiningVestingPositionTransfers",
        where: `fromEffectiveUser: "${user}", ${timeFilter}`,
        fields: `serialId ${TX_FIELDS} fromEffectiveUser { id } toEffectiveUser { id } vestingPosition { positionId oTokenAmount paymentAmountWei pairTaxesPaid status }`,
        map: row => eventFromVesting(chainKey, row, "out"),
      },
      {
        entity: "liquidityMiningVestingPositionTransfers",
        where: `toEffectiveUser: "${user}", ${timeFilter}`,
        fields: `serialId ${TX_FIELDS} fromEffectiveUser { id } toEffectiveUser { id } vestingPosition { positionId oTokenAmount paymentAmountWei pairTaxesPaid status }`,
        map: row => sameEffectiveVestingUser(row) ? [] : eventFromVesting(chainKey, row, "in"),
      },
      {
        entity: "liquidityMiningLevelUpdateRequests",
        orderBy: "id",
        where: `user: "${user}", ${initiateTimeFilter}`,
        fields: `id requestId user { id } level isFulfilled initiateTransaction { id timestamp blockNumber } fulfilmentTransaction { id timestamp blockNumber }`,
        map: row => eventFromRewardLevelUpdate(chainKey, row, "initiated", "initiateTransaction"),
      },
      {
        entity: "liquidityMiningLevelUpdateRequests",
        orderBy: "id",
        where: `user: "${user}", ${fulfilmentTimeFilter}`,
        fields: `id requestId user { id } level isFulfilled initiateTransaction { id timestamp blockNumber } fulfilmentTransaction { id timestamp blockNumber }`,
        map: row => eventFromRewardLevelUpdate(chainKey, row, "fulfilled", "fulfilmentTransaction"),
      },
    ];

    const warnings = [];
    const batches = await mapLimit(specs, HISTORY_ENTITY_CONCURRENCY, async spec => {
      try {
        const result = await paginateEntity(chain.subgraph, spec.entity, spec.where, spec.fields, spec.orderBy, HISTORY_GRAPH_OPTIONS);
        if (result.truncated) {
          warnings.push(`${chain.name} ${spec.entity} reached ${result.rows.length.toLocaleString()} rows; narrow the year or chain filter for exact export.`);
        }
        return result.rows.flatMap(row => eventsWithSourceEntity(spec.map(row), spec.entity));
      } catch (error) {
        console.debug(`History query failed for ${chain.name} ${spec.entity}:`, error);
        warnings.push(`${chain.name} ${spec.entity} unavailable: ${error.message || String(error)}`);
        return [];
      }
    });

    const claimResult = await fetchRewardClaimEvents(chainKey, address, bounds);
    warnings.push(...(claimResult.warnings || []));

    const events = batches.flat()
      .concat(claimResult.events || [])
      .filter(event => event.txHash);

    return { events, warnings };
  }

  async function loadRewardClaimPayload(chainKey) {
    if (!REWARD_CLAIM_INDEX_CHAIN_KEYS.has(chainKey)) {
      return { payload: { chains: {}, events: [] } };
    }
    if (!rewardClaimEventsPromises.has(chainKey)) {
      const promise = fetchOptionalJson(`${REWARD_CLAIM_EVENTS_BASE}/${chainKey}.json`).then(async result => {
        if (!result.error) return result;
        const combined = await fetchOptionalJson(REWARD_CLAIM_EVENTS_URL);
        if (!combined.error && Array.isArray(combined.payload?.events) && combined.payload.events.length) {
          return {
            payload: {
              ...combined.payload,
              chains: combined.payload?.chains?.[chainKey] ? { [chainKey]: combined.payload.chains[chainKey] } : {},
              events: (combined.payload.events || []).filter(row => (row.chainKey || combined.payload.chainKey || "berachain") === chainKey),
            },
          };
        }
        if (chainKey === "berachain") {
          const legacy = await fetchOptionalJson(ODOLO_CLAIM_EVENTS_URL);
          if (!legacy.error) {
            return {
              payload: {
                schemaVersion: 1,
                chains: {
                  berachain: {
                    chainKey: "berachain",
                    chainName: "Berachain",
                    fromTimestamp: legacy.payload?.fromTimestamp || 0,
                    toTimestamp: legacy.payload?.toTimestamp || 0,
                    source: legacy.payload?.source || "Berachain RewardClaimed logs",
                  },
                },
                events: (legacy.payload?.events || []).map(row => ({
                  ...row,
                  chainKey: "berachain",
                  chainName: "Berachain",
                  tokenSymbol: row.tokenSymbol || "oDOLO",
                  tokenAddress: row.tokenAddress || ODOLO_TOKEN_ADDRESS,
                  tokenDecimals: row.tokenDecimals || 18,
                })),
              },
            };
          }
        }
        return result;
      });
      rewardClaimEventsPromises.set(chainKey, promise);
    }
    return rewardClaimEventsPromises.get(chainKey);
  }

  async function fetchRewardClaimEvents(chainKey, address, bounds) {
    const chain = CHAINS[chainKey];
    const chainName = chain?.name || chainKey;
    const selected = selectedActionKeys();
    const claimFilterRelevant = actionFilterAllSelected()
      || selected.includes("claim");
    if (!claimFilterRelevant || !REWARD_CLAIM_INDEX_CHAIN_KEYS.has(chainKey)) {
      return { events: [], warnings: [] };
    }
    const result = await loadRewardClaimPayload(chainKey);
    if (result.error) {
      return {
        events: [],
        warnings: claimFilterRelevant ? [`${chainName} reward claims unavailable: ${result.error}`] : [],
      };
    }
    const payload = result.payload || {};
    const warnings = [];
    const hasChainMeta = !!(payload.chains?.[chainKey] || (payload.chainKey === chainKey ? payload : null));
    const meta = payload.chains?.[chainKey] || (payload.chainKey === chainKey ? payload : {});
    const coverageStatus = String(meta.coverageStatus || "").toLowerCase();
    const fromTimestamp = Number(meta.fromTimestamp || 0);
    const toTimestamp = Number(meta.toTimestamp || 0);
    if (claimFilterRelevant && REWARD_CLAIM_INDEX_CHAIN_KEYS.has(chainKey) && !hasChainMeta) {
      warnings.push(`${chainName} reward claim index is missing; reward claim transactions may be incomplete until the RewardClaimed workflow scans this chain.`);
    }
    if (claimFilterRelevant && coverageStatus !== "complete" && fromTimestamp && bounds?.start && bounds.start < fromTimestamp) {
      warnings.push(`${chainName} reward claim index starts ${formatDate(fromTimestamp)}; earlier reward claims may be missing until the full claim-event workflow refreshes.`);
    }
    const now = Math.floor(Date.now() / 1000);
    if (claimFilterRelevant && toTimestamp && bounds?.end && Math.min(bounds.end, now) > toTimestamp + 3600) {
      warnings.push(`${chainName} reward claim index is current through ${formatDate(toTimestamp)}; newer reward claims may be missing until the next workflow refresh.`);
    }
    if (claimFilterRelevant && meta.warning) warnings.push(`${chainName} reward claim index warning: ${meta.warning}`);
    const wallet = normalizeAddress(address);
    const events = (Array.isArray(payload.events) ? payload.events : [])
      .map(row => ({ ...row, chainKey: row.chainKey || payload.chainKey || "berachain" }))
      .filter(row => row.chainKey === chainKey)
      .filter(row => normalizeAddress(row.user) === wallet)
      .filter(row => timestampOverlapsBounds(Number(row.timestamp || 0), bounds))
      .map(row => eventFromRewardClaim(chainKey, row));
    return { events, warnings };
  }

  function eventFromOdoloClaim(chainKey, row) {
    return eventFromRewardClaim(chainKey, {
      ...row,
      tokenSymbol: row.tokenSymbol || "oDOLO",
      tokenAddress: row.tokenAddress || ODOLO_TOKEN_ADDRESS,
      tokenDecimals: row.tokenDecimals || 18,
    });
  }

  function eventFromRewardClaim(chainKey, row) {
    const symbol = String(row.tokenSymbol || (chainKey === "berachain" ? "oDOLO" : "Reward")).trim() || "Reward";
    const tokenAddress = row.tokenAddress || (symbol === "oDOLO" ? ODOLO_TOKEN_ADDRESS : "");
    const isOdoloClaim = chainKey === "berachain" && symbol.toLowerCase() === "odolo";
    const action = isOdoloClaim ? "odoloClaim" : "rewardClaim";
    const sourceEntity = isOdoloClaim ? "odoloRewardClaimEvents" : "rewardClaimEvents";
    const taxCategory = isOdoloClaim ? "odolo_reward_claim" : "reward_claim";
    const amount = cleanAmount(row.amount || "");
    return {
      chainKey,
      txHash: String(row.txHash || "").toLowerCase(),
      timestamp: Number(row.timestamp || 0),
      blockNumber: String(row.blockNumber || ""),
      action,
      role: "in",
      serialId: `${action}-${row.epoch ?? ""}-${row.logIndex ?? ""}`,
      ...taxFields(taxCategory, "income_candidate", [
        assetLeg("in", { symbol, id: tokenAddress }, row.amount || "", 0),
      ], `${symbol} reward claimed on Dolomite. Report records the on-chain claim event and asset amount; final tax treatment depends on jurisdiction.`),
      account: "",
      counterparty: row.distributor || ODOLO_REWARDS_DISTRIBUTOR,
      label: amount ? `+${amount} ${symbol} reward` : `${symbol} reward claim`,
      asset: symbol,
      usd: 0,
      rewardEpoch: row.epoch ?? "",
      rewardAmountWei: row.amountWei || "",
      sourceEntity,
    };
  }

  function eventFromRewardLevelUpdate(chainKey, row, stage, txProp) {
    const level = row.level ?? "";
    const requestId = row.requestId || row.id || "";
    const stageLabel = stage === "fulfilled" ? "fulfilled" : "requested";
    const label = level !== "" ? `Reward level ${level} ${stageLabel}` : `Reward level update ${stageLabel}`;
    return {
      ...eventBase(chainKey, row, "rewardLevelUpdate", stage, txProp),
      ...taxFields("reward_level_update", "not_taxable_by_default", [], "Dolomite reward-level operation. Report records the on-chain request/fulfilment transaction; no token movement is recorded in this event."),
      account: "",
      counterparty: "",
      label,
      asset: "Reward level",
      usd: 0,
      rewardLevel: String(level),
      rewardRequestId: String(requestId),
      rewardUpdateStage: stage,
    };
  }

  function eventsWithSourceEntity(mapped, sourceEntity) {
    const events = Array.isArray(mapped) ? mapped : [mapped];
    return events
      .filter(Boolean)
      .map(event => ({
        ...event,
        sourceEntity: event.sourceEntity || sourceEntity || "",
      }));
  }

  async function fetchBorrowReplayBalances(chainKeys, address, bounds) {
    if (!canUseCurrentBalanceReplay(bounds)) {
      return { balances: new Map(), warnings: [] };
    }
    const warnings = [];
    const balances = new Map();
    await mapLimit(chainKeys, HISTORY_CHAIN_CONCURRENCY, async chainKey => {
      try {
        const chainBalances = await fetchCurrentMarginBalances(chainKey, address, HISTORY_GRAPH_OPTIONS);
        chainBalances.forEach((value, key) => balances.set(key, value));
      } catch (error) {
        warnings.push(`${CHAINS[chainKey].name} current borrow balances unavailable; borrow/repay labels use in-range activity only.`);
      }
    });
    return { balances, warnings, currentBalanceReplay: balances.size > 0 };
  }

  function canUseCurrentBalanceReplay(bounds) {
    const now = Math.floor(Date.now() / 1000);
    return Number(bounds?.end || 0) >= now - 3600;
  }

  async function fetchCurrentMarginBalances(chainKey, address, graphOptions = {}) {
    const user = address.toLowerCase();
    const fields = `id accountNumber tokenValues { id token { id symbol decimals marketId } valuePar }`;
    const [result, interestIndexes] = await Promise.all([
      paginateEntity(CHAINS[chainKey].subgraph, "marginAccounts", `effectiveUser: "${user}"`, fields, "id", graphOptions),
      fetchCurrentInterestIndexes(chainKey, graphOptions),
    ]);
    const balances = new Map();
    result.rows.forEach(account => {
      const accountNumberValue = accountNumber(account);
      (account.tokenValues || []).forEach(tokenValue => {
        const token = tokenValue.token || {};
        const key = balanceKey(chainKey, accountNumberValue, token.id || token.symbol || tokenValue.id || "");
        balances.set(key, parBalanceToTokenBalance(tokenValue.valuePar, interestIndexes.get(normalizeAddress(token.id))));
      });
    });
    return balances;
  }

  async function fetchCurrentInterestIndexes(chainKey, graphOptions = {}) {
    if (interestIndexCache.has(chainKey)) return interestIndexCache.get(chainKey);
    const query = `{
      interestIndexes(first: 200) {
        token { id symbol marketId }
        borrowIndex
        supplyIndex
      }
    }`;
    const data = await graphQuery(CHAINS[chainKey].subgraph, query, graphOptions);
    const indexes = new Map();
    (data.interestIndexes || []).forEach(row => {
      const tokenId = normalizeAddress(row.token?.id || "");
      if (!tokenId) return;
      indexes.set(tokenId, {
        borrowIndex: row.borrowIndex || "1",
        supplyIndex: row.supplyIndex || "1",
      });
    });
    interestIndexCache.set(chainKey, indexes);
    return indexes;
  }

  function parBalanceToTokenBalance(valuePar, indexes) {
    const par = decimalToScaledBigInt(valuePar);
    if (par === 0n) return 0n;
    const index = par < 0n ? indexes?.borrowIndex : indexes?.supplyIndex;
    return multiplyScaledDecimal(par, index || "1");
  }

  async function paginateEntity(endpoint, entity, where, fields, orderBy = "serialId", graphOptions = {}) {
    const rows = [];
    let truncated = false;
    let lastChunkWasFull = false;
    for (let page = 0; page < MAX_PAGES; page++) {
      const skip = page * PAGE_SIZE;
      const query = `{
        ${entity}(first: ${PAGE_SIZE}, skip: ${skip}, orderBy: ${orderBy}, orderDirection: desc, where: { ${where} }) {
          ${fields}
        }
      }`;
      const data = await graphQuery(endpoint, query, graphOptions);
      const chunk = Array.isArray(data[entity]) ? data[entity] : [];
      rows.push(...chunk);
      lastChunkWasFull = chunk.length === PAGE_SIZE;
      if (chunk.length < PAGE_SIZE) break;
    }
    if (lastChunkWasFull) {
      const probeQuery = `{
        ${entity}(first: 1, skip: ${MAX_PAGES * PAGE_SIZE}, orderBy: ${orderBy}, orderDirection: desc, where: { ${where} }) {
          id
        }
      }`;
      const probeData = await graphQuery(endpoint, probeQuery, graphOptions);
      truncated = Array.isArray(probeData[entity]) && probeData[entity].length > 0;
    }
    return { rows, truncated };
  }

  async function graphQuery(endpoint, query, options = {}) {
    let lastError = null;
    const timeoutMs = Math.max(1, Number(options.timeoutMs || DEFAULT_GRAPH_TIMEOUT_MS));
    const attempts = Math.max(1, Number(options.attempts || DEFAULT_GRAPH_ATTEMPTS));
    for (let attempt = 0; attempt < attempts; attempt++) {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), timeoutMs);
      try {
        const response = await fetch(endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          signal: ctrl.signal,
          body: JSON.stringify({ query }),
        });
        clearTimeout(timer);
        if (!response.ok) throw new Error(`Subgraph HTTP ${response.status}`);
        const json = await response.json();
        if (json.errors && json.errors.length) throw new Error(json.errors[0].message || "GraphQL error");
        return json.data || {};
      } catch (error) {
        clearTimeout(timer);
        lastError = error && error.name === "AbortError" ? new Error("Subgraph timeout") : error;
      }
    }
    throw lastError || new Error("Subgraph query failed");
  }

  function eventBase(chainKey, row, action, role, txProp = "transaction") {
    const tx = row[txProp] || {};
    return {
      chainKey,
      txHash: String(tx.id || "").toLowerCase(),
      timestamp: Number(tx.timestamp || 0),
      blockNumber: String(tx.blockNumber || ""),
      action,
      role,
      serialId: String(row.serialId || row.id || row.key || ""),
      usd: 0,
    };
  }

  function taxFields(taxCategory, reviewFlag, legs, taxNote, reviewReason = "") {
    return {
      taxCategory,
      reviewFlag,
      legs: (legs || []).filter(leg => leg && leg.symbol && leg.amount !== ""),
      taxNote: taxNote || "",
      reviewReason: reviewReason || "",
    };
  }

  function assetLeg(direction, token, amount, usd) {
    const symbol = typeof token === "string" ? token : (token?.symbol || "");
    const tokenAddress = typeof token === "string" ? "" : (token?.id || "");
    return {
      direction,
      symbol,
      tokenAddress,
      rawAmount: String(amount ?? "").trim(),
      amount: cleanAmount(amount),
      usd: decimalForCsv(usd),
    };
  }

  function eventFromDeposit(chainKey, row) {
    const token = row.token || {};
    const amount = cleanAmount(row.amountDeltaWei);
    const usd = decimalToNumber(row.amountUSDDeltaWei);
    return {
      ...eventBase(chainKey, row, "deposit", "user"),
      ...taxFields("protocol_deposit", "not_taxable_by_default", [
        assetLeg("out", token, row.amountDeltaWei, usd),
      ], "Movement into Dolomite account; jurisdiction-specific treatment may vary."),
      account: accountNumber(row.marginAccount),
      label: `+${amount} ${token.symbol || "Token"}`,
      asset: token.symbol || "",
      usd,
    };
  }

  function eventFromWithdrawal(chainKey, row) {
    const token = row.token || {};
    const amount = cleanAmount(row.amountDeltaWei);
    const usd = decimalToNumber(row.amountUSDDeltaWei);
    return {
      ...eventBase(chainKey, row, "withdraw", "user"),
      ...taxFields("protocol_withdrawal", "not_taxable_by_default", [
        assetLeg("in", token, row.amountDeltaWei, usd),
      ], "Movement out of Dolomite account; jurisdiction-specific treatment may vary."),
      account: accountNumber(row.marginAccount),
      label: `-${amount} ${token.symbol || "Token"}`,
      asset: token.symbol || "",
      usd,
    };
  }

  function eventFromTransfer(chainKey, row, direction) {
    const token = row.token || {};
    const amount = cleanAmount(row.amountDeltaWei);
    const sign = direction === "in" ? "+" : "-";
    const counterparty = direction === "in" ? row.fromEffectiveUser?.id : row.toEffectiveUser?.id;
    const usd = decimalToNumber(row.amountUSDDeltaWei);
    return {
      ...eventBase(chainKey, row, "transfer", direction),
      ...taxFields("transfer", "not_taxable_by_default", [
        assetLeg(direction, token, row.amountDeltaWei, usd),
      ], counterparty ? `Counterparty ${counterparty}` : "Wallet-to-wallet or account transfer."),
      account: accountNumber(direction === "in" ? row.toMarginAccount : row.fromMarginAccount),
      fromAccount: accountNumber(row.fromMarginAccount),
      toAccount: accountNumber(row.toMarginAccount),
      isSelfTransfer: !!row.isSelfTransfer || normalizeAddress(row.fromEffectiveUser?.id) === normalizeAddress(row.toEffectiveUser?.id),
      isTransferForMarginPosition: !!row.isTransferForMarginPosition,
      counterparty: counterparty || "",
      label: `${sign}${amount} ${token.symbol || "Token"}`,
      asset: token.symbol || "",
      usd,
    };
  }

  function eventFromTrade(chainKey, row, side) {
    const takerToken = row.takerToken || {};
    const makerToken = row.makerToken || {};
    const takerAmount = cleanAmount(row.takerTokenDeltaWei);
    const makerAmount = cleanAmount(row.makerTokenDeltaWei);
    const takerLabel = `${takerAmount} ${takerToken.symbol || "Token"}`;
    const makerLabel = `${makerAmount} ${makerToken.symbol || "Token"}`;
    const takerUsd = decimalToNumber(row.takerAmountUSD || row.amountUSD);
    const makerUsd = decimalToNumber(row.makerAmountUSD || row.amountUSD);
    const legs = side === "taker"
      ? [assetLeg("out", takerToken, row.takerTokenDeltaWei, takerUsd), assetLeg("in", makerToken, row.makerTokenDeltaWei, makerUsd)]
      : [assetLeg("in", takerToken, row.takerTokenDeltaWei, takerUsd), assetLeg("out", makerToken, row.makerTokenDeltaWei, makerUsd)];
    return {
      ...eventBase(chainKey, row, "trade", side),
      ...taxFields("swap", "possible_taxable_disposal", legs, "Dolomite Trade-tab swap candidate; taxability depends on jurisdiction."),
      account: accountNumber(side === "taker" ? row.takerMarginAccount : row.makerMarginAccount),
      label: side === "taker" ? `-${takerLabel} / +${makerLabel}` : `+${takerLabel} / -${makerLabel}`,
      asset: `${takerToken.symbol || "Token"} / ${makerToken.symbol || "Token"}`,
      usd: decimalToNumber(row.amountUSD || row.takerAmountUSD || row.makerAmountUSD),
    };
  }

  function eventFromLiquidation(chainKey, row, side) {
    const held = row.heldToken || {};
    const borrowed = row.borrowedToken || {};
    const heldLabel = `${cleanAmount(row.heldTokenAmountDeltaWei)} ${held.symbol || "Held"}`;
    const borrowedLabel = `${cleanAmount(row.borrowedTokenAmountDeltaWei)} ${borrowed.symbol || "Borrowed"}`;
    const heldUsd = decimalToNumber(row.heldTokenAmountUSD);
    const borrowedUsd = decimalToNumber(row.borrowedTokenAmountUSD);
    const legs = side === "liquid"
      ? [assetLeg("out", held, row.heldTokenAmountDeltaWei, heldUsd), assetLeg("in", borrowed, row.borrowedTokenAmountDeltaWei, borrowedUsd)]
      : [assetLeg("in", held, row.heldTokenAmountDeltaWei, heldUsd), assetLeg("out", borrowed, row.borrowedTokenAmountDeltaWei, borrowedUsd)];
    return {
      ...eventBase(chainKey, row, "liquidation", side),
      ...taxFields("liquidation", "needs_review", legs, "Forced settlement/liquidation; source row is evidence of a balance-changing settlement, not a final tax outcome.", "liquidation_forced_settlement"),
      account: accountNumber(side === "liquid" ? row.liquidMarginAccount : row.solidMarginAccount),
      label: side === "liquid" ? `-${heldLabel} / +${borrowedLabel}` : `+${heldLabel} / -${borrowedLabel}`,
      asset: `${held.symbol || "Held"} / ${borrowed.symbol || "Borrowed"}`,
      usd: heldUsd + borrowedUsd,
    };
  }

  function eventFromVaporization(chainKey, row, side) {
    const held = row.heldToken || {};
    const borrowed = row.borrowedToken || {};
    const usd = decimalToNumber(row.amountUSDVaporized);
    return {
      ...eventBase(chainKey, row, "vaporization", side),
      ...taxFields("vaporization", "needs_review", [
        assetLeg(side === "vapor" ? "out" : "in", borrowed, row.borrowedTokenAmountDeltaWei, usd),
      ], "Debt settlement event; source row is evidence of Dolomite debt-clearing mechanics, not a final tax outcome.", "vaporization_debt_absorption"),
      account: accountNumber(row.solidMarginAccount),
      label: `Debt settlement: ${cleanAmount(row.borrowedTokenAmountDeltaWei)} ${borrowed.symbol || "Borrowed"}`,
      asset: `${held.symbol || "Held"} / ${borrowed.symbol || "Borrowed"}`,
      usd,
    };
  }

  function eventFromZap(chainKey, row) {
    const path = Array.isArray(row.tokenPath) ? row.tokenPath : [];
    const inSymbol = path[0]?.symbol || "Input";
    const outSymbol = path[path.length - 1]?.symbol || "Output";
    const input = cleanAmount(row.amountInToken);
    const output = cleanAmount(row.amountOutToken);
    const inUsd = decimalToNumber(row.amountInUSD);
    const outUsd = decimalToNumber(row.amountOutUSD);
    return {
      ...eventBase(chainKey, row, "zap", "user"),
      ...taxFields("zap", "possible_taxable_disposal", [
        assetLeg("out", inSymbol, row.amountInToken, inUsd),
        assetLeg("in", outSymbol, row.amountOutToken, outUsd),
      ], "Routed zap candidate; route token labels are reconciled with the paired trade event when available.", "zap_route_review"),
      account: accountNumber(row.marginAccount),
      label: `Zap: -${input} ${inSymbol} / +${output} ${outSymbol}`,
      asset: `${inSymbol} / ${outSymbol}`,
      routeEvidence: "token_path_unverified",
      routeMatchConfidence: "none",
      usd: Math.max(inUsd, outUsd),
    };
  }

  function eventFromBorrowPositionLifecycle(chainKey, row, stage) {
    const isOpen = stage === "open";
    const semanticAction = isOpen ? "openBorrow" : "closeBorrow";
    const txProp = isOpen ? "openTransaction" : "closeTransaction";
    const label = isOpen ? "Borrow position opened" : "Borrow position closed";
    return {
      ...eventBase(chainKey, row, isOpen ? "borrowPositionOpen" : "borrowPositionClose", "user", txProp),
      ...taxFields("borrow_position_lifecycle", "not_taxable_by_default", [], `${label} on Dolomite; token movements are shown by the paired account transfer rows.`),
      account: accountNumber(row.marginAccount),
      label,
      asset: "Borrow position",
      borrowPositionId: row.id || "",
      borrowPositionStatus: row.status || "",
      borrowSemanticAction: semanticAction,
      borrowSemanticLabel: ACTION_LABELS[semanticAction],
      borrowSemanticConfidence: "borrow_position_lifecycle",
    };
  }

  function eventFromAsync(chainKey, row, action, stage, txProp) {
    const input = row.inputToken?.symbol || "Input";
    const output = row.outputToken?.symbol || "Output";
    const inputAmount = cleanAmount(row.inputAmount);
    const outputAmount = cleanAmount(row.outputAmount || row.minOutputAmount);
    const stageLabel = stage === "executed" ? "Executed" : "Created";
    const legs = stage === "executed"
      ? [assetLeg("in", output, row.outputAmount || row.minOutputAmount, 0)]
      : [assetLeg("out", input, row.inputAmount, 0)];
    return {
      ...eventBase(chainKey, row, action, stage, txProp),
      ...taxFields("defi_position_change", "needs_review", legs, "Async Dolomite position change; created/executed stages may settle in separate transactions.", "async_position_timing"),
      account: accountNumber(row.marginAccount),
      label: `${stageLabel}: ${inputAmount} ${input} -> ${outputAmount} ${output}`,
      asset: `${input} / ${output}`,
      usd: 0,
    };
  }

  function eventFromAmmTrade(chainKey, row, role) {
    const token0 = row.pair?.token0?.symbol || "Token0";
    const token1 = row.pair?.token1?.symbol || "Token1";
    const legs = [
      positiveAmount(row.amount0In) ? assetLeg("out", token0, row.amount0In, 0) : null,
      positiveAmount(row.amount1In) ? assetLeg("out", token1, row.amount1In, 0) : null,
      positiveAmount(row.amount0Out) ? assetLeg("in", token0, row.amount0Out, 0) : null,
      positiveAmount(row.amount1Out) ? assetLeg("in", token1, row.amount1Out, 0) : null,
    ].filter(Boolean);
    const paid = [
      positiveAmount(row.amount0In) ? `${cleanAmount(row.amount0In)} ${token0}` : "",
      positiveAmount(row.amount1In) ? `${cleanAmount(row.amount1In)} ${token1}` : "",
    ].filter(Boolean).join(" + ") || "input";
    const received = [
      positiveAmount(row.amount0Out) ? `${cleanAmount(row.amount0Out)} ${token0}` : "",
      positiveAmount(row.amount1Out) ? `${cleanAmount(row.amount1Out)} ${token1}` : "",
    ].filter(Boolean).join(" + ") || "output";
    return {
      ...eventBase(chainKey, row, "amm", role),
      ...taxFields("swap", "possible_taxable_disposal", legs, "AMM trade candidate; taxability depends on jurisdiction.", "amm_swap_review"),
      label: `AMM trade: -${paid} / +${received}`,
      asset: `${token0} / ${token1}`,
      usd: decimalToNumber(row.amountUSD),
    };
  }

  function eventFromAmmLiquidity(chainKey, row, kind, role) {
    const token0 = row.pair?.token0?.symbol || "Token0";
    const token1 = row.pair?.token1?.symbol || "Token1";
    const sign = kind === "mint" ? "+" : "-";
    const direction = kind === "mint" ? "out" : "in";
    return {
      ...eventBase(chainKey, row, "amm", role),
      ...taxFields(kind === "mint" ? "liquidity_deposit" : "liquidity_withdrawal", "needs_review", [
        assetLeg(direction, token0, row.amount0, 0),
        assetLeg(direction, token1, row.amount1, 0),
      ], "AMM liquidity add/remove event; report records asset movement, not LP cost-basis treatment.", "amm_liquidity_review"),
      label: `${kind === "mint" ? "Add liquidity" : "Remove liquidity"}: ${sign}${cleanAmount(row.amount0)} ${token0} / ${sign}${cleanAmount(row.amount1)} ${token1}`,
      asset: `${token0} / ${token1}`,
      usd: decimalToNumber(row.amountUSD),
    };
  }

  function eventFromVesting(chainKey, row, direction) {
    const position = row.vestingPosition || {};
    const positionId = position.positionId ? ` #${position.positionId}` : "";
    const flowLabel = vestingEventFlowLabel(row, direction);
    const kind = vestingPositionKind(position);
    const oTokenAmount = cleanAmount(position.oTokenAmount || "");
    const paymentAmount = cleanAmount(position.paymentAmountWei || "");
    const amountLabel = vestingPositionAmountLabel(position, flowLabel);
    return {
      ...eventBase(chainKey, row, "vesting", direction),
      ...taxFields(vestingTaxCategory(flowLabel), "needs_review", vestingPositionLegs(position, flowLabel, direction), "Dolomite oDOLO/veDOLO flow. Pair rows combine oDOLO with DOLO. Claim rows show the USDC exercise payment used to receive a veDOLO lock. Review receipts for final tax treatment.", vestingReviewReason(flowLabel)),
      label: `${vestingActionLabel(flowLabel)}${positionId}${amountLabel ? ` (${amountLabel})` : ""}`,
      asset: vestingPositionAssetLabel(position, flowLabel),
      vestingPositionId: position.positionId || "",
      vestingKind: kind,
      vestingStatus: position.status || "",
      vestingFlowLabel: flowLabel,
      vestingOTokenAmount: oTokenAmount,
      vestingPaymentAmount: paymentAmount,
      vestingPairTaxesPaid: position.pairTaxesPaid || "",
      usd: 0,
    };
  }

  function vestingEventFlowLabel(row, direction) {
    const hasFrom = !!row?.fromEffectiveUser?.id;
    const hasTo = !!row?.toEffectiveUser?.id;
    if (sameEffectiveVestingUser(row)) return "Moved internally";
    if (!hasFrom && hasTo) return "Opened";
    if (hasFrom && !hasTo) return "Claimed veDOLO";
    return direction === "in" ? "Received" : "Transferred out";
  }

  function sameEffectiveVestingUser(row) {
    const from = normalizeAddress(row?.fromEffectiveUser?.id);
    const to = normalizeAddress(row?.toEffectiveUser?.id);
    return !!from && !!to && from === to;
  }

  function vestingActionLabel(flowLabel) {
    if (vestingFlowIsOpen(flowLabel)) return "Pair oDOLO + DOLO";
    if (vestingFlowIsExercise(flowLabel)) return "Claim veDOLO";
    if (vestingFlowIsInternal(flowLabel)) return "Move veDOLO position";
    return `${flowLabel || "Move"} vesting position`;
  }

  function vestingPositionLegs(position, flowLabel, direction) {
    const legs = [];
    const isOpen = vestingFlowIsOpen(flowLabel);
    const isExercise = vestingFlowIsExercise(flowLabel);
    if (isOpen && positiveAmount(position?.oTokenAmount)) {
      legs.push(assetLeg("out", "oDOLO", position.oTokenAmount, 0));
      legs.push(assetLeg("out", "DOLO", position.oTokenAmount, 0));
    } else if (isExercise && positiveAmount(position?.oTokenAmount)) {
      legs.push(assetLeg("out", "oDOLO/DOLO vesting pair", position.oTokenAmount, 0));
      legs.push(assetLeg("in", "veDOLO lock", position.oTokenAmount, 0));
    } else if (positiveAmount(position?.oTokenAmount)) {
      legs.push(assetLeg(direction, "oDOLO/DOLO vesting pair", position.oTokenAmount, 0));
    }
    if (isExercise && positiveAmount(position?.paymentAmountWei)) {
      legs.push(assetLeg("out", "USDC exercise cost", position.paymentAmountWei, 0));
    } else if (!legs.length && positiveAmount(position?.paymentAmountWei)) {
      legs.push(assetLeg(direction, "USDC exercise cost", position.paymentAmountWei, 0));
    }
    if (!legs.length) {
      legs.push(assetLeg(direction, "Dolomite vesting claim", position?.oTokenAmount || position?.paymentAmountWei || "", 0));
    }
    return legs;
  }

  function vestingPositionKind(position) {
    if (positiveAmount(position?.oTokenAmount)) return "oDOLO/DOLO vesting pair";
    if (positiveAmount(position?.paymentAmountWei)) return "USDC exercise cost";
    return "Dolomite vesting claim";
  }

  function vestingTaxCategory(flowLabel) {
    if (vestingFlowIsOpen(flowLabel)) return "odolo_dolo_pair";
    if (vestingFlowIsExercise(flowLabel)) return "vedolo_claim";
    if (vestingFlowIsInternal(flowLabel)) return "vesting_internal_move";
    return "vesting_transfer";
  }

  function vestingPositionAssetLabel(position, flowLabel) {
    if (vestingFlowIsOpen(flowLabel) && positiveAmount(position?.oTokenAmount)) return "oDOLO + DOLO -> paired vesting position";
    if (vestingFlowIsExercise(flowLabel) && positiveAmount(position?.oTokenAmount)) {
      return positiveAmount(position?.paymentAmountWei)
        ? "USDC payment + paired oDOLO/DOLO -> veDOLO lock"
        : "oDOLO/DOLO pair -> veDOLO lock";
    }
    if (vestingFlowIsInternal(flowLabel) && positiveAmount(position?.oTokenAmount)) return "oDOLO/DOLO vesting position moved internally";
    const assets = [positiveAmount(position?.oTokenAmount) ? "oDOLO/DOLO vesting pair" : ""].filter(Boolean);
    return assets.length ? assets.join(" / ") : vestingPositionKind(position);
  }

  function vestingPositionAmountLabel(position, flowLabel) {
    const oTokenAmount = positiveAmount(position?.oTokenAmount) ? cleanAmount(position.oTokenAmount) : "";
    if (vestingFlowIsOpen(flowLabel)) {
      return oTokenAmount ? `paired ${oTokenAmount} oDOLO + ${oTokenAmount} DOLO` : "";
    }
    if (vestingFlowIsExercise(flowLabel)) {
      return [
        positiveAmount(position?.paymentAmountWei) ? `paid ${cleanAmount(position.paymentAmountWei)} USDC` : "",
        oTokenAmount ? `used ${oTokenAmount} paired oDOLO/DOLO` : "",
        oTokenAmount ? "received veDOLO lock" : "",
      ].filter(Boolean).join("; ");
    }
    return [
      oTokenAmount ? `vesting pair ${oTokenAmount} oDOLO/DOLO` : "",
    ].filter(Boolean).join("; ");
  }

  function vestingFlowIsOpen(flowLabel) {
    return String(flowLabel || "").toLowerCase() === "opened";
  }

  function vestingFlowIsExercise(flowLabel) {
    const text = String(flowLabel || "").toLowerCase();
    return text.includes("exercised") || text.includes("claimed vedolo");
  }

  function vestingFlowIsInternal(flowLabel) {
    return String(flowLabel || "").toLowerCase().includes("moved internally");
  }

  function vestingReviewReason(flowLabel) {
    if (vestingFlowIsOpen(flowLabel)) return "odolo_vedolo_pairing_review";
    if (vestingFlowIsExercise(flowLabel)) return "odolo_vedolo_exercise_review";
    if (vestingFlowIsInternal(flowLabel)) return "odolo_vedolo_internal_move_review";
    return "odolo_vedolo_transfer_review";
  }

  function groupEvents(events, replayContext = {}) {
    const byTx = new Map();
    events.forEach(event => {
      const key = `${event.chainKey}:${event.txHash}`;
      if (!byTx.has(key)) {
        byTx.set(key, {
          key,
          chainKey: event.chainKey,
          txHash: event.txHash,
          timestamp: event.timestamp,
          blockNumber: event.blockNumber,
          events: [],
          actions: new Set(),
          usdVolume: 0,
          gas: { status: "pending" },
        });
      }
      const row = byTx.get(key);
      row.events.push(event);
      row.actions.add(event.action);
      row.timestamp = Math.max(row.timestamp, event.timestamp || 0);
    });
    const rows = Array.from(byTx.values()).map(row => {
      reconcileZapEvents(row);
      return {
        ...row,
        usdVolume: transactionUsdValue(row),
      };
    }).sort((a, b) => b.timestamp - a.timestamp);
    enrichBorrowSemanticsForRows(rows, replayContext);
    return rows;
  }

  function enrichBorrowSemanticsForRows(rows, replayContext = {}) {
    if (!Array.isArray(rows) || !rows.length) return rows;
    const currentBalances = replayContext.currentBalances instanceof Map ? replayContext.currentBalances : new Map();
    if (replayContext.currentBalanceReplay && currentBalances.size) {
      annotateBorrowSemanticsFromCurrentBalances(rows, currentBalances);
    } else {
      annotateBorrowSemanticsFromRangeStart(rows);
    }
    applyBorrowPositionLifecycleSemantics(rows);
    rows.forEach(row => {
      row.semanticActions = new Set((row.events || []).map(event => event.borrowSemanticAction).filter(Boolean));
    });
    return rows;
  }

  function applyBorrowPositionLifecycleSemantics(rows) {
    (rows || []).forEach(row => {
      const events = Array.isArray(row?.events) ? row.events : [];
      const openAccounts = new Set(events
        .filter(event => event?.action === "borrowPositionOpen")
        .map(event => normalizeAccountNumberValue(event.account))
        .filter(Boolean));
      const closeAccounts = new Set(events
        .filter(event => event?.action === "borrowPositionClose")
        .map(event => normalizeAccountNumberValue(event.account))
        .filter(Boolean));
      if (!openAccounts.size && !closeAccounts.size) return;
      events.forEach(event => {
        if (event?.action === "borrowPositionOpen") {
          setBorrowSemantic(event, "openBorrow", "borrow_position_lifecycle");
          return;
        }
        if (event?.action === "borrowPositionClose") {
          setBorrowSemantic(event, "closeBorrow", "borrow_position_lifecycle");
          return;
        }
        const account = normalizeAccountNumberValue(event?.account);
        if (
          event?.action === "deposit"
          && openAccounts.has(account)
          && isBorrowRouteAccountNumber(account)
        ) {
          setBorrowSemantic(event, "openBorrow", "borrow_position_lifecycle");
          return;
        }
        if (
          event?.action === "withdraw"
          && closeAccounts.has(account)
          && isBorrowRouteAccountNumber(account)
        ) {
          setBorrowSemantic(event, "closeBorrow", "borrow_position_lifecycle");
          return;
        }
        if (event?.action !== "transfer" || !event.isSelfTransfer) return;
        const fromAccount = normalizeAccountNumberValue(event.fromAccount);
        const toAccount = normalizeAccountNumberValue(event.toAccount);
        if (openAccounts.has(toAccount) && isBorrowRouteAccountNumber(toAccount)) {
          setBorrowSemantic(event, "openBorrow", "borrow_position_lifecycle");
        } else if (
          (closeAccounts.has(fromAccount) && isBorrowRouteAccountNumber(fromAccount))
          || (closeAccounts.has(toAccount) && isBorrowRouteAccountNumber(toAccount))
        ) {
          setBorrowSemantic(event, "closeBorrow", "borrow_position_lifecycle");
        }
      });
    });
  }

  function setBorrowSemantic(event, action, confidence) {
    event.borrowSemanticAction = action;
    event.borrowSemanticLabel = ACTION_LABELS[action] || action;
    event.borrowSemanticConfidence = confidence || event.borrowSemanticConfidence || "semantic";
  }

  function annotateBorrowSemanticsFromCurrentBalances(rows, currentBalances) {
    const running = new Map(currentBalances);
    rows.slice().sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0)).forEach(row => {
      const deltas = aggregateBalanceDeltas(balanceDeltasForRow(row));
      annotateBorrowEventsForRow(row, deltas, running, "current_balance_replay");
      deltas.forEach((delta, key) => running.set(key, (running.get(key) || 0n) - delta));
    });
  }

  function annotateBorrowSemanticsFromRangeStart(rows) {
    const running = new Map();
    const observed = new Set();
    rows.slice().sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0)).forEach(row => {
      const deltas = aggregateBalanceDeltas(balanceDeltasForRow(row));
      annotateBorrowEventsForRow(row, deltas, running, "range_replay", observed);
      deltas.forEach((delta, key) => {
        running.set(key, (running.get(key) || 0n) + delta);
        observed.add(key);
      });
    });
  }

  function annotateBorrowEventsForRow(row, rowDeltas, balances, confidence, observedKeys = null) {
    if (rowHasSwapRouteWithoutBorrowLifecycle(row)) return;
    (row.events || []).forEach(event => {
      const eventDeltas = borrowClassifiableEventDeltas(event);
      if (!eventDeltas.length) return;
      const semanticMatches = eventDeltas.map(eventDelta => {
        const rowDelta = rowDeltas.get(eventDelta.key) || 0n;
        if (rowDelta === 0n) return null;
        const hasBaseline = !observedKeys || observedKeys.has(eventDelta.key) || balances.has(eventDelta.key);
        const before = confidence === "current_balance_replay"
          ? (balances.get(eventDelta.key) || 0n) - rowDelta
          : (balances.get(eventDelta.key) || 0n);
        const after = before + rowDelta;
        const collateralContext = accountHasDebtExposure(eventDelta.key, rowDeltas, balances, confidence);
        const transitionSemantic = borrowSemanticForBalanceTransition(event, before, after, hasBaseline, eventDelta.delta, collateralContext);
        const routeSemantic = borrowRouteTransferSemantic(event, eventDelta);
        const semantic = transitionSemantic?.action === "addCollateral" ? transitionSemantic : transitionSemantic || routeSemantic;
        return semantic ? {
          semantic,
          before,
          after,
          eventDelta,
          isRouteAccount: isBorrowRouteAccountNumber(eventDelta.account),
          hasKnownBalance: balances.has(eventDelta.key),
        } : null;
      }).filter(Boolean);
      const routeMatches = semanticMatches.filter(match => match.isRouteAccount);
      const knownBalanceMatches = semanticMatches.filter(match => match.hasKnownBalance);
      const candidates = routeMatches.length ? routeMatches : knownBalanceMatches.length ? knownBalanceMatches : semanticMatches;
      const match = candidates.sort((a, b) => borrowSemanticPriority(b.semantic.action) - borrowSemanticPriority(a.semantic.action))[0];
      if (!match) return;
      event.borrowSemanticAction = match.semantic.action;
      event.borrowSemanticLabel = match.semantic.label;
      event.borrowSemanticConfidence = confidence;
      event.borrowBalanceBefore = scaledBigIntToDecimal(match.before);
      event.borrowBalanceAfter = scaledBigIntToDecimal(match.after);
    });
  }

  function rowHasSwapRouteWithoutBorrowLifecycle(row) {
    const events = Array.isArray(row?.events) ? row.events : [];
    const hasSwapRoute = events.some(event => event?.action === "zap" || event?.action === "trade" || event?.taxCategory === "swap");
    if (!hasSwapRoute) return false;
    return !events.some(event => BORROW_POSITION_LIFECYCLE_ACTIONS.has(event?.action));
  }

  function classificationSourceForRow(row) {
    const events = Array.isArray(row?.events) ? row.events : [];
    const borrowSource = events
      .map(event => borrowClassificationSourceForEvent(event))
      .find(Boolean);
    if (borrowSource) return borrowSource;
    if (rowHasSwapRouteWithoutBorrowLifecycle(row)) return "Swap route; no borrow lifecycle signal";
    const semanticSource = events
      .map(event => classificationSourceForEvent(row, event))
      .find(source => source && source !== "Dolomite subgraph");
    if (semanticSource) return semanticSource;
    return events.length ? "Dolomite subgraph" : "";
  }

  function classificationSourceForEvent(row, event) {
    const borrowSource = borrowClassificationSourceForEvent(event);
    if (borrowSource) return borrowSource;
    if (isSwapLikeEvent(event) && rowHasSwapRouteWithoutBorrowLifecycle(row)) return "Swap route; no borrow lifecycle signal";
    return event ? "Dolomite subgraph" : "";
  }

  function borrowClassificationSourceForEvent(event) {
    if (event?.borrowSemanticConfidence) return classificationConfidenceLabel(event.borrowSemanticConfidence);
    if (BORROW_POSITION_LIFECYCLE_ACTIONS.has(event?.action)) return CLASSIFICATION_SOURCE_LABELS.borrow_position_lifecycle;
    return "";
  }

  function classificationConfidenceLabel(confidence) {
    const key = String(confidence || "").trim();
    return CLASSIFICATION_SOURCE_LABELS[key] || key || "";
  }

  function balanceAccountPrefix(key) {
    const parts = String(key || "").split(":");
    if (parts.length < 3) return "";
    return `${parts[0]}:${parts[1]}:`;
  }

  function balanceBeforeAfterForKey(key, rowDeltas, balances, confidence) {
    const rowDelta = rowDeltas.get(key) || 0n;
    const before = confidence === "current_balance_replay"
      ? (balances.get(key) || 0n) - rowDelta
      : (balances.get(key) || 0n);
    return { before, after: before + rowDelta };
  }

  function accountHasDebtExposure(targetKey, rowDeltas, balances, confidence) {
    const prefix = balanceAccountPrefix(targetKey);
    if (!prefix) return false;
    const keys = new Set([...Array.from(balances.keys()), ...Array.from(rowDeltas.keys())]);
    for (const key of keys) {
      if (key === targetKey || !String(key).startsWith(prefix)) continue;
      const { before, after } = balanceBeforeAfterForKey(key, rowDeltas, balances, confidence);
      if (before < 0n || after < 0n) return true;
    }
    return false;
  }

  function borrowSemanticForBalanceTransition(event, before, after, hasBaseline, delta = null, collateralContext = false) {
    if (!hasBaseline && !collateralContext) return null;
    const change = typeof delta === "bigint"
      ? delta
      : event.action === "withdraw"
        ? -1n
        : event.action === "deposit"
          ? 1n
          : after - before;
    if (change < 0n) {
      if (after < 0n && before >= 0n) return { action: "openBorrow", label: ACTION_LABELS.openBorrow };
      if (before < 0n && after < before) return { action: "borrow", label: ACTION_LABELS.borrow };
      if (collateralContext && before > 0n && after < before) return { action: "withdrawCollateral", label: ACTION_LABELS.withdrawCollateral };
      return null;
    }
    if (change > 0n) {
      if (before < 0n && after >= 0n) return { action: "repay", label: ACTION_LABELS.repay };
      if (before < 0n && after > before) return { action: "repay", label: ACTION_LABELS.repay };
      if (collateralContext && after > before && after > 0n) return { action: "addCollateral", label: ACTION_LABELS.addCollateral };
      return null;
    }
    return null;
  }

  function borrowSemanticPriority(action) {
    if (action === "openBorrow" || action === "closeBorrow") return 3;
    if (action === "borrow" || action === "repay") return 2;
    return 1;
  }

  function normalizeAccountNumberValue(value) {
    return String(value ?? "").trim();
  }

  function hasAccountNumber(value) {
    return normalizeAccountNumberValue(value) !== "";
  }

  function isBorrowRouteAccountNumber(value) {
    const account = normalizeAccountNumberValue(value);
    return account !== "" && account !== "0" && account.length > 12;
  }

  function borrowRouteTransferSemantic(event, eventDelta) {
    if (event?.action !== "transfer" || !event.isSelfTransfer) return null;
    const fromAccount = normalizeAccountNumberValue(event.fromAccount);
    const toAccount = normalizeAccountNumberValue(event.toAccount);
    const deltaAccount = normalizeAccountNumberValue(eventDelta?.account);
    if (!fromAccount || !toAccount || fromAccount === toAccount || !deltaAccount) return null;
    if (isBorrowRouteAccountNumber(fromAccount) && toAccount === "0" && deltaAccount === fromAccount && eventDelta.delta < 0n) {
      return { action: "borrow", label: ACTION_LABELS.borrow };
    }
    return null;
  }

  function borrowClassifiableEventDeltas(event) {
    if (!event) return [];
    if (event.action === "deposit" || event.action === "withdraw") {
      const leg = firstLeg(event.legs, event.action === "deposit" ? "out" : "in") || (event.legs || [])[0];
      if (!leg || !hasAccountNumber(event.account)) return [];
      const amount = decimalToScaledBigInt(leg.rawAmount ?? leg.amount);
      if (amount <= 0n) return [];
      return [{
        key: balanceKey(event.chainKey, event.account, leg.tokenAddress || leg.symbol),
        account: event.account,
        delta: event.action === "deposit" ? amount : -amount,
      }];
    }
    if (event.action !== "transfer" || !event.isSelfTransfer || !hasAccountNumber(event.fromAccount) || !hasAccountNumber(event.toAccount)) return [];
    const leg = (event.legs || [])[0];
    if (!leg) return [];
    const amount = decimalToScaledBigInt(leg.rawAmount ?? leg.amount);
    if (amount <= 0n) return [];
    const token = leg.tokenAddress || leg.symbol;
    return [
      { key: balanceKey(event.chainKey, event.fromAccount, token), account: event.fromAccount, delta: -amount },
      { key: balanceKey(event.chainKey, event.toAccount, token), account: event.toAccount, delta: amount },
    ];
  }

  function borrowClassifiableEventDelta(event) {
    return borrowClassifiableEventDeltas(event)[0] || null;
  }

  function balanceDeltasForRow(row) {
    const events = Array.isArray(row?.events) ? row.events : [];
    const hasConcreteZapPath = events.some(event => ["deposit", "withdraw", "transfer", "trade"].includes(event?.action));
    return events.flatMap(event => {
      if (event?.action === "zap" && hasConcreteZapPath) return [];
      return balanceDeltasForEvent(event);
    });
  }

  function balanceDeltasForEvent(event) {
    if (!event) return [];
    const account = normalizeAccountNumberValue(event.account);
    if (event.action === "deposit" || event.action === "withdraw") {
      const classified = borrowClassifiableEventDelta(event);
      return classified ? [classified] : [];
    }
    if (event.action === "transfer") {
      const leg = (event.legs || [])[0];
      if (!leg) return [];
      const amount = decimalToScaledBigInt(leg.rawAmount ?? leg.amount);
      if (amount <= 0n) return [];
      const token = leg.tokenAddress || leg.symbol;
      if (event.isSelfTransfer && hasAccountNumber(event.fromAccount) && hasAccountNumber(event.toAccount)) {
        return [
          { key: balanceKey(event.chainKey, event.fromAccount, token), delta: -amount },
          { key: balanceKey(event.chainKey, event.toAccount, token), delta: amount },
        ];
      }
      const delta = event.role === "in" ? amount : -amount;
      return account ? [{ key: balanceKey(event.chainKey, account, token), delta }] : [];
    }
    if (!account) return [];
    if (["trade", "liquidation", "vaporization", "zap", "asyncDeposit", "asyncWithdrawal"].includes(event.action) || event.taxCategory === "swap") {
      return (event.legs || []).map(leg => {
        const amount = decimalToScaledBigInt(leg.rawAmount ?? leg.amount);
        if (amount <= 0n) return null;
        return {
          key: balanceKey(event.chainKey, account, leg.tokenAddress || leg.symbol),
          delta: leg.direction === "in" ? amount : -amount,
        };
      }).filter(Boolean);
    }
    return [];
  }

  function aggregateBalanceDeltas(deltas) {
    const out = new Map();
    (deltas || []).forEach(item => {
      if (!item?.key || !item.delta) return;
      out.set(item.key, (out.get(item.key) || 0n) + item.delta);
    });
    return out;
  }

  function balanceKey(chainKey, account, token) {
    return `${chainKey}:${normalizeAccountNumberValue(account) || "0"}:${String(token || "").toLowerCase()}`;
  }

  function reconcileZapEvents(row) {
    const events = Array.isArray(row?.events) ? row.events : [];
    events
      .filter(event => event?.action === "zap")
      .forEach(zap => reconcileZapEventWithTrades(zap, events));
  }

  function reconcileZapEventWithTrades(zap, events) {
    const trades = events.filter(event => event?.action === "trade");
    const match = matchedTradeRouteFlowForZap(zap, trades);
    if (!match) return;

    const originalAsset = zap.asset || "";
    const { outLeg, inLeg } = match;
    const before = cleanFlowFromLegs(zap.legs);
    zap.legs = [
      { ...outLeg },
      { ...inLeg },
    ];
    zap.asset = `${outLeg.symbol || "Input"} / ${inLeg.symbol || "Output"}`;
    zap.label = `Zap: -${cleanLegAmountSymbol(outLeg)} / +${cleanLegAmountSymbol(inLeg)}`;
    zap.usd = Math.max(
      Math.abs(Number(zap.usd || 0)),
      Math.abs(Number(match.usd || 0)),
      Math.abs(Number(outLeg.usd || 0)),
      Math.abs(Number(inLeg.usd || 0)),
    );
    const after = cleanFlowFromLegs(zap.legs);
    zap.routeEvidence = before === after ? "paired_trade_verified" : "paired_trade_reconciled";
    zap.routeMatchConfidence = match.confidence;
    zap.routeHopCount = match.hopCount;
    if (originalAsset && originalAsset !== zap.asset) zap.routeTokenPath = originalAsset;
    if (before !== after) {
      zap.taxNote = appendTaxNote(zap.taxNote, "Zap token labels were reconciled against the paired trade event in the same transaction.");
    }
  }

  function matchedTradeFlowForZap(zap, trade) {
    return matchedTradeRouteFlowForZap(zap, [trade]);
  }

  function matchedTradeRouteFlowForZap(zap, trades) {
    const zapOut = firstLeg(zap.legs, "out");
    const zapIn = firstLeg(zap.legs, "in");
    if (!zapOut || !zapIn) return null;
    const startCandidates = (trades || [])
      .map(trade => routeEndpointCandidate(zapOut, trade, "out"))
      .filter(Boolean);
    const endCandidates = (trades || [])
      .map(trade => routeEndpointCandidate(zapIn, trade, "in"))
      .filter(Boolean);
    return startCandidates.flatMap(start => (
      endCandidates.map(end => routeMatchCandidate(start, end))
    ))
      .filter(Boolean)
      .sort((a, b) => b.score - a.score)[0] || null;
  }

  function routeEndpointCandidate(zapLeg, trade, direction) {
    const leg = firstLeg(trade?.legs, direction);
    if (!leg || !sameLegAmount(zapLeg, leg)) return null;
    const usdMatch = legUsdMatches(zapLeg, leg);
    const identityScore = legIdentityScore(zapLeg, leg);
    const identityMatch = identityScore > 0;
    const score = (usdMatch ? 10 : 0) + identityScore;
    if (score < 6) return null;
    return { trade, leg, usdMatch, identityMatch, score };
  }

  function routeMatchCandidate(start, end) {
    if (!start?.leg || !end?.leg) return null;
    const usdScore = [start.usdMatch, end.usdMatch].filter(Boolean).length;
    const identityScore = [start.identityMatch, end.identityMatch].filter(Boolean).length;
    const sameTrade = start.trade === end.trade;
    const score = start.score + end.score + (sameTrade ? 1 : 0);
    return {
      trade: start.trade || end.trade,
      outLeg: { ...start.leg },
      inLeg: { ...end.leg },
      score,
      confidence: usdScore === 2 ? "high" : identityScore === 2 ? "medium" : "low",
      hopCount: sameTrade ? 1 : 2,
      usd: Math.max(Math.abs(Number(start.trade?.usd || 0)), Math.abs(Number(end.trade?.usd || 0))),
    };
  }

  function firstLeg(legs, direction) {
    return (Array.isArray(legs) ? legs : []).find(leg => leg?.direction === direction) || null;
  }

  function sameLegAmount(a, b) {
    return canonicalDecimalString(a?.rawAmount ?? a?.amount) === canonicalDecimalString(b?.rawAmount ?? b?.amount);
  }

  function legUsdMatches(a, b) {
    const aUsd = Math.abs(decimalToNumber(a?.usd));
    const bUsd = Math.abs(decimalToNumber(b?.usd));
    if (!Number.isFinite(aUsd) || !Number.isFinite(bUsd) || (aUsd === 0 && bUsd === 0)) return false;
    const tolerance = Math.max(0.000001, Math.max(aUsd, bUsd) * 0.001);
    return Math.abs(aUsd - bUsd) <= tolerance;
  }

  function legIdentityMatches(a, b) {
    return legIdentityScore(a, b) > 0;
  }

  function legIdentityScore(a, b) {
    const aAddress = normalizeAddress(a?.tokenAddress || "");
    const bAddress = normalizeAddress(b?.tokenAddress || "");
    if (aAddress && bAddress) return aAddress === bAddress ? 6 : 0;
    const aSymbol = String(a?.symbol || "").trim().toUpperCase();
    const bSymbol = String(b?.symbol || "").trim().toUpperCase();
    return !!aSymbol && aSymbol === bSymbol ? 3 : 0;
  }

  function canonicalDecimalString(value) {
    const raw = String(value ?? "0").trim().replace(/,/g, "").replace(/^[+]/, "");
    const unsigned = raw.replace(/^-/, "");
    const [wholeRaw, fracRaw = ""] = unsigned.split(".");
    const whole = wholeRaw.replace(/^0+(?=\d)/, "") || "0";
    const frac = fracRaw.replace(/0+$/, "");
    const sign = raw.startsWith("-") && (whole !== "0" || frac) ? "-" : "";
    return `${sign}${whole}${frac ? `.${frac}` : ""}`;
  }

  function decimalToScaledBigInt(value, decimals = 18) {
    const raw = expandScientificDecimal(String(value ?? "0").trim().replace(/,/g, ""));
    if (!raw || raw === ".") return 0n;
    const negative = raw.startsWith("-");
    const unsigned = raw.replace(/^[+-]/, "");
    if (!/^\d*(?:\.\d*)?$/.test(unsigned)) return 0n;
    const [wholeRaw = "0", fracRaw = ""] = unsigned.split(".");
    const whole = BigInt(wholeRaw.replace(/^0+(?=\d)/, "") || "0");
    const scale = 10n ** BigInt(decimals);
    const fracPadded = (fracRaw + "0".repeat(decimals)).slice(0, decimals);
    const frac = BigInt(fracPadded || "0");
    const scaled = whole * scale + frac;
    return negative ? -scaled : scaled;
  }

  function expandScientificDecimal(value) {
    const raw = String(value || "0");
    if (!/[eE]/.test(raw)) return raw;
    const negative = raw.startsWith("-");
    const unsigned = raw.replace(/^[+-]/, "");
    const [mantissaRaw, exponentRaw = "0"] = unsigned.split(/[eE]/);
    const exponent = Number(exponentRaw);
    if (!Number.isInteger(exponent)) return raw;
    const [whole = "0", frac = ""] = mantissaRaw.split(".");
    const digits = `${whole}${frac}`.replace(/^0+(?=\d)/, "") || "0";
    const decimalIndex = whole.length + exponent;
    let expanded;
    if (decimalIndex <= 0) expanded = `0.${"0".repeat(Math.abs(decimalIndex))}${digits}`;
    else if (decimalIndex >= digits.length) expanded = `${digits}${"0".repeat(decimalIndex - digits.length)}`;
    else expanded = `${digits.slice(0, decimalIndex)}.${digits.slice(decimalIndex)}`;
    return `${negative ? "-" : ""}${expanded}`;
  }

  function scaledBigIntToDecimal(value, decimals = 18) {
    const negative = value < 0n;
    const abs = negative ? -value : value;
    const scale = 10n ** BigInt(decimals);
    const whole = abs / scale;
    let fraction = (abs % scale).toString().padStart(decimals, "0").replace(/0+$/, "");
    return `${negative ? "-" : ""}${whole.toString()}${fraction ? `.${fraction}` : ""}`;
  }

  function multiplyScaledDecimal(value, factor, decimals = 18) {
    const scale = 10n ** BigInt(decimals);
    return (value * decimalToScaledBigInt(factor, decimals)) / scale;
  }

  function appendTaxNote(note, addition) {
    const base = String(note || "").trim();
    const next = String(addition || "").trim();
    if (!next || base.includes(next)) return base;
    return base ? `${base} ${next}` : next;
  }

  function transactionUsdValue(row) {
    const primary = primaryTransactionEvent(row.events);
    const primaryUsd = Math.abs(Number(primary?.usd || 0));
    if (Number.isFinite(primaryUsd) && primaryUsd > 0) return primaryUsd;
    const values = (row.events || [])
      .map(event => Math.abs(Number(event?.usd || 0)))
      .filter(value => Number.isFinite(value) && value > 0);
    return values.length ? Math.max(...values) : 0;
  }

  function historyRowsForGasPriority(rows) {
    const classificationRows = [];
    const backgroundRows = [];
    const skippedRows = [];
    (rows || []).forEach(row => {
      if (row?.gas?.status && row.gas.status !== "pending") return;
      if (row?.receiptClassificationPending) {
        classificationRows.push(row);
      } else {
        backgroundRows.push(row);
      }
    });
    return { classificationRows, backgroundRows, skippedRows };
  }

  async function enrichGasForRows(rows, address, runId) {
    const renderEvery = rows.length > 500 ? 50 : rows.length > 120 ? 20 : 5;
    const priority = historyRowsForGasPriority(rows);
    const completeRow = async row => {
      if (runId !== state.runId) return;
      row.gas = await fetchGas(row, address);
      state.gasChecked += 1;
      if (state.gasChecked % renderEvery === 0 || state.gasChecked === state.gasTotal) {
        setStatus(`Checking gas receipts ${state.gasChecked}/${state.gasTotal}...`);
        render();
      }
    };
    await mapLimit(priority.classificationRows, HISTORY_CLASSIFICATION_RECEIPT_CONCURRENCY, completeRow);
    await mapLimit(priority.backgroundRows, HISTORY_BACKGROUND_GAS_CONCURRENCY, completeRow);
  }

  function gasCacheKey(row, address) {
    return `${row?.chainKey || ""}:${String(row?.txHash || "").toLowerCase()}:${normalizeAddress(address)}`;
  }

  function gasStorageKey(cacheKey) {
    return `${GAS_STORAGE_PREFIX}:${cacheKey}`;
  }

  function readStoredGasResult(cacheKey) {
    try {
      const raw = window.localStorage?.getItem(gasStorageKey(cacheKey));
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") return null;
      if (!parsed.ts || Date.now() - Number(parsed.ts) > GAS_STORAGE_TTL_MS) {
        window.localStorage?.removeItem(gasStorageKey(cacheKey));
        return null;
      }
      const result = parsed.result;
      return result && typeof result === "object" && result.status ? result : null;
    } catch (error) {
      console.debug("History gas cache read unavailable:", error);
      return null;
    }
  }

  function writeStoredGasResult(cacheKey, result) {
    if (!result || !["ok", "not-payer"].includes(result.status)) return false;
    try {
      window.localStorage?.setItem(gasStorageKey(cacheKey), JSON.stringify({
        ts: Date.now(),
        result,
      }));
      return true;
    } catch (error) {
      console.debug("History gas cache write unavailable:", error);
      return false;
    }
  }

  async function fetchGas(row, address) {
    const cacheKey = gasCacheKey(row, address);
    if (gasCache.has(cacheKey)) {
      const cached = await gasCache.get(cacheKey);
      applyBorrowLifecycleSemanticsToRow(row, cached?.borrowLifecycleSemantics || []);
      return cached;
    }
    const stored = readStoredGasResult(cacheKey);
    if (stored) {
      gasCache.set(cacheKey, stored);
      applyBorrowLifecycleSemanticsToRow(row, stored.borrowLifecycleSemantics || []);
      return stored;
    }
    const chain = CHAINS[row.chainKey];
    const request = (async () => {
      try {
        const [receipt, tx] = await Promise.all([
          rpcRequest(row.chainKey, "eth_getTransactionReceipt", [row.txHash]),
          rpcRequest(row.chainKey, "eth_getTransactionByHash", [row.txHash]),
        ]);
        if (!receipt || !tx) return { status: "missing", paidByWallet: false };
        const borrowLifecycleSemantics = borrowLifecycleSemanticsFromReceipt(receipt, tx, address);
        applyBorrowLifecycleSemanticsToRow(row, borrowLifecycleSemantics);
        const from = normalizeAddress(tx.from || receipt.from || "");
        const paidByWallet = from === address.toLowerCase();
        const gasUsed = hexToBigInt(receipt.gasUsed);
        const gasPrice = hexToBigInt(receipt.effectiveGasPrice || tx.gasPrice);
        const executionFeeWei = gasUsed * gasPrice;
        const extraFeeWei = receiptExtraFeeWei(receipt);
        const feeWei = executionFeeWei + extraFeeWei;
        const nativeDecimals = chain.nativeDecimals || 18;
        const nativeAmountExact = formatUnits(feeWei, nativeDecimals, nativeDecimals);
        const nativeAmount = formatUnits(feeWei, nativeDecimals, 8);
        if (!paidByWallet) {
          return {
            status: "not-payer",
            paidByWallet,
            from,
            nativeAmount,
            nativeAmountExact,
            nativeSymbol: chain.nativeSymbol,
            gasUsed: gasUsed.toString(),
            feeWei: feeWei.toString(),
            extraFeeWei: extraFeeWei.toString(),
            borrowLifecycleSemantics,
          };
        }
        const price = await historicalPrice(chain.priceId, row.timestamp);
        const nativeAmountNumber = unitsToNumber(feeWei, nativeDecimals);
        const gasUsd = price === null ? null : nativeAmountNumber * price;
        return {
          status: price === null ? "price-missing" : "ok",
          paidByWallet,
          from,
          nativeAmount,
          nativeAmountExact,
          nativeSymbol: chain.nativeSymbol,
          gasUsed: gasUsed.toString(),
          gasPriceWei: gasPrice.toString(),
          feeWei: feeWei.toString(),
          executionFeeWei: executionFeeWei.toString(),
          extraFeeWei: extraFeeWei.toString(),
          historicalPrice: price,
          gasUsd,
          borrowLifecycleSemantics,
        };
      } catch (error) {
        return { status: "error", paidByWallet: false, error: error.message || String(error) };
      }
    })();
    gasCache.set(cacheKey, request);
    const result = await request;
    gasCache.set(cacheKey, result);
    writeStoredGasResult(cacheKey, result);
    return result;
  }

  function applyBorrowReceiptSemanticsForRow(row, receipt, tx, address = "") {
    return applyBorrowLifecycleSemanticsToRow(row, borrowLifecycleSemanticsFromReceipt(receipt, tx, address));
  }

  function borrowLifecycleSemanticsFromReceipt(receipt, tx, address = "") {
    const wallet = normalizeAddress(address || tx?.from || receipt?.from || "");
    const input = String(tx?.input || "").toLowerCase();
    let inputSemantic = null;
    if (input.startsWith(OPEN_BORROW_POSITION_SELECTOR)) {
      inputSemantic = { action: "openBorrow", account: calldataUintToDecimal(input, 1), source: "calldata" };
    } else if (input.startsWith(CLOSE_BORROW_POSITION_SELECTOR)) {
      inputSemantic = { action: "closeBorrow", account: calldataUintToDecimal(input, 0), source: "calldata" };
    }
    if (!inputSemantic) return [];
    const semantics = [];
    (receipt?.logs || []).forEach(log => {
      const topic0 = String(log?.topics?.[0] || "").toLowerCase();
      if (topic0 === BORROW_POSITION_OPEN_TOPIC) {
        if (inputSemantic && inputSemantic.action !== "openBorrow") return;
        const borrower = topicAddressToAddress(log.topics?.[1]);
        if (!wallet || !borrower || borrower === wallet) {
          semantics.push({ action: "openBorrow", account: topicUintToDecimal(log.topics?.[2]), source: "receipt_log" });
        }
      } else if (topic0 === BORROW_POSITION_CLOSE_TOPIC) {
        if (inputSemantic && inputSemantic.action !== "closeBorrow") return;
        const borrower = topicAddressToAddress(log.topics?.[1]) || topicAddressToAddress(log.topics?.[2]);
        if (!wallet || !borrower || borrower === wallet) {
          semantics.push({ action: "closeBorrow", account: calldataUintToDecimal(log.data, 0), source: "receipt_log" });
        }
      }
    });
    if (inputSemantic) semantics.push(inputSemantic);
    return dedupeBorrowLifecycleSemantics(semantics);
  }

  function applyBorrowLifecycleSemanticsToRow(row, semantics = []) {
    if (!row || !Array.isArray(row.events) || !semantics.length) return false;
    let changed = false;
    semantics.forEach(semantic => {
      const action = semantic?.action;
      const account = normalizeAccountNumberValue(semantic?.account);
      if (action !== "openBorrow" && action !== "closeBorrow") return;
      row.events.forEach(event => {
        if (event?.action !== "transfer" || !event.isSelfTransfer) return;
        const fromAccount = normalizeAccountNumberValue(event.fromAccount);
        const toAccount = normalizeAccountNumberValue(event.toAccount);
        const matchesOpen = action === "openBorrow"
          && isBorrowRouteAccountNumber(toAccount)
          && (!account || account === toAccount);
        const matchesClose = action === "closeBorrow"
          && (isBorrowRouteAccountNumber(fromAccount) || isBorrowRouteAccountNumber(toAccount))
          && (!account || account === fromAccount || account === toAccount);
        if (!matchesOpen && !matchesClose) return;
        setBorrowSemantic(event, action, semantic.source === "calldata" ? "borrow_position_calldata" : "borrow_position_receipt");
        changed = true;
      });
    });
    if (changed) refreshRowSemanticActions(row);
    return changed;
  }

  function refreshRowSemanticActions(row) {
    if (!row) return;
    row.semanticActions = new Set((row.events || []).map(event => event.borrowSemanticAction).filter(Boolean));
  }

  function dedupeBorrowLifecycleSemantics(semantics = []) {
    const seen = new Set();
    return semantics.filter(semantic => {
      const key = `${semantic?.action || ""}:${normalizeAccountNumberValue(semantic?.account)}`;
      if (!semantic?.action || seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  function topicAddressToAddress(topic) {
    const raw = String(topic || "").toLowerCase();
    if (!/^0x[0-9a-f]{64}$/.test(raw)) return "";
    return normalizeAddress(`0x${raw.slice(-40)}`);
  }

  function topicUintToDecimal(topic) {
    const raw = String(topic || "").toLowerCase();
    if (!/^0x[0-9a-f]+$/.test(raw)) return "";
    try {
      return BigInt(raw).toString();
    } catch (error) {
      return "";
    }
  }

  function calldataUintToDecimal(data, index) {
    let raw = String(data || "").toLowerCase().replace(/^0x/, "");
    if (raw.length % 64 === 8) raw = raw.slice(8);
    const start = Math.max(0, Number(index || 0)) * 64;
    const word = raw.slice(start, start + 64);
    if (!/^[0-9a-f]{64}$/.test(word)) return "";
    try {
      return BigInt(`0x${word}`).toString();
    } catch (error) {
      return "";
    }
  }

  function configuredRpcGatewayValue() {
    if (window.__DOLO_RPC_GATEWAY) return window.__DOLO_RPC_GATEWAY;
    try {
      const raw = window.localStorage?.getItem(RPC_GATEWAY_STORAGE_KEY);
      if (!raw) return null;
      try {
        return JSON.parse(raw);
      } catch (_error) {
        return raw;
      }
    } catch (error) {
      console.debug("History RPC gateway config unavailable:", error);
      return null;
    }
  }

  function gatewayUrlsForChain(chainKey) {
    const config = configuredRpcGatewayValue();
    const values = [];
    const addValue = value => {
      if (!value) return;
      if (Array.isArray(value)) {
        value.forEach(addValue);
        return;
      }
      if (typeof value !== "string") return;
      const trimmed = value.trim();
      if (!trimmed) return;
      values.push(trimmed.replaceAll("{chain}", chainKey));
    };
    if (typeof config === "string" || Array.isArray(config)) {
      addValue(config);
    } else if (config && typeof config === "object") {
      addValue(config[chainKey]);
      addValue(config.default);
    }
    return values;
  }

  function rpcUrlsForChain(chainKey) {
    const chain = CHAINS[chainKey];
    const urls = [...gatewayUrlsForChain(chainKey), ...(chain?.rpcs || [])].filter(Boolean);
    return Array.from(new Set(urls));
  }

  async function rpcRequest(chainKey, method, params) {
    const chain = CHAINS[chainKey];
    const rpcs = rpcUrlsForChain(chainKey);
    let lastError = null;
    for (let attempt = 0; attempt < rpcs.length * 2; attempt++) {
      const rpc = rpcs[chain.rpcIdx % rpcs.length];
      chain.rpcIdx += 1;
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 10000);
      try {
        const response = await fetch(rpc, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          signal: ctrl.signal,
          body: JSON.stringify({ jsonrpc: "2.0", method, params, id: Date.now() }),
        });
        clearTimeout(timer);
        if (!response.ok) throw new Error(`RPC HTTP ${response.status}`);
        const json = await response.json();
        if (json.error) throw new Error(json.error.message || "RPC error");
        return json.result;
      } catch (error) {
        clearTimeout(timer);
        lastError = error && error.name === "AbortError" ? new Error("RPC timeout") : error;
      }
    }
    throw lastError || new Error("RPC request failed");
  }

  async function historicalPrice(priceId, timestamp) {
    if (!priceId || !timestamp) return null;
    const ts = Math.max(1, Number(timestamp || 0));
    const cacheKey = `${priceId}:${Math.floor(ts / 300) * 300}`;
    if (priceCache.has(cacheKey)) return priceCache.get(cacheKey);
    const request = (async () => {
      try {
        const url = `https://coins.llama.fi/prices/historical/${ts}/${encodeURIComponent(priceId)}`;
        const response = await fetch(url, { cache: "force-cache" });
        if (!response.ok) throw new Error(`price HTTP ${response.status}`);
        const json = await response.json();
        const price = Number(json?.coins?.[priceId]?.price);
        return Number.isFinite(price) && price > 0 ? price : null;
      } catch (error) {
        console.debug("Historical price unavailable:", priceId, timestamp, error);
        return null;
      }
    })();
    priceCache.set(cacheKey, request);
    return request;
  }

  function emptyEarnState(status = "idle") {
    return {
      status,
      manifest: null,
      rewardsManifest: null,
      snapshotManifest: null,
      ledgers: {},
      rewards: {},
      yearYields: {},
      prices: {},
      warnings: [],
      generatedAt: "",
    };
  }

  async function fetchEarnEnrichment(address, bounds, runId) {
    const earn = emptyEarnState("ready");
    try {
      const [manifestResult, rewardsManifestResult, snapshotManifestResult] = await Promise.all([
        fetchOptionalJson(`${EARN_LEDGER_BASE}/manifest.json`),
        fetchOptionalJson(`${EARN_REWARDS_BASE}/manifest.json`),
        fetchOptionalJson(`${EARN_SNAPSHOT_BASE}/manifest.json`),
      ]);
      if (runId !== state.runId) return emptyEarnState("idle");
      earn.manifest = manifestResult.payload || null;
      earn.rewardsManifest = rewardsManifestResult.payload || null;
      earn.snapshotManifest = snapshotManifestResult.payload || null;
      earn.generatedAt = earn.manifest?.generatedAt || "";
      [manifestResult, rewardsManifestResult, snapshotManifestResult].forEach(result => {
        if (result.error) earn.warnings.push(result.error);
      });

      const chainKeys = selectedChainKeys();
      const selectedChainSet = new Set(chainKeys);
      const ledgerResults = await mapLimit(chainKeys, 4, async chainKey => {
        const result = await fetchOptionalJson(`${EARN_LEDGER_BASE}/${chainKey}/${address}.json`);
        return { chainKey, result };
      });
      if (runId !== state.runId) return emptyEarnState("idle");
      ledgerResults.forEach(({ chainKey, result }) => {
        if (result.payload && result.payload.markets && typeof result.payload.markets === "object") {
          const latestSnapshotDate = String(earn.manifest?.chains?.[chainKey]?.snapshotDate || "");
          const ledgerSnapshotDate = String(result.payload.snapshotDate || "");
          if (latestSnapshotDate && ledgerSnapshotDate && ledgerSnapshotDate < latestSnapshotDate) {
            result.payload.__historySnapshotStale = true;
            result.payload.__historyManifestSnapshotDate = latestSnapshotDate;
            earn.warnings.push(`${CHAINS[chainKey].name} EARN ledger snapshot ${ledgerSnapshotDate} is older than manifest ${latestSnapshotDate}; marked review-only.`);
          }
          earn.ledgers[chainKey] = result.payload;
        } else if (result.error) {
          earn.warnings.push(`${CHAINS[chainKey].name} EARN ledger unavailable: ${result.error}`);
        }
      });

      const rewardChains = Object.keys(earn.rewardsManifest?.chains || {}).filter(chainKey => CHAINS[chainKey] && selectedChainSet.has(chainKey));
      const rewardResults = await mapLimit(rewardChains, 4, async chainKey => {
        const result = await fetchOptionalJson(`${EARN_REWARDS_BASE}/${chainKey}/${address}.json`);
        return { chainKey, result };
      });
      if (runId !== state.runId) return emptyEarnState("idle");
      rewardResults.forEach(({ chainKey, result }) => {
        const rewards = result.payload?.rewards || result.payload;
        if (rewards && typeof rewards === "object" && !Array.isArray(rewards) && Object.keys(rewards).length) {
          earn.rewards[chainKey] = {
            ...result.payload,
            rewards,
          };
        } else if (result.error) {
          earn.warnings.push(`${CHAINS[chainKey].name} reward cache unavailable: ${result.error}`);
        }
      });

      await fetchEarnYearYields(earn, address, bounds, runId);
      if (runId !== state.runId) return emptyEarnState("idle");
      await enrichEarnPrices(earn, bounds);
      return earn;
    } catch (error) {
      const failed = emptyEarnState("error");
      failed.warnings = [error.message || String(error)];
      return failed;
    }
  }

  async function fetchOptionalJson(path) {
    try {
      const response = await fetch(path, { cache: "no-store" });
      if (response.status === 404) return { payload: null, error: "" };
      if (!response.ok) return { payload: null, error: `HTTP ${response.status}` };
      const payload = await response.json();
      return { payload, error: "" };
    } catch (error) {
      return { payload: null, error: error.message || String(error) };
    }
  }

  async function fetchEarnYearYields(earn, address, bounds, runId) {
    const manifest = earn.snapshotManifest || {};
    const dates = Array.isArray(manifest.dates) ? manifest.dates : [];
    if (!dates.length || !Object.keys(earn.ledgers || {}).length) return;
    const chainDates = new Map();
    Object.keys(earn.ledgers || {}).forEach(chainKey => {
      chainDates.set(chainKey, dates.filter(date => {
        if (!timestampOverlapsBounds(dateToUnix(date), bounds)) return false;
        const chainsForDate = manifest.chains?.[date];
        return !Array.isArray(chainsForDate) || chainsForDate.includes(chainKey);
      }));
    });
    const neededDates = Array.from(new Set(Array.from(chainDates.values()).flat())).sort();
    if (neededDates.length < 2) {
      earn.warnings.push(`EARN yearly snapshot series unavailable for ${bounds.label}; cumulative ledger fallback may be used.`);
      return;
    }
    const snapshotResults = await mapLimit(neededDates, 6, async date => {
      const result = await fetchOptionalJson(`${EARN_SNAPSHOT_BASE}/${date}.json`);
      return { date, result };
    });
    if (runId !== state.runId) return;
    const snapshotsByDate = {};
    snapshotResults.forEach(({ date, result }) => {
      if (result.payload?.snapshots) snapshotsByDate[date] = result.payload;
      else if (result.error) earn.warnings.push(`EARN snapshot ${date} unavailable: ${result.error}`);
    });
    Object.entries(earn.ledgers || {}).forEach(([chainKey, ledger]) => {
      const datesForChain = chainDates.get(chainKey) || [];
      const marketHistory = {};
      datesForChain.forEach(date => {
        const walletMarkets = snapshotsByDate[date]?.snapshots?.[chainKey]?.[address]?.markets;
        if (!walletMarkets) return;
        Object.entries(walletMarkets).forEach(([marketId, row]) => {
          if (!ledger.markets?.[marketId]) return;
          if (!marketHistory[marketId]) marketHistory[marketId] = [];
          marketHistory[marketId].push({
            date,
            par: safeBigInt(row.par),
            wei: safeBigInt(row.wei),
            token: row.token || ledger.markets[marketId]?.token || "",
            symbol: row.symbol || ledger.markets[marketId]?.symbol || "Token",
            decimals: row.decimals ?? ledger.markets[marketId]?.decimals ?? 18,
          });
        });
      });
      Object.entries(marketHistory).forEach(([marketId, points]) => {
        const sorted = points.sort((a, b) => a.date.localeCompare(b.date));
        if (sorted.length < 2) return;
        let yearlyYield = 0n;
        let skippedPairs = 0;
        for (let i = 1; i < sorted.length; i++) {
          const prev = sorted[i - 1];
          const curr = sorted[i];
          if (prev.par === 0n || curr.par === 0n) {
            skippedPairs += 1;
            continue;
          }
          yearlyYield += (prev.par * curr.wei - prev.wei * curr.par) / curr.par;
        }
        if (yearlyYield === 0n) return;
        const first = sorted[0];
        const last = sorted[sorted.length - 1];
        if (!earn.yearYields[chainKey]) earn.yearYields[chainKey] = {};
        earn.yearYields[chainKey][marketId] = {
          cumulativeYield: yearlyYield.toString(),
          firstDate: first.date,
          firstPar: first.par.toString(),
          firstWei: first.wei.toString(),
          lastDate: last.date,
          lastPar: last.par.toString(),
          lastWei: last.wei.toString(),
          days: Math.max(1, Math.round((dateToUnix(last.date) - dateToUnix(first.date)) / 86400)),
          snapshotCount: sorted.length,
          skippedPairs,
          token: last.token,
          symbol: last.symbol,
          decimals: last.decimals,
          source: "earn-snapshot-series",
          method: "snapshot-series-year",
        };
      });
    });
  }

  async function enrichEarnPrices(earn, bounds) {
    const tasks = [];
    Object.entries(earn.ledgers || {}).forEach(([chainKey, ledger]) => {
      Object.entries(ledger.markets || {}).forEach(([marketId, market]) => {
        const yearly = resolveEarnMarketYield(earn, chainKey, marketId, ledger, market, bounds);
        if (!yearly) return;
        const yieldWei = safeBigInt(yearly.cumulativeYield);
        if (yieldWei === 0n) return;
        const priceId = tokenPriceId(chainKey, yearly.token || market.token);
        const timestamp = dateToUnix(yearly.lastDate || market.lastDate || ledger.snapshotDate);
        if (!priceId || !timestamp) return;
        tasks.push({
          key: earnMarketPriceKey(chainKey, marketId),
          priceId,
          timestamp,
          amountWei: yieldWei,
          decimals: market.decimals ?? 18,
          kind: "ledger",
        });
      });
    });
    Object.entries(earn.rewards || {}).forEach(([chainKey, payload]) => {
      const timestamp = dateToUnix(payload.generatedAt || earn.rewardsManifest?.generatedAt);
      Object.entries(payload.rewards || {}).forEach(([symbol, reward]) => {
        const amount = decimalToNumber(reward?.accumulated);
        if (!amount || !Number.isFinite(amount)) return;
        const priceId = tokenPriceId(chainKey, reward?.token);
        if (!priceId || !timestamp) return;
        tasks.push({
          key: earnRewardPriceKey(chainKey, symbol),
          priceId,
          timestamp,
          amount,
          kind: "reward",
        });
      });
    });
    await mapLimit(tasks, 4, async task => {
      const price = await historicalPrice(task.priceId, task.timestamp);
      const usd = price === null
        ? null
        : task.kind === "ledger"
          ? unitsToNumber(task.amountWei, Number(task.decimals || 18)) * price
          : Number(task.amount || 0) * price;
      earn.prices[task.key] = {
        price,
        priceId: task.priceId,
        timestamp: task.timestamp,
        usd: Number.isFinite(usd) ? usd : null,
      };
    });
  }

  function earnTaxEntriesForCurrentView() {
    if (state.filtersDirty) return [];
    const allSelected = actionFilterAllSelected();
    const bounds = getBounds(state.year);
    const entries = [];
    if (state.earn && state.earn.status === "ready" && allSelected) {
      entries.push(...earnLedgerTaxEntries(bounds), ...earnRewardTaxEntries(bounds));
    }
    return entries;
  }

  function earnLedgerTaxEntries(bounds) {
    const entries = [];
    Object.entries(state.earn.ledgers || {}).forEach(([chainKey, ledger]) => {
      if (!state.selectedChains.has(chainKey)) return;
      Object.entries(ledger.markets || {}).forEach(([marketId, market]) => {
        const yearly = resolveEarnMarketYield(state.earn, chainKey, marketId, ledger, market, bounds);
        if (!yearly) return;
        const yieldWei = safeBigInt(yearly.cumulativeYield);
        if (yieldWei === 0n) return;
        const chain = CHAINS[chainKey];
        const decimals = Number(yearly.decimals ?? market.decimals ?? 18);
        const symbol = yearly.symbol || market.symbol || "Token";
        const ledgerStale = !!ledger.__historySnapshotStale;
        const evidenceLevel = ledgerStale ? "review" : earnLedgerEvidenceLevel(market);
        const trusted = evidenceLevel === "verified";
        const positive = yieldWei > 0n;
        const absWei = yieldWei < 0n ? -yieldWei : yieldWei;
        const priceInfo = state.earn.prices?.[earnMarketPriceKey(chainKey, marketId)] || {};
        const amountExact = formatUnits(absWei, decimals, decimals);
        const signedAmountExact = formatUnits(yieldWei, decimals, decimals);
        const timestamp = dateToUnix(yearly.lastDate || market.lastDate || ledger.snapshotDate);
        const status = String(market.strictStatus || market.status || "unverified");
        const method = yearly.method || String(market.strictMethod || market.method || "");
        const coverage = ledgerStale
          ? `stale_snapshot:${ledger.snapshotDate || "unknown"}<${ledger.__historyManifestSnapshotDate || "manifest"}`
          : String(market.canonicalHistoryCoverageStatus || ledger.canonicalHistory?.coverageStatus || "");
        const dateWindow = [yearly.firstDate, yearly.lastDate || ledger.snapshotDate].filter(Boolean).join(" to ");
        const yearSource = yearly.source === "earn-verified-ledger"
          ? `${evidenceLevel === "verified" ? "Verified" : evidenceLevel === "inferred" ? "Inferred" : "Review-only"} EARN ledger baseline.`
          : yearly.source === "earn-snapshot-series"
          ? `Year snapshot series (${yearly.snapshotCount || 0} points${yearly.skippedPairs ? `, ${yearly.skippedPairs} skipped pairs` : ""}).`
          : "Cumulative ledger fallback.";
        const staleNote = ledgerStale
          ? ` Ledger snapshot is older than the current EARN manifest (${ledger.__historyManifestSnapshotDate}); review-only.`
          : "";
        const reviewReason = earnReviewReasonFromSignals({
          trusted,
          evidenceLevel,
          positive,
          status,
          coverage,
          method,
          priceInfo,
        });
        entries.push({
          source: "earn-verified-ledger",
          chainKey,
          chainName: chain.name,
          timestamp,
          marketId,
          action: "EARN yield summary",
          taxCategory: positive
            ? (trusted ? "earn_yield_candidate" : evidenceLevel === "inferred" ? "earn_yield_inferred" : "earn_yield_unverified")
            : "earn_negative_yield_candidate",
          reviewFlag: positive && trusted ? "income_candidate" : "needs_review",
          reviewReason,
          claimProofStatus: "",
          assetInSymbol: positive ? symbol : "",
          assetInAmount: positive ? signedAmountExact : "",
          assetInUsd: positive && Number.isFinite(Number(priceInfo.usd)) ? decimalForCsv(priceInfo.usd) : "",
          assetOutSymbol: positive ? "" : symbol,
          assetOutAmount: positive ? "" : amountExact,
          assetOutUsd: !positive && Number.isFinite(Number(priceInfo.usd)) ? decimalForCsv(Math.abs(priceInfo.usd)) : "",
          usd: Number.isFinite(Number(priceInfo.usd)) ? Number(priceInfo.usd) : 0,
          feeStatus: "",
          priceSource: priceInfo.price === null
            ? `${priceInfo.priceId || tokenPriceId(chainKey, market.token)} price unavailable`
            : priceInfo.priceId
              ? `${priceInfo.priceId} @ ${decimalForCsv(priceInfo.price)}`
              : "",
          dataSource: "EARN snapshot-series/verified-ledger + DefiLlama snapshot-date prices where available",
          note: `${yearSource} EARN yield candidate${dateWindow ? `, window ${dateWindow}` : ""}; not a jurisdiction-specific realization ruling.${reviewReason ? ` Review reason: ${reviewReason}.` : ""}${staleNote}`,
          earnSource: "earn-verified-ledger",
          earnStatus: status,
          earnMethod: method,
          earnSnapshotDate: yearly.lastDate || ledger.snapshotDate || market.lastDate || "",
          earnCoverage: coverage,
          earnPeriodSource: yearly.source || "earn-verified-ledger",
          rewardAccumulated: "",
          rewardUnclaimed: "",
          rewardClaimedEstimate: "",
          trusted,
        });
      });
    });
    return entries;
  }

  function earnRewardTaxEntries(bounds) {
    const entries = [];
    Object.entries(state.earn.rewards || {}).forEach(([chainKey, payload]) => {
      if (!state.selectedChains.has(chainKey)) return;
      const timestamp = dateToUnix(payload.generatedAt || state.earn.rewardsManifest?.generatedAt);
      if (!timestampOverlapsBounds(timestamp, bounds)) return;
      Object.entries(payload.rewards || {}).forEach(([symbol, reward]) => {
        const accumulated = decimalToNumber(reward?.accumulated);
        if (!accumulated || !Number.isFinite(accumulated)) return;
        const unclaimed = decimalToNumber(reward?.unclaimed);
        const claimedEstimate = Math.max(0, accumulated - unclaimed);
        const priceInfo = state.earn.prices?.[earnRewardPriceKey(chainKey, symbol)] || {};
        const usd = Number.isFinite(Number(priceInfo.usd)) ? Number(priceInfo.usd) : 0;
        entries.push({
          source: "earn-merkl-rewards",
          chainKey,
          chainName: CHAINS[chainKey].name,
          timestamp,
          marketId: "",
          action: "EARN reward summary",
          taxCategory: "external_reward_candidate",
          reviewFlag: "needs_review",
          reviewReason: "reward_period_unverified; reward_claim_timing_unverified",
          claimProofStatus: "estimated_from_accumulated_minus_unclaimed",
          assetInSymbol: symbol,
          assetInAmount: decimalForCsv(accumulated),
          assetInUsd: Number.isFinite(Number(priceInfo.usd)) ? decimalForCsv(priceInfo.usd) : "",
          assetOutSymbol: "",
          assetOutAmount: "",
          assetOutUsd: "",
          usd,
          feeStatus: "",
          priceSource: priceInfo.price === null
            ? `${priceInfo.priceId || tokenPriceId(chainKey, reward?.token)} price unavailable`
            : priceInfo.priceId
              ? `${priceInfo.priceId} @ ${decimalForCsv(priceInfo.price)}`
              : "",
          dataSource: "EARN cached Merkl rewards + DefiLlama snapshot-date prices where available",
          note: `Cached external reward candidate as of ${payload.generatedAt || state.earn.rewardsManifest?.generatedAt || "unknown"}; accumulated ${decimalForCsv(accumulated)} ${symbol}; claimed estimate ${decimalForCsv(claimedEstimate)} ${symbol}; unclaimed ${decimalForCsv(unclaimed)} ${symbol}; cache is not a year-attributed claim ledger; claim proof status estimated_from_accumulated_minus_unclaimed; review earning/claim timing for the target jurisdiction.`,
          earnSource: "earn-merkl-rewards",
          earnStatus: "cached",
          earnMethod: "merkl-v3-rewards",
          earnSnapshotDate: payload.generatedAt || state.earn.rewardsManifest?.generatedAt || "",
          earnCoverage: "cached",
          earnPeriodSource: "reward-cache",
          rewardAccumulated: decimalForCsv(accumulated),
          rewardUnclaimed: decimalForCsv(unclaimed),
          rewardClaimedEstimate: decimalForCsv(claimedEstimate),
          trusted: true,
        });
      });
    });
    return entries;
  }

  function earnLedgerOverlapsBounds(ledger, market, bounds) {
    if (!bounds || !bounds.start || !bounds.end) return true;
    const start = dateToUnix(market.firstDate || ledger.snapshotDate || market.lastDate);
    const end = dateToUnix(market.lastDate || ledger.snapshotDate || market.firstDate);
    if (!start && !end) return false;
    return (end || start) >= bounds.start && (start || end) <= bounds.end;
  }

  function resolveEarnMarketYield(earn, chainKey, marketId, ledger, market, bounds) {
    const ledgerYield = earnLedgerYieldFromMarket(ledger, market);
    if (canUseEarnLedgerBaselineEntry(market) && earnLedgerOverlapsBounds(ledger, market, bounds)) {
      return ledgerYield;
    }
    const yearly = earn?.yearYields?.[chainKey]?.[marketId];
    if (yearly) return yearly;
    if (!earnLedgerOverlapsBounds(ledger, market, bounds)) return null;
    return ledgerYield;
  }

  function earnLedgerYieldFromMarket(ledger, market) {
    return {
      ...market,
      source: "earn-verified-ledger",
      method: market.strictMethod || market.method || "ledger-cumulative",
      token: market.token,
      symbol: market.symbol,
      decimals: market.decimals,
      cumulativeYield: market.cumulativeYield || "0",
      firstDate: market.firstDate,
      lastDate: market.lastDate || ledger.snapshotDate,
    };
  }

  function timestampOverlapsBounds(timestamp, bounds) {
    if (!bounds || !bounds.start || !bounds.end) return true;
    if (!timestamp) return false;
    return timestamp >= bounds.start && timestamp <= bounds.end;
  }

  function canUseEarnLedgerMarketEntry(entry) {
    return earnLedgerEvidenceLevel(entry) === "verified";
  }

  function canUseEarnLedgerBaselineEntry(entry) {
    const level = earnLedgerEvidenceLevel(entry);
    return level === "verified" || level === "inferred";
  }

  function earnLedgerEvidenceLevel(entry) {
    if (!entry || typeof entry !== "object") return "review";
    const status = String(entry.strictStatus || entry.status || "").trim().toLowerCase();
    const rawStatus = String(entry.status || "").trim().toLowerCase();
    const method = String(entry.strictMethod || entry.method || "").trim().toLowerCase();
    const canonicalCoverage = String(entry.canonicalHistoryCoverageStatus || "").trim().toLowerCase();
    if (status === "verified") return "verified";
    if (
      status === "inferred"
      && rawStatus === "pre_snapshot_carry"
      && canonicalCoverage === "fresh"
      && method !== "snapshot-fallback"
    ) return "inferred";
    return "review";
  }

  function earnReviewReasonFromSignals({ trusted, evidenceLevel, positive, status, coverage, method, priceInfo }) {
    const reasons = [];
    const statusText = String(status || "").toLowerCase();
    const coverageText = String(coverage || "").toLowerCase();
    const methodText = String(method || "").toLowerCase();
    if (!positive) reasons.push("negative_yield_review");
    if (!trusted) {
      if (evidenceLevel === "inferred") reasons.push("inferred_yield_review");
      if (coverageText.startsWith("stale_snapshot")) reasons.push("stale_ledger");
      if (statusText.includes("mismatch")) reasons.push("status_mismatch");
      if (statusText.includes("coverage_incomplete") || coverageText.includes("coverage_incomplete")) reasons.push("coverage_incomplete");
      if (methodText.includes("snapshot-fallback")) reasons.push("snapshot_fallback");
      if (!reasons.length) reasons.push("untrusted_earn_method");
    }
    if (priceInfo?.price === null) reasons.push("price_unavailable");
    return Array.from(new Set(reasons)).join("; ");
  }

  function tokenPriceId(chainKey, tokenAddress) {
    const chain = CHAINS[chainKey];
    const address = normalizeAddress(tokenAddress || "");
    if (!chain?.llamaSlug || !isAddress(address)) return "";
    return `${chain.llamaSlug}:${address}`;
  }

  function earnMarketPriceKey(chainKey, marketId) {
    return `${chainKey}:market:${marketId}`;
  }

  function earnRewardPriceKey(chainKey, symbol) {
    return `${chainKey}:reward:${String(symbol || "").toUpperCase()}`;
  }

  function safeBigInt(value) {
    try {
      return BigInt(String(value ?? "0"));
    } catch (error) {
      return 0n;
    }
  }

  function dateToUnix(value) {
    const raw = String(value || "").trim();
    if (!raw) return 0;
    const normalized = /^\d{4}-\d{2}-\d{2}$/.test(raw) ? `${raw}T12:00:00Z` : raw;
    const ms = Date.parse(normalized);
    return Number.isFinite(ms) ? Math.floor(ms / 1000) : 0;
  }

  function render() {
    state.filteredRows = rowsMatchingCurrentFilters(state.rows);
    state.visiblePage = clampHistoryVisiblePage(state.visiblePage, state.filteredRows.length);
    renderRows();
    renderLoadingPanel();
    renderReportFiles();
    syncControls();
  }

  function rowsMatchingCurrentFilters(rows) {
    return (rows || []).filter(row => {
      if (!state.selectedChains.has(row.chainKey)) return false;
      if (!actionFilterAllSelected() && !rowMatchesAnyActionFilter(row, selectedActionKeys())) return false;
      return true;
    });
  }

  function renderRows() {
    const rows = state.filteredRows;
    const earnEntries = earnTaxEntriesForCurrentView();
    const readiness = reportExportReadiness(rows, earnEntries);
    els.count.textContent = historyCountLabel(rows, earnEntries);
    els.taxExport.disabled = !readiness.canFullReport;
    renderHistoryPagination(rows);

    if (state.loading) {
      els.body.innerHTML = `<tr class="empty-row"><td colspan="${HISTORY_TABLE_COLSPAN}">Loading Dolomite history...</td></tr>`;
      return;
    }
    if (!rows.length) {
      const msg = emptyHistoryMessageHtml(earnEntries);
      els.body.innerHTML = `<tr class="empty-row"><td colspan="${HISTORY_TABLE_COLSPAN}">${msg}</td></tr>`;
      return;
    }

    const pageStart = historyVisiblePageStartIndex();
    const pageRows = historyVisibleRowsForPage(rows);
    const dataRowsHtml = pageRows.map((row, index) => {
      const expanded = state.expandedKey === row.key;
      return rowHtml(row, expanded, pageStart + index) + (expanded ? detailHtml(row) : "");
    }).join("");
    els.body.innerHTML = dataRowsHtml + historySpacerRowsHtml(pageRows.length);
  }

  function historyVisiblePageCount(rows = state.filteredRows) {
    const count = Array.isArray(rows) ? rows.length : Number(rows || 0);
    return Math.max(1, Math.ceil(count / HISTORY_VISIBLE_PAGE_SIZE));
  }

  function clampHistoryVisiblePage(page, rowCount = state.filteredRows.length) {
    const total = historyVisiblePageCount(Number(rowCount || 0));
    const safePage = Number.isFinite(Number(page)) ? Math.trunc(Number(page)) : 1;
    return Math.max(1, Math.min(safePage || 1, total));
  }

  function historyVisiblePageStartIndex() {
    return (state.visiblePage - 1) * HISTORY_VISIBLE_PAGE_SIZE;
  }

  function historyVisibleRowsForPage(rows = state.filteredRows) {
    const sourceRows = Array.isArray(rows) ? rows : [];
    const start = historyVisiblePageStartIndex();
    return sourceRows.slice(start, start + HISTORY_VISIBLE_PAGE_SIZE);
  }

  function historySpacerRowsHtml(visibleRowCount) {
    const missingRows = Math.max(0, HISTORY_VISIBLE_PAGE_SIZE - Number(visibleRowCount || 0));
    return Array.from({ length: missingRows }, () => `
      <tr class="history-spacer-row" aria-hidden="true" tabindex="-1">
        <td colspan="${HISTORY_TABLE_COLSPAN}"><span>&nbsp;</span></td>
      </tr>
    `).join("");
  }

  function renderHistoryPagination(rows = state.filteredRows) {
    if (!els.pagination) return;
    const count = Array.isArray(rows) ? rows.length : 0;
    const total = historyVisiblePageCount(count);
    if (state.loading || count <= HISTORY_VISIBLE_PAGE_SIZE) {
      els.pagination.hidden = true;
      els.pagination.innerHTML = "";
      return;
    }
    state.visiblePage = clampHistoryVisiblePage(state.visiblePage, count);
    const page = state.visiblePage;
    els.pagination.hidden = false;
    els.pagination.innerHTML = `<button class="flow-pager-btn" type="button" data-history-page="1" aria-label="First transaction page" ${page === 1 ? "disabled" : ""}>«</button>` +
      `<button class="flow-pager-btn" type="button" data-history-page="${page - 1}" aria-label="Previous transaction page" ${page === 1 ? "disabled" : ""}>‹</button>` +
      `<span class="flow-pager-info">${page} / ${total}</span>` +
      `<button class="flow-pager-btn" type="button" data-history-page="${page + 1}" aria-label="Next transaction page" ${page === total ? "disabled" : ""}>›</button>` +
      `<button class="flow-pager-btn" type="button" data-history-page="${total}" aria-label="Last transaction page" ${page === total ? "disabled" : ""}>»</button>`;
  }

  function goHistoryPage(page) {
    const nextPage = clampHistoryVisiblePage(page, state.filteredRows.length);
    if (nextPage === state.visiblePage) return;
    state.visiblePage = nextPage;
    state.expandedKey = "";
    renderRows();
    if (els.tableWrap) els.tableWrap.scrollLeft = 0;
  }

  function historyCountLabel(rows = [], earnEntries = []) {
    const txLabel = `${rows.length.toLocaleString()} transaction${rows.length === 1 ? "" : "s"}`;
    if (!earnEntries.length) return txLabel;
    return `${txLabel} · ${earnEntries.length.toLocaleString()} evidence row${earnEntries.length === 1 ? "" : "s"}`;
  }

  function emptyHistoryMessageHtml(earnEntries = []) {
    if (earnEntries.length) {
      return `
        <div class="history-empty-note">
          <strong>No transaction rows match these filters.</strong>
          <span>${earnEntries.length.toLocaleString()} evidence row${earnEntries.length === 1 ? "" : "s"} will still be included in the downloadable report.</span>
        </div>
      `;
    }
    const msg = state.rows.length ? "No rows match the selected filters." : "No wallet loaded yet.";
    return escapeHtml(msg);
  }

  function rowHtml(row, expanded, index = 0) {
    const chain = CHAINS[row.chainKey];
    const actions = displayActionsForRow(row);
    const eventPreview = compactTransactionAssetPreview(row) || row.events.slice(0, 2).map(event => event.label).join(" | ");
    const assetFlowClass = semanticAssetFlowClass(row);
    const detailToggle = historyDetailToggleHtml(expanded);
    const rowClassName = [expanded ? "expanded" : "", index % 2 === 1 ? "row-even" : "row-odd"].filter(Boolean).join(" ");
    return `
      <tr class="${rowClassName}" data-row-key="${escapeAttr(row.key)}">
        <td class="chain-td">${chainChip(row.chainKey)}</td>
        <td class="date-td">
          <div class="date-cell">
            <div class="date-top">
              <span class="date-main">${escapeHtml(formatHistoryDate(row.timestamp))}</span>
              <a class="date-tx" href="${escapeAttr(chain.explorerTx + row.txHash)}" target="_blank" rel="noopener" aria-label="Open transaction ${escapeAttr(shortHash(row.txHash))}">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><path d="M15 3h6v6"/><path d="M10 14 21 3"/></svg>
              </a>
            </div>
            <span class="date-sub">${escapeHtml(formatRelativeTime(row.timestamp))} · ${escapeHtml(formatClockTime(row.timestamp))}</span>
          </div>
        </td>
        <td class="action-td"><div class="action-list">${actions.map(actionChip).join("")}</div></td>
        <td class="asset-td">
          <div class="asset-cell">
            <span class="asset-line${assetFlowClass ? ` ${escapeAttr(assetFlowClass)}` : ""}">${escapeHtml(eventPreview || "-")}</span>
          </div>
        </td>
        <td class="num volume-td">${formatUsd(row.usdVolume)}</td>
        <td class="num gas-td">${gasHtml(row)}</td>
        <td class="details-cell">${detailToggle}</td>
      </tr>`;
  }

  function displayActionsForRow(row) {
    if (rowClassificationPending(row)) return ["classificationPending"];
    const semanticActions = Array.from(row.semanticActions || []).filter(Boolean);
    const hasBorrowSemantic = semanticActions.some(action => ["borrow", "openBorrow"].includes(action));
    const hasRepaySemantic = semanticActions.some(action => ["repay", "closeBorrow"].includes(action));
    const hasCollateralSemantic = semanticActions.some(action => ["addCollateral", "withdrawCollateral"].includes(action));
    const actions = Array.from(row.actions || [])
      .filter(Boolean)
      .filter(action => !BORROW_POSITION_LIFECYCLE_ACTIONS.has(action))
      .filter(action => !(action === "withdraw" && (hasBorrowSemantic || hasRepaySemantic || hasCollateralSemantic)))
      .filter(action => !(action === "deposit" && (hasBorrowSemantic || hasRepaySemantic || hasCollateralSemantic)))
      .filter(action => !(action === "transfer" && (hasBorrowSemantic || hasRepaySemantic || hasCollateralSemantic)));
    const vestingChips = vestingActionChipsForRow(row);
    const mergedActions = [
      ...semanticActions,
      ...actions.filter(action => !semanticActions.includes(action)),
    ];
    const ammChips = ammActionChipsForRow(row);
    if (mergedActions.includes("zap")) {
      const semanticZapActions = semanticActions.filter(action => !["zap", "deposit", "withdraw"].includes(action));
      return ["zap", ...semanticZapActions, ...vestingChips];
    }
    const nonAmmActions = mergedActions.filter(action => action !== "amm");
    if (!nonAmmActions.includes("vesting")) return [...nonAmmActions, ...ammChips];
    return [
      ...nonAmmActions.filter(action => action !== "vesting"),
      ...ammChips,
      ...(vestingChips.length ? vestingChips : [{ key: "vesting", className: "vesting", label: ACTION_TABLE_LABELS.vesting }]),
    ];
  }

  function semanticAssetFlowClass(row) {
    const actions = row?.semanticActions || new Set();
    if (actions.has("addCollateral")) return "collateral-up";
    if (actions.has("withdrawCollateral")) return "collateral-down";
    return "";
  }

  function ammActionChipsForRow(row) {
    const seen = new Set();
    return (row?.events || [])
      .filter(event => event?.action === "amm")
      .map(event => {
        const key = ammActionKey(event);
        if (!key || seen.has(key)) return null;
        seen.add(key);
        return {
          key,
          className: "amm",
          label: ACTION_TABLE_LABELS[key] || cleanReportActionLabel(event),
        };
      })
      .filter(Boolean);
  }

  function ammActionKey(event) {
    if (event?.taxCategory === "swap") return "ammTrade";
    if (event?.taxCategory === "liquidity_deposit") return "ammAddLiquidity";
    if (event?.taxCategory === "liquidity_withdrawal") return "ammRemoveLiquidity";
    return "amm";
  }

  function rowMatchesActionFilter(row, action) {
    action = normalizeActionFilter(action);
    if (rowClassificationPending(row)) return false;
    const rowActions = row.actions || new Set();
    const semanticActions = row.semanticActions || new Set();
    const hasSwapLikeAction = rowActions.has("trade")
      || rowActions.has("zap")
      || (row.events || []).some(event => event?.taxCategory === "swap" || event?.taxCategory === "zap");
    if (action === "borrow") return semanticActions.has("borrow") || semanticActions.has("openBorrow");
    if (action === "repay") return semanticActions.has("repay");
    if (action === "closeBorrow") return semanticActions.has("closeBorrow");
    if (action === "addCollateral") return semanticActions.has("addCollateral");
    if (action === "withdrawCollateral") return semanticActions.has("withdrawCollateral");
    if (action === "withdraw" && (semanticActions.has("borrow") || semanticActions.has("openBorrow") || semanticActions.has("repay") || semanticActions.has("closeBorrow") || semanticActions.has("addCollateral") || semanticActions.has("withdrawCollateral"))) return false;
    if (action === "deposit" && (semanticActions.has("borrow") || semanticActions.has("openBorrow") || semanticActions.has("repay") || semanticActions.has("closeBorrow") || semanticActions.has("addCollateral") || semanticActions.has("withdrawCollateral"))) return false;
    if (action === "transfer" && (semanticActions.has("borrow") || semanticActions.has("openBorrow") || semanticActions.has("repay") || semanticActions.has("closeBorrow") || semanticActions.has("addCollateral") || semanticActions.has("withdrawCollateral"))) return false;
    if (action === "transfer" && hasSwapLikeAction) return false;
    if (action === "swap") {
      return hasSwapLikeAction;
    }
    if (action === "vestingPair") {
      return vestingEventsForRow(row).some(event => vestingFlowIsOpen(event.vestingFlowLabel));
    }
    if (action === "vestingClaim") {
      return vestingEventsForRow(row).some(event => vestingFlowIsExercise(event.vestingFlowLabel));
    }
    if (action === "claim") {
      return rowActions.has("odoloClaim")
        || rowActions.has("rewardClaim")
        || vestingEventsForRow(row).some(event => vestingFlowIsExercise(event.vestingFlowLabel));
    }
    return rowActions.has(action);
  }

  function rowClassificationPending(row) {
    if (!row?.receiptClassificationPending) return false;
    const gasStatus = row?.gas?.status || "pending";
    if (gasStatus !== "pending") return false;
    return rowNeedsReceiptClassification(row);
  }

  function markReceiptClassificationPendingRows(rows) {
    (rows || []).forEach(row => {
      row.receiptClassificationPending = rowNeedsReceiptClassification(row);
    });
    return rows;
  }

  function rowNeedsReceiptClassification(row) {
    const events = Array.isArray(row?.events) ? row.events : [];
    if (!events.length) return false;
    return events.some(eventNeedsReceiptClassification) || rowHasRouteSwap(events);
  }

  function eventNeedsReceiptClassification(event) {
    if (!event) return false;
    const action = event.action;
    const account = normalizeAccountNumberValue(event.account);
    if ((action === "deposit" || action === "withdraw") && isBorrowRouteAccountNumber(account)) return true;
    if (action !== "transfer" || !event.isSelfTransfer) return false;
    return isBorrowRouteAccountNumber(account)
      || isBorrowRouteAccountNumber(normalizeAccountNumberValue(event.fromAccount))
      || isBorrowRouteAccountNumber(normalizeAccountNumberValue(event.toAccount));
  }

  function rowHasRouteSwap(events) {
    const hasSwap = events.some(event => isSwapLikeEvent(event));
    if (!hasSwap) return false;
    return events.some(event => {
      return isBorrowRouteAccountNumber(normalizeAccountNumberValue(event?.account))
        || isBorrowRouteAccountNumber(normalizeAccountNumberValue(event?.fromAccount))
        || isBorrowRouteAccountNumber(normalizeAccountNumberValue(event?.toAccount));
    });
  }

  function rowMatchesAnyActionFilter(row, actions) {
    return (actions || []).some(action => rowMatchesActionFilter(row, action));
  }

  function vestingEventsForRow(row) {
    return (row?.events || []).filter(event => event?.action === "vesting");
  }

  function vestingActionChipsForRow(row) {
    const seen = new Set();
    return vestingEventsForRow(row)
      .map(event => {
        const fullLabel = cleanReportActionLabel(event);
        const key = vestingActionKey(event);
        const label = ACTION_TABLE_LABELS[key] || fullLabel;
        if (!label || seen.has(key)) return null;
        seen.add(key);
        return {
          key,
          className: "vesting",
          label,
        };
      })
      .filter(Boolean);
  }

  function vestingActionKey(event) {
    if (vestingFlowIsOpen(event?.vestingFlowLabel)) return "vestingPair";
    if (vestingFlowIsExercise(event?.vestingFlowLabel)) return "vestingClaim";
    if (vestingFlowIsInternal(event?.vestingFlowLabel)) return "vestingInternal";
    return "vesting";
  }

  function historyDetailToggleHtml(expanded) {
    return `
      <button class="history-detail-toggle" type="button" data-history-detail-toggle aria-label="${expanded ? "Hide transaction details" : "Show transaction details"}" aria-expanded="${expanded ? "true" : "false"}">
        <span>${expanded ? "Hide" : "Details"}</span>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"/></svg>
      </button>
    `;
  }

  function detailEventFlowLabel(event) {
    const label = String(event?.label || "").trim();
    if (event?.action === "vesting") {
      return compactVestingTableFlow(event) || stripDetailActionPrefix(label, cleanReportActionLabel(event)) || "-";
    }
    if (isSwapLikeEvent(event)) {
      return cleanSwapOutcomeFlow(event, "detail") || stripDetailActionPrefix(label, cleanReportActionLabel(event)) || "-";
    }
    if (event?.action === "transfer") {
      return compactTransferTableFlow(event, "detail") || stripDetailActionPrefix(label, cleanReportActionLabel(event)) || "-";
    }
    return stripDetailActionPrefix(label, cleanReportActionLabel(event)) || "-";
  }

  function stripDetailActionPrefix(label, prefix) {
    const text = String(label || "").trim();
    const head = String(prefix || "").trim();
    if (!text || !head) return text;
    const lower = text.toLowerCase();
    const colonPrefix = `${head}:`.toLowerCase();
    if (lower.startsWith(colonPrefix)) return text.slice(colonPrefix.length).trim() || text;
    const spacedPrefix = `${head} `.toLowerCase();
    if (lower.startsWith(spacedPrefix)) return text.slice(spacedPrefix.length).trim() || text;
    return text;
  }

  function detailDisplayEventsForRow(row) {
    const events = Array.isArray(row?.events) ? row.events : [];
    const displayEvents = events.filter(event => !isBorrowPositionLifecycleEvent(event));
    const sourceEvents = displayEvents.length ? displayEvents : events;
    if (sourceEvents.length <= 1) return sourceEvents;
    const vestingEvents = sourceEvents.filter(event => event?.action === "vesting");
    if (vestingEvents.length) return uniqueDetailEvents(vestingEvents);
    const primary = primaryTransactionEvent(sourceEvents);
    if (primary) return [primary];
    return uniqueDetailEvents(sourceEvents);
  }

  function isBorrowPositionLifecycleEvent(event) {
    return BORROW_POSITION_LIFECYCLE_ACTIONS.has(event?.action);
  }

  function uniqueDetailEvents(events) {
    const seen = new Set();
    return (Array.isArray(events) ? events : []).filter(event => {
      const key = [
        event?.action || "",
        event?.borrowSemanticAction || "",
        event?.vestingFlowLabel || "",
        detailEventFlowLabel(event),
        event?.account || "",
        event?.fromAccount || "",
        event?.toAccount || "",
      ].join("|");
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  function detailHtml(row) {
    const gas = row.gas || {};
    const senderLink = sameHistoryWalletAddress(gas.from) ? "" : addressExplorerLinkHtml(row.chainKey, gas.from);
    const payerLink = addressExplorerLinkHtml(row.chainKey, gas.from);
    const displayEvents = detailDisplayEventsForRow(row);
    const gasProof = gas.status === "ok"
      ? [
        `<span><b>Gas paid:</b> ${escapeHtml(gas.nativeAmountExact || gas.nativeAmount)} ${escapeHtml(gas.nativeSymbol)}</span>`,
        `<span><b>Historical price:</b> ${formatUsd(gas.historicalPrice)} / ${escapeHtml(gas.nativeSymbol)} at tx timestamp</span>`,
        `<span><b>Gas USD:</b> ${formatUsd(gas.gasUsd)}</span>`,
        gas.extraFeeWei && gas.extraFeeWei !== "0" ? `<span><b>Extra receipt fee:</b> ${escapeHtml(formatUnits(BigInt(gas.extraFeeWei), 18, 10))} ${escapeHtml(gas.nativeSymbol)}</span>` : "",
        senderLink ? `<span><b>Tx sender:</b> ${senderLink}</span>` : "",
      ].filter(Boolean).join("")
      : gas.status === "price-missing"
        ? [
          gas.nativeAmount ? `<span><b>Gas paid:</b> ${escapeHtml(gas.nativeAmountExact || gas.nativeAmount)} ${escapeHtml(gas.nativeSymbol || CHAINS[row.chainKey]?.nativeSymbol || "")}</span>` : "",
          `<span><b>Historical price:</b> unavailable for ${escapeHtml(gas.nativeSymbol || CHAINS[row.chainKey]?.nativeSymbol || "native token")} at tx timestamp</span>`,
          `<span><b>Gas USD:</b> price review required</span>`,
          senderLink ? `<span><b>Tx sender:</b> ${senderLink}</span>` : "",
        ].filter(Boolean).join("")
      : gas.status === "not-payer"
        ? `${payerLink ? `<span><b>Gas paid by:</b> ${payerLink}</span>` : ""}<span>This wallet was involved in Dolomite events but was not the transaction sender.</span>`
        : `<span><b>Gas evidence:</b> ${escapeHtml(gas.error || gas.status || "pending")}</span>`;
    return `
      <tr class="detail-row">
        <td colspan="${HISTORY_TABLE_COLSPAN}">
          <div class="detail-box">
            <div class="detail-grid">
              <div>
                <div class="detail-title">What happened</div>
                <div class="event-list">
                  ${displayEvents.map(event => `
                    <div class="event-row">
                      <div class="event-action-stack">
                        <strong>${escapeHtml(cleanReportActionLabel(event))}</strong>
                        <span class="event-flow">${escapeHtml(detailEventFlowLabel(event))}</span>
                        ${eventTransactionLinkHtml(row, true)}
                      </div>
                      ${eventMetaBlockHtml(row, event)}
                    </div>
                  `).join("")}
                </div>
              </div>
              <div>
                <div class="detail-title">Gas fee</div>
                <div class="gas-proof">${gasProof}</div>
              </div>
              <div class="review-note-box">
                <label>Review note</label>
                <textarea data-review-note="${escapeAttr(row.key)}" spellcheck="true" placeholder="Optional note for your accountant or tax tool export">${escapeHtml(reviewNoteForRow(row))}</textarea>
                <small>Included in exports; saved in this browser when local storage is available.</small>
              </div>
            </div>
          </div>
        </td>
      </tr>`;
  }

  function renderLoadingPanel() {
    if (!els.loadingPanel) return;
    const phase = state.loadingPhase || "idle";
    const visible = !!state.address && !["idle", "done"].includes(phase);
    syncLoadingTicker(visible && !["done", "error", "idle"].includes(phase));
    els.loadingPanel.hidden = !visible;
    els.loadingPanel.classList.toggle("visible", visible);
    els.loadingPanel.classList.toggle("done", visible && phase === "done");
    els.loadingPanel.classList.toggle("warn", visible && phase === "error");
    if (!visible) return;
    const progress = loadingProgressPercent();
    const copy = loadingCopyForPhase(phase);
    els.loadingTitle.textContent = copy.title;
    els.loadingSub.textContent = copy.sub;
    if (els.loadingPercent) els.loadingPercent.textContent = `${progress}%`;
    if (els.loadingClock) els.loadingClock.textContent = loadingClockText(progress);
    else if (els.loadingEta) els.loadingEta.textContent = loadingClockText(progress);
    if (els.loadingEta) els.loadingEta.setAttribute("aria-label", `${progress}% loaded, ${loadingClockText(progress)}`);
    els.loadingBar.style.width = `${progress}%`;
    els.loadingBar.parentElement?.setAttribute("aria-valuenow", String(progress));
    setLoadingStep(els.loadingStepSubgraphs, stepStateForPhase("subgraphs", phase));
    setLoadingStep(els.loadingStepReceipts, stepStateForPhase("receipts", phase));
    setLoadingStep(els.loadingStepEvidence, stepStateForPhase("earn", phase));
    setLoadingStep(els.loadingStepReports, stepStateForPhase("reports", phase));
  }

  function renderReportFiles() {
    if (!els.reportStatus) return;
    const rows = state.filteredRows;
    const earnEntries = earnTaxEntriesForCurrentView();
    const readiness = reportExportReadiness(rows, earnEntries);
    els.reportStatus.textContent = reportStatusLabel(readiness);
    if (els.reportDetail) els.reportDetail.textContent = reportStatusDetail(readiness);
    if (els.reportMenu) els.reportMenu.dataset.reportState = reportStateName(readiness);
    const progress = reportProgressPercent(readiness);
    if (els.reportProgress) els.reportProgress.hidden = progress <= 0 || progress >= 100;
    if (els.reportProgressBar) els.reportProgressBar.style.width = `${progress}%`;
    els.taxExport.disabled = !readiness.canFullReport;
    els.reportJson.disabled = !readiness.canFullReport;
    els.reportPrint.disabled = !readiness.canFullReport;
    [els.taxExport, els.reportJson, els.reportPrint].forEach(button => {
      if (!button) return;
      const title = readiness.canFullReport ? "Download ready." : reportStatusDetail(readiness);
      button.title = title;
      button.setAttribute("aria-disabled", readiness.canFullReport ? "false" : "true");
    });
  }

  function earnSummaryAmountLabel(item) {
    if (Number(item.earnAmount || 0) !== 0) return formatSignedAmount(item.earnAmount, item.symbol);
    if (Number(item.earnReviewAmount || 0) !== 0) return "Review only";
    return formatSignedAmount(0, item.symbol);
  }

  function earnSummaryUsdLabel(item) {
    const verifiedUsd = Number(item.earnUsd || 0);
    const reviewUsd = Number(item.earnReviewUsd || 0);
    if (reviewUsd > 0) return `${formatUsd(verifiedUsd)} verified · ${formatUsd(reviewUsd)} review-only`;
    return formatUsd(verifiedUsd);
  }

  function loadingCopyForPhase(phase) {
    if (phase === "subgraphs") {
      const total = state.chainTotal || Object.keys(CHAINS).length;
      return {
        title: "Scanning Dolomite subgraphs",
        sub: `${state.loadedChains}/${total} chains scanned for ${getBounds(state.year).label}.`,
      };
    }
    if (phase === "receipts") {
      const evidenceText = state.earn?.status === "loading" ? "Candidate evidence is still being prepared." : "Candidate evidence is ready.";
      const total = state.gasTotal || state.rows.length;
      const warningText = compactDataWarningText();
      return {
        title: "Checking gas receipts and prices",
        sub: `${state.gasChecked}/${total} receipts checked. ${evidenceText}${warningText}`,
      };
    }
    if (phase === "done") {
      return {
        title: "Report evidence ready",
        sub: "Exports now include visible filters, gas status, price source and candidate evidence.",
      };
    }
    if (phase === "error") {
      return {
        title: "History TX load needs retry",
        sub: "Some source failed before the report could be completed.",
      };
    }
    return {
      title: "Ready",
      sub: "Enter a wallet to start.",
    };
  }

  function loadingProgressPercent() {
    const phase = state.loadingPhase || "idle";
    if (phase === "done") return 100;
    if (phase === "error") return 100;
    if (phase === "subgraphs") {
      const total = state.chainTotal || Object.keys(CHAINS).length;
      return Math.max(8, Math.min(38, Math.round((state.loadedChains / Math.max(1, total)) * 38)));
    }
    if (phase === "receipts") {
      const total = state.gasTotal || state.rows.length || 1;
      const receiptProgress = Math.min(1, state.gasChecked / Math.max(1, total));
      const earnProgress = state.earn?.status === "loading" ? 0 : 1;
      return Math.min(96, 40 + Math.round(receiptProgress * 46) + Math.round(earnProgress * 10));
    }
    return 0;
  }

  function loadingClockText(progress) {
    const phase = state.loadingPhase || "idle";
    const elapsed = loadingElapsedSeconds();
    if (phase === "done") return `Done in ${formatDuration(elapsed)}`;
    if (phase === "error") return `Stopped after ${formatDuration(elapsed)}`;
    if (!state.loadingStartedAt) return `${loadingEtaText()} left`;
    const eta = loadingEtaText();
    return `${formatDuration(elapsed)} elapsed · ${eta} left`;
  }

  function loadingElapsedSeconds() {
    if (!state.loadingStartedAt) return 0;
    return Math.max(0, Math.round((Date.now() - state.loadingStartedAt) / 1000));
  }

  function formatDuration(seconds) {
    const safe = Math.max(0, Math.round(seconds || 0));
    if (safe < 60) return `${safe}s`;
    const minutes = Math.floor(safe / 60);
    const rest = safe % 60;
    return rest ? `${minutes}m ${rest}s` : `${minutes}m`;
  }

  function syncLoadingTicker(active) {
    if (active && !loadingTicker) {
      loadingTicker = window.setInterval(() => {
        const phase = state.loadingPhase || "idle";
        if (!state.address || ["idle", "done", "error"].includes(phase)) {
          syncLoadingTicker(false);
          return;
        }
        renderLoadingPanel();
      }, 1000);
      return;
    }
    if (!active && loadingTicker) {
      window.clearInterval(loadingTicker);
      loadingTicker = 0;
    }
  }

  function loadingEtaText() {
    const phase = state.loadingPhase || "idle";
    if (phase === "done") return "Ready";
    if (phase === "error") return "Retry";
    if (phase === "subgraphs") {
      const total = state.chainTotal || Object.keys(CHAINS).length;
      const remaining = Math.max(1, total - state.loadedChains);
      return state.loadedChains ? formatEtaRange(remaining * 4, remaining * 12) : "~20-60s";
    }
    if (phase === "receipts") {
      const remaining = Math.max(0, (state.gasTotal || state.rows.length || 0) - state.gasChecked);
      if (!remaining && state.earn?.status === "loading") return "~5-20s";
      if (!remaining) return "~1-5s";
      return formatEtaRange(Math.ceil((remaining * 2) / 4), Math.ceil((remaining * 8) / 4));
    }
    return "~20-60s";
  }

  function formatEtaRange(lowSeconds, highSeconds) {
    const low = Math.max(1, Math.round(lowSeconds));
    const high = Math.max(low, Math.round(highSeconds));
    const formatPart = seconds => seconds >= 90 ? `${Math.round(seconds / 60)}m` : `${seconds}s`;
    return `~${formatPart(low)}-${formatPart(high)}`;
  }

  function stepStateForPhase(step, phase) {
    if (phase === "error") return step === "reports" ? "warn" : "done";
    const order = ["subgraphs", "receipts", "earn", "reports"];
    const stepIndex = order.indexOf(step);
    const activePhase = phase === "done" ? "reports" : phase;
    const phaseIndex = order.indexOf(activePhase);
    if (phase === "done") return "done";
    if (step === "earn" && state.earn?.status !== "loading" && phase === "receipts") return "done";
    if (stepIndex < phaseIndex) return "done";
    if (stepIndex === phaseIndex) return "active";
    return "pending";
  }

  function setLoadingStep(element, stateName) {
    if (!element) return;
    element.classList.toggle("done", stateName === "done");
    element.classList.toggle("active", stateName === "active");
    element.classList.toggle("warn", stateName === "warn");
  }

  function reportExportReadiness(rows = state.filteredRows, earnEntries = earnTaxEntriesForCurrentView()) {
    const gasPending = rows.some(row => row.gas?.status === "pending");
    const gasSkippedFast = rows.some(row => row.gas?.status === FAST_GAS_STATUS);
    const earnPending = state.earn?.status === "loading";
    const activeWarnings = activeDataWarnings();
    const dataWarnings = activeWarnings.length;
    const hasRows = rows.length > 0;
    const hasReportRows = hasRows || earnEntries.length > 0;
    const blocked = state.loading || state.filtersDirty || gasPending || gasSkippedFast || earnPending || dataWarnings > 0;
    const receiptTotal = state.gasTotal || rows.length || 0;
    const receiptChecked = Math.min(receiptTotal || rows.length, state.gasChecked || rows.filter(row => row.gas?.status && row.gas.status !== "pending").length);
    return {
      gasPending,
      gasSkippedFast,
      earnPending,
      dataWarnings,
      activeWarnings,
      filtersDirty: state.filtersDirty,
      hasRows,
      hasReportRows,
      receiptChecked,
      receiptTotal,
      canFullReport: hasReportRows && !blocked,
    };
  }

  function reportStatusLabel(readiness) {
    if (!state.address) return "Awaiting wallet";
    if (readiness.filtersDirty) return "Load required";
    if (state.loading) return "Scanning";
    if (readiness.gasPending || readiness.earnPending) return "Completing evidence";
    if (readiness.gasSkippedFast) return "Incomplete gas";
    if (readiness.dataWarnings) return "Incomplete data";
    if (!readiness.hasReportRows) return "No rows";
    return "Ready";
  }

  function reportStatusDetail(readiness) {
    if (!state.address) return "Load a wallet to generate CSV, JSON and print files.";
    if (readiness.filtersDirty) return "Filters changed. Click Load history before downloading.";
    if (state.loading) {
      return `${loadingProgressPercent()}% loaded. ${loadingEtaText()} left.`;
    }
    if (readiness.gasPending) {
      const checked = readiness.receiptChecked || 0;
      const total = readiness.receiptTotal || 0;
      const receiptText = total ? `${checked}/${total} receipts` : "Receipts";
      return `${receiptText} checked. ${loadingEtaText()} left.`;
    }
    if (readiness.earnPending) return `EARN evidence is still loading. ${loadingEtaText()} left.`;
    if (readiness.gasSkippedFast) return "Legacy fast-mode rows need full gas evidence. Reload and wait for receipts.";
    if (readiness.dataWarnings) {
      const first = readiness.activeWarnings?.[0] || "Source data warning.";
      const more = readiness.dataWarnings > 1 ? ` +${readiness.dataWarnings - 1} more.` : "";
      return `${first}${more}`;
    }
    if (!readiness.hasReportRows) return "No export rows match the current filters.";
    return "CSV, JSON and print are ready to download.";
  }

  function reportProgressPercent(readiness) {
    if (!state.address) return 0;
    if (readiness.canFullReport) return 100;
    if (state.loading) return loadingProgressPercent();
    if (readiness.gasPending) {
      const total = readiness.receiptTotal || state.gasTotal || 0;
      if (!total) return 45;
      return Math.max(8, Math.min(96, Math.round(((readiness.receiptChecked || 0) / Math.max(1, total)) * 100)));
    }
    if (readiness.earnPending) return 92;
    return 0;
  }

  function reportStateName(readiness) {
    if (!state.address) return "idle";
    if (readiness.canFullReport) return "ready";
    if (state.loading || readiness.gasPending || readiness.earnPending) return "working";
    if (readiness.filtersDirty || readiness.gasSkippedFast || readiness.dataWarnings) return "blocked";
    return "idle";
  }

  function reportCompletenessForRows(rows, earnEntries = []) {
    const receiptChecked = rows.filter(row => row.gas && row.gas.status && row.gas.status !== "pending").length;
    const eventCount = rows.reduce((sum, row) => sum + row.events.length, 0);
    const pricedEvents = rows.reduce((sum, row) => {
      return sum + row.events.filter(event => Number.isFinite(Number(event.usd)) && Number(event.usd) !== 0).length;
    }, 0);
    const pricedEarn = earnEntries.filter(entry => Number.isFinite(Number(entry.usd)) && Number(entry.usd) !== 0).length;
    const exportRows = eventCount + earnEntries.length;
    return {
      receiptChecked,
      eventCount,
      pricedEvents,
      pricedEarn,
      exportRows,
      receiptLabel: rows.length ? `${receiptChecked}/${rows.length}` : "-",
      priceLabel: eventCount || earnEntries.length ? `${pricedEvents + pricedEarn}/${eventCount + earnEntries.length}` : "-",
      earnLabel: state.earn?.status === "loading" ? "loading" : earnEntries.length.toLocaleString(),
      exportRowsLabel: exportRows ? exportRows.toLocaleString() : "-",
    };
  }

  function assetActivitySummaryForRows(rows, earnEntries = []) {
    const bySymbol = new Map();
    const getEntry = symbol => {
      const cleanSymbol = String(symbol || "").trim() || "Unknown";
      if (!bySymbol.has(cleanSymbol)) {
        bySymbol.set(cleanSymbol, {
          symbol: cleanSymbol,
          intoAmount: 0,
          intoUsd: 0,
          outAmount: 0,
          outUsd: 0,
          routeInAmount: 0,
          routeOutAmount: 0,
          routeUsd: 0,
          earnAmount: 0,
          earnUsd: 0,
          earnReviewAmount: 0,
          earnReviewUsd: 0,
          eventCount: 0,
          reviewCount: 0,
          chains: new Set(),
        });
      }
      return bySymbol.get(cleanSymbol);
    };

    rows.forEach(row => {
      row.events.forEach(event => {
        const profile = taxProfileForEvent(event);
        const group = activityGroupForEvent(event);
        const needsReview = !!reviewReasonForTaxProfile(profile);
        profile.legs.forEach(leg => {
          const symbol = String(leg.symbol || "").trim();
          if (!symbol) return;
          const entry = getEntry(symbol);
          const amount = Math.abs(decimalToNumber(leg.amount));
          const usd = Math.abs(decimalToNumber(leg.usd));
          if (group === "dolomite_in" && leg.direction === "out") {
            entry.intoAmount += amount;
            entry.intoUsd += usd;
          } else if (group === "dolomite_out" && leg.direction === "in") {
            entry.outAmount += amount;
            entry.outUsd += usd;
          } else if (leg.direction === "in") {
            entry.routeInAmount += amount;
            entry.routeUsd += usd;
          } else if (leg.direction === "out") {
            entry.routeOutAmount += amount;
            entry.routeUsd += usd;
          }
          entry.eventCount += 1;
          if (needsReview) entry.reviewCount += 1;
          entry.chains.add(row.chainKey);
        });
      });
    });

    earnEntries.forEach(entry => {
      const usd = Math.abs(Number.isFinite(Number(entry.usd)) ? Number(entry.usd) : 0);
      const verifiedEarn = isVerifiedEarnEntry(entry);
      if (entry.assetInSymbol && entry.assetInAmount) {
        const row = getEntry(entry.assetInSymbol);
        if (verifiedEarn) {
          row.earnAmount += Math.abs(decimalToNumber(entry.assetInAmount));
          row.earnUsd += usd;
        } else {
          row.earnReviewAmount += Math.abs(decimalToNumber(entry.assetInAmount));
          row.earnReviewUsd += usd;
        }
        row.eventCount += 1;
        if (!verifiedEarn) row.reviewCount += 1;
        row.chains.add(entry.chainKey);
      }
      if (entry.assetOutSymbol && entry.assetOutAmount) {
        const row = getEntry(entry.assetOutSymbol);
        if (verifiedEarn) {
          row.earnAmount -= Math.abs(decimalToNumber(entry.assetOutAmount));
          row.earnUsd += usd;
        } else {
          row.earnReviewAmount -= Math.abs(decimalToNumber(entry.assetOutAmount));
          row.earnReviewUsd += usd;
        }
        row.eventCount += 1;
        row.reviewCount += 1;
        row.chains.add(entry.chainKey);
      }
    });

    return Array.from(bySymbol.values())
      .map(entry => {
        const routeNetAmount = entry.routeInAmount - entry.routeOutAmount;
        const netAmount = entry.outAmount - entry.intoAmount + routeNetAmount + entry.earnAmount;
        return {
          ...entry,
          chainCount: entry.chains.size,
          chains: Array.from(entry.chains),
          routeNetAmount,
          netAmount,
          totalUsd: entry.intoUsd + entry.outUsd + entry.routeUsd + entry.earnUsd + entry.earnReviewUsd,
        };
      })
      .sort((a, b) => b.totalUsd - a.totalUsd || b.eventCount - a.eventCount || a.symbol.localeCompare(b.symbol));
  }

  function isVerifiedEarnEntry(entry) {
    return entry?.source === "earn-verified-ledger"
      && entry.trusted === true
      && entry.reviewFlag === "income_candidate"
      && !String(entry.reviewReason || "").trim()
      && !entry.claimProofStatus;
  }

  function positionLifecycleForRows(rows, earnEntries = []) {
    const byPosition = new Map();
    const getPosition = (key, chainKey, accountLabel, sourceType = "tx") => {
      if (!byPosition.has(key)) {
        byPosition.set(key, {
          key,
          chainKey,
          chain: CHAINS[chainKey]?.name || chainKey,
          accountLabel,
          sourceType,
          firstTimestamp: 0,
          lastTimestamp: 0,
          txHashes: new Set(),
          assets: new Set(),
          eventCount: 0,
          reviewCount: 0,
          intoUsd: 0,
          outUsd: 0,
          routeUsd: 0,
          earnUsd: 0,
          sourceEntities: new Set(),
        });
      }
      return byPosition.get(key);
    };

    rows.forEach(row => {
      row.events.forEach(event => {
        const account = event.account !== undefined && event.account !== null && event.account !== "" ? event.account : "external";
        const accountLabel = account === "external" ? "External / AMM" : `Account ${account}`;
        const position = getPosition(`${row.chainKey}:${account}`, row.chainKey, accountLabel);
        const profile = taxProfileForEvent(event);
        const group = activityGroupForEvent(event);
        position.firstTimestamp = position.firstTimestamp ? Math.min(position.firstTimestamp, row.timestamp) : row.timestamp;
        position.lastTimestamp = Math.max(position.lastTimestamp, row.timestamp || 0);
        position.txHashes.add(row.txHash);
        position.eventCount += 1;
        if (event.sourceEntity) position.sourceEntities.add(event.sourceEntity);
        if (reviewReasonForTaxProfile(profile)) position.reviewCount += 1;
        if (event.asset) {
          String(event.asset).split("/").map(part => part.trim()).filter(Boolean).forEach(asset => position.assets.add(asset));
        }
        profile.legs.forEach(leg => {
          if (leg.symbol) position.assets.add(leg.symbol);
        });
        const usd = Math.abs(Number(event.usd || 0));
        if (group === "dolomite_in") position.intoUsd += usd;
        else if (group === "dolomite_out") position.outUsd += usd;
        else position.routeUsd += usd;
      });
    });

    earnEntries.forEach(entry => {
      const symbol = entry.assetInSymbol || entry.assetOutSymbol || entry.marketId || "EARN";
      const key = `${entry.chainKey}:earn:${entry.marketId || symbol}`;
      const position = getPosition(key, entry.chainKey, entry.marketId ? `EARN market ${entry.marketId}` : "EARN rewards", "earn");
      position.firstTimestamp = position.firstTimestamp ? Math.min(position.firstTimestamp, entry.timestamp) : entry.timestamp;
      position.lastTimestamp = Math.max(position.lastTimestamp, entry.timestamp || 0);
      position.assets.add(symbol);
      position.eventCount += 1;
      if (entry.source) position.sourceEntities.add(entry.source);
      position.earnUsd += Math.abs(Number.isFinite(Number(entry.usd)) ? Number(entry.usd) : 0);
      if (entry.reviewFlag === "needs_review") position.reviewCount += 1;
    });

    return Array.from(byPosition.values())
      .map(entry => {
        const assets = Array.from(entry.assets).filter(Boolean).sort();
        return {
          ...entry,
          txCount: entry.txHashes.size,
          txHashes: Array.from(entry.txHashes),
          sourceEntities: Array.from(entry.sourceEntities || []),
          sourceEntityLabel: summarizeUniqueCsvLabels(Array.from(entry.sourceEntities || []), 5),
          assets,
          assetsLabel: assets.length ? assets.slice(0, 4).join(", ") + (assets.length > 4 ? ` +${assets.length - 4}` : "") : "No asset symbol",
          totalUsd: entry.intoUsd + entry.outUsd + entry.routeUsd + entry.earnUsd,
        };
      })
      .sort((a, b) => b.lastTimestamp - a.lastTimestamp || b.totalUsd - a.totalUsd || a.key.localeCompare(b.key));
  }

  function lifecycleStatusForItem(item) {
    const evidenceLabel = item.sourceType === "earn"
      ? `${item.eventCount.toLocaleString()} EARN row${item.eventCount === 1 ? "" : "s"}`
      : `${item.eventCount.toLocaleString()} event${item.eventCount === 1 ? "" : "s"} · ${item.txCount.toLocaleString()} tx`;
    if (item.sourceType === "earn") {
      return item.reviewCount
        ? { label: "EARN review", sub: `${evidenceLabel} · timing needs review`, tone: "warn" }
        : { label: "EARN evidence", sub: `${evidenceLabel} · snapshot/ledger-derived`, tone: "good" };
    }
    if (item.reviewCount) {
      return { label: "Review needed", sub: `${evidenceLabel} · ${item.reviewCount.toLocaleString()} flagged`, tone: "warn" };
    }
    return { label: "Lifecycle evidence", sub: `${evidenceLabel} · open/closed not inferred`, tone: "good" };
  }

  function lifecycleFlowLabel(item) {
    const parts = [];
    if (Number(item.intoUsd || 0) > 0) parts.push(`${formatUsd(item.intoUsd)} in`);
    if (Number(item.outUsd || 0) > 0) parts.push(`${formatUsd(item.outUsd)} out`);
    if (Number(item.routeUsd || 0) > 0) parts.push(`${formatUsd(item.routeUsd)} routes`);
    if (Number(item.earnUsd || 0) > 0) parts.push(`${formatUsd(item.earnUsd)} EARN`);
    return parts.length ? parts.join(" / ") : "No priced flow";
  }

  function reviewQueueForRows(rows, earnEntries = []) {
    const reasons = new Map();
    const add = (key, label, example, tone = "warn") => {
      const id = String(key || label || "review");
      const current = reasons.get(id) || { label, example, tone, count: 0 };
      current.count += 1;
      if (!current.example && example) current.example = example;
      reasons.set(id, current);
    };
    rows.forEach(row => {
      row.events.forEach(event => {
        const profile = taxProfileForEvent(event);
        const reason = reviewReasonForTaxProfile(profile);
        if (reason) {
          add(`event:${reason}`, reviewReasonLabel(reason), `${CHAINS[row.chainKey].name} ${shortHash(row.txHash)}: ${event.label || ACTION_LABELS[event.action] || event.action}`);
        }
      });
      if (row.gas?.status === "pending") add("gas:pending", "Gas pending", `${CHAINS[row.chainKey].name} ${shortHash(row.txHash)} still waiting for receipt.`, "muted");
      if (row.gas?.status && !["ok", "not-payer", "pending"].includes(row.gas.status)) {
        add(`gas:${row.gas.status}`, "Gas evidence gap", `${CHAINS[row.chainKey].name} ${shortHash(row.txHash)} gas status: ${row.gas.status}.`);
      }
      if (reviewNoteForRow(row)) {
        add("user:note", "User review notes", `${CHAINS[row.chainKey].name} ${shortHash(row.txHash)} has a local note.`, "good");
      }
    });
    earnEntries.forEach(entry => {
      String(entry.reviewReason || "")
        .split(";")
        .map(reason => reason.trim())
        .filter(Boolean)
        .forEach(reason => add(`earn:${reason}`, reviewReasonLabel(reason), `${entry.chainName || "EARN"} ${entry.assetInSymbol || entry.assetOutSymbol || entry.marketId || "entry"}.`));
      if (entry.claimProofStatus) {
        add(`claim:${entry.claimProofStatus}`, reviewReasonLabel(entry.claimProofStatus), `${entry.chainName || "EARN"} reward/yield claim proof is ${entry.claimProofStatus}.`);
      }
    });
    const historyWarnings = activeHistoryWarnings();
    const earnWarnings = activeEarnWarnings();
    if (historyWarnings.length) add("warnings:history", "History source warnings", historyWarnings[0]);
    if (earnWarnings.length) add("warnings:earn", "EARN source warnings", earnWarnings[0]);
    return Array.from(reasons.values())
      .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
  }

  function reportMethodology() {
    return {
      status: "Dolomite-only evidence",
      scopeTitle: "Dolomite only",
      scopeSub: "External cost basis excluded",
      priceTitle: "Historical USD",
      priceSub: "Gas at tx timestamp; candidate rows at snapshot timestamp",
      balanceTitle: "Movement only",
      balanceSub: "Opening/closing snapshots need a separate data source",
      balanceSnapshotStatus: "not_included",
      balanceSnapshotReason: "This report includes Dolomite event movement only. It does not infer Jan 1 / Dec 31 balances without a static historical balance snapshot source.",
      taxToolSchema: "generic_mappable_csv",
      statementKind: "Dolomite History Report",
    };
  }

  function earnSummaryForCurrentView() {
    if (state.earn?.status === "loading") {
      return {
        statusLabel: "Checking EARN...",
        yieldCount: 0,
        yieldSub: "Loading verified ledger",
        trustedYield: 0,
        trustedSub: "Waiting for ledger status",
        rewardCount: 0,
        rewardSub: "Loading reward cache",
        coverage: "-",
        coverageSub: "Loading",
      };
    }
    if (state.earn?.status === "error") {
      return {
        statusLabel: "EARN unavailable",
        yieldCount: 0,
        yieldSub: "Raw history export remains available",
        trustedYield: 0,
        trustedSub: "No reusable EARN proof",
        rewardCount: 0,
        rewardSub: "Reward cache unavailable",
        coverage: "-",
        coverageSub: state.earn.warnings.slice(0, 1).join("") || "EARN fetch failed",
      };
    }
    if (!actionFilterAllSelected()) {
      const ledgerChains = Object.keys(state.earn?.ledgers || {}).filter(chainKey => state.selectedChains.has(chainKey)).length;
      return {
        statusLabel: ledgerChains ? "EARN filtered out" : "Raw ledger only",
        yieldCount: 0,
        yieldSub: "Use All actions to include EARN activity",
        trustedYield: 0,
        trustedSub: "Hidden by action filter",
        rewardCount: 0,
        rewardSub: "Hidden by action filter",
        coverage: ledgerChains ? `${ledgerChains}/${state.selectedChains.size}` : "-",
        coverageSub: "Action filter excludes EARN summary rows",
      };
    }
    const entries = earnTaxEntriesForCurrentView();
    const yieldEntries = entries.filter(entry => entry.source === "earn-verified-ledger");
    const rewardEntries = entries.filter(entry => entry.source === "earn-merkl-rewards");
    const trustedYield = yieldEntries.filter(entry => entry.trusted).length;
    const pricedYield = yieldEntries.filter(entry => Number.isFinite(Number(entry.usd)) && Number(entry.usd) !== 0).length;
    const yearSeriesYield = yieldEntries.filter(entry => entry.earnPeriodSource === "earn-snapshot-series").length;
    const reviewReason = earnReviewReasonSummary(yieldEntries);
    const rewardUsd = rewardEntries.reduce((sum, entry) => sum + (Number.isFinite(Number(entry.usd)) ? Math.abs(Number(entry.usd)) : 0), 0);
    const ledgerChainKeys = Object.keys(state.earn?.ledgers || {}).filter(chainKey => state.selectedChains.has(chainKey));
    const rewardChainKeys = Object.keys(state.earn?.rewards || {}).filter(chainKey => state.selectedChains.has(chainKey));
    const coverageChainCount = new Set([...ledgerChainKeys, ...rewardChainKeys]).size;
    const latestYearSnapshot = yieldEntries
      .map(entry => entry.earnSnapshotDate || "")
      .filter(Boolean)
      .sort()
      .pop();
    const latestLedgerSnapshot = Object.values(state.earn?.ledgers || {})
      .map(ledger => ledger.snapshotDate || "")
      .filter(Boolean)
      .sort()
      .pop();
    const hasAnyEarn = coverageChainCount > 0;
    return {
      statusLabel: hasAnyEarn ? "EARN enriched" : "Raw ledger only",
      yieldCount: yieldEntries.length,
      yieldSub: yieldEntries.length ? `${yearSeriesYield}/${yieldEntries.length} year-series · ${pricedYield}/${yieldEntries.length} priced` : "No year-matched EARN yield",
      trustedYield,
      trustedSub: yieldEntries.length ? (reviewReason || "Verified/inferred entries only") : "No reusable EARN proof",
      rewardCount: rewardEntries.length,
      rewardSub: rewardEntries.length ? `${formatUsd(rewardUsd)} priced rewards` : "No year-matched cached rewards",
      coverage: hasAnyEarn ? `${coverageChainCount}/${state.selectedChains.size}` : "-",
      coverageSub: latestYearSnapshot
        ? `Latest EARN snapshot ${latestYearSnapshot}`
        : latestLedgerSnapshot
          ? `Latest ledger ${latestLedgerSnapshot}`
          : (state.earn?.warnings?.[0] || "Awaiting wallet"),
    };
  }

  function earnReviewReasonSummary(entries) {
    const reasons = new Set();
    entries.forEach(entry => {
      if (entry.trusted) return;
      String(entry.reviewReason || "")
        .split(";")
        .map(reason => reason.trim())
        .filter(Boolean)
        .forEach(reason => reasons.add(reviewReasonLabel(reason).replace(/^EARN /, "").toLowerCase()));
      if (!reasons.size) reasons.add("review-only");
    });
    const text = Array.from(reasons).slice(0, 3).join(" · ");
    return text ? `Review: ${text}` : "";
  }

  function reviewSummaryForCurrentView() {
    if (state.loading) {
      return {
        status: "Checking...",
        items: [{ label: "Loading evidence", tone: "muted" }],
      };
    }
    if (!state.address) {
      return {
        status: "Awaiting wallet",
        items: [{ label: "No lookup yet", tone: "muted" }],
      };
    }
    const reasons = new Map();
    const addReason = (key, label, tone = "warn") => {
      const id = String(key || label || "review");
      const current = reasons.get(id) || { label, tone, count: 0 };
      current.count += 1;
      reasons.set(id, current);
    };

    state.filteredRows.forEach(row => {
      row.events.forEach(event => {
        const profile = taxProfileForEvent(event);
        const reason = reviewReasonForTaxProfile(profile);
        if (reason) addReason(`event:${reason}`, reviewReasonLabel(reason));
      });
      if (row.gas?.status === "pending") addReason("gas:pending", "Gas pending");
      if (row.gas?.status && !["ok", "not-payer", "pending"].includes(row.gas.status)) {
        addReason(`gas:${row.gas.status}`, "Gas evidence gap");
      }
    });

    const includeEarnReview = actionFilterAllSelected();
    earnTaxEntriesForCurrentView().forEach(entry => {
      String(entry.reviewReason || "")
        .split(";")
        .map(reason => reason.trim())
        .filter(Boolean)
        .forEach(reason => addReason(`earn:${reason}`, reviewReasonLabel(reason)));
      if (entry.claimProofStatus) addReason(`claim:${entry.claimProofStatus}`, reviewReasonLabel(entry.claimProofStatus));
      if (String(entry.priceSource || "").toLowerCase().includes("unavailable")) addReason("price:missing", "Price missing");
    });

    if (activeHistoryWarnings().length) addReason("warnings:history", "Data warnings");
    if (includeEarnReview && state.earn?.status === "loading") addReason("earn:loading", "EARN pending", "muted");
    if (includeEarnReview && activeEarnWarnings().length) addReason("warnings:earn", "EARN warnings");

    const rawItems = Array.from(reasons.values())
      .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
    const items = rawItems.slice(0, 6).map(item => ({
      label: item.count > 1 ? `${item.label} (${item.count})` : item.label,
      tone: item.tone,
    }));
    if (rawItems.length > items.length) {
      items.push({ label: `+${rawItems.length - items.length} more`, tone: "muted" });
    }
    if (!items.length) {
      return {
        status: state.filteredRows.length ? "No review flags" : "No rows loaded",
        items: [{ label: state.filteredRows.length ? "Export evidence clean" : "No rows match filters", tone: "good" }],
      };
    }
    const signalCount = rawItems.reduce((sum, item) => sum + item.count, 0);
    return {
      status: `${signalCount} review signal${signalCount === 1 ? "" : "s"}`,
      items,
    };
  }

  function reviewReasonForTaxProfile(profile) {
    if (!profile || typeof profile !== "object") return "unknown_tax_category";
    const explicitReason = String(profile.reviewReason || "").trim();
    if (explicitReason) return explicitReason.split(";").map(item => item.trim()).filter(Boolean)[0] || explicitReason;
    if (profile.reviewFlag === "possible_taxable_disposal") return "possible_taxable_disposal";
    if (profile.reviewFlag === "needs_review" || profile.taxCategory === "unknown") {
      return profile.taxCategory === "unknown" ? "unknown_tax_category" : profile.taxCategory;
    }
    return "";
  }

  function reviewReasonLabel(reason) {
    const labels = {
      possible_taxable_disposal: "Possible disposal",
      unknown_tax_category: "Unknown tax category",
      zap_route_review: "Zap route review",
      amm_swap_review: "AMM trade review",
      amm_liquidity_review: "AMM liquidity review",
      odolo_vedolo_pairing_review: "oDOLO/veDOLO pairing review",
      odolo_vedolo_exercise_review: "oDOLO/veDOLO exercise review",
      odolo_vedolo_transfer_review: "oDOLO/veDOLO transfer review",
      async_position_timing: "Async timing review",
      liquidation_forced_settlement: "Forced liquidation review",
      vaporization_debt_absorption: "Debt settlement review",
      liquidation: "Liquidation review",
      vaporization: "Debt settlement review",
      defi_position_change: "DeFi position change",
      liquidity_deposit: "Liquidity deposit review",
      liquidity_withdrawal: "Liquidity withdrawal review",
      earn_yield_inferred: "EARN inferred yield",
      inferred_yield_review: "EARN inferred yield",
      earn_yield_unverified: "EARN unverified yield",
      earn_negative_yield_candidate: "EARN negative yield",
      negative_yield_review: "EARN negative yield",
      stale_ledger: "EARN stale ledger",
      status_mismatch: "EARN verification incomplete",
      coverage_incomplete: "EARN coverage incomplete",
      snapshot_fallback: "EARN snapshot fallback",
      untrusted_earn_method: "EARN untrusted method",
      price_unavailable: "Price unavailable",
      reward_period_unverified: "Reward period unverified",
      reward_claim_timing_unverified: "Reward claim timing",
      estimated_from_accumulated_minus_unclaimed: "Reward claim estimate",
    };
    const key = String(reason || "").trim();
    if (labels[key]) return labels[key];
    return key
      .replace(/_/g, " ")
      .replace(/\b[a-z]/g, char => char.toUpperCase());
  }

  function syncControls() {
    els.run.disabled = state.loading;
    els.run.classList.toggle("pending", state.filtersDirty && !state.loading);
    els.year.disabled = state.loading;
    els.action.disabled = state.loading;
    [els.yearButton, els.actionButton, els.networkButton].forEach(button => {
      if (button) button.disabled = state.loading;
    });
    syncYearDropdown();
    syncDateRangeControls();
    syncActionDropdown();
    syncNetworkDropdown();
  }

  function syncYearDropdown() {
    if (!els.yearMenu) return;
    const selected = Array.from(els.year.options).find(option => option.value === "custom");
    state.year = "custom";
    els.year.value = state.year;
    els.yearLabel.textContent = dateFilterButtonLabel(selected);
    els.yearCount.textContent = "Range";
    els.yearButton.classList.toggle("filtered", !dateRangeIsDefault());
    els.yearButton.classList.add("custom-range");
  }

  function dateFilterButtonLabel(selected) {
    if (validDateInput(state.dateFrom) && validDateInput(state.dateTo)) {
      return `${formatDateInputShort(state.dateFrom)}-${formatDateInputShort(state.dateTo)}`;
    }
    return selected?.textContent || "Custom range";
  }

  function formatDateInputShort(value) {
    if (!validDateInput(value)) return "Date";
    const [year, month, day] = String(value).split("-").map(Number);
    return new Intl.DateTimeFormat("en-GB", {
      day: "numeric",
      month: "short",
    }).format(new Date(Date.UTC(year, month - 1, day)));
  }

  function syncActionDropdown() {
    if (!els.actionMenu) return;
    const allActions = actionFilterKeys();
    const selectedActions = selectedActionKeys();
    const selectedSet = new Set(selectedActions);
    const allSelected = selectedActions.length === allActions.length;
    state.action = allSelected ? "all" : selectedActions.join(",");
    els.action.value = allSelected ? "all" : (selectedActions[0] || "all");
    els.actionLabel.textContent = actionFilterLabel(selectedActions, allSelected);
    els.actionCount.textContent = `${selectedActions.length}/${allActions.length}`;
    els.actionButton.classList.toggle("filtered", !allSelected);
    els.actionMenu.querySelectorAll("[data-history-action]").forEach(option => {
      const action = normalizeActionFilter(option.dataset.historyAction);
      const active = action === "all" ? allSelected : (!allSelected && selectedSet.has(action));
      option.classList.toggle("active", active);
      option.setAttribute("aria-selected", active ? "true" : "false");
    });
  }

  function actionFilterKeys() {
    const seen = new Set();
    return Array.from(els.action?.options || [])
      .map(option => normalizeActionFilter(option.value))
      .filter(action => action && action !== "all")
      .filter(action => {
        if (seen.has(action)) return false;
        seen.add(action);
        return true;
      });
  }

  function selectAllActions() {
    state.selectedActions = new Set(actionFilterKeys());
    state.action = "all";
    if (els.action) els.action.value = "all";
  }

  function setSelectedActionsFromValues(values) {
    const allActions = actionFilterKeys();
    const valid = new Set(allActions);
    const selected = (values || [])
      .flatMap(value => String(value || "").split(","))
      .map(value => normalizeActionFilter(value.trim()))
      .filter(action => valid.has(action));
    if (!selected.length || selected.length === allActions.length) {
      selectAllActions();
      return;
    }
    state.selectedActions = new Set(selected);
    state.action = selected.join(",");
    if (els.action) els.action.value = selected[0] || "all";
  }

  function selectedActionKeys() {
    const allActions = actionFilterKeys();
    if (!state.selectedActions || !state.selectedActions.size) {
      state.selectedActions = new Set(allActions);
    }
    const valid = new Set(allActions);
    const selected = allActions.filter(action => state.selectedActions.has(action) && valid.has(action));
    if (!selected.length) {
      state.selectedActions = new Set(allActions);
      return allActions;
    }
    if (selected.length !== state.selectedActions.size) {
      state.selectedActions = new Set(selected);
    }
    return selected;
  }

  function actionFilterAllSelected() {
    const allActions = actionFilterKeys();
    return allActions.length > 0 && selectedActionKeys().length === allActions.length;
  }

  function actionFilterLabel(selectedActions = selectedActionKeys(), allSelected = actionFilterAllSelected()) {
    if (allSelected) return "All actions";
    if (selectedActions.length === 1) {
      const option = Array.from(els.action.options).find(item => normalizeActionFilter(item.value) === selectedActions[0]);
      return option?.textContent || ACTION_LABELS[selectedActions[0]] || selectedActions[0];
    }
    return `${selectedActions.length} actions`;
  }

  function actionFilterParam() {
    return actionFilterAllSelected() ? "" : selectedActionKeys().join(",");
  }

  function syncNetworkDropdown() {
    if (!els.networkMenu) return;
    const selectableChains = chainFilterKeys();
    const defaultChains = defaultChainKeys();
    const selectedKeys = selectedChainKeys();
    const selectedCount = selectedKeys.length;
    const allSelected = chainSelectionIsDefault();
    els.networkLabel.textContent = networkFilterLabel();
    els.networkCount.textContent = allSelected ? `${defaultChains.length}/${defaultChains.length}` : `${selectedCount}/${selectableChains.length}`;
    els.networkButton.classList.toggle("filtered", !allSelected);
    els.networkIcon.innerHTML = networkButtonIconHtml(allSelected);
    els.networkMenu.querySelectorAll("[data-history-network]").forEach(option => {
      const chain = option.dataset.historyNetwork;
      const active = chain === "all" ? allSelected : !allSelected && state.selectedChains.has(chain);
      option.classList.toggle("active", active);
      option.setAttribute("aria-selected", active ? "true" : "false");
    });
  }

  function networkButtonIconHtml(allSelected) {
    if (!allSelected && state.selectedChains.size === 1) {
      const chainKey = Array.from(state.selectedChains)[0];
      const icon = CHAINS[chainKey]?.icon;
      if (icon) return `<img src="${escapeAttr(icon)}" alt="" onerror="this.style.display='none'">`;
    }
    return globeIconHtml();
  }

  function networkFilterLabel() {
    if (chainSelectionIsDefault()) return "All Chains";
    if (state.selectedChains.size === 1) {
      const chainKey = Array.from(state.selectedChains)[0];
      return chainMenuLabel(chainKey);
    }
    return `${state.selectedChains.size} chains`;
  }

  function warningAppliesToCurrentChains(message) {
    const text = String(message || "");
    const mentionedChains = Object.entries(CHAINS)
      .filter(([, chain]) => text.startsWith(`${chain.name} `) || text.startsWith(`${chain.name}:`) || text.includes(`${chain.name} reward claim`))
      .map(([chainKey]) => chainKey);
    if (!mentionedChains.length) return true;
    const selected = new Set(selectedChainKeys());
    return mentionedChains.some(chainKey => selected.has(chainKey));
  }

  function isRewardClaimWarning(message) {
    return /reward claim/i.test(String(message || ""));
  }

  function reportIncludesClaimData() {
    return actionFilterAllSelected() || selectedActionKeys().includes("claim");
  }

  function reportIncludesEarnData() {
    return actionFilterAllSelected();
  }

  function activeHistoryWarnings() {
    return (state.warnings || [])
      .filter(warningAppliesToCurrentChains)
      .filter(message => reportIncludesClaimData() || !isRewardClaimWarning(message));
  }

  function activeEarnWarnings() {
    if (!reportIncludesEarnData()) return [];
    return (state.earn?.warnings || []).filter(warningAppliesToCurrentChains);
  }

  function activeDataWarnings() {
    return activeHistoryWarnings().concat(activeEarnWarnings());
  }

  function historyWarningCount() {
    return activeDataWarnings().length;
  }

  function compactDataWarningText() {
    const count = historyWarningCount();
    if (!count) return "";
    return ` ${count.toLocaleString()} data warning${count === 1 ? "" : "s"} in review/export.`;
  }

  function historyCompletionStatusMessage(rowCount, visibleRowCount, evidenceEntryCount, finalizeComplete) {
    const safeRows = Number(rowCount || 0);
    const safeVisibleRows = Number(visibleRowCount || 0);
    const parts = [`Loaded ${safeRows.toLocaleString()} tx.`];
    if (!finalizeComplete) {
      parts.push("Evidence continues in the progress panel.");
    } else {
      parts.push("Reports ready.");
    }
    if (safeRows && safeVisibleRows !== safeRows) {
      parts.push(`${safeVisibleRows.toLocaleString()} match current filters.`);
    }
    return parts.join(" ");
  }

  function setStatus(message, tone) {
    els.status.textContent = message;
    els.status.classList.toggle("warn", tone === "warn");
    els.status.classList.toggle("good", tone === "good");
  }

  function exportHistoryReportCsv() {
    const rows = state.filteredRows;
    const earnEntries = earnTaxEntriesForCurrentView();
    if (!ensureFullReportReady(rows, earnEntries, "CSV report")) return;
    downloadCsvFile(
      `dolomite-history-report-${state.address || "wallet"}-${state.year}-${HISTORY_VERSION}.csv`,
      cleanHistoryReportHeaders(),
      cleanHistoryReportCsvRows(rows, earnEntries),
    );
  }

  function cleanHistoryReportHeaders() {
    return [
      "row_type",
      "date_utc",
      "year",
      "network",
      "tx_hash",
      "block",
      "action",
      "asset_flow",
      "usd_value_at_time",
      "gas_asset",
      "gas_amount",
      "gas_usd_at_time",
      "gas_status",
      "review_status",
      "review_reason",
      "user_note",
      "source",
      "source_entity",
      "report_scope",
      "gas_coverage",
      "warnings_count",
      "report_generated_at",
    ];
  }

  function cleanHistoryReportCsvRows(rows, earnEntries) {
    const meta = taxExportMeta(rows, earnEntries);
    return [
      ...rows.map(row => cleanHistoryTransactionCsvRow(row, meta)),
      ...earnEntries.map(entry => cleanHistoryEarnCsvRow(entry, meta)),
    ];
  }

  function cleanHistoryTransactionCsvRow(row, meta) {
    const chain = CHAINS[row.chainKey] || {};
    const gas = row.gas || {};
    const profiles = row.events.map(taxProfileForEvent);
    const usdValue = Number(row.usdVolume || 0);
    const hasWalletGas = !!gas.paidByWallet;
    return [
      "transaction",
      isoDate(row.timestamp),
      row.timestamp ? String(new Date(row.timestamp * 1000).getUTCFullYear()) : "",
      chain.name || row.chainKey,
      row.txHash,
      row.blockNumber,
      cleanTransactionAction(row),
      cleanTransactionAssetFlow(row),
      Number.isFinite(usdValue) && usdValue !== 0 ? decimalForCsv(usdValue) : "",
      hasWalletGas ? gas.nativeSymbol || chain.nativeSymbol || "" : "",
      hasWalletGas ? gas.nativeAmountExact || gas.nativeAmount || "" : "",
      hasWalletGas && Number.isFinite(Number(gas.gasUsd)) ? decimalForCsv(gas.gasUsd) : "",
      gas.status || "",
      cleanHistoryReviewStatus(profiles, gas, reviewNoteForRow(row)),
      cleanHistoryReviewReason(profiles, gas),
      reviewNoteForRow(row),
      cleanHistoryTransactionSource(gas, chain, row),
      cleanHistorySourceEntities(row),
      meta.scope,
      meta.gasCoverage,
      meta.warningsCount,
      meta.generatedAt,
    ];
  }

  function cleanHistoryEarnCsvRow(entry, meta) {
    const usdValue = Number(entry.usd || 0);
    return [
      "earn_candidate",
      isoDate(entry.timestamp),
      entry.timestamp ? String(new Date(entry.timestamp * 1000).getUTCFullYear()) : "",
      entry.chainName || "",
      "",
      "",
      cleanEarnActionLabel(entry),
      cleanEarnAssetFlow(entry),
      Number.isFinite(usdValue) && usdValue !== 0 ? decimalForCsv(usdValue) : "",
      "",
      "",
      "",
      entry.feeStatus || "",
      cleanEarnReviewStatus(entry),
      cleanEarnReviewReason(entry),
      "",
      cleanEarnSourceLabel(entry),
      entry.source || entry.dataSource || "",
      meta.scope,
      meta.gasCoverage,
      meta.warningsCount,
      meta.generatedAt,
    ];
  }

  function cleanTransactionAssetFlow(row) {
    const primary = primaryTransactionEvent(row.events);
    if (primary?.action === "vesting") {
      const vestingFlow = cleanVestingEventsFlow(row);
      if (vestingFlow) return vestingFlow;
    }
    if (primary) {
      const flow = cleanPrimaryEventFlow(primary);
      if (flow) return flow;
    }
    const summaries = row.events
      .map(cleanEventFlowSummary)
      .filter(Boolean);
    return summarizeUniqueCsvLabels(summaries, 4);
  }

  function compactTransactionAssetPreview(row) {
    const primary = primaryTransactionEvent(row.events);
    if (primary?.action === "vesting") {
      return compactVestingEventsPreview(row);
    }
    if (isSwapLikeEvent(primary)) {
      return cleanSwapOutcomeFlow(primary, "table") || cleanTransactionAssetFlow(row);
    }
    const semanticEvent = (row?.events || [])
      .find(event => event?.borrowSemanticConfidence === "borrow_position_lifecycle" && !BORROW_POSITION_LIFECYCLE_ACTIONS.has(event?.action));
    if (semanticEvent) {
      const direction = semanticEvent.action === "withdraw" ? "in" : semanticEvent.action === "deposit" ? "out" : semanticEvent.role;
      const amount = cleanLegGroup(semanticEvent.legs, direction, "table") || cleanFlowFromLegs(semanticEvent.legs, "table");
      if (amount) return amount;
    }
    if (!primary && row?.actions?.has("transfer")) {
      const transfer = (row.events || []).find(event => event?.action === "transfer");
      if (transfer) return compactTransferTableFlow(transfer, "table");
    }
    return cleanTransactionAssetFlow(row);
  }

  function compactTransferTableFlow(event, mode = "table") {
    const amount = cleanLegGroup(event.legs, event.role, mode) || cleanFlowFromLegs(event.legs, mode);
    if (!amount) return "";
    if (semanticActionUsesUnsignedFlow(event.borrowSemanticAction)) return amount;
    if (event.role === "in") return `+${amount}`;
    if (event.role === "out") return `-${amount}`;
    return amount;
  }

  function cleanTransactionAction(row) {
    const primary = primaryTransactionEvent(row.events);
    const semanticLabels = Array.from(row.semanticActions || [])
      .map(action => ACTION_LABELS[action] || action)
      .filter(Boolean);
    if (primary) {
      const labels = [cleanReportActionLabel(primary), ...semanticLabels];
      vestingEventsForRow(row).forEach(event => labels.push(cleanReportActionLabel(event)));
      return summarizeUniqueCsvLabels(labels, 3);
    }
    if (semanticLabels.length) return summarizeUniqueCsvLabels(semanticLabels, 3);
    return summarizeUniqueCsvLabels(
      Array.from(row.actions).map(action => cleanReportActionLabel({ action })),
      3,
    );
  }

  function primaryTransactionEvent(events) {
    const list = Array.isArray(events) ? events : [];
    return list.find(event => event.action === "zap")
      || list.find(event => event.action === "trade" || (event.taxCategory === "swap" && event.action !== "amm"))
      || list.find(event => event.action === "amm" && event.taxCategory === "swap")
      || list.find(event => event.action === "liquidation")
      || list.find(event => event.action === "vaporization")
      || list.find(event => event.action === "asyncDeposit" || event.action === "asyncWithdrawal")
      || list.find(event => event.action === "amm" && event.taxCategory)
      || list.find(event => event.action === "vesting")
      || list.find(event => event.action === "odoloClaim")
      || list.find(event => event.action === "rewardClaim")
      || list.find(event => event.action === "rewardLevelUpdate")
      || null;
  }

  function cleanPrimaryEventFlow(event) {
    if (event.action === "vesting") return cleanVestingEventFlow(event);
    const flow = cleanActionAssetFlow(event);
    if (flow) return flow;
    return cleanEventFlowSummary(event);
  }

  function cleanEventFlowSummary(event) {
    if (event.action === "vesting") return cleanVestingEventFlow(event);
    const flow = cleanActionAssetFlow(event);
    if (flow) return flow;
    return event.label || cleanReportActionLabel(event);
  }

  function cleanVestingEventsFlow(row) {
    return summarizeUniqueCsvLabels(
      vestingEventsForRow(row).map(cleanVestingEventFlow),
      4,
    );
  }

  function cleanActionAssetFlow(event, mode = "report") {
    if (isSwapLikeEvent(event)) {
      const swapFlow = cleanSwapOutcomeFlow(event, mode);
      if (swapFlow) return swapFlow;
    }
    const flow = cleanFlowFromLegs(event.legs, mode);
    if (!flow) return "";
    if (event.action === "deposit") return cleanLegGroup(event.legs, "out", mode) || flow;
    if (event.action === "withdraw") return cleanLegGroup(event.legs, "in", mode) || flow;
    if (event.action === "transfer") {
      const amount = cleanLegGroup(event.legs, event.role, mode) || flow;
      if (semanticActionUsesUnsignedFlow(event.borrowSemanticAction)) return `${cleanReportActionLabel(event)}: ${amount}`;
      return `${event.role === "in" ? "Transfer in" : "Transfer out"}: ${amount}`;
    }
    return `${cleanReportActionLabel(event)}: ${flow}`;
  }

  function semanticActionUsesUnsignedFlow(action) {
    return ["borrow", "openBorrow", "repay", "closeBorrow", "addCollateral", "withdrawCollateral"].includes(action);
  }

  function isSwapLikeEvent(event) {
    return !!event && (event.action === "zap" || event.action === "trade" || event.taxCategory === "swap");
  }

  function cleanSwapOutcomeFlow(event, mode = "report") {
    const outLegs = cleanLegGroup(event?.legs, "out", mode);
    const inLegs = cleanLegGroup(event?.legs, "in", mode);
    const parsed = (!outLegs || !inLegs) ? parseSignedSwapLabel(event?.label, cleanReportActionLabel(event), mode) : null;
    const paid = outLegs || parsed?.paid || "";
    const received = inLegs || parsed?.received || "";
    if (paid && received) {
      return mode === "table" ? `${paid} -> ${received}` : `Paid ${paid} -> Received ${received}`;
    }
    if (received) return `Received ${received}`;
    if (paid) return `Paid ${paid}`;
    return "";
  }

  function parseSignedSwapLabel(label, actionLabel, mode = "report") {
    const stripped = stripDetailActionPrefix(label, actionLabel);
    const match = String(stripped || "").match(/^\s*[-+]?(.+?)\s*\/\s*[-+]?(.+?)\s*$/);
    if (!match) return null;
    return {
      paid: cleanSignedFlowPart(match[1], mode),
      received: cleanSignedFlowPart(match[2], mode),
    };
  }

  function cleanSignedFlowPart(value, mode = "report") {
    const text = String(value || "").trim().replace(/^[+-]\s*/, "");
    return mode === "report" ? text : formatUiFlowPart(text);
  }

  function cleanFlowFromLegs(legs, mode = "report") {
    const outLegs = cleanLegGroup(legs, "out", mode);
    const inLegs = cleanLegGroup(legs, "in", mode);
    if (outLegs && inLegs) return `${outLegs} -> ${inLegs}`;
    if (inLegs) return `Received ${inLegs}`;
    if (outLegs) return `Sent ${outLegs}`;
    return "";
  }

  function cleanLegGroup(legs, direction, mode = "report") {
    return Array.from(new Set((legs || [])
      .filter(leg => leg?.direction === direction)
      .map(leg => cleanLegAmountSymbol(leg, mode))
      .filter(Boolean))).join(" + ");
  }

  function cleanLegAmountSymbol(leg, mode = "report") {
    const amount = cleanAmount(String(leg?.amount ?? "").replace(/^[+-]/, ""));
    const symbol = String(leg?.symbol || "").trim();
    if (!symbol) return "";
    const displayAmount = mode === "report" ? amount : formatUiTokenAmount(amount, symbol);
    return displayAmount && displayAmount !== "0" ? `${displayAmount} ${symbol}` : symbol;
  }

  function cleanReportActionLabel(event) {
    if (event?.borrowSemanticAction) return ACTION_LABELS[event.borrowSemanticAction] || event.borrowSemanticLabel || event.borrowSemanticAction;
    if (event?.action === "zap") return "Zap";
    if (event?.action === "amm") {
      if (event.taxCategory === "swap") return "AMM Trade";
      if (event.taxCategory === "liquidity_deposit") return "Add Liquidity";
      if (event.taxCategory === "liquidity_withdrawal") return "Remove Liquidity";
      return "AMM";
    }
    if (event?.action === "trade" || event?.taxCategory === "swap") return "Trade";
    if (event?.action === "deposit") return "Deposit";
    if (event?.action === "withdraw") return "Withdraw";
    if (event?.action === "transfer") return "Transfer";
    if (event?.action === "liquidation") return "Liquidation";
    if (event?.action === "vaporization") return "Debt Settlement";
    if (event?.action === "asyncDeposit") return "Delayed Deposit";
    if (event?.action === "asyncWithdrawal") return "Delayed Withdraw";
    if (event?.action === "vesting") return event.vestingFlowLabel ? vestingActionLabel(event.vestingFlowLabel) : ACTION_LABELS.vesting;
    if (event?.action === "rewardClaim") return "Claim Rewards";
    return ACTION_LABELS[event?.action] || event?.action || "Dolomite action";
  }

  function cleanVestingEventFlow(event) {
    const direction = vestingActionLabel(event.vestingFlowLabel || (event.role === "in" ? "Received" : "Transferred out"));
    const id = event.vestingPositionId ? ` #${event.vestingPositionId}` : "";
    const amount = cleanVestingAmountLabel(event);
    const status = event.vestingStatus ? `, current status: ${formatVestingStatus(event.vestingStatus)}` : "";
    return `${direction}${id}${amount}${status}`;
  }

  function compactVestingTableFlow(event) {
    const id = event.vestingPositionId ? ` #${event.vestingPositionId}` : "";
    const amount = event.vestingOTokenAmount && event.vestingOTokenAmount !== "0" ? event.vestingOTokenAmount : "";
    const payment = event.vestingPaymentAmount && event.vestingPaymentAmount !== "0" ? event.vestingPaymentAmount : "";
    if (vestingFlowIsExercise(event.vestingFlowLabel)) {
      const paymentLabel = payment ? ` · paid ${formatRoundedTokenAmount(payment, "USDC")}` : "";
      return `veDOLO${id}${paymentLabel}`;
    }
    if (vestingFlowIsOpen(event.vestingFlowLabel)) {
      const amountLabel = amount ? formatRoundedTokenAmount(amount, "oDOLO + DOLO") : "";
      return amountLabel ? `${amountLabel} -> Position${id}` : `Position${id}`;
    }
    const amountLabel = amount ? ` (${formatRoundedTokenAmount(amount, "oDOLO/DOLO")})` : "";
    return `Vesting position${id}${amountLabel}`;
  }

  function compactVestingEventsPreview(row) {
    return summarizeUniqueCsvLabels(
      vestingEventsForRow(row).map(compactVestingTableFlow),
      3,
    );
  }

  function cleanVestingAmountLabel(event) {
    const oTokenAmount = event.vestingOTokenAmount && event.vestingOTokenAmount !== "0" ? event.vestingOTokenAmount : "";
    const flowLabel = event.vestingFlowLabel || "";
    if (vestingFlowIsOpen(flowLabel)) {
      return oTokenAmount ? ` (paired ${oTokenAmount} oDOLO + ${oTokenAmount} DOLO)` : "";
    }
    if (vestingFlowIsExercise(flowLabel)) {
      const exerciseParts = [
        event.vestingPaymentAmount && event.vestingPaymentAmount !== "0" ? `paid ${event.vestingPaymentAmount} USDC` : "",
        oTokenAmount ? `used ${oTokenAmount} paired oDOLO/DOLO` : "",
        oTokenAmount ? "received veDOLO lock" : "",
      ].filter(Boolean);
      return exerciseParts.length ? ` (${exerciseParts.join("; ")})` : "";
    }
    const parts = [
      oTokenAmount ? `vesting pair ${oTokenAmount} oDOLO/DOLO` : "",
    ].filter(Boolean);
    if (parts.length) return ` (${parts.join("; ")})`;
    const leg = (event.legs || [])[0];
    const amount = cleanLegAmountSymbol(leg);
    return amount ? ` (${amount})` : "";
  }

  function formatVestingStatus(status) {
    const text = String(status || "").trim();
    if (!text) return "";
    return text
      .toLowerCase()
      .replace(/_/g, " ")
      .replace(/\b[a-z]/g, char => char.toUpperCase());
  }

  function summarizeUniqueCsvLabels(labels, limit) {
    const unique = Array.from(new Set((labels || []).map(label => String(label || "").trim()).filter(Boolean)));
    if (unique.length <= limit) return unique.join("; ");
    return `${unique.slice(0, limit).join("; ")}; +${unique.length - limit} more Dolomite events`;
  }

  function cleanEarnAssetFlow(entry) {
    const received = entry.assetInAmount && entry.assetInSymbol
      ? `${cleanAmount(entry.assetInAmount)} ${entry.assetInSymbol}`
      : "";
    const sent = entry.assetOutAmount && entry.assetOutSymbol
      ? `${cleanAmount(entry.assetOutAmount)} ${entry.assetOutSymbol}`
      : "";
    const reviewOnly = entry.reviewFlag === "needs_review";
    if (received && String(entry.action || "").toLowerCase().includes("reward")) return `${reviewOnly ? "Review-only reward candidate" : "Reward candidate"}: ${received}`;
    if (received) return `${reviewOnly ? "Review-only yield candidate" : "Yield candidate"}: ${received}`;
    if (sent) return `${reviewOnly ? "Review-only negative yield candidate" : "Negative yield candidate"}: ${sent}`;
    return "";
  }

  function cleanEarnActionLabel(entry) {
    const action = String(entry?.action || "").toLowerCase();
    if (entry?.source === "earn-merkl-rewards" || action.includes("reward")) return "Reward / Claim";
    if (entry?.source === "earn-verified-ledger" || action.includes("yield")) return "EARN Yield";
    return entry?.action || "EARN candidate";
  }

  function cleanHistoryReviewStatus(profiles, gas, userNote) {
    const hasGasGap = gas?.status && !["ok", "not-payer"].includes(gas.status);
    if (hasGasGap) return "needs_review";
    if (profiles.some(profile => profile.reviewFlag === "needs_review" || profile.taxCategory === "unknown")) return "needs_review";
    if (profiles.some(profile => profile.reviewFlag === "income_candidate")) return "income_candidate";
    if (profiles.some(profile => profile.reviewFlag === "possible_taxable_disposal")) return "review";
    if (userNote) return "has_note";
    return "ok";
  }

  function cleanHistoryReviewReason(profiles, gas) {
    const reasons = [];
    profiles.forEach(profile => {
      const reason = reviewReasonForTaxProfile(profile);
      if (reason) reasons.push(reviewReasonLabel(reason));
    });
    if (gas?.status && !["ok", "not-payer"].includes(gas.status)) {
      reasons.push(`Gas status: ${gas.status}`);
    }
    return Array.from(new Set(reasons)).join("; ");
  }

  function cleanEarnReviewStatus(entry) {
    if (entry.reviewFlag === "income_candidate") return "income_candidate";
    if (entry.reviewFlag === "possible_taxable_disposal") return "review";
    return entry.reviewFlag || "needs_review";
  }

  function cleanEarnReviewReason(entry) {
    const reasons = [];
    String(entry.reviewReason || "")
      .split(";")
      .map(reason => reason.trim())
      .filter(Boolean)
      .forEach(reason => reasons.push(reviewReasonLabel(reason)));
    if (entry.claimProofStatus) reasons.push(reviewReasonLabel(entry.claimProofStatus));
    return Array.from(new Set(reasons)).join("; ");
  }

  function cleanHistoryTransactionSource(gas, chain, row = null) {
    const sourceEntities = new Set((row?.events || []).map(event => event?.sourceEntity).filter(Boolean));
    const hasRewardClaimSource = sourceEntities.has("odoloRewardClaimEvents") || sourceEntities.has("rewardClaimEvents");
    const sources = hasRewardClaimSource
      ? ["Dolomite RewardClaimed log"]
      : ["Dolomite subgraph"];
    if (sourceEntities.size > 1 && hasRewardClaimSource) {
      sources.unshift("Dolomite subgraph");
    }
    if (gas?.status && gas.status !== "pending" && gas.status !== FAST_GAS_STATUS) sources.push("RPC receipt gas");
    if (gas?.paidByWallet && Number.isFinite(Number(gas.historicalPrice))) {
      sources.push(`${chain.priceId || "native asset"} historical gas price`);
    }
    return sources.join(" + ");
  }

  function cleanHistorySourceEntities(row) {
    return summarizeUniqueCsvLabels((row?.events || []).map(event => event.sourceEntity), 6);
  }

  function cleanEarnSourceLabel(entry) {
    if (entry.source === "earn-merkl-rewards") return "EARN rewards cache + historical price";
    if (entry.source === "earn-verified-ledger") return "EARN ledger/snapshot + historical price";
    return entry.dataSource || entry.source || "Dolomite history report";
  }

  function exportEvidenceJson() {
    const rows = state.filteredRows;
    const earnEntries = earnTaxEntriesForCurrentView();
    if (!ensureFullReportReady(rows, earnEntries, "Tax evidence JSON")) return;
    downloadJson(
      `dolomite-evidence-${state.address || "wallet"}-${state.year}-${HISTORY_VERSION}.json`,
      evidencePayload(rows, earnEntries),
    );
  }

  function printAnnualStatement() {
    const rows = state.filteredRows;
    const earnEntries = earnTaxEntriesForCurrentView();
    if (!ensureFullReportReady(rows, earnEntries, "Printable report")) return;
    const win = window.open("", "_blank");
    if (!win) {
      setStatus("Printable report was blocked by the browser. Allow pop-ups for this local page and try again.", "warn");
      return;
    }
    win.document.write(statementPrintHtml(annualStatementPayload(rows, earnEntries)));
    win.document.close();
    win.focus();
    window.setTimeout(() => win.print(), 250);
  }

  function downloadCsvFile(filename, headers, rows) {
    const csv = [headers, ...rows].map(row => row.map(csvCell).join(",")).join("\n");
    downloadTextFile(filename, csv, "text/csv;charset=utf-8");
  }

  function downloadTextFile(filename, text, mimeType) {
    const canUseBlobUrl = typeof URL !== "undefined" && typeof URL.createObjectURL === "function";
    const url = canUseBlobUrl
      ? URL.createObjectURL(new Blob([text], { type: mimeType }))
      : `data:${mimeType},${encodeURIComponent(text)}`;
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    if (canUseBlobUrl && typeof URL.revokeObjectURL === "function") URL.revokeObjectURL(url);
  }

  function statementPrintHtml(payload) {
    const assetSummaryRows = payload.assetSummary.slice(0, 12).map(item => `
      <tr>
        <td>${escapeHtml(item.symbol)}</td>
        <td>${escapeHtml(formatAmountOnly(item.intoAmount, item.symbol))}<br><span class="muted">${escapeHtml(formatUsd(item.intoUsd))}</span></td>
        <td>${escapeHtml(formatAmountOnly(item.outAmount, item.symbol))}<br><span class="muted">${escapeHtml(formatUsd(item.outUsd))}</span></td>
        <td>${escapeHtml(formatSignedAmount(item.routeNetAmount, item.symbol))}<br><span class="muted">${escapeHtml(formatUsd(item.routeUsd))}</span></td>
        <td>${escapeHtml(earnSummaryAmountLabel(item))}<br><span class="muted">${escapeHtml(earnSummaryUsdLabel(item))}</span></td>
        <td>${escapeHtml(formatSignedAmount(item.netAmount, item.symbol))}</td>
      </tr>
    `).join("");
    const lifecycleRows = payload.positionLifecycle.slice(0, 12).map(item => {
      const status = lifecycleStatusForItem(item);
      return `
      <tr>
        <td>${escapeHtml(item.chain)}<br><span class="muted">${escapeHtml(item.accountLabel)}</span></td>
        <td>${escapeHtml(item.assetsLabel)}</td>
        <td>${escapeHtml(formatDate(item.firstTimestamp))}</td>
        <td>${escapeHtml(formatDate(item.lastTimestamp))}</td>
        <td>${escapeHtml(`${item.eventCount} events / ${item.txCount} tx`)}<br><span class="muted">${escapeHtml(lifecycleFlowLabel(item))}</span></td>
        <td>${escapeHtml(status.label)}<br><span class="muted">${escapeHtml(status.sub)}</span></td>
      </tr>`;
    }).join("");
    const reviewRows = payload.reviewQueue.slice(0, 10).map(item => `
      <tr>
        <td>${escapeHtml(item.label)}</td>
        <td>${escapeHtml(String(item.count))}</td>
        <td>${escapeHtml(item.example || "")}</td>
      </tr>
    `).join("");
    return `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Dolomite History Report ${escapeHtml(payload.reportYear)}</title>
<style>
body{font-family:Inter,Arial,sans-serif;color:#111;background:#fff;margin:32px;line-height:1.45}
h1{font-size:26px;margin:0 0 4px} h2{font-size:15px;margin:28px 0 8px}
.muted{color:#666;font-size:12px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:20px}
.cell{border:1px solid #ddd;border-radius:8px;padding:12px}.cell span{display:block;color:#666;font-size:10px;text-transform:uppercase;letter-spacing:.08em}.cell strong{font-family:monospace;font-size:16px}
table{width:100%;border-collapse:collapse;margin-top:8px;font-size:12px}th,td{border-bottom:1px solid #ddd;text-align:left;padding:8px;vertical-align:top}th{color:#555;font-size:10px;text-transform:uppercase;letter-spacing:.08em}
.scope{border:1px solid #ddd;border-radius:8px;padding:12px;margin-top:18px;background:#fafafa}
@media print{body{margin:18mm}.no-print{display:none}}
</style>
</head>
<body>
<h1>Dolomite History Report</h1>
<div class="muted">Wallet ${escapeHtml(payload.wallet)} · Year ${escapeHtml(payload.reportYear)} · Generated ${escapeHtml(payload.generatedAt)} · Version ${escapeHtml(payload.version)}</div>
<div class="scope">
  <strong>${escapeHtml(payload.methodology.statementKind)}</strong><br>
  Scope: ${escapeHtml(payload.scope.reportScope)}. External cost basis included: ${escapeHtml(payload.scope.externalCostBasisIncluded)}.
  ${escapeHtml(payload.scope.scopeNotes)}
</div>
<div class="grid">
  <div class="cell"><span>Transactions</span><strong>${escapeHtml(String(payload.totals.transactions))}</strong></div>
  <div class="cell"><span>Events</span><strong>${escapeHtml(String(payload.totals.events))}</strong></div>
  <div class="cell"><span>Gas paid USD</span><strong>${escapeHtml(formatUsd(payload.totals.gasPaidUsd))}</strong></div>
  <div class="cell"><span>Evidence rows</span><strong>${escapeHtml(String(payload.totals.exportRows))}</strong></div>
</div>
<h2>Completeness</h2>
<table><tr><th>Receipts</th><th>Priced events</th><th>EARN rows</th><th>Export rows</th></tr><tr><td>${escapeHtml(payload.completeness.receiptLabel)}</td><td>${escapeHtml(payload.completeness.priceLabel)}</td><td>${escapeHtml(payload.completeness.earnLabel)}</td><td>${escapeHtml(payload.completeness.exportRowsLabel)}</td></tr></table>
<h2>Annual asset summary</h2>
<table><tr><th>Asset</th><th>Into Dolomite</th><th>Out of Dolomite</th><th>Routes net</th><th>EARN</th><th>Activity net</th></tr>${assetSummaryRows || `<tr><td colspan="6">No asset summary.</td></tr>`}</table>
<h2>Position lifecycle</h2>
<table><tr><th>Position</th><th>Assets</th><th>First</th><th>Last</th><th>Evidence</th><th>Status</th></tr>${lifecycleRows || `<tr><td colspan="6">No position lifecycle evidence.</td></tr>`}</table>
<h2>Review queue</h2>
<table><tr><th>Reason</th><th>Count</th><th>Example</th></tr>${reviewRows || `<tr><td colspan="3">No review flags.</td></tr>`}</table>
<h2>Methodology</h2>
<p class="muted">${escapeHtml(payload.methodology.balanceSnapshotReason)} Prices use historical timestamps where available. This statement is not jurisdiction-specific tax advice.</p>
</body>
</html>`;
  }

  function ensureFullReportReady(rows, earnEntries, label) {
    if (state.filtersDirty) {
      setStatus(`${label} needs the current filters to be loaded first. Click Load history and try again.`, "warn");
      return false;
    }
    if (!rows.length && !earnEntries.length) return false;
    if (rows.some(row => row.gas?.status === "pending")) {
      setStatus(`${label} waits for all visible gas receipts to finish so fee evidence is not partial.`, "warn");
      return false;
    }
    if (rows.some(row => row.gas?.status === FAST_GAS_STATUS)) {
      setStatus(`${label} needs full gas evidence. Reload history and wait for receipts to finish.`, "warn");
      return false;
    }
    if (state.earn?.status === "loading") {
      setStatus(`${label} waits for candidate evidence to finish so export rows are not omitted.`, "warn");
      return false;
    }
    const activeWarnings = activeDataWarnings();
    if (activeWarnings.length) {
      setStatus(`${label} waits for complete source data: ${activeWarnings[0]}${activeWarnings.length > 1 ? ` (+${activeWarnings.length - 1} more)` : ""}`, "warn");
      return false;
    }
    return true;
  }

  function evidencePayload(rows, earnEntries) {
    const meta = taxExportMeta(rows, earnEntries);
    return {
      version: HISTORY_VERSION,
      generatedAt: meta.generatedAt,
      wallet: state.address,
      reportYear: state.year,
      protocol: "Dolomite",
      scope: {
        reportScope: meta.scope,
        externalCostBasisIncluded: meta.externalCostBasisIncluded,
        scopeNotes: meta.scopeNotes,
      },
      filters: {
        dateRange: meta.period,
        chainsIncluded: selectedChainNames(),
        action: actionFilterAllSelected() ? "all" : selectedActionKeys(),
      },
      methodology: reportMethodology(),
      completeness: reportCompletenessForRows(rows, earnEntries),
      reviewQueue: reviewQueueForRows(rows, earnEntries),
      assetSummary: assetActivitySummaryForRows(rows, earnEntries),
      positionLifecycle: positionLifecycleForRows(rows, earnEntries),
      reviewNotes: reviewNotesForExport(rows),
      warnings: activeDataWarnings(),
      rows: rows.map(row => evidenceRowPayload(row)),
      earnEntries: earnEntries.map(entry => ({ ...entry })),
    };
  }

  function evidenceRowPayload(row) {
    const chain = CHAINS[row.chainKey];
    return {
      chainKey: row.chainKey,
      chain: chain.name,
      txHash: row.txHash,
      blockNumber: row.blockNumber,
      timestamp: row.timestamp,
      timestampUtc: isoDate(row.timestamp),
      explorer: chain.explorerTx + row.txHash,
      actions: displayActionsForRow(row).map(actionDisplayLabel),
      actionSummary: cleanTransactionAction(row),
      classificationSource: classificationSourceForRow(row),
      assetFlowSummary: cleanTransactionAssetFlow(row),
      sourceEntities: cleanHistorySourceEntities(row),
      usdVolume: decimalForCsv(row.usdVolume),
      gas: {
        ...(row.gas || {}),
        nativeSymbol: row.gas?.nativeSymbol || chain.nativeSymbol,
      },
      userNote: reviewNoteForRow(row),
      events: row.events.map((event, eventIndex) => {
        const profile = taxProfileForEvent(event);
        return {
          eventIndex: eventIndex + 1,
          serialId: event.serialId || "",
          sourceEntity: event.sourceEntity || "",
          action: event.action,
          actionLabel: cleanReportActionLabel(event),
          classificationSource: classificationSourceForEvent(row, event),
          borrowSemanticAction: event.borrowSemanticAction || "",
          borrowSemanticConfidence: event.borrowSemanticConfidence || "",
          borrowBalanceBefore: event.borrowBalanceBefore || "",
          borrowBalanceAfter: event.borrowBalanceAfter || "",
          activityGroup: activityGroupForEvent(event),
          accountNumber: event.account || "",
          label: event.label || "",
          asset: event.asset || "",
          fairMarketValueUsd: decimalForCsv(event.usd),
          taxCategory: profile.taxCategory,
          reviewFlag: profile.reviewFlag,
          reviewReason: reviewReasonForTaxProfile(profile),
          routeEvidence: event.routeEvidence || "",
          routeTokenPath: event.routeTokenPath || "",
          routeMatchConfidence: event.routeMatchConfidence || "",
          routeHopCount: event.routeHopCount || "",
          feeAllocation: feeAllocationForEvent(row, eventIndex),
          legs: profile.legs,
          note: profile.taxNote,
        };
      }),
    };
  }

  function annualStatementPayload(rows, earnEntries) {
    const meta = taxExportMeta(rows, earnEntries);
    const taxSummary = taxSummaryForRows(rows, earnEntries);
    const activitySummary = dolomiteActivitySummaryForRows(rows);
    const completeness = reportCompletenessForRows(rows, earnEntries);
    const gasSummary = gasSummaryForRows(rows);
    const methodology = reportMethodology();
    return {
      version: HISTORY_VERSION,
      generatedAt: meta.generatedAt,
      wallet: state.address,
      reportYear: state.year,
      protocol: "Dolomite",
      scope: {
        reportScope: meta.scope,
        externalCostBasisIncluded: meta.externalCostBasisIncluded,
        scopeNotes: meta.scopeNotes,
      },
      filters: {
        dateRange: meta.period,
        chainsIncluded: selectedChainNames(),
        action: actionFilterAllSelected() ? "all" : selectedActionKeys(),
      },
      methodology,
      balanceSnapshots: {
        status: methodology.balanceSnapshotStatus,
        reason: methodology.balanceSnapshotReason,
      },
      totals: {
        transactions: rows.length,
        events: completeness.eventCount,
        exportRows: completeness.exportRows,
        protocolVolumeUsd: decimalForCsv(rows.reduce((sum, row) => sum + Number(row.usdVolume || 0), 0)),
        gasPaidUsd: decimalForCsv(gasSummary.gasUsd),
        possibleDisposals: taxSummary.disposalCount,
        possibleDisposalUsd: decimalForCsv(taxSummary.disposalUsd),
        incomeCandidates: taxSummary.incomeCount,
        incomeCandidateUsd: decimalForCsv(taxSummary.incomeUsd),
        needsReviewRows: taxSummary.reviewCount,
        needsReviewEvents: taxSummary.reviewEventCount,
      },
      gas: gasSummary,
      activity: activitySummary,
      completeness,
      earn: {
        status: state.earn?.status || "idle",
        summary: earnSummaryForCurrentView(),
        rows: earnEntries,
      },
      review: reviewSummaryForCurrentView(),
      reviewQueue: reviewQueueForRows(rows, earnEntries),
      reviewNotes: reviewNotesForExport(rows),
      assetSummary: assetActivitySummaryForRows(rows, earnEntries),
      positionLifecycle: positionLifecycleForRows(rows, earnEntries),
      warnings: activeDataWarnings(),
    };
  }

  function gasSummaryForRows(rows) {
    const byChain = new Map();
    let gasUsd = 0;
    let paidRows = 0;
    let checkedRows = 0;
    rows.forEach(row => {
      const gas = row.gas || {};
      if (gas.status && gas.status !== "pending") checkedRows += 1;
      if (!gas.paidByWallet) return;
      paidRows += 1;
      const chain = CHAINS[row.chainKey];
      const nativeAmount = decimalToNumber(gas.nativeAmountExact || gas.nativeAmount);
      const usd = Number.isFinite(Number(gas.gasUsd)) ? Number(gas.gasUsd) : 0;
      gasUsd += usd;
      const entry = byChain.get(row.chainKey) || {
        chainKey: row.chainKey,
        chain: chain.name,
        nativeSymbol: gas.nativeSymbol || chain.nativeSymbol,
        nativeAmount: 0,
        gasUsd: 0,
        paidRows: 0,
      };
      entry.nativeAmount += nativeAmount;
      entry.gasUsd += usd;
      entry.paidRows += 1;
      byChain.set(row.chainKey, entry);
    });
    return {
      checkedRows,
      totalRows: rows.length,
      paidRows,
      gasUsd,
      byChain: Array.from(byChain.values()).map(entry => ({
        ...entry,
        nativeAmount: decimalForCsv(entry.nativeAmount),
        gasUsd: decimalForCsv(entry.gasUsd),
      })),
    };
  }

  function selectedChainNames() {
    return selectedChainKeys()
      .map(chainKey => CHAINS[chainKey].name);
  }

  function selectedChainKeys() {
    const keys = chainFilterKeys().filter(chainKey => state.selectedChains.has(chainKey));
    return keys.length ? keys : defaultChainKeys();
  }

  function downloadJson(filename, payload) {
    downloadTextFile(filename, JSON.stringify(payload, null, 2), "application/json;charset=utf-8");
  }

  function taxExportMeta(rows, earnEntries) {
    const checked = rows.filter(row => row.gas && row.gas.status && row.gas.status !== "pending").length;
    const earnSummary = earnSummaryForCurrentView();
    return {
      generatedAt: new Date().toISOString(),
      year: state.year,
      period: getBounds(state.year).label,
      chainsIncluded: selectedChainKeys()
        .map(chainKey => CHAINS[chainKey].name)
        .join("; "),
      warningsCount: String(historyWarningCount()),
      gasCoverage: rows.length ? `${checked}/${rows.length}` : "0/0",
      earnCoverage: `${earnSummary.statusLabel}; ${earnSummary.coverage}; ${earnEntries.length} earn rows`,
      scope: TAX_REPORT_SCOPE,
      externalCostBasisIncluded: TAX_EXTERNAL_COST_BASIS_INCLUDED,
      scopeNotes: TAX_SCOPE_NOTES,
    };
  }

  function taxProfileForEvent(event) {
    return {
      taxCategory: event.taxCategory || "unknown",
      reviewFlag: event.reviewFlag || "needs_review",
      reviewReason: event.reviewReason || "",
      legs: Array.isArray(event.legs) ? event.legs : [],
      taxNote: event.taxNote || "",
    };
  }

  function activityGroupForEvent(event) {
    if (!event || typeof event !== "object") return "unknown";
    if (event.borrowSemanticAction === "borrow" || event.borrowSemanticAction === "openBorrow") return "borrow";
    if (event.borrowSemanticAction === "repay" || event.borrowSemanticAction === "closeBorrow") return "repay";
    if (event.action === "deposit" || event.taxCategory === "protocol_deposit") return "dolomite_in";
    if (event.action === "withdraw" || event.taxCategory === "protocol_withdrawal") return "dolomite_out";
    if (event.action === "trade" || event.action === "zap" || event.taxCategory === "swap" || event.taxCategory === "zap") return "trade_route";
    if (event.action === "amm") {
      return event.taxCategory === "swap" ? "trade_route" : "amm_liquidity";
    }
    if (event.action === "transfer") return "dolomite_transfer";
    if (event.action === "liquidation" || event.action === "vaporization") return "forced_settlement";
    if (event.action === "asyncDeposit" || event.action === "asyncWithdrawal") return "async_position";
    if (event.action === "vesting") return vestingTaxCategory(event.vestingFlowLabel);
    if (event.action === "odoloClaim" || event.action === "rewardClaim") return "external_reward";
    if (event.action === "rewardLevelUpdate") return "reward_level_update";
    return event.action || event.taxCategory || "unknown";
  }

  function activityGroupForEarnEntry(entry) {
    if (entry?.source === "earn-verified-ledger") return "earn_yield";
    if (entry?.source === "earn-merkl-rewards") return "external_reward";
    return "earn_summary";
  }

  function feeAllocationForEvent(row, eventIndex) {
    const gas = row?.gas || {};
    if (!gas.status || gas.status === "pending") return "pending_receipt";
    if (!gas.paidByWallet) return gas.status === "not-payer" ? "not_paid_by_wallet" : "no_wallet_fee";
    if (eventIndex === 0) {
      return row.events?.length > 1 ? "transaction_level_fee_recorded_once" : "transaction_level_fee";
    }
    return "shared_tx_fee_not_repeated";
  }

  function dolomiteActivitySummaryForRows(rows) {
    const summary = {
      inCount: 0,
      inUsd: 0,
      outCount: 0,
      outUsd: 0,
      tradeCount: 0,
      tradeUsd: 0,
      eventCount: 0,
      chainCount: 0,
      accountCount: 0,
    };
    const chains = new Set();
    const accounts = new Set();
    rows.forEach(row => {
      chains.add(row.chainKey);
      row.events.forEach(event => {
        summary.eventCount += 1;
        if (event.account !== undefined && event.account !== null && event.account !== "") {
          accounts.add(`${row.chainKey}:${event.account}`);
        }
        const usd = Number(event.usd || 0);
        const absUsd = Number.isFinite(usd) ? Math.abs(usd) : 0;
        const group = activityGroupForEvent(event);
        if (group === "dolomite_in") {
          summary.inCount += 1;
          summary.inUsd += absUsd;
        } else if (group === "dolomite_out") {
          summary.outCount += 1;
          summary.outUsd += absUsd;
        } else if (group === "trade_route") {
          summary.tradeCount += 1;
          summary.tradeUsd += absUsd;
        }
      });
    });
    summary.chainCount = chains.size;
    summary.accountCount = accounts.size;
    return summary;
  }

  function taxSummaryForRows(rows, earnEntries = []) {
    const summary = {
      disposalCount: 0,
      disposalUsd: 0,
      incomeCount: 0,
      incomeUsd: 0,
      feeCount: 0,
      feeChecked: 0,
      feeTotal: rows.length,
      feeUsd: 0,
      reviewCount: 0,
      reviewEventCount: 0,
    };
    const includeProfile = profile => {
      const usd = Number(profile.usd || 0);
      if (profile.reviewFlag === "possible_taxable_disposal") {
        summary.disposalCount += 1;
        summary.disposalUsd += Number.isFinite(usd) ? Math.abs(usd) : 0;
      }
      if (profile.reviewFlag === "income_candidate") {
        summary.incomeCount += 1;
        summary.incomeUsd += Number.isFinite(usd) ? Math.abs(usd) : 0;
      }
      if (profile.reviewFlag === "needs_review" || profile.taxCategory === "unknown") {
        summary.reviewEventCount += 1;
        return true;
      }
      return false;
    };
    rows.forEach(row => {
      let rowNeedsReview = false;
      row.events.forEach(event => {
        const profile = taxProfileForEvent(event);
        rowNeedsReview = includeProfile({ ...profile, usd: event.usd }) || rowNeedsReview;
      });
      if (rowNeedsReview) summary.reviewCount += 1;
      if (row.gas?.status && row.gas.status !== "pending") summary.feeChecked += 1;
      if (row.gas?.paidByWallet && Number.isFinite(Number(row.gas.gasUsd))) {
        summary.feeCount += 1;
        summary.feeUsd += Number(row.gas.gasUsd || 0);
      }
    });
    earnEntries.forEach(entry => {
      const needsReview = includeProfile(entry);
      if (needsReview) summary.reviewCount += 1;
    });
    return summary;
  }

  function getBounds(yearValue) {
    const nowYear = new Date().getUTCFullYear();
    const currentYear = Math.max(nowYear, START_YEAR);
    if (yearValue === "custom") {
      const from = String(state.dateFrom || "").trim();
      const to = String(state.dateTo || "").trim();
      if (!validDateInput(from) || !validDateInput(to)) {
        throw new Error("Select both From and To dates for the custom history range.");
      }
      const start = dateInputToUnixStart(from);
      const end = dateInputToUnixEnd(to);
      if (!start || !end) throw new Error("Select a valid custom history date range.");
      if (start < Math.floor(Date.UTC(START_YEAR, 0, 1) / 1000)) {
        throw new Error(`Custom history starts at ${START_YEAR}-01-01 or later.`);
      }
      if (end < start) throw new Error("Custom history range must end after the From date.");
      return {
        start,
        end,
        label: `${from} to ${to}`,
      };
    }
    if (yearValue === "all") {
      return {
        start: Math.floor(Date.UTC(START_YEAR, 0, 1) / 1000),
        end: Math.floor(Date.UTC(currentYear, 11, 31, 23, 59, 59) / 1000),
        label: `since ${START_YEAR}`,
      };
    }
    const year = Math.max(START_YEAR, Math.min(currentYear, Number(yearValue) || currentYear));
    return {
      start: Math.floor(Date.UTC(year, 0, 1) / 1000),
      end: Math.floor(Date.UTC(year, 11, 31, 23, 59, 59) / 1000),
      label: String(year),
    };
  }

  function validDateInput(value) {
    const raw = String(value || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return false;
    const [year, month, day] = raw.split("-").map(Number);
    const date = new Date(Date.UTC(year, month - 1, day));
    return date.getUTCFullYear() === year
      && date.getUTCMonth() === month - 1
      && date.getUTCDate() === day;
  }

  function dateInputToUnixStart(value) {
    if (!validDateInput(value)) return 0;
    const [year, month, day] = String(value).split("-").map(Number);
    return Math.floor(Date.UTC(year, month - 1, day, 0, 0, 0) / 1000);
  }

  function dateInputToUnixEnd(value) {
    if (!validDateInput(value)) return 0;
    const [year, month, day] = String(value).split("-").map(Number);
    return Math.floor(Date.UTC(year, month - 1, day, 23, 59, 59) / 1000);
  }

  function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async function waitForHistoryFinalize(tasks, timeoutMs = HISTORY_FINALIZE_BUDGET_MS) {
    const pending = (tasks || []).filter(Boolean).map(task => Promise.resolve(task));
    if (!pending.length) return true;
    return Promise.race([
      Promise.all(pending).then(() => true),
      delay(timeoutMs).then(() => false),
    ]);
  }

  async function mapLimit(items, limit, worker) {
    const results = new Array(items.length);
    let next = 0;
    const runners = Array.from({ length: Math.min(limit, items.length) }, async () => {
      while (next < items.length) {
        const index = next;
        next += 1;
        results[index] = await worker(items[index], index);
      }
    });
    await Promise.all(runners);
    return results;
  }

  function chainChip(chainKey) {
    const chain = CHAINS[chainKey];
    return `<span class="chain-chip" title="${escapeAttr(chain.name)}" aria-label="${escapeAttr(chain.name)}"><img src="${escapeAttr(chain.icon)}" alt=""><span class="chain-name">${escapeHtml(chain.name)}</span></span>`;
  }

  function actionChip(action) {
    if (action && typeof action === "object") {
      const key = action.key || action.value || "custom";
      const className = action.className || key;
      const label = actionDisplayLabel(action, true);
      return `<span class="action-chip ${escapeAttr(className)}">${escapeHtml(label)}</span>`;
    }
    const label = actionDisplayLabel(action, true);
    return `<span class="action-chip ${escapeAttr(action)}">${escapeHtml(label)}</span>`;
  }

  function actionDisplayLabel(action, table = false) {
    if (action && typeof action === "object") {
      const key = action.key || action.value || "custom";
      return action.label || (table ? ACTION_TABLE_LABELS[key] : ACTION_LABELS[key]) || ACTION_LABELS[key] || key;
    }
    return (table ? ACTION_TABLE_LABELS[action] : ACTION_LABELS[action]) || ACTION_LABELS[action] || action;
  }

  function gasHtml(row) {
    const gas = row.gas || { status: "pending" };
    if (gas.status === "pending") {
      return `<div class="gas-cell"><div class="gas-main muted">Checking...</div></div>`;
    }
    if (gas.status === "ok") {
      return `<div class="gas-cell"><div class="gas-main good">${formatUsd(gas.gasUsd)}</div></div>`;
    }
    if (gas.status === "price-missing") {
      return `<div class="gas-cell"><div class="gas-main warn">Price missing</div></div>`;
    }
    if (gas.status === "not-payer") {
      return `<div class="gas-cell"><div class="gas-main muted">Not payer</div></div>`;
    }
    if (gas.status === FAST_GAS_STATUS) {
      return `<div class="gas-cell"><div class="gas-main warn">Incomplete</div></div>`;
    }
    if (gas.status === "missing") {
      return `<div class="gas-cell"><div class="gas-main muted">No receipt</div></div>`;
    }
    return `<div class="gas-cell"><div class="gas-main warn">Unavailable</div></div>`;
  }

  function formatUnits(value, decimals, maxDecimals) {
    const negative = value < 0n;
    const abs = negative ? -value : value;
    const scale = 10n ** BigInt(decimals);
    const whole = abs / scale;
    const fraction = abs % scale;
    let fracText = fraction.toString().padStart(decimals, "0").slice(0, maxDecimals);
    fracText = fracText.replace(/0+$/, "");
    return `${negative ? "-" : ""}${whole.toString()}${fracText ? "." + fracText : ""}`;
  }

  function hexToBigInt(hex) {
    if (hex === undefined || hex === null || hex === "" || hex === "0x") return 0n;
    return BigInt(String(hex));
  }

  function receiptExtraFeeWei(receipt) {
    const explicitFeeFields = ["l1Fee", "l1DataFee", "operatorFee"];
    let total = 0n;
    explicitFeeFields.forEach(field => {
      if (receipt && receipt[field] !== undefined && receipt[field] !== null) {
        total += hexToBigInt(receipt[field]);
      }
    });
    const blobGasUsed = hexToBigInt(receipt?.blobGasUsed);
    const blobGasPrice = hexToBigInt(receipt?.blobGasPrice || receipt?.effectiveBlobGasPrice);
    if (blobGasUsed > 0n && blobGasPrice > 0n) total += blobGasUsed * blobGasPrice;
    return total;
  }

  function unitsToNumber(value, decimals) {
    return decimalToNumber(formatUnits(value, decimals, decimals));
  }

  function cleanAmount(value) {
    const raw = String(value ?? "0").trim();
    if (!raw || raw === "0") return "0";
    if (!raw.includes(".")) return raw;
    const [whole, frac] = raw.split(".");
    const trimmedFrac = (frac || "").replace(/0+$/, "");
    if (!trimmedFrac) return whole;
    const firstNonZero = trimmedFrac.search(/[1-9]/);
    const visibleDigits = firstNonZero >= 8
      ? Math.min(trimmedFrac.length, firstNonZero + 4, 18)
      : 8;
    const cleanFrac = trimmedFrac.slice(0, visibleDigits).replace(/0+$/, "");
    return cleanFrac ? `${whole}.${cleanFrac}` : whole;
  }

  function formatUiTokenAmount(value, symbol = "") {
    const exact = cleanAmount(value);
    const number = Number(String(exact).replace(/,/g, ""));
    if (!Number.isFinite(number) || number === 0) return exact;
    const abs = Math.abs(number);
    const upperSymbol = String(symbol || "").toUpperCase();
    if (abs < 0.000001) return exact;
    const maxDecimals = uiTokenDecimals(abs, upperSymbol);
    const minDecimals = uiTokenMinDecimals(abs, upperSymbol, maxDecimals);
    const rounded = roundDisplayNumber(number, minDecimals, maxDecimals);
    return rounded === "0" ? exact : rounded;
  }

  function uiTokenDecimals(abs, symbol) {
    const stableSymbols = new Set(["USDC", "USDT", "DAI", "USD1", "USDE", "USDS", "FRAX"]);
    const btcSymbols = new Set(["BTC", "WBTC", "TBTC", "CBBTC", "LBTC"]);
    const gasSymbols = new Set(["ETH", "WETH", "BERA", "WBERA", "MNT", "WMNT", "OKB", "BTC"]);
    const doloSymbols = new Set(["DOLO", "ODOLO", "ODOLO/DOLO"]);
    if (btcSymbols.has(symbol)) return abs >= 1 ? 6 : 8;
    if (stableSymbols.has(symbol)) return abs >= 1 ? 2 : 6;
    if (doloSymbols.has(symbol)) return abs >= 1000 ? 2 : 4;
    if (gasSymbols.has(symbol)) return abs >= 1 ? 4 : 8;
    if (abs >= 1000) return 2;
    if (abs >= 1) return 4;
    return 8;
  }

  function uiTokenMinDecimals(abs, symbol, maxDecimals) {
    const stableSymbols = new Set(["USDC", "USDT", "DAI", "USD1", "USDE", "USDS", "FRAX"]);
    if (stableSymbols.has(symbol) && abs >= 1) return Math.min(2, maxDecimals);
    return 0;
  }

  function roundDisplayNumber(number, minDecimals, maxDecimals) {
    return number.toLocaleString("en-US", {
      useGrouping: true,
      minimumFractionDigits: minDecimals,
      maximumFractionDigits: maxDecimals,
    }).replace(/,/g, "");
  }

  function formatUiFlowPart(value) {
    const text = String(value || "").trim();
    const match = text.match(/^([0-9][0-9,]*(?:\.[0-9]+)?)\s+(.+)$/);
    if (!match) return text;
    return `${formatUiTokenAmount(match[1], match[2])} ${match[2]}`;
  }

  function decimalToNumber(value) {
    const number = Number(String(value ?? "0").replace(/,/g, ""));
    return Number.isFinite(number) ? number : 0;
  }

  function positiveAmount(value) {
    return decimalToNumber(value) > 0;
  }

  function decimalForCsv(value) {
    const number = Number(value);
    return Number.isFinite(number) ? String(Math.round(number * 100000000) / 100000000) : "";
  }

  function formatUsd(value) {
    const number = Number(value || 0);
    if (!Number.isFinite(number)) return "$0.00";
    if (Math.abs(number) >= 1000000) return "$" + (number / 1000000).toFixed(2) + "M";
    if (Math.abs(number) >= 1000) return "$" + number.toLocaleString(undefined, { maximumFractionDigits: 2 });
    if (Math.abs(number) >= 1) return "$" + number.toFixed(2);
    if (Math.abs(number) > 0) return "$" + number.toFixed(4);
    return "$0.00";
  }

  function formatRoundedTokenAmount(value, symbol) {
    const number = Number(String(value ?? "0").replace(/,/g, ""));
    if (!Number.isFinite(number) || number === 0) return symbol || "";
    const amount = number.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    return `${amount} ${symbol || ""}`.trim();
  }

  function formatCompactDecimal(value) {
    const number = Number(value || 0);
    if (!Number.isFinite(number) || number === 0) return "0";
    if (Math.abs(number) >= 1) return number.toFixed(4).replace(/0+$/, "").replace(/\.$/, "");
    return number.toPrecision(4).replace(/0+$/, "").replace(/\.$/, "");
  }

  function formatAmountOnly(value, symbol) {
    const number = Number(value || 0);
    if (!Number.isFinite(number) || number === 0) return "-";
    return `${formatCompactDecimal(number)} ${symbol || ""}`.trim();
  }

  function formatSignedAmount(value, symbol) {
    const number = Number(value || 0);
    if (!Number.isFinite(number) || number === 0) return `0${symbol ? ` ${symbol}` : ""}`;
    const sign = number > 0 ? "+" : "";
    return `${sign}${formatCompactDecimal(number)}${symbol ? ` ${symbol}` : ""}`;
  }

  function formatDate(timestamp) {
    if (!timestamp) return "-";
    const date = new Date(timestamp * 1000);
    return new Intl.DateTimeFormat("en-GB", {
      day: "2-digit",
      month: "short",
      year: "numeric",
    }).format(date);
  }

  function formatHistoryDate(timestamp) {
    if (!timestamp) return "-";
    const date = new Date(timestamp * 1000);
    return new Intl.DateTimeFormat("en-GB", {
      day: "numeric",
      month: "short",
      year: "numeric",
    }).format(date);
  }

  function formatRelativeTime(timestamp) {
    if (!timestamp) return "-";
    const ms = timestamp * 1000;
    const diff = Math.max(0, Math.floor((Date.now() - ms) / 1000));
    if (diff < 60) return "just now";
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    if (diff < 86400 * 7) return `${Math.floor(diff / 86400)}d ago`;
    return `${Math.floor(diff / 604800)}w ago`;
  }

  function formatClockTime(timestamp) {
    if (!timestamp) return "-";
    const date = new Date(timestamp * 1000);
    return new Intl.DateTimeFormat("en-US", {
      hour: "2-digit",
      minute: "2-digit",
    }).format(date);
  }

  function formatTime(timestamp) {
    if (!timestamp) return "-";
    const date = new Date(timestamp * 1000);
    return new Intl.DateTimeFormat("en-GB", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    }).format(date);
  }

  function isoDate(timestamp) {
    if (!timestamp) return "";
    return new Date(timestamp * 1000).toISOString();
  }

  function isoDateInput(timestamp) {
    if (!timestamp) return "";
    return new Date(timestamp * 1000).toISOString().slice(0, 10);
  }

  function shortHash(hash) {
    const value = String(hash || "");
    return value.length > 12 ? `${value.slice(0, 8)}...${value.slice(-4)}` : value;
  }

  function explorerTxUrl(row) {
    const chain = CHAINS[row?.chainKey] || {};
    const hash = String(row?.txHash || "").trim();
    return chain.explorerTx && hash ? `${chain.explorerTx}${hash}` : "";
  }

  function explorerAddressUrl(chainKey, address) {
    const value = String(address || "").trim();
    if (!isAddress(value)) return "";
    const chain = CHAINS[chainKey] || {};
    if (chain.explorerAddress) return `${chain.explorerAddress}${value}`;
    if (!chain.explorerTx) return "";
    return `${chain.explorerTx.replace(/\/tx\/?$/, "/address/")}${value}`;
  }

  function externalLinkSvg() {
    return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M18 13v6a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><path d="M15 3h6v6"/><path d="M10 14 21 3"/></svg>`;
  }

  function eventTransactionLinkHtml(row, iconOnly = false) {
    const href = explorerTxUrl(row);
    if (!href) return "";
    return `
      <a class="event-open-link ${iconOnly ? "icon-only" : ""}" href="${escapeAttr(href)}" target="_blank" rel="noopener" aria-label="Open transaction ${escapeAttr(shortHash(row.txHash))}">
        ${iconOnly ? "" : "<span>Tx</span>"}${externalLinkSvg()}
      </a>
    `;
  }

  function addressExplorerLinkHtml(chainKey, address) {
    const value = String(address || "").trim();
    if (!value) return "";
    const label = shortAddress(value);
    const href = explorerAddressUrl(chainKey, value);
    if (!href) return `<span class="address-inline"><span>${escapeHtml(label)}</span></span>`;
    return `
      <span class="address-inline">
        <span>${escapeHtml(label)}</span>
        <a class="address-open-link" href="${escapeAttr(href)}" target="_blank" rel="noopener" aria-label="Open address ${escapeAttr(label)}">${externalLinkSvg()}</a>
      </span>
    `;
  }

  function sameHistoryWalletAddress(address) {
    return !!state.address && normalizeAddress(address) === normalizeAddress(state.address);
  }

  function eventMetaHtml(row, event) {
    const parts = [];
    const classification = classificationSourceForEvent(row, event);
    if (classification) parts.push(eventClassificationSourceHtml(classification));
    const account = eventAccountHtml(event);
    if (account) parts.push(account);
    const counterparty = eventCounterpartyHtml(row, event);
    if (counterparty) parts.push(counterparty);
    return parts.join("");
  }

  function eventMetaBlockHtml(row, event) {
    const html = eventMetaHtml(row, event);
    return html ? `<span class="event-meta">${html}</span>` : "";
  }

  function eventClassificationSourceHtml(source) {
    return `
      <span class="event-detail-item event-classification-source">
        <span class="event-detail-label">Classification</span>
        <span class="event-detail-value">${escapeHtml(source)}</span>
      </span>
    `;
  }

  function eventAccountHtml(event) {
    const fromAccount = accountDisplayName(event?.fromAccount);
    const toAccount = accountDisplayName(event?.toAccount);
    const isInternalRoute = event?.isSelfTransfer && fromAccount && toAccount && fromAccount !== toAccount;
    const account = String(event?.account ?? "").trim();
    const value = isInternalRoute ? `${fromAccount} -> ${toAccount}` : account && account !== "0" ? formatAccountValue(account) : "";
    if (!value) return "";
    const label = isInternalRoute ? "Internal route" : "Subaccount";
    return `
      <span class="event-detail-item event-account-item">
        <span class="event-detail-label">${escapeHtml(label)}</span>
        <span class="event-detail-value">${escapeHtml(value)}</span>
      </span>
    `;
  }

  function eventCounterpartyHtml(row, event) {
    if (sameHistoryWalletAddress(event?.counterparty)) return "";
    const link = addressExplorerLinkHtml(row?.chainKey, event?.counterparty);
    if (!link) return "";
    const label = event?.role === "in" ? "From" : event?.role === "out" ? "To" : "Address";
    return `
      <span class="event-detail-item event-counterparty">
        <span class="event-detail-label">${escapeHtml(label)}</span>
        <span class="event-detail-value">${link}</span>
      </span>
    `;
  }

  function shortAddress(address) {
    const value = String(address || "");
    return value.length > 12 ? `${value.slice(0, 6)}...${value.slice(-4)}` : value;
  }

  function formatAccountValue(account) {
    const value = String(account || "").trim();
    if (!value) return "";
    return value.length > 12 ? `${value.slice(0, 4)}...${value.slice(-4)}` : value;
  }

  function accountDisplayName(account) {
    const value = String(account ?? "").trim();
    if (!value) return "";
    if (value === "0") return "Main account (0)";
    return `Subaccount ${formatAccountValue(value)}`;
  }

  function normalizeAddress(address) {
    return String(address || "").trim().toLowerCase();
  }

  function isAddress(address) {
    return /^0x[a-fA-F0-9]{40}$/.test(String(address || ""));
  }

  function accountNumber(account) {
    return account && account.accountNumber !== undefined && account.accountNumber !== null
      ? String(account.accountNumber)
      : "";
  }

  function setUrlAddress(address) {
    const url = new URL(window.location.href);
    url.searchParams.set("address", address);
    state.year = "custom";
    ensureCustomRangeDefaults();
    state.action = actionFilterParam() || "all";
    url.searchParams.set("year", state.year);
    url.searchParams.set("from", state.dateFrom);
    url.searchParams.set("to", state.dateTo);
    const actionParam = actionFilterParam();
    if (actionParam) url.searchParams.set("action", actionParam);
    else url.searchParams.delete("action");
    if (state.selectedChains.size && !chainSelectionIsDefault()) {
      url.searchParams.set("chains", selectedChainKeys().join(","));
    } else {
      url.searchParams.delete("chains");
    }
    url.searchParams.delete("fast");
    window.history.replaceState(null, "", url.toString());
  }

  function reviewNoteStorageKey(address = state.address, year = state.year) {
    return `${NOTE_STORAGE_PREFIX}:${normalizeAddress(address)}:${year || "all"}`;
  }

  function loadReviewNotes(address, year) {
    try {
      const raw = window.localStorage.getItem(reviewNoteStorageKey(address, year));
      const parsed = raw ? JSON.parse(raw) : {};
      return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
    } catch (error) {
      console.debug("Review notes unavailable:", error);
      return {};
    }
  }

  function saveReviewNotes() {
    try {
      const cleanEntries = Object.entries(state.reviewNotes || {})
        .map(([key, value]) => [key, String(value || "").trim()])
        .filter(([, value]) => value);
      window.localStorage.setItem(reviewNoteStorageKey(), JSON.stringify(Object.fromEntries(cleanEntries)));
    } catch (error) {
      console.debug("Review notes could not be saved:", error);
    }
  }

  function reviewNoteForRow(row) {
    return String(state.reviewNotes?.[row.key] || "");
  }

  function reviewNotesForExport(rows = state.filteredRows) {
    return rows
      .map(row => ({
        chainKey: row.chainKey,
        chain: CHAINS[row.chainKey]?.name || row.chainKey,
        txHash: row.txHash,
        note: reviewNoteForRow(row).trim(),
      }))
      .filter(item => item.note);
  }

  function shortWarnings() {
    const warnings = activeDataWarnings();
    const visible = warnings.slice(0, 2).join(" | ");
    return warnings.length > 2 ? `${visible} | +${warnings.length - 2} more` : visible;
  }

  function csvCell(value) {
    const text = String(value ?? "");
    return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function escapeAttr(value) {
    return escapeHtml(value);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
