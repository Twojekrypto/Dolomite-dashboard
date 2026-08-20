const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const root = path.resolve(__dirname, "..");
const portfolioSource = fs.readFileSync(path.join(root, "portfolio-preview.html"), "utf8");
const historySource = fs.readFileSync(path.join(root, "history", "history.js"), "utf8");
const walletUxSource = fs.readFileSync(path.join(root, "wallet-table-ux.js"), "utf8");
const walletUxCss = fs.readFileSync(path.join(root, "wallet-table-ux.css"), "utf8");
const assetsSource = fs.readFileSync(path.join(root, "assets-preview.html"), "utf8");

function portfolioRiskLoader(fetchImpl) {
  const start = portfolioSource.indexOf("  const normAddr = a =>");
  const end = portfolioSource.indexOf("  async function sgQuery", start);
  assert.notEqual(start, -1, "portfolio address helper must exist");
  assert.notEqual(end, -1, "portfolio risk loader must exist");
  const fragment = portfolioSource.slice(start, end);
  const sandbox = { fetch: fetchImpl, Promise, Object, Array };
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(`${fragment}\nglobalThis.api = { loadRiskForWallet };`, sandbox);
  return sandbox.api.loadRiskForWallet;
}

function historyApi() {
  const marker = "\n  if (document.readyState === \"loading\") {";
  const instrumented = historySource.replace(
    marker,
    "\n  globalThis.__historyCloseBorrowTest = { displayActionsForRow, rowMatchesActionFilter, cleanTransactionAction };" + marker,
  );
  const sandbox = {
    console,
    URL,
    URLSearchParams,
    Blob,
    Set,
    Map,
    Date,
    Math,
    Intl,
    setInterval() { return 1; },
    clearInterval() {},
    setTimeout() { return 1; },
    clearTimeout() {},
    fetch() { return Promise.reject(new Error("fetch disabled")); },
    localStorage: { getItem() { return null; }, setItem() {}, removeItem() {} },
    location: { search: "", pathname: "/history/", origin: "https://example.test" },
    history: { replaceState() {} },
    document: { readyState: "loading", addEventListener() {}, createElement() { return {}; }, body: { appendChild() {} }, getElementById() { return null; }, querySelectorAll() { return []; } },
    window: {},
  };
  sandbox.window = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(instrumented, sandbox);
  return sandbox.__historyCloseBorrowTest;
}

test("Portfolio falls back to the current Lending Positions payload when a wallet shard is stale", async () => {
  const wallet = "0x32cd32b45277bf025c1df7bb4388e6a412b77fe5";
  const calls = [];
  const loadRiskForWallet = portfolioRiskLoader(async url => {
    calls.push(url);
    const payload = url === "data/liquidation-risk/manifest.json"
      ? { generatedAtISO: "2026-07-13T15:14:53Z", chains: { ethereum: {} } }
      : url === "data/liquidation-risk/ethereum/32.json"
        ? { positions: [] }
        : url === "liquidation_risk.json"
          ? { generatedAt: 1787226320, positions: [{ address: wallet, chain: "ethereum", debtUSD: 1000.26 }] }
          : null;
    return { ok: !!payload, json: async () => payload };
  });

  const result = await loadRiskForWallet(wallet);
  assert.deepEqual(JSON.parse(JSON.stringify(result.positions)), [{ address: wallet, chain: "ethereum", debtUSD: 1000.26 }]);
  assert.equal(result.updatedAt, 1787226320);
  assert.deepEqual(calls, [
    "data/liquidation-risk/manifest.json",
    "data/liquidation-risk/ethereum/32.json",
    "liquidation_risk.json",
  ]);
});

test("a receipt-confirmed Close Borrow suppresses its internal transfer and trade from History actions", () => {
  const api = historyApi();
  const row = {
    actions: new Set(["transfer", "trade"]),
    semanticActions: new Set(["closeBorrow"]),
    events: [
      { action: "transfer", isSelfTransfer: true, taxCategory: "protocol_transfer", borrowSemanticAction: "closeBorrow", legs: [] },
      { action: "trade", taxCategory: "swap", legs: [] },
    ],
  };

  assert.deepEqual(Array.from(api.displayActionsForRow(row)), ["closeBorrow"]);
  assert.equal(api.cleanTransactionAction(row), "Close Borrow");
  assert.equal(api.rowMatchesActionFilter(row, "closeBorrow"), true);
  assert.equal(api.rowMatchesActionFilter(row, "transfer"), false);
  assert.equal(api.rowMatchesActionFilter(row, "swap"), false);
});

test("the native All Routes filter portals while open and preserves the green selected state", () => {
  const syncStart = walletUxSource.indexOf("  function syncDropdownPortals(scope){");
  const syncEnd = walletUxSource.indexOf("\n  function runEnhancements()", syncStart);
  const routeClickStart = walletUxSource.indexOf("  function handleRouteClick(event){");
  const routeClickEnd = walletUxSource.indexOf("\n  function portalDropdown", routeClickStart);
  const sync = walletUxSource.slice(syncStart, syncEnd);
  const routeClick = walletUxSource.slice(routeClickStart, routeClickEnd);

  assert.doesNotMatch(sync, /routeModel === "native"[\s\S]{0,120}return;/);
  assert.match(routeClick, /if\(anyDropdown && anyDropdown\.dataset\.routeModel === "native"\) return;/);
  assert.match(portfolioSource, /refreshExerciseRouteDd\(dd, filterState\)/);
  assert.match(walletUxCss, /\.pf-exercise-route-filter\.dolo-dropdown-portal \.dd-btn\.filtered\{[^}]*background:rgba\(117,184,123,\.075\)/);
});

test("Assets reuses the dashboard empty-state shell for an unmatched address", () => {
  assert.match(assetsSource, /<tr class="dolo-empty-state-row assets-empty-row"><td colspan="6" class="dolo-empty-state-cell"[^>]*><div class="dolo-empty-state" role="status"><span>No assets found<\/span>/);
});
