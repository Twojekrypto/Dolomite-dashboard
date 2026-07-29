# Supply Health and Supply Market Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align Supply Pool Health, Dolomite Supply market selection, and Borrow card headings while preserving raw market data and calculations.

**Architecture:** Keep the existing TVL and Supply renderers. Add pure, Node-testable presentation and selection helpers to their current JavaScript files, use exact `chain + token address` keys for GM markets and deep links, then make surgical markup/CSS changes to the affected tables and headers. Do not introduce a new dependency or refactor unrelated token maps.

**Tech Stack:** Static HTML/CSS, browser JavaScript, Node `node:test`, Python `unittest`, Playwright, GitHub Pages route loaders.

## Global Constraints

- Preserve the existing Graphite + Gold identity and DOLO Holders interaction pattern.
- Supply Pool Health desktop columns are exactly `Chain, Asset, Supply, Suppliers, Top 10, Largest, 30D Change, Quality, Details`.
- Do not change Supply Health scoring, concentration methodology, supply history, supplier calculations, activity calculations, token IDs, market IDs, oracle prices, or interest indexes.
- Hide past date-coded `dPT` markets, but do not remove all zero-liquidity markets.
- Use precise address matching; do not select a market by symbol.
- The asset dropdown is single-select and must not show a checkbox/checkmark affordance.
- Keep the ten-row Supply Health viewport stable after filters and expansion.
- Use `python3 -m http.server` for browser testing and verify computed styles plus bounding boxes.
- Add no dependency, environment configuration, or secret.

---

### Task 1: Supply market presentation and eligibility helpers

**Files:**
- Modify: `supply/supply-draft.js:1-55`
- Modify: `supply/supply-draft.js:1180-1530`
- Test: `tests/supply-ui-behavior.test.js`

**Interfaces:**
- Consumes: raw Supply token objects shaped as `{ id, symbol, name, marketId, supplyLiquidityUSD }` and a chain key string.
- Produces:
  - `getSupplyMarketPresentation(token, chain) -> { symbol, name, icon }`
  - `isExpiredSupplyMarket(token, nowMs = Date.now()) -> boolean`
  - `getSelectableSupplyMarkets(tokens, chain, nowMs = Date.now()) -> token[]`
  - `filterSupplyMarketOptions(tokens, query, chain, nowMs = Date.now()) -> token[]`
  - `parseSupplyMarketDeepLink(search) -> { chain, asset } | null`

- [ ] **Step 1: Add failing presentation, expiry, duplicate, search, and deep-link tests**

Add these cases to `tests/supply-ui-behavior.test.js`:

```js
const DGM_BTC = '0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef';
const SAVETH_BASE = '0x23e3df1196b3249c9b0a9476f990f105591872de';
const DSAVETH = '0x51bc8e41cbec0aa97ec07c73597829c70b2eed46';
const SAVETH_ICON = 'https://app.dolomite.io/static/media/savETH.1c28535854c4a65f2a4786a2f02ae499.svg';

test('Supply resolves exact GM labels and icons by chain plus address', () => {
  const presentation = ui.getSupplyMarketPresentation({
    id: DGM_BTC,
    symbol: 'dGM',
    name: 'Dolomite Isolation: GMX Market',
  }, 'arbitrum');
  assert.equal(presentation.symbol, 'gmBTC-USD');
  assert.match(presentation.icon, /WBTC-GM/);
});

test('Supply presents the active dsavETH wrapper as savETH with the official icon', () => {
  const presentation = ui.getSupplyMarketPresentation({
    id: DSAVETH,
    symbol: 'dsavETH',
    name: 'Dolomite Isolation: Staked avETH',
  }, 'arbitrum');
  assert.equal(presentation.symbol, 'savETH');
  assert.equal(presentation.icon, SAVETH_ICON);
});

test('Supply removes the obsolete savETH duplicate when active dsavETH exists', () => {
  const tokens = [
    { id: SAVETH_BASE, symbol: 'savETH', supplyLiquidityUSD: '0' },
    { id: DSAVETH, symbol: 'dsavETH', supplyLiquidityUSD: '1081512.89' },
  ];
  assert.deepEqual(
    ui.getSelectableSupplyMarkets(tokens, 'arbitrum').map(token => token.id),
    [DSAVETH],
  );
});

test('Supply removes matured dPT markets but preserves future and undated tokens', () => {
  const now = Date.UTC(2026, 6, 29);
  const tokens = [
    { id: '0x1', symbol: 'dPT-rsETH-26SEP2024' },
    { id: '0x2', symbol: 'dPT-rsETH-26SEP2027' },
    { id: '0x3', symbol: 'WETH' },
  ];
  assert.deepEqual(
    ui.getSelectableSupplyMarkets(tokens, 'arbitrum', now).map(token => token.id),
    ['0x2', '0x3'],
  );
});

test('Supply asset search matches resolved and raw market identities', () => {
  const token = {
    id: DGM_BTC,
    symbol: 'dGM',
    name: 'Dolomite Isolation: GMX Market',
  };
  assert.deepEqual(ui.filterSupplyMarketOptions([token], 'gmBTC-USD', 'arbitrum'), [token]);
  assert.deepEqual(ui.filterSupplyMarketOptions([token], 'dGM', 'arbitrum'), [token]);
});

test('Supply deep links require a supported chain and exact token address', () => {
  assert.deepEqual(
    ui.parseSupplyMarketDeepLink(`?chain=arbitrum&asset=${DGM_BTC}`),
    { chain: 'arbitrum', asset: DGM_BTC },
  );
  assert.equal(ui.parseSupplyMarketDeepLink('?chain=arbitrum&asset=dGM'), null);
  assert.equal(ui.parseSupplyMarketDeepLink('?chain=unknown&asset=0x1111111111111111111111111111111111111111'), null);
});
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
node --test tests/supply-ui-behavior.test.js
```

Expected: failures state that the five helper exports do not exist.

- [ ] **Step 3: Implement the pure helpers before the Node/browser split**

At the top of `supply/supply-draft.js`, define the official icon base, month
map, supported chain set, exact GM market presentations, and savETH identities.
Use the existing official filenames already present later in the file:

```js
const SUPPLY_ASSET_ICON_CDN = 'https://app.dolomite.io/static/media/';
const SUPPLY_SUPPORTED_CHAINS = new Set([
  'ethereum', 'berachain', 'arbitrum', 'mantle', 'xlayer', 'base', 'polygon_zkevm',
]);
const SUPPLY_SAVETH_BASE = '0x23e3df1196b3249c9b0a9476f990f105591872de';
const SUPPLY_DSAVETH = '0x51bc8e41cbec0aa97ec07c73597829c70b2eed46';
const SUPPLY_SAVETH_ICON = SUPPLY_ASSET_ICON_CDN + 'savETH.1c28535854c4a65f2a4786a2f02ae499.svg';
const SUPPLY_MARKET_PRESENTATIONS = {
  'arbitrum:0x2c799166c9f0dbf9efc5004cbce4c5a37fa39329': ['gmARB-USD', 'ARB-GM.50df3ed4a1a52b938992cb5e08efbc36.svg'],
  'arbitrum:0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef': ['gmBTC-USD', 'WBTC-GM.6e7f69538bb02b42b881b86aea5c6d6e.svg'],
  'arbitrum:0x505582242757f16d72f8c4462a616e388ca1b074': ['gmETH-USD', 'ETH-GM.0b7d447f3c11298af07411c926352c71.svg'],
  'arbitrum:0x18cb14564fbb015bd3439220d177799355abc0e0': ['gmLINK-USD', 'LINK-GM.7d4b33346ec9822f9dc7c22a393f7698.svg'],
  'arbitrum:0xb15bbbfcff6c411410c66642306d1ffa7ecec4d8': ['gmBTC', 'WBTC-GM.6e7f69538bb02b42b881b86aea5c6d6e.svg'],
  'arbitrum:0x2d165a76dd3e552df3860789331ab73c5a3d7f92': ['gmETH', 'ETH-GM.0b7d447f3c11298af07411c926352c71.svg'],
  'arbitrum:0x20d51cb520c4622dcc3d7e35003dbab07d547e7e': ['gmUNI-USD', 'UNI-GM.8a4dfd0dc79f5b60138039338d28a6c7.svg'],
  'arbitrum:0x24c9121c75c099b38d40020872b8a0d2c27c614d': ['gmAAVE-USD', 'gmAAVE.e032a2febd818f511cf782e09b12f212.svg'],
  'arbitrum:0x1beed3b7d1237b7773b5c4c249933e3ca5e027c1': ['gmDOGE-USD', 'gmDOGE.36090e2ebcd305890c779e005d41d331.svg'],
  'arbitrum:0x5c99f6cf6069698d234d50bf69ebd2f53e45ed1c': ['gmGMX-USD', 'gmGMX.2c5cb2e0f1769629b38580607b77ecbc.svg'],
  'arbitrum:0x1ebb1c7023addbb2b6e30e6f4c8d4a4440bfd412': ['gmSOL-USD', 'gmSOL.73d56a4a2dcf3d39fc5c946b8c65c631.svg'],
  'arbitrum:0xc587646f67b38739006ed0200e2e0a26fdb01c9b': ['gmWstETH-USD', 'wstETH.2e97640d284bbe78da3776549d27ec47.svg'],
  'arbitrum:0xcf248baf933c7b1b876b997246f25021a65383b3': ['gmGMX', 'gmGMX.2c5cb2e0f1769629b38580607b77ecbc.svg'],
  'arbitrum:0xe5d6fe410c69b44c357403a1936b3bfaddbe340b': ['gmPENDLE-USD', 'gmPENDLE.cd8acede00414f70056c0fb9aa2baa7c.svg'],
  'arbitrum:0x6586f1db71513daf94b0431156d225a46c00f20b': ['gmPEPE-USD', 'gmPEPE.966f4beb1b823729066c29c52921b664.svg'],
  'arbitrum:0xf5063b40fa66ab2fbda2e6807ac5759a41a1b0c3': ['gmWIF-USD', 'gmWIF.8dfcfc27c0c56651a2e523e97c7fdcb4.svg'],
};

const SUPPLY_MATURITY_MONTHS = {
  jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
  jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11,
};

function normalizeSupplyAddress(value) {
  return String(value || '').trim().toLowerCase();
}

function getSupplyMarketPresentation(token, chain) {
  const chainKey = String(chain || '').trim().toLowerCase();
  const address = normalizeSupplyAddress(token?.id);
  const mapped = SUPPLY_MARKET_PRESENTATIONS[`${chainKey}:${address}`];

  if (mapped) {
    return {
      symbol: mapped[0],
      name: String(token?.name || 'Dolomite GM Market'),
      icon: SUPPLY_ASSET_ICON_CDN + mapped[1],
    };
  }

  if (chainKey === 'arbitrum' && address === SUPPLY_DSAVETH) {
    return {
      symbol: 'savETH',
      name: String(token?.name || 'Savant ETH'),
      icon: SUPPLY_SAVETH_ICON,
    };
  }

  return {
    symbol: String(token?.symbol || ''),
    name: String(token?.name || ''),
    icon: String(token?.icon || ''),
  };
}

function isExpiredSupplyMarket(token, nowMs = Date.now()) {
  if (
    token?.isActive === false
    || token?.active === false
    || String(token?.status || '').toLowerCase() === 'inactive'
  ) {
    return true;
  }

  const match = String(token?.symbol || '').match(
    /^dPT-.+-(\d{1,2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{4})$/i,
  );
  if (!match) return false;

  const month = SUPPLY_MATURITY_MONTHS[match[2].toLowerCase()];
  const maturityMs = Date.UTC(Number(match[3]), month, Number(match[1]), 23, 59, 59, 999);
  return Number.isFinite(maturityMs) && maturityMs < Number(nowMs);
}

function getSelectableSupplyMarkets(tokens, chain, nowMs = Date.now()) {
  const chainKey = String(chain || '').trim().toLowerCase();
  const active = (Array.isArray(tokens) ? tokens : []).filter(
    (token) => !isExpiredSupplyMarket(token, nowMs),
  );
  const hasActiveWrappedSavEth = chainKey === 'arbitrum' && active.some((token) => (
    normalizeSupplyAddress(token?.id) === SUPPLY_DSAVETH
    && Number(token?.supplyLiquidityUSD || 0) > 0
  ));

  return active.filter((token) => !(
    hasActiveWrappedSavEth
    && normalizeSupplyAddress(token?.id) === SUPPLY_SAVETH_BASE
  ));
}

function filterSupplyMarketOptions(tokens, query, chain, nowMs = Date.now()) {
  const normalizedQuery = String(query || '').trim().toLowerCase();
  const options = getSelectableSupplyMarkets(tokens, chain, nowMs);
  if (!normalizedQuery) return options;

  return options.filter((token) => {
    const presentation = getSupplyMarketPresentation(token, chain);
    return [
      presentation.symbol,
      presentation.name,
      token?.symbol,
      token?.name,
      token?.id,
    ].some((value) => String(value || '').toLowerCase().includes(normalizedQuery));
  });
}

function parseSupplyMarketDeepLink(search) {
  const params = new URLSearchParams(String(search || '').replace(/^\?/, ''));
  const chain = String(params.get('chain') || '').toLowerCase();
  const asset = normalizeSupplyAddress(params.get('asset'));
  if (!SUPPLY_SUPPORTED_CHAINS.has(chain) || !/^0x[a-f0-9]{40}$/.test(asset)) {
    return null;
  }
  return { chain, asset };
}
```

Use these bodies so fallback presentation preserves raw metadata, expiry uses
the same `DDMMMYYYY` rule as Supply Health, and duplicate removal only occurs
when `SUPPLY_DSAVETH` has positive supply liquidity. Extend the existing Node
export object with the five helpers instead of adding a second
`module.exports`.

- [ ] **Step 4: Run the tests and verify GREEN**

Run:

```bash
node --test tests/supply-ui-behavior.test.js
node --check supply/supply-draft.js
```

Expected: all Supply UI behavior tests pass and syntax check exits 0.

- [ ] **Step 5: Commit the pure behavior**

```bash
git add supply/supply-draft.js tests/supply-ui-behavior.test.js
git commit -m "feat: normalize selectable supply markets"
```

---

### Task 2: Supply Pool Health column and market-link contract

**Files:**
- Modify: `tests/supply-pool-health.test.js`
- Modify: `tests/test_supply_pool_health_contracts.py`
- Modify: `tvl-preview.html:928-942`
- Modify: `tvl/supply-health.js:15-690`
- Modify: `tvl/supply-health.css:250-760`

**Interfaces:**
- Consumes: Supply Health market rows containing `chain`, `tokenId`, and the existing presentation resolver.
- Produces:
  - `buildSupplyMarketHref(market) -> string`
  - nine-column table markup with a final `data-health-toggle` button.

- [ ] **Step 1: Add failing unit and structural tests**

Add to `tests/supply-pool-health.test.js`:

```js
test('Supply Health builds an exact chain and address market link', () => {
  assert.equal(
    buildSupplyMarketHref({
      chain: 'arbitrum',
      tokenId: '0x1E8E8B7A2F827B3BC12B00EE402145061B7050EF',
    }),
    './supply/?chain=arbitrum&asset=0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef',
  );
});
```

Import `buildSupplyMarketHref` from `tvl/supply-health.js`.

In `tests/test_supply_pool_health_contracts.py`, replace the old Average and
centering expectations with assertions for:

```python
self.assertIn('<th data-health-sort="chain"><span class="th-content">Chain', html)
self.assertNotIn('data-health-sort="avgWalletUsd"', html)
self.assertIn('<th class="supply-health-details-head">Details</th>', html)
self.assertIn('class="supply-health-chain-cell"', source)
self.assertIn('class="supply-health-details-cell"', source)
self.assertIn('<td colspan="9">', source)
hover_start = styles.index(
    "#supply-health-card .supply-health-table thead th[data-health-sort]:hover"
)
hover_end = styles.index("}", hover_start)
header_hover_block = styles[hover_start:hover_end + 1]
self.assertNotIn('background: var(--bg-3);', header_hover_block)
```

Also assert that the new centered selectors target columns 4–9 and that no
Supply Health selector still treats the old Average column as column 4.

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
node --test tests/supply-pool-health.test.js
python3 -m unittest tests.test_supply_pool_health_contracts
```

Expected: failures mention the missing href helper, old Average header, old
eight-column colspans, and header hover background.

- [ ] **Step 3: Implement the nine-column markup and renderer**

Change the TVL table header to:

```html
<th data-health-sort="chain"><span class="th-content">Chain <span class="sort-arrow"></span></span></th>
<th data-health-sort="symbol"><span class="th-content">Asset <span class="sort-arrow"></span></span></th>
<th class="num" data-health-sort="supplyUsd"><span class="th-content">Supply <span class="sort-arrow"></span></span></th>
<th class="num" data-health-sort="wallets"><span class="th-content">Suppliers <span class="sort-arrow"></span></span></th>
<th class="num" data-health-sort="top10Pct"><span class="th-content">Top 10 <span class="sort-arrow"></span></span></th>
<th class="num" data-health-sort="largestPct"><span class="th-content">Largest <span class="sort-arrow"></span></span></th>
<th class="num" data-health-sort="supply30dPct"><span class="th-content">30D Change <span class="sort-arrow"></span></span></th>
<th class="num" data-health-sort="scoreTotal"><span class="th-content">Quality <span class="sort-arrow"></span></span></th>
<th class="supply-health-details-head">Details</th>
```

In `tvl/supply-health.js`, add `chain` to `healthSortValue` and add:

```js
function buildSupplyMarketHref(market) {
  const chain = String(market?.chain || '').trim().toLowerCase();
  const asset = String(market?.tokenId || '').trim().toLowerCase();
  if (!chain || !/^0x[a-f0-9]{40}$/.test(asset)) return './supply/';
  return `./supply/?chain=${encodeURIComponent(chain)}&asset=${encodeURIComponent(asset)}`;
}

function getSupplyHealthChainPresentation(chainKey) {
  const normalized = String(chainKey || '').trim().toLowerCase();
  return healthChains.find((chain) => chain.key === normalized) || {
    key: normalized,
    label: chainKey || '—',
    icon: 'dolomite-logo.svg',
  };
}
```

For each row, render the Lending Positions-style chain badge first:

```js
const chain = getSupplyHealthChainPresentation(market.chain);
const chainCell = `
  <td class="supply-health-chain-cell">
    <span class="supply-health-chain-badge">
      <img src="${escapeHtml(chain.icon)}" alt="" aria-hidden="true">
      <span>${escapeHtml(chain.label)}</span>
    </span>
  </td>`;
```

Remove the expander from the Asset cell and render it after Quality:

```js
const detailsCell = `
  <td class="supply-health-details-cell">
    <button type="button" class="supply-health-row-toggle"
      data-health-toggle="${escapeHtml(market.key)}"
      aria-label="Show ${escapeHtml(market.symbol)} details"
      aria-expanded="${expanded ? 'true' : 'false'}">
      <span aria-hidden="true">⌄</span>
    </button>
  </td>`;
```

Change detail, empty, loading, error, and spacer `colspan` values from 8 to 9.
Inside the expanded detail renderer, build the action exactly from the market:

```js
const supplyHref = buildSupplyMarketHref(market);
const marketAction = `
  <a class="supply-health-market-link" href="${escapeHtml(supplyHref)}">
    Open Supply markets
  </a>`;
```

Keep the current data-row hover/click expansion, but ignore row clicks that
originate inside `a`, `button`, or another interactive descendant. Export
`buildSupplyMarketHref` from the existing Node export.

- [ ] **Step 4: Update widths and all positional CSS**

Use this desktop width contract:

```css
th:nth-child(1) { width: 12%; } /* Chain */
th:nth-child(2) { width: 23%; } /* Asset */
th:nth-child(3) { width: 13%; } /* Supply */
th:nth-child(4) { width: 10%; } /* Suppliers */
th:nth-child(5),
th:nth-child(6) { width: 9%; }  /* Concentration */
th:nth-child(7) { width: 10%; } /* 30D */
th:nth-child(8) { width: 9%; }  /* Quality */
th:nth-child(9) { width: 5%; }  /* Details */
```

Columns 4–9 are centered; Supply (column 3) stays right-aligned. Add a
scoped `.supply-health-chain-badge` matching Lending Positions (`15px` icon,
`12.5px`, weight `500`, `7px` gap). Make the final cell/button centered.
Audit every desktop and responsive `nth-child` selector against the new
nine-column order. At the narrow dashboard breakpoint, keep Chain, Asset,
Quality, and Details visible; hide only lower-priority numeric columns if the
existing responsive pattern requires it.

Change header hover to:

```css
#supply-health-card .supply-health-table thead th[data-health-sort]:hover {
  color: var(--fg-1);
}
```

Do not change the header background. Preserve the existing data-row hover and
gold rail.

- [ ] **Step 5: Run targeted tests and verify GREEN**

Run:

```bash
node --test tests/supply-pool-health.test.js
python3 -m unittest tests.test_supply_pool_health_contracts
node --check tvl/supply-health.js
git diff --check
```

Expected: all pass.

- [ ] **Step 6: Commit Supply Health**

```bash
git add tvl-preview.html tvl/supply-health.js tvl/supply-health.css tests/supply-pool-health.test.js tests/test_supply_pool_health_contracts.py
git commit -m "feat: align Supply Health market navigation"
```

---

### Task 3: Dolomite Supply selector rendering and deep-link application

**Files:**
- Modify: `supply/supply-draft.js:1200-1535`
- Modify: `supply/supply-draft.css:370-470`
- Modify: `tests/test_supply_table_ux_contracts.py`
- Modify: `tests/token-icon-presentation-contract.test.js`

**Interfaces:**
- Consumes: Task 1 pure helpers, current hidden controls `#supply-chain-select`
  and `#supply-asset-select`, and the existing `selectSupplyChain` /
  `selectSupplyAsset` functions.
- Produces: filtered single-select options, exact display label/icon, and an
  auto-applied deep-linked market.

- [ ] **Step 1: Add failing selector and icon contract tests**

Extend `tests/test_supply_table_ux_contracts.py` with source contracts:

```python
self.assertIn("filterSupplyMarketOptions(currentSupplyTokensList", source)
self.assertIn("option.setAttribute('aria-selected', selected ? 'true' : 'false')", source)
self.assertIn("activateSupplyMarketDeepLink()", source)
self.assertIn("getSupplyMarketPresentation(token, getCurrentSupplyChain())", source)
self.assertIn(
    "body.supply-draft-route #asset-options-container "
    ".premium-supply-dropdown-item::before",
    styles,
)
checker_start = styles.index(
    "body.supply-draft-route #asset-options-container "
    ".premium-supply-dropdown-item::before"
)
checker_end = styles.index("}", checker_start)
asset_checker_block = styles[checker_start:checker_end + 1]
self.assertIn(
    "body.supply-draft-route #asset-options-container "
    ".premium-supply-dropdown-item.active::after",
    styles,
)
self.assertIn("display: none !important", asset_checker_block)
suffix_start = styles.index(
    "body.supply-draft-route #asset-options-container "
    ".premium-supply-dropdown-item.active::after"
)
suffix_end = styles.index("}", suffix_start)
selected_suffix_block = styles[suffix_start:suffix_end + 1]
self.assertIn("content: none !important", selected_suffix_block)
```

Extend `tests/token-icon-presentation-contract.test.js` to require
`supply/supply-draft.js` and assert that `dsavETH` resolves to the same official
icon as Assets and that the known GM address resolves to the WBTC-GM icon.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 -m unittest tests.test_supply_table_ux_contracts
node --test tests/token-icon-presentation-contract.test.js
```

Expected: failures show the old original renderer path, missing deep-link
activation, and visible single-select checker.

- [ ] **Step 3: Replace the Supply route option wrapper with an exact renderer**

Add the current-chain helper beside the selection state:

```js
const SUPPLY_CHAIN_LABELS = {
  ethereum: 'Ethereum',
  berachain: 'Berachain',
  arbitrum: 'Arbitrum',
  mantle: 'Mantle',
  base: 'Botanix',
  polygon_zkevm: 'Polygon zkEVM',
  xlayer: 'X Layer',
};

function getCurrentSupplyChain() {
  return String(
    document.getElementById('supply-chain-select')?.value || 'ethereum'
  ).toLowerCase();
}
```

Replace the current `originalRenderSupplyAssetOptions` wrapper with:

```js
function patchOptionsRenderer() {
  if (optionsPatched || typeof window.renderSupplyAssetOptions !== 'function') return;
  optionsPatched = true;

  window.renderSupplyAssetOptions = function supplyDraftRenderSupplyAssetOptions() {
    const container = document.getElementById('asset-options-container');
    if (!container) return;
    const chain = getCurrentSupplyChain();
    const query = document.getElementById('asset-search-input')?.value || '';
    const options = filterSupplyMarketOptions(
      currentSupplyTokensList,
      query,
      chain,
    );
    container.replaceChildren();

    if (!options.length) {
      const empty = document.createElement('div');
      empty.className = 'supply-draft-option-empty';
      empty.textContent = 'No active supply markets found';
      container.appendChild(empty);
      return;
    }

    options.forEach((token) => {
      const presentation = getSupplyMarketPresentation(token, chain);
      const selected = token.id === stagedAssetId;
      const option = document.createElement('button');
      option.type = 'button';
      option.className = `premium-supply-dropdown-item${selected ? ' active' : ''}`;
      option.dataset.assetId = token.id;
      option.setAttribute('aria-selected', selected ? 'true' : 'false');
      option.innerHTML = `
        <img src="${supplyDraftEscape(presentation.icon || getIconPath(token))}"
          alt="" aria-hidden="true">
        <span class="supply-draft-option-copy">
          <strong>${supplyDraftEscape(presentation.symbol)}</strong>
          <small>${supplyDraftEscape(
            `${token.id.slice(0, 6)}...${token.id.slice(-4)}`
          )}</small>
        </span>`;
      option.addEventListener('click', () => stageSupplyAsset(token.id));
      container.appendChild(option);
    });

    installAssetSearchClear();
    if (!activateSupplyMarketDeepLink()) {
      setTimeout(autoApplyDefaultSupplyAsset, 0);
    }
  };

  window.filterSupplyAssets = function supplyDraftFilterSupplyAssets() {
    window.renderSupplyAssetOptions(currentSupplyTokensList);
  };
}
```

This deliberately rerenders from `currentSupplyTokensList`, because resolved
`gmBTC-USD` cannot match the raw `dGM` label before the presentation helper
runs. Preserve the raw address subtitle and existing Confirm workflow.

Update `setSelectorUi`, `syncEmptyState`, and `syncApplyButton` to call
`getSupplyMarketPresentation(token, getCurrentSupplyChain())` and use its
resolved symbol. Update `getIconPath` to resolve through the same helper, using
`token.id` and the hidden chain selector; do not require nonexistent
`token.chain` or `token.tokenId` fields.

- [ ] **Step 4: Apply valid deep links and preserve safe fallback**

Parse the URL once:

```js
const requestedSupplyMarket = parseSupplyMarketDeepLink(window.location.search);
let supplyDeepLinkApplied = false;
```

Make `getDefaultSupplyToken()` prefer `requestedSupplyMarket.asset` only when
the current chain matches and the token survives `getSelectableSupplyMarkets`.
In `boot()`, after selection functions are patched, call
`activateSupplyMarketDeepLink()`:

```js
function getDefaultSupplyToken() {
  let tokens = [];
  try {
    if (Array.isArray(currentSupplyTokensList)) {
      tokens = getSelectableSupplyMarkets(
        currentSupplyTokensList,
        getCurrentSupplyChain(),
      );
    }
  } catch (error) {}

  if (
    requestedSupplyMarket
    && requestedSupplyMarket.chain === getCurrentSupplyChain()
  ) {
    const requested = tokens.find(
      (token) => normalizeSupplyAddress(token.id) === requestedSupplyMarket.asset,
    );
    if (requested) return requested;
  }

  if (defaultSupplyAssetSymbol) {
    const configured = tokens.find(
      (token) => String(token?.symbol || '').toUpperCase() === defaultSupplyAssetSymbol,
    );
    if (configured) return configured;
  }
  return getLargestSupplyToken(tokens);
}

function activateSupplyMarketDeepLink() {
  if (!requestedSupplyMarket || supplyDeepLinkApplied) return false;
  const chain = requestedSupplyMarket.chain;
  const activeChain = getCurrentSupplyChain();
  if (activeChain !== chain) {
    const label = SUPPLY_CHAIN_LABELS[chain];
    window.selectSupplyChain(chain, label);
    return true;
  }
  let selectable = [];
  try {
    selectable = getSelectableSupplyMarkets(currentSupplyTokensList, chain);
  } catch (error) {}
  const token = selectable.find(
    (candidate) => normalizeSupplyAddress(candidate.id) === requestedSupplyMarket.asset,
  );
  if (!token) return false;
  supplyDeepLinkApplied = true;
  window.selectSupplyAsset(token.id, { auto: true });
  return true;
}
```

If the address is invalid, absent, expired, or removed as the savETH duplicate,
leave the existing largest-market default untouched.

- [ ] **Step 5: Remove multi-select affordances in scoped CSS**

Add exact overrides:

```css
body.supply-draft-route #asset-options-container .premium-supply-dropdown-item::before,
body.supply-draft-route #asset-options-container .premium-supply-dropdown-item.active::before {
  display: none !important;
  content: none !important;
}

body.supply-draft-route #asset-options-container .premium-supply-dropdown-item.active::after {
  display: none !important;
  content: none !important;
}
```

Keep the current restrained gold active-row background and border.

- [ ] **Step 6: Run targeted tests and verify GREEN**

Run:

```bash
node --test tests/supply-ui-behavior.test.js tests/token-icon-presentation-contract.test.js
python3 -m unittest tests.test_supply_table_ux_contracts
node --check supply/supply-draft.js
git diff --check
```

Expected: all pass.

- [ ] **Step 7: Commit Supply selection**

```bash
git add supply/supply-draft.js supply/supply-draft.css tests/supply-ui-behavior.test.js tests/test_supply_table_ux_contracts.py tests/token-icon-presentation-contract.test.js
git commit -m "feat: clarify Supply market selection"
```

---

### Task 4: Borrow heading parity with DOLO Holders

**Files:**
- Modify: `liquidation-preview.html:6300-6365`
- Modify: `liquidation-preview.html:6400-6470`
- Modify: `tests/test_borrow_ux_contracts.py`

**Interfaces:**
- Consumes: existing Lending Positions and Liquidation History `header-count`
  elements and Risk Simulator title/subtitle markup.
- Produces: one shared heading scale without changing copy, data freshness, or
  count semantics.

- [ ] **Step 1: Add failing Borrow heading contract tests**

Add to `tests/test_borrow_ux_contracts.py`:

```python
self.assertIn(
    "body.route-liquidation .liquidation-table-heading h3,\n"
    "        body.route-liquidation .liquidation-sim-title {",
    SOURCE,
)
heading_start = SOURCE.rindex(
    "body.route-liquidation .liquidation-table-heading h3,"
)
heading_block = SOURCE[heading_start:SOURCE.index("}", heading_start)]
self.assertIn("font-size: 16px !important", heading_block)
self.assertIn("font-weight: 600 !important", heading_block)

count_start = SOURCE.rindex(
    "body.route-liquidation .liquidation-table-title-row .header-count"
)
count_block = SOURCE[count_start:SOURCE.index("}", count_start)]
self.assertIn("font-size: 11px !important", count_block)
self.assertIn("background: var(--bg-3) !important", count_block)
self.assertIn("border: 1px solid var(--line-2) !important", count_block)
self.assertIn("border-radius: 999px !important", count_block)
```

Also assert the subtitle is `12px` and table header padding is
`20px 24px 18px`.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 -m unittest tests.test_borrow_ux_contracts
```

Expected: current Risk Simulator title is 15px and count lacks the DOLO badge
background/border.

- [ ] **Step 3: Add one final scoped parity block**

Append a final `body.route-liquidation` override block so it wins the existing
large-file cascade:

```css
body.route-liquidation .liquidation-table-heading h3,
body.route-liquidation .liquidation-sim-title {
  color: var(--fg-1) !important;
  font-size: 16px !important;
  font-weight: 600 !important;
  letter-spacing: -.1px !important;
  line-height: 1.2 !important;
}

body.route-liquidation .liquidation-table-title-row .header-count {
  padding: 3px 8px !important;
  border: 1px solid var(--line-2) !important;
  border-radius: 999px !important;
  background: var(--bg-3) !important;
  color: var(--fg-2) !important;
  font-family: var(--mono) !important;
  font-size: 11px !important;
  font-weight: 500 !important;
}

body.route-liquidation .liquidation-table-header {
  padding: 20px 24px 18px !important;
}

body.route-liquidation .liquidation-table-subtitle,
body.route-liquidation .liquidation-sim-subtitle {
  margin-top: 5px !important;
  color: var(--fg-3) !important;
  font-size: 12px !important;
  line-height: 1.45 !important;
}
```

Keep the existing title icons, descriptions, `Data updated` blocks, and count
text generation.

- [ ] **Step 4: Run tests and verify GREEN**

Run:

```bash
python3 -m unittest tests.test_borrow_ux_contracts
node --check <(sed -n '/<script>/,/<\\/script>/p' liquidation-preview.html)
git diff --check
```

If process substitution is unavailable, extract the inline script to a
temporary file with `apply_patch`, run `node --check`, then delete it with
`apply_patch`.

- [ ] **Step 5: Commit Borrow header parity**

```bash
git add liquidation-preview.html tests/test_borrow_ux_contracts.py
git commit -m "style: align Borrow card headings"
```

---

### Task 5: Cache versions, full regression, and browser verification

**Files:**
- Modify: `tvl-preview.html:14-15`
- Modify: `tvl/index.html:20-35`
- Modify: `supply/index.html:20-45`
- Modify: `borrow/index.html:20-35`
- Create temporarily, then delete: `.codex-verify-supply-parity.py`

**Interfaces:**
- Consumes: completed Tasks 1–4.
- Produces: cache-busted routes and evidence that the exact user journey works.

- [ ] **Step 1: Bump deterministic asset and route versions**

Use `20260729-supply-market-parity` in:

- `tvl/supply-health.css` and `tvl/supply-health.js` URLs;
- the TVL route version;
- `supply/supply-draft.css` and `supply/supply-draft.js` URLs;
- the Supply route version;
- the Borrow route version.

Do not use `Date.now()` cache busting.

- [ ] **Step 2: Run all targeted automated checks**

Run:

```bash
node --test tests/supply-pool-health.test.js tests/supply-ui-behavior.test.js tests/token-icon-presentation-contract.test.js
python3 -m unittest \
  tests.test_supply_pool_health_contracts \
  tests.test_supply_table_ux_contracts \
  tests.test_borrow_ux_contracts \
  tests.test_tvl_preview_contracts
node --check tvl/supply-health.js
node --check supply/supply-draft.js
git diff --check
```

Expected: all targeted checks pass.

- [ ] **Step 3: Run the broader project suites**

Run:

```bash
node --test tests/*.test.js
python3 -m unittest discover -s tests -p 'test_*.py'
```

Expected: no new failures. If an existing unrelated baseline failure remains,
record the exact test and prove that no file in its scope changed.

- [ ] **Step 4: Inspect the webapp helper before starting the server**

Run:

```bash
python3 skills/webapp-testing/scripts/with_server.py --help
```

Then create `.codex-verify-supply-parity.py` with Playwright. It must:

1. open `http://127.0.0.1:8000/tvl/?verify=20260729-supply-market-parity`;
2. wait for `#supply-health-table:not([hidden])`;
3. assert the nine header labels and absence of Average;
4. measure table height before and after search/expansion;
5. inspect computed header hover background before/after and assert it does
   not change;
6. inspect a row hover and assert its background changes plus its gold rail is
   visible;
7. expand `gmBTC-USD`, click `Open Supply markets`, and wait for the Supply
   route;
8. assert chain is `arbitrum`, hidden selected asset is the exact GM address,
   and visible selected text is `gmBTC-USD`;
9. open the asset dropdown and assert no expired `dPT`, only one visible
   `savETH`, unique GM labels, no `::before` checker, and official icon URLs;
10. resize to `820px` wide, return to Supply Pool Health, and assert Chain,
    Asset, Quality, and Details remain visible with no text/chevron overlap or
    destructive card overflow;
11. expand and collapse a row from the final chevron at `820px`;
12. open `/borrow/`, inspect title/count computed styles, and assert 16px/600
    titles plus 11px pill count.

- [ ] **Step 5: Execute browser verification**

Run:

```bash
python3 skills/webapp-testing/scripts/with_server.py \
  --server "python3 -m http.server 8000 --bind 127.0.0.1" \
  --port 8000 \
  -- python3 .codex-verify-supply-parity.py
```

Expected output:

```text
supply_health_columns=9
supply_health_height_stable=true
supply_market_deeplink=gmBTC-USD
supply_selector_unique=true
borrow_heading_parity=true
```

Delete the temporary script with `apply_patch` after the test.

- [ ] **Step 6: Perform the two required review passes**

Correctness/regression pass:

```bash
git diff -- tvl-preview.html tvl/supply-health.js tvl/supply-health.css \
  supply/supply-draft.js supply/supply-draft.css liquidation-preview.html \
  tvl/index.html supply/index.html borrow/index.html tests
rg -n "nth-child|colspan|Average|data-health-sort" tvl/supply-health.css tvl/supply-health.js tvl-preview.html
```

Maintainability/security pass:

```bash
rg -n "api[_-]?key|secret|password|token=" tvl/supply-health.js supply/supply-draft.js liquidation-preview.html
git status --short
```

Confirm that address maps are business/presentation logic, no config or secret
was added, and no generated data file changed.

- [ ] **Step 7: Commit cache versions and final verification adjustments**

```bash
git add tvl-preview.html tvl/index.html supply/index.html borrow/index.html
git commit -m "build: publish supply market parity assets"
```

- [ ] **Step 8: Prepare deployment handoff**

Verify the final commit list and remote divergence:

```bash
git status --short --branch
git log --oneline dolomite-dashboard/master..HEAD
git fetch dolomite-dashboard master
```

Do not push unless the user explicitly authorizes production deployment.
