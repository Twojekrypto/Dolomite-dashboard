# Supply Health Market Dossier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the approved institutional Supply Health market dossier and make its token icon presentation match Dolomite Assets.

**Architecture:** Keep the existing Supply Health module and data contract. Add one pure icon-presentation helper plus a pure exported detail renderer, then update only the current row/detail markup and scoped CSS. Reuse the TVL page's existing canonical icon resolver, frame classifier, and grayscale classifier rather than adding a new registry.

**Tech Stack:** Static HTML/CSS, browser JavaScript, Node `node:test`, Python `unittest`, Playwright, GitHub Pages.

## Global Constraints

- Preserve the existing Graphite + Gold identity and the exact Supply Health data contract.
- Do not change scoring, weights, grades, concentration methodology, sorting, filters, pagination, or table columns.
- Resolve icons with the displayed market symbol plus exact chain and token address.
- Match Dolomite Assets' 30px frame, full-logo, grayscale, and object-fit behavior.
- Keep the 662px Supply Health viewport stable and prevent horizontal overflow.
- Use no new dependency, configuration, data source, or secret.
- Use `python3 -m http.server` and verify computed styles plus bounding boxes.

---

### Task 1: Lock icon and dossier behavior with failing tests

**Files:**
- Modify: `tests/supply-pool-health.test.js`
- Modify: `tests/test_supply_pool_health_contracts.py`

**Interfaces:**
- Consumes: published Supply Health market objects and injected icon resolver/classifier functions.
- Produces test contracts for:
  - `getSupplyHealthIconPresentation(market, resolvers)`
  - `renderSupplyHealthDetail(market)`

- [ ] **Step 1: Add the icon presentation regression**

Add a Node test that uses literal resolver outputs and records the arguments:

```js
test('Supply Health icon presentation uses canonical market identity', () => {
  const calls = [];
  const market = {
    chain: 'arbitrum',
    tokenId: '0x1e8e8b7a2f827b3bc12b00ee402145061b7050ef',
    symbol: 'dGM',
  };
  const presentation = getSupplyHealthIconPresentation(market, {
    icon: (symbol, row) => {
      calls.push([symbol, row]);
      return 'official-gm-icon.svg';
    },
    frame: symbol => symbol.startsWith('gm') ? 'full-logo' : '',
    image: () => '',
  });

  assert.deepEqual(calls, [[
    'gmBTC-USD',
    { chain: 'arbitrum', addr: market.tokenId },
  ]]);
  assert.deepEqual(presentation, {
    src: 'official-gm-icon.svg',
    frameClass: 'full-logo',
    imageClass: '',
  });
});
```

The production change this catches is passing the raw wrapper symbol or
omitting the exact address, which can select a generic icon.

- [ ] **Step 2: Add the dossier renderer regression**

Use a literal market fixture and assert the consumer-visible HTML contains:

```js
const detail = renderSupplyHealthDetail({
  chain: 'ethereum',
  tokenId: '0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d',
  symbol: 'USD1',
  name: 'World Liberty Financial USD',
  supplyUsd: 278049648.05,
  score: {
    wallet: 96.5,
    concentration: 15,
    stability: 99.5,
    growth: 67.1,
    resilience: 60,
    total: 64.6,
    grade: 'C',
  },
  growth: {
    supply30dPct: 16.68,
    supply7dPct: -1.13,
    wallets30dPct: -8.96,
    avgDailyChange30dPct: 1.05,
  },
  topWallets: [{
    address: '0x5be9a4959308a0d0c7bc0870e319314d8d957dbb',
    sharePct: 40.02,
    usd: 111278882.78,
  }],
});

assert.match(detail, /Market intelligence/);
assert.match(detail, /Quality anatomy/);
assert.match(detail, /Market momentum/);
assert.match(detail, /Supply concentration/);
assert.match(detail, /supply-health-supplier-bar/);
assert.match(detail, /Open Supply market/);
assert.match(detail, /asset=0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d/);
```

The production change this catches is reverting to disconnected columns or
dropping supplier evidence/navigation from the dossier.

- [ ] **Step 3: Add scoped CSS/markup contracts**

In the Python contract suite, assert the detail output uses:

```python
self.assertIn('class="supply-health-detail-panel"', source)
self.assertIn('class="supply-health-detail-head"', source)
self.assertIn('class="supply-health-token-icon ${iconPresentation.frameClass}"', source)
self.assertIn("#supply-health-card .supply-health-detail-panel::before", styles)
self.assertIn("#supply-health-card .supply-health-detail-head", styles)
self.assertIn("#supply-health-card .supply-health-token-icon.full-logo", styles)
self.assertIn("@media (max-width: 1100px)", styles)
```

Also assert the obsolete direct image class
`class="supply-health-asset-icon"` no longer appears in the row renderer.

- [ ] **Step 4: Run tests and verify RED**

Run:

```bash
node --test tests/supply-pool-health.test.js
python3 -m unittest tests.test_supply_pool_health_contracts
```

Expected: failures identify missing helper export, missing dossier hierarchy,
and missing Assets-style token frame.

- [ ] **Step 5: Commit the test contract**

```bash
git add tests/supply-pool-health.test.js tests/test_supply_pool_health_contracts.py
git commit -m "test: define Supply Health market dossier"
```

### Task 2: Implement canonical icon presentation

**Files:**
- Modify: `tvl/supply-health.js`
- Modify: `tvl/supply-health.css`

**Interfaces:**
- Consumes:
  - `getSupplyHealthMarketPresentation(market)`
  - browser globals `tokenIcon`, `tokenIconFrameClass`, `tokenIconClass`
- Produces:
  - `getSupplyHealthIconPresentation(market, resolvers = {}) -> { src, frameClass, imageClass }`

- [ ] **Step 1: Add the pure helper**

Implement:

```js
function getSupplyHealthIconPresentation(market, resolvers = {}) {
  const presentation = getSupplyHealthMarketPresentation(market);
  const symbol = presentation.symbol || String(market?.symbol || '');
  const iconResolver = resolvers.icon
    || (typeof tokenIcon === 'function' ? tokenIcon : null);
  const frameResolver = resolvers.frame
    || (typeof tokenIconFrameClass === 'function' ? tokenIconFrameClass : null);
  const imageResolver = resolvers.image
    || (typeof tokenIconClass === 'function' ? tokenIconClass : null);
  const row = {
    chain: String(market?.chain || ''),
    addr: String(market?.tokenId || ''),
  };

  return {
    src: iconResolver ? iconResolver(symbol, row) : 'dolomite-logo.svg',
    frameClass: frameResolver ? frameResolver(symbol) : '',
    imageClass: imageResolver ? imageResolver(symbol) : '',
  };
}
```

Export the helper from the module.

- [ ] **Step 2: Use one icon presentation in the parent row**

Replace the direct image with:

```html
<span class="supply-health-token-icon ${iconPresentation.frameClass}">
  <img class="${iconPresentation.imageClass}"
    src="${escapeHealthHtml(iconPresentation.src)}"
    alt="" aria-hidden="true"
    onerror="this.src='dolomite-logo.svg'">
</span>
```

- [ ] **Step 3: Match Dolomite Assets icon geometry**

Add scoped styles equivalent to the Assets `.tok-ico` contract:

```css
#supply-health-card .supply-health-token-icon {
  display: grid;
  place-items: center;
  width: 30px;
  height: 30px;
  margin-right: 10px;
  flex: 0 0 30px;
  overflow: hidden;
  border: 1px solid var(--line-2);
  border-radius: 50%;
  background: var(--bg-3);
}

#supply-health-card .supply-health-token-icon img {
  width: 100%;
  height: 100%;
  object-fit: cover;
}

#supply-health-card .supply-health-token-icon img.grayscale {
  filter: grayscale(1) brightness(1.5);
}

#supply-health-card .supply-health-token-icon.full-logo {
  border-color: transparent;
  border-radius: 5px;
  background: transparent;
}

#supply-health-card .supply-health-token-icon.full-logo img {
  border-radius: 0;
  object-fit: contain;
}
```

- [ ] **Step 4: Run the icon test and verify GREEN**

Run:

```bash
node --test tests/supply-pool-health.test.js tests/token-icon-presentation-contract.test.js
```

Expected: icon-presentation tests pass.

### Task 3: Build the institutional dossier

**Files:**
- Modify: `tvl/supply-health.js`
- Modify: `tvl/supply-health.css`
- Modify: `tvl-preview.html`

**Interfaces:**
- Consumes: the existing Supply Health market payload.
- Produces: `renderSupplyHealthDetail(market) -> string` with one dossier shell,
  identity header, three evidence sections, and exact-address Supply action.

- [ ] **Step 1: Rebuild the detail renderer**

Make `renderSupplyHealthDetail` produce:

```html
<div class="supply-health-detail-panel">
  <div class="supply-health-detail-head">…identity…supply…quality…</div>
  <div class="supply-health-detail-content">
    <section class="supply-health-detail-section score">…</section>
    <section class="supply-health-detail-section momentum">…</section>
    <section class="supply-health-detail-section concentration">…</section>
  </div>
</div>
```

Reuse `renderSupplyHealthScoreBreakdown`, the existing stat values, exact
explorer links, `buildSupplyMarketHref`, and `getSupplyHealthIconPresentation`.
Add a proportional `.supply-health-supplier-bar` capped to `0..100`.

- [ ] **Step 2: Implement the panel and section hierarchy**

Replace the old flat `.supply-health-detail` column rules with:

- inset panel and quality spine;
- flex header with identity, supply, and quality blocks;
- a three-column evidence grid;
- quiet section dividers;
- lead 30D signal;
- supplier rows with share bars;
- exact Supply action with arrow.

Do not add colored section backgrounds or more than one gold signature.

- [ ] **Step 3: Implement responsive layouts**

At `max-width: 1100px`, use two columns and let concentration span the row.
At `max-width: 840px`, stack all sections, wrap the header, and remove vertical
section dividers. At `max-width: 560px`, make meta blocks and the Supply action
full width.

- [ ] **Step 4: Bump static asset versions**

Change only the existing query versions for:

```html
tvl/supply-health.css?v=20260730-market-dossier
tvl/supply-health.js?v=20260730-market-dossier
```

- [ ] **Step 5: Run targeted tests and verify GREEN**

Run:

```bash
node --test tests/supply-pool-health.test.js tests/token-icon-presentation-contract.test.js
python3 -m unittest tests.test_supply_pool_health_contracts
node --check tvl/supply-health.js
```

Expected: all commands exit 0.

- [ ] **Step 6: Commit the implementation**

```bash
git add tvl/supply-health.js tvl/supply-health.css tvl-preview.html
git commit -m "refine Supply Health market dossier"
```

### Task 4: Browser QA, full verification, and production push

**Files:**
- Verify: `tvl-preview.html`
- Verify: `tvl/supply-health.js`
- Verify: `tvl/supply-health.css`

**Interfaces:**
- Consumes: local HTTP-served TVL page and production GitHub Pages.
- Produces: browser evidence for geometry, icon parity, responsiveness, and
  navigation.

- [ ] **Step 1: Start the approved local server helper**

Run:

```bash
python3 skills/webapp-testing/scripts/with_server.py --help
```

Then run the reviewed literal server command:

```bash
python3 skills/webapp-testing/scripts/with_server.py \
  --server "python3 -m http.server 8765" --port 8765 \
  -- python3 /tmp/verify_supply_health_dossier.py
```

- [ ] **Step 2: Verify desktop behavior**

In Playwright at 1440px:

- open `/tvl-preview.html`;
- wait for Supply Health data;
- click the first visible Details control;
- assert the token frame is `30x30`;
- assert three evidence sections share one row;
- assert the panel `scrollWidth <= clientWidth`;
- inspect the gold spine and panel background via `getComputedStyle`;
- verify the Supply link contains the exact chain and token address.

- [ ] **Step 3: Verify mobile behavior**

At 390px:

- assert the evidence sections stack vertically;
- assert the detail panel remains inside the card/table viewport;
- assert the token identity, quality score, and Supply action stay visible;
- assert no horizontal page overflow.

- [ ] **Step 4: Run full repository verification**

Run the full Python and Node suites used by the repository:

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
node --test tests/*.test.js
```

Record any pre-existing unrelated baseline failure separately; do not describe
it as caused by this change.

- [ ] **Step 5: Perform two review passes**

1. Correctness/regression: compare the plan and diff line by line; confirm no
   data/scoring/table-column behavior changed.
2. Maintainability/security: confirm scoped CSS, escaped values, exact-address
   links, no secrets, no new config, and no unrelated edits.

- [ ] **Step 6: Commit documentation and push production**

```bash
git add docs/superpowers/specs/2026-07-30-supply-health-market-dossier-design.md \
  docs/superpowers/plans/2026-07-30-supply-health-market-dossier.md
git commit -m "docs: plan Supply Health market dossier"
git push dolomite-dashboard HEAD:master
```

- [ ] **Step 7: Confirm live deployment**

Check the Pages workflow for the pushed commit, wait for success, then open the
production TVL route with a cache-busting query and repeat the expanded-panel
smoke check.
