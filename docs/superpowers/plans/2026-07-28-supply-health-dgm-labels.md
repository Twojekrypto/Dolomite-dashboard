# Supply Pool Health dGM Labels Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace ambiguous repeated `dGM` labels with the exact GM market display symbols in Supply Pool Health.

**Architecture:** Add a pure address-based presentation resolver in `tvl/supply-health.js`. Use it for filtering and row rendering while keeping raw market identifiers and calculations untouched.

**Tech Stack:** Static HTML, browser JavaScript, Node test runner, Python Playwright.

## Global Constraints

- Preserve raw subgraph symbols and generated JSON.
- Resolve labels only from exact `chain + tokenId` keys.
- Unknown markets must fall back to raw symbol and name.
- Search must match both display and raw labels.
- Do not change Supply Pool Health metrics or row keys.

---

### Task 1: Presentation resolver and search behavior

**Files:**
- Modify: `tests/supply-pool-health.test.js`
- Modify: `tvl/supply-health.js`

**Interfaces:**
- Consumes: a Supply Pool Health market object containing `chain`, `tokenId`, `symbol`, and `name`.
- Produces: `getSupplyHealthMarketPresentation(market) -> { symbol, name }`.

- [ ] **Step 1: Write failing tests**

Add tests that expect the Arbitrum GM BTC/USD address to resolve to `gmBTC-USD`, unknown `dGM` addresses to retain their raw metadata, search for `gmBTC-USD` to find the matching row, and every published `dGM` row to receive a unique display symbol.

- [ ] **Step 2: Verify the tests fail**

Run:

```bash
node --test tests/supply-pool-health.test.js
```

Expected: failure because `getSupplyHealthMarketPresentation` is not exported and display-aware search does not exist.

- [ ] **Step 3: Implement the minimal resolver**

Add an exact address map, implement the pure resolver, include presentation fields in the search haystack, use the resolved symbol for symbol sorting, and render resolved labels and accessible copy in table rows.

- [ ] **Step 4: Verify the tests pass**

Run:

```bash
node --test tests/supply-pool-health.test.js
node --check tvl/supply-health.js
```

Expected: all Supply Pool Health tests pass and the script parses.

### Task 2: Cache and browser verification

**Files:**
- Modify: `tvl-preview.html`
- Modify: `tvl/index.html`

**Interfaces:**
- Consumes: the updated `tvl/supply-health.js`.
- Produces: cache-busted TVL and Supply Pool Health URLs.

- [ ] **Step 1: Bump static versions**

Update the Supply Pool Health script query and the TVL route-loader version with a `dgm-market-labels-20260728` suffix.

- [ ] **Step 2: Run targeted regression checks**

Run:

```bash
node --test tests/supply-pool-health.test.js tests/token-icon-presentation-contract.test.js
python3 -m unittest tests.test_supply_pool_health_contracts tests.test_tvl_preview_contracts
git diff --check
```

- [ ] **Step 3: Verify rendered UI**

Serve the repository with `python3 -m http.server`, open `/tvl/` in headless Chromium, search for `gmBTC-USD`, confirm the specific label and `Dolomite GM Market` subtitle, then search for `dGM` and confirm all rendered result labels are specific and unique across pages/data.

- [ ] **Step 4: Commit and publish**

Commit the implementation, rebase onto the latest `dolomite-dashboard/master`, rerun targeted tests, push `HEAD:master`, wait for GitHub Pages, and repeat the public browser assertions.

