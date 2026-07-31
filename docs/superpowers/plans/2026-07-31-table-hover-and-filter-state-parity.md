# Table Hover and Filter State Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver DOLO Holders-parity row hover, terminal-row rounding, polished Earn/Supply selector states, and Dolomite Assets-parity icons for Past and routed assets.

**Architecture:** Keep the change scoped to the existing static route assets. Add explicit terminal-row classes where Earn detail/spacer rows make structural CSS selectors unreliable, keep state styling in route-specific CSS, and resolve Past and routed icons through the existing address-first `KNOWN_TOKENS` registry before symbol fallbacks.

**Tech Stack:** Static HTML/CSS/JavaScript, Node contract tests, Python bundle generator, local HTTP server, in-app browser computed-style verification.

## Global Constraints

- Preserve the Graphite + Gold identity and existing table data, sorting, expansion, and routing.
- Paginated/footer joins stay square; only a true terminal data row receives lower corner rounding.
- Empty, loading, detail, and spacer rows never receive the gold hover rail.
- Keep the Earn control deck at 56px and preserve accessible names and `:focus-visible` treatment.
- Resolve token artwork by exact chain plus address before symbol aliases.
- Do not add dependencies or reformat the large static source files.

---

### Task 1: Terminal-row hover contract

**Files:**
- Modify: `tests/table-surface-consistency.test.js`
- Modify: `tests/table-chain-ux-contract.test.js`
- Modify: `rewards-preview.html`
- Modify: `assets-preview.html`
- Modify: `dashboard-core.css`
- Modify: `earn/earn-draft.css`
- Modify: `dashboard-core.js`

**Interfaces:**
- Consumes: existing `.tbl`, `.earn-data-row`, `.earn-lend-row`, `.earn-detail-row`, and `.earn-table-spacer` markup.
- Produces: `.earn-terminal-row` on the last primary Earn row and one consistent two-pixel gold hover rail.

- [ ] **Step 1: Write the failing contract tests**

Add assertions that Ended Programs and Dolomite Assets expose scoped terminal-row rules, and that all three Earn primary row types expose the same hover rail plus `.earn-terminal-row` lower radii:

```js
assert.match(rewards, /\.ended-programs-card \.tbl tbody tr:last-child:hover td:first-child\{[^}]*border-radius:0 0 0 var\(--r-xl\)/s);
assert.match(assets, /\.assets-table-wrap #tbl tbody tr\.data:last-of-type:hover td:first-child\{[^}]*border-radius:0 0 0 var\(--r-xl\)/s);
assert.match(coreCss, /\.earn-terminal-row:hover td:first-child\{[^}]*border-radius:\s*0 0 0 14px/s);
assert.match(earnDraftCss, /\.earn-lending-table tbody tr\.earn-lend-row:hover td:first-child::before/s);
```

- [ ] **Step 2: Run the tests and verify they fail**

Run:

```bash
node --test tests/table-surface-consistency.test.js tests/table-chain-ux-contract.test.js
```

Expected: failures for missing scoped terminal-row and Earn rail contracts.

- [ ] **Step 3: Implement the minimum row-state changes**

Give the Ended Programs card a stable class, restore lower hover radii only for its final row, and round the lower end of its `::before` rail. Add equivalent scoped rules to the internally scrolling Dolomite Assets table.

In Earn rendering, mark only the last primary row after HTML construction:

```js
function earn_markTerminalPrimaryRow(tbody) {
    if (!tbody) return;
    tbody.querySelectorAll('.earn-terminal-row').forEach(row => row.classList.remove('earn-terminal-row'));
    const rows = tbody.querySelectorAll('tr.earn-data-row, tr.earn-lend-row');
    if (rows.length) rows[rows.length - 1].classList.add('earn-terminal-row');
}
```

Call it after Supply, Borrow, and Past table bodies are rendered. In `earn/earn-draft.css`, use the same graphite hover and gold `::before` rail for `.earn-data-row` and `.earn-lend-row`; round only `.earn-terminal-row` first/last cells and the rail bottom.

- [ ] **Step 4: Run the contract tests**

Run the Task 1 command again. Expected: PASS.

- [ ] **Step 5: Commit Task 1**

```bash
git add rewards-preview.html assets-preview.html dashboard-core.css earn/earn-draft.css dashboard-core.js tests/table-surface-consistency.test.js tests/table-chain-ux-contract.test.js
git commit -m "Polish terminal table row hover states"
```

---

### Task 2: Dolomite Earnings control deck

**Files:**
- Modify: `tests/test_earn_premium_ux_contracts.py`
- Modify: `earn/earn-draft.css`

**Interfaces:**
- Consumes: `.earn-input-row`, `.earn-chain-dropdown`, `.earn-chain-trigger`, `.earn-chain-option`, and `.earn-search-btn`.
- Produces: full-height 54px trigger geometry and coherent hover/open/focus states.

- [ ] **Step 1: Add failing CSS contract assertions**

Assert the route override includes the complete hit target and state hierarchy:

```python
self.assertIn("height: 54px !important", self.css)
self.assertIn("body.earn-draft-route .earn-chain-dropdown.open .earn-chain-trigger", self.css)
self.assertIn("body.earn-draft-route .earn-chain-trigger:focus-visible", self.css)
self.assertIn("outline: none !important", self.css)
```

- [ ] **Step 2: Run the focused test and verify failure**

```bash
python3 -m unittest tests.test_earn_premium_ux_contracts
```

Expected: missing full-height/open/focus contracts.

- [ ] **Step 3: Implement the control states**

Set the network trigger to `height:54px`, stretch/center it inside the deck, and apply:

```css
body.earn-draft-route .earn-chain-trigger:hover {
  background: rgba(255,255,255,.035) !important;
}
body.earn-draft-route .earn-chain-dropdown.open .earn-chain-trigger,
body.earn-draft-route .earn-chain-trigger:focus-visible {
  background: var(--earn-gold-wash) !important;
  color: var(--earn-fg-1) !important;
  outline: none !important;
  box-shadow: inset 0 0 0 1px var(--earn-gold-line) !important;
}
```

Keep the Search button gold, stable at 54px, and remove layout-changing hover transforms. Keep selected menu rows gold-washed and other row hovers graphite.

- [ ] **Step 4: Run the focused test**

Run the Task 2 command again. Expected: PASS.

- [ ] **Step 5: Commit Task 2**

```bash
git add earn/earn-draft.css tests/test_earn_premium_ux_contracts.py
git commit -m "Refine Earn search control interactions"
```

---

### Task 3: Supply Markets applied and pending states

**Files:**
- Modify: `tests/supply-ui-behavior.test.js`
- Modify: `supply/supply-draft.css`

**Interfaces:**
- Consumes: existing body classes `.supply-has-asset` and `.supply-has-pending-asset`, `.premium-supply-dropdown`, and `.supply-draft-apply-btn` state classes.
- Produces: three-level state hierarchy: neutral applied context, asset pending emphasis, lighter network contextual emphasis, and strongest actionable Apply button.

- [ ] **Step 1: Add failing state-style contracts**

Read `supply/supply-draft.css` in the Node test and assert:

```js
assert.match(supplyCss, /supply-has-pending-asset #custom-asset-selector[^}]*background:\s*rgba\(201, 162, 39, \.075\)/s);
assert.match(supplyCss, /supply-has-pending-asset #custom-chain-selector[^}]*background:\s*rgba\(201, 162, 39, \.035\)/s);
assert.match(supplyCss, /\.supply-draft-apply-btn\.is-applied[^}]*var\(--supply-gold-line\)/s);
```

- [ ] **Step 2: Run and verify failure**

```bash
node --test tests/supply-ui-behavior.test.js
```

Expected: the contextual network pending state is absent.

- [ ] **Step 3: Implement the segmented state hierarchy**

Retain the current stronger pending asset wash. Add a lighter contextual wash to `#custom-chain-selector` only while the body has `.supply-has-pending-asset`. Add hover/open selectors after semantic state selectors so hover changes brightness without clearing state. Keep `.is-applied` restrained and `.is-pending` fully gold/actionable.

- [ ] **Step 4: Run the Supply test**

Run the Task 3 command again. Expected: PASS.

- [ ] **Step 5: Commit Task 3**

```bash
git add supply/supply-draft.css tests/supply-ui-behavior.test.js
git commit -m "Unify Supply market selector states"
```

---

### Task 4: Address-first Past and routed icons

**Files:**
- Modify: `tests/token-icon-presentation-contract.test.js`
- Modify: `dashboard-core.js`
- Generated: `earn/earn-core.js`
- Generated: `earn/earn-core.html`
- Modify: `build_earn_bundle.py`

**Interfaces:**
- Consumes: `KNOWN_TOKENS`, `SYMBOL_ICONS`, current chain key, token address, symbol, and optional fallback icon.
- Produces: `earn_resolveCanonicalTokenIcon(symbol, tokenAddr, chainId, fallbackIcon) -> string | null`.

- [ ] **Step 1: Add a failing resolver contract**

Extract/evaluate the resolver with the existing registries and assert that the exact Arbitrum GM address overrides a deliberately wrong generic fallback:

```js
assert.equal(
  resolveEarnIcon('dGM', DGM_BTC, 'arbitrum', 'generic-gmx.svg'),
  OFFICIAL_WBTC_GM_ICON,
);
```

Also assert the Past and routed renderer calls `earn_resolveCanonicalTokenIcon` with `item.symbol`, `item.tokenAddr`, the selected chain, and `item.icon`.

- [ ] **Step 2: Run and verify failure**

```bash
node --test tests/token-icon-presentation-contract.test.js
```

Expected: missing resolver and renderer integration.

- [ ] **Step 3: Implement address-first icon resolution**

Add:

```js
function earn_resolveCanonicalTokenIcon(symbol, tokenAddr, chainId, fallbackIcon = null) {
    const address = String(tokenAddr || '').toLowerCase();
    const exact = address ? KNOWN_TOKENS[`${chainId}:${address}`] : null;
    return exact?.icon
        || fallbackIcon
        || SYMBOL_ICONS[symbol]
        || SYMBOL_ICONS[String(symbol || '').toUpperCase()]
        || null;
}
```

Use it when rendering Past and routed primary rows. Preserve the current full-logo frame classifier and fallback initials.

- [ ] **Step 4: Rebuild the dedicated Earn bundle and bump its layout version**

Update `STATIC_LAYOUT_VERSION`, then run:

```bash
python3 build_earn_bundle.py
python3 build_earn_bundle.py --check
```

Expected: generated assets current.

- [ ] **Step 5: Run icon and bundle tests**

```bash
node --test tests/token-icon-presentation-contract.test.js
python3 -m unittest tests.test_build_earn_bundle
```

Expected: PASS.

- [ ] **Step 6: Commit Task 4**

```bash
git add dashboard-core.js earn/earn-core.js earn/earn-core.html build_earn_bundle.py tests/token-icon-presentation-contract.test.js
git commit -m "Align Earn history token icons with Assets"
```

---

### Task 5: Integrated verification and production publication

**Files:**
- Verify all files modified in Tasks 1–4.

**Interfaces:**
- Consumes: completed source and generated route assets.
- Produces: tested production commit on `master` and verified live routes.

- [ ] **Step 1: Run the focused regression suite**

```bash
node --test tests/table-surface-consistency.test.js tests/table-chain-ux-contract.test.js tests/supply-ui-behavior.test.js tests/token-icon-presentation-contract.test.js
python3 -m unittest tests.test_earn_premium_ux_contracts tests.test_build_earn_bundle
python3 build_earn_bundle.py --check
git diff --check
```

Expected: all checks pass and no whitespace errors.

- [ ] **Step 2: Verify in the browser on the local HTTP server**

Inspect `/rewards/`, `/assets/`, `/earn/`, and `/supply/` at desktop and mobile widths. Confirm computed 54px Earn trigger height, the pending Supply background hierarchy, lower cell and rail radii on true terminal rows, no rail on spacer/detail/loading rows, canonical GM icon URLs, no console errors, and no dropdown overflow.

- [ ] **Step 3: Rebase safely on current production**

```bash
git fetch dolomite-dashboard master
git rebase dolomite-dashboard/master
```

Resolve only conflicts in files in scope, rerun Step 1, and preserve unrelated production changes.

- [ ] **Step 4: Push the tested commit to production**

```bash
git push dolomite-dashboard HEAD:master
```

- [ ] **Step 5: Verify live deployment**

Check GitHub Pages/Actions completion and revisit the four live routes with cache-busting query parameters. Confirm the deployed commit and the same computed/visual contracts used locally.
