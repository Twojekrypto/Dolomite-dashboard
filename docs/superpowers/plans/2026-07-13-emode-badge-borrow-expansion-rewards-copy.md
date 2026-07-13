# E-Mode Badge, Borrow Expansion, And Rewards Copy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unify active E-Mode table badges, reveal hidden Portfolio Open Borrows assets on row expansion, and rename visible Rewards supply campaigns from Lend to Supply.

**Architecture:** Keep each static page self-contained and port the accepted Portfolio badge markup to the active Liquidation and Earn renderers. Reuse the existing Liquidation row-expansion model in Portfolio with a Set keyed by chain/account, delegated events, and collapsible token wrappers. Normalize Rewards copy at render time so generated source classifications remain intact.

**Tech Stack:** Static HTML/CSS/JavaScript, Python `unittest` contract tests, Node contract tests, GitHub Pages route loaders.

## Global Constraints

- Do not change Open Borrows column order, accepted percentage widths, or spacer position.
- Only rows hiding tokens are expandable; the threshold is more than three collateral tokens or more than three debt tokens.
- Interactive descendants must not toggle a row.
- E-Mode stays in its current table location and uses the full `E-Mode` label.
- Rewards source `action: "LEND"` data remains unchanged.
- No new dependencies.

---

### Task 1: Lock The E-Mode UX Contract

**Files:**
- Create: `tests/test_emode_ux_contracts.py`
- Modify: `liquidation-preview.html`
- Modify: `dashboard-core.css`
- Modify: `dashboard-core.js`

**Interfaces:**
- Consumes: Portfolio `EMODE_ICON`, `emodeTip`, `.pf-emode-badge`, `.pf-emode-icon`, and `.pf-emode-flame` as the reference geometry and behavior.
- Produces: Liquidation `.emode-badge/.emode-icon/.emode-flame` and Earn `.earn-hf-emode-inline/.earn-emode-icon/.earn-emode-flame` with equivalent markup and dimensions.

- [ ] **Step 1: Write the failing E-Mode contract test**

Add a unittest that reads Portfolio, Liquidation, dashboard CSS, and dashboard JS. Assert all active renderers include the full three-path flame geometry, the full `E-Mode` label, the Portfolio tooltip text, an 18 px circular icon shell, a 24 px pill, and restrained `scale(1.04)` hover behavior. Assert both Earn render paths use the same helper or exact badge markup.

```python
def test_active_emode_badges_match_portfolio(self):
    for source in (self.portfolio, self.liquidation, self.earn_js):
        self.assertIn('fill="#fff1c1"', source)
        self.assertIn('E-Mode applies special risk parameters', source)
    self.assertEqual(self.earn_js.count("earn_emodeBadge(p.eMode)"), 2)
    self.assertIn('class="emode-icon"', self.liquidation)
    self.assertIn('class="earn-emode-icon"', self.earn_js)
    self.assertIn('width:18px', self.liquidation)
    self.assertIn('min-height:24px', self.earn_css)
```

- [ ] **Step 2: Run the E-Mode test and verify RED**

Run: `python3 -m unittest tests/test_emode_ux_contracts.py -v`

Expected: failures for the old two-path Liquidation/Earn flame, Earn `E` label, and missing circular icon shells.

- [ ] **Step 3: Implement the badge parity**

In Liquidation, keep the existing dedicated column and tooltip delegate while replacing badge geometry/styles with the Portfolio three-path flame and circular shell. In Earn, define one `earn_emodeBadge()` renderer used by both initial and sorted-row render paths; keep it beside Health Factor and use the full label and tooltip.

```javascript
function earn_emodeBadge(active) {
    if (!active) return '';
    return `<span class="earn-hf-emode-inline" data-tip="${EARN_EMODE_TIP}">${EARN_EMODE_ICON}E-Mode</span>`;
}
```

- [ ] **Step 4: Run the E-Mode test and verify GREEN**

Run: `python3 -m unittest tests/test_emode_ux_contracts.py -v`

Expected: all E-Mode contract tests pass.

---

### Task 2: Add Open Borrows Asset Expansion

**Files:**
- Modify: `tests/test_portfolio_preview_contracts.py`
- Modify: `portfolio-preview.html`

**Interfaces:**
- Produces: `borrowRowKey(row) -> string`, `hasBorrowTokenOverflow(row) -> boolean`, `tokenPills(tokens, chain, options) -> string`, and `state.expandedBorrows: Set<string>`.
- Event contract: delegated click and keydown handlers on `#pf-borrows-body`; interactive descendants are excluded.

- [ ] **Step 1: Write the failing Portfolio expansion contract test**

Assert the Set exists, key combines chain/account identity, overflow checks both token arrays against three, collapsed rendering slices three, extra tokens use an `aria-hidden` wrapper, rows expose `tabindex` and `aria-expanded`, click and Enter/Space toggle expansion, interactive descendants are ignored, and `loadWallet` clears expanded state. Retain all current seven-column assertions.

```python
self.assertIn("expandedBorrows: new Set()", self.html)
self.assertIn("return `${row.chain}:${row.accountId || row.account || row.search}`;", self.html)
self.assertIn("row.collateralTokens.length > 3 || row.debtTokens.length > 3", self.html)
self.assertIn(".slice(0, limit)", self.html)
self.assertIn('class="pf-token-pill-extra" aria-hidden="${expanded ? "false" : "true"}"', self.html)
self.assertIn("if (e.key !== \"Enter\" && e.key !== \" \") return;", self.html)
self.assertIn("state.expandedBorrows.clear();", self.html)
```

- [ ] **Step 2: Run the Portfolio contract and verify RED**

Run: `python3 -m unittest tests/test_portfolio_preview_contracts.py -v`

Expected: the new expansion assertions fail while existing column assertions pass.

- [ ] **Step 3: Implement minimal expansion behavior**

Extend `tokenPills` with `{limit, expanded, collapsible}`. Render first three tokens plus a `+N` hint when collapsed and an animated extra-token wrapper when expandable. Add row classes/attributes, Set-backed state, click/keyboard delegation, interaction exclusions, expanded-row styling, and reset the Set on wallet load.

```javascript
function borrowRowKey(row) {
  return `${row.chain}:${row.accountId || row.account || row.search}`;
}
function hasBorrowTokenOverflow(row) {
  return row.collateralTokens.length > 3 || row.debtTokens.length > 3;
}
function shouldIgnoreBorrowExpandClick(target) {
  return !!target?.closest?.('a,button,input,select,textarea,[data-copy]');
}
```

- [ ] **Step 4: Run the Portfolio contract and verify GREEN**

Run: `python3 -m unittest tests/test_portfolio_preview_contracts.py -v`

Expected: all Portfolio contracts pass.

---

### Task 3: Normalize Rewards Supply Copy

**Files:**
- Modify: `tests/rewards-preview-contract.test.js`
- Modify: `rewards-preview.html`
- Modify: `rewards/index.html`

**Interfaces:**
- Produces: visible program labels using `Supply TOKEN` when `action === "LEND"`; source program objects are unchanged.

- [ ] **Step 1: Write the failing Rewards copy contract**

Assert the renderer returns ``Supply ${market}`` for LEND and does not contain ``Lend ${market}``. Assert the source action remains `LEND` and the route version includes the new rewards-copy cache key.

```javascript
assert(html.includes("if (market && action === 'LEND') return `Supply ${market}`"));
assert(!html.includes("return `Lend ${market}`"));
assert(html.includes("replace(/^Lend\\s+/i, 'Supply ')"));
```

- [ ] **Step 2: Run the Rewards test and verify RED**

Run: `node --test tests/rewards-preview-contract.test.js`

Expected: failure on the current `Lend ${market}` output.

- [ ] **Step 3: Implement the copy normalization**

Change only the visible fallback label in `programName()` from `Lend` to `Supply`, normalize provider-supplied names beginning with `Lend ` before display, and bump the Rewards route version.

```javascript
const suppliedName = String(p.name || '').replace(/^Lend\s+/i, 'Supply ');
if (suppliedName) return suppliedName;
if (market && action === 'LEND') return `Supply ${market}`;
```

- [ ] **Step 4: Run the Rewards test and verify GREEN**

Run: `node --test tests/rewards-preview-contract.test.js`

Expected: all Rewards contracts pass.

---

### Task 4: Cache Busting And End-To-End Verification

**Files:**
- Modify: `portfolio/index.html`
- Modify: `liquidation/index.html`
- Modify: `borrow/index.html`
- Modify: `supply/index.html`
- Modify: `dashboard-core.html`

**Interfaces:**
- Produces: fresh route-loader versions for changed preview files and fresh dashboard CSS/JS asset versions.

- [ ] **Step 1: Add failing cache-version assertions**

Extend the relevant contract tests to require `20260713-emode-expand` in Portfolio/Liquidation/Borrow/Supply versions and `20260713-emode` in dashboard-core CSS/JS query strings.

```python
self.assertIn("20260713-emode-expand", (ROOT / "portfolio/index.html").read_text())
self.assertIn("20260713-emode", self.dashboard_html)
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `python3 -m unittest tests/test_emode_ux_contracts.py tests/test_portfolio_preview_contracts.py -v && node --test tests/rewards-preview-contract.test.js`

Expected: cache-version assertions fail.

- [ ] **Step 3: Bump route and asset versions**

Update only the version strings for the files changed in Tasks 1-3.

- [ ] **Step 4: Run all focused contracts**

Run: `python3 -m unittest tests/test_emode_ux_contracts.py tests/test_portfolio_preview_contracts.py tests/test_earn_dashboard_contracts.py -q`

Run: `node --test tests/rewards-preview-contract.test.js tests/table-chain-ux-contract.test.js`

Expected: all tests pass with zero failures.

- [ ] **Step 5: Verify served pages in a real browser**

Run: `python3 -m http.server 4173`

Check Portfolio with a four-token fixture/DOM injection: collapsed list shows three plus `+N`; row click and Enter expand all; Account copy does not toggle; row collapses again; table `scrollWidth === clientWidth` at desktop width. Check Liquidation and Earn badges for full label, icon dimensions, tooltip, and no clipping. Check Rewards displays `Supply HONEY` and no visible `Lend HONEY`.

- [ ] **Step 6: Run final repository checks and commit**

Run: `git diff --check`

Run: `python3 -m unittest tests/test_emode_ux_contracts.py tests/test_portfolio_preview_contracts.py tests/test_earn_dashboard_contracts.py -q`

Run: `node --test tests/rewards-preview-contract.test.js tests/table-chain-ux-contract.test.js`

Expected: all commands exit zero. Commit the exact implementation/test/cache files, rebase onto current production `master`, rerun the same checks, push, wait for Pages success, and verify the public URLs.
