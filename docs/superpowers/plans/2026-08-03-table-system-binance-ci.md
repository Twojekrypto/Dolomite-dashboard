# Table System, Binance Labels, and TVL CI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the audited table inconsistencies, restore readable EARN tables on phones, identify the confirmed Binance deposit wallet, and make TVL history refresh resilient to one transient stale-market response.

**Architecture:** Preserve the existing Graphite + Gold components and use DOLO Holders as the visual reference. Apply route-local changes because this static dashboard renders tables from independent preview files, while putting wallet identity in the existing shared `dolo-address-labels.js` source. Keep strict data validation and retry only failed/stale official metric markets before validation.

**Tech Stack:** Static HTML/CSS/JavaScript, CommonJS contract tests, Python 3 `unittest`, GitHub Actions.

## Global Constraints

- GitHub Pages production is `dolomite-dashboard/master`.
- Keep confirmed and heuristic wallet labels separate; Binance is `type:"cex"`, `confidence:"confirmed"`.
- Do not relax the `MAX_STALE_MARKET_SHARE` validation threshold.
- Use `Price updated` only for price data, `Data updated` for table snapshots, `Verified at` for EARN, and `Report generated` for History.
- Pagination uses `1–10 of TOTAL`, SVG first/previous/next/last controls, 30 px buttons, and accessible labels.
- Browser QA must verify real computed styles and bounding boxes at 1440×1000, 768×1024, and 390×844.

---

### Task 1: Regression contracts for the approved audit

**Files:**
- Create: `tests/test_table_audit_remediation_contracts.py`
- Modify: `tests/test_dolo_address_labels.py`
- Modify: `tests/test_fetch_dolomite_total_supply_history.py`
- Create: `tests/test_audit_dolo_cex_labels.py`

**Interfaces:**
- Consumes: current route markup/CSS/JS and audit helpers.
- Produces: failing behavioral contracts for mobile layout, table metadata/pagers, Binance identity, public explorer fallback, and stale-market retry.

- [ ] **Step 1: Write failing tests**

```python
def test_earn_mobile_uses_readable_layout_without_desktop_spacers():
    assert "MOBILE_BREAKPOINT = 560" in EARN_LAYOUT
    assert "clearDesktopLayout" in EARN_LAYOUT

def test_confirmed_binance_deposit_wallet_is_shared():
    info = labels[BINANCE_DEPOSIT]
    assert info["type"] == "cex"
    assert info["confidence"] == "confirmed"

def test_material_stale_market_is_refetched_before_validation():
    refreshed = retry_stale_market_histories(histories)
    assert refreshed[0]["points"]
```

- [ ] **Step 2: Run tests and verify expected failures**

Run: `python3 -m unittest tests.test_table_audit_remediation_contracts tests.test_dolo_address_labels tests.test_fetch_dolomite_total_supply_history tests.test_audit_dolo_cex_labels`

Expected: FAIL because mobile cleanup, Binance label, public fallback, range pager, and retry helpers do not exist yet.

### Task 2: Fix EARN phone layout

**Files:**
- Modify: `earn/earn-static-layout.js`
- Modify: `earn/earn-draft.css`
- Modify: `dashboard-core.html`
- Modify: `earn/earn-core.html`

**Interfaces:**
- Consumes: `[data-earn-layout-table]` and `[data-column]` contracts.
- Produces: `applyMobileLayout(name, table, documentLike)` and a resize-aware `applyAll`.

- [ ] **Step 1: Implement the smallest layout switch**

At `<=560px`, remove the optional desktop spacer, clear desktop inline widths, retain the semantic column order, and let the existing CSS widths distribute the 760 px scroll table. Above 560 px, keep the exact accepted exported layout.

- [ ] **Step 2: Add a resize listener and bump route cache keys**

Use one scheduled `resize` listener and change the `earn-static-layout.js?v=` value in both EARN bundles.

- [ ] **Step 3: Run the EARN contracts**

Run: `python3 -m unittest tests.test_table_audit_remediation_contracts tests.test_earn_layout_contracts tests.test_earn_premium_ux_contracts`

Expected: PASS.

### Task 3: Standardize table freshness information and pagination

**Files:**
- Modify: `assets-preview.html`
- Modify: `dolo-preview.html`
- Modify: `odolo-preview.html`
- Modify: `vedolo-preview.html`
- Modify: `rewards-preview.html`
- Modify: `dashboard-core.html`
- Modify: `dashboard-core.js`
- Modify: `history/history.html` or the actual History markup source selected by `history/index.html`
- Modify: `history/history.css`
- Modify: `history/history.js`
- Modify: `liquidation-preview.html`
- Modify: `tvl-preview.html`

**Interfaces:**
- Consumes: source timestamps already loaded by each route.
- Produces: source-specific metadata labels and the shared visual pager contract.

- [ ] **Step 1: Correct metadata wording at the data boundary**

Use `Data updated · …` on Assets, Fresh Wallets, Ended Programs, Revenue tables, and table snapshots; `Verified at · …` in EARN section metadata; `Report generated · …` after History report generation. Keep DOLO market price as `Price updated · …`.

- [ ] **Step 2: Normalize pagers**

Render the left summary as `${start.toLocaleString()}–${end.toLocaleString()} of ${total.toLocaleString()}` and use existing SVG pager icon constants with `aria-label` on every button. Align the footer as `1fr auto 1fr`, with 30×30 controls.

- [ ] **Step 3: Run metadata and pager contracts**

Run: `python3 -m unittest tests.test_table_audit_remediation_contracts tests.test_data_freshness_surface_contracts tests.test_vedolo_preview_contracts tests.test_borrow_ux_contracts tests.test_history_tax_export_contracts`

Expected: PASS.

### Task 4: Align veDOLO and Revenue tables to DOLO Holders

**Files:**
- Modify: `vedolo-preview.html`
- Modify: `revenue-preview.html`

**Interfaces:**
- Consumes: existing semantic green/red status colors and date-range controls.
- Produces: DOLO-style 22 px table cards, 16/600 headings, 10/600 headers, disciplined 12 px body typography, and divided metadata headers.

- [ ] **Step 1: Add final, scoped veDOLO parity rules**

Keep risk and flow colors semantic, but normalize title/count hierarchy, table header rhythm, body weight, row height, and footer pager geometry.

- [ ] **Step 2: Convert Protocol Revenue by Chain to the shared card shell**

Scope the change to `.table-panel`: `border-radius:22px`, `border:1px solid rgba(255,255,255,.08)`, title `16px/600`, table headers `10px/600`, and a right-aligned `Data updated · …` label. Preserve the date controls.

- [ ] **Step 3: Run route contracts**

Run: `python3 -m unittest tests.test_table_audit_remediation_contracts tests.test_vedolo_preview_contracts && node --test tests/table-surface-consistency.test.js`

Expected: PASS.

### Task 5: Tighten Borrow table density and footer summaries

**Files:**
- Modify: `liquidation-preview.html`
- Modify: `tests/test_borrow_ux_contracts.py`

**Interfaces:**
- Consumes: `currentPage`, `pageSize`, `filteredPositions`, `liquidationHistoryPage`, and filtered liquidation rows.
- Produces: DOLO-style 42 px headers, compact 72–76 px rows, brighter 12 px data typography, and actual visible ranges in both footers.

- [ ] **Step 1: Replace total-only footer copy with current ranges**

Use one formatter for Lending Positions and Liquidation History so page 2 reads `11–20 of TOTAL`.

- [ ] **Step 2: Apply final scoped density rules**

Set header height to 42 px, reduce cell padding, use `font-weight:500` for primary table data, and keep address/token secondary rows muted.

- [ ] **Step 3: Audit every related `nth-child` selector**

Run: `rg -n 'positions-table.*nth-child|liquidation-history-table.*nth-child' liquidation-preview.html` and verify the existing 7/5-column contracts are unchanged.

- [ ] **Step 4: Run Borrow tests**

Run: `python3 -m unittest tests.test_borrow_ux_contracts tests.test_table_audit_remediation_contracts`

Expected: PASS.

### Task 6: Repair Binance discovery and TVL history CI

**Files:**
- Modify: `dolo-address-labels.js`
- Modify: `audit_dolo_cex_labels.py`
- Modify: `.github/workflows/audit-dolo-cex-labels.yml`
- Modify: `fetch_dolomite_total_supply_history.py`
- Modify: `.github/workflows/update-tvl-data.yml` only if the retry needs an operational knob.

**Interfaces:**
- Consumes: ranked CEX audit candidates, Etherscan V2 response, public Etherscan address HTML, and stale metric history rows.
- Produces: confirmed Binance deposit identity, a public-label fallback report, correct nested audit summary fields, and `retry_stale_market_histories`.

- [ ] **Step 1: Add the confirmed Binance wallet to the shared map**

```javascript
"0x06fd4ba7973a0d39a91734bbc35bc2bcaa99e3b0": {
  label:"Binance Deposit", type:"cex",
  source:"etherscan-public-label", confidence:"confirmed"
}
```

- [ ] **Step 2: Add public explorer fallback without automatic heuristic promotion**

Fetch `https://etherscan.io/address/{address}` with a browser user agent, extract the `<title>`, accept only titles containing a strict CEX keyword, and write those rows to `confirmedCexSuggestions` with `source:"etherscan-public-page"`. The report remains advisory; only confirmed shared-map entries appear at runtime.

- [ ] **Step 3: Fix the workflow summary schema**

Read `full["api"]["status"]` and `full["api"]["confirmedCexSuggestions"]`, and preserve the actual source/status in the committed summary.

- [ ] **Step 4: Retry stale official markets once before strict validation**

Refetch only rows classified stale, replace each row only when the retry returns recent points, recompute stale coverage, and keep the existing one-percent validation threshold unchanged.

- [ ] **Step 5: Run data-pipeline tests**

Run: `python3 -m unittest tests.test_dolo_address_labels tests.test_audit_dolo_cex_labels tests.test_fetch_dolomite_total_supply_history`

Expected: PASS.

### Task 7: Browser QA, deployment, and live verification

**Files:**
- No committed QA artifacts.

**Interfaces:**
- Consumes: local static server and the production GitHub Pages URL.
- Produces: browser measurements/screenshots, a production commit, a successful push, and a verified Pages deployment.

- [ ] **Step 1: Run the complete relevant automated suite**

Run: `python3 -m unittest discover -s tests -p 'test_*.py'` and `node --test tests/*.test.js`.

- [ ] **Step 2: Serve the worktree**

Run: `python3 -m http.server 8765` from the worktree.

- [ ] **Step 3: Verify desktop, tablet, and phone**

Inspect EARN Supply/Past, veDOLO tables, Protocol Revenue by Chain, Lending Positions, Liquidation History, and the Binance row. Record `getComputedStyle()` and `getBoundingClientRect()` evidence, test a pager interaction, and require no relevant console errors/warnings.

- [ ] **Step 4: Commit and publish**

Commit on `codex/table-system-binance-ci`, fetch/rebase onto the newest `dolomite-dashboard/master`, rerun the narrow regression suite, then push `HEAD:master` without force.

- [ ] **Step 5: Monitor deployment**

Wait for the Pages workflow and the manually dispatched TVL workflow, then open cache-busted production URLs and repeat the key mobile/table/Binance checks.
