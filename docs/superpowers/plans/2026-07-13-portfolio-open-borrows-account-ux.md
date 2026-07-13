# Portfolio Open Borrows Account UX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce visual noise in Portfolio Open Borrows by replacing the repeated Address column with the already-supported Dolomite Account number.

**Architecture:** The table remains a fixed-layout HTML table. Its six-column contract removes the repeated address, while `accountNumberCell` preserves and copies exact identifiers and renders a fixed-width `#123…789` label for long values. A header help trigger explains Account ID without changing table data, and the E-Mode renderer uses a centered, high-contrast three-tone flame inside its existing state pill.

**Tech Stack:** Static HTML, inline CSS and JavaScript, Python `unittest`, in-app browser verification.

## Global Constraints

- Preserve account-number strings exactly, including `0`; do not coerce them through a falsy fallback.
- Keep the table's `colgroup`, row cells, skeletons, empty states, spacer rows, and `nth-child` CSS selectors synchronized.
- Retain the existing account tooltip and full-number copy control.
- Render long account numbers with exactly three leading and three trailing digits; keep the visual chip width fixed.
- Name the second column `Account ID` and explain it through the shared `data-tooltip` system: it identifies the wallet's Dolomite subaccount on the selected chain, can contain multiple collateral/debt assets, and is not a loan ID.
- Preserve E-Mode data, sort behavior, label, and tooltip while using an 18px centered circular flame tile and a 24px-high badge.
- Verify desktop geometry against a real borrow wallet served by `python3 -m http.server`.

---

### Task 1: Define the six-column Open Borrows contract

**Files:**
- Modify: `tests/test_portfolio_preview_contracts.py`
- Modify: `portfolio-preview.html:1301-1396,1883-1886,3261-3286,3418-3422`
- Test: `tests/test_portfolio_preview_contracts.py`

**Interfaces:**
- Consumes: `accountNumberCell(account)` and `buildRiskBorrowRows`, which already supply exact account-number strings.
- Produces: A six-column Open Borrows table with `Chain`, `Account`, `Health Factor`, `E-Mode`, `Collateral`, and `Debt` headers.

- [ ] **Step 1: Write the failing test**

```python
def test_open_borrows_uses_risk_positions_ux(self):
    borrow_render = self.html.split("function renderBorrowPositionsTable()", 1)[1].split("function exerciseSummaryItem", 1)[0]
    self.assertIn('>Account</th>', self.html)
    self.assertNotIn('data-sort="address" data-table="bor">Address', self.html)
    self.assertIn('td colspan="6"', borrow_render)
    self.assertNotIn('td colspan="7"', borrow_render)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests/test_portfolio_preview_contracts.py -v`

Expected: `test_open_borrows_uses_risk_positions_ux` fails because the current table still has the Address header and seven-column row contract.

- [ ] **Step 3: Write the minimal implementation**

```html
<thead><tr>
  <th data-sort="chain" data-table="bor">Chain<span class="pf-sort"></span></th>
  <th>Account</th>
  <th data-sort="hf" data-table="bor">Health Factor<span class="pf-sort"></span></th>
  <th data-sort="emode" data-table="bor" style="text-align:center">E-Mode<span class="pf-sort"></span></th>
  <th data-sort="collateral" data-table="bor">Collateral<span class="pf-sort"></span></th>
  <th class="sort-active" data-sort="debt" data-table="bor">Debt<span class="pf-sort">▼</span></th>
</tr></thead>
```

Remove the address `<td>` from each borrow row. Update the six `colgroup` widths, skeleton cells, empty/spacer-row `colspan`, and affected CSS `nth-child` selectors so Account is the second column, Health Factor the third, E-Mode the fourth, and the financial columns the fifth and sixth.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests/test_portfolio_preview_contracts.py -v`

Expected: all Portfolio contract tests pass.

- [ ] **Step 5: Verify rendered UI**

Run: `python3 -m http.server 4173`

Open `http://127.0.0.1:4173/portfolio-preview.html`, load wallet `0x0480f1cbe27fd5eae8ae7c4d5ff764e30f91aa5f`, and verify in browser JavaScript:

```js
({
  headers: [...document.querySelectorAll('.pf-borrow-positions thead th')].map(x => x.textContent.trim()),
  cells: document.querySelector('#pf-borrows-body tr.pf-row').querySelectorAll('td').length,
  width: document.querySelector('.pf-borrow-positions').getBoundingClientRect().width,
  scrollWidth: document.querySelector('.pf-borrow-positions').scrollWidth,
})
```

Expected: six headers without Address, six data cells, and `scrollWidth === width` at desktop width. Click an Account copy control and confirm the clipboard contains the exact full number.

- [ ] **Step 6: Run focused regression suite and commit**

Run: `python3 -m unittest tests/test_portfolio_preview_contracts.py tests/test_earn_dashboard_contracts.py -q && git diff --check`

Expected: all tests pass and `git diff --check` has no output.

```bash
git add portfolio-preview.html tests/test_portfolio_preview_contracts.py
git commit -m "refactor: simplify portfolio borrow account UX"
```

### Task 2: Standardize the Account label width

**Files:**
- Modify: `tests/test_portfolio_preview_contracts.py`
- Modify: `portfolio-preview.html:1368-1378,2465-2472`
- Test: `tests/test_portfolio_preview_contracts.py`

**Interfaces:**
- Consumes: `shortAccountNumber(account)` with a string account identifier.
- Produces: The full input when it has six or fewer digits, otherwise `first three digits + ellipsis + last three digits`; `accountNumberCell` continues to receive the exact string for its `data-copy` value.

- [ ] **Step 1: Write the failing test**

```python
self.assertIn('return number.length > 6 ? `${number.slice(0, 3)}…${number.slice(-3)}` : number;', self.html)
self.assertIn('width:72px;', self.html)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests/test_portfolio_preview_contracts.py -v`

Expected: `test_open_borrows_uses_risk_positions_ux` fails because the existing source keeps six leading and trailing digits and has no fixed account-chip width.

- [ ] **Step 3: Write the minimal implementation**

```js
function shortAccountNumber(account){
  const number = String(account == null ? "" : account).trim();
  return number.length > 6 ? `${number.slice(0, 3)}…${number.slice(-3)}` : number;
}
```

```css
.pf-account-number,.pf-account-unknown{width:72px;min-width:72px}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests/test_portfolio_preview_contracts.py -v`

Expected: all Portfolio contract tests pass.

- [ ] **Step 5: Verify rendered UI and exact copy behavior**

Run: `python3 -m http.server 4173`

Load wallet `0x0480f1cbe27fd5eae8ae7c4d5ff764e30f91aa5f` in `portfolio-preview.html`. Confirm each visible Account label matches `#ddd…ddd`, every `.pf-account-number` has the same computed width, and clicking the `#742…937` copy control writes `74231045533973466746745298919099460939860226000975118112653482510195681276937` to the clipboard.

- [ ] **Step 6: Run focused regression suite and commit**

Run: `python3 -m unittest tests/test_portfolio_preview_contracts.py tests/test_earn_dashboard_contracts.py -q && git diff --check`

Expected: all tests pass and `git diff --check` has no output.

```bash
git add portfolio-preview.html tests/test_portfolio_preview_contracts.py docs/superpowers/specs/2026-07-13-portfolio-open-borrows-account-ux-design.md docs/superpowers/plans/2026-07-13-portfolio-open-borrows-account-ux.md
git commit -m "refactor: standardize portfolio account labels"
```

### Task 3: Clarify Account ID and align the E-Mode indicator

**Files:**
- Modify: `tests/test_portfolio_preview_contracts.py`
- Modify: `portfolio-preview.html:1367-1398,1864-1867,2488-2496`
- Modify: `shared-hover-tooltips.js:132-154`
- Modify: `docs/superpowers/specs/2026-07-13-portfolio-open-borrows-account-ux-design.md`
- Modify: `docs/superpowers/plans/2026-07-13-portfolio-open-borrows-account-ux.md`
- Test: `tests/test_portfolio_preview_contracts.py`

**Interfaces:**
- Consumes: static Open Borrows `<th>` markup, the shared `data-tooltip` runtime, and `emodeCell(active)`.
- Produces: an `Account ID` header with a focusable help icon whose shared tooltip opens on pointer hover or keyboard focus; `EMODE_ICON` renders a high-contrast three-tone flame inside a fixed circular `.pf-emode-icon` tile while active E-Mode rows retain their existing tooltip and `E-Mode` text.

- [ ] **Step 1: Write the failing test**

```python
self.assertIn('>Account ID<span class="pf-table-head-info"', self.html)
self.assertIn('aria-label="About Account ID"', self.html)
self.assertIn('tabindex="0"', self.html)
self.assertIn("Dolomite Account ID identifies", self.html)
self.assertIn("It can contain multiple collateral and debt assets", self.html)
self.assertIn("document.addEventListener('focusin'", self.shared_tooltips)
self.assertIn("document.addEventListener('focusout'", self.shared_tooltips)
self.assertIn("const EMODE_ICON =", self.html)
self.assertIn('class="pf-emode-icon"', self.html)
self.assertIn('width:18px;height:18px', self.html)
self.assertIn('min-height:24px', self.html)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests/test_portfolio_preview_contracts.py -v`

Expected: `test_open_borrows_uses_risk_positions_ux` fails because the current header is `Account` and the E-Mode renderer still uses `EMODE_FLAME` without a circular icon tile.

- [ ] **Step 3: Write the minimal implementation**

```html
<th><span class="pf-table-head-help">Account ID<span class="pf-table-head-info" tabindex="0" data-tooltip="Dolomite Account ID identifies this wallet's subaccount on the selected chain. It can contain multiple collateral and debt assets, so it is not a loan ID." aria-label="About Account ID"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="8.5"/><path d="M12 11v5M12 8h.01"/></svg></span></span></th>
```

```js
const EMODE_ICON = '<span class="pf-emode-icon" aria-hidden="true"><svg class="pf-emode-flame" viewBox="0 0 24 24"><path d="M12.2 1.25c.2 3.05-1.3 5.2-3.85 7.36-1.85 1.58-3 3.6-3 6.03a6.65 6.65 0 0 0 13.3 0c0-5.03-3.2-8.56-6.45-13.39Z" fill="#fb923c"/><path d="M12.2 8.05c.18 2.08-1.9 3.1-1.9 5.2a1.9 1.9 0 1 0 3.8 0c0-1.76-.78-3.08-1.9-5.2Z" fill="#fbbf24"/><path d="M12.2 11.45c.1.93-.78 1.32-.78 2.22a.78.78 0 1 0 1.56 0c0-.74-.3-1.35-.78-2.22Z" fill="#fff1c1"/></svg></span>';
```

```css
.pf-table-head-help{display:inline-flex;align-items:center;gap:5px}
.pf-table-head-info{display:inline-grid;place-items:center;width:15px;height:15px;border:1px solid var(--line-2);border-radius:50%;color:var(--fg-3);cursor:default}
.pf-table-head-info svg{width:9px;height:9px}
.pf-emode-badge{justify-content:center;gap:5px;min-height:24px;padding:2px 7px 2px 3px;border-radius:999px}
.pf-emode-icon{display:inline-grid;place-items:center;width:18px;height:18px;flex:0 0 18px;border:1px solid rgba(251,191,36,.32);border-radius:50%;background:rgba(251,191,36,.12);color:#fcd34d}
.pf-emode-icon svg{width:12px;height:12px}
```

Change only `EMODE_FLAME` references to `EMODE_ICON` and leave `emodeTip` unchanged.

```js
document.addEventListener('focusin', function (event) {
  var data = tooltipText(event.target);
  if (!data || !data.text) return;
  show(data);
});

document.addEventListener('focusout', function (event) {
  if (!tooltipText(event.target)) return;
  scheduleHide();
});
```

Place the focus handlers beside the existing mouse handlers in `shared-hover-tooltips.js`. This keeps the current body-level tooltip element and positioning code intact while making the focusable Account ID information icon useful to keyboard users.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests/test_portfolio_preview_contracts.py -v`

Expected: all Portfolio contract tests pass.

- [ ] **Step 5: Verify rendered UI and accessible tooltip triggers**

Run: `python3 -m http.server 4173`

Load wallet `0x0480f1cbe27fd5eae8ae7c4d5ff764e30f91aa5f` in `portfolio-preview.html`. Confirm in browser JavaScript that the header texts are `Chain`, `Account ID`, `Health Factor`, `E-Mode`, `Collateral`, and `Debt`; the help icon has `tabindex="0"`; all active `.pf-emode-icon` elements have equal 18px square boxes centered in 24px-high badges; the table has six cells per borrow row with no horizontal overflow.

- [ ] **Step 6: Run focused regression suite and commit**

Run: `python3 -m unittest tests/test_portfolio_preview_contracts.py tests/test_earn_dashboard_contracts.py -q && git diff --check`

Expected: all tests pass and `git diff --check` has no output.

```bash
git add portfolio-preview.html tests/test_portfolio_preview_contracts.py docs/superpowers/specs/2026-07-13-portfolio-open-borrows-account-ux-design.md docs/superpowers/plans/2026-07-13-portfolio-open-borrows-account-ux.md
git commit -m "refactor: clarify portfolio borrow account id"
```
