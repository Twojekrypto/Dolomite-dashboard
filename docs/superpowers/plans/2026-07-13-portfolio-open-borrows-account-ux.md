# Portfolio Open Borrows Account UX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce visual noise in Portfolio Open Borrows by replacing the repeated Address column with the already-supported Dolomite Account number.

**Architecture:** The table remains a fixed-layout HTML table. Only its presentational column contract changes from seven columns to six; the existing `accountNumberCell` continues to preserve and copy account identifiers exactly.

**Tech Stack:** Static HTML, inline CSS and JavaScript, Python `unittest`, in-app browser verification.

## Global Constraints

- Preserve account-number strings exactly, including `0`; do not coerce them through a falsy fallback.
- Keep the table's `colgroup`, row cells, skeletons, empty states, spacer rows, and `nth-child` CSS selectors synchronized.
- Retain the existing account tooltip and full-number copy control.
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
