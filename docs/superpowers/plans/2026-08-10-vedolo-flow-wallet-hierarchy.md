# veDOLO Flow Wallet Hierarchy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render every wallet in the two veDOLO Flow tables with a white wallet name first and a quieter address/actions/date line beneath it.

**Architecture:** Keep the existing `flowAddressCell(addr, date, txHash, timestamp)` boundary and change only its markup plus flow-scoped CSS. Reuse the current label resolver, full-address tooltip attributes, Copy/DeBank actions, date formatter, and transaction link; advance the route cache key after the contract is green.

**Tech Stack:** Static HTML/CSS/JavaScript, Python `unittest` string-contract tests, local `python3 -m http.server`, Browser plugin or Python Playwright for rendered QA.

## Global Constraints

- Change only `vedolo-preview.html`, `vedolo/index.html`, and `tests/test_vedolo_preview_contracts.py` for the implementation.
- Preserve flow data, filters, sorting, pagination, action URLs, and the existing 71px stable row height.
- Keep the wallet label white and the shortened address grey on desktop and mobile.
- Keep the second-line order exact: address, Copy, DeBank, date, transaction link.
- Do not introduce new dependencies or refactor other wallet tables.

---

### Task 1: Implement the two-line veDOLO Flow wallet cell

**Files:**
- Modify: `tests/test_vedolo_preview_contracts.py`
- Modify: `vedolo-preview.html:611-625`
- Modify: `vedolo-preview.html:1898-1915`
- Modify: `vedolo/index.html:22`

**Interfaces:**
- Consumes: `vedoloAddressName(address)`, `vedoloAddressInfo(address)`, `shortAddr(address)`, `flowDateLine(date, txHash, timestamp)`, `copySvg`, and the existing DeBank URL.
- Produces: `flowAddressCell(...) -> string` with `.flow-wallet-top` followed by `.flow-address-main`; `.flow-address-main` contains `.addr-mono`, Copy, DeBank, then `.flow-address-meta`.

- [ ] **Step 1: Add a failing contract test**

Append this test to `VeDoloPreviewContractsTest`:

```python
def test_flow_wallet_cell_uses_shared_two_line_identity_hierarchy(self):
    flow_cell = re.search(
        r'function flowAddressCell\(addr, date, txHash, timestamp\)\{(?P<body>.*?)\n\}',
        self.html,
        re.S,
    ).group("body")
    required = (
        'class="flow-wallet-top"',
        'class="addr-name ${info ? "" : "addr-generic"}"',
        'class="flow-tx-line flow-address-main"',
        'class="addr-mono addr-tooltip-wrap"',
        'class="copy-btn addr-copy"',
        'class="addr-debank"',
        '${flowDateLine(date, txHash, timestamp)}',
    )
    positions = [flow_cell.index(item) for item in required]
    self.assertEqual(positions, sorted(positions))
    self.assertNotIn('class="addr addr-tooltip-wrap"', flow_cell)
    self.assertIn('.flow-wallet-top .addr-name{', self.html)
    self.assertIn('.flow-address-main .addr-mono{', self.html)
    self.assertIn('color:var(--fg-1)', self.html)
    self.assertIn('color:var(--fg-3)', self.html)
    self.assertIn('vedolo-flow-wallet-hierarchy-20260810', self.route)
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
python3 -m unittest tests.test_vedolo_preview_contracts.VeDoloPreviewContractsTest.test_flow_wallet_cell_uses_shared_two_line_identity_hierarchy
```

Expected: FAIL because `.flow-wallet-top`, the grey `.addr-mono` address, and the new cache key do not exist in the old flow cell.

- [ ] **Step 3: Implement the minimal flow-scoped CSS**

Replace the current flow address rules with:

```css
.flow-wallet-top{display:flex;align-items:center;min-width:0}
.flow-wallet-top .addr-name{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:13px;font-weight:600;line-height:1.2;letter-spacing:-.1px;color:var(--fg-1)}
.flow-address-main{display:flex;align-items:center;gap:6px;min-width:0;flex-wrap:nowrap}
.flow-address-main .addr-mono{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:var(--mono);font-size:11px;font-weight:500;line-height:1.2;color:var(--fg-3)}
.flow-address-meta{display:flex;align-items:center;gap:6px;min-width:0;margin-left:2px;padding-left:8px;border-left:1px solid var(--line-1);font-family:var(--mono);font-size:10.5px;line-height:1.2;color:var(--fg-3);white-space:nowrap}
.flow-address-meta span{min-width:0;overflow:hidden;text-overflow:ellipsis}
```

Leave the existing Copy, DeBank, transaction-link, and mobile font-size rules intact.

- [ ] **Step 4: Implement the approved DOM order**

Change `flowAddressCell(...)` to:

```javascript
function flowAddressCell(addr, date, txHash, timestamp){
  const displayAddr = addr || "";
  const info = vedoloAddressInfo(displayAddr);
  return `<div class="flow-tx flow-address-cell">
    <div class="flow-wallet-top">
      <span class="addr-name ${info ? "" : "addr-generic"}">${esc(vedoloAddressName(displayAddr))}</span>
    </div>
    <div class="flow-tx-line flow-address-main">
      <span class="addr-mono addr-tooltip-wrap" data-full-addr="${esc(displayAddr)}">${esc(shortAddr(displayAddr))}</span>
      <button class="copy-btn addr-copy" data-copy="${esc(displayAddr)}" aria-label="Copy address">${copySvg}</button>
      <a class="addr-debank" href="https://debank.com/profile/${esc(displayAddr)}" target="_blank" rel="noopener" aria-label="View on DeBank" onclick="event.stopPropagation()"><img src="https://debank.com/favicon.ico" alt="DeBank" onerror="this.outerHTML='<span class=&quot;debank-fallback&quot;>D</span>'"></a>
      ${flowDateLine(date, txHash, timestamp)}
    </div>
  </div>`;
}
```

- [ ] **Step 5: Advance the route cache key**

Append `-vedolo-flow-wallet-hierarchy-20260810` to the `version` value in `vedolo/index.html`.

- [ ] **Step 6: Run focused and full GREEN checks**

Run:

```bash
python3 -m unittest tests.test_vedolo_preview_contracts.VeDoloPreviewContractsTest.test_flow_wallet_cell_uses_shared_two_line_identity_hierarchy
python3 -m unittest tests.test_vedolo_preview_contracts
git diff --check
```

Expected: focused test PASS, full veDOLO suite PASS with 31 tests, and no whitespace errors.

- [ ] **Step 7: Commit the implementation**

```bash
git add vedolo-preview.html vedolo/index.html tests/test_vedolo_preview_contracts.py
git commit -m "fix: align veDOLO flow wallet identity"
```

---

### Task 2: Verify responsive behavior and publish

**Files:**
- Verify: `vedolo-preview.html`
- Verify: `vedolo/index.html`
- Temporary only: `/tmp/verify_vedolo_flow_wallet.py`

**Interfaces:**
- Consumes: the Task 1 DOM/CSS contract and local static JSON loaded by the veDOLO page.
- Produces: rendered desktop/mobile evidence and a live GitHub Pages deployment from production `master`.

- [ ] **Step 1: Start the static server and inspect the local page**

Run `python3 skills/webapp-testing/scripts/with_server.py --help`, then execute a reviewed literal `python3 -m http.server` command through the helper or start it directly on an unused localhost port.

Open `vedolo-preview.html?qa=1&v=vedolo-flow-wallet-hierarchy-20260810` at `1440x900` and `390x844`.

- [ ] **Step 2: Verify the rendered contract**

For the first real row in both `#locks-table` and `#unlocks-table`, assert:

```text
.flow-wallet-top precedes .flow-address-main
.addr-name computed color equals --fg-1
.addr-mono computed color equals --fg-3
.addr-copy is visible and writes the exact data-copy address
.addr-debank is visible and href equals https://debank.com/profile/<exact address>
.flow-address-meta is inside .flow-address-main after DeBank
row height is 71px (±1px)
no address/date/action overflow or clipping
```

Click Copy and confirm the copied-state UI. Confirm no relevant console/page errors and no failed request for the veDOLO page's own assets.

- [ ] **Step 3: Run final pre-push verification**

Run:

```bash
python3 -m unittest tests.test_vedolo_preview_contracts
git diff --check
git status --short
```

Expected: 31 tests PASS, clean diff check, clean worktree.

- [ ] **Step 4: Push and merge to production master**

Push `codex/vedolo-flow-wallet-cell`, create a ready pull request against `master`, wait for required GitHub checks, and merge only when green. Do not force-push and do not overwrite automatic data commits on `master`.

- [ ] **Step 5: Verify GitHub Pages live**

Wait for the Pages deployment triggered by the merge. Load:

```text
https://twojekrypto.github.io/Dolomite-dashboard/vedolo/?v=vedolo-flow-wallet-hierarchy-20260810&t=<unique-timestamp>
```

Repeat the wallet hierarchy, Copy/DeBank visibility, row-height, console, and mobile checks. Report the PR, merge commit, Pages workflow, and live URL.
