# Address Highlight Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make repeated wallet-address highlights clearly visible and remove the help cursor from every canonical dashboard address without changing table layout.

**Architecture:** Keep address detection and table scoping unchanged. Strengthen only the shared CSS source/peer states, add semantic cursor rules for canonical address wrappers, and rotate the existing preview/route cache keys so the static GitHub Pages deployment updates immediately.

**Tech Stack:** Static HTML/CSS, delegated vanilla JavaScript, Node test runner, Python unittest, Chromium browser verification, GitHub Pages.

## Global Constraints

- The gold treatment applies only to rendered address text, never to Wallet labels, cells, or rows.
- Clickable address links use `cursor: pointer`; non-clickable address text uses `cursor: default`; genuine help controls retain `cursor: help`.
- Matching remains exact, table-scoped, and active only when at least two visible address strings share one canonical EVM address.
- Do not add padding, borders, display changes, dependencies, configuration, data files, or workflow changes.
- Preserve reduced-motion behavior and the existing full-address tooltip.

---

### Task 1: Premium Strong address and cursor treatment

**Files:**
- Create temporarily: `/tmp/verify_address_highlight_contract.py`
- Modify: `shared-hover-tooltips.css:38-68`
- Test: `tests/address-match-highlighting.test.js`

**Interfaces:**
- Consumes: `.addr-tooltip-wrap[data-full-addr]`, `.address-match-source`, and `.address-match-peer` classes produced by `shared-hover-tooltips.js`.
- Produces: stronger computed source/peer styles plus semantic computed cursors for canonical address anchors and spans.

- [ ] **Step 1: Write the failing real-browser style contract**

Create a temporary Playwright script that loads the real shared stylesheet into a minimal table fixture and asserts the intended computed behavior:

```python
from playwright.sync_api import sync_playwright

HTML = """
<link rel="stylesheet" href="http://127.0.0.1:8765/shared-hover-tooltips.css">
<table data-address-match-cells><tbody><tr><td>
  <a id="source" class="addr-tooltip-wrap address-match-active address-match-source"
     data-full-addr="0x1111111111111111111111111111111111111111">0x1111...1111</a>
  <span id="peer" class="addr-tooltip-wrap address-match-active address-match-peer"
        data-full-addr="0x1111111111111111111111111111111111111111">0x1111...1111</span>
  <span id="help" data-tooltip="Help">?</span>
</td></tr></tbody></table>
"""

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 390, "height": 300})
    page.set_content(HTML, wait_until="networkidle")
    result = page.evaluate("""() => {
      const style = id => getComputedStyle(document.getElementById(id));
      const box = id => document.getElementById(id).getBoundingClientRect().toJSON();
      return {
        source: {background: style('source').backgroundColor, color: style('source').color,
                 shadow: style('source').boxShadow, cursor: style('source').cursor, box: box('source')},
        peer: {background: style('peer').backgroundColor, shadow: style('peer').boxShadow,
               cursor: style('peer').cursor, box: box('peer')},
        helpCursor: style('help').cursor,
      };
    }""")
    assert result["source"]["background"] == "rgba(201, 162, 39, 0.22)"
    assert result["source"]["color"] == "rgb(229, 200, 93)"
    assert "0.92" in result["source"]["shadow"] and "16px" in result["source"]["shadow"]
    assert result["peer"]["background"] == "rgba(201, 162, 39, 0.12)"
    assert "0.52" in result["peer"]["shadow"]
    assert result["source"]["cursor"] == "pointer"
    assert result["peer"]["cursor"] == "default"
    assert result["helpCursor"] == "help"
    browser.close()
```

- [ ] **Step 2: Run the contract to verify RED**

Run:

```bash
python3 skills/webapp-testing/scripts/with_server.py \
  --server "python3 -m http.server 8765 --bind 127.0.0.1" \
  --port 8765 --timeout 30 \
  python3 /tmp/verify_address_highlight_contract.py
```

Expected: FAIL because the current source wash is `0.12`, the source text is not promoted to gold, and both address elements inherit `cursor: help`.

- [ ] **Step 3: Implement the minimal shared CSS change**

Add semantic cursor overrides after the generic tooltip cursor rule:

```css
.addr-tooltip-wrap[data-full-addr] {
  cursor: default;
}

a.addr-tooltip-wrap[data-full-addr] {
  cursor: pointer;
}
```

Use the approved visual values:

```css
table[data-address-match-cells] tbody .addr-tooltip-wrap.address-match-active.address-match-source {
  color: var(--gold-hi, #e5c85d);
  background-color: rgba(201, 162, 39, .22);
  box-shadow:
    0 0 0 1px rgba(201, 162, 39, .92),
    0 0 0 3px rgba(201, 162, 39, .12),
    0 0 16px rgba(201, 162, 39, .24);
}

table[data-address-match-cells] tbody .addr-tooltip-wrap.address-match-active.address-match-peer {
  background-color: rgba(201, 162, 39, .12);
  box-shadow:
    0 0 0 1px rgba(201, 162, 39, .52),
    0 0 8px rgba(201, 162, 39, .10);
}
```

Extend the existing transition with `color` while preserving the reduced-motion override. Do not add padding or a CSS border.

- [ ] **Step 4: Run GREEN behavior checks**

Run the temporary browser contract and the delegated interaction suite:

```bash
python3 skills/webapp-testing/scripts/with_server.py \
  --server "python3 -m http.server 8765 --bind 127.0.0.1" \
  --port 8765 --timeout 30 \
  python3 /tmp/verify_address_highlight_contract.py
node --test tests/address-match-highlighting.test.js
```

Expected: the browser contract passes and all seven address-matching interaction tests pass.

- [ ] **Step 5: Commit the visual behavior**

```bash
git add shared-hover-tooltips.css
git commit -m "fix: strengthen repeated address highlights"
```

### Task 2: Cache rotation for all affected routes

**Files:**
- Modify: `tests/test_address_match_table_scope.py:90-127`
- Modify: `vedolo-preview.html:11-12`
- Modify: `odolo-preview.html:11-12`
- Modify: `liquidation-preview.html:14,20551`
- Modify: `vedolo/index.html:22`
- Modify: `odolo/index.html:22`
- Modify: `borrow/index.html:22`
- Modify: `liquidation/index.html:22`
- Modify: `supply/index.html:22`

**Interfaces:**
- Consumes: static route loader `version` strings and shared asset `?v=` keys.
- Produces: `20260804-address-strong` shared-asset version and `address-strong-20260804` route version suffix.

- [ ] **Step 1: Change the cache contract first**

In `tests/test_address_match_table_scope.py`, set:

```python
version = "20260804-address-strong"
```

and require:

```python
self.assertIn("address-strong-20260804", route, route_name)
```

- [ ] **Step 2: Run the cache test to verify RED**

Run:

```bash
python3 -m unittest tests.test_address_match_table_scope -q
```

Expected: FAIL because previews and route loaders still use the `address-link` cache versions.

- [ ] **Step 3: Rotate only the affected cache keys**

Change the three preview references to `shared-hover-tooltips.css?v=20260804-address-strong` and `shared-hover-tooltips.js?v=20260804-address-strong`, including the dynamic fallback in `liquidation-preview.html`. Replace only the final route suffix `address-link-20260804` with `address-strong-20260804` in the five affected route loaders.

- [ ] **Step 4: Run the cache contract to verify GREEN**

```bash
python3 -m unittest tests.test_address_match_table_scope -q
```

Expected: all three scope/cache tests pass and the audited opt-in table count remains exactly 10.

- [ ] **Step 5: Commit cache rotation and tests**

```bash
git add tests/test_address_match_table_scope.py \
  vedolo-preview.html odolo-preview.html liquidation-preview.html \
  vedolo/index.html odolo/index.html borrow/index.html liquidation/index.html supply/index.html
git commit -m "test: rotate address highlight cache keys"
```

### Task 3: Browser QA, regression review, and live publication

**Files:**
- Verify: `shared-hover-tooltips.css`
- Verify: `shared-hover-tooltips.js`
- Verify: affected preview and route files from Task 2

**Interfaces:**
- Consumes: local routed Borrow and veDOLO pages plus the GitHub Pages production URL.
- Produces: browser evidence for computed styles, cursor semantics, layout stability, and successful live deployment.

- [ ] **Step 1: Run the complete local regression set**

```bash
node --check shared-hover-tooltips.js
node --test tests/address-match-highlighting.test.js tests/route-loader-scripts.test.js
python3 -m unittest \
  tests.test_address_match_table_scope \
  tests.test_vedolo_preview_contracts \
  tests.test_odolo_preview_contracts \
  tests.test_borrow_ux_contracts \
  tests.test_supply_activity_ui_contracts \
  tests.test_pages_workflow_contracts -q
git diff --check dolomite-dashboard/master..HEAD
```

Expected: 9 Node tests and 87 Python contract tests pass with no whitespace errors.

- [ ] **Step 2: Verify real rendered tables with Browser**

Start the reviewed literal server command:

```bash
python3 -m http.server 8765 --bind 127.0.0.1
```

Use the Browser plugin on `/borrow/` and `/vedolo/` at 1440px and 390px. Hover a repeated shortened address and assert with computed styles and bounding boxes:

- source background is `rgba(201, 162, 39, 0.22)` and its text is gold;
- peer background is `rgba(201, 162, 39, 0.12)`;
- the clicked-capable Borrow address cursor is `pointer`;
- the non-clickable veDOLO address cursor is `default`;
- a genuine help control remains `help`;
- no `td` or `tr` receives address-match classes;
- source, cell, and row bounding boxes are unchanged before/after hover;
- no relevant console errors or framework overlay appears.

- [ ] **Step 3: Perform two review passes**

Correctness/regression: confirm only shared styling, tests, and cache keys changed; all 10 opted-in tables retain exact matching and no table markup or `nth-child` selector changed.

Maintainability/security: confirm no dependency, configuration, secret, generated data, workflow, or metric logic changed; cursor behavior is centralized in one shared selector.

- [ ] **Step 4: Rebase and push safely to production**

```bash
git fetch dolomite-dashboard master
git rebase dolomite-dashboard/master
git push dolomite-dashboard HEAD:master
```

If automation advances `master`, repeat fetch/rebase and retry the non-force push. Never force-push.

- [ ] **Step 5: Confirm GitHub Pages and live assets**

Wait for Secret Leak Guard, Earn Audit Checks, and Deploy GitHub Pages. Confirm the Pages smoke test succeeds, then fetch the live CSS with a cache-busting query and verify that it contains the stronger source/peer treatment and semantic canonical-address cursor rules.
