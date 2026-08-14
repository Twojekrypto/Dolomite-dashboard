# veDOLO Early Exit Oxblood Continuity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the remaining neutral table-header strip and make Early Exit Analytics and Recent Early Exits read as one cohesive premium oxblood suite.

**Architecture:** Extend the existing `exit-suite` CSS token set with explicit toolbar and table-header layers. Apply an early-exit-specific selector after the generic institutional table rules so unrelated tables remain untouched, then advance the route cache key and verify computed browser styles at desktop and mobile widths.

**Tech Stack:** Static HTML/CSS, Python contract tests, local HTTP server, Browser plugin or Playwright fallback.

## Global Constraints

- Preserve the existing Graphite + Gold identity and muted coral `#c98678` accent.
- Do not change data, sorting, filtering, pagination, address interactions, or table geometry.
- Do not use `!important`.
- Keep the period dropdown unclipped and the outer corner geometry intact.
- Verify 1440×900 and 390×844.

---

### Task 1: Own every early-exit surface layer

**Files:**
- Modify: `tests/test_vedolo_preview_contracts.py`
- Modify: `vedolo-preview.html`
- Modify: `vedolo/index.html`

**Interfaces:**
- Consumes: existing `.exit-suite`, `#exits-table`, and generic institutional table CSS.
- Produces: `--exit-surface-toolbar`, `--exit-surface-th`, and a scoped column-header rule with higher specificity than the generic `#exits-table th` rule.

- [ ] **Step 1: Write the failing CSS ownership contract**

Extend `test_early_exit_cards_share_crimson_surface_and_rounded_geometry`:

```python
self.assertIn('--exit-surface-toolbar:', self.html)
self.assertIn('--exit-surface-th:', self.html)
self.assertIn(
    '.exit-suite.exit-table-card #exits-table th{background:var(--exit-surface-th);border-bottom:1px solid var(--exit-surface-line)}',
    self.html,
)
self.assertIn('vedolo-exit-oxblood-continuity-20260814', self.route)
```

This catches the current bug where the generic `#exits-table th` declaration overrides the earlier crimson rule.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
python3 -m unittest tests.test_vedolo_preview_contracts.VeDoloPreviewContractsTest.test_early_exit_cards_share_crimson_surface_and_rounded_geometry
```

Expected: FAIL because the new tokens, scoped override, and cache key do not exist.

- [ ] **Step 3: Implement the Layered Oxblood CSS**

Add explicit tokens to `.exit-suite` and use them for the controls and header band:

```css
.exit-suite{
  --exit-surface-toolbar:rgba(33,17,19,.58);
  --exit-surface-th:linear-gradient(180deg,rgba(47,23,25,.96),rgba(29,16,18,.98));
}
.exit-suite .toolbar{background:var(--exit-surface-toolbar);border-bottom-color:var(--exit-surface-line)}
.exit-suite.exit-table-card #exits-table th{background:var(--exit-surface-th);border-bottom:1px solid var(--exit-surface-line)}
```

Place the scoped column-header ownership rule after the generic institutional rule. Refine the existing base/body/footer gradients only within the approved muted oxblood range and keep body text contrast unchanged. Append `vedolo-exit-oxblood-continuity-20260814` to the route version.

- [ ] **Step 4: Run the veDOLO contracts**

Run:

```bash
python3 -m unittest tests.test_vedolo_preview_contracts
```

Expected: PASS.

- [ ] **Step 5: Verify rendered behavior**

Serve the static site over HTTP and verify the target flow:

```text
/vedolo/ -> Early Exit Analytics and Recent Early Exits render -> open the 30D period dropdown -> header stays oxblood, dropdown is visible, and both cards remain inside the viewport.
```

At 1440×900 and 390×844, record computed values/bounding boxes for:

```js
getComputedStyle(document.querySelector('#exits-table th')).backgroundImage
getComputedStyle(document.querySelector('#early-exit-analytics')).borderColor
getComputedStyle(document.querySelector('#recent-early-exits-section')).borderColor
document.querySelector('#recent-early-exits-section').getBoundingClientRect()
```

Capture screenshots outside the repository and confirm no relevant console errors.

- [ ] **Step 6: Commit the visual refinement**

```bash
git add vedolo-preview.html vedolo/index.html tests/test_vedolo_preview_contracts.py
git commit -m "fix: unify early exit oxblood surfaces"
```

---

### Task 2: Release verification and production push

**Files:**
- Verify all files changed by both plans.

**Interfaces:**
- Consumes: the three independently green implementation commits.
- Produces: a clean, production-ready branch pushed to `dolomite-dashboard/master`.

- [ ] **Step 1: Run combined verification**

```bash
python3 -m unittest tests.test_earn_dashboard_contracts tests.test_vedolo_preview_contracts
node --test tests/dolomite-token-icons.test.mjs
bash -n scripts/stage_earn_publishable_wallet_paths.sh
node --check scripts/sync_dolomite_token_icons.mjs
git diff --check dolomite-dashboard/master...HEAD
```

Expected: every command passes.

- [ ] **Step 2: Perform two self-review passes**

- Correctness/regression: inspect only the branch diff, verify every changed line maps to the approved specs, and confirm oDOLO production code is untouched because its fix is already live.
- Maintainability/security: confirm no secrets, new dependencies, broad formatting, unbounded retry, or unrelated generated data entered the diff.

- [ ] **Step 3: Rebase on the latest production branch and re-run narrow checks**

```bash
git fetch dolomite-dashboard master
git rebase dolomite-dashboard/master
python3 -m unittest tests.test_earn_dashboard_contracts tests.test_vedolo_preview_contracts
node --test tests/dolomite-token-icons.test.mjs
git diff --check dolomite-dashboard/master...HEAD
```

- [ ] **Step 4: Push production**

```bash
git push dolomite-dashboard HEAD:master
```

- [ ] **Step 5: Confirm deployment state**

Verify the remote `master` SHA equals local `HEAD`, inspect the newly triggered workflow runs, and load the live veDOLO page with its new cache key.
