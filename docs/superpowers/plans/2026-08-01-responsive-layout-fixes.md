# Responsive Layout Fixes Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove the confirmed navigation, table, simulator, and touch-target regressions across desktop, tablet, and mobile without changing dashboard data or desktop visual identity.

**Architecture:** Keep the fixes in the existing shared responsive layers (`mobile-nav.css`, `mobile-polish.css`) and in the two route-owned surfaces (`vedolo-preview.html`, `liquidation-preview.html`). Add source-contract regression tests and validate computed layout in the in-app browser served over HTTP.

**Tech Stack:** Static HTML/CSS/JavaScript, Python `unittest`, GitHub Pages.

---

### Task 1: Add responsive regression contracts

**Files:**
- Create: `tests/test_responsive_layout_contracts.py`
- Modify: `tests/test_borrow_ux_contracts.py`

**Step 1: Write the failing tests**

Add contracts that require the shared mobile navigation to engage before the desktop tabs clip, cap the open menu by dynamic viewport height, provide a scrollable veDOLO Revenue Impact table, preserve a readable simulator percentage field, and expose 44px mobile touch targets.

**Step 2: Run the tests to verify they fail**

Run: `python3 -m unittest tests.test_responsive_layout_contracts tests.test_borrow_ux_contracts`

Expected: FAIL on the newly required responsive rules while all unrelated contracts remain green.

### Task 2: Fix shared navigation and mobile menu geometry

**Files:**
- Modify: `mobile-nav.css`
- Modify: `route-loader.js`

**Step 1: Move the shared mobile breakpoint above the measured desktop-nav width**

Switch the shared navigation at `1180px`, which is above the measured 1179px tab strip and prevents clipping at 1024px.

**Step 2: Reserve viewport space below the menu**

Use a `100dvh`-based maximum height and safe bottom reserve so every destination, including History, can be scrolled into view on 320×568 screens.

**Step 3: Bump the route asset cache key**

Update `NAV_VERSIONS.nav` so GitHub Pages clients receive the new CSS immediately.

### Task 3: Fix route-specific responsive surfaces

**Files:**
- Modify: `vedolo-preview.html`
- Modify: `liquidation-preview.html`
- Modify: `mobile-polish.css`
- Modify: `tvl/supply-health.css`
- Modify: `dashboard-core.css`

**Step 1: Make Revenue Impact internally scrollable on narrow screens**

Keep the panel inside the viewport, allow horizontal table scrolling, and give the metric table a stable minimum width so labels and values are not cut off.

**Step 2: Make Risk Simulator controls usable at 320px**

Keep the percentage value centered over its rail, enforce readable value width, and enlarge step/remove controls while preserving the fixed three-row simulator viewport.

**Step 3: Raise compact mobile actions to 44px touch targets**

Apply mobile-only minimum dimensions to address actions, pagination, filters, and Details controls without changing desktop table density.

### Task 4: Verify all affected routes and publish

**Files:**
- Test: all changed files

**Step 1: Run source and syntax checks**

Run:
- `python3 -m unittest tests.test_responsive_layout_contracts tests.test_borrow_ux_contracts tests.test_vedolo_preview_contracts tests.test_pages_workflow_contracts`
- `npm run check:earn-audit`
- `node --check route-loader.js`

Expected: all checks pass.

**Step 2: Run browser-computed layout checks**

Serve with `python3 -m http.server 8765` and inspect 1920, 1366, 1180, 1024, 768, 390, and 320 widths. Verify navigation visibility, menu scroll reachability, Revenue Impact bounds, Risk Simulator control geometry, document overflow, and console output.

**Step 3: Commit and deploy production**

Commit only the responsive files and tests, fetch/rebase onto the latest `dolomite-dashboard/master`, push `HEAD:master`, then confirm the production commit and live page assets.
