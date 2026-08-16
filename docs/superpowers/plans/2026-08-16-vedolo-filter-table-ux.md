# veDOLO Filter and Position Activity Table UX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace two pill segments with Portfolio-style single-select dropdowns and promote veDOLO Position Activity to a standalone searchable 10-row table.

**Architecture:** Keep the existing static HTML architecture and reuse the page's existing `.dd`, `.dd-btn`, `.dd-panel`, `.dd-opt`, `.search`, table, and pager primitives. Separate activity query state from flow query state so each card rerenders independently.

**Tech Stack:** Static HTML/CSS/JavaScript, Python `unittest` contract tests, local HTTP server, Browser plugin.

## Global Constraints

- Do not change blockchain data, data sources, calculations, transaction URLs, or default sorting.
- Route and activity filters are single-select dropdowns.
- Position Activity has its own search and displays 10 rows per page.
- Mobile may scroll inside the activity table but must not create document-level horizontal overflow.

---

### Task 1: Contract the standalone table and dropdown behavior

**Files:**
- Modify: `tests/test_vedolo_preview_contracts.py`
- Modify: `vedolo-preview.html`

**Interfaces:**
- Consumes: existing `state.flows.source`, `state.activity.kind`, `setDropdown`, `openDropdown`, `closeDropdowns`, `renderLocks`, `renderUnlocks`, and `renderPositionActivity`.
- Produces: `state.activity.q`, `#q-position-activity`, `#dd-flow-source`, `#dd-position-activity-kind`, and a standalone `.position-activity-card`.

- [ ] **Step 1: Write failing behavioral contract tests**

Assert that Activity is outside `.flow-card`, has its own `card-head` and toolbar, contains `#q-position-activity`, uses the two dropdown IDs/options, stores `activity:{q:"",kind:"all",...,perPage:10}`, and no longer uses pill button handlers.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `python3 -m unittest tests.test_vedolo_preview_contracts`

Expected: failures for missing standalone card, dropdowns, independent query state, and 10-row pagination.

- [ ] **Step 3: Implement the minimal markup, state, and event wiring**

Move Position Activity after the closing `veDOLO Flow` section. Add its search and dropdown. Convert route pills to a dropdown. Route dropdown selections update only locks; activity selections and activity search update only the activity table.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run: `python3 -m unittest tests.test_vedolo_preview_contracts tests.test_address_match_table_scope`

Expected: all tests pass.

### Task 2: Polish geometry and verify rendered behavior

**Files:**
- Modify: `vedolo-preview.html`
- Modify: `vedolo/index.html`

**Interfaces:**
- Consumes: approved dropdown/table primitives and existing `tableSpacerRows`/pager renderer.
- Produces: stable 10-row body, fixed activity colgroup, desktop toolbar layout, mobile-contained horizontal scrolling, fresh route cache key.

- [ ] **Step 1: Apply fixed column and responsive layout rules**

Use a 48px rank column, 300px address column, 118px action column, 180px position column, 150px DOLO column, and 140px lock-end column with a 936px minimum table width. Keep numeric headings and cells aligned right.

- [ ] **Step 2: Run static and syntax checks**

Run: `python3 -m unittest tests.test_vedolo_preview_contracts tests.test_address_match_table_scope && node --check vedolo-position-activity.js && git diff --check`

Expected: exit 0.

- [ ] **Step 3: Browser-verify interactions and viewports**

Serve with `python3 -m http.server`. Verify at 1440×900, 1024×768, and 390×844: route dropdown changes locks only; activity dropdown changes Activity only; search and clear work; 10 stable rows render; pager advances; no document overflow; mobile scroll stays inside the activity table.

- [ ] **Step 4: Refresh route cache, review, commit, rebase, and deploy**

Update `vedolo/index.html`, request scoped code review, fix all Important findings, commit only scoped files, rebase on current remote `master`, rerun checks, push non-force to `master`, and verify Pages plus live DOM.
