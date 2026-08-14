# veDOLO Early Exit Crimson Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give Early Exit Analytics and Recent Early Exits one polished dark-crimson surface, complete rounded corners, and identical Data updated dots, then deploy it live.

**Architecture:** Keep the existing static HTML structure and implement the surface through narrowly scoped CSS under `.exit-suite`. Preserve `overflow: visible` for the period dropdown and round the painted first/last child surfaces explicitly. Extend the existing contract test and advance the veDOLO route cache key.

**Tech Stack:** Static HTML/CSS/JavaScript, Python `unittest`, Playwright browser verification, GitHub Pages route loader.

## Global Constraints

- Preserve the existing Graphite + Gold identity.
- Use a low-saturation dark-crimson wash, not a saturated warning-red card.
- Keep the period dropdown unclipped.
- Verify at 1440×900 and 390×844.
- Push the verified result to production `master`.

---

### Task 1: Lock the crimson surface contract

**Files:**
- Modify: `tests/test_vedolo_preview_contracts.py`
- Test: `tests/test_vedolo_preview_contracts.py`

**Interfaces:**
- Consumes: literal CSS and route cache tokens from the static veDOLO files.
- Produces: `test_early_exit_cards_share_crimson_surface_and_rounded_geometry` as the regression contract.

- [ ] **Step 1: Write the failing contract test**

Add assertions for the shared `--exit-surface-*` variables, explicit painted-child radii, a shared freshness-dot selector, the retained `overflow:visible`, and route token `vedolo-exit-crimson-surface-20260814`.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `python3 -m unittest tests.test_vedolo_preview_contracts.VeDoloPreviewContractsTest.test_early_exit_cards_share_crimson_surface_and_rounded_geometry`

Expected: FAIL because the new surface and cache tokens do not exist.

---

### Task 2: Implement the Crimson Glass surface

**Files:**
- Modify: `vedolo-preview.html`
- Modify: `vedolo/index.html`
- Test: `tests/test_vedolo_preview_contracts.py`

**Interfaces:**
- Consumes: the Task 1 literal CSS contract.
- Produces: shared `.exit-suite` surface variables and child-radius rules without changing table behavior.

- [ ] **Step 1: Add the minimal shared CSS**

Define the crimson surface tokens on `.exit-suite`; use them for the card background, border, inset depth, internal table surfaces, and both freshness dots. Override the later neutral `.exit-table-card` rules with narrowly scoped crimson header/footer rules. Add top radii to `.card-head`/`.exit-table-head` and bottom radii to `.exit-summary`/`.tbl-foot` while retaining `overflow:visible`.

- [ ] **Step 2: Advance the route cache key**

Append `vedolo-exit-crimson-surface-20260814` to the `version` in `vedolo/index.html`.

- [ ] **Step 3: Run the focused and full contract tests**

Run:

```bash
python3 -m unittest tests.test_vedolo_preview_contracts.VeDoloPreviewContractsTest.test_early_exit_cards_share_crimson_surface_and_rounded_geometry
python3 -m unittest tests.test_vedolo_preview_contracts
```

Expected: PASS, 0 failures.

---

### Task 3: Browser verification and production publication

**Files:**
- Verify: `vedolo-preview.html`
- Verify: `vedolo/index.html`

**Interfaces:**
- Consumes: rendered local veDOLO preview and route.
- Produces: computed-style, geometry, dropdown, responsive, and deployment evidence.

- [ ] **Step 1: Serve through HTTP and inspect both viewports**

Run `python3 -m http.server 8765` and use Playwright at 1440×900 and 390×844. Assert the two cards have matching computed background/border colors and freshness-dot colors, Recent Early Exits painted children have non-zero outer radii, both modified cards remain within the viewport, the table scroll remains internal, and the opened period panel is visible.

- [ ] **Step 2: Run static verification**

Run:

```bash
git diff --check
python3 -m unittest tests.test_vedolo_preview_contracts
```

Expected: PASS, 0 whitespace errors and 0 test failures.

- [ ] **Step 3: Review and commit only intended files**

Inspect `git diff -- vedolo-preview.html vedolo/index.html tests/test_vedolo_preview_contracts.py docs/superpowers/specs/2026-08-14-vedolo-early-exit-crimson-surface-design.md docs/superpowers/plans/2026-08-14-vedolo-early-exit-crimson-surface.md`, stage those paths, and commit with `fix: polish early exit crimson surfaces`.

- [ ] **Step 4: Synchronize and push production**

Fetch `dolomite-dashboard/master`, rebase the feature commit if remote advanced, rerun the contract suite, then push `HEAD:master` to `dolomite-dashboard`.
