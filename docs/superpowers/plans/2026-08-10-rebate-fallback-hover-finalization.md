# Rebate Fallback and Address Hover Finalization Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the two fail-closed revenue gaps, confirm repeated-address hover semantics in every opted-in table, and publish the verified result to the GitHub Pages `master` branch.

**Architecture:** Keep the existing revenue recovery and shared address-hover implementation. Add narrow validation at the two trust boundaries: prior epoch rows may be restored only when their epoch window is canonical, bounded by the refreshed epoch range, and unambiguous; canonical token symbols may be reused only when they are real symbols rather than address-derived placeholders. Exercise both behaviors with focused regressions before changing production code.

**Tech Stack:** Python 3, `unittest`, vanilla JavaScript, Node.js tests, static JSON/HTML, local HTTP server, browser-computed QA, GitHub Actions and GitHub Pages.

---

### Task 1: Harden partial epoch preservation

**Files:**
- Modify: `tests/test_fetch_dolomite_revenue.py`
- Modify: `fetch_dolomite_revenue.py`

- [x] Add RED tests proving a forged future epoch and duplicate prior rows are not restored.
- [x] Require exact integer epoch IDs, canonical `rebate_epoch_window(...)` timestamps, an epoch no newer than the refreshed maximum, and exactly one prior candidate per missing epoch.
- [x] Run focused and complete revenue tests.

### Task 2: Reject address-derived token symbols

**Files:**
- Modify: `tests/test_fetch_dolomite_revenue.py`
- Modify: `fetch_dolomite_revenue.py`

- [x] Add a RED test for a matching canonical token whose symbol is the legacy address prefix.
- [x] Validate both RPC and canonical symbols and fail closed when neither is a legitimate symbol.
- [x] Run focused and complete revenue tests.

### Task 3: Revalidate the artifact and repeated-address hover

**Files:**
- Verify: `dolomite_revenue.json`
- Verify: `shared-hover-tooltips.js`
- Verify: `tests/address-match-highlighting.test.js`
- Verify: the nine pages loading the shared hover assets

- [x] Regenerate the revenue artifact with the official fetcher and run all validators.
- [x] Confirm epoch 9 and 10 exact totals, July 30 onward pending, and no invalid symbol placeholders.
- [x] Run Node tests and browser checks for row hover versus direct address hover on desktop and mobile without layout shift.

### Task 4: Publish and verify production

- [ ] Build a clean release from the latest remote `master`, apply only the intended product changes, and rerun targeted checks.
- [ ] Push the release branch, merge it to `master`, wait for GitHub Actions/Pages, and verify the live artifact and UI.
- [ ] Report the live commit, workflow result, exact data reconciliation, and the table-hover behavior.
