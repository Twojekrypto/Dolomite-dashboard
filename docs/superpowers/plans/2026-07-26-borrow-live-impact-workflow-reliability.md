# Borrow Live Impact and Workflow Reliability — Implementation Plan

> Execute test-first and keep changes limited to Borrow UI, liquidation position
> history, its contracts, and directly affected workflow recovery.

## Task 1: Lock the data and UI contracts with failing tests

**Files**

- Modify: `tests/test_fetch_liquidation_risk.py`
- Modify: `tests/test_borrow_ux_contracts.py`

1. Add a generator test proving that an out-of-skew nearest snapshot produces
   `fallbackChange` with the actual `windowSeconds` while `change24h` stays null.
2. Update the committed-history consistency test to validate a nullable exact
   change and a required valid fallback when older observations exist.
3. Add contracts for the five-column Liquidation History table and absence of
   the translated spacer.
4. Add simulator contracts for Build Scenario, Price shock presets, Live
   Impact, the zero state, and risk level.
5. Run the targeted tests and confirm the new expectations fail before editing
   production code.

## Task 2: Add the honest fallback comparison

**Files**

- Modify: `fetch_liquidation_risk.py`
- Modify: `liquidation-preview.html`

1. Select the non-current retained observation nearest to the 24-hour target.
2. Serialize `fallbackChange` only when a fallback is needed and available.
3. Make the badge prefer `change24h`, then `fallbackChange`.
4. Label fallback results with the actual rounded duration and accessible
   baseline-building copy.
5. Preserve the unavailable state only when there is no usable older snapshot.
6. Run generator and Borrow contract tests.

## Task 3: Remove the Liquidation History hover seam

**File**

- Modify: `liquidation-preview.html`

1. Remove the Liquidation History spacer from colgroup, header, and rendered
   rows.
2. Update amount-column selectors from 5/6 to 4/5.
3. Remove Date translation and use normal padding.
4. Change empty rows from colspan 6 to 5.
5. Audit every Liquidation History `nth-child` selector with `rg`.
6. Run Borrow contracts.

## Task 4: Implement Institutional Live Impact

**File**

- Modify: `liquidation-preview.html`

1. Replace the editor and result copy.
2. Add row-level preset buttons that update the existing input and invoke the
   existing simulation path.
3. Add the primary result sentence, risk level, and zero/active state elements.
4. Update `runMultiAssetSim` so these labels follow the current computed result.
5. Add final targeted CSS for the causal two-column hierarchy and mobile stack.
6. Run Borrow contracts.

## Task 5: Verify locally

1. Run:

   ```bash
   python3 -m unittest tests.test_fetch_liquidation_risk tests.test_borrow_ux_contracts
   python3 run_earn_audit_checks.py
   python3 -m py_compile fetch_liquidation_risk.py
   ```

2. Serve the worktree with `python3 -m http.server`.
3. In Chromium, verify:
   - badge content and tooltip/ARIA,
   - simulator zero state and a `−10%` preset,
   - five Liquidation History cells and equal hover backgrounds,
   - Date right edge meets Collateral left edge,
   - desktop and mobile bounding boxes do not overlap.
4. Request the repository-required browser subagent QA after the layout change.
5. Review `git diff --check`, scoped diff, and worktree status.

## Task 6: Publish and recover workflows

1. Commit the scoped change.
2. Push `HEAD` to production `master`.
3. Dispatch Update Liquidation Risk Data and wait for its generated-data commit.
4. Dispatch/observe Earn Audit Checks and representative EARN recovery.
5. Wait for the resulting GitHub Pages deployment.
6. Open the public Borrow route and confirm the badge, simulator, and table fix
   are live.
