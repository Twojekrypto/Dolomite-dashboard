# E-Mode Tooltip And EIP-7702 Wallet Classification Plan

> **For Codex:** Execute this plan inline with test-driven development and verify the rendered routes before deployment.

**Goal:** Use Dolomite's official E-Mode explanation everywhere the badge appears and stop classifying EIP-7702 delegated user accounts as protocol contracts/CA.

**Architecture:** Keep E-Mode copy local to the three existing render surfaces, because each page already owns its tooltip renderer. Add one exact EIP-7702 delegation-designator predicate to each Python data generator that performs `eth_getCode`, persist delegated accounts as `contract_wallet_type: delegated_eoa`, and make the DOLO classification UI fail safely if it encounters an older artifact that still contains `is_contract: true`.

**Tech Stack:** Static HTML/JavaScript, Python 3 generators, unittest, Node test runner, GitHub Pages route loader.

---

### Task 1: Lock the expected behavior with regression tests

**Files:**
- Modify: `tests/test_emode_ux_contracts.py`
- Modify: `tests/test_generate_dolo_holders.py`
- Modify: `tests/test_generate_dolo_flows_rpc.py`
- Modify: `tests/test_fresh_wallets.py`
- Modify: `tests/dolo-flow-protocol-filter.test.js`

1. Replace the expected E-Mode tooltip contract with the official wording supplied by the user.
2. Add a holder-generator test proving `0xef0100 || address` is classified as a delegated EOA and stale `is_contract` is removed.
3. Add a flow-generator test proving delegated EOAs are not returned as infrastructure contracts.
4. Add Python and browser-model classification tests proving delegated EOAs resolve to the wallet/EOA audience even when an older artifact still says `is_contract: true`.
5. Run the focused tests and confirm the new assertions fail for the intended reasons.

### Task 2: Implement the minimal data and UI correction

**Files:**
- Modify: `generate_dolo_holders.py`
- Modify: `generate_dolo_flows.py`
- Modify: `dolo-preview.html`
- Modify: `dolo_holders.json`

1. Detect only the exact 23-byte EIP-7702 delegation format (`0xef0100` plus a 20-byte address).
2. Preserve normal contract and Safe handling unchanged.
3. Mark delegated accounts as `delegated_eoa`, remove stale `is_contract`, and retain their delegate address as audit provenance.
4. Exclude delegated EOAs from flow contract exclusion.
5. Make the DOLO UI classify `delegated_eoa` as an EOA before checking legacy `is_contract`.
6. Repair the two currently affected rows in the generated holder artifact using the production detector.

### Task 3: Synchronize E-Mode copy and cache versions

**Files:**
- Modify: `portfolio-preview.html`
- Modify: `liquidation-preview.html`
- Modify: `dashboard-core.js`
- Modify: `earn/earn-core.js`
- Modify: `liq-monitor.html`
- Modify: `portfolio/index.html`
- Modify: `liquidation/index.html`
- Modify: `borrow/index.html`
- Modify: `supply/index.html`
- Modify: `earn/index.html`
- Modify: `earn/earn-core.html`
- Modify: `dashboard-core.html`
- Modify: `dolo/index.html`
- Modify: `index.html`

1. Replace the stale E-Mode explanation in Portfolio, Borrow/Liquidation, Earn, and the legacy monitor.
2. Keep the existing premium tooltip components and geometry unchanged.
3. Advance only the route/script cache keys needed for changed assets.

### Task 4: Verify and prepare deployment

1. Run focused Python and Node tests, syntax checks, and `git diff --check`.
2. Serve the worktree over `python3 -m http.server`.
3. Verify E-Mode tooltips and DOLO wallet classification at desktop and mobile widths using computed styles/bounding boxes.
4. Confirm both supplied addresses render as wallets, not CA, and that ordinary contracts and Safes retain their existing classifications.
5. Review the final diff, commit only task files, and push only with explicit deployment authorization.
