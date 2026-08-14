# oDOLO Analytics Data Audit Implementation Plan

> **Execution:** Use `superpowers:executing-plans` in the existing isolated worktree. Apply strict TDD for every production correction and verify live behavior before deployment.

**Goal:** Make Claimer Behavior, oDOLO Distribution, and oDOLO Claimer Breakdown reconcile to canonical on-chain sources and exact aggregate values, then publish the verified correction to GitHub Pages.

**Architecture:** Keep the existing static dashboard and scheduled JSON generators. Preserve the immutable 200M allocation model, add raw-wei provenance for exact supply reconciliation, treat canonical `RewardClaimed` events as authoritative from their first indexed block while retaining earlier reward-wallet transfers for historical coverage, and expose exact claimer aggregates so the UI never reconstructs token amounts from rounded percentages.

**Tech stack:** Python 3 generators and `unittest`, static HTML/JavaScript, JSON artifacts, GitHub Actions, GitHub Pages.

## Audit findings that the implementation must address

- The current Distribution components reconcile visually, but the artifact only stores floating-point display values and the validator accepts a two-token tolerance. The on-chain raw values reconcile exactly in wei and should become the validation source of truth.
- Claimer Behavior reconstructs displayed amounts from percentages rounded to one decimal place. On the audited snapshot this understates exercised, outflow, and remaining amounts by about 20.9K, 24.8K, and 8.4K oDOLO respectively.
- The cached transfer ledger classifies `0xd195…12a8` as a recent 6,553.71 oDOLO claim although neither the canonical claim index nor current-chain logs contain that claim in the declared 7D window. Recent canonical events must not be overridden by transfer-only cache entries.
- Claimer Breakdown row partitions reconcile within cent rounding, addresses are unique, and current `Held` correctly remains independent from the claimed lifecycle. Aggregate fields and provenance still need stronger validation.

## Constraints

- Allocation is exactly `200_000_000 * 10^18` raw units.
- Canonical claims require distributor `0x79e6e932bf6686a4d357d7821e6e08835ba8a026` and token `0x02e513b5b54ee216bf836ceb471507488fc89543`.
- Retain reward-wallet transfers only before the first canonical `RewardClaimed` block; use canonical events from that block onward.
- `Held` is current `balanceOf(wallet)` and is not part of `claimed = exercised + outflow + claim_remaining`.
- Keep backward-compatible UI fallbacks while making exact totals the primary path.
- Do not add dependencies or unrelated refactors.

## Task 1: Make Distribution validation exact in raw wei

**Files:**
- Modify: `fetch_odolo_contract.py`
- Modify: `validate_data.py`
- Test: `tests/test_odolo_contract_supply.py`

- [x] Add RED tests proving a one-wei partition mismatch fails while a fully reconciled raw snapshot passes.
- [x] Emit string raw fields for allocation, total supply, future rewards reserve, vester balance, circulating supply, and redeemed/burned supply.
- [x] Validate raw integer identities exactly and keep display-number checks only as secondary consistency checks.
- [x] Run `tests.test_odolo_contract_supply` GREEN.

## Task 2: Make canonical claim events authoritative after coverage begins

**Files:**
- Modify: `generate_odolo_flows.py`
- Modify: `validate_data.py`
- Test: `tests/test_generate_odolo_flows.py`
- Test: `tests/test_odolo_flows_validation.py`

- [x] Add RED fixtures with a valid pre-index reward transfer, a canonical indexed claim, and an unmatched post-index transfer.
- [x] Derive the first canonical event block from exact distributor/token events.
- [x] Build claim totals as historical pre-index reward transfers plus canonical indexed events; reject or report unmatched post-index transfer-only amounts instead of promoting them to claims.
- [x] Store claim-source reconciliation metadata and validate its structure and non-negative totals.
- [x] Apply the same source boundary to every Claimer Breakdown period.
- [x] Run generator and validation suites GREEN.

## Task 3: Use exact claimer aggregates in data and UI

**Files:**
- Modify: `generate_odolo_flows.py`
- Modify: `validate_data.py`
- Modify: `odolo-preview.html`
- Modify: `odolo/index.html`
- Test: `tests/test_generate_odolo_flows.py`
- Test: `tests/test_odolo_flows_validation.py`
- Test: `tests/test_odolo_preview_contracts.py`

- [x] Add RED tests for exact aggregate totals, row-to-aggregate reconciliation, and UI use of totals rather than rounded percentage reconstruction.
- [x] Emit exact display aggregates (`total_exercised`, `total_outflow`, `total_claim_remaining`, `total_held`) for all-time and every period.
- [x] Render the Claimer Behavior donut from exact token totals and derive the center percentage from those totals.
- [x] Advance the oDOLO route cache key.
- [x] Run all focused oDOLO suites GREEN.

## Task 4: Refresh, verify, and deploy

**Files:**
- Refresh: `odolo_contract_data.json`
- Refresh via official workflow: `odolo_flows.json` and canonical reward-claim artifacts

- [x] Run the official contract generator and oDOLO validators.
- [x] Run Python compile, JavaScript syntax, focused suites, and `git diff --check`.
- [x] Serve with `python3 -m http.server` and verify all three views at desktop and mobile widths, including computed layout and console/network health.
- [ ] Commit only audited files, fetch/rebase the latest remote `master`, rerun final checks, and push `HEAD:master`.
- [ ] Dispatch the oDOLO workflows, wait for successful regeneration, and verify the live GitHub Pages artifacts and UI with a cache-busting URL.

## Review hardening

- [x] Deduplicate overlapping claim indexes by canonical `(txHash, logIndex)` identity while retaining disjoint events.
- [x] Require complete Berachain claim-index coverage from deployment and cap the flow snapshot to its covered block.
- [x] Fail closed when canonical coverage is stale beyond the bounded workflow lag.
- [x] Reconcile published, row-rounded claim totals back to canonical source totals within the maximum cent-rounding envelope.
