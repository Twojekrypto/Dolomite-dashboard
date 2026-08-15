# Wallet Table UX Consistency Implementation Plan

> **For Codex:** Execute this plan task-by-task with test-driven development. Keep edits surgical and preserve unrelated generated data.

**Goal:** Unify wallet identity and Flow table UX, add exact last-transaction metadata to aggregate DOLO/oDOLO flow rows, merge Team/Investor wallets into Holder Distribution ranges, and expose address-level CEX details.

**Architecture:** Extend the canonical label module with a pure identity resolver, add a small shared wallet/table presentation asset, and keep thin page adapters for badges and amounts. Recover aggregate flow transaction metadata after ranking from exact single-block Transfer-log lookups, leaving the large transfer cache schema unchanged. Reuse existing holder history for Team/Investor and CEX address composition.

**Tech stack:** Static HTML/CSS/JavaScript, Node contract tests, Python `unittest`, JSON-RPC, GitHub Actions, Playwright/browser verification.

---

## Task 1: Lock the canonical wallet identity contract

**Files:**

- Modify: `tests/test_dolo_address_labels.py`
- Modify: `dolo-address-labels.js`
- Create: `wallet-table-ux.js`
- Create: `wallet-table-ux.css`
- Create: `tests/wallet-table-ux.test.js`

**Steps:**

1. Add RED tests proving canonical labels win over fallbacks, unknown addresses have no synthetic name, malformed addresses fail closed, and the shared renderer preserves copy/DeBank/address-match hooks.
2. Run the focused Python and Node tests and record the expected failures.
3. Add a pure `resolveDoloWalletIdentity` helper to the canonical label module.
4. Add a small presentation helper for known/unknown wallet cells, optional transaction metadata, stable sort markers, and English date formatting.
5. Add shared CSS tokens/classes for primary name, address actions, muted TX metadata, compact rank columns, stable inline-flex sort headers, and canonical Flow table typography.
6. Re-run focused tests, `node --check`, and `git diff --check`.
7. Commit: `feat: add canonical wallet table primitives`.

## Task 2: Add exact flow transaction metadata helpers

**Files:**

- Modify: `tests/test_generate_dolo_flows_integrity.py`
- Modify: `tests/test_generate_odolo_flows.py`
- Modify: `generate_dolo_flows.py`
- Modify: `generate_odolo_flows.py`

**Steps:**

1. Add RED tests for latest inbound selection, latest outbound selection, same-block log disambiguation, chain assignment, exact block timestamp, deduplicated block lookups, and fail-closed RPC errors.
2. Add a pure direction-aware latest-block selector over existing four-field transfer tuples.
3. Add exact one-block Transfer-log recovery that validates hash/address/topics before returning metadata.
4. Add cached block timestamp lookup through existing RPC endpoint rotation.
5. Add a row enrichment helper that only appends `latest_tx_hash`, `latest_tx_timestamp`, and `latest_tx_chain` when all fields are exact.
6. Prove with regression tests that original flow row fields and ordering are byte-equivalent before metadata keys are attached.
7. Run focused suites and `py_compile`.
8. Commit: `feat: enrich flow rows with exact latest tx metadata`.

## Task 3: Wire metadata into DOLO and oDOLO generators and validation

**Files:**

- Modify: `generate_dolo_flows.py`
- Modify: `generate_odolo_flows.py`
- Modify: `validate_data.py`
- Modify: `tests/test_generate_dolo_flows_integrity.py`
- Modify: `tests/test_generate_odolo_flows.py`
- Modify: `tests/test_odolo_flows_validation.py`
- Modify: `tests/test_validate_dolo_flows.py`

**Steps:**

1. Add RED output-contract tests for every period/chain accumulator and outflow collection.
2. Enrich rows after existing ranking, balance lookup, label assignment, and numeric calculations.
3. Make optional metadata validation all-or-none: exact 32-byte hash, positive exact integer timestamp, supported chain key.
4. Reject partial or malformed metadata while allowing backward-compatible old rows with no metadata.
5. Run both generator suites, validators, and `py_compile`.
6. Commit: `feat: publish aggregate flow transaction metadata`.

## Task 4: Unify DOLO/oDOLO/veDOLO Flow UX and sorting

**Files:**

- Modify: `dolo-preview.html`
- Modify: `odolo-preview.html`
- Modify: `vedolo-preview.html`
- Modify: `dolo/index.html`
- Modify: `odolo/index.html`
- Modify: `vedolo/index.html`
- Create: `tests/wallet-flow-ux.test.js`
- Modify: `tests/test_vedolo_preview_contracts.py`
- Modify: `tests/test_odolo_preview_contracts.py`

**Steps:**

1. Add RED markup/behavior contracts for shared assets, known/unknown layouts, TX explorer routing, compact rank lanes, persistent sort slots, and identical Flow classes.
2. Load shared wallet-table assets in all three preview pages with stable version keys.
3. Replace the duplicated Flow address markup with thin adapters over the shared renderer.
4. Keep veDOLO's existing event hash/timestamp; consume optional generated metadata in DOLO/oDOLO.
5. Apply the canonical Flow header/body classes to all Flow tables.
6. Replace oDOLO's remove/append sorting marker with the stable shared synchronization helper.
7. Center and narrow all Flow `#` columns and move Address closer without changing amount columns.
8. Preserve current search, filters, pagination, row hover, same-address highlighting, and default sorts.
9. Bump only the affected route cache keys.
10. Run focused Node/Python contracts, inline-script parsing, and `git diff --check`.
11. Commit: `feat: unify DOLO wallet flow tables`.

## Task 5: Align DOLO Holders rank and sort behavior

**Files:**

- Modify: `dolo-preview.html`
- Modify: `tests/holder-distribution-contract.test.js`
- Modify: `tests/test_dolo_address_labels.py`

**Steps:**

1. Add RED tests for a fixed compact rank column, centered header/body axes, persistent sort slots, and no synthetic name for an unknown holder.
2. Apply shared rank and sortable-header primitives to DOLO Holders.
3. Keep the current sort key/direction and all holder type filters.
4. Verify all related `nth-child`, colgroup, spacer-row, and mobile selectors.
5. Run focused contracts and commit: `fix: align DOLO holder rank and sorting UX`.

## Task 6: Merge Team/Investor wallets into Holder Distribution

**Files:**

- Modify: `dolo-preview.html`
- Modify: `tests/holder-distribution-contract.test.js`

**Steps:**

1. Replace existing tests that require the standalone allocation card with RED tests requiring its complete absence.
2. Add RED pure-model tests that place Team/Investor wallets into each exact balance threshold while leaving CEX, CA, potential custody/MM, and bot exclusions unchanged.
3. Remove the standalone allocation card markup, state, renderer, brush bindings, and now-unused CSS.
4. Reuse existing holder history and allocation classification to add Team/Investor balances and wallet counts to the selected main bucket model.
5. Include Team/Investor rows in bucket drilldowns with canonical badges and selected-range changes.
6. Add tooltip contribution lines without creating a separate allocation table/card.
7. Preserve the current synthetic-Now reconciliation guard.
8. Audit all removed IDs and JavaScript references.
9. Run holder contracts and commit: `feat: include allocations in holder distribution ranges`.

## Task 7: Add address-level CEX details

**Files:**

- Modify: `dolo-preview.html`
- Modify: `tests/holder-distribution-contract.test.js`

**Steps:**

1. Add RED tests for exchange disclosure rows, address-level composition, canonical names, copy, DeBank, exact range changes, keyboard toggles, and unavailable-delta fallback.
2. Build start/end CEX address unions from existing holder snapshots/history and canonical CEX types.
3. Group rows by the existing canonical exchange-name normalizer.
4. Render compact nested address rows below each exchange; do not add a second label mapping or infer new CEX identities.
5. Ensure the lazy history fetch occurs only when CEX details need address-level historical deltas.
6. Run focused tests and commit: `feat: expose wallet addresses in CEX details`.

## Task 8: Normalize remaining active wallet tables

**Files:**

- Modify: `dolo-preview.html`
- Modify: `odolo-preview.html`
- Modify: `vedolo-preview.html`
- Modify: `liquidation-preview.html`
- Modify: `revenue-preview.html`
- Modify: affected route loader cache keys
- Create: `tests/wallet-label-consumer-audit.test.js`
- Modify: `tests/test_dolo_address_labels.py`

**Steps:**

1. Add a source audit test enumerating every active wallet-address table and its canonical resolver use.
2. Add RED contracts proving unknown wallet cells omit synthetic names in all active consumers.
3. Migrate only wallet identity cells; leave token/market contract address cells untouched.
4. Preserve page-specific badges, explorer destinations, tooltip behavior, same-address highlighting, and all data values.
5. Re-run the consumer audit and affected page contract suites.
6. Commit: `fix: normalize wallet labels across dashboard tables`.

## Task 9: Browser verification and generated-data smoke

**Files:**

- Modify only if verification exposes an in-scope defect.
- Add ignored evidence under `.superpowers/sdd/2026-08-15-wallet-table-ux-consistency/`.

**Steps:**

1. Start `python3 -m http.server` from the worktree.
2. Verify DOLO, oDOLO, veDOLO, Borrow/Supply, and Revenue wallet tables at 1440×900, 1024×768, 768×1024, and 390×844.
3. Record computed typography/header values and compare Flow tables exactly.
4. Record bounding boxes for rank heading/value alignment and before/after sort activation; require zero header layout shift.
5. Verify known/unknown address hierarchy, copy, DeBank, transaction links, keyboard CEX disclosures, and same-address hover behavior.
6. Require no document-level horizontal overflow; ensure intentional table scrolling stays inside wrappers.
7. Run bounded generator-helper smoke tests with mocked RPC evidence. Do not hand-edit generated JSON.
8. Run full relevant Python/Node suites, `py_compile`, `node --check`, inline HTML script parsing, validators, and `git diff --check`.
9. Commit any verification-only in-scope fix separately.

## Task 10: Review, integration, and live deployment

**Files:**

- No new product scope.

**Steps:**

1. Review the complete diff against the approved design and confirm no unrelated changes.
2. Fetch the latest production `master`, rebase/reconcile the small code commits, and rerun the final verification gate.
3. Push the feature branch and integrate it into `master` without force-pushing production.
4. Dispatch and monitor the existing DOLO and oDOLO flow workflows so optional TX metadata is generated and validated.
5. Monitor GitHub Pages deployment and verify cache-busted live URLs.
6. Report changed files, tables missing labels before, reused primitives, TX metadata solution, verified viewports, workflow/deployment status, and explicitly confirm that data sources, flow calculations, and default sorting logic were not changed.

## Task 11: Correct selected-range holder deltas and refine Flow/CEX hierarchy

**Files:**

- Modify: `dolo-preview.html`
- Modify: `odolo-preview.html`
- Modify: `vedolo-preview.html`
- Modify: `wallet-table-ux.js`
- Modify: `wallet-table-ux.css`
- Modify: `tests/holder-distribution-contract.test.js`
- Modify: `tests/wallet-table-ux.test.js`
- Modify: `tests/wallet-flow-ux.test.js`

**Steps:**

1. Add a RED behavioral contract proving the 1 Jun–15 Aug 1M+ delta is calculated from `254,030,706.10` to `244,674,520.62`, yielding `−9,356,185.48`, not from the allocation-free reconstructed baseline.
2. Add RED renderer contracts for separate address and transaction metadata lines and the stacked CEX current/change amount block.
3. Use the exact first visible precomputed aggregate as the legend baseline while retaining the wallet-level fallback point only for Details.
4. Add a shared opt-in Flow renderer option that moves exact transaction metadata to its own third line; enable it in DOLO, oDOLO, and veDOLO Flow only.
5. Change the CEX exchange row to three columns with a stacked amount block, larger exchange-name text, semantic range-change color, and the existing address-count disclosure.
6. Run the holder/CEX data reconciliation scan, focused Node/Python suites, inline-script parsing, validator, and `git diff --check`.
7. Verify 1440×900, 1024×768, 768×1024, and 390×844 with computed styles, bounding boxes, exact visible values, keyboard disclosure behavior, and no document overflow.
8. Rebase on the latest production `master`, rerun the final gate, push `HEAD:master`, monitor Pages, and verify cache-busted public URLs.
