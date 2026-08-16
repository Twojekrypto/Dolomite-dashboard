# veDOLO Position Activity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct veDOLO lock classification and historical replay, then add a dedicated, responsive position-activity table for transfers, merges, splits, and lock extensions.

**Architecture:** Enrich type-4/type-5 Deposit rows in the Python generator with calldata-derived source/target token IDs. Put deterministic event classification, activity-row construction, and per-token daily replay in small CommonJS/browser helpers consumed by `vedolo-preview.html` and mirrored by the semantic Python validator.

**Tech Stack:** Python 3 standard library and existing RPC client; static HTML/CSS/JavaScript; Node assertion tests; Python `unittest`; Playwright/browser QA.

## Global Constraints

- Preserve the existing Graphite + Gold visual identity and reuse wallet/table UX helpers.
- Do not change blockchain sources, token values, unrelated metrics, or existing default sorting.
- Types `0/1/2` are true locked-DOLO inflows; types `3/4/5` and ERC-721 transfers are position activity.
- Merge, split, extend, and transfer must never add to Locked DOLO.
- Mobile is a primary surface; the page must not gain document-level horizontal overflow.

---

### Task 1: Canonical event classification and activity rows

**Files:**
- Create: `vedolo-position-activity.js`
- Create: `tests/vedolo_position_activity.test.js`
- Modify: `vedolo-preview.html`

**Interfaces:**
- Produces: `VeDoloPositionActivity.depositKind(row)`, `isExternalLock(row)`, and `buildActivityRows(locks, transfers)`.
- Consumes: existing lock rows with `depositType`, transfer rows, wallet/transaction metadata renderers.

- [ ] Write literal Node fixtures proving types `0/1/2` are locks, types `3/4/5` are not, type `4` is Merge, type `5` is Split, and wallet transfers become Transfer rows.
- [ ] Run `node tests/vedolo_position_activity.test.js` and confirm the helper/module failure.
- [ ] Implement the minimal pure helper and rerun the Node test to green.
- [ ] Add the new script to `vedolo-preview.html`, filter `Recent DOLO Locks` through `isExternalLock`, and derive lock badges through the canonical route helper.
- [ ] Rerun the Node test and `python3 -m unittest tests.test_vedolo_preview_contracts`.

### Task 2: Exact merge/split token transitions

**Files:**
- Modify: `generate_vedolo_flows.py`
- Modify: `tests/test_generate_vedolo_flows.py`
- Modify: `vedolo_flows.json`

**Interfaces:**
- Produces: `parse_position_action_calldata(lock, input_data)` and lock fields `sourceTokenId`, `targetTokenId` for deposit types `4/5`.
- Consumes: `eth_getTransactionByHash` through the existing RPC path and state cache `position_action_tokens`.

- [ ] Add Python tests with hand-encoded merge and split calldata, including wrong target, malformed input, and unresolved annotation cases.
- [ ] Run the focused tests and confirm RED failures for the missing parser/annotation behavior.
- [ ] Implement strict calldata parsing, cached RPC annotation, and fail-closed generation.
- [ ] Run the focused tests and the full `tests.test_generate_vedolo_flows` module.
- [ ] Run the official generator using the existing verified local incremental cache, then verify every type-4/type-5 row has valid unequal source/target IDs.

### Task 3: Correct Locked DOLO replay and validator

**Files:**
- Modify: `vedolo-locked-history.js`
- Modify: `tests/vedolo_locked_history.test.js`
- Modify: `validate_vedolo_locked_history.py`
- Modify: `tests/test_validate_vedolo_locked_history.py`
- Modify: `vedolo-preview.html`

**Interfaces:**
- Produces: `VeDoloLockedHistory.buildActiveLockedHistory(locks, unlocks, snapshotSec, currentLocked)`.
- Consumes: annotated lock rows and existing holder-snapshot active total.

- [ ] Add RED fixtures proving create/increase add principal, extend adds zero, merge/split conserve principal, merge moves the end date, transfer is irrelevant, and early withdrawal removes active principal.
- [ ] Implement chronological per-token replay and end-of-day points in JavaScript.
- [ ] Port the same state transitions to Python validation and require complete merge/split annotations.
- [ ] Replace the inline chart reconstruction with the shared helper while preserving the holder endpoint.
- [ ] Run Node, validator-unit, semantic-validator, and preview-contract tests to green.

### Task 4: Position Activity card and responsive UX

**Files:**
- Modify: `vedolo-preview.html`
- Modify: `vedolo/index.html`
- Modify: `tests/test_vedolo_preview_contracts.py`

**Interfaces:**
- Consumes: `buildActivityRows`, canonical wallet renderer, shared sort header and pagination helpers.
- Produces: `#position-activity-card`, action segmented control, six-row paginated table, and route cache key.

- [ ] Add RED preview contracts for the new card, five filters, six-row pagination, canonical wallet cells, neutral moved-DOLO formatting, accessible tooltips, and route cache keys.
- [ ] Implement the smallest full-width card below the existing flow grid using existing table tokens and no decorative duplication.
- [ ] Add filter, pagination, sorting, copy, hover-match, and keyboard wiring.
- [ ] Run preview contracts and inline-script syntax parsing to green.

### Task 5: Verification, documentation, and live deployment

**Files:**
- Modify: `lessons.md`
- Modify: `.github/workflows/update-vedolo-flows.yml` only if validation order needs adjustment.

**Interfaces:**
- Produces: durable production guardrails and verified live deployment.

- [ ] Record that `Deposit` is not synonymous with new locked DOLO and document types `0–5`.
- [ ] Run focused Python/Node suites, `py_compile`, `node --check`, semantic validators, inline-script parsing, YAML parsing, and `git diff --check`.
- [ ] Serve with `python3 -m http.server` and verify 1440×900, 1024×768, 768×800, and 390×844: card rendering, every filter, transaction link, no console errors, no document overflow, and stable chart interaction.
- [ ] Request a scoped code review and resolve all Important findings.
- [ ] Commit only the scoped files, reconcile with the latest remote `master`, rerun the final gate, push to `master`, and verify the successful Pages deployment plus the live route.

