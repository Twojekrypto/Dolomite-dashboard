# veDOLO Epoch 9 Rebate Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover the published veDOLO borrow-fee rebate for 2026-07-16 through 2026-07-22 from Dolomite's audited epoch 9 snapshot reset, apply it exactly once to savings and net revenue, expose provenance, and keep later unpublished dates pending.

**Architecture:** Decode root-setting transactions as ordered groups and preserve the normal cumulative-delta path. A transaction-specific Berachain allowlist enables a pure, strictly validated full-reset classifier only for epoch 9 transaction `0x6d85363b5942efbaff9ed80943e4e415edc5e578a3f1e8f1b0c9207c2bec8a7c`; accepted reset totals become the epoch amounts, while unknown or mixed decreases remain non-financial metadata. Propagate the calculation mode from epoch to daily series and the chart tooltip, then regenerate the static JSON through the official fetcher.

**Tech Stack:** Python 3, `web3.py`, `Decimal`, `unittest`, static JSON, vanilla JavaScript/SVG chart UI, GitHub Pages.

## Global Constraints

- The exception is valid only on chain ID `80094`, expected epoch `9`, block `24055329`, and the exact audited transaction hash.
- The transaction must increment the rolling-claims epoch and reset every previously non-zero market to a smaller positive total.
- At least two previously non-zero markets must participate; zero totals, duplicate markets, incomplete resets, and mixed increases/decreases are rejected.
- Accepted `rebateRaw` equals the newly published `totalAmount`; normal transactions continue using `totalAmount - previousTotal`.
- Unknown corrections never create positive rebate, user-savings, or revenue-netting values.
- The recovered rebate is priced using the existing historical price resolver and allocated exactly once by the existing daily Berachain borrow-interest weights.
- Dates from 2026-07-30 onward remain pending until Dolomite publishes another closed epoch.
- No new dependency or runtime GitHub-history fetch is added.

---

### Task 1: Add a pure, strict known-reset classifier

**Files:**
- Modify: `tests/test_fetch_dolomite_revenue.py:1-25,89-180`
- Modify: `fetch_dolomite_revenue.py:45-75,257-291`

**Interfaces:**
- Consumes: chain ID string, normalized transaction hash, decoded transaction context, decoded root events, and pre-transaction market totals.
- Produces: `classify_known_rebate_snapshot_reset(chain_id, tx_hash, context, events, previous_totals) -> dict | None` and `fee_rebate_transaction_context_from_input(w3, transaction_input) -> dict | None`.

- [ ] **Step 1: Write failing classifier tests**

Import the new classifier and add fixture helpers using integer raw totals. The accepted case must assert this exact structure:

```python
result = classify_known_rebate_snapshot_reset(
    "80094",
    "0x6d85363b5942efbaff9ed80943e4e415edc5e578a3f1e8f1b0c9207c2bec8a7c",
    {"expectedEpoch": 9, "incrementEpoch": True},
    [
        {"marketId": 1, "totalRaw": 40, "blockNumber": 24055329},
        {"marketId": 2, "totalRaw": 15, "blockNumber": 24055329},
    ],
    {1: 140, 2: 85},
)
self.assertEqual(result["calculationMode"], "known_epoch_snapshot_reset")
self.assertEqual(result["rebateRawByMarket"], {1: 40, 2: 15})
self.assertEqual(result["resetMarketCount"], 2)
self.assertEqual(result["aggregateAdjustmentRaw"], -170)
```

Add separate assertions that the function returns `None` for an unknown hash, wrong chain, wrong epoch, `incrementEpoch=False`, block mismatch, a missing prior market, duplicate market IDs, a zero new total, and a mixed `{1: 40, 2: 95}` transaction.

- [ ] **Step 2: Run the classifier tests and verify RED**

Run: `python3 -m unittest tests.test_fetch_dolomite_revenue.FetchDolomiteRevenueTest.test_known_epoch_snapshot_reset_requires_full_audited_transaction`

Expected: ERROR because `classify_known_rebate_snapshot_reset` does not exist.

- [ ] **Step 3: Add exact allowlist and transaction-context decoding**

Add this immutable configuration near the existing fee-rebate constants:

```python
KNOWN_FEE_REBATE_SNAPSHOT_RESETS = {
    (BERACHAIN_CHAIN_ID, "0x6d85363b5942efbaff9ed80943e4e415edc5e578a3f1e8f1b0c9207c2bec8a7c"): {
        "epoch": 9,
        "blockNumber": 24_055_329,
        "calculationMode": "known_epoch_snapshot_reset",
        "sourceLabel": "Published epoch snapshot reset",
    },
}
```

Decode both `_expectedEpoch` and `_incrementEpoch` while preserving the existing epoch helper:

```python
def fee_rebate_transaction_context_from_input(w3, transaction_input):
    if not transaction_input:
        return None
    try:
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(FEE_REBATE_ROLLING_CLAIMS_ADDRESS),
            abi=FEE_REBATE_ROLLING_CLAIMS_ABI,
        )
        _function, args = contract.decode_function_input(transaction_input)
        epoch = int(args.get("_expectedEpoch") or 0)
        if epoch <= 0:
            return None
        return {
            "expectedEpoch": epoch,
            "incrementEpoch": bool(args.get("_incrementEpoch")),
        }
    except Exception:
        return None


def fee_rebate_epoch_from_transaction_input(w3, transaction_input):
    context = fee_rebate_transaction_context_from_input(w3, transaction_input)
    return context.get("expectedEpoch") if context else None
```

- [ ] **Step 4: Implement the strict pure classifier**

The classifier must normalize the hash to lowercase, require exact event/prior market-set equality, reject duplicates, and return:

```python
return {
    "calculationMode": spec["calculationMode"],
    "sourceLabel": spec["sourceLabel"],
    "rebateRawByMarket": {event["marketId"]: event["totalRaw"] for event in events},
    "resetMarketCount": len(events),
    "aggregateAdjustmentRaw": sum(
        event["totalRaw"] - int(previous_totals[event["marketId"]])
        for event in events
    ),
}
```

Return `None` unless every event block matches the allowlist, every previous value is positive, and `0 < totalRaw < previousRaw` for every market.

- [ ] **Step 5: Run focused and full Python tests**

Run:

```bash
python3 -m unittest tests.test_fetch_dolomite_revenue.FetchDolomiteRevenueTest.test_known_epoch_snapshot_reset_requires_full_audited_transaction
python3 -m unittest tests.test_fetch_dolomite_revenue
```

Expected: both commands exit 0.

- [ ] **Step 6: Commit the classifier**

```bash
git add fetch_dolomite_revenue.py tests/test_fetch_dolomite_revenue.py
git commit -m "feat: classify audited veDOLO epoch resets"
```

---

### Task 2: Integrate transaction-group recovery into the event parser

**Files:**
- Modify: `tests/test_fetch_dolomite_revenue.py:180-430`
- Modify: `fetch_dolomite_revenue.py:313-446`

**Interfaces:**
- Consumes: `classify_known_rebate_snapshot_reset(...)` and existing pricing/token metadata functions.
- Produces: epoch rows with optional `calculationMode`, `sourceLabel`, `transactionHash`, `resetMarketCount`, and `aggregateAdjustmentRaw`; chain metadata with `unsupportedCorrections`.

- [ ] **Step 1: Write a failing transaction-processing test**

Use mocks for RPC logs, transaction input, token metadata, and historical prices. Provide cumulative epoch 8 totals followed by the exact known epoch 9 reset transaction. Assert:

```python
epoch9 = next(row for row in result["chains"]["Berachain"]["epochRebates"] if row["epoch"] == 9)
self.assertGreater(epoch9["rebateUSD"], 0)
self.assertEqual(epoch9["calculationMode"], "known_epoch_snapshot_reset")
self.assertEqual(epoch9["sourceLabel"], "Published epoch snapshot reset")
self.assertEqual(epoch9["transactionHash"], KNOWN_EPOCH9_TX)
self.assertEqual(epoch9["resetMarketCount"], 2)
self.assertTrue(all(float(market["amount"]) > 0 for market in epoch9["markets"]))
```

Add one unknown-reset fixture and assert it is absent from `epochRebates`, present in `unsupportedCorrections`, and does not change `totalRebateUSD`.

- [ ] **Step 2: Run the parser tests and verify RED**

Run: `python3 -m unittest tests.test_fetch_dolomite_revenue.FetchDolomiteRevenueTest.test_fetch_rebate_data_recovers_only_known_epoch9_reset`

Expected: FAIL because the existing parser skips every non-positive delta.

- [ ] **Step 3: Process logs as ordered transaction groups**

Group adjacent sorted logs by normalized transaction hash. Decode all `(marketId, totalRaw, blockNumber)` entries before mutating `market_totals`, fetch one transaction context, and call the strict classifier against a snapshot of prior totals.

For accepted resets use:

```python
effective_raw = reset_result["rebateRawByMarket"][market_id]
calculation_mode = reset_result["calculationMode"]
```

For all other entries retain:

```python
effective_raw = total_raw - previous_raw
calculation_mode = "cumulative_delta"
if effective_raw <= 0:
    continue
```

Update every market total after classifying the complete transaction so one event cannot change another event's validation baseline.

- [ ] **Step 4: Record unsupported corrections without applying money**

When a non-allowlisted transaction contains any `totalRaw <= previousRaw`, append one deduplicated metadata row:

```python
{
    "transactionHash": tx_hash,
    "eventBlock": max(event["blockNumber"] for event in decoded_events),
    "marketCount": len(decoded_events),
    "reason": "unsupported_aggregate_correction",
}
```

Do not add its values to `epoch_rebates`, `pricedEventCount`, or `totalRebateUSD`.

- [ ] **Step 5: Attach provenance to accepted epoch and market rows**

For epoch 9 set `calculationMode`, `sourceLabel`, `transactionHash`, `resetMarketCount`, and `aggregateAdjustmentRaw` from the classifier. Each recovered market row must include `calculationMode`, `transactionHash`, `previousTotalRaw`, `publishedTotalRaw`, and a positive human-unit `amount` derived from `effective_raw`.

- [ ] **Step 6: Run parser and complete revenue tests**

Run:

```bash
python3 -m unittest tests.test_fetch_dolomite_revenue.FetchDolomiteRevenueTest.test_fetch_rebate_data_recovers_only_known_epoch9_reset
python3 -m unittest tests.test_fetch_dolomite_revenue
python3 -m py_compile fetch_dolomite_revenue.py
```

Expected: every command exits 0.

- [ ] **Step 7: Commit parser integration**

```bash
git add fetch_dolomite_revenue.py tests/test_fetch_dolomite_revenue.py
git commit -m "fix: recover published veDOLO epoch 9 rebates"
```

---

### Task 3: Propagate provenance and enforce validator contracts

**Files:**
- Modify: `tests/test_fetch_dolomite_revenue.py:430-860`
- Modify: `fetch_dolomite_revenue.py:508-655,760-815`
- Modify: `validate_data.py:733-770`
- Modify: `revenue-preview.html:3740-3780`

**Interfaces:**
- Consumes: epoch-row `calculationMode` and `sourceLabel`.
- Produces: daily `borrowFeeRebateCalculationMode`, strict JSON validation, and a reset-source tooltip row.

- [ ] **Step 1: Write failing accounting and UI contract tests**

Add an epoch 9 fixture with `rebateUSD: 70`, a seven-day window, and `calculationMode: "known_epoch_snapshot_reset"`. Assert that the seven daily rebates sum to exactly 70 within six decimal places, daily net revenue equals gross minus rebate, all revenue total/window validators pass, and each affected row exposes the calculation mode. Assert the HTML contains:

```javascript
const rebateSourceRow = !pending && row.borrowFeeRebateCalculationMode === "known_epoch_snapshot_reset"
  ? `<div class="tt-row"><span>Source</span><b>Published epoch snapshot reset</b></div>`
  : "";
```

Also assert the validator rejects a positive epoch row using `known_epoch_snapshot_reset` with the wrong transaction hash or epoch.

- [ ] **Step 2: Run the new contract tests and verify RED**

Run:

```bash
python3 -m unittest tests.test_fetch_dolomite_revenue.FetchDolomiteRevenueTest.test_known_reset_is_allocated_and_netted_exactly_once
python3 -m unittest tests.test_fetch_dolomite_revenue.FetchDolomiteRevenueTest.test_revenue_page_exposes_known_reset_provenance
```

Expected: FAIL because daily provenance and tooltip copy are absent.

- [ ] **Step 3: Propagate the mode during allocation**

Inside `apply_epoch_rebate_to_chain`, when the epoch mode is non-empty, assign it to each affected chain payload:

```python
calculation_mode = str(epoch_rebate.get("calculationMode") or "").strip()
if calculation_mode:
    payload["borrowFeeRebateCalculationMode"] = calculation_mode
```

When recomputing the row from chains, copy the single non-empty chain mode to `row["borrowFeeRebateCalculationMode"]`; omit the field when no chain supplies a mode.

- [ ] **Step 4: Preserve reset provenance across refresh fallback**

Extend `PRESERVED_BORROW_FEE_REBATE_EPOCH_FIELDS` with:

```python
"calculationMode",
"sourceLabel",
"transactionHash",
"resetMarketCount",
"aggregateAdjustmentRaw",
```

The current row remains authoritative when it already supplies one of these fields.

- [ ] **Step 5: Add strict validator rules**

Extend `_dolomite_revenue_borrow_fee_rebate_max_audits_valid` so an epoch row with `calculationMode == "known_epoch_snapshot_reset"` is valid only when all of these exact predicates hold:

```python
row.get("epoch") == 9
row.get("transactionHash", "").lower() == "0x6d85363b5942efbaff9ed80943e4e415edc5e578a3f1e8f1b0c9207c2bec8a7c"
int(row.get("eventBlock") or 0) == 24055329
int(row.get("resetMarketCount") or 0) >= 2
_safe_number(row.get("aggregateAdjustmentRaw")) < 0
row.get("sourceLabel") == "Published epoch snapshot reset"
```

Reject any other non-empty calculation mode except `cumulative_delta`. Require every `unsupportedCorrections` item to have `reason == "unsupported_aggregate_correction"` and never derive financial totals from it.

- [ ] **Step 6: Add source provenance to both chart modes**

Compute `rebateSourceRow` once beside `dailyRows` and append `${rebateSourceRow}` to both non-pending daily and cumulative tooltip templates. Pending tooltips must not show the reset source.

- [ ] **Step 7: Run focused and complete contract checks**

Run:

```bash
python3 -m unittest tests.test_fetch_dolomite_revenue
python3 -m py_compile fetch_dolomite_revenue.py validate_data.py
git diff --check
```

Expected: every command exits 0.

- [ ] **Step 8: Commit provenance and validation**

```bash
git add fetch_dolomite_revenue.py validate_data.py revenue-preview.html tests/test_fetch_dolomite_revenue.py
git commit -m "feat: expose audited rebate reset provenance"
```

---

### Task 4: Regenerate and reconcile production revenue data

**Files:**
- Modify: `dolomite_revenue.json`
- Verify: `fetch_dolomite_revenue.py`
- Verify: `validate_data.py`

**Interfaces:**
- Consumes: official DeFiLlama revenue data, Berachain rolling-claims logs, Dolomite metadata, and historical token prices.
- Produces: a static `dolomite_revenue.json` with epochs 1 through 10 including recovered epoch 9 and pending dates after the latest published boundary.

- [ ] **Step 1: Run the production fetcher**

Run: `python3 fetch_dolomite_revenue.py`

Expected: exits 0, prints a positive total borrow-fee rebate, and writes `dolomite_revenue.json` without direct manual number edits.

- [ ] **Step 2: Run the production validator**

Run: `python3 validate_data.py dolomite_revenue.json`

Expected: exits 0 with the revenue series, totals, chain windows, max-rebate audits, and known-reset provenance accepted.

- [ ] **Step 3: Reconcile epoch 9 and daily allocation exactly**

Run a read-only Python assertion that verifies:

```python
epoch9 = next(row for row in data["borrowFeeRebates"]["chains"]["Berachain"]["epochRebates"] if row["epoch"] == 9)
assert epoch9["calculationMode"] == "known_epoch_snapshot_reset"
assert epoch9["transactionHash"].lower() == "0x6d85363b5942efbaff9ed80943e4e415edc5e578a3f1e8f1b0c9207c2bec8a7c"
affected = [row for row in data["series"] if 1784160000 <= row["timestamp"] < 1784764800]
assert len(affected) == 7
assert abs(sum(row["borrowFeeRebateUSD"] for row in affected) - epoch9["rebateUSD"]) < 1e-5
assert all(row["borrowFeeRebateUSD"] > 0 for row in affected)
assert all(abs(row["grossRevenueUSD"] - row["revenueUSD"] - row["borrowFeeRebateUSD"]) < 1 for row in affected)
```

Also assert rows dated 2026-07-30 and later have no fabricated positive epoch allocation and the JSON `latestRebateDate` remains the true published boundary.

- [ ] **Step 4: Review generated scope**

Run: `git diff --stat && git diff --numstat -- dolomite_revenue.json`

Expected: only the revenue values and metadata caused by the current official refresh and epoch 9 recovery change; no unrelated generated file is touched.

- [ ] **Step 5: Commit generated data**

```bash
git add dolomite_revenue.json
git commit -m "data: publish recovered veDOLO epoch 9 rebates"
```

---

### Task 5: Browser verification and production handoff

**Files:**
- Modify: `revenue-preview.html:1417`
- Verify: `dolomite_revenue.json`
- Verify: `revenue-preview.html`

**Interfaces:**
- Consumes: regenerated production JSON.
- Produces: cache key `revenue-20260810-epoch9-reset` and browser-verified chart behavior.

- [ ] **Step 1: Advance the revenue data cache key**

Change only:

```javascript
const DATA_URL = "dolomite_revenue.json?v=revenue-20260810-epoch9-reset";
```

- [ ] **Step 2: Serve and inspect the Revenue page**

Run: `python3 -m http.server 8765`

Inspect `revenue-preview.html` at 1440×900, 1024×768, and 390×844. In daily and cumulative modes hover 2026-07-16 and 2026-07-22, then hover a date on or after 2026-07-30.

Expected: recovered dates show positive published savings and `Source · Published epoch snapshot reset`; cumulative totals rise once; the later date remains striped and says `Pending rebate data`; no chart, brush, tooltip, or panel overflows its viewport.

- [ ] **Step 3: Run final local verification**

Run:

```bash
node --test tests/address-match-highlighting.test.js
python3 -m unittest tests.test_fetch_dolomite_revenue
python3 -m py_compile fetch_dolomite_revenue.py validate_data.py
python3 validate_data.py dolomite_revenue.json
git diff --check
git status --short
```

Expected: all commands exit 0 and status contains only the intended revenue cache-key change before its commit.

- [ ] **Step 4: Commit the cache key**

```bash
git add revenue-preview.html
git commit -m "chore: refresh audited revenue dataset cache"
```

- [ ] **Step 5: Publish through `master`**

Push the feature branch, open a pull request targeting `master`, wait for required checks, merge only after they pass, wait for the GitHub Pages workflow, and verify the deployed page with `?verify=20260810-epoch9-reset` so no stale browser cache is reused.
