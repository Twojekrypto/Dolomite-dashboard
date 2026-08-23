# DOLO Flow RPC Integrity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild trustworthy DOLO Flow data for every period and prevent a single RPC from silently deleting valid Transfer logs.

**Architecture:** Fetch every transfer-log chunk from two independent RPC providers and compare a canonical digest before accepting it. Use a third provider only to resolve disagreement, keep the current cache untouched until a complete scan passes, then perform one verified backfill from each token deployment block. After the backfill, normal runs use a small verified overlap instead of re-reading 30 days from one provider.

**Tech Stack:** Python 3, JSON-RPC `eth_getLogs`, GitHub Actions/cache, static JSON validation, `unittest`.

**Spec:** `docs/superpowers/plans/2026-08-23-dolo-flow-rpc-integrity.md#observed-failure-and-acceptance-criteria`

## Global Constraints

- Do not publish a generated JSON file when any scanned chunk lacks provider quorum.
- Never replace the active transfer cache with a partial or unverified scan.
- Preserve the existing DOLO Flow definitions, exclusions, bridge neutralization, protocol attribution, sorting and UI.
- Use integer wei arithmetic and canonical `(blockNumber, transactionHash, logIndex)` event identity.
- Do not hardcode a corrected net-flow value for any wallet.
- Keep RPC usage bounded: two providers normally, third provider only on disagreement.
- Production remains GitHub Pages from `master`; verify the hosted JSON after deployment.

## Observed failure and acceptance criteria

On 2026-08-23 the live Berachain file contained 232 transfers in each of `1d`, `7d`, and `30d`. An independent Berachain RPC returned 927 events for the same 1D cutoff and 12,599 events for the same 7D cutoff. Transaction `0xcc41fb29534dc8adb1440454087a6a738fdfbeaaa2d20880d2265edbbc8997b3` transferred 21,100 DOLO to `0xb490d2a5d857c0357a8c2ac23c30ba0e6e02f909` at block 24,990,784; one provider returned the log while another returned an empty successful response.

The repair is accepted only when:

- the known transaction is present in the validated event store;
- its 21,100 DOLO inflow contributes to every period whose cutoff contains its block;
- each accepted chunk has an exact two-provider digest match or a two-of-three majority;
- unresolved disagreement makes generation fail before cache promotion or JSON commit;
- `total_transfers` is derived from the validated event store;
- a verified full backfill covers Ethereum and Berachain from their configured deployment blocks;
- hosted `dolo_flows.json` passes the same integrity checks after GitHub Pages deploys.

---

### Task 1: Canonical log identity and provider-quorum selection

**Files:**
- Modify: `generate_dolo_flows.py`
- Test: `tests/test_generate_dolo_flows_rpc.py`

**Interfaces:**
- Produces: `canonical_transfer_log(log: dict) -> tuple[str, str, int, str, str, int]`
- Produces: `transfer_log_digest(logs: list[dict]) -> str`
- Produces: `select_quorum_logs(provider_results: list[tuple[str, list[dict]]]) -> tuple[list[dict], dict]`
- Consumes: raw `eth_getLogs` results without converting wei to floating point.

- [ ] **Step 1: Add failing tests for exact agreement, empty disagreement and third-provider majority**

```python
def test_quorum_rejects_one_empty_success_against_nonempty_provider(self):
    event = make_transfer_log(tx_hash="0xcc41", log_index=35)
    with self.assertRaises(flows.TransferLogQuorumError):
        flows.select_quorum_logs([("rpc-a", []), ("rpc-b", [event])])

def test_quorum_uses_two_of_three_exact_majority(self):
    event = make_transfer_log(tx_hash="0xcc41", log_index=35)
    logs, meta = flows.select_quorum_logs([
        ("rpc-a", []), ("rpc-b", [event]), ("rpc-c", [event]),
    ])
    self.assertEqual(logs, [event])
    self.assertEqual(meta["quorum"], 2)
    self.assertEqual(meta["providerCount"], 3)
```

- [ ] **Step 2: Run the tests and confirm they fail before implementation**

Run: `python3 -m unittest tests.test_generate_dolo_flows_rpc -v`

Expected: FAIL because quorum helpers and `TransferLogQuorumError` do not exist.

- [ ] **Step 3: Implement deterministic canonicalization and SHA-256 digests**

Canonical sorting must use block number, transaction hash and log index. The digest payload must also include address, topics and data so two different logs cannot share a digest accidentally.

```python
class TransferLogQuorumError(RuntimeError):
    pass

def canonical_transfer_log(log):
    return (
        str(log["blockNumber"]).lower(),
        str(log["transactionHash"]).lower(),
        int(str(log["logIndex"]), 16),
        str(log["address"]).lower(),
        tuple(str(topic).lower() for topic in log.get("topics", [])),
        str(log.get("data", "0x0")).lower(),
    )
```

- [ ] **Step 4: Implement exact two-provider agreement and two-of-three majority**

Empty results are valid only when at least one other independent provider returns the same empty digest. Two conflicting providers without a majority must raise `TransferLogQuorumError`.

- [ ] **Step 5: Run the focused tests**

Run: `python3 -m unittest tests.test_generate_dolo_flows_rpc -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add generate_dolo_flows.py tests/test_generate_dolo_flows_rpc.py
git commit -m "fix: require RPC quorum for DOLO transfer logs"
```

---

### Task 2: Quorum-aware chunk fetching with transactional cache promotion

**Files:**
- Modify: `generate_dolo_flows.py`
- Test: `tests/test_generate_dolo_flows_rpc.py`
- Test: `tests/test_generate_dolo_flows_integrity.py`

**Interfaces:**
- Consumes: `select_quorum_logs(...)` from Task 1.
- Produces: `fetch_verified_transfer_chunk(chain_key: str, start_block: int, end_block: int) -> tuple[list[tuple], dict]`.
- Produces: `promote_verified_transfer_range(cached: list[tuple], verified: list[tuple], start_block: int, end_block: int, audit: dict) -> list[tuple]`.

- [ ] **Step 1: Add a failing test proving a bad empty RPC cannot erase cached logs**

```python
def test_unverified_replacement_never_erases_active_cache(self):
    cached = [(FROM, TO, 21_100 * 10**18, 24_990_784)]
    with self.assertRaises(flows.TransferLogQuorumError):
        flows.promote_verified_transfer_range(
            cached, [], 24_900_000, 25_000_000,
            {"status": "disputed", "quorum": 1},
        )
    self.assertEqual(cached, [(FROM, TO, 21_100 * 10**18, 24_990_784)])
```

- [ ] **Step 2: Run the test and verify the unsafe current behavior is exposed**

Run: `python3 -m unittest tests.test_generate_dolo_flows_integrity -v`

Expected: FAIL because cache promotion does not require an integrity audit.

- [ ] **Step 3: Fetch each chunk from two distinct endpoints and use the third only on mismatch**

Do not retry the same URL under a different provider name. Record endpoint labels without secrets, response digests, counts, attempts and the selected majority in audit metadata.

- [ ] **Step 4: Make range replacement transactional**

Build the complete verified replacement in memory or a staging state key. Call `replace_transfer_range(...)` only after every chunk in the requested range has `status == "verified"`. On interruption or disagreement, retain the previous active cache and fail generation.

- [ ] **Step 5: Restore a small verified reorg overlap after the full backfill**

Set normal overlap to 100,000 Berachain blocks and 50,000 Ethereum blocks. Quorum makes the overlap trustworthy, so normal runs no longer need to delete and re-fetch 30 days from one provider.

- [ ] **Step 6: Run RPC and integrity tests**

Run: `python3 -m unittest tests.test_generate_dolo_flows_rpc tests.test_generate_dolo_flows_integrity -v`

Expected: PASS, including cache preservation after disagreement.

- [ ] **Step 7: Commit**

```bash
git add generate_dolo_flows.py tests/test_generate_dolo_flows_rpc.py tests/test_generate_dolo_flows_integrity.py
git commit -m "fix: promote only verified DOLO flow ranges"
```

---

### Task 3: Integrity metadata and publication guard

**Files:**
- Modify: `generate_dolo_flows.py`
- Modify: `validate_data.py`
- Test: `tests/test_validate_dolo_flows.py`

**Interfaces:**
- Produces: `flow_history_integrity` with `status`, `schemaVersion`, `coverage`, `verifiedChunkCount`, `unresolvedChunkCount`, `lastVerifiedAt`, and per-chain aggregate digests.
- Consumes: per-chunk audit metadata from Task 2.

- [ ] **Step 1: Add failing validation fixtures**

Add cases where `status` is `disputed`, `unresolvedChunkCount > 0`, coverage begins after the configured deployment block, or the known canary block lies inside coverage but is absent.

- [ ] **Step 2: Run validation tests and confirm failure**

Run: `python3 -m unittest tests.test_validate_dolo_flows -v`

Expected: FAIL because current validation trusts the hardcoded `complete` marker.

- [ ] **Step 3: Generate integrity metadata from actual verified chunks**

Remove the unconditional:

```python
{"status": "complete", "unresolvedGapCount": 0}
```

Set `status: "complete"` only when coverage reaches the configured deployment block and every required chunk has provider quorum.

- [ ] **Step 4: Make validation fail closed**

`validate_data.py dolo_flows.json ...` must exit non-zero for disputed or incomplete history. Keep the existing workflow behavior that skips the commit after generation or validation failure.

- [ ] **Step 5: Add a temporal sanity warning**

Emit a warning when `total_transfers` is identical across `1d`, `7d`, and `30d`, but do not use equality alone as a hard failure. The hard gate remains provider quorum and verified coverage.

- [ ] **Step 6: Run validation tests**

Run: `python3 -m unittest tests.test_validate_dolo_flows -v`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add generate_dolo_flows.py validate_data.py tests/test_validate_dolo_flows.py
git commit -m "fix: fail DOLO flow publication on unverified history"
```

---

### Task 4: One-time verified full-history backfill mode

**Files:**
- Modify: `generate_dolo_flows.py`
- Modify: `.github/workflows/update-dolo-flows.yml`
- Test: `tests/test_generate_dolo_flows_integrity.py`

**Interfaces:**
- Consumes: environment variable `DOLO_FLOWS_FULL_VERIFIED_BACKFILL` with values `0` or `1`.
- Produces: verified state coverage from Ethereum block `21,500,000` and Berachain block `2,900,000` through the audited chain tips.

- [ ] **Step 1: Add failing tests for full-backfill scan boundaries**

```python
def test_full_verified_backfill_ignores_unverified_checkpoint(self):
    self.assertEqual(
        flows.transfer_scan_start("bera", full_verified_backfill=True, last_block=25_245_037),
        flows.CHAINS["bera"]["deploy_block"],
    )
```

- [ ] **Step 2: Run the test and confirm failure**

Run: `python3 -m unittest tests.test_generate_dolo_flows_integrity -v`

Expected: FAIL because full verified backfill mode does not exist.

- [ ] **Step 3: Add isolated staging state for the backfill**

Use a distinct staging key/file so `actions/cache/save` cannot promote an interrupted full scan as active history. Promote it only after complete quorum and validation.

- [ ] **Step 4: Add a manual workflow input**

Add `workflow_dispatch.inputs.full_verified_backfill` as a boolean and export it as `DOLO_FLOWS_FULL_VERIFIED_BACKFILL`. Scheduled runs keep the value `0`.

- [ ] **Step 5: Preserve progress without preserving unverified chunks**

Checkpoint only chunks whose quorum status is verified. Resume a later backfill from the last contiguous verified block; never skip a disputed range.

- [ ] **Step 6: Run generator tests and workflow syntax checks**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_flows_integrity tests.test_generate_dolo_flows_rpc -v
python3 -m py_compile generate_dolo_flows.py validate_data.py
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add generate_dolo_flows.py .github/workflows/update-dolo-flows.yml tests/test_generate_dolo_flows_integrity.py
git commit -m "feat: add verified DOLO flow history backfill"
```

---

### Task 5: Regression evidence for the known Berachain transfer

**Files:**
- Modify: `tests/test_generate_dolo_flows_integrity.py`
- Modify: `lessons.md`

**Interfaces:**
- Consumes: a deterministic fixture for block `24,990,784` and transaction `0xcc41fb29534dc8adb1440454087a6a738fdfbeaaa2d20880d2265edbbc8997b3`.
- Produces: regression coverage proving 21,100 DOLO is included in all periods whose block cutoff includes the event.

- [ ] **Step 1: Add the regression fixture with exact integer wei**

```python
KNOWN_BERA_INFLOW = (
    "0x52256ef863a713ef349ae6e97a7e8f35785145de",
    "0xb490d2a5d857c0357a8c2ac23c30ba0e6e02f909",
    21_100 * 10**18,
    24_990_784,
)
```

- [ ] **Step 2: Assert period inclusion from block cutoffs**

The event must affect `7d`, `30d`, `90d`, `180d`, and `all` whenever their computed cutoff is at or below block `24,990,784`. The test must not assert a hardcoded final wallet net when other events may exist.

- [ ] **Step 3: Document the production lesson**

Add to `lessons.md`: an HTTP-200 empty `eth_getLogs` response is not evidence of completeness; destructive overlap replacement requires independent quorum and transactional promotion.

- [ ] **Step 4: Run all DOLO flow tests**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_flows_rpc tests.test_generate_dolo_flows_integrity tests.test_validate_dolo_flows -v
node --test tests/dolo-flow-balance-display.test.js tests/dolo-flow-lp-display.test.js tests/dolo-flow-protocol-filter.test.js
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_generate_dolo_flows_integrity.py lessons.md
git commit -m "test: lock Berachain DOLO flow completeness regression"
```

---

### Task 6: Backfill, deploy and verify production

**Files:**
- Generated: `dolo_flows.json`
- Generated: `dolo_holders.json`
- Generated: `dolo_holder_wallet_history.json`
- Generated cache: `dolo_flows_state.json` in GitHub Actions cache only.

**Interfaces:**
- Consumes: verified backfill workflow from Task 4.
- Produces: published, verified DOLO Flow JSON on GitHub Pages.

- [ ] **Step 1: Push the code-only commits to `master`**

Confirm the remote SHA for every touched source file before publishing so unrelated automated commits are preserved.

- [ ] **Step 2: Run the manual full verified backfill**

Trigger `update-dolo-flows.yml` with `full_verified_backfill=true`. Monitor provider disagreements, verified chunk counts, contiguous coverage and RPC rate limiting.

- [ ] **Step 3: Verify workflow validation and commit**

Require successful generation, cache promotion, `validate_data.py`, and the generated-data commit. A failed or disputed run must leave the previous live JSON untouched.

- [ ] **Step 4: Verify production JSON with a cache-busting URL**

Check:

```bash
curl -fsS 'https://twojekrypto.github.io/Dolomite-dashboard/dolo_flows.json?verified-backfill=<timestamp>'
```

Assert that integrity is complete, unresolved chunks equal zero, coverage begins at both configured deployment blocks, and the known transaction contributes to the correct periods.

- [ ] **Step 5: Reconcile period counts and selected wallets**

Compare live `total_transfers` against the verified event store for `1d`, `7d`, `30d`, `90d`, `180d`, and `all`. Confirm counts are non-decreasing by period and inspect the previously affected `b490` and `28da` wallets.

- [ ] **Step 6: Verify the rendered dashboard**

Serve locally with `python3 -m http.server`, then verify DOLO Flows on desktop and mobile. Confirm filters, sorting and labels are unchanged; this repair is data-only.

- [ ] **Step 7: Publish the final evidence**

Report the workflow URL, generated commit SHA, live JSON timestamp, provider-quorum coverage, final period counts and the corrected known-wallet values.

---

## Self-review

- Spec coverage: root cause, quorum, cache safety, full backfill, validation, workflow, regression and live verification are each mapped to a task.
- Placeholder scan: no deferred implementation items or unspecified error handling remain.
- Type consistency: quorum metadata flows from Task 1 through transactional promotion, output integrity metadata, validation and deployment checks.
- Data logic: existing flow calculations remain unchanged; only source completeness and publication safety change.
