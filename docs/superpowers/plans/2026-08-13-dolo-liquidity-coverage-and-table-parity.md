# DOLO Liquidity Coverage and Table Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Attribute material Kodiak Island and non-canonical Uniswap v4 vault liquidity without guessing owners, expose honest per-pool coverage, and align the DOLO Liquidity Providers table with the approved dashboard UX.

**Architecture:** Extend the existing exact-integer liquidity pipeline with reusable ERC-20 share replay and allocation, then wire it into the Kodiak and Uniswap v4 live builders. The generated artifact carries explicit verified-versus-custody coverage, while the static DOLO page renders the same rows using existing Graphite + Gold table components and Fresh-wallet summary hierarchy.

**Tech Stack:** Python 3 standard library, `requests`, `eth_abi`, existing RPC/Routescan helpers, static HTML/CSS/JavaScript, Node `node:test`, Python `unittest`, Playwright browser verification.

## Global Constraints

- Never infer a beneficial owner without an exact on-chain ownership or staking path.
- Never count an underlying vault position and its allocated share rows at the same time.
- Use exact integer arithmetic for raw token/share allocation; do not use `parseFloat` or floating-point wei math.
- Keep unresolved custody visible with `quality: "unavailable"` and `positionStatus: "custodied_unresolved"`.
- Coverage is diagnostic and must never be used to fabricate a balancing wallet row.
- Preserve the existing ten-row stable table viewport and Graphite + Gold visual identity.
- The production site is served from `master`; deployment is incomplete until the GitHub Pages workflow succeeds and the live page is checked.

---

### Task 1: Production Kodiak Island and farm attribution

**Files:**
- Modify: `generate_dolo_liquidity.py`
- Test: `tests/test_generate_dolo_liquidity.py`

**Interfaces:**
- Consumes: existing `discover_kodiak_islands(logs, pool_tokens, dolo_address)`, `allocate_kodiak_island_position(underlying_position, island_state, farm_states, contract_addresses=...)`, `_routescan_logs(...)`, `_eth_call(...)`, `_contract_owners(...)`.
- Produces: `replay_erc20_share_balances(logs: list[dict], total_supply: int) -> dict[str, int]`, `replay_kodiak_farm_balances(logs: list[dict], total_locked: int) -> dict[str, int]`, `_kodiak_island_state(...) -> dict`, `_kodiak_farms_for_island(...) -> list[dict]`, and an Island-aware `_build_kodiak_v3_live_source(...)`.

- [ ] **Step 1: Write failing pure replay tests**

Add tests which mint, transfer and burn Island shares and assert exact final balances and `totalSupply`; add StakeLocked/WithdrawLocked fixtures and assert per-user balances reconcile to `totalLiquidityLocked`. Include negative cases for an over-withdrawal and a supply mismatch.

```python
def test_erc20_share_replay_reconciles_mints_transfers_and_burns(self):
    balances = liquidity.replay_erc20_share_balances(
        [transfer(ZERO, ALICE, 100), transfer(ALICE, BOB, 40), transfer(BOB, ZERO, 10)],
        90,
    )
    self.assertEqual(balances, {ALICE: 60, BOB: 30})

def test_kodiak_farm_replay_reconciles_locked_liquidity(self):
    balances = liquidity.replay_kodiak_farm_balances(
        [stake(ALICE, 70), stake(BOB, 30), withdraw(ALICE, 20)],
        80,
    )
    self.assertEqual(balances, {ALICE: 50, BOB: 30})
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `python3 -m unittest tests.test_generate_dolo_liquidity.BeneficialOwnerTests`

Expected: errors naming missing `replay_erc20_share_balances` and `replay_kodiak_farm_balances`.

- [ ] **Step 3: Implement exact replay helpers**

Decode only canonical event topics, normalize every address, reject negative intermediate balances, remove zero balances, and require the final sum to equal the supplied on-chain total. Farm replay accepts only `StakeLocked` and `WithdrawLocked` and rejects a withdrawal exceeding the user's proven stake.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `python3 -m unittest tests.test_generate_dolo_liquidity.BeneficialOwnerTests`

Expected: all BeneficialOwner tests pass.

- [ ] **Step 5: Write a failing production-builder integration test**

Patch network helpers with deterministic Island/farm fixtures. Assert the builder:

```python
self.assertNotIn(island_owned_nft_id, {row["id"] for row in result["activePositions"]})
self.assertEqual(sum(int(row["doloRaw"]) for row in island_rows), island_dolo)
self.assertEqual(sum(int(row["pairedRaw"]) for row in island_rows), island_paired)
self.assertEqual(by_owner[ALICE]["attributionPath"], "kodiak_island")
self.assertEqual(by_owner[BOB]["attributionPath"], "kodiak_island_farm")
self.assertEqual(unresolved["positionStatus"], "custodied_unresolved")
self.assertEqual(unresolved["quality"], "unavailable")
```

- [ ] **Step 6: Run the integration test and verify RED**

Run the exact new unittest by dotted name.

Expected: the current builder returns only direct NFT rows and contains no Island-share rows.

- [ ] **Step 7: Wire Island and farm discovery into the live builder**

Discover `IslandCreated` events from `kodiakIslandFactory`; validate `pool()`, `token0()` and `token1()`; read `getUnderlyingBalances()`, `lowerTick()`, `upperTick()` and `totalSupply()`; reconstruct holder shares from all Island `Transfer` events. Discover farms from `kodiakFarmFactory`, keep only exact `stakingToken() == island`, replay `StakeLocked`/`WithdrawLocked`, and pass supported farm state into allocation. Allocate a synthetic exact underlying row per Island and suppress direct rows whose proven custodian is that Island.

- [ ] **Step 8: Verify Kodiak GREEN and full Python regression**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_liquidity.BeneficialOwnerTests
python3 -m unittest tests.test_generate_dolo_liquidity
python3 -m py_compile generate_dolo_liquidity.py
```

Expected: all commands exit 0.

- [ ] **Step 9: Commit Task 1**

```bash
git add generate_dolo_liquidity.py tests/test_generate_dolo_liquidity.py
git commit -m "Fix Kodiak Island liquidity attribution"
```

---

### Task 2: Non-canonical Uniswap v4 vaults and coverage contract

**Files:**
- Modify: `generate_dolo_liquidity.py`
- Modify: `validate_data.py`
- Test: `tests/test_generate_dolo_liquidity.py`
- Test: `tests/test_validate_dolo_liquidity.py`

**Interfaces:**
- Consumes: Task 1 exact share replay/allocation pattern and current `_build_uniswap_v4_live_source(...)`.
- Produces: non-canonical sender discovery, exact fungible-vault allocation rows, `pool["coverage"]`, and strict coverage validation.

- [ ] **Step 1: Write failing non-canonical sender tests**

Create a mixed `ModifyLiquidity` fixture containing the canonical Position Manager and a share vault sender. Assert discovery retains both senders and that the vault path uses `poolKey()`, `getTotalAmounts()`, `totalSupply()` and share `Transfer` evidence. Assert an unknown sender remains a single unresolved custody row rather than disappearing or becoming Verified.

- [ ] **Step 2: Verify RED**

Run the two new tests by dotted unittest names.

Expected: the current query filters to the canonical sender and omits the vault.

- [ ] **Step 3: Implement bounded v4 sender classification**

Query pool `ModifyLiquidity` logs by pool ID only, group active position keys by sender, keep canonical NFT handling unchanged, and classify non-canonical contracts by exact callable interface. For the verified multi-position vault interface, validate `poolKey`, read `getTotalAmounts` and ERC-20 share state, allocate current totals to share holders, and mark unresolved contract holders as custody. For unsupported managers, emit an aggregate custody row with the exact manager address and explicit reason.

- [ ] **Step 4: Verify v4 GREEN**

Run the focused non-canonical tests and the complete `tests.test_generate_dolo_liquidity` suite.

Expected: all pass and canonical row results are unchanged.

- [ ] **Step 5: Write failing coverage assembly and validator tests**

Create a pool with `$100` pool liquidity, `$75` verified positions and `$10` unresolved custody. Assert:

```python
self.assertEqual(pool["coverage"], {
    "attributedValueUsd": 85.0,
    "verifiedWalletValueUsd": 75.0,
    "unresolvedCustodyValueUsd": 10.0,
    "coveragePct": 85.0,
    "residualValueUsd": 15.0,
    "status": "partial",
})
```

Add validator failures for a wrong coverage percentage, wrong custody subtotal, negative residual and a missing residual reason on material partial coverage.

- [ ] **Step 6: Verify coverage RED**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_liquidity.ArtifactAssemblyTests
python3 -m unittest tests.test_validate_dolo_liquidity.DoloLiquidityValidatorTests
```

Expected: failures because coverage is not yet assembled or validated.

- [ ] **Step 7: Implement and validate per-pool coverage**

After valuation, group active rows by `poolId`; compute Decimal subtotals for attributed, verified-owner and unresolved-custody value. Compare to finite `pool.liquidityUsd`, quantize monetary fields to six decimals and percentage to four decimals, clamp only rounding dust within the existing tolerance, and record `status` plus an evidence-based residual reason. Extend `_dolo_liquidity_valid` to recompute every subtotal and percentage from rows.

- [ ] **Step 8: Verify coverage GREEN and regression**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_liquidity
python3 -m unittest tests.test_validate_dolo_liquidity
python3 -m py_compile generate_dolo_liquidity.py validate_data.py
```

Expected: all pass.

- [ ] **Step 9: Commit Task 2**

```bash
git add generate_dolo_liquidity.py validate_data.py tests/test_generate_dolo_liquidity.py tests/test_validate_dolo_liquidity.py
git commit -m "Audit DOLO liquidity coverage"
```

---

### Task 3: DOLO Liquidity Providers table parity

**Files:**
- Modify: `dolo-preview.html`
- Modify: `index.html`
- Modify: `dolo/index.html`
- Test: `tests/dolo-liquidity-ui.test.js`

**Interfaces:**
- Consumes: Task 2 artifact rows and coverage metadata.
- Produces: Chain clear control, Fresh-style summary rail, left/right column grouping, blue Value, readable price bounds and custody labels.

- [ ] **Step 1: Write failing UI contracts**

Add Node tests asserting:

```js
assert.match(html, /data-dolo-lp-clear="chain"/);
assert.match(html, /dolo-lp-summary fresh-wallet-stats selected-market-rail/);
assert.match(html, /class="num dolo-lp-value"/);
assert.match(html, /\.dolo-lp-value\{color:#9ab7c2\}/);
assert.equal(formatRangeBound('0.000999'), '0.000999');
assert.doesNotMatch(formatRangeBound('0.000999'), /e/i);
```

Also assert first-four headers have no numeric class, the next three have `num`, Details is centered, and the custody label/copy exists.

- [ ] **Step 2: Run Node tests and verify RED**

Run: `node --test tests/dolo-liquidity-ui.test.js`

Expected: failures for missing Chain clear, summary hierarchy, Value class and fixed-decimal bound presentation.

- [ ] **Step 3: Implement the approved table UX**

Reuse Fresh-wallet summary classes and icons, adding only LP-specific sublabels. Add the independent Chain `dd-clear`, share the existing clear handler, and restore `All chains` icon/label/state before rerendering dependent filters. Add explicit column alignment classes and fixed widths. Apply `#9ab7c2` only to finite Value text. Replace scientific compact bound formatting with readable fixed decimals for ordinary bounds and one shared unit below the Lower/Upper pair. Render `Kodiak Island custody` or the exact manager name above the muted contract address when `beneficialOwner` is absent.

- [ ] **Step 4: Advance entry-point and data cache keys**

Use one new cache token in `index.html`, `dolo/index.html` and the `data/dolo-liquidity.json` fetch URL so GitHub Pages does not serve the old markup or artifact.

- [ ] **Step 5: Verify Node GREEN and syntax**

Run:

```bash
node --test tests/dolo-liquidity-ui.test.js
node --test tests/dolo-liquidity-workflow.test.js
node --check shared-hover-tooltips.js
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 6: Commit Task 3**

```bash
git add dolo-preview.html index.html dolo/index.html tests/dolo-liquidity-ui.test.js
git commit -m "Polish DOLO liquidity provider table"
```

---

### Task 4: Generate, validate and deploy audited live data

**Files:**
- Modify: `data/dolo-liquidity.json` (generated)

**Interfaces:**
- Consumes: Tasks 1–3 production generator and UI.
- Produces: current audited JSON and verified live deployment.

- [ ] **Step 1: Generate one bounded official artifact**

Run:

```bash
python3 generate_dolo_liquidity.py --registry data/dolo-liquidity-pools.json --output data/dolo-liquidity.json
```

Expected: exit 0, Island/farm and non-canonical manager sources are reconciled or explicitly preserved/unresolved; no fabricated owner is emitted.

- [ ] **Step 2: Validate generated totals and coverage**

Run:

```bash
python3 validate_data.py data/dolo-liquidity.json
python3 -m unittest tests.test_generate_dolo_liquidity tests.test_validate_dolo_liquidity
node --test tests/dolo-liquidity-ui.test.js tests/dolo-liquidity-workflow.test.js
python3 -m py_compile generate_dolo_liquidity.py validate_data.py
git diff --check
```

Expected: validator and all tests exit 0. Inspect the JSON and confirm Berachain DOLO/WBERA attributed value is materially above the former ~$20, custody rows remain non-verified, and Ethereum pool coverage reports match row totals.

- [ ] **Step 3: Verify browser geometry and interactions**

Start `python3 -m http.server 8765`, load `http://127.0.0.1:8765/dolo-preview.html`, and use Playwright at 1440×900, 1024×768 and 390×844. At each viewport assert no page-level overflow; summary values remain contained; selecting Berachain shows a visible Chain `×`; clearing it restores All chains without opening the menu; the first four columns are left aligned; Value computes to `rgb(154, 183, 194)`; Details displays fixed-decimal bounds and custody explanation without overflow.

- [ ] **Step 4: Commit generated data after fresh verification**

```bash
git add data/dolo-liquidity.json
git commit -m "Refresh audited DOLO liquidity data"
```

- [ ] **Step 5: Review exact publication scope**

Run `git status -sb`, `git diff dolomite-dashboard/master...HEAD --stat`, and `git diff dolomite-dashboard/master...HEAD --check`. Confirm every changed file belongs to the approved data/UX/spec/plan scope.

- [ ] **Step 6: Publish to production**

Push the feature branch, open/update the PR against `master`, merge after checks, then fetch and verify `dolomite-dashboard/master` contains the implementation. Do not force-push `master`.

- [ ] **Step 7: Verify GitHub Pages live**

Watch the repository Actions workflows for the merged commit. After Pages succeeds, request the live `dolo/` page and `data/dolo-liquidity.json` with a cache-busting query, verify the new cache token, Chain clear control, blue Value CSS, fixed-decimal bounds code and material Berachain rows. Report the merge commit and live URL.
