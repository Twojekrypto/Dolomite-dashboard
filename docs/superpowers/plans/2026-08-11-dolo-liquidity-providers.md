# DOLO Liquidity Providers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a production-ready DOLO Liquidity Providers card that presents exact active LP ownership and auditable add/remove history across Ethereum and Berachain, with verified custody attribution and the dashboard's existing institutional table UX.

**Architecture:** A dedicated Python pipeline discovers registered DOLO pools, replays protocol-specific on-chain events with a 128-block overlap, reconciles live position state, resolves supported custody paths, values exact principal amounts, and writes one compact validated artifact. The static DOLO page lazy-loads that artifact and renders one stable card with Active positions and History modes. Per-adapter status and last-known-good rows make failures visible without deleting healthy data.

**Tech Stack:** Python 3.11, `web3==7.16.0`, `eth-abi==5.2.0`, shared `rpc_client.py`, unittest fixtures, static HTML/CSS/JavaScript, Node contract tests, GitHub Actions, Playwright/browser verification through a local `python3 -m http.server`.

## Global Constraints

- Preserve the Graphite + Gold design and existing DOLO Holders table primitives; do not introduce a component framework or runtime dependency.
- Use exact integer parsing for raw token/liquidity values. Decimal conversion happens only at display/artifact boundaries.
- Never infer a beneficial owner from `tx.origin`, transaction sender, router caller, or a deposit heuristic.
- Never turn unavailable prices or undecodable v4 action amounts into zero.
- Treat Uniswap v4 pool IDs as `bytes32`, never as contract addresses.
- The five primary and two secondary pools in the approved specification are acceptance fixtures; dynamic discovery may add more.
- Keep `data/dolo-liquidity.json` below 2,000,000 minified bytes. Crossing the limit is a hard failure.
- Keep edits surgical. After changing table columns, audit all `.dolo-lp-*` `nth-child` selectors.
- Use a local HTTP server for UI verification and prove geometry with computed styles/bounding boxes.
- Every task ends with fresh targeted checks and a focused commit.

---

### Task 1: Establish the registry, artifact contract, and exact math primitives

**Files:**
- Create: `data/dolo-liquidity-pools.json`
- Create: `generate_dolo_liquidity.py`
- Create: `tests/test_generate_dolo_liquidity.py`
- Create: `tests/fixtures/dolo-liquidity/registry-minimal.json`

- [ ] **Step 1: Write failing registry and identity tests**

Add tests that load the production registry and assert:

```python
PRIMARY_IDENTIFIERS = {
    ("ethereum", "uniswap-v4", "0x2d97d14362ae5a19a15adb230cf8840ee7e133bf942fd8efd754ae4d078727ea"),
    ("ethereum", "uniswap-v3", "0x003896387666c5c11458eeb3f927b72a11b19783"),
    ("ethereum", "uniswap-v4", "0x6f6f24b5a1cd819382379eb032466b8bac7ea0697cfcf31b7350b55ff4f1c472"),
    ("ethereum", "uniswap-v4", "0x728e6e3b736e28f6b52f72ecec16a056b8ac6d9e05736a84e6b6128df9b1a12a"),
    ("berachain", "kodiak-v3", "0xd5980e98a89e2d2361b3be657e8a003c6d3514e3"),
}
SECONDARY_IDENTIFIERS = {
    ("berachain", "bulla-v2", "0x8991017b74f9f8070bff5b322802dd26e05e0cc7"),
    ("berachain", "kodiak-v3", "0x8194ed4d6701b7a1b40e48431de37047f0248b0b"),
}
```

The tests must also reject duplicate `(chain, adapter, identifier)` values, non-`bytes32` v4 pool IDs, non-address v2/v3 pools, missing creation/discovery blocks, unknown adapter names, and non-positive display thresholds.

- [ ] **Step 2: Run the RED test**

Run: `python3 -m unittest tests.test_generate_dolo_liquidity.RegistryContractTests -v`

Expected: FAIL because the module and registry do not exist.

- [ ] **Step 3: Add the concrete production registry**

Record:

- DOLO `0x0F81001eF0A83ecCE5ccebf63EB302c70a39a654` on both chains.
- Ethereum discovery start block `21_500_000`.
- Berachain discovery start block `2_900_000`.
- Uniswap v3 Factory `0x1F98431c8aD98523631AE4a59f267346ea31F984`.
- Uniswap v3 NPM `0xC36442b4a4522E871399CD717aBDD847Ab11FE88`.
- Uniswap v4 PoolManager `0x000000000004444c5dc75cB358380D2e3dE08A90`.
- Uniswap v4 PositionManager `0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e`.
- Kodiak v3 Factory `0xD84CBf0B02636E7f53dB9E5e45A616E05d710990`.
- Kodiak NPM `0xFE5E8C83FFE4d9627A75EaA7Fee864768dB989bD`.
- Kodiak v2 Factory `0x5e705e184d233ff2a7cb1553793464a9d0c3028f`.
- Kodiak Island Factory `0x5261c5A5f08818c08Ed0Eb036d9575bA1E02c1d6`.
- Kodiak Farm Factory `0xAeAa563d9110f833FA3fb1FF9a35DFBa11B0c9cF`.
- Default hidden-liquidity threshold `$1,000` and the seven approved pool rows.

Every row stores `identifierType` as exactly `contract` or `poolId`, pair display name, priority, primary flag, and adapter.

- [ ] **Step 4: Implement strict parsing and math helpers**

In `generate_dolo_liquidity.py`, expose pure functions:

```python
load_registry(path) -> dict
event_key(chain_key, tx_hash, log_index) -> str
tick_to_paired_per_dolo(tick, token0, token1, decimals0, decimals1, dolo_address) -> Decimal
amounts_for_liquidity(liquidity, sqrt_price_x96, sqrt_lower_x96, sqrt_upper_x96) -> tuple[int, int]
v2_underlying(lp_balance, total_supply, reserve0, reserve1) -> tuple[int, int]
classify_range(current_tick, tick_lower, tick_upper) -> str
```

Use `Decimal` with precision at least 90 for `1.0001 ** tick`; return raw integer principal amounts by flooring at the exact boundary. `classify_range` returns `in_range`, `out_of_range`, or `unavailable`.

- [ ] **Step 5: Add deterministic math tests**

Cover token ordering, negative ticks, below/inside/above range, exact boundary behavior, V2 proportional ownership, zero total supply rejection, and event-key normalization. Assert raw integers, not rounded UI strings.

- [ ] **Step 6: Run GREEN checks and commit**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_liquidity -v
python3 -m py_compile generate_dolo_liquidity.py
python3 -m json.tool data/dolo-liquidity-pools.json >/dev/null
git diff --check
```

Expected: all tests pass and the registry parses cleanly.

Commit: `feat: establish DOLO liquidity data contract`

---

### Task 2: Implement reusable RPC scanning, decoding, and incremental replay

**Files:**
- Modify: `generate_dolo_liquidity.py`
- Modify: `tests/test_generate_dolo_liquidity.py`
- Create: `tests/fixtures/dolo-liquidity/rpc-log-pages.json`

- [ ] **Step 1: Write failing scanner contracts**

Test that the scanner:

- starts from `max(configuredStart, previousLastScannedBlock - 127)`;
- chunks Ethereum and Berachain ranges without a skipped or duplicated boundary block;
- accepts JSON-RPC log order variations but returns `(blockNumber, transactionIndex, logIndex)` order;
- deduplicates only exact `(chain, txHash, logIndex)` matches;
- does not advance `lastScannedBlock` after any chunk fails;
- records sanitized adapter errors without RPC URLs or keys;
- keeps previous adapter rows and marks them `stale` after a refresh failure.

- [ ] **Step 2: Run RED**

Run: `python3 -m unittest tests.test_generate_dolo_liquidity.IncrementalReplayTests -v`

Expected: FAIL because scanner APIs are absent.

- [ ] **Step 3: Implement the scanner using `rpc_client.py`**

Add:

```python
scan_logs(chain, addresses, topics, from_block, to_block, chunk_size, rpc=rpc_single_request)
resume_block(previous_source, configured_start, overlap=128)
dedupe_logs(chain_key, logs)
load_previous_artifact(path)
preserve_stale_adapter(previous, adapter_key, error, generated_at)
```

Do not add a second RPC client. Use `get_endpoints`, `rpc_single_request`, and existing sanitization. Convert RPC hex with `int(value, 16)` and validate `removed != true`.

- [ ] **Step 4: Add block timestamp batching**

Implement a cache keyed by `(chain, blockNumber)` and use `rpc_batch_requests` for `eth_getBlockByNumber`. Reject a missing timestamp for a history row; do not substitute generation time.

- [ ] **Step 5: Run GREEN checks and commit**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_liquidity.IncrementalReplayTests -v
python3 -m unittest tests.test_generate_dolo_liquidity -v
python3 -m py_compile generate_dolo_liquidity.py
git diff --check
```

Commit: `feat: add incremental liquidity event replay`

---

### Task 3: Add V2-style pool ownership and history

**Files:**
- Modify: `generate_dolo_liquidity.py`
- Modify: `tests/test_generate_dolo_liquidity.py`
- Create: `tests/fixtures/dolo-liquidity/v2-bulla.json`

- [ ] **Step 1: Write failing V2 adapter tests**

The fixture must contain `Transfer`, `Mint`, `Burn`, and `Sync` logs for the approved DOLO/HONEY pool, including minimum-liquidity burn, transfer between wallets, full exit, and re-entry. Assert:

- one active row per nonzero wallet/pool LP balance;
- zero/dead addresses are excluded as beneficial owners;
- underlying amounts equal `balance / totalSupply × reserves` in raw units;
- the price range is `Full range`;
- mint/burn action amounts come from exact Mint/Burn events;
- transfer-only events change current ownership but do not create Added/Removed history;
- an unsupported contract holder produces `beneficialOwner: null`, keeps `custodian`, and is `custodied_unresolved`.

- [ ] **Step 2: Run RED**

Run: `python3 -m unittest tests.test_generate_dolo_liquidity.V2AdapterTests -v`

- [ ] **Step 3: Implement V2 decoding and reconciliation**

Add exact event topics/decoders for:

```solidity
Transfer(address indexed from,address indexed to,uint256 value)
Mint(address indexed sender,uint256 amount0,uint256 amount1)
Burn(address indexed sender,uint256 amount0,uint256 amount1,address indexed to)
Sync(uint112 reserve0,uint112 reserve1)
```

Current balances come from replayed LP transfers and are reconciled against latest `balanceOf`, `totalSupply`, `getReserves`, `token0`, and `token1` reads. Any mismatch marks the affected row/source partial rather than altering replayed numbers to match expectations.

- [ ] **Step 4: Run GREEN checks and commit**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_liquidity.V2AdapterTests -v
python3 -m unittest tests.test_generate_dolo_liquidity -v
python3 -m py_compile generate_dolo_liquidity.py
git diff --check
```

Commit: `feat: index DOLO V2 liquidity providers`

---

### Task 4: Add Uniswap v3 and Kodiak v3 position adapters

**Files:**
- Modify: `generate_dolo_liquidity.py`
- Modify: `tests/test_generate_dolo_liquidity.py`
- Create: `tests/fixtures/dolo-liquidity/v3-uniswap.json`
- Create: `tests/fixtures/dolo-liquidity/v3-kodiak.json`

- [ ] **Step 1: Write failing v3 lifecycle tests**

Fixtures cover mint, increase, partial decrease, NFT transfer, full close, reopen in a new token ID, below/inside/above range, and a historically burned NFT. Assert:

- Factory `PoolCreated` filters discover only pools containing DOLO.
- NPM `IncreaseLiquidity`/`DecreaseLiquidity` token IDs are mapped to a pool using `positions(tokenId)` at the event block; an archive-read failure keeps the row partial instead of guessing.
- Latest `positions`, `ownerOf`, pool `slot0`, and liquidity reads determine current active state.
- Current principal excludes `tokensOwed0/tokensOwed1`.
- History amounts use exact NPM event `amount0/amount1`.
- A decrease is `Closed` only when exact post-event liquidity is zero.
- Ethereum and Kodiak use separate official NPM/factory addresses.

- [ ] **Step 2: Run RED**

Run: `python3 -m unittest tests.test_generate_dolo_liquidity.V3AdapterTests -v`

- [ ] **Step 3: Implement v3 pool discovery and event decoding**

Use Factory `PoolCreated(address indexed token0,address indexed token1,uint24 indexed fee,int24 tickSpacing,address pool)` with two DOLO topic filters. Decode NPM:

```solidity
IncreaseLiquidity(uint256 indexed tokenId,uint128 liquidity,uint256 amount0,uint256 amount1)
DecreaseLiquidity(uint256 indexed tokenId,uint128 liquidity,uint256 amount0,uint256 amount1)
Transfer(address indexed from,address indexed to,uint256 indexed tokenId)
```

For every first-seen token ID, call `positions(tokenId)` at that event block before accepting it into a DOLO pool. Cache immutable token0/token1/fee/ticks by `(chain,npm,tokenId)`.

- [ ] **Step 4: Implement latest position reconciliation**

Batch `ownerOf`, `positions`, pool `slot0`, token metadata, and decimals. A reverted `ownerOf` plus zero position liquidity means closed; a reverted call with nonzero or undecodable state is partial/unavailable.

- [ ] **Step 5: Run GREEN checks and commit**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_liquidity.V3AdapterTests -v
python3 -m unittest tests.test_generate_dolo_liquidity -v
python3 -m py_compile generate_dolo_liquidity.py
git diff --check
```

Commit: `feat: index DOLO concentrated liquidity positions`

---

### Task 5: Add Uniswap v4 pool and position reconstruction

**Files:**
- Modify: `generate_dolo_liquidity.py`
- Modify: `tests/test_generate_dolo_liquidity.py`
- Create: `tests/fixtures/dolo-liquidity/v4-uniswap.json`

- [ ] **Step 1: Write failing v4 tests**

Fixture transactions include one mint, one unambiguous increase, one bundled two-position update, one decrease, one burn, and an NFT transfer for each approved v4 pool ID. Assert:

- `Initialize` uses indexed currency topics to discover DOLO pools and retains the exact pool ID.
- `ModifyLiquidity` uses `salt == bytes32(tokenId)` only when `sender` is the canonical PositionManager.
- PositionManager `getPoolAndPositionInfo`, `getPositionLiquidity`, and `ownerOf` establish current range/liquidity/owner.
- An unambiguous action reconstructs principal deltas from liquidity, ticks, and the transaction-point pool price.
- A bundled action without one-to-one proof has `amountStatus: unavailable`, `quality: partial`, and null token/USD amounts.
- No v4 pool ID receives an Etherscan address URL.

- [ ] **Step 2: Run RED**

Run: `python3 -m unittest tests.test_generate_dolo_liquidity.V4AdapterTests -v`

- [ ] **Step 3: Implement v4 event decoding**

Decode official PoolManager events:

```solidity
Initialize(bytes32 indexed id,address indexed currency0,address indexed currency1,uint24 fee,int24 tickSpacing,address hooks,uint160 sqrtPriceX96,int24 tick)
ModifyLiquidity(bytes32 indexed id,address indexed sender,int24 tickLower,int24 tickUpper,int256 liquidityDelta,bytes32 salt)
Swap(bytes32 indexed id,address indexed sender,int128 amount0,int128 amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick,uint24 fee)
```

Decode PositionManager `ModifyPosition` and ERC-721 Transfer events where available. Map a canonical PositionManager modification to `tokenId = int(salt, 16)`. Reject noncanonical senders from NFT attribution.

- [ ] **Step 4: Implement historical price-point reconstruction**

Within each transaction, sort PoolManager logs by log index, maintain the last exact `sqrtPriceX96`, and use an archive `StateView`/pool state call at `blockNumber - 1` when no preceding Initialize/Swap establishes the price. If no exact price is available, keep the history action visible with unavailable amounts.

- [ ] **Step 5: Run GREEN checks and commit**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_liquidity.V4AdapterTests -v
python3 -m unittest tests.test_generate_dolo_liquidity -v
python3 -m py_compile generate_dolo_liquidity.py
git diff --check
```

Commit: `feat: reconstruct DOLO Uniswap v4 liquidity`

---

### Task 6: Resolve Kodiak Island and supported custody ownership fail-closed

**Files:**
- Modify: `data/dolo-liquidity-pools.json`
- Modify: `generate_dolo_liquidity.py`
- Modify: `tests/test_generate_dolo_liquidity.py`
- Create: `tests/fixtures/dolo-liquidity/kodiak-island.json`
- Create: `tests/fixtures/dolo-liquidity/kodiak-farm.json`

- [ ] **Step 1: Write failing beneficial-owner tests**

Cover direct NPM ownership, Island share holders, share transfer, user deposit/withdrawal, manager rebalance, farm stake/unstake, and an unknown custodian. Assert:

- Island rows allocate exact underlying raw amounts using `userShares / totalShares`, with the final deterministic row receiving the integer remainder so rows sum exactly to the Island principal.
- Island rebalances do not create user Added/Removed actions.
- Island share mint/burn creates the user history action using exact deposit/withdraw token events.
- Farm staking moves the custody path but does not create liquidity history.
- A supported farm resolves the staker's exact share balance.
- An unsupported farm/vault keeps only `custodian`, null `beneficialOwner`, and `quality: unavailable` for ownership.

- [ ] **Step 2: Run RED**

Run: `python3 -m unittest tests.test_generate_dolo_liquidity.BeneficialOwnerTests -v`

- [ ] **Step 3: Implement Island discovery and proportional allocation**

Scan Kodiak Island Factory `IslandCreated` events from the configured start block, retain only Islands whose `token0/token1` contain DOLO, read their underlying V3 positions/current balances, and replay ERC-20 Transfer balances for share holders.

- [ ] **Step 4: Implement supported farm accounting**

Discover farms through the official Kodiak Farm Factory and accept a farm only after its staking token exactly equals the Island/LP token and its per-user stake interface reconciles to custody balance. Store the full path as `direct`, `kodiak_island`, or `kodiak_island_farm`.

- [ ] **Step 5: Run GREEN checks and commit**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_liquidity.BeneficialOwnerTests -v
python3 -m unittest tests.test_generate_dolo_liquidity -v
python3 -m py_compile generate_dolo_liquidity.py
python3 -m json.tool data/dolo-liquidity-pools.json >/dev/null
git diff --check
```

Commit: `feat: attribute Kodiak liquidity beneficiaries`

---

### Task 7: Assemble, value, validate, and generate the compact artifact

**Files:**
- Modify: `generate_dolo_liquidity.py`
- Modify: `validate_data.py`
- Modify: `tests/test_generate_dolo_liquidity.py`
- Create: `tests/test_validate_dolo_liquidity.py`
- Create: `data/dolo-liquidity.json`

- [ ] **Step 1: Write failing artifact validation tests**

Require:

```json
{
  "schemaVersion": 1,
  "generatedAt": "ISO-8601 UTC",
  "summary": {},
  "sources": [],
  "pools": [],
  "activePositions": [],
  "history": [],
  "quality": {}
}
```

Tests reject duplicate active row IDs, duplicate history event keys, invalid source cursors, missing seven acceptance pools, v4 identifier/address confusion, negative raw amounts, zero substituted for unavailable USD values, inconsistent summary counts, wrapper double counting, proportional sums that do not reconcile, and files at or above 2,000,000 bytes.

- [ ] **Step 2: Run RED**

Run:

```bash
python3 -m unittest tests.test_validate_dolo_liquidity -v
python3 validate_data.py data/dolo-liquidity.json
```

Expected: FAIL before validator registration/artifact creation.

- [ ] **Step 3: Implement pricing and enrichment**

Fetch Dexscreener pair metadata only for liquidity/volume/URL and a quote-token USD cross-check. Use the checked-in `dolo_price.json` DOLO USD price and exact paired-token price evidence. Store `valueUsd: null` plus a reason when either side lacks price proof. Preserve previous enrichment as stale on an enrichment-only failure.

- [ ] **Step 4: Assemble deterministic output**

Sort pools by priority/pair/adapter, active rows by descending available USD value then stable row ID, and history by descending block/log. Compute active and period summaries from emitted rows. Write minified JSON atomically using a temporary file and `os.replace`.

- [ ] **Step 5: Register strict validation**

Add `_dolo_liquidity_valid(data)` helpers and a `RULES["dolo-liquidity.json"]` entry in `validate_data.py`. Freshness is at most 3 hours. Validate statuses in `{complete,partial,stale,unavailable}`, qualities in `{verified,partial,stale,unavailable}`, and exact summary reconciliation.

- [ ] **Step 6: Run the real bounded generator**

Run:

```bash
python3 generate_dolo_liquidity.py --registry data/dolo-liquidity-pools.json --output data/dolo-liquidity.json
python3 validate_data.py data/dolo-liquidity.json
```

Expected: exit 0; all seven acceptance pools are present; incomplete adapters are explicitly partial/stale rather than silently omitted; artifact is below 2 MB.

- [ ] **Step 7: Run GREEN checks and commit**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_liquidity tests.test_validate_dolo_liquidity -v
python3 -m py_compile generate_dolo_liquidity.py validate_data.py
python3 validate_data.py data/dolo-liquidity.json
git diff --check
```

Commit: `feat: generate validated DOLO liquidity artifact`

---

### Task 8: Add the hourly production workflow and Pages dependency

**Files:**
- Create: `.github/workflows/update-dolo-liquidity.yml`
- Modify: `.github/workflows/pages.yml`
- Create: `tests/dolo-liquidity-workflow.test.js`

- [ ] **Step 1: Write the failing workflow contract test**

Assert the workflow:

- is named `Update DOLO Liquidity`;
- schedules `17 * * * *` and supports manual dispatch;
- checks out `master` and installs `requirements.txt`;
- runs both focused Python test modules before generation;
- exposes Ethereum/Berachain RPC secrets already used by the repository;
- runs generator then `validate_data.py data/dolo-liquidity.json`;
- stages only `data/dolo-liquidity.json` and `data/dolo-liquidity-pools.json`;
- retries a rebase/push three times and fails if none succeed;
- uses `cancel-in-progress: true` for stale hourly jobs;
- is listed under `workflow_run.workflows` in Pages.

- [ ] **Step 2: Run RED**

Run: `node --test tests/dolo-liquidity-workflow.test.js`

- [ ] **Step 3: Add workflow and Pages trigger**

Use Python 3.11 and a 55-minute timeout. The workflow must not use `if: always()` on publish. If validation fails, no data is staged.

- [ ] **Step 4: Run GREEN checks and commit**

Run:

```bash
node --test tests/dolo-liquidity-workflow.test.js
python3 -c 'import yaml' 2>/dev/null || true
git diff --check
```

Also inspect the YAML indentation directly because PyYAML is not a repository dependency.

Commit: `ci: refresh DOLO liquidity hourly`

---

### Task 9: Build the one-card/two-mode DOLO table shell

**Files:**
- Modify: `dolo-preview.html`
- Create: `tests/dolo-liquidity-ui.test.js`

- [ ] **Step 1: Write failing static UI contracts**

Assert the new card occurs after `DOLO Flows` and before `Fresh 10K+ DOLO Wallets`, and contains:

- `DOLO Liquidity Providers` title/count/freshness;
- `Active positions` and `History` segmented buttons;
- four fixed summary cells;
- search/chain/pair/DEX/low-liquidity controls;
- history period/action controls;
- one table shell with exact active and history header templates;
- a ten-row body target, footer info, centered pager, and details target;
- `data-address-match-cells` on the table and existing shared address scripts.

- [ ] **Step 2: Run RED**

Run: `node --test tests/dolo-liquidity-ui.test.js`

- [ ] **Step 3: Add semantic markup between Flows and Fresh Wallets**

Use unique IDs prefixed `dolo-lp-`. Keep the table in a `.tbl-wrap` so mobile overflow is contained. Use buttons with `aria-pressed`, labels for inputs, and `aria-expanded` on Details controls.

- [ ] **Step 4: Add scoped Graphite + Gold styles**

Reuse existing color tokens and sizes. Add only `.dolo-lp-*` styles for mode control, summary rail, status/quality chips, mixed pair cell, details expansion, inactive spacer rows, and responsive toolbar wrapping. The card background/footer must match DOLO Holders exactly.

- [ ] **Step 5: Audit column selectors**

Search: `rg -n "dolo-lp.*nth-child|nth-child.*dolo-lp" dolo-preview.html`

Verify every selector against both nine-column schemas; prefer semantic classes over `nth-child`.

- [ ] **Step 6: Run GREEN checks and commit**

Run:

```bash
node --test tests/dolo-liquidity-ui.test.js
python3 -m unittest tests.test_table_ui_consistency_contracts tests.test_responsive_layout_contracts -v
git diff --check
```

Commit: `feat: add DOLO liquidity table shell`

---

### Task 10: Implement table state, filters, sorting, details, and address UX

**Files:**
- Modify: `dolo-preview.html`
- Modify: `tests/dolo-liquidity-ui.test.js`
- Modify: `tests/test_address_match_table_scope.py`

- [ ] **Step 1: Expand tests for behavior contracts**

Test exported/pure helpers for:

- Active default and mode switch to History.
- Shared search/chain/pair/DEX state survives the switch.
- History period/action states retain last values.
- Added filter includes Added+Increased; Removed includes Removed+Closed.
- Low-liquidity pools hide only when `liquidityUsd < 1000`; null liquidity remains visible.
- Active sort defaults to Value descending with unavailable values last.
- History sort defaults to Date descending.
- Search clear dispatches `input`.
- Footer says `1–10 of N` and ten visible rows/spacers remain stable.
- Pool links distinguish `contract` from `poolId`.
- Duplicate-address highlighting is scoped to the LP table.

- [ ] **Step 2: Run RED**

Run:

```bash
node --test tests/dolo-liquidity-ui.test.js
python3 -m unittest tests.test_address_match_table_scope -v
```

- [ ] **Step 3: Implement lazy data loading and state**

Fetch `data/dolo-liquidity.json?v=20260811-dolo-liquidity-v1` after first paint. On failure render ten stable unavailable rows and `Data unavailable — try again later`; do not collapse the card.

Use one state object:

```javascript
const doloLpState = {
  mode: "active",
  query: "",
  chain: "all",
  pairs: new Set(),
  dexes: new Set(),
  includeLowLiquidity: false,
  historyPeriod: "30d",
  historyAction: "all",
  activeSort: {key:"valueUsd", dir:"desc"},
  historySort: {key:"timestamp", dir:"desc"},
  page: 0,
  pageSize: 10,
};
```

- [ ] **Step 4: Implement renderers and exact summaries**

Render title counts, freshness, adapter warning tooltip, mode-specific summary rail, headers, rows, ten-row spacers, footer, and pager from the same filtered row set. `LP Wallets` counts unique non-null beneficial owners. Use `Unavailable` for null amounts/prices.

- [ ] **Step 5: Implement controls and details**

Populate pair/DEX filter choices from `pools[]`, use existing dropdown checkmark patterns, reset page on any filter/sort change, and render premium in-row Details without an extra nested border. Include exact attribution path and provenance links.

- [ ] **Step 6: Reuse labels and repeated-address interactions**

Build wallet cells with the same white label + muted address + copy/DeBank/explorer controls used elsewhere. Set normalized beneficial-owner addresses on the shared hover trigger. Row hover quietly highlights same-address cells; direct address hover is stronger.

- [ ] **Step 7: Run GREEN checks and commit**

Run:

```bash
node --test tests/dolo-liquidity-ui.test.js tests/address-match-highlighting.test.js
python3 -m unittest tests.test_address_match_table_scope tests.test_dolo_address_labels -v
node --check shared-hover-tooltips.js
git diff --check
```

Commit: `feat: make DOLO liquidity table interactive`

---

### Task 11: Advance route caches and verify every target viewport

**Files:**
- Modify: `dolo/index.html`
- Modify: `index.html`
- Modify: `tests/dolo-liquidity-ui.test.js`
- Modify only if a verified bug requires it: `dolo-preview.html`

- [ ] **Step 1: Write the failing cache contract**

Require the exact new route version suffix `dolo-liquidity-providers-20260811` in both route entry points and the exact data URL cache key `20260811-dolo-liquidity-v1` in `dolo-preview.html`.

- [ ] **Step 2: Advance both route loaders**

Append the suffix once; do not rewrite the existing long version history.

- [ ] **Step 3: Run the full focused test set**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_liquidity tests.test_validate_dolo_liquidity tests.test_address_match_table_scope tests.test_dolo_address_labels -v
node --test tests/dolo-liquidity-ui.test.js tests/dolo-liquidity-workflow.test.js tests/address-match-highlighting.test.js tests/table-surface-consistency.test.js
python3 validate_data.py data/dolo-liquidity.json
python3 -m py_compile generate_dolo_liquidity.py validate_data.py
git diff --check
```

- [ ] **Step 4: Start a local server and run browser verification**

Start: `python3 -m http.server 8765`

Verify `http://127.0.0.1:8765/dolo/?v=20260811-dolo-liquidity-v1` at `1440×900`, `1024×768`, `768×1024`, and `390×844`.

For each viewport, record:

- `document.documentElement.scrollWidth === document.documentElement.clientWidth`;
- LP card/table/footer bounding boxes;
- active/history card-height delta;
- before/after search-empty card-height delta;
- contained table scroll width on mobile;
- filter panel inside viewport;
- exactly one strong address target under direct hover and quiet duplicate targets under row hover;
- no console error or failed `data/dolo-liquidity.json` request.

All document-overflow and stable-shell deltas must be `0px`; only the contained `.tbl-wrap` may horizontally scroll on mobile.

- [ ] **Step 5: Reconcile representative live rows**

Check one row from each available class against RPC evidence:

1. Ethereum Uniswap v3 direct position.
2. Ethereum Uniswap v4 direct position.
3. Berachain Kodiak v3 direct position.
4. Kodiak Island/farm attributed position, or an explicit zero-count/unavailable source status if none exists.
5. Berachain Bulla Algebra Integral direct position.

Record pool identifier, owner/custodian, raw liquidity/share balance, token amounts, status, source block, and transaction link. Do not promote an adapter when the evidence is incomplete.

- [ ] **Step 6: Final self-review and commit**

Run:

```bash
git status --short
git diff --stat dolomite-dashboard/master...HEAD
git diff --check dolomite-dashboard/master...HEAD
rg -n "TBD|TODO|FIXME|placeholder" generate_dolo_liquidity.py data/dolo-liquidity-pools.json tests/dolo-liquidity-ui.test.js
```

Confirm every acceptance criterion from `docs/superpowers/specs/2026-08-11-dolo-liquidity-providers-design.md` has evidence.

Commit: `chore: verify DOLO liquidity providers`

---

### Task 12: Publish to production and verify GitHub Pages

**Files:** No source edits unless deployment verification reveals a scoped defect.

- [ ] **Step 1: Rebase safely onto latest production**

Fetch `origin/master`, inspect divergence, and rebase the feature branch without overwriting unrelated production work. Re-run all Task 11 focused tests after the rebase.

- [ ] **Step 2: Push the completed history to `master`**

Push only after all local checks and browser matrices pass. If credentials or branch protection block the push, report the exact command/status instead of claiming the feature is live.

- [ ] **Step 3: Verify workflows**

Wait for `Deploy GitHub Pages` and `Update DOLO Liquidity` to succeed. Inspect logs if either fails, fix only the diagnosed issue, and repeat validation.

- [ ] **Step 4: Verify production with cache busting**

Open:

`https://twojekrypto.github.io/Dolomite-dashboard/dolo/?v=20260811-dolo-liquidity-live`

Confirm the route contains the new card, the artifact returns HTTP 200 with schemaVersion 1, freshness is current, both modes work, and the representative rows still reconcile.

- [ ] **Step 5: Report the exact deployment outcome**

Provide the production commit hash, successful workflow run links/statuses, pools/adapters covered, verified versus partial/stale counts, artifact timestamp/size, and the viewport matrix result.
