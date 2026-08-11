# DOLO Liquidity Providers — Design Specification

**Status:** User-approved design, awaiting written-spec review

**Date:** 2026-08-11

**Target:** DOLO tab in the Dolomite analytics dashboard

## 1. Objective

Add one institutional-grade `DOLO Liquidity Providers` card to the DOLO tab. The card must let users compare every supported DOLO liquidity pool in one stable table and switch the same table shell between:

1. `Active positions` — current liquidity positions and their current on-chain state.
2. `History` — liquidity additions, increases, removals, and closes.

The feature must preserve the dashboard's existing Graphite + Gold identity, table geometry, address interactions, filters, sorting, pagination, tooltips, responsive behavior, and freshness metadata.

## 2. Product Principles

1. **One card, two modes.** Pools are filters and row attributes, not separate pages or separate tables.
2. **One row represents one wallet-position attribution in Active mode.** A direct position produces one row; an Island or supported wrapper produces one proportional row per verified beneficial wallet. A wallet with several ranges appears once per range so information is never lost.
3. **One row represents one liquidity action in History mode.** Repeated additions to the same position remain independently auditable.
4. **On-chain ownership first.** Dexscreener may enrich pool metadata but cannot determine LP ownership.
5. **No guessed beneficiaries or amounts.** Unresolved custody and unavailable historical amounts remain explicit.
6. **Technical identity and beneficial ownership stay separate.** An Island, farm, vault, or position manager can custody liquidity while a user wallet remains the beneficiary.

## 3. Scope

### 3.1 Included

- Ethereum Uniswap v3 concentrated-liquidity positions.
- Ethereum Uniswap v4 concentrated-liquidity positions.
- Berachain Kodiak v3 concentrated-liquidity positions.
- Kodiak Island share ownership and supported farm/reward-vault custody paths.
- Berachain V2-style pools discovered on Bulla and BeraSwap.
- Current principal token amounts, normalized price ranges, USD value, and in-range status.
- Add/increase/remove/close history with transaction provenance.
- Pool discovery, freshness, completeness, and stale-source reporting.
- Existing DOLO wallet labels, copy, explorer, DeBank, and repeated-address highlighting.

### 3.2 Excluded

- LP profit and loss, impermanent loss, or tax cost basis.
- Estimated APR from fees or incentives.
- Guessed ownership behind an unsupported vault, farm, bridge, or custody contract.
- Treating Dexscreener liquidity figures as proof of an LP wallet's balance.
- Presenting Uniswap v4 `poolId` values as contract addresses.
- Silent zero-value fallbacks for missing prices or undecodable token amounts.

## 4. Pool Coverage

The pipeline discovers DOLO pools on-chain and maintains an explicit operational registry. The registry controls display names, protocol adapters, creation blocks, and priority; strict event classification remains in code with tests.

### 4.1 Primary pools

| Chain | Pair | DEX | Version | Pool contract or poolId |
|---|---|---|---|---|
| Ethereum | DOLO/USD1 | Uniswap | v4 | `0x2d97d14362ae5a19a15adb230cf8840ee7e133bf942fd8efd754ae4d078727ea` |
| Ethereum | DOLO/USDC | Uniswap | v3 | `0x003896387666c5c11458eeb3f927b72a11b19783` |
| Ethereum | DOLO/ETH | Uniswap | v4 | `0x6f6f24b5a1cd819382379eb032466b8bac7ea0697cfcf31b7350b55ff4f1c472` |
| Ethereum | DOLO/USDC | Uniswap | v4 | `0x728e6e3b736e28f6b52f72ecec16a056b8ac6d9e05736a84e6b6128df9b1a12a` |
| Berachain | DOLO/WBERA | Kodiak | v3 | `0xd5980e98a89e2d2361b3be657e8a003c6d3514e3` |

### 4.2 Secondary pools

| Chain | Pair | DEX | Version | Pool contract |
|---|---|---|---|---|
| Berachain | DOLO/HONEY | Bulla | Algebra Integral CL | `0x8991017b74f9f8070bff5b322802dd26e05e0cc7` |
| Berachain | DOLO/USDC.e | Kodiak | v3 | `0x8194ed4d6701b7a1b40e48431de37047f0248b0b` |

Every other on-chain-discovered DOLO pool is retained in the dataset. The UI hides a non-primary pool by default only when its most recent verified liquidity is below `$1,000`. `Include low liquidity` reveals it. A pool with unavailable USD liquidity is not silently hidden; it appears with `Unavailable` metadata.

## 5. Definitions and Amount Semantics

### 5.1 Active position

A position is active when its current on-chain liquidity or LP-share balance is greater than zero.

- Direct concentrated liquidity: one row per position NFT or equivalent unique position key.
- Island or supported wrapped liquidity: one proportional row per beneficial wallet and underlying position/custody path. The custodian's aggregate row is not also emitted, preventing double counting.
- V2 liquidity: one row per beneficial wallet and pool because the LP balance is fungible and full-range.
- A transferred position belongs to its current beneficial owner, while History preserves the original actions and owners at their event times.

### 5.2 Current asset amounts

`DOLO` and `Paired Asset` in Active mode are the current principal amounts implied by the position's liquidity, tick bounds, and current pool price. They do not include unclaimed fees.

- Claimable fees may appear in Details only when the adapter can calculate them exactly.
- The `Active Liquidity` summary is the USD value of current principal only.
- Proportional wrapper rows must sum back to the wrapper's exact underlying principal before the summary is accepted.
- Missing price data produces `Unavailable`, never `$0`.

### 5.3 Historical action amounts

History rows report the actual principal token deltas for that liquidity action.

- V3 and V2 adapters use emitted token amounts when available.
- V4 reconstructs token deltas using ordered PoolManager events, position tick bounds, liquidity delta, and the pool price at the action point.
- If a bundled transaction cannot be attributed unambiguously to one position, token amounts remain `Unavailable` and the row is marked `Partial`; the pipeline must not distribute aggregate transaction deltas heuristically.
- Island History records user share mint/burn deposits and withdrawals. Manager rebalances are protocol operations and do not appear as user `Added` or `Removed` rows.
- Farm staking and unstaking change custody only. They update the attribution path but do not change liquidity and therefore do not create `Added` or `Removed` rows.

### 5.4 Price range

All ranges are normalized as units of the paired token per `1 DOLO`, regardless of token ordering in the pool.

- Stablecoin pairs render as `$lower–$upper`.
- ETH and WBERA pairs render as `lower–upper ETH/WBERA` with a USD equivalent in Details when available.
- V2 pools render `Full range`.
- `In range` is determined from current tick versus normalized lower and upper ticks.

## 6. Beneficial-Owner Attribution

Each position stores both `custodian` and `beneficialOwner`.

The resolver applies these levels in order:

1. **Direct NFT/LP ownership:** current NFT owner or LP-token holder is the beneficial owner.
2. **Known Kodiak Island:** Island share holders receive proportional attribution to the Island's underlying V3 position.
3. **Known farm/reward vault:** staked Island or LP shares are resolved to the staker using the supported farm's exact on-chain balance/accounting interface.
4. **Known vault wrapper:** resolve only when the wrapper exposes exact per-user share balances and an exact conversion to underlying position ownership.
5. **Unsupported custody:** keep the custodian address, set `beneficialOwner=null`, and label the row `Custodied / unresolved`.

The UI displays `Wallet · via Kodiak Island`, `Wallet · via Farm`, or the equivalent verified custody path. It never substitutes `tx.origin`, transaction sender, router caller, or depositor guesses for beneficial ownership.

Safe/multisig and EIP-7702 delegated EOAs reuse the existing DOLO wallet-classification rules.

## 7. User Experience

### 7.1 Placement

Insert `DOLO Liquidity Providers` immediately after `DOLO Flows` and before `Fresh 10K+ DOLO Wallets`.

### 7.2 Header

- Title: `DOLO Liquidity Providers`
- Count: `X wallets · Y active positions`
- Freshness: `Data updated · Xm ago`, using the same component placement as `DOLO Holders`
- Segmented mode control: `Active positions` (default) / `History`
- A partial or stale source adds a quiet warning badge and a tooltip listing affected chains/adapters.

### 7.3 Summary rail

Active mode shows:

1. `Active Liquidity`
2. `LP Wallets`
3. `Active Positions`
4. `Out of Range`

`LP Wallets` counts unique verified non-null beneficial owners. `Active Positions` counts all emitted active attribution rows, including explicit unresolved-custody rows.

History mode reuses the same rail geometry and shows:

1. `Added · selected period`
2. `Removed · selected period`
3. `Net Liquidity · selected period`
4. `Active Wallets · selected period`

### 7.4 Toolbar

Both modes:

- Search wallet address or label, with the existing quick-clear `×`.
- Single-select chain filter: All, Ethereum, Berachain.
- Multi-select pair filter because two Uniswap versions can share `DOLO/USDC`.
- Multi-select DEX/version filter.
- `Include low liquidity` checkbox, off by default.

History additionally includes:

- Period: `24H`, `7D`, `30D`, `90D`, `All`.
- Action: `All`, `Added`, `Removed`.

Changing mode preserves chain, pair, DEX, and search state. Mode-specific filters retain their last values within the session.

### 7.5 Active table

Columns:

`Chain · Pair · Wallet · Price Range · DOLO · Paired Asset · Value · Status · Details`

- Default sort: `Value` descending.
- Sortable: Chain, Pair, Wallet, range width, DOLO, Paired Asset, Value, Status.
- `Paired Asset` header and unit adapt per row; mixed-pair sorting uses USD value of that token amount.
- Status values: `In range`, `Out of range`, `Custodied`, `Unavailable`.

### 7.6 History table

Columns:

`Date · Chain · Pair · Wallet · Action · DOLO · Paired Asset · Value · Details`

- Default sort: Date descending.
- Actions map exact protocol events to `Added`, `Increased`, `Removed`, or `Closed`. The toolbar's `Added` group includes Added + Increased; its `Removed` group includes Removed + Closed.
- `Closed` requires the position's liquidity to reach exact zero after the action.
- A later reopen is a new active lifecycle while preserving the original Position ID.

### 7.7 Details

The existing premium details pattern displays:

- Position ID/NFT ID or V2 wallet-pool key.
- DEX and version.
- Pool contract or `poolId`, correctly typed.
- Fee tier and tick spacing.
- Full normalized and raw tick range.
- Principal amounts and exact-value methodology.
- Custodian, beneficial owner, and attribution path.
- Transaction hash, block, and timestamp for History.
- Data quality: `Verified`, `Partial`, `Stale`, or `Unavailable` with a concise explanation.

### 7.8 Table behavior

- Reuse DOLO Holders typography, continuous background, row hover, address component, sorting affordance, footer, and pager.
- Ten-row stable viewport with inert spacer rows so search and filters do not resize the card.
- Repeated-address hover works only within this table: row hover quietly highlights identical wallet cells; direct address hover remains stronger.
- Page footer follows `1–10 of N`, with centered pager controls.
- Links include the appropriate chain explorer, DeBank for wallet addresses, and Dexscreener/DEX for pools.

### 7.9 Responsive behavior

- Desktop keeps summary rail, filters, and mode control on one coherent card.
- Tablet wraps toolbar groups without detaching dropdown panels.
- Mobile stacks header metadata and toolbar controls, preserves touch target size, and uses contained horizontal scrolling for the table.
- The document itself must have zero horizontal overflow at `390×844`, `768×1024`, and desktop widths.

## 8. Data Architecture

### 8.1 Files

- `generate_dolo_liquidity.py` — discovery, event replay, current-state reads, attribution, valuation, and artifact generation.
- `data/dolo-liquidity-pools.json` — operational pool/adapter registry and display priorities.
- `data/dolo-liquidity.json` — generated artifact consumed by the DOLO page.
- `tests/fixtures/dolo-liquidity/` — deterministic protocol event and RPC fixtures.
- `.github/workflows/update-dolo-liquidity.yml` — scheduled and manual refresh.

Strict classification logic, event signatures, supported custody adapters, and quality promotion rules remain in Python code with tests. Operational RPC endpoints, starting blocks, pool priority, and display thresholds remain in config.

### 8.2 Generated artifact

The artifact contains:

- `schemaVersion`
- `generatedAt`
- `summary`
- `sources[]` with chain, adapter, status, last scanned block, latest chain block, and errors
- `pools[]`
- `activePositions[]`
- `history[]`
- `quality` and unresolved-custody counts

The first release uses one compact JSON artifact. The workflow fails if the minified artifact exceeds `2 MB`; exceeding that limit requires an explicit versioned migration to lazy history shards rather than silently degrading initial DOLO-page performance.

### 8.3 Incremental replay

The generator reads the last valid checked-in artifact and resumes each adapter from its `lastScannedBlock` with a 128-block overlap.

- Events are deduplicated by `(chain, txHash, logIndex)`.
- Overlap rows are rebuilt so short reorganizations do not create duplicates.
- A source advances its cursor only after all chunks validate.
- First scan begins at the configured pool/manager deployment block, not genesis.
- Current ownership and position state are reconciled against current on-chain reads after replay.

### 8.4 Protocol adapters

**Uniswap v3**

- Discover DOLO pools through Factory `PoolCreated` events.
- Replay pool Mint/Burn events and NonfungiblePositionManager Transfer/liquidity events.
- Read current `positions(tokenId)`, `ownerOf`, pool slot state, ticks, and liquidity.

**Uniswap v4**

- Discover DOLO pools from PoolManager initialization events.
- Treat pool identifiers as `bytes32 poolId`.
- Replay PoolManager liquidity changes and PositionManager NFT ownership/actions.
- Use ordered event replay plus archive state where required for exact historical token deltas.

**Kodiak v3**

- Use the Kodiak deployment/factory registry and Uniswap-v3-compatible pool/position semantics.
- Resolve direct position NFTs separately from Island-owned positions.

**Kodiak Islands/farms**

- Read Island total supply, underlying position, current underlying balances, and user share balances.
- Resolve only explicitly supported farm/reward-vault staking contracts.
- Preserve the full custody path.

**Bulla/BeraSwap V2-style pools**

- Replay Mint/Burn and LP-token Transfer events.
- Calculate current wallet underlying amounts from verified LP balance / total supply × current reserves.
- Render `Full range`.

### 8.5 Pricing and enrichment

- Pool state determines tick, range, principal token amounts, and in-range status.
- Existing verified dashboard price sources determine USD values.
- Dexscreener enriches pool TVL, 24-hour volume, page URL, and discovery cross-checks.
- A Dexscreener outage does not invalidate exact on-chain positions; enrichment fields become unavailable and the previous verified values are marked stale.

## 9. Failure and Freshness Policy

Each adapter is independently `complete`, `partial`, `stale`, or `unavailable`.

- Failure in one adapter does not delete or downgrade verified rows from other adapters.
- A failed refresh preserves the last valid rows for that adapter and marks them stale with their original timestamp.
- A partial initial scan cannot be labeled complete.
- Unsupported custody cannot be promoted to a wallet attribution.
- Missing prices cannot be converted to zero.
- Historical v4 actions without an exact amount proof remain visible with `Partial` and unavailable amounts.
- The UI lists the affected adapter in a tooltip rather than showing a generic warning.

## 10. Workflow and Deployment

`Update DOLO Liquidity` runs hourly and on manual dispatch.

1. Use the Python standard library and dependencies already present in the repository. A new runtime dependency requires a separate explicit design approval; the generator must not add one opportunistically.
2. Run focused unit tests.
3. Run the incremental generator.
4. Validate schema, deduplication, pool coverage, source cursors, sums, and file-size cap.
5. Commit only the registry/artifact when changed.
6. Trigger or participate in the existing Pages deployment chain so generated data reaches production.

Workflow concurrency cancels stale runs but must not discard a completed validated artifact.

## 11. Verification Strategy

### 11.1 Python tests

- Pool discovery for all supported protocols.
- V3 and v4 event decoding.
- V2 Mint/Burn/Transfer correlation.
- Tick math and both token orderings.
- Principal amount calculation below, within, and above range.
- V2 full-range share calculation.
- Direct NFT ownership and NFT transfer.
- Kodiak Island proportional ownership.
- Supported farm attribution.
- Unsupported custody fail-closed behavior.
- Add, increase, remove, close, and reopen lifecycle.
- Deduplication and 128-block reorg overlap.
- Per-adapter stale fallback without cross-adapter data loss.
- Missing price and partial v4 action behavior.
- Checked-in artifact schema and pool coverage.

### 11.2 JavaScript/static contracts

- Mode switch preserves shared filters.
- Column/sort mappings match both table schemas.
- Search clear dispatches the normal input path.
- Low-liquidity rows are hidden only by the dedicated toggle.
- v4 pool IDs never receive wallet/explorer address treatment.
- Shared wallet labels and repeated-address hover are used.
- Route/data cache keys advance.

### 11.3 Browser verification

Run from `python3 -m http.server` and verify the real DOLO route at:

- `1440×900`
- `1024×768`
- `768×1024`
- `390×844`

Verify computed styles and bounding boxes for:

- stable card height across mode/search/filter changes;
- zero document horizontal overflow;
- contained mobile table scrolling;
- dropdown placement and checkmarks;
- header, freshness, summary rail, and pager alignment;
- details expansion without geometry breakage;
- source-row and duplicate-address highlighting;
- direct hover/copy/DeBank/pool link behavior.

### 11.4 Live verification

After push to `master`:

- Wait for Pages and relevant audit workflows to succeed.
- Fetch the public DOLO route with a unique cache-busting parameter.
- Confirm the new data cache key and generated artifact are served.
- Confirm one Ethereum v3, one Ethereum v4, one Kodiak direct, one Island/farm, and one V2 row against their on-chain sources.

## 12. Acceptance Criteria

The feature is complete only when:

1. One DOLO card switches between Active positions and History without changing its shell geometry.
2. All five primary and two secondary pools are present, and other discovered pools follow the low-liquidity rule.
3. Direct and supported wrapped positions show verified beneficial wallets; unsupported custody is explicit.
4. Active principal amounts, normalized ranges, status, and USD values reconcile to on-chain state.
5. History actions reconcile to exact events, with ambiguous amounts marked partial rather than guessed.
6. Filters, sorting, address UX, details, pagination, responsive layout, and freshness match existing DOLO table standards.
7. Focused tests, validators, browser matrices, workflow checks, and live cache verification pass.
