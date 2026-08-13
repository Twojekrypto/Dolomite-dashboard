# DOLO Liquidity Coverage and Table Parity Design

**Date:** 2026-08-12
**Scope:** the `DOLO Liquidity Providers` data pipeline and table on the DOLO page.

## Goal

Show the economically meaningful DOLO liquidity on Berachain and Ethereum without guessing wallet ownership, prevent double counting between vault positions and vault-share holders, and bring the table's summary, filters, columns, values and price-bound details into the dashboard's established table UX.

## Observed data gap

The current Kodiak v3 adapter discovers only direct NFT positions. The DOLO/WBERA pool has roughly $128K–$136K of pool liquidity, while the current artifact attributes only about $20 to five small direct NFTs. The missing liquidity is managed by Kodiak Island contracts that the production builder does not expand, even though Island discovery and allocation helpers already exist.

The current Ethereum rows are exact for positions discovered through the canonical Uniswap position managers, but the set is not complete:

- DOLO/USDC v3 positions account for about 99.1% of pool liquidity.
- DOLO/USD1 v4 positions account for about 79.4%.
- DOLO/USDC v4 positions account for about 81.2%.
- DOLO/ETH v4 positions account for about 41.0%.
- DOLO/ETH has an active non-canonical `ModifyLiquidity` sender, `0x536e4b123e3b7e8e2576e400f9ec3988099afbd0`, which the current canonical-sender filter omits.

Pool liquidity and attributed position value are not assumed to be identical. Uncollected fees, idle balances and non-canonical managers are audited separately; a residual is never assigned to a wallet merely to force equality.

## Data design

### Kodiak Island discovery and attribution

1. Discover every Island created for a registered Kodiak pool from the official Island factory's `IslandCreated` logs.
2. Validate every discovered Island on-chain by checking its pool and token addresses.
3. Read the Island's current underlying token balances and exact ERC-20 share supply.
4. Reconstruct current share balances from `Transfer` logs and reconcile their sum to `totalSupply`.
5. Allocate the Island's current underlying DOLO and paired asset to share holders using exact integer arithmetic and deterministic remainder handling.
6. Resolve supported farm or custody contracts to beneficial owners only when the complete custody path is proven.
7. When the final wallet cannot be proven, keep the amount visible as an aggregate `Kodiak Island custody` row. Display the contract address and mark the ownership path `unresolved`; never promote it to `verified`.

The underlying Island-owned concentrated-liquidity NFT must not also appear as a direct active position after Island-share allocation. This suppression is keyed to the validated Island ownership path, not to an address label, so the same liquidity cannot be counted twice.

### Ethereum completeness

1. Continue reconciling canonical Uniswap v3/v4 NFT positions through their official position managers and current `ownerOf` state.
2. Audit every registered Uniswap v4 pool for active `ModifyLiquidity` senders, rather than filtering discovery to the canonical Position Manager at query time.
3. Reconcile canonical senders exactly as today.
4. For non-canonical senders, reconstruct the current position key and amount. Resolve a beneficial owner only from an exact manager/vault ownership path; otherwise expose an aggregate custody row.
5. Preserve explicit partial status for unavailable history or unresolved custody. Current ownership verification and historical completeness remain separate claims.

### Coverage and failure behavior

For every pool, the artifact records:

- current pool liquidity in USD;
- attributed active-position value;
- verified-wallet value;
- unresolved-custody value;
- coverage percentage where both values are available;
- an explicit reason for any residual that cannot be classified.

Coverage is a diagnostic, not a balancing mechanism. The generator must fail closed on invalid Island metadata, share-supply mismatch, negative balances, duplicate allocation, invalid decimals or a material unexplained regression from the previous artifact. A transient RPC failure preserves the prior audited source instead of publishing a fabricated or empty refresh.

## Table UX

### Header summary

The four summary metrics keep their existing meaning—Active Liquidity, LP Wallets, Active Positions and Out of Range—but adopt the exact visual hierarchy used by `Fresh 10K+ DOLO Wallets`:

- four equal rail segments on desktop;
- 24 px tabular metric values;
- icon and uppercase label above each value;
- a short muted sublabel below each value;
- the same padding, separators, background and responsive stacking.

`LP Wallets` counts only proven beneficial owners. Unresolved custody contracts remain visible in the table but do not inflate the wallet count. The sublabel states the verified-owner scope.

### Filters

When Chain is not `All chains`, its button displays the same independent clear `×` control used by Pair and DEX. Activating the clear control:

- resets Chain to `All chains`;
- refreshes dependent Pair and DEX options;
- resets pagination;
- does not open the dropdown;
- remains keyboard accessible and has a descriptive accessible label.

### Column geometry

The active table retains eight fixed columns:

1. Chain
2. Address
3. Pair
4. Price Range
5. DOLO
6. Paired Asset
7. Value
8. Details

The first four form the left-aligned identity/context group. DOLO, Paired Asset and Value are right-aligned tabular numbers; Details remains centered. Fixed widths and the existing ten-row stable viewport prevent long addresses, bounds or amounts from resizing the table. Narrow screens retain horizontal table scrolling without page-level overflow.

`Value` uses the same muted blue semantic colour as the Price value in `Latest oDOLO Exercises`, while zero/unavailable states remain muted. Colour does not replace the currency symbol or numeric value.

### Price bounds details

Price bounds become a compact two-value presentation:

- `Lower bound` and `Upper bound` are visually distinct labels;
- ordinary small values use readable fixed decimal notation, for example `0.000999`, rather than `9.99e−4`;
- protocol edges read `Protocol minimum` or `Protocol maximum`;
- the unit, for example `WBERA per DOLO`, appears once below the pair of values;
- the exact unrounded decimal remains available through the existing tooltip/focus interaction.

The presentation remains compact inside the expanded details grid and does not add a nested outer card.

### Custody rows

An unresolved Island or manager row uses:

- white label `Kodiak Island custody` or the verified protocol-manager name;
- muted contract address below it with Copy and explorer actions;
- normal position/value columns so material liquidity remains visible;
- an explicit `Custody unresolved` explanation in Details describing why no final wallet is claimed.

No guessed wallet label, synthetic owner or `Verified` status is permitted.

## Verification contract

Data tests must prove:

- Island discovery accepts only registered-pool Islands;
- share balances reconcile exactly to total supply;
- allocated token amounts reconcile exactly to the Island underlying balances;
- an Island NFT and its distributed shares cannot both enter the active total;
- supported custody resolves only with complete evidence;
- unresolved custody remains visible and non-verified;
- non-canonical Uniswap v4 liquidity is discovered;
- generated coverage metadata reconciles with the rendered summary inputs;
- refresh failure preserves the prior audited artifact.

UI tests must prove:

- the Chain clear control has the same state and event behavior as Pair/DEX clear;
- the Fresh-wallet summary hierarchy is reused at desktop and narrow widths;
- the first four headers/cells are left aligned and the numeric group is right aligned;
- Value has the required computed blue colour;
- `0.000999` renders without scientific notation while exact precision remains accessible;
- expanded custody details are understandable and do not overflow;
- the table keeps stable row geometry and no page-level overflow at 1440×900, 1024×768 and 390×844.

## Deployment

After targeted data, syntax, contract and browser checks pass, commit the implementation intentionally, update generated liquidity data with the repaired adapter, push the production branch served by GitHub Pages, and verify the live deployment rather than assuming a branch push is sufficient.
