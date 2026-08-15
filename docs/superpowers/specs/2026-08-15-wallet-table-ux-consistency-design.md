# Dolomite Wallet Table UX Consistency Design

**Date:** 2026-08-15  
**Status:** Approved by the user  
**Primary references:** `DOLO Holders` and `DOLO Flows`

## Goal

Unify wallet identity, transaction metadata, rank columns, typography, table headers, and sorting presentation across the DOLO, oDOLO, and veDOLO wallet tables without changing protocol data sources, flow values, calculations, or default sort semantics.

The only approved output-schema extension is exact last-transaction metadata for aggregate DOLO and oDOLO flow rows. It must not affect the numbers used to rank or display flows.

## Canonical wallet identity source

`dolo-address-labels.js` remains the only canonical static wallet-label source. Dynamic vesting labels continue to enter through `loadDoloVestingLabels`. No page may add another address-to-name mapping.

A small shared resolver may be exposed from the existing label module. It must:

- normalize addresses to lowercase for lookup;
- prefer the canonical shared label over page-local or generated fallbacks;
- return an explicit `known` state;
- preserve the existing verified type/badge metadata;
- return no synthetic name for an unknown wallet.

Known wallet presentation:

1. wallet name in primary white text;
2. shortened address with copy and DeBank actions;
3. optional transaction metadata in secondary muted text.

Unknown wallet presentation:

1. shortened address with copy and DeBank actions;
2. optional transaction metadata in secondary muted text;
3. no synthetic `Wallet`, `EOA`, or `Smart Contract` name.

Token and market contract addresses are outside this wallet-identity contract and must not be relabeled.

## Audit baseline

The following active tables already resolve known wallet labels from the canonical source, although their markup and visual treatment are duplicated or inconsistent:

- DOLO Holders;
- DOLO Holder Distribution wallet details;
- DOLO Flows: Top Accumulators and Top Outflows;
- Fresh 10K+ DOLO Wallets;
- DOLO Liquidity Providers;
- oDOLO Claimer Breakdown;
- Latest oDOLO Exercises;
- Latest oDOLO Pair;
- Top oDOLO Exercisers;
- oDOLO Flows: Top Accumulators and Top Outflows;
- veDOLO Recent Early Exits;
- veDOLO Holders;
- veDOLO Flow: Recent DOLO Locks and Recent veDOLO Unlocks;
- Expired veDOLO Ready to Claim;
- Risk Simulator wallet results;
- Lending Positions;
- Liquidation History;
- Supplier Leaderboard;
- Asset Activity;
- Top users saved with current veDOLO.

The concrete missing/partial cases are:

- CEX Details has exchange aggregates but no address-level rows or wallet actions;
- unknown-wallet renderers often show synthetic generic labels;
- DOLO and oDOLO aggregate flow rows have no transaction date/hash metadata;
- veDOLO Flow typography and table header styling differ from DOLO Flows;
- oDOLO Flows uses a different transient sort-marker implementation;
- rank-column width/alignment differs among DOLO Holders and the three flow surfaces.

## Shared presentation primitives

Use a deliberately small shared layer rather than a full table rewrite:

- canonical wallet identity resolution;
- wallet/address typography classes;
- optional transaction metadata classes;
- a stable sortable-header marker slot;
- a compact centered rank-column class;
- shared Flow table typography/header tokens.

Page-specific badges and amount columns remain page-owned. Existing address-match hover classes and tooltip hooks must remain compatible.

## Flow transaction metadata contract

### Generated output

Each displayed aggregate DOLO/oDOLO flow row may receive:

- `latest_tx_hash` — exact 32-byte transaction hash;
- `latest_tx_timestamp` — exact block timestamp in Unix seconds;
- `latest_tx_chain` — existing chain key used to select the current explorer.

The fields are presentation metadata only. Existing `address`, `net_flow`, `balance`, gross components, transaction counts, ranking, exclusions, and sorting remain unchanged.

### Selection semantics

- Top Accumulators use the latest qualifying inbound transfer for that wallet inside the selected period and chain.
- Top Outflows/Sellers use the latest qualifying outbound transfer inside the selected period and chain.
- Mint, burn, vester, bridge-neutralization, and existing excluded-address rules remain exactly as defined by the flow calculation.

### Exact recovery without cache migration

The existing flow state stores block-level transfer tuples without hashes. Do not rewrite or migrate the large historical state cache.

After ranking:

1. identify the highest relevant block for each displayed address from the already scanned transfers;
2. group identical blocks to avoid duplicate calls;
3. fetch the token Transfer logs for that exact block;
4. select the matching direction/address log and exact transaction hash;
5. fetch/cache the exact block timestamp;
6. attach metadata to the output row.

If exact recovery fails, omit all transaction metadata for that row. Never fabricate a date, link to an address page as if it were a transaction, or reuse stale metadata from another period.

### Address-cell layout

Known wallet:

```text
Wallet Name
0x1234…abcd  [copy] [DeBank] · 11 Aug 2026 ↗
```

Unknown wallet:

```text
0x1234…abcd  [copy] [DeBank]
11 Aug 2026 ↗
```

The transaction date and external-link icon form one link target and open the exact transaction in a new tab. On narrow screens the metadata may wrap below the address actions; it must not enlarge the table's intrinsic minimum width.

The same visual contract applies to DOLO Flows, oDOLO Flows, and veDOLO Flow. veDOLO keeps its existing exact event hash and timestamp.

## Flow and holder table alignment

- Apply DOLO Flows' body font family, font size, font weight, line height, primary/secondary colors, cell padding, and row height to veDOLO Flow.
- Apply the same Flow header background, border, height, typography, and padding to all three flow surfaces.
- Use a compact rank lane (target 36 px, adjusted only if measured content requires more) in DOLO Holders and all Flow tables.
- Center the `#` heading and values on the same axis.
- Reduce the empty gap before Address without making the cells visually cramped.
- Keep a permanent sort-icon slot so inactive and active states have identical geometry.
- Use DOLO Flows' chevron direction, size, hover, active state, cursor, and `aria-sort` behavior in oDOLO Flows.
- Align DOLO Holders' label and chevron as one inline-flex control with a fixed gap and no layout shift.
- Preserve every table's current default sort field and direction.

## DOLO Holder Distribution

Remove the complete standalone `Team & Investor Allocations Over Time` card, including its period controls, chart shell, and independent render/binding path.

Use the existing holder balance history and canonical Team/Investor classifications to place each allocation wallet in the normal balance threshold that applies at each chart point:

- 1M+;
- 500K–1M;
- 100K–500K;
- 50K–100K;
- 10K–50K;
- 1K–10K.

The main distribution chart and wallet drilldown include those rows. Team and Investor badges remain visible, and tooltips disclose their wallet count and balance contribution inside each bucket. CEX, contract, potential custody/MM, and bot exclusions remain unchanged.

When the veDOLO inclusion option is active, bucket assignment must use the same liquid-plus-locked metric as the rest of that view. The current methodology mismatch guard remains in force: no synthetic `Now` point may be appended when component balances do not reconcile.

## CEX Details

Keep the compact exchange-level disclosure. Each exchange row can expand to show its address-level composition for the selected range.

Address-level rows include:

- canonical wallet name when known;
- shortened address;
- copy action;
- DeBank link;
- current/end balance;
- selected-range change when exact start/end holder snapshots are available.

Use the existing holder snapshot/history data and canonical CEX classification. Do not infer new CEX identities or add a second label mapping. If exact address-level range data is unavailable, show the current composition with an explicit unavailable range-change state rather than fabricating a delta.

## Responsive behavior

Verify at minimum:

- 1440 × 900;
- 1024 × 768;
- 768 × 1024;
- 390 × 844.

Requirements:

- no document-level horizontal overflow;
- no new avoidable table overflow caused by wallet/date metadata;
- intentional table scroll remains bounded inside the existing table wrapper;
- address and transaction metadata wrap without clipping actions;
- sort activation produces zero header geometry shift;
- rank headings and values share the same horizontal center;
- CEX nested rows remain readable and keyboard accessible.

## Testing and verification

Use test-driven changes.

Automated contracts must cover:

- canonical known/unknown wallet resolution;
- absence of synthetic names for unknown wallets;
- exact DOLO/oDOLO metadata direction and period selection;
- correct chain explorer link construction;
- fail-closed missing metadata behavior;
- unchanged flow numeric fields and ranking;
- Team/Investor bucket inclusion and unchanged CEX/potential exclusions;
- removal of the standalone allocation card;
- CEX address/copy/DeBank rows;
- stable rank and sort-marker markup;
- shared Flow table classes and cache-key updates.

Run the relevant Python and Node suites, `py_compile`, inline-script syntax parsing, data validators, `git diff --check`, and browser-computed style/bounding-box verification.

## Non-goals

- no new data source;
- no change to token-flow arithmetic;
- no change to holder balances;
- no change to classification labels or confidence rules;
- no change to default sorting semantics;
- no explorer-link destination changes outside adding exact transaction links;
- no redesign of unrelated dashboard cards;
- no broad table-framework refactor.

## Deployment

The UI must remain backward-compatible when older generated flow rows do not yet contain transaction metadata. After code deployment, run the existing DOLO and oDOLO flow workflows to generate and validate the new optional fields, then confirm the GitHub Pages deployment with cache-busted URLs.

## Approved follow-up: exact range deltas and three-line Flow metadata

The Holder Distribution legend must derive every selected-range change from the exact first and last precomputed points visible in the chart. It must never substitute a nearest 1D/7D/30D/90D/180D reconstruction, and it must never compare a `holders` endpoint against a `market` baseline that excludes Team/Investor allocations. Address-level Details may keep its separate fallback resolution only when an exact wallet snapshot is unavailable.

All three Flow surfaces use a strict three-line identity hierarchy: wallet name, address with copy/DeBank actions, then transaction date with the external-link icon. The transaction line is optional and appears only when the complete exact metadata tuple is present. It uses shared typography and does not change numeric flow data, ranking, or explorer destinations.

CEX Details keeps a three-column exchange row: exchange name, a stacked current/change amount block, and the address-count disclosure. The current amount is primary; the selected-range change sits directly below it and uses positive/negative/neutral semantic color. The exchange name is increased one restrained type step while preserving mobile containment and keyboard disclosure behavior.
