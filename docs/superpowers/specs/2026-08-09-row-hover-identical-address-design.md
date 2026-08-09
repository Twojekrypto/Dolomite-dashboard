# Row Hover Identical Address Highlighting Design

**Date:** 2026-08-09

**Status:** Approved for implementation

## Goal

Make repeated-wallet activity discoverable before the pointer reaches the address text. Hovering anywhere on a supported data row should reveal exact copies of that row's wallet address in other visible rows of the same table. Hovering or keyboard-focusing the address itself keeps the existing stronger address treatment.

This is a visual relationship aid. It must not change data, sorting, filtering, pagination, links, copy actions, table geometry, existing row-hover surfaces, or risk/status colors.

## Audited Scope

The behavior remains opt-in through `data-address-match-cells` and applies to the ten existing event- or position-level tables where one wallet can legitimately occupy multiple visible rows:

- `#exits-table` — Recent Early Exits
- `#locks-table` — Recent DOLO Locks
- `#unlocks-table` — Recent veDOLO Unlocks
- `#claimable-table` — Expired veDOLO Ready to Claim
- `#tbl-latest-ex` — Latest oDOLO Exercises
- `#tbl-latest-pair` — Latest oDOLO Pairings
- `.sim-atrisk-table` — Risk Simulator wallet results
- `#positions-table` — Lending Positions
- `#liquidation-history-table` — Liquidation History
- `#supply-activity-table` — Asset Activity

Aggregate tables with at most one visible row per wallet and token/market contract tables remain excluded.

## Interaction Hierarchy

### Row hover

1. Entering a supported `tbody` data row collects every valid canonical wallet address rendered in that row.
2. For each collected address, the controller finds exact case-insensitive matches in other visible rows of the nearest opted-in table.
3. Only matching address wrappers in those other rows receive the quiet peer treatment.
4. The source row's address wrappers remain visually unchanged because the table's existing row hover already identifies the source.
5. If none of the source row's addresses has a peer in another visible row, no address-match treatment appears.
6. Leaving the row clears the row-derived peer state immediately, except when the pointer has moved onto an address trigger and the stronger address interaction is active.

### Direct address hover or focus

1. The directly hovered or keyboard-focused address receives the existing Premium Strong gold treatment.
2. Every exact visible occurrence of that canonical address in another row of the same table receives the existing quiet peer treatment.
3. This direct-address state has higher priority than row hover and replaces the row-derived state until pointer or focus leaves the address.
4. Clickable addresses keep the pointer cursor; non-clickable address text keeps the default cursor. No address uses the help cursor.

### Rows containing more than one wallet

`Asset Activity` may render a primary and secondary wallet in one row. Row hover considers both canonical addresses independently. A peer address is highlighted only when its own normalized address exactly equals one of the source row addresses; the two source addresses are never treated as matching each other.

## Matching Rules

- Matching is limited to the nearest `table[data-address-match-cells]`.
- Values normalize to lowercase and must match `0x` plus exactly 40 hexadecimal characters.
- Only displayed wallet-address wrappers participate; wallet labels and shortened text without canonical `data-full-addr` do not define identity.
- Matching is limited to currently rendered rows on the current page. There is no cross-page, cross-table, or cross-card state.
- The source row itself is excluded from the row-hover peer collection.
- Multiple wrappers for the same canonical address in one cell are deduplicated visually.
- Malformed, empty, non-EVM, hidden, and unique addresses produce no peer treatment.

## Visual Treatment

- Row-hover peers use the existing quiet gold address frame and wash (`address-match-peer`).
- Direct address hover/focus keeps the existing stronger gold text, frame, outer ring, and restrained glow (`address-match-source`).
- No class is added to related rows or cells solely to paint their background; only address wrappers change.
- The existing hover surface on the source row remains authoritative.
- The treatment adds no padding, border width, display change, or dimensions, so address, cell, row, and table bounding boxes remain stable.
- Reduced-motion mode removes transitions while preserving the static distinction.

## Shared Implementation

The behavior stays in `shared-hover-tooltips.js` and `shared-hover-tooltips.css`.

The controller will use delegated pointer/mouse events because rows are dynamically rendered, sorted, filtered, and paginated. It will maintain two explicit interaction modes:

- `row`: source row plus its set of canonical addresses and quiet peer wrappers;
- `address`: source address wrapper plus strong source and quiet peer wrappers.

Address mode wins while active. On exit, the controller reconciles against the element currently under the pointer so moving between a row and its address does not flicker or leave stale classes. Scroll, resize, route reload, mutation-driven rerender, and window blur clear stale state.

The existing inline tooltip marker on `liquidation-preview.html` remains authoritative for tooltips. The shared file continues to install only one address-matching controller regardless of route reloads.

No dependency, data file, workflow, API request, column definition, or `nth-child` selector changes are required.

## Accessibility and Touch

- Existing address `focusin` and `focusout` behavior remains equivalent to direct address hover.
- Row hover does not add tab stops or keyboard-only row semantics for this cosmetic feature.
- Touch events are not intercepted, and taps continue to trigger the existing address and row actions.
- A touch interaction must not leave a persistent row-derived match state.

## Verification

### Automated behavior checks

- A row with one repeated address highlights only matching address wrappers in other rows.
- The source row's address remains unchanged during row hover.
- Direct address hover upgrades only the source address to the strong state and keeps peers quiet.
- A row containing two wallet addresses highlights peers for either exact address.
- The same address in another opted-in table remains unchanged.
- Unique, malformed, hidden, and source-row-only duplicates do not activate peer state.
- Moving between row children does not flicker or clear the state.
- Pointer exit, focus exit, rerender, scroll, resize, and blur clear stale classes.
- The existing ten-table allowlist remains unchanged.

### Browser checks

Serve the static site with `python3 -m http.server` and inspect Borrow, Supply, veDOLO, and oDOLO routes in Chromium at desktop and phone widths. Use `getComputedStyle()` and bounding boxes to confirm:

- row hover changes only peer address wrappers;
- direct address hover produces the stronger source treatment;
- the same address in another table is unchanged;
- row backgrounds, rails, risk colors, cells, and table dimensions do not shift;
- sorting, filtering, pagination, copy, DeBank, explorer, details, and row-click actions still work;
- no horizontal overflow or stuck highlight appears after dynamic rerender.

## Done Criteria

The feature is complete when the shared controller implements the two-level interaction for all ten audited tables, targeted automated tests pass, real browser-computed styles and bounding boxes confirm address-only emphasis without layout shift, route cache keys are advanced, and the production `master` branch is pushed and verified live.
