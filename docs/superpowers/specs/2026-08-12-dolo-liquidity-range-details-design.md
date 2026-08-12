# DOLO Liquidity Range and Details UX Design

**Date:** 2026-08-12  
**Scope:** `DOLO Liquidity Providers` on the DOLO page only.

## Goal

Make concentrated-liquidity ranges understandable without exposing protocol-level tick math, reduce table width, and keep quality evidence available inside the expanded position details.

## Active table

The table uses eight fixed columns in this order:

1. Chain
2. Address
3. Pair
4. Price Range
5. DOLO
6. Paired Asset
7. Value
8. Details

The `Status` column is removed. `Data status` and source health remain in the expanded details and keep the existing explanatory tooltip/copy. Detail and spacer rows span exactly eight columns.

`Price Range` remains textual and restrained:

- `In range` uses the dashboard's muted positive green.
- `Out of range` uses the dashboard's muted negative red.
- `Full range` and unavailable states remain neutral.

Only the status text receives semantic colour; the row background and supporting range description remain unchanged.

## Expanded details

- Remove `Tick interval` entirely.
- Replace technically enormous near-full upper bounds such as `3.38e+38` with `Protocol maximum`.
- Keep the real lower bound and state its unit as the paired asset per DOLO.
- A true full-range position reads `Protocol minimum → Protocol maximum`.
- Rename `Exact DOLO` to `Current DOLO` and `Exact paired asset` to `Current paired asset`, because the displayed values are intentionally rounded.
- Round both current token amounts mathematically to two fractional digits with thousands separators. Example: `16,387,908.718…` becomes `16,387,908.72 DOLO`.
- Preserve precise integer arithmetic with `BigInt`; do not convert wei amounts through floating point.

The evidence grid reflows after removing Tick interval and retains ownership, position ID, price bounds, data status and links.

## Amount tooltip positioning

The compact table amount continues to expose the exact token amount on hover. The tooltip trigger moves from the entire amount-and-icon container to the visible numeric text only. This anchors the tooltip directly above values such as `0`, instead of positioning it relative to the wider cell or token icon.

## Responsive and accessibility contract

- Keep the ten-row stable viewport and fixed table layout.
- Rebalance column widths for eight columns without changing row height.
- Retain horizontal table scrolling on narrow screens while preventing page-level overflow.
- Range colour is supporting information only; the visible `In range` / `Out of range` text remains the primary signal.
- Tooltip triggers remain keyboard-focusable only where the existing shared tooltip system requires it; no native browser title tooltip is introduced.

## Verification

- Contract tests cover the eight-column schema, `colspan=8`, removal of Status/Tick interval, two-decimal BigInt rounding, semantic range classes, and numeric-only tooltip anchor.
- Browser checks cover desktop and mobile geometry, expanded details text, range computed colours, zero-amount tooltip placement, and absence of console errors.
