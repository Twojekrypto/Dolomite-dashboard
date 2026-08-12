# DOLO Liquidity Providers Active-Only Design

## Goal

Make DOLO Liquidity Providers a focused current-state table that shows only meaningful pools and explains every displayed position without requiring protocol knowledge.

## Approved behavior

- The table has one mode: active positions. History controls and history rendering are removed.
- Pools are eligible only when their current reported liquidity is at least $10,000. Pools below that floor, or without a finite liquidity value, do not appear in rows, summaries, Pair options, or DEX options.
- `Hide dust` remains a position-level control for active positions below $10. `Low-liq pools` is removed because the $10,000 pool floor is permanent.
- Pair and DEX filters use an exclusive `All` option. `All pairs` or `All DEXes` clears individual selections; selecting one individual option while All is active starts a focused selection.
- Chain selection narrows the available Pair and DEX choices to eligible pools on that chain.
- Successful freshness metadata always uses the standard gold pulse used by DOLO Flows.

## Table presentation

- Columns remain `Chain`, `Address`, `Pair`, `Price Range`, `DOLO`, `Paired Asset`, `Value`, `Status`, and `Details`.
- DOLO and paired-asset amounts use 16px official token icons and compact notation such as `1.2K`, `4.8M`, or `0`. Full exact token quantities remain available in Details.
- Status pills remain visually restrained. Hovering any status explains its data or range meaning in plain English through the shared dashboard tooltip.
- Pagination keeps ten visual row slots so filters do not resize the table.

## Details presentation

The expanded row follows the Dolomite Assets hierarchy:

1. A gold eyebrow and plain title identify the position and pair.
2. Compact pills identify the chain, DEX/version, and exact USD position value.
3. A four-cell overview shows exact DOLO, exact paired asset, range state, and pool liquidity.
4. A labeled evidence grid explains ownership attribution, position identifier, price bounds/ticks, data quality, and pool/transaction links.

Unavailable values use plain descriptions rather than protocol-specific shorthand. The panel remains responsive as four columns on desktop, two on tablet, and one on mobile.

## Visual direction

- Preserve Graphite + Gold tokens, Inter for interface text, and JetBrains Mono for quantities.
- Reuse the Dolomite Assets expanded-row surface: quiet gold rail, transparent outer panel, compact header, dark inset information surface, and restrained metric dividers.
- The distinctive element is the two-token amount treatment: compact amounts stay scannable in the row, while the expanded panel turns them into an exact, auditable position snapshot.

## Verification

- Node contracts must cover the $10,000 fail-closed pool floor, active-only shell, exclusive DEX semantics, gold freshness pulse, status explanations, official token icons, compact amount rendering, and the Assets-style details hierarchy.
- Browser checks cover 1440x900 and 390x844, including filter behavior, stable geometry, status tooltip, expanded Details, token icons, and absence of document overflow.
