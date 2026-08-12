# DOLO Liquidity Providers Table UX Design

Date: 2026-08-12

## Goal

Make DOLO Liquidity Providers and DOLO Flows follow the established Dolomite Assets table language while preventing extreme concentrated-liquidity ranges from changing table geometry.

## Scope

- Restructure the DOLO Liquidity Providers header and toolbar.
- Standardize sortable headers in DOLO Liquidity Providers and DOLO Flows.
- Reuse the Dolomite Assets Details control treatment in DOLO Liquidity Providers.
- Keep ten stable table slots and fixed column widths in both liquidity modes.
- Replace raw price bounds in the collapsed row with a compact range classification and move useful range evidence into Details.
- Preserve all LP calculations, ownership classification, source quality, pagination, filtering, and generated data.

## Header and Controls

The primary header row contains the title, `wallets · positions/events` count, and `Data updated` metadata. A full-width hairline separator immediately follows this row, matching DOLO Flows.

The Active positions / History segmented control sits in a dedicated sub-row directly below the separator. Its existing Holder Distribution visual language remains unchanged.

The toolbar has two desktop groups:

- Left: search, Chain, Pairs, and DEXes.
- Right: History period and action controls when applicable, followed by `Low-liq pools` at the far right.

On screens at or below 640 px, both groups become a single-column stack. Dropdown panels remain constrained to the toolbar width.

## Fixed Table Geometry

The liquidity table uses `table-layout: fixed` and a mode-specific `colgroup`. Column proportions are explicit and total 100%.

Active positions:

- Chain 9%
- Pair 13%
- Wallet 19%
- Price Range 14%
- DOLO 10%
- Paired Asset 12%
- Value 10%
- Status 7%
- Details 6%

History:

- Date 10%
- Chain 9%
- Pair 13%
- Wallet 19%
- Action 9%
- DOLO 11%
- Paired Asset 13%
- Value 10%
- Details 6%

Cell content must truncate or wrap inside its assigned column; it must never resize another column. The table continues to render ten row slots per page. Opening Details may increase card height, as explicitly selected by the user, but ordinary filtering, sorting, mode changes, and long values must not.

The desktop table keeps its existing minimum readable width and uses only the table wrapper for horizontal scrolling on narrower screens. The document itself must not overflow horizontally.

## Price Range Presentation

Collapsed active rows never render raw lower or upper price bounds.

The cell contains:

- Primary state: `In range`, `Out of range`, or `Full range`.
- Secondary classification: `Near-full range`, `Custom range`, or `Always active`.

A concentrated position is `Near-full range` when its lower and upper ticks are within one tick-spacing step of the protocol-supported minimum and maximum for that position. All other bounded positions are `Custom range`.

The Details panel adds dedicated range evidence:

- Lower bound
- Upper bound
- Tick interval

Bounds use a compact, locale-independent significant-digit formatter. Values that are very large or very small use scientific notation such as `2.95e−39`; normal values use a short decimal representation. Full raw decimal strings are not inserted into the visible table. Missing evidence displays `Unavailable` and is never inferred.

## Details Control and Panel

The Details button copies the Dolomite Assets control contract:

- 24 px gold pill
- uppercase 9 px label
- downward chevron
- gold hover ring and one-pixel lift
- `Hide` label and rotated chevron while expanded
- minimum 44 px touch target on mobile

The expanded panel retains Attribution, Position, Source quality, and Provenance, and adds the three range fields for active concentrated positions. History rows show only fields supported by their event data. The panel uses the existing responsive grid and may increase table height.

## Sortable Header Standard

DOLO Liquidity Providers and both DOLO Flows tables use the Dolomite Assets header contract:

- Inter, 10 px, weight 600
- uppercase
- 1.6 px letter spacing
- muted default color
- pointer cursor only on sortable columns
- gold active label and gold `▲` or `▼` marker
- numeric headers right-aligned

Changing sort direction updates the marker without changing column width. Non-sortable headers such as Details retain the neutral cursor and do not change color on hover.

## Accessibility and Interaction

- Sortable headers expose `aria-sort` as `ascending`, `descending`, or `none`.
- Details exposes `aria-expanded` and an action-specific accessible label.
- `Low-liq pools` remains a keyboard-operable switch and moves visually without changing its behavior.
- Focus-visible states remain visible against the Graphite + Gold palette.

## Verification

Automated contracts must cover:

- header separator and toolbar grouping;
- fixed colgroups in Active positions and History;
- compact range rendering for the burn address and `0xe90d…1e95` fixtures;
- no raw extreme bounds in collapsed rows;
- Details range evidence and Dolomite Assets button behavior;
- Dolomite Assets sort markers and `aria-sort` parity in liquidity and flows;
- ten row slots after sorting, filtering, and mode changes.

Browser verification runs at 1440×900, 1024×768, 768×1024, and 390×844. It records document overflow, table-wrapper overflow, header widths, row heights, mode changes, sort markers, dropdown placement, Details expansion, and the two reported wallet cases. Production is complete only after the GitHub Pages workflow succeeds and the live page returns the new cache version.
