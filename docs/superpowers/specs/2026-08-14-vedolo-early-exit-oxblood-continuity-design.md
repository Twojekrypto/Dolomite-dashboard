# veDOLO Early Exit Oxblood Continuity Design

## Objective

Refine `Early Exit Analytics` and `Recent Early Exits` so they read as one restrained, premium early-exit suite. The previous crimson treatment remains the foundation; this change closes the remaining visual break where the Recent Early Exits column header falls back to the neutral graphite table background.

## Approved direction

Use the approved **Layered Oxblood** treatment:

- retain the dashboard's graphite base and low-saturation coral-red accent;
- keep both cards dark and readable rather than turning them into warning-red panels;
- make the title area, controls, table header, body, and footer feel like layers of one continuous surface;
- reserve brighter red for small semantic accents, freshness dots, sorted state, and hover feedback.

## Surface hierarchy

- Both cards continue to share the existing `exit-suite` surface tokens.
- `Early Exit Analytics` keeps its inner analytical panels, but their borders and fills must sit naturally within the same oxblood shell.
- `Recent Early Exits` uses a clearly oxblood column-header band instead of the generic `--bg-1` table header.
- The column header is slightly more opaque than the row area and is separated with one quiet coral rule.
- The table body stays near-black with a subtle warm cast so white values remain dominant.
- The footer returns to the same family as the title surface, at lower contrast.

## CSS ownership

- The early-exit-specific selector must have enough specificity to override the generic `#exits-table th` rule without `!important`.
- The shared generic table rules remain unchanged for unrelated tables.
- Duplicate or conflicting early-exit declarations should be consolidated where practical without broad reformatting.

## Interaction and geometry

- Search, period filtering, sorting, pagination, address hover, and horizontal scrolling remain unchanged.
- Existing rounded outer corners remain visible on desktop and mobile.
- Row hover remains quiet; no full-row saturated red fill is introduced.
- The dropdown remains unclipped because the card keeps visible overflow.

## Verification

- Verify at 1440×900 and 390×844 using the local HTTP server.
- Confirm computed column-header background is not the generic `--bg-1` value.
- Confirm the two cards share the expected surface border and freshness-dot colors.
- Confirm the Recent Early Exits header, toolbar, table, and footer have no unintended neutral strip or viewport overflow.
- Advance the veDOLO route cache key after the visual change.
