# Portfolio Open Borrows — Account UX

## Goal

Make the Dolomite account number easy to find in Open Borrows without repeating the already-selected wallet address in every row.

## Approved design

Open Borrows will use six columns:

`Chain | Account ID | Health Factor | E-Mode | Collateral | Debt`

The `Address` column is removed because the Portfolio page is scoped to one wallet entered above the table. `Account ID` uses a fixed-width monospace chip. Account numbers longer than six digits render as `#123…789`; six-digit and shorter values, including the valid number `0`, render in full. Its header has an inline, keyboard-focusable information icon explaining that it identifies this wallet's Dolomite subaccount on the selected chain, can contain multiple collateral and debt assets, and is not a loan ID. The shared tooltip runtime responds to both pointer hover and keyboard focus. The hover explanation and copy control retain access to the exact full number. Unknown fallback rows display an em dash.

Active E-Mode rows use a compact amber pill with a centered, high-contrast three-tone Dolomite flame. The fixed 18px circular icon tile and 24px pill height keep every badge visually aligned while preserving the existing E-Mode label and explanatory tooltip.

## Constraints

- Change only `portfolio-preview.html`, `shared-hover-tooltips.js`, their contract test, and this design and implementation plan.
- Preserve exact account-number strings, including the valid number `0`.
- Use three leading and three trailing digits for truncated account labels, with one consistent chip width across rows.
- Keep Account ID and E-Mode explanations in the existing shared body-level `data-tooltip` system; do not use native `title` bubbles.
- Do not change E-Mode data, sorting, or the active/inactive meaning; only update its label presentation.
- Keep the borrow table's fixed layout, spacer rows, and empty states structurally correct at six columns.
- Do not add dependencies, fetches, or a new interaction model.
- Verify the rendered table with real browser geometry and a real borrow wallet.

## Out of scope

- Showing account numbers for aggregated Deposited Assets.
- Changing risk, debt, collateral, filtering, sorting, or copy semantics.
