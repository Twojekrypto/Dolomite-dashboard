# Portfolio Open Borrows — Account UX

## Goal

Make the Dolomite account number easy to find in Open Borrows without repeating the already-selected wallet address in every row.

## Approved design

Open Borrows will use six columns:

`Chain | Account | Health Factor | E-Mode | Collateral | Debt`

The `Address` column is removed because the Portfolio page is scoped to one wallet entered above the table. The `Account` column uses a fixed-width monospace chip. Account numbers longer than six digits render as `#123…789`; six-digit and shorter values, including the valid number `0`, render in full. The hover explanation and copy control retain access to the exact full number. Unknown fallback rows display an em dash.

## Constraints

- Change only `portfolio-preview.html` and its contract test, plus this design and implementation plan.
- Preserve exact account-number strings, including the valid number `0`.
- Use three leading and three trailing digits for truncated account labels, with one consistent chip width across rows.
- Keep the borrow table's fixed layout, spacer rows, and empty states structurally correct at six columns.
- Do not add dependencies, fetches, or a new interaction model.
- Verify the rendered table with real browser geometry and a real borrow wallet.

## Out of scope

- Showing account numbers for aggregated Deposited Assets.
- Changing risk, debt, collateral, filtering, sorting, or copy semantics.
