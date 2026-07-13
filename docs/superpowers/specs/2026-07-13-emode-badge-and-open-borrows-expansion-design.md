# E-Mode Badge And Open Borrows Expansion Design

## Goal

Make every active dashboard table that displays E-Mode use the same UX as the
Portfolio Open Borrows table, and let Portfolio Open Borrows reveal all assets
when either side of a position contains more than three tokens.

## Scope

- `portfolio-preview.html`: remains the visual reference for E-Mode and gains
  expandable asset lists in Open Borrows.
- `liquidation-preview.html`: update the active Liquidation/Borrow positions
  table E-Mode badge to match Portfolio.
- `dashboard-core.css` and `dashboard-core.js`: update both Earn Borrow
  Positions render paths to use the same full E-Mode badge.
- Route loaders for changed preview assets receive cache-busting updates.
- `liq-monitor.html` is a legacy standalone copy and is not loaded by the live
  Liquidation, Borrow, or Supply routes, so it is outside production scope.

## E-Mode Contract

All three active views use:

- the full `E-Mode` label;
- the three-layer orange/yellow/cream flame from Portfolio;
- an 18 px circular icon shell inside a 24 px pill;
- the existing Portfolio explanation in the dashboard tooltip system;
- the same restrained hover scale, glow, border, and orange palette.

The location of E-Mode does not change. Portfolio and Liquidation retain their
dedicated columns. Earn retains E-Mode beside Health Factor to preserve the
existing compact table contract.

## Open Borrows Expansion

A Portfolio Open Borrows row is expandable when `collateralTokens.length > 3`
or `debtTokens.length > 3`, because those are the cases where the collapsed
view hides data.

Collapsed rows show the first three tokens per side and a `+N` indicator for
each side with hidden tokens. Clicking or pressing Enter/Space on the row
reveals all collateral and debt tokens; repeating the action collapses it.
Expanded rows grow naturally and keep the existing table widths unchanged.

The expanded state is stored in a `Set` keyed by chain plus account identity.
It survives table rerenders caused by sort, filter, and pagination, and resets
when another wallet is loaded. Rows without hidden assets remain non-interactive.

Clicks originating from links, buttons, inputs, account copy controls, or
other interactive descendants do not toggle the row. Expandable rows expose
`tabindex="0"` and `aria-expanded`; hidden token groups expose `aria-hidden`.

## Styling

- Use the existing Liquidation expansion pattern: a pointer only on expandable
  rows, a subtle expanded background, and animated extra-token wrappers.
- Do not change the accepted Open Borrows colgroup widths or spacer position.
- Keep the collapsed row height stable; only expanded rows may grow.
- Preserve the horizontal-scroll mobile behavior already defined for Portfolio.

## Verification

Contract tests must prove:

- all active E-Mode render paths contain the shared flame geometry, full label,
  tooltip, icon shell, and matching badge dimensions;
- Portfolio keeps three-token collapsed rendering, `+N`, expansion state,
  keyboard support, interaction exclusions, and wallet-reset behavior;
- the accepted seven-column Open Borrows order, widths, spacer, skeleton, and
  colspans are unchanged;
- all changed route loaders use fresh cache-busting versions.

Browser verification uses a served local Portfolio wallet with an injected or
fixture-backed position containing at least four collateral or debt tokens, and
checks collapsed/expanded row heights, visible token counts, row interaction,
table overflow, and the E-Mode badge in Portfolio, Liquidation, and Earn.

## Rewards Copy

The Rewards UI normalizes user-facing supply campaign names from `Lend TOKEN`
to `Supply TOKEN`. Source `action: "LEND"` values remain unchanged because they
are data classifications used by the generator and renderer, not visible UX.
Already-correct `Supply ...` names and non-supply actions remain unchanged.
