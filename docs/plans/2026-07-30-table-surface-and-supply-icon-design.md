# Table Surface and Supply Icon Consistency Design

## Goal

Polish the veDOLO, Liquidation History, Supply Pool Health, and Supply market
selector surfaces without changing any underlying analytics or filtering
behavior.

## Approved UX

### veDOLO Position Activity

- Use one continuous `var(--bg-2)` surface from the card title through the
  summary metrics, filters, table body, and footer.
- Remove the section-specific green gradients that currently create visible
  transitions between those regions.
- Keep the table column-header row on the darker `var(--bg-1)` surface.
- Preserve the existing green accent for status, title, active sorting, and
  interaction feedback.

### Liquidation History column rhythm

- Increase the Chain column from its current narrow width to approximately
  11%, matching the visual separation used by Lending Positions.
- Reduce Liquidated wallet to approximately 16% so Date begins close to the
  wallet without moving the monetary columns.
- Keep Date at approximately 39%, Collateral seized at 18.2%, and Debt repaid
  at 16%.
- Verify that chain and wallet contents do not overlap and that the table does
  not gain horizontal overflow beyond its existing responsive behavior.

### Supply Pool Health details

- Remove the duplicated detail header containing “Market intelligence”, asset
  identity, chain, full name, Total supply, and Quality score.
- Open the expanded row directly on the three useful analysis sections:
  Quality anatomy, Market momentum, and Supply concentration.
- Rebalance the remaining padding so the expanded content fills the available
  width without an empty top region.

### SolvBTC icon consistency

- Treat Dolomite Assets as the visual source of truth for SolvBTC artwork.
- Resolve the icon address-first and reuse the same canonical icon result in
  Supply Pool Health, Selected Market, Supplier Leaderboard, and the Supply
  market selector.
- Remove or correct any SolvBTC-specific override that produces a different
  image from Dolomite Assets.

### Supply market selector

- Remove the gold vertical pseudo-element from the left edge of the active
  market option.
- Keep the selected option understandable through its restrained background
  wash and `Selected` label.

## Verification

- Add failing contract tests before implementation for the continuous veDOLO
  surface, revised Liquidation History widths, removed duplicate detail
  header, canonical SolvBTC icon, and absent selector rail.
- Run the relevant JavaScript and Python contract suites.
- Serve the dashboard with `python3 -m http.server`.
- In a real browser, compare computed backgrounds across the veDOLO card
  regions, icon `src` values across all requested Supply surfaces, and column
  bounding boxes on desktop and mobile.
- Confirm the Supply selector’s `::before` computed `content` is absent.

## Deployment

Rebase the UI commit onto the latest production `master`, preserve generated
data updates, push the resulting commit to `master`, and verify the GitHub
Pages deployment and live UI.
