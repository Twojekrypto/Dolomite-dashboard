# Data Freshness and Surface Consistency Design

## Goal

Make the affected TVL, Supply, DOLO, and veDOLO cards visually consistent with the existing Dolomite Assets and DOLO Holders patterns, while clarifying when Supply data was last updated. Keep all metric calculations and source data unchanged.

## Scope

### Supply Pool Health network selection

- Keep the existing multi-select behavior and available networks.
- Give each selected network the same visible gold checkbox and check icon used by the Dolomite Assets network filter.
- Keep the fix scoped to Supply Pool Health so it does not change unrelated dropdowns.
- Preserve keyboard focus, Escape dismissal, outside-click dismissal, the active-network count, and the clear-selection control.

### Supply data freshness

- Add `Data updated · <relative time>` to the upper-right area of:
  - Selected Market
  - Supplier Leaderboard
  - Asset Activity
- Use one authoritative timestamp for all three surfaces: the block timestamp returned with the Supply subgraph bundle that supplies their market data.
- While the bundle is refreshing or its timestamp is not yet available, show a subdued `Data updating` state instead of inventing a time.
- Format freshness using the dashboard's existing relative-time rules (`just now`, minutes, hours, or days) and refresh the displayed relative time without refetching data.
- Match the DOLO Holders metadata treatment: small monospaced muted copy with a gold status dot, not a pill or price-change badge.
- Keep Asset Activity's existing transaction-loading message separate from the shared data freshness metadata.

### Card surface consistency

- Give Asset Activity one solid `var(--bg-2)` graphite surface from header through rows and footer, matching the DOLO Holders card. Nested sections remain transparent and row hover feedback remains visible.
- Replace the Fresh 10K+ DOLO Wallets card gradient with the same solid `var(--bg-2)` surface used by DOLO Holders.
- Make the veDOLO Position Activity summary rail and each metric cell use one consistent background, preserving its existing separators, responsive layout, and accent treatments.

### veDOLO Position Activity units

- Display `DOLO` beside Total Locked because the value represents locked DOLO.
- Remove the `veDOLO` suffix from the primary values for oDOLO Exercises and Vote Power.
- Preserve the underlying calculations and retain `USDC per veDOLO` where it describes the Average Price metric.

## Non-goals

- No changes to market calculations, historical data, RPC calls, classification rules, or generated datasets.
- No new data source or timestamp derived from the visitor's local clock.
- No redesign of unrelated cards, tables, filters, or navigation.
- No dependency or configuration changes.

## Accessibility and responsive behavior

- The Supply Pool Health network options retain checkbox semantics and visible selected state.
- Freshness text remains readable at narrow widths and may wrap beneath the heading without overlapping controls.
- Existing mobile stacking for summary rails and Supply card headers remains intact.

## Verification

- Add focused regression contracts for the selected-network checker, shared Supply freshness labels and timestamp source, solid card surfaces, and corrected veDOLO units.
- Run the relevant JavaScript syntax checks, Node tests, Python UI-contract tests, the Earn audit suite, and `git diff --check`.
- Serve the dashboard over a local HTTP server and verify computed styles, selected-network interaction, freshness labels, card backgrounds, and units in a real browser.
- After pushing to `master`, confirm the GitHub Pages workflow succeeds and smoke-check the live TVL, Supply, DOLO, and Portfolio routes.
