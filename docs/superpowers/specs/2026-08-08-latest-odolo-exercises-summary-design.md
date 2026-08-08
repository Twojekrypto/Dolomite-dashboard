# Latest oDOLO Exercises Summary Design

## Goal

Make **Latest oDOLO Exercises** easier to scan by adding a `14D` period and a compact activity summary that matches the visual hierarchy used above **Fresh 10K+ DOLO Wallets**.

The summary must describe the complete selected period, while search continues to narrow only the transaction rows shown below it.

## Selected approach

Add one five-field summary rail between the section header and the existing search/period toolbar:

1. `Exercises` — transaction count, with `N unique wallets` as supporting text.
2. `veDOLO Received` — total veDOLO received across qualifying exercises.
3. `USDC Paid` — total USDC paid across qualifying exercises.
4. `Avg Exercise Price` — volume-weighted price, calculated as total USDC paid divided by total veDOLO received.
5. `Avg Lock` — veDOLO-weighted lock duration in days.

This is preferred over six equal headline metrics because wallet count is supporting context for transaction count, and over period-over-period deltas because the first version should establish clear activity totals without adding comparison noise.

## Period behavior

- Add `14D` / `14 days` between `7D` and `30D` in the **Latest oDOLO Exercises** period selector.
- Keep `7D` as the default selection.
- The summary and table period filter use the same rolling cutoff based on transaction timestamps.
- The `14D` option is scoped to this section; unrelated period controls do not gain a new option as a side effect.

## Data definitions

All summary fields use the same canonical qualifying rows as the existing table: oDOLO exercises paid in USDC. Pair-based or non-USDC transactions remain excluded by the existing exercise predicate.

- `Exercises`: number of qualifying transactions in the selected period.
- `Unique wallets`: distinct normalized lowercase wallet addresses among those transactions.
- `veDOLO Received`: sum of finite, non-negative `veDOLO` amounts.
- `USDC Paid`: sum of finite, non-negative `USDC` amounts.
- `Avg Exercise Price`: `total USDC Paid / total veDOLO Received`. It is not an arithmetic mean of individual transaction prices.
- `Avg Lock`: `sum(veDOLO amount × lock days) / sum(veDOLO amount)` for rows with a finite, non-negative lock duration and positive veDOLO amount. Weighting prevents tiny exercises from distorting the headline duration.

Calculations use the unsearched period row set. Changing the search query must not change the five summary fields.

## Information hierarchy and interaction

- Preserve the existing Graphite + Gold identity and reuse the summary-rail treatment from **Fresh 10K+ DOLO Wallets**.
- Place the rail above the search and period controls so the order is: section identity, period overview, controls, table.
- Use restrained white values, muted uppercase labels, subtle green/gold accents and internal dividers rather than five independent cards.
- The `Exercises` supporting line reads `N unique wallet` or `N unique wallets` with correct singular/plural grammar.
- The count badge beside the section title represents all qualifying rows in the selected period. Search-result counts remain in the table footer/status area so the header does not conflict with the period summary.
- Period changes update the rail, header count, rows and pagination together and reset the table to page one.
- Search updates only rows, search-result status and pagination.

## Responsive layout

- Wide desktop: five equal columns in one uninterrupted rail.
- Tablet: allow a balanced wrapped grid without horizontal page overflow; labels and values must not collide.
- Mobile: stack into a compact single-column rail consistent with the Fresh 10K+ reference, then place search and period controls vertically.
- Numeric values remain single-line where practical; supporting text may wrap naturally.
- The summary must not introduce a fixed minimum width that widens the page or table container.

## Loading, empty and invalid states

- While oDOLO data is loading, show neutral placeholders in the rail instead of transient zero values.
- With no qualifying activity in a selected period, show `0` exercises, `0 unique wallets`, zero totals, and an em dash for both averages.
- Ignore malformed numeric inputs rather than rendering `NaN` or `Infinity`.
- If total veDOLO is zero, `Avg Exercise Price` is an em dash.
- If no row has both positive veDOLO and a valid lock duration, `Avg Lock` is an em dash.
- Existing table error and no-result messages remain intact.

## Scope

In scope:

- `14D` in the Latest oDOLO Exercises period dropdown.
- The five-field period summary rail.
- Supporting render/filter changes required to separate period totals from search-filtered rows.
- Focused regression tests and desktop/mobile browser verification.

Out of scope:

- Changing the underlying oDOLO exercise data pipeline.
- Changing what qualifies as a USDC exercise.
- Adding comparison-period deltas, charts or downloadable exports.
- Adding `14D` to other dashboard sections.

## Test strategy

Follow TDD with focused coverage:

1. A failing UI contract test requires the `14D` dropdown option and its 14-day cutoff mapping.
2. A failing summary contract test requires the five fields, their order and the summary rail placement before the toolbar.
3. Calculation tests cover transaction count, normalized unique wallets, summed volumes, volume-weighted price and veDOLO-weighted average lock.
4. A regression test proves search does not change the summary source set.
5. Empty and malformed data tests prove that averages render as an em dash and never as `NaN` or `Infinity`.
6. Existing oDOLO preview contract tests remain green.

## Acceptance criteria

- `14D` appears between `7D` and `30D`, filters exactly the latest rolling 14 days and does not change the default period.
- The five summary fields use the formulas above and reconcile with the visible source data for the selected period.
- Search cannot change the rail totals.
- The rail visually matches the Fresh 10K+ summary pattern and remains legible without page overflow at representative desktop, tablet and mobile widths.
- The local page has no new console errors.
- Only files required for this feature are changed, verified, committed and then deployed to production `master` after implementation approval.
