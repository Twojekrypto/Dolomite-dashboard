# TVL Supply Pool Health and Asset Activity UX

## Goal

Move `Supply Pool Health` from the Supply route to the TVL route directly below
`Token Composition`, make the table easier to scan and filter, and simplify the
Supply route's `Asset Activity` summary.

## Supply Pool Health

- The card lives only on the TVL route, immediately below `Token Composition`.
- The card keeps the current data source, sorting, expandable detail rows,
  pagination, freshness indicator, and stable ten-row viewport.
- Primary row values use the same white foreground treatment as `DOLO Holders`.
  Secondary labels and supporting details remain muted.
- The toolbar adds:
  - an asset search covering symbol, asset name, token identifier, and chain;
  - an independent multi-select network filter, defaulting to all networks.
- Search and network changes reset pagination and close an expanded row.
- The search clear control is visible, keeps focus, and triggers the normal
  input event.
- Filtering never changes the table viewport height. Missing rows are rendered
  as non-interactive spacers.
- The empty state clearly states that no assets match the current filters.

The filter is intentionally independent from the `Token Composition` chain
filter so that exploring one table does not unexpectedly change the other.

## Asset Activity

- Remove the `Net Flow · 30D` headline metric.
- Keep four metrics: `Deposits`, `Withdrawals`, `Transfers`, and
  `Active Wallets`.
- Present them as a four-column metric rail matching the visual language of
  `Selected Market · USD1`: transparent background, balanced spacing, vertical
  separators, compact uppercase labels, and prominent values.
- Preserve useful semantic colors for directional activity while keeping the
  layout and typography consistent with `Selected Market`.
- The existing period selector continues to control all four metrics.

## Verification

- Contract tests confirm that Supply Pool Health is mounted on TVL and no
  longer mounted on Supply.
- Browser verification checks card order, white row values, search, network
  filtering, stable table height, pagination, and expandable rows.
- Browser verification on Supply checks that Asset Activity contains exactly
  four metrics, omits Net Flow, and matches the Selected Market metric rail.
- Production is pushed to `dolomite-dashboard/master` and verified on GitHub
  Pages after deployment.
