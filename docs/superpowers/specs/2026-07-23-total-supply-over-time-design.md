# Total Supply Over Time — Design

## Goal

Add a `Total Supply Over Time` card immediately above `TVL Over Time` on the
TVL page. Both cards must use the same Graphite + Gold presentation, hover
tooltip, zero-based USD scale, draggable range window, handles, date label, and
mini-chart. `Total Supply` means all supplied liquidity in USD:

`Total Supply = Net TVL + Total Borrowed`

The existing `TVL Over Time` card must be corrected to show Net TVL rather than
the combined supply series.

## Current Problem

`fetch_defillama.py` currently adds the borrowed history to DeFiLlama's base
`tvl` history and publishes the result under the `tvl` key. `tvl-preview.html`
then renders that combined series under the `TVL Over Time` title. The chart is
therefore already showing Total Supply but is mislabeled, and the original Net
TVL history is discarded.

## Data Contract

`fetch_defillama.py` will publish two sorted historical arrays:

- `tvl`: DeFiLlama's base Net TVL history.
- `totalSupply`: the timestamp-aligned sum of Net TVL and borrowed history.

Both arrays use the existing point shape:

```json
{"date": 1777098059, "totalLiquidityUSD": 444727075}
```

Only finite, positive numeric timestamps and values are retained. A timestamp
present in only one source remains present; missing borrowed value is treated
as zero. The output is still written to `defillama_data.json`, so the existing
TVL workflow already stages and publishes the changed generated file.

The frontend will tolerate a temporarily old payload: when `totalSupply` is
missing it may use the legacy combined `tvl` series for the Total Supply card,
while Net TVL falls back to the embedded series until the new payload is
available. The locally generated payload will be committed with the code so the
production deployment receives both series atomically.

## Frontend Design

The new card is inserted directly above the existing TVL card. Its visible copy
is:

- Title: `Total Supply Over Time`
- Range badge: `All Time` or the selected window
- Meta: `drag the window below to zoom`

The two cards use independent brush selections. Moving or resizing the Total
Supply navigator must not alter the TVL navigator, and vice versa.

The existing single-chart code will become one reusable chart controller
configured with:

- history getter;
- main SVG, grid, line, area, hover line, hover dot, tooltip, and range badge;
- mini-chart SVG paths, overlay, selection window, dims, handles, and date
  label;
- its own `{from, to}` brush state.

This retains one implementation of geometry, hover behavior, USD formatting,
and drag logic while allowing two identical chart instances. Unique SVG
gradient IDs prevent one card from referencing the other card's definitions.

## Error Handling and Compatibility

- Histories with fewer than two valid points use the embedded fallback.
- Zero/negative/non-finite values are excluded.
- Scaling between the latest official snapshot and historical source remains
  limited to the existing 5% tolerance, independently for Net TVL and Total
  Supply. Large methodology gaps are never normalized.
- No live browser API replay is introduced.
- No new dependency, secret, configuration, or workflow is required.

## Verification

Automated tests will prove:

1. The fetcher emits distinct Net TVL and Total Supply histories from the same
   input, including timestamp alignment.
2. `buildTvlSnapshot` exposes both histories and normalizes each against its
   matching current metric.
3. Both chart cards and both independent brush contracts exist.
4. The TVL route cache version changes.
5. Existing TVL fallback, chain, token, and workflow contract tests remain
   green.

Browser verification will run through `python3 -m http.server` and confirm:

- Total Supply is above TVL;
- both charts render a line, area, tooltip, and mini-chart;
- dragging each brush changes only its own range badge/window;
- computed card widths and chart heights match at desktop and mobile widths;
- the browser console has no errors.

## Git and Deployment

Implementation is based on the latest production `master` on
`codex/total-supply-over-time`. Only scoped source, tests, generated
`defillama_data.json`, route cache version, and these design/plan documents are
committed. After verification, the commit is pushed to
`dolomite-dashboard/master`, then the Pages workflow and live `/tvl/` route are
checked.
