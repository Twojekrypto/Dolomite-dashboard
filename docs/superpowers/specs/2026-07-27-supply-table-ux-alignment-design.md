# Supply Table UX Alignment Design

## Goal

Align the Supply, TVL, DOLO, veDOLO, and Borrow table controls with their established dashboard references, while keeping all displayed market data unchanged.

## Scope

### Supply Pool Health network filter

- Keep `Search asset` and the network trigger as one adjacent desktop control group.
- Reuse the interaction language of the Dolomite Assets network menu: a fixed-width trigger, `All Chains` globe row, per-chain icons, checkbox-style multi-select rows, selected-count label, and an X reset shown only for an active subset.
- Keep the existing functional behavior: empty selected-chain set means all available chains; individual selections remain open for multi-select; clicking outside or Escape closes the panel; search and network changes reset the page and close expanded rows.
- Preserve the single-column toolbar layout on narrow screens.

### Lending Positions network options

- Remove `Polygon zkEVM` and `Botanix` from the Lending Positions network filter only.
- If a stale saved filter contains either retired chain, discard it and retain the remaining active selection; do not alter underlying datasets or historical rows.

### Supply table footers and Asset Activity

- Render the Supplier Leaderboard and Asset Activity footers in the DOLO Holders pattern: result range/count aligned left, page controls centered, and the total/result context aligned right when present.
- Give Asset Activity one continuous graphite surface from its header through the rows and footer, without segmented metric or nested-card backgrounds. Preserve semantic transaction badges and readable row hover state.
- Move the Asset Activity time-range dropdown to the right side of the toolbar on desktop; keep it full-width and naturally stacked on mobile.
- Remove the persistent `30D ready` copy. While a market's latest activity is loading, show an aria-live loading message such as `Loading latest 30D activity…`; after completion expose normal freshness status rather than a permanent readiness badge.

### Summary rails

- Restyle the Fresh 10K+ DOLO Wallets and veDOLO Position Activity summary metrics using the Selected Market pattern: uppercase metric label, prominent tabular value, and one muted explanatory line, divided by subtle vertical rules on desktop and stacked on mobile.
- Preserve all current metric calculations, labels, totals, and source data.

## Non-goals

- No data-pipeline, RPC, workflow, or metric-definition changes.
- No additional chains, filters, or new dashboard dependencies.
- No redesign of unrelated tables.

## Accessibility and responsive behavior

- Network menus retain a labelled trigger, `aria-expanded`, Escape dismissal, visible keyboard focus, and clear reset labels.
- Loading copy uses an aria-live region.
- At mobile widths, toolbar controls occupy the available row width and summary metrics stack without horizontal clipping.

## Verification

- Add narrow structural tests for the retired Lending Positions options, Supply footer summary markup, loading-copy behavior, and toolbar placement.
- Use a local HTTP server and browser checks to verify computed layout, filtering, pagination, loading state, and responsive widths.
- Run targeted tests plus the project audit suite before deployment. Verify GitHub Actions and both live pages after the push to `master`.
