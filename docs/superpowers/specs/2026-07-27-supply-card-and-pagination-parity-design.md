# Supply Card and Pagination Parity Design

## Goal

Bring the Supplier Leaderboard and Asset Activity presentation into the same visual and information hierarchy as DOLO Holders. Remove redundant history and footer controls while preserving all Supply data sources, calculations, filters, and automatic history loading.

## Scope

### Asset Activity surface

- Match the DOLO Holders card shell: one solid `var(--bg-2)` graphite surface, a subtle one-pixel border, a 22-pixel radius, and the same restrained inset/drop shadow treatment.
- Keep the header, summary, toolbar, table viewport, and footer transparent inside that shell so the card reads as one continuous surface.
- Preserve the current transaction-type badges, row hover feedback, stable table height, and mobile layout.

### Header counts and freshness

- Replace parenthesized counts with DOLO Holders-style count badges:
  - `<total> suppliers · showing <filtered>` for Supplier Leaderboard.
  - `<total> events · showing <filtered>` for Asset Activity.
- Keep `total` tied to the complete currently loaded market dataset. Update `showing` after search, type filters, or date filters without changing pagination semantics.
- Place each `Data updated · <relative time>` label in the same upper-right header position and visual treatment as DOLO Holders.
- Preserve the existing authoritative Supply bundle timestamp and the subdued `Data updating` fallback.

### Pagination footers

- Use the DOLO Holders footer hierarchy on both Supply tables:
  - visible row range and filtered total on the left, for example `1–10 of 777`;
  - first, previous, current-page, next, and last controls centered;
  - no redundant right-aligned total.
- Remove `777 wallets`, `280 events`, and equivalent right-side total labels for all result sizes.
- Keep the footer height stable when filters produce zero or fewer than one page of results.

### Activity history status

- Remove the `Load older tx` button.
- Keep automatic history loading triggered by the existing date-range behavior.
- Present the history state as quiet, borderless inline metadata beside the date-period control:
  - `Loading full history…` while the static full-history dataset is loading;
  - `Full history` when complete;
  - the existing clear unavailable/error wording if the verified static history cannot be loaded.
- Retain the existing aria-live announcement and status indicator, but remove the framed pill/button treatment.

### Selected Market divider

- Keep the single divider below the Selected Market header and subtitle.
- Remove the adjacent top border from the statistics rail so the header and rail do not render as two horizontal lines.

## Data and behavior

- Do not modify RPC calls, subgraph queries, static history generation, dataset coverage, market metrics, or freshness timestamps.
- Do not add configuration, dependencies, or generated files.
- Reuse existing filtered arrays to calculate the `showing` values and existing unfiltered loaded arrays to calculate the `total` values.
- Searching, transaction-type filtering, date filtering, sorting, pagination, and automatic full-history loading continue to work as they do now.

## Accessibility and responsive behavior

- Count badges may wrap below the title on narrow screens without colliding with freshness metadata.
- Freshness metadata follows the DOLO Holders responsive header behavior.
- History loading remains exposed through an aria-live region.
- Pager controls retain accessible disabled states and visible keyboard focus.

## Verification

- Add focused UI-contract tests for the dynamic count copy, DOLO-style footer structure, absence of redundant right-side totals, removal of the manual history button, borderless history status, and single Selected Market divider.
- Run JavaScript syntax checks and the narrow Supply UI tests, followed by the repository audit suite and `git diff --check`.
- Serve the dashboard over a local HTTP server and verify desktop and mobile computed styles, bounding boxes, search/type/date filtering, pagination, automatic full-history loading, status copy, and stable card dimensions.
- Push the verified commit to `master`, confirm the GitHub Pages deployment succeeds, then smoke-check the live Supply route with a cache-busting query.
