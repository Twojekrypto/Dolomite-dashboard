# veDOLO Green Surface and TVL Metadata Retry Design

## Goal

Make the `veDOLO Position Activity` section use the same restrained green
surface treatment as `Latest oDOLO Exercises` while preserving one continuous
table surface, and prevent transient incomplete subgraph metadata from
producing a TVL snapshot that fails later in the workflow.

## Scope

- Update the parent surface of `#pf-exercises-section` in
  `portfolio-preview.html`.
- Keep the section header, summary rail, filters, metrics, and footer
  transparent so the green treatment remains visually continuous.
- Keep the table header intentionally darker for column readability.
- Add bounded retry handling for incomplete GraphQL `_meta.block` data in
  `fetch_dolomite_tvl.py`.
- Add focused regression tests for both changes.
- Bump the portfolio route asset version so GitHub Pages clients receive the
  updated preview.

## Visual Design

The section parent will use the exact restrained green-to-graphite background
from `.latest-activity-section`:

```css
background:
  linear-gradient(180deg,rgba(117,184,123,.055),rgba(15,17,15,.82)),
  var(--bg-2);
```

The existing green accent rail, borders, title, pulse, controls, and badges
remain unchanged. Child surfaces stay transparent; no separate green blocks or
additional cards are introduced.

## TVL Metadata Reliability

The subgraph response is acceptable only when `_meta.block` contains:

- a positive integer `number`;
- a non-empty string `hash`;
- a positive integer `timestamp`;
- a non-empty string `deployment`.

The GraphQL fetch will retry an incomplete or transiently failing response using
the existing bounded delays `(2, 4, 8)` seconds. Token liquidity and price API
requests will run only after a complete subgraph payload is obtained, avoiding
duplicate downstream calls during metadata retries.

If all attempts return incomplete metadata, the active chain is marked failed.
The existing partial-snapshot guard then stops execution before
`dolomite_tvl.json` is written and reports the affected chain. The validator
remains strict; no previous metadata is copied forward and no field is
fabricated.

## Verification

- Unit test: an incomplete `_meta` response followed by a complete response is
  accepted after one retry.
- Unit test: persistently incomplete `_meta` exhausts retries and raises a
  chain-specific error.
- Contract tests: the veDOLO parent contains the approved green gradient while
  all child surface layers remain transparent.
- Run the focused TVL and UI contract suites.
- Serve the static site through `python3 -m http.server`, inspect the portfolio
  page in a real browser, and verify computed backgrounds and bounding boxes.
- Push the verified commit to production `master`, then confirm GitHub Actions
  and the live GitHub Pages route.

## Non-Goals

- Relaxing `validate_data.py`.
- Reusing stale metadata from a previous snapshot.
- Changing TVL calculation formulas or source selection.
- Redesigning unrelated portfolio sections or controls.
