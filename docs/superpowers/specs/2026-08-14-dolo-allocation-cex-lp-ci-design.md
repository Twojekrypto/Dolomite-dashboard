# DOLO allocation, CEX breakdown, LP formatting and Early Exits CI design

## Goal

Complete the previously approved DOLO analytics work without mixing allocation wallets with freely circulating market wallets, improve the readability of DOLO Liquidity Providers, and repair the semantic validation failure from GitHub Actions run `31807029375`.

## Holder distribution and allocations

- Keep `DOLO Holder Distribution Over Time` limited to market-wallet cohorts.
- Keep Core Team and Investor allocations in the existing separate allocation history card; do not overlay or add them to market-holder totals.
- Verify that the card exposes the current allocation-wallet count and total from generated data rather than a hard-coded display value.
- Preserve verified CEX labels and classification.

## CEX drilldown

- Extend the existing `CEX Supply Over Time` card with an on-demand breakdown instead of creating another chart.
- Aggregate verified CEX wallets by canonical exchange label.
- For the currently selected chart range, show each exchange's current DOLO balance and signed net change. Positive change means the exchange balance increased; negative change means it decreased.
- Allow an exchange row to expand to its constituent labeled addresses when more detail is available.
- Keep the summary compact, sortable/readable on desktop, and horizontally safe or stacked on narrow screens.
- Do not classify new wallets as CEX from behavior alone.

## DOLO Liquidity Providers formatting

- Compact large DOLO values adaptively so examples such as `731,333… DOLO` render as `731K`, not `731.…`.
- Retain exact values in the existing details/tooltip surface.
- Prevent the visible compact value from receiving ellipsis while preserving fixed table geometry.
- Verify the reported wallet `0x229eabdaebd158b998d2f50280b4c9e0853f08ae` and representative larger values.

## Early Exits workflow repair

- Keep the strict schema, coverage and reconciliation checks for `early_exits.json`.
- Treat the checked-in, fully reconciled artifact as the incremental checkpoint when the new cache schema has no complete historical cache.
- Fetch only blocks after the checkpoint's audited `coverage.toBlock`, reconcile every new Withdraw event, and merge the exact counts/raw totals and early-exit rows into the checkpoint.
- Never replace the 9,063-event audited history with a recent-only scan. A regression must prove that a checkpoint plus three new normal withdrawals produces 9,066 total withdrawals and preserves every historical early-exit row.
- Remove the obsolete Actions cache bootstrap after the artifact checkpoint path is active; a cache miss must not trigger an unreliable full-history public-RPC scan.
- Keep the 1,000-byte size guard as defense in depth. Incomplete, stale-only, regressing, or unreconciled output must still fail closed.
- Run the affected workflow after deployment and verify that the validation job completes.

## Verification and deployment

- Run focused JavaScript/Python tests and syntax checks.
- Serve the static site over HTTP and verify the DOLO page at desktop and mobile sizes using computed layout/bounding-box evidence.
- Confirm Team/Investor remains separate, CEX details follow the selected range, and LP cells no longer clip.
- Cache-bust changed static assets if applicable.
- Commit only scoped changes, push to `master`, verify GitHub Actions and the live GitHub Pages site.
