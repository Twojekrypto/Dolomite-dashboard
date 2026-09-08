# Holder range and wallet delta fixes

> Execute inline with regression tests; the existing isolated worktree is reused. No new data sources, RPC scans, or fabricated balances.

**Goal:** Fix the two issues reproduced in the 2026-09-08 holder audit.

**Architecture:** Keep the daily historical snapshots authoritative. Resolve the brush to real snapshot boundaries and display the actual duration. Separate wallet exposure changes from contributions to balance buckets. Missing sub-100K endpoint balances remain explicitly unavailable, never assumed zero.

**Scope:** `dolo-preview.html` and focused Node regression tests. Bucket totals, classification, CEX history, and the data pipeline remain unchanged.

## 1. Snapshot boundaries

- [x] Add behavioral tests: between-snapshot start must retain the preceding snapshot, selected end must use the preceding snapshot, bounds must match the points used by chart/Details, short windows keep two points, All stays unchanged.
- [x] Run `node --test tests/holder-range-wallet-change.test.js` and confirm failures.
- [x] Add a pure `holderSnapshotWindow(points, selection)` resolver. Use it for chart data and brush labels. On release commit the resolved bounds. During dragging keep cheap frame paints and defer the expensive chart render as before.
- [x] Show actual snapshot duration instead of rounding a partial day down to a nominal 24H. Expose exact UTC timestamps in the existing tooltip UX.

## 2. Wallet changes

- [x] Add behavioral tests: 924046.3993 -> 1134808.7393 must show +210762.34 in either bucket, while the 1M+ contribution remains +1134808.7393. Test downward crossings, mixed sources, genuine zero, and missing endpoint balances.
- [x] Run the failing tests before implementation.
- [x] Keep `rangeCurrentTotal`/`rangeBaselineTotal` for group accounting; compute `rangeChange` from actual endpoint rows. Unknown endpoint rows get explicit unavailable metadata, never an invented zero.
- [x] Make Details explain that wallet changes differ from bucket migration contributions; put unavailable numeric values last when sorting.

## Verification

- [x] Run focused and existing holder tests, inline JavaScript syntax checks, and the data validators.
- [x] Serve the existing worktree with `python3 -m http.server`; test local /dolo/ with the frozen public artifacts from the audit.
- [x] Check desktop/laptop/mobile, brush drag/release, date labels, changed/unknown wallet values, source components, and unchanged group balances.
- [x] Review correctness, source compatibility, and scope before handoff.

Results: 84 Node tests passed; 26 data checks passed. Browser checks covered 1/7/30/90/180/All windows, with and without veDOLO, at 1440px, 1024px and 390px. No page overflow or runtime errors. Independent review found no critical or important issues. The new holder regression suite also runs in GitHub Actions.

Full `npm run check:earn-audit` passed: 52 focused tests and 1416 discovered tests. The initial attempt lacked two tracked data fixtures due to sparse checkout; restoring those unchanged HEAD files resolved all three import/read errors without modifying tests or production data.
