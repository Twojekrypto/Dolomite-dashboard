# Workflow Stability and Borrow Freshness Design

## Goal

Remove the reproducible GitHub Actions failures from the last 24 hours without weakening data-quality gates, and make every Borrow freshness label match the compact DOLO Holders treatment. The Borrow hero 24-hour position change must show both the absolute change and its percentage.

## Workflow design

### EARN snapshot consistency

The commit helper currently compares only `data/earn-snapshots/manifest.json` before and after rebasing. A same-day snapshot can change while the manifest remains byte-for-byte identical, allowing ledgers and audit shards built from an older snapshot to overwrite current data.

The helper will compare the Git tree object for the complete `data/earn-snapshots` directory. Any manifest or payload change during rebase will trigger the existing targeted ledger, shard, and representative-audit rebuild.

### Deployment queue coalescing

Bot-authored data commits still need the explicit Monitor and Pages dispatch paths because a `GITHUB_TOKEN` push does not reliably start another push workflow. Those paths stay intact.

Before dispatching Monitor or Pages, the caller will check whether that workflow already has a queued run. Both workflows explicitly check out `master` when execution starts, so the queued run consumes the newest pending data instead of an event-time SHA. An additional duplicate dispatch is skipped. An in-progress run alone does not suppress the next queued run.

### TVL endpoint tolerance

Official metric endpoints occasionally return a transient error for a very small market. A failed market fetch will be represented as an empty/stale market result and passed through the existing strict coverage gate:

- tolerate missing official markets only while their current supply is at most 0.1% of total supply;
- fail the workflow when the missing share is larger, invalid, or all histories are unavailable;
- expose the fetch error in stale-market diagnostics.

This keeps the published aggregate conservative while avoiding a full workflow failure for immaterial endpoint noise.

## Borrow UI design

The hero, Lending Positions, and Liquidation History will share one freshness component:

- 11 px monospaced muted text;
- 6 px gap;
- 6 px gold pulse with gold glow;
- copy formatted as `Data updated · 19 min ago`;
- no dynamic green/amber/red coloring.

The 24-hour change becomes a two-line semantic badge:

- primary line: direction icon, absolute position count, and `position(s)`;
- secondary line: signed percentage and `· 24h`;
- green for growth, red for decline, neutral for no change.

For example, a move from 1,645 to 1,639 positions renders as `↓ 6 positions` and `−0.36% · 24h`.

## Verification

- Add regression tests before production edits.
- Run targeted Python and workflow contract tests.
- Run the representative EARN audit locally.
- Serve the static site with `python3 -m http.server`.
- Inspect Borrow at desktop and mobile sizes in Chromium, including computed typography, pulse dimensions, and badge layout.
- Push to production `master`, run the affected refresh workflows, and monitor Actions until the data rebuild and Pages deployment succeed.
