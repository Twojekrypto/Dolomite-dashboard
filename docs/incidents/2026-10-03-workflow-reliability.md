# Workflow incident audit — 2026-10-03

## Scope and evidence

Audit window: 2026-10-02 08:42 UTC to 2026-10-03 08:42 UTC.
55 failed runs were observed; these were not 55 independent data bugs.

| Cause | Failed runs | Evidence |
| --- | ---: | --- |
| Arbitrum subgraph indexing failure | 28 | TVL 9, Assets 11, Supply History 6, Earn Snapshots 2 |
| Freshness Guard detecting delayed artifacts | 25 | Run 37109961315 reports the same blocked producers |
| Explorer burst-rate limit | 1 | oDOLO run 37010786034; also reproduced in earlier run 36972025183 |
| GitHub Pages deployment sync timeout | 1 | Run 37040403814; later deployment 37109875891 succeeded |

Representative logs:
- https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/37109988244
- https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/37109369010
- https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/37100827231
- https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/37099873944
- https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/37010786034
- https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/37040403814

## Root causes and safeguards

### 1. Upstream Arbitrum indexing error — not an RPC capacity issue

Both the configured Goldsky endpoint and the official Dolomite-domain
`dolomite-arbitrum/latest/gn` return `indexing_error`. The direct official
probe returned `hasIndexingErrors: true`, block `511108435`, timestamp
`1790976743`, deployment `QmQFGS8j19SsoPnpofntUzL8r9ahDrRktuC423YxzdKH7G`.
The official registry's old `v0.1.4` route no longer resolves and is not a
usable fallback. Switching domains does not repair the failed deployment.

The strict generators correctly refuse partial publication. Do not use
`subgraphError: allow`, remove Arbitrum, overwrite missing values with zero,
or renew timestamps on stale artifacts to turn these runs green.
An upstream reindex/redeployment or a separately validated equivalent source
is required. No such recovery is claimed by this patch.

### 2. Monitor-created retry pressure

Freshness Guard previously measured cooldown and recent-run budget from
`created_at`. A long run or rerun could fail after its entire cooldown had
already elapsed. It now uses completed-run `updated_at`, rejects malformed
or impossible completion times, and backs off consecutive failures.

Caps: Assets 120m, TVL 160m, Supply History 180m, Earn Snapshots 180m.
A successful run resets the failure streak. Existing dispatch budgets and
active-run checks remain. Scheduled producer runs are not disabled; the
change limits additional monitor dispatches, not external outages.
The 8-hour stale-data failure remains active during backoff.

### 3. Explorer burst throttling

The explorer rejected a paginated request with `3/sec` rate limiting.
Pagination is paced at 0.4 seconds, and the shared transient retry budget
is five identical-page attempts with waits of 2, 4, 8, and 16 seconds.
Retry-After remains bounded at 30 seconds. No page is skipped, no partial
history is returned, and existing output survives exhaustion. Access,
credential, and hard-quota errors are not converted into transient retries.
This pacing is per process: shared quotas across independent jobs can still
throttle, and prolonged outages remain explicit failures.

### 4. Pages sync stall

Upload succeeded, but three deploy attempts timed out in GitHub's syncing
phase. A later deployment completed in 1m42s. The artifact was about 692MB
compressed with roughly 167k entries; size could contribute, but was not
proven causal. No datasets were removed and no unproven Pages change made.

## Verification and recovery

Regression tests cover completion-based timing, capped backoff, recovery,
invalid metadata/configuration, stale alarms during cooldown, identical-page
retry, retained rows, terminal quota/access errors, and preservation of output.
The corresponding workflows run these tests before their production steps.
No balance, APR, TVL, supply, history calculation or generated artifact changed.

When the Arbitrum deployment is repaired, verify a fresh `_meta` without
indexing errors and then run the affected producers with their normal strict
validation. Confirm source freshness as well as publication timestamps;
workflow success alone is not proof of a healthy source.
