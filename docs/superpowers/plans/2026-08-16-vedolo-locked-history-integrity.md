# veDOLO Locked History Integrity — Implementation Plan

**Goal:** Remove the false 13–15 August Locked DOLO jump, rebuild a trustworthy event ledger, and make incomplete RPC scans impossible to publish silently.

## 1. Lock the failure into tests

- Add a generator regression test where one RPC returns an empty range while another returns the real event.
- Add regression tests that reject a candidate ledger which loses already-published immutable lock/unlock events.
- Add a semantic validator test for a reconstructed active balance that materially disagrees with the active veDOLO holder snapshot.
- Add a frontend behavior test proving the chart endpoint uses active, unexpired positions rather than veDOLO contract custody.

## 2. Make RPC log scanning fail closed

- Fetch bounded block ranges accepted by the configured providers.
- Require independent confirmation before accepting an empty multi-provider range.
- Prefer any non-empty valid response over an earlier empty response.
- Raise a clear error when an empty response cannot be confirmed, preserving the previous artifact.

## 3. Add durable publication guards

- Define stable identities for lock and unlock events.
- Reject a newly generated artifact if it drops immutable events from the prior checked-in artifact.
- Add a semantic validator that reconciles the event-derived active balance with active holder positions within a narrow documented tolerance.
- Run that validator in the veDOLO flow workflow before committing generated data.

## 4. Correct the chart metric

- Keep the historical series defined as active, unexpired locked DOLO.
- Derive the current endpoint from active holder positions at the holder snapshot.
- Never splice contract custody (which includes expired but unwithdrawn DOLO) into the active-lock series.
- Advance the deployed asset cache key.

## 5. Rebuild and verify data

- Rebuild/repair `vedolo_flows.json` through the guarded generator path.
- Verify event uniqueness, totals, active-balance reconciliation, and exact recent archive-RPC balances.
- Verify the chart in a local HTTP-served browser at desktop, laptop, narrow, and mobile sizes.
- Confirm the false +5.25M discontinuity is gone and no horizontal/layout regression appears.

## 6. Publish safely

- Run targeted and full relevant tests plus syntax/data validators.
- Reconcile with the latest remote `master`, rerun the final gate, commit, and push to production.
- Watch GitHub Pages/Actions and verify the live artifact/page after deployment.
