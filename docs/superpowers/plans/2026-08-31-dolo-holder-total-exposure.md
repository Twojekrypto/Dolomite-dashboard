# DOLO Holder Total Exposure Implementation Plan

## Goal

Extend `DOLO Holder Distribution Over Time` so it can chart each wallet's full
DOLO exposure: liquid wallet balance plus positive DOLO supplied inside
Dolomite, with optional veDOLO. Keep a wallet-balance-only view for comparison.

Historical values must be pinned to the historical chain block for each chart
point. Current Dolomite balances must continue to use the already validated
cross-chain snapshot produced by DOLO Flows. No current balance may be copied
backwards into historical points.

## Tasks

1. Add failing regression tests for:
   - historical Dolomite DOLO positions queried at an explicit block;
   - aggregation by effective user across subaccounts;
   - incremental snapshot cache reuse and fail-closed behavior;
   - wallet-only and total-exposure bucket views remaining distinct;
   - the default chart mode and compatible fallback for old data.
2. Extend `generate_dolo_flows.py` with a pinned-block position fetcher and a
   versioned state-cache section for daily Ethereum and Berachain snapshots.
3. Generate `total_exposure` and `total_exposure_with_vedolo` models alongside
   the existing `liquid` and `with_vedolo` models, including address-level
   detail rows and coverage metadata.
4. Add strict validation for complete chain/point coverage and latest-current
   reconciliation. Publish no partially combined historical exposure series.
5. Add a compact `Total exposure / Wallet balance` control to the holder chart.
   Default to `Total exposure` when the new generated series exists and fall
   back visibly to `Wallet balance` for legacy artifacts.
6. Verify targeted Python and Node tests, syntax, generated-data validation and
   responsive browser behavior. Commit, push to `master`, trigger the DOLO
   Flows refresh and monitor the deployment.

## Done Criteria

- Current total exposure reuses the validated `dolomite_balances` snapshot.
- Every historical point has complete Ethereum and Berachain protocol-position
  evidence or the generator fails closed.
- The cache makes normal refreshes fetch only newly missing daily snapshots.
- Wallet-balance history is unchanged and remains selectable.
- Total exposure is the default only when complete generated data is present.
- No synthetic historical use of today's Dolomite balance exists.
