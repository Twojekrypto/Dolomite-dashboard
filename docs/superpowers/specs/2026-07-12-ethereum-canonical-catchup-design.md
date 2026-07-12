# Ethereum Canonical Coverage Catch-up

## Goal

Make the Ethereum canonical EARN backfill finish and publish instead of repeatedly discarding its incomplete scan plan.

## Evidence

- The scheduled run `29183096895` selected 80 wallets, then discarded its existing plan because its target was 2,419 blocks behind a 600-block resume budget.
- The replacement plan contained 16,318 ten-block scan tasks. It had completed 4,945 tasks when GitHub cancelled the job at its 90-minute limit.
- The workflow only builds and commits public canonical history after a complete checkpoint, so cancelled runs leave the live coverage at 80/3,087 wallets.
- The event scanner already retries a larger range with smaller adaptive chunks on RPC/range errors; its correctness does not depend on a ten-block plan task.

## Design

1. Keep the strict canonical coverage requirement and the UI warning unchanged. The fix must make data catch up, not hide its absence.
2. Configure Ethereum to resume an incomplete target for up to 28,800 blocks, matching the established Arbitrum operational pattern. This prevents a routine new head block from throwing away completed scan shards.
3. Replace the ten-block plan partition with 1,000-block tasks. The scanner's existing adaptive fallback halves an unsafe range and preserves exact event processing.
4. Increase the selected catch-up batch to 240 wallets and use 12 incremental scan/apply workers. Include known wallets with no history so the existing strict new-address backfill can close the 93-wallet coverage gap. The global event scan is shared by the selected wallets, so this improves coverage per completed scan without changing yield math.
5. Set the normal checkpoint to 1,200 polling steps so it exits and saves state before the 90-minute job timeout. A completed checkpoint still publishes the usual canonical history and verified ledger.

## Acceptance Criteria

- A normal Ethereum restart resumes a target that is a few thousand blocks old instead of rebuilding it.
- Ethereum uses 1,000-block plan tasks with the existing adaptive RPC range fallback.
- The workflow's normal checkpoint is below the runner timeout and selects 240 stale wallets.
- Known wallets without canonical history remain eligible for strict backfill.
- The watchdog dispatches the same safe catch-up inputs.
- Existing strict verification logic and coverage-warning semantics remain unchanged.
