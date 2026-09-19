# Freshness repairs — 2026-09-19

Follow-up to [the source audit](2026-09-19-dashboard-freshness-and-flow-lp.md).

## Implemented

- Freshness Guard checks per-chain reward-claim coverage timestamps, Earn canonical timestamps and reported head lags, and TVL subgraph block timestamps. Fresh wrappers no longer hide these sources. Missing timestamps are invalid, not zero lag. Source lag and wrapper publication have separate limits.
- The Guard's CI invocation exits nonzero after bounded remediation if a source remains stale/invalid or publication is at least eight hours old. Failed/incomplete collection is therefore visible instead of being certified by a green wrapper-only check. Existing dispatch budgets/concurrency/cooldowns remain in force.
- Guard also runs after Pages / cross-chain claim workflow completion; it no longer relies exclusively on the delayed scheduled trigger. This is not a promise that GitHub scheduling will always meet a hard SLA.
- In R2 production mode, LP freshness uses the public production artifact, not the intentionally frozen Git recovery baseline. No storage mode, readiness digest or credential was changed.
- Non-Berachain reward claims run every four hours instead of daily and trigger Pages. The two claim-manifest writers share a concurrency group so a Berachain snapshot cannot race another chain's manifest publication.
- X Layer claims use the shared RPC endpoint resolver and receive all supported existing secret names. A bounded private checkpoint stores only fully scanned contiguous block ranges across every distributor batch. Exhausting the request/runtime budget or a failed request saves progress; only a complete scan is returned for public publication. Changed distributor sets invalidate the cached coverage. A moving head does not discard the completed prefix.
- X Layer's checkpointed scan reads every RewardClaimed event from the emitter, discovering distributors from logs rather than relying on the frozen subgraph's distributor list. Newly discovered distributors have token metadata resolved before decoding/publication.
- X Layer netflow/canonical scans try configured RPCs before public fallbacks. Dedicated providers can serve up to 10,000-block chunks; both official public endpoints retain their 100-block cap and wider requests skip them. Chunk reduction remains fail-closed.
- Ethereum uses two existing archive fallbacks before exhausted public/free endpoints. Both returned the identical 55-log response for blocks 25,921,345–25,922,344: SHA-256 `2018bf7d31ab73341529ef1439bf73ab9c860f888b34c98c14143bb9b4213710`. Canonical scan concurrency was reduced from 12 to 3 (new-address backfill from 6 to 2); local materialization remains parallel.
- Ethereum's combined error `ranges over 10000 blocks ...` plus monthly-capacity rejection was mistakenly classified as a topic-filter problem. The regression reproduced **12 calls instead of one**. It now propagates the range rejection for adaptive chunk reduction; a one-block range failure stops rather than looping forever.
- Exercise/average-lock pagination retries the identical page with bounded backoff on transient explorer failures. It does not rotate keys on throttling, retry access/quota rejection or return a partial history after exhausted retries.

## X Layer: does it need a better RPC?

The repository currently exposes `XLAYER_RPC` and `ALCHEMY_XLAYER_RPC_ZEN`; only secret names were inspected, never their values. No QuickNode secret was present. Support for that optional name is wiring, not a newly configured provider.

On September 19 the official RPC accepted a 100-block log query but rejected 1,000 blocks with `block range greater than 100 max`. Little Dolomite activity does not make scanning tens of millions of empty blocks free. A dedicated endpoint with historical log coverage and a practical per-query range may be necessary if the currently configured providers still fail. First test the repaired incremental/checkpoint path; do not assume another free API key has a larger historical range allowance.

[Official X Layer RPC documentation](https://web3.okx.com/onchainos/dev-docs/xlayer/developer/rpc-endpoints/rpc-endpoints) notes public rate/traffic restrictions. Required capability is historical `eth_getLogs`, not necessarily archive state access for every method.

## External blocker still unresolved

The official X Layer `latest` subgraph returned block **51,513,602**, timestamp **1770282638**, deployment `Qmb8enDwc6jVQEYmwWoWJ2rmPRV7DdpaEoEqiWkjFTonwH`. The alternative version `v0.1.4` listed in Dolomite's public subgraph repository returned `subgraph name/version error` during verification. No fresh, compatible official replacement was established.

Changing RPC does **not** update a stopped third-party subgraph. This needs a repaired index / supported endpoint from Dolomite, or a separately scoped RPC-backed replacement for all fields consumed by those tables. Keep the source stale/unavailable rather than rewrite its timestamp or substitute guessed values. Existing TVL fallback presentation is unchanged.

## Verification

- Red/green regression tests cover identical-page retries, hard rejection/exhaustion, source-level lag, R2 baseline handling, claim checkpoint resume/distributor invalidation and the Ethereum query amplification.
- Final Python suite: **1,606 tests, OK (one skipped)** in 29.236 seconds; targeted checkpoint, RPC, pagination and monitor regressions pass.
- Changed workflow YAML parses successfully; Python compile and `git diff --check` pass.
- A live read-only Guard run with R2 mode correctly surfaced stale Earn/TVL sources and invalid X Layer claim coverage while classifying LP as fresh with `recovery_baseline` for Git.

No balances, yields, wallet classifications, on-chain transaction labels, historical `generatedAt` values or strict Verified rules were relaxed. Workflow completion and fresh source artifacts must still be checked after deployment; a shipped repair is not evidence that a backlog has already caught up.
