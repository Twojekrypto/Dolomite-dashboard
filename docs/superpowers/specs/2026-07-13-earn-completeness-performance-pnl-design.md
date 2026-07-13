# EARN Completeness, Performance, And Historical P&L Design

## Goal

Make EARN canonical data converge, reduce the dedicated EARN route payload,
validate representative wallets on every deployment, and publish a separate
historical yield P&L that does not change with today's token prices.

## Canonical Coverage And Ledgers

Arbitrum and Berachain currently refresh existing histories but exclude wallets
without a canonical history. Their scheduled selectors must admit a bounded
missing-wallet cohort on every run while retaining enough capacity for head
freshness. The same incremental plan and cache remain the single source of
truth, avoiding concurrent writers with independent cursors.

Canonical histories remain workflow/cache data. Public ledgers are materialized
as two-character address-prefix shards with a manifest. The browser first tries
the existing per-address file for compatibility, then the matching shard. This
publishes complete chain coverage without tens of thousands of HTTP objects.

## Dedicated EARN Bundle

`build_earn_bundle.py` extracts the EARN view and EARN JavaScript namespace from
the shared dashboard sources. The generated `earn/earn-core.html` contains only
the EARN DOM and required overlays. `earn/earn-core.js` contains the EARN
namespace, shared tooltip behavior, and a small explicit prelude for the few
shared helpers it uses.

The generator has a `--check` mode. CI fails when the generated files drift from
the shared source, so the split cannot silently become stale.

## Liquidation Risk Shards

`fetch_liquidation_risk.py` continues writing the complete root payload for the
liquidation monitor. It additionally writes:

- `data/liquidation-risk/manifest.json`, containing metadata and shard indexes,
- `data/liquidation-risk/{chain}/{prefix}.json`, containing positions whose
  effective wallet address starts with the two-character prefix.

EARN loads one chain/address shard only after current debt is detected. Portfolio
loads one matching shard per selected chain. A missing shard is an empty result;
a malformed manifest or shard remains a visible data error.

## Representative Deployment Audit

The latest snapshot supplies deterministic representative wallets for every
published chain/market: the largest positive position and, when available, a
second deterministic position. The audit checks snapshot metadata, ledger
presence, numeric fields, strict-status coherence, and historical P&L coverage.

The generated report is committed for inspection. The Pages workflow rebuilds
and validates it on every deployment. Missing market coverage, malformed data,
or an impossible `verified` state blocks deployment; partial canonical quality
is reported as a warning rather than relabeled as verified.

## Historical Yield P&L

Historical P&L values accrued yield, not principal deposits/withdrawals and not
capital appreciation. For each consecutive daily snapshot pair:

1. require the wallet and market in both snapshots,
2. require positive supply principal and unchanged `par`,
3. calculate token yield as `current wei - previous wei`,
4. value that interval with the token's historical USD price for the current
   snapshot date,
5. skip and count intervals with principal changes or missing prices.

Stablecoin symbols may use an explicit one-dollar fallback when no historical
price is available. Other assets never use today's price as a substitute.

Each ledger market publishes the historical USD value, eligible/priced/skipped
interval counts, valuation status, and methodology. EARN shows this as a
separate `Historical Yield P&L` KPI. Existing `Total Yield Earned` remains a
token-yield total valued at current prices.

## Success Criteria

- Scheduled Arbitrum/Berachain runs process missing histories and freshen heads.
- Public ledgers and liquidation risk expose deterministic shard manifests.
- `/earn/` no longer loads the full dashboard DOM or JavaScript bundle.
- Every active chain/market has deployment-time representative audit coverage.
- Historical P&L never counts a known principal-change interval as yield.
- Generated-artifact drift, malformed shards, and impossible verification states
  fail tests or deployment.
