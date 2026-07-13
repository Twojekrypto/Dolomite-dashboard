# EARN Completeness, Performance, And Historical P&L Plan

**Goal:** Complete the EARN data path from canonical history through compact
publication, browser loading, deployment audit, and historical-price P&L.

**Constraints:** Preserve current-price Total Yield, avoid concurrent canonical
state writers, keep strict statuses honest, and retain the full liquidation file
for existing liquidation pages.

## Tasks

- [ ] Add failing contracts for missing-wallet canonical scheduling and ledger
  shard publication; update Arbitrum and Berachain workflows and ledger builder.
- [ ] Add failing bundle extraction tests; implement `build_earn_bundle.py`,
  generated EARN assets, route wiring, and generated-drift checks.
- [ ] Add failing liquidation shard tests; generate chain/prefix shards and move
  EARN/Portfolio wallet lookups to the shard endpoint.
- [ ] Add failing representative-audit tests; generate the report and make Pages
  validate every active chain/market before deployment.
- [ ] Add failing historical-price and P&L tests; build incremental price data,
  add conservative interval valuation to ledgers, and render a separate KPI.
- [ ] Rebuild generated artifacts and run targeted tests, the full EARN audit,
  syntax checks, diff checks, and local browser computed-layout/network checks.
