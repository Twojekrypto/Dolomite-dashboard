# DOLO Flow Reconciliation Design

## Scope

Correct DOLO Flow for the tracked Ethereum and Berachain networks without adding Arbitrum. The combined view must aggregate complete per-address market flows before selecting its top rows, use timestamp-exact period boundaries, keep verified Safe and Bot/MM wallets visible, and expose how much bridge cancellation came from exact adapter evidence versus the legacy heuristic.

## Data contract

- Keep the existing per-chain `eth` and `bera` leaderboards.
- Add a generated `all` leaderboard for every period. Its rows are ranked only after summing the complete Ethereum and Berachain address maps.
- Publish exact period start block/timestamp metadata for both tracked networks.
- Preserve current transfer, balance, Dolomite-position, protocol-neutralization, and sorting semantics.
- Keep Arbitrum outside this pipeline and label the combined UI option `Ethereum + Berachain`.
- Preserve confirmed user wallets with `contract_wallet_type` of `safe`, `multisig`, or `delegated_eoa` during dynamic contract exclusion.
- Preserve confirmed `bot`, `mm`, and `trader` labels so Trading bots can display them.
- Publish separate exact-adapter and legacy-heuristic bridge cancellation counts/volumes. Do not silently remove the legacy fallback in this change.

## BrownFi verification rule

Treat DeBank as a discovery signal, not an identity authority. An address can be added to the liquidity registry only after its chain, pool/pair, and protocol relationship are confirmed from on-chain contract calls/events or an authoritative protocol registry. A normal wallet holding a BrownFi LP position must not be mislabeled as the pool contract.
