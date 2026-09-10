# Workflow failures — 24-hour audit, 10 September 2026

Scope: runs created since 2026-09-09 06:17:49 UTC, inspected on 10 September.
Twelve failed runs were found. Cancelled/superseded runs are counted separately,
not treated as failed data calculations.

| Workflow | Failed run IDs | Confirmed cause |
| --- | --- | --- |
| Update Earn Snapshots | 34346094214, 34378592919, 34404327833, 34438852944 | `build_earn_resolved_interest_ledger` imports `rpc_client`, which requires `requests`; workflow never installed requirements. |
| Update oDOLO Data | 34379290815, 34404998582, 34439779683 | `calculate_avg_lock` received Routescan `chain not supported` for Berachain. Direct API check reproduced this. |
| Update DOLO Liquidity | 34345169275 | Missing canonical receipt in a batch; pipeline rejected degraded publication. |
| Update DOLO Liquidity | 34377921133, 34403931465, 34438162192 | Routescan Berachain unsupported; latest run also had a Kodiak subgraph read timeout. RPC logs additionally contained HTTP 429/500. |
| Repair Active EARN Strict Verification | 34413647292 | GitHub annotation: hosted runner lost communication during the Berachain job. No failure log was available. This does not establish RPC overload, OOM, or a code defect. Subsequent run 34440224625 succeeded. |

Run links: `https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/<ID>`.

## Changes prepared

- Install the existing pinned requirements before generating Earn snapshots.
- Shared Etherscan-compatible request helper: when Routescan explicitly reports
  an unsupported chain, retry the same query against Etherscan V2 with the existing
  `ETHERSCAN_API_KEY` (Berachain key as fallback when absent).
- Wire secrets into the oDOLO and liquidity workflows; include the helper in the
  liquidity sparse checkout.
- Treat API errors as errors, not end-of-pagination/zero new transactions. This
  also closes the single-block empty-log and partial-exerciser-history cases.
- Retry only missing batch receipts through the existing RPC provider rotation.
- Add bounded retry for transient Kodiak index transport failures; keep the page
  cursor unchanged and retain all validation/degraded-publication checks.
- Run regression coverage when these source files or workflows change.

No financial formula, classification rule, or generated dataset is changed.
Provider/API-plan access with production secrets still needs validation by a
post-deployment workflow run; local unit tests cannot prove that access. No new
paid service or key was purchased. Persistent upstream failure still blocks
publication instead of replacing valid data with incomplete data.

## Local verification

- 1,424 Python tests passed after restoring the test data omitted by sparse checkout.
- 119 focused explorer/oDOLO/liquidity tests passed in a fresh dependency environment.
- Two liquidity workflow Node tests passed; modified YAML parsed successfully.
- Dependency-install ordering and sparse-checkout local import coverage checked.
- Fresh virtual environment installed `requirements.txt` and imported the formerly
  failing Earn ledger builder, liquidity generator, and explorer helper successfully.
- No production workflow was rerun and no deployment was performed by this audit.
