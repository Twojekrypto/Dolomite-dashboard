# Dashboard freshness and reported DOLO LP outflow audit — 2026-09-19

## Conclusion and scope

**The dashboard cannot yet be certified to keep every underlying source within 6–8 hours.** At the audit snapshot all 28 primary published artifacts were younger than 8 hours, but some source histories inside those artifacts were substantially older. A successful checkpoint-saving workflow is not evidence that its source history caught up.

Read-only observations were collected on 2026-09-19, approximately 16:10–16:25 UTC (18:10–18:25 Europe/Warsaw). The final primary-artifact audit was generated at **16:19:35 UTC**. Ages below are a point-in-time observation, not a continuing guarantee.

Scope: static table/chart source artifacts across dashboard tabs, their manifests, active-chain Supply child files, source metadata and relevant Actions logs. This is a freshness/coverage audit, not a fresh mathematical reconciliation of every displayed metric or every wallet's on-demand Portfolio/History request. No historical timestamps were rewritten to make data appear fresh.

## Primary published artifacts

These are public GitHub Pages artifact ages, compared independently with raw master. Several configured Guard targets are stricter than the user's 8-hour ceiling: assets (15 minutes), price (60), TVL (30), and liquidation risk (120). Passing 8 hours does **not** mean those stricter targets passed.

| Area / source consumers | Artifact | Public age, minutes |
| --- | --- | ---: |
| Assets / Dolomite Assets | assets_live.json | 86.0 |
| DOLO headline price | dolo_price.json | 107.8 |
| TVL / composition | dolomite_tvl.json | 89.0 |
| Revenue charts and chain breakdown | dolomite_revenue.json | 85.3 |
| Borrow / Lending Positions / risk / shared Portfolio borrows | liquidation_risk.json | 107.5 |
| DOLO Liquidity Providers / shared Portfolio Liquidity | data/dolo-liquidity.json | 24.0 |
| DOLO Flows | dolo_flows.json | 201.4 |
| DOLO Holders | dolo_holders.json | 201.4 |
| oDOLO Flows | odolo_flows.json | 20.4 |
| oDOLO contract / distribution sources | odolo_contract_data.json | 17.4 |
| oDOLO exercisers / latest exercise sources | exercisers_by_address.json | 285.4 |
| oDOLO exercise value | exercised_usd.json | 287.7 |
| oDOLO average lock | avg_lock_data.json | 286.6 |
| Early Exit Analytics / Recent Early Exits | early_exits.json | 285.4 |
| veDOLO Flow / Position Activity sources | vedolo_flows.json | 14.0 |
| veDOLO headline / locked-history sources | vedolo_stats.json | 20.8 |
| veDOLO Holders | vedolo_holders.json | 20.8 |
| Expiry / Expired Ready to Claim | vedolo_expiry.json | 20.8 |
| Historical vote power | data/vedolo-vote-power-history.json | 20.8 |
| Early Unlock Simulation | vedolo_early_unlock.json | 17.4 |
| Holder Distribution / CEX historical wallet source | dolo_holder_wallet_history.json | 201.4 |
| Liquidation History | liquidation_history.json | 107.5 |
| Live / Ended Programs | rewards_programs.json | 217.0 |
| Supply Pool Health | data/supply-health/latest.json | 19.0 |
| Supply / market histories | data/supply-history/manifest.json | 282.6 |
| Supply / Asset Activity | data/supply-activity/manifest.json | 218.1 |
| Earn source freshness status | data/earn-freshness/status.json | 6.7 |
| Earn quality status | data/earn-quality/status.json | 6.4 |

Supplemental checks: total_supply_history.json, defillama_data.json and veborrow_simulation.json had same-day updates within 8 hours. The veBorrow simulation's three reported chains had status OK and recent source blocks. Earn snapshot 2026-09-19 was timestamped 11:13 UTC; Merkl's manifest was generated around 14:34 UTC, with no reported Ethereum fetch failures.

An old last event or last *closed official rebate epoch* is not by itself proof of stale collection. Quiet tables can legitimately contain old events; source/checkpoint time is the relevant freshness measure. Static wallet labels/reference allocations are not market-feed freshness timestamps.

### Supply child files

The audit fetched all manifest-listed files for the five active chains, not just their manifests:

| Chain | Activity files | History files |
| --- | ---: | ---: |
| Berachain | 48 | 44 |
| Arbitrum | 77 | 74 |
| Ethereum | 23 | 22 |
| Mantle | 16 | 16 |
| X Layer | 5 | 5 |
| **Total** | **169** | **161** |

All **330** files were present and had publication timestamps within 8 hours: activity around 12:40 UTC, history around 11:36 UTC. However, these child files do not independently establish upstream block freshness. In particular, the X Layer upstream problem below prevents certification of that chain merely from a fresh generatedAt.

Retired Botanix/Polygon zkEVM archives were not treated as active feeds. Manifest-skipped markets and every historical market/wallet shard were not independently reconciled in this audit.

## Source-level exceptions and operational gaps

### 1. Ethereum Earn canonical history: approximately 34.5 hours old

The public Earn status wrapper was fresh, but Ethereum canonical history was last updated **2026-09-18 05:22 UTC**, reporting roughly 2,070 minutes of lag. Recent Ethereum net-flow data was around 92 minutes old; that does not repair the historical ledger.

[Canonical history run 35449733748](https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/35449733748) saved a checkpoint successfully rather than completing catch-up. Its logs show:

- configured Alchemy endpoints returning monthly-capacity HTTP 429 errors;
- dRPC rejecting log ranges above 10,000 blocks on the applicable free plan;
- another public fallback returning HTTP 429;
- an explicit message that later scheduled runs will continue from cache.

The next run, 35453700219, was already in progress during inspection; no duplicate scan was dispatched. Current Guard assessment of the fresh Earn status wrapper does not substitute for inspecting each chain's canonical/net-flow lag.

**Needed:** endpoint-aware bounded log ranges and rate-limit/backoff handling, plus Guard escalation on underlying per-chain lag. Preserve checkpoints and never promote incomplete histories to Verified.

### 2. X Layer: fresh wrappers around stale/failed source coverage

The Earn status reported canonical lag of roughly **19.35 days**, and net-flow lag of roughly **205.49 days**. TVL's X Layer source metadata was frozen at block 51,513,602 / timestamp 1770282638 and listed X Layer as stale. Other active TVL chain source timestamps were recent.

**Follow-up at 16:24 UTC:** the existing background job published a new X Layer canonical checkpoint at 16:19:45 UTC. Canonical estimated lag improved to about 129 minutes, with 265/267 wallets covered. This is genuine progress, but net-flow coverage still reported a critical block backlog, and the separate TVL/claim-source problems remain. The day-equivalent Earn lag values above are estimates derived from block gaps and configured block times, not independently timestamped last-event ages.

The reward-claim index also marked X Layer **failed**, with no valid terminal timestamp and a chunk-budget warning (roughly 899,788 additional chunks at the learned range size). Fresh Supply child-file publication therefore cannot certify the underlying X Layer source.

**Needed:** repair/replace the failed source and resume bounded catch-up; expose source-level pending/stale until coverage is demonstrated. Do not substitute generatedAt for source time or fill missing activity with zero.

### 3. Cross-chain claim history: schedule cannot meet the stated ceiling

The fresh reward-claim artifact had recent Berachain coverage, but Arbitrum and Mantle source timestamps were already about **8.7 hours** old. X Layer was failed as above.

The dedicated non-Berachain workflow, [Update Cross-chain Reward Claim Events](https://github.com/Twojekrypto/Dolomite-dashboard/actions/workflows/update-reward-claim-events.yml), is scheduled **once daily** (`41 2 * * *`). Its latest inspected success was run 35429568185. Daily scheduling cannot reliably maintain a 6–8-hour ceiling.

**Needed:** a suitable incremental schedule, coverage-aware success criteria and per-chain freshness checks. More frequent dispatch alone will not repair the failed X Layer catch-up.

### 4. Guard execution gaps and R2 comparison

Dashboard Freshness Guard requests a run every 15 minutes, but inspected scheduled runs included 01:40 and 06:42 UTC on September 19: a gap of about **5 hours**. This proves the requested cron cadence was not actually achieved in that interval; the audit does not establish the GitHub scheduler's internal cause. [Inspected Guard run](https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/35449941587).

LP production now comes from R2. At the final check its public artifact was 24 minutes old, while the Git recovery baseline was 317 minutes old. The generic public-versus-raw comparator still treats stale raw master as a producer problem; once the deliberately frozen Git baseline ages out, this can request unnecessary LP refreshes.

**Needed:** monitor the production storage source rather than require the frozen LP Git baseline to remain fresh. Retain public freshness validation and integrity checks. No R2 mode, readiness variable, credential or object was changed in this audit.

### 5. oDOLO explorer rate-limit failure

[Update oDOLO Data run 35453651122](https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/35453651122) failed when generate_exercisers.py received the explorer's “Max calls per sec rate limit reached (3/sec)” response around page 50. The last good exerciser/value/lock data was still under 5 hours old, but repeated failures could break the 8-hour ceiling.

**Needed:** bounded retry/backoff and shared rate-limit discipline. This failure was diagnosed, not repaired by the small audit-tool change below.

### LP source quality caveat

The inspected fresh LP artifact had 94 active positions: 88 verified, 6 unavailable, none stale; it also reported 7 unresolved custody records. Scanners reported their latest scanned block at the observed chain head. Freshness does not prove complete beneficial-owner attribution.

## Reported wallet: the 89,300.69 DOLO outflow is swaps, not LP additions

Wallet: **0x37a38fb190345c147b7c33afb9b564d8c351c289**.

The four outbound DOLO transfer events behind the displayed 7D outflow belong to two successful Ethereum transactions:

| Transaction | DOLO sent |
| --- | ---: |
| [0x7f9f…b90b](https://etherscan.io/tx/0x7f9f9b1b8f76a12ffb06d77e4207f126a4e39c813552e913cea5048b5002b90b) | 22,325.172611448496220519 |
| [0x0642…860f](https://etherscan.io/tx/0x0642d9504e3c93117ab43c5fe4ec8a8a67b894c857d61f1bdb63f1713e33860f) | 66,975.517834345488661558 |
| **Total** | **89,300.690445793984882077** |

Each receipt contains Uniswap V3/V4 **Swap** events, including DOLO transfers to a V3 pool and the V4 PoolManager. Neither contains the LP mint/increase evidence needed to classify this as adding liquidity. A transfer to a pool is not sufficient: swaps also send tokens to pools.

Receipts were retrieved through public Ethereum JSON-RPC and transfer totals cross-checked against Blockscout's account token-transfer response. The checked-in fixture preserves both receipts' relevant public log fields and provenance. Integer raw-token arithmetic reconciles the total exactly.

The existing receipt classifier correctly returns no LP annotation for both transactions. **No misleading “→ LP” badge was added, and the outflow was not changed.** This conclusion applies to these two transactions, not to a claim that the wallet has never supplied liquidity.

## Changes shipped with this audit

- Fixed `scripts/check_dashboard_freshness.py --no-remediation`: read-only audits no longer require GH_TOKEN or instantiate the authenticated Actions API when stale rows are found.
- Added a regression test reproducing the previous `github_token_missing` failure and asserting no authenticated API request is made.
- Added a real-receipt regression fixture/test for this reported wallet: exact sum and no false LP classification.
- Added this report.

These changes do **not** repair the outstanding source pipelines listed above, change displayed balances, alter classification rules, modify timestamps or trigger backfills.

## Verification and reproduction

- `python3 scripts/check_dashboard_freshness.py --no-remediation`: successful live read-only audit after the fix.
- `python3 -m unittest tests.test_dashboard_freshness`: 25 passed.
- `python3 -m unittest tests.test_flow_tx_metadata tests.test_generate_dolo_flows_integrity tests.test_validate_dolo_flows`: 110 passed.
- `node --test tests/dolo-flow-lp-display.test.js`: 6 passed.
- `python3 -m unittest discover -s tests -p 'test_*.py'`: 1,590 tests, OK, 1 skipped.
- No UI markup/styles changed; browser rendering was not a relevant validation for this patch.

Read-only primary sources: published artifacts under https://twojekrypto.github.io/Dolomite-dashboard/, corresponding raw master artifacts and workflow code in Twojekrypto/Dolomite-dashboard, linked Actions logs, public Ethereum receipts and Blockscout transfer history. All findings are time-bound; rerun the audit before treating the recorded ages as current.
