# Historical borrow-position P&L

`fetch_borrow_position_pnl.py` writes `borrow_position_pnl.json`. This is separate
from `build_borrow_earnings.py`, its APR ranking, and the risk refresh.

## Accounting

A position is `(chain, raw contract owner, decimal uint256 account number)`.
`effectiveAddress` is attribution only. Equity is unadjusted collateral minus
debt at Dolomite's historical oracle marks, including accrued interest. P&L is
closing equity minus opening equity minus signed external capital. Deposits and
external repayments add capital; withdrawals and borrow proceeds remove it.
Transfers across raw positions are external on each side, even with the same
owner. Trades, internal collateral-funded repayments, liquidation and vaporization
are economic activity. Router-dependent ambiguous capital legs are unavailable.
Gas, unobserved external activity, rewards, points and projected APR are excluded.
This is marked P&L, including unrealized changes, not tax or realized profit.

Classification inspects the complete transaction at the **raw owner plus account
number** boundary. Deposits without withdrawals into that position prove
external capital, including third-party debt repayment. Withdrawals without
deposits prove external outflow, including borrow proceeds to another recipient.
A router address alone is neither a rejection nor an allowlist. Opposite
directions in the same raw position remain unavailable without a proved route,
even if both route addresses equal the owner: an internal collateral swap and
its fees must not become fresh capital. Distinct raw subaccounts are never
wallet-netted.

All valuation arithmetic uses Python integers: signed raw token Wei times the
raw protocol oracle quote is USD with 36 decimals. Graph normalized Par converts
exactly to raw token principal. `Interest.parToWei` rounds half-up on **both**
supply and borrow. Pinned `getMarketCurrentIndex` supplies each market's index.
Legacy Arbitrum `getAccountValues` uses cached indices, so it is not used as a
current-interest NAV. Arbitrum `getAccountBalances` does use current indices;
its signed Par and Wei are checked independently in the published audit sample.
Ethereum/Berachain getter values additionally reconcile to the current valuation.

TWR geometrically links performance between external cash flows. For active
accounts, action-order Par replay and `LogOperation` / `LogIndexUpdate` /
`LogOraclePrice` identify exact operation-local valuations of **every** held
market before and after each flow. Missing interim marks, ambiguous routes,
nonpositive capital, a reopened zero balance, or any Wei/Par rounding residue
make the return unavailable independently of P&L. This version deliberately does
not approximate intra-block returns. No-flow positions use their exact endpoint
ratio. Rational numerator/denominator retain exact return; `pctExact` is the
rational percentage truncated to 36 decimal places for presentation.

Example: 1,000,000 opening equity grows to 1,050,000, then 1,045,000 is withdrawn.
Closing equity is 5,000, net inflow is -1,045,000, P&L is +50,000, and TWR is +5%.

## Coverage and evidence

Opening/closing blocks are pinned independently per chain at a shared timestamp,
with confirmation buffers, and opening is the last block at/before closing minus
30 days. Events are opening-exclusive and closing-inclusive. Final block-hash and
Graph deployment checks prevent publishing during a detected reorganization.
Full deterministic Graph cursor pagination discovers opening and closing
borrowers. A full raw balance-event scan discovers in-window and closed accounts.
Full Graph event pagination is independently compared by transaction/log identity
to the raw logs; any unmatched event or range gap aborts the chain. Full account
counterpart legs are retained and end Par is reconciled to pinned Graph balances.
No top-N/current-size cap is used. Accounts that never borrowed are counted and
excluded; all discovered borrowers remain in the snapshot with independent
metric statuses, including closed and unavailable rows.

Endpoint proof is complete Graph principal plus pinned RPC index/quote, with
complete event-range and ending-Par reconciliation. It is **not** a claim that
all individual accounts received independent RPC balance calls. The manifest
contains deterministic positive/negative/reduced/closed account getter audits
where such cases exist, and each audited row explicitly identifies this. Full raw
request/response hashes live in the immutable external cache; the public file
retains hash references, coverage counts, boundaries, reasons and audit results.

## Public schema v1

Top-level fields:

- `schemaVersion: 1`, `methodology: "dolomite-oracle-marked-equity-v1"`
- `generatedAt`: UTC ISO generation time; `sourceAsOf`: earliest closing Unix time
- `windowSeconds: 2592000`, `maxAgeSeconds: 21600`
- `chains`: `ethereum`, `arbitrum`, `berachain`; each contains `opening`, `closing`
  headers (`number`, `hash`, `timestamp`), coverage, valuation, source hashes and
  failures. `coverage.complete` must be true for all three.
- `summary`: positions, verifiedPnl, verifiedReturn, unavailablePnl, closedPositions
- `positions`: full borrower union, independent of sorting or dust controls

Each position:

```json
{
  "chain": "ethereum",
  "chainId": 1,
  "address": "0x<raw-owner>",
  "effectiveAddress": "0x<attribution>",
  "accountId": "0x<raw-owner>-<account-number>",
  "accountNumber": "<full-uint256-decimal>",
  "closed": false,
  "openingCollateralUsdExact": "1000000",
  "openingDebtUsdExact": "0",
  "closingCollateralUsdExact": "5000",
  "closingDebtUsdExact": "0",
  "openingEquityUsdExact": "1000000",
  "closingEquityUsdExact": "5000",
  "netInflowUsdExact": "-1045000",
  "totalInflowUsdExact": "0",
  "totalOutflowUsdExact": "1045000",
  "historicalSizeUsdExact": "1050000",
  "currentSizeUsdExact": "5000",
  "historicalSizeMethod": "max_observed_boundary_operation_gross_exposure_or_verified_flow",
  "openingBalances": [],
  "closingBalances": [],
  "tokens": ["WETH", "USDC"],
  "eventCount": 1,
  "externalFlowCount": 1,
  "returnProof": ["<ordered operation-flow proof; fields specified below>"],
  "pnl": {"status": "verified", "valueUsdExact": "50000", "reasonCodes": []},
  "return": {
    "status": "verified", "pctExact": "5",
    "ratioNumerator": "1", "ratioDenominator": "20", "reasonCodes": []
  },
  "provenance": {
    "endpointMethod": "graph_par_rpc_current_index_oracle",
    "eventRangeComplete": true,
    "endingParReconciled": true,
    "flowMethod": "operation_oracle_transaction_boundary_or_position_transfer",
    "boundaryGetterAudited": false
  }
}
```

The above numbers illustrate the regression, not a production account. The proof
placeholder abbreviates a real list of objects. Every row requires `returnProof`;
it is empty for no-flow or unavailable returns. For a verified active return,
one ordered entry per external flow contains:

- `blockNumber`, `transactionIndex`, `logIndex`: integer event order within the
  chain's opening-exclusive, closing-inclusive range.
- `eventRef`: transaction hash plus decimal log index; `operationRef`: the same
  transaction hash plus the preceding `LogOperation` hex log index.
- `marketId`, `deltaWeiRawSigned`, `oraclePriceRaw`: exact decimal strings.
- `signedUsdExact`, `preNavUsdExact`, `postNavUsdExact`: exact cash flow and
  same-mark pre/post equity, in USD decimal strings.

The strict gate verifies raw Wei times quote equals flow USD36, pre/post NAV
matches the flow, the proof count and totals match capital summaries, and the
geometrically linked rational equals the published return. No-flow ratios are
recomputed directly from endpoints. Nonpositive opening capital cannot receive
a verified percentage. The nonempty-proof count varies with the source window.

Every chain is checked independently for identity, block/hash framing, cutoff
window, next-opening-block proof, freshness, source hashes and coverage counts.
Required row fields, signed financial totals, identities and every summary count
are validated before replacement and again before publication.
Public balance arrays contain `{marketId,address,symbol,decimals,usdExact}`.
`provenance.boundaryBalancesHash` commits to the full opening/closing endpoint
records, including signed Par, signed Wei and raw oracle quote. Those records
are reproducible from the retained Graph/RPC inputs without loading raw units in
the browser. All financial values and raw amounts are
strings; unavailable financial values are `null`, never zero. `status` is
`verified` or `unavailable`; unavailable metrics always carry `reasonCodes`.
The view should distinguish absent/failed fetching from an expired snapshot and
from individual unavailable metrics. Use source boundary age, not generation
age alone. Hide expired rows after six hours even if a refresh is pending.
Current APR has its own independent freshness/availability rules.

An entirely unpriced historical size is `null`, not zero/dust.
Historical size is the maximum **observed** gross collateral plus debt at the
boundaries/fully valued operations, also bounded below by verified external flows;
it is not a continuous-time maximum. Use it for historical dust filtering so
reduced and closed positions with meaningful prior exposure remain visible.

Typical reasons: `missing_boundary_quote`, `incomplete_event_coverage`,
`ending_par_mismatch`, `ambiguous_router_capital_flow`,
`ambiguous_liquidation_route`, `missing_operation_oracle_quote`,
`missing_exact_flow_nav`, `flow_nav_mismatch`, `nonpositive_opening_equity`,
`nonpositive_interim_equity`, `zero_equity_then_activity`.

## Refresh and verification

```sh
python3 -m unittest discover -s tests -p 'test_borrow_position_pnl*.py'
python3 fetch_borrow_position_pnl.py --cache .cache/borrow-position-pnl
python3 build_borrow_position_pnl.py --validate borrow_position_pnl.json
```

The dedicated workflow runs every two hours, on manual dispatch, and on pushes
that change its producer/tests/workflow. Cache retains raw immutable sources
referenced by the current successful snapshot outside the served bundle; only
unreferenced cache keys are pruned after success. Manual and failed runs also
upload seven-day evidence artifacts after publication, including the candidate
snapshot, so publication failures also retain their run evidence. The initial retained cache was 97.8 MB
(including per-chain derived rows); raw source response hashes are validated on
reuse. This bounded policy does not promise permanent archival of every prior
refresh. A partial chain, stale source, source mismatch,
or all-unavailable result cannot replace the previous snapshot. Output replacement
is atomic after strict schema validation. Concurrent publishing fetches the latest
master, checks that producer source did not change, and applies only the generated
file to that exact parent before retrying a normal non-force push. Existing APR
and unrelated dashboard files are never written.

Official semantics: [Interest.sol](https://github.com/dolomite-exchange/dolomite-margin/blob/master/contracts/protocol/lib/Interest.sol),
[Events.sol](https://github.com/dolomite-exchange/dolomite-margin/blob/master/contracts/protocol/lib/Events.sol),
[OperationImpl.sol](https://github.com/dolomite-exchange/dolomite-margin/blob/master/contracts/protocol/impl/OperationImpl.sol),
[legacy Getters.sol](https://github.com/dolomite-exchange/dolomite-margin/blob/master/contracts/protocol/Getters.sol),
[Graph schema](https://github.com/dolomite-exchange/dolomite-subgraph/blob/master/schema.graphql).


## Transaction-boundary classification repair (2026-10-30)

An exact replay of the initial retained window keeps all 12,744 positions and
171 closed positions. Verified P&L increases from 12,423 to 12,671; independently
verified returns increase from 11,640 to 11,732. It verifies 253 formerly unknown
positions and conservatively withdraws verification from five mixed-direction
positions whose direct-owner check did not prove the route economics. The strict
schema, exact arithmetic, ending-Par reconciliation, full event identity/range
coverage and independent TWR requirements remain mandatory.

The remaining 73 unavailable dollar values comprise 61 Arbitrum boundary-quote
failures, six liquidation-route cases and six mixed-direction router cases.
These rows remain visible and unknown. Arbitrum market 9 is plvGLP with an
expired automation quote; market 41 is matured PT-GLP whose configured oracle
returns zero at the historical boundary and is rejected by DolomiteMargin.
Market 77 (USDG) was not yet available at opening and has no opening exposure in
these positions. Maturity or a redeemable underlying does not establish a fresh
USD quote for the wrapped claim. No stale/current quote, peg or zero replaces
missing historical evidence. Liquidator routes include protocol-fee transfers
and swaps through temporary raw subaccounts; fee/capital attribution requires
its own proof before verification.

## Initial real snapshot (2026-10-30)

The first final collection covers September 30 to October 30, approximately
10:22 UTC, with 12,744 borrower positions and 171 closed positions across
Ethereum, Arbitrum and Berachain. All 10,091 balance-event identities reconcile
between full Graph pages and 341 contiguous RPC shards. 12,423 positions have
verified P&L, 11,640 have verified returns (including 69 with external flows).
321 P&L values remain unavailable, mainly ambiguous router capital flows and
expired historical Arbitrum oracles. Overlapping reasons are not additive.

Independent ABI decoding and 120-digit Decimal accounting reproduced these
actual examples from retained getter responses, operation logs and oracle marks:

| Case | Chain / raw owner | Opening equity USD | Closing equity USD | Net inflow USD | Marked P&L USD |
|---|---|---:|---:|---:|---:|
| Positive | ETH / `0xffc1bfbd3775462cbb7a1915169d2dee82489904` | 3.3446002443025546389448199 | 3.35228573928625356901171776 | 0 | 0.00768549498369893006689786 |
| Negative | ETH / `0x0034a39e3454452be2cdd8d64fc6ce41629b459f` | 16.5626990255556094476797663 | 11.94074516941388579547115384 | 0 | -4.62195385614172365220861246 |
| Reduced | BERA / `0x0c2129feaee1ba1994f26ecfa6059d5edb8fadb5` | 274.24629038971920673671503259 | 0.01537203237005006854098271 | -241.1761089700774 | -33.05480938727175666817404988 |
| Closed | ETH / `0x2b7b6bedfae21b0cb46b73633192f6577b733026` | 4556.09814265744771809863871408 | 0 | -4500.98181753029499935177738222 | -55.11632512715271874686133186 |

These are concrete verification examples, not representative portfolio statistics.
The reduced example's percentage remains unavailable; the closed example has
verified TWR -1.209726467722771009825685810652327367%. The test suite embeds an
additional real pinned Arbitrum account fixture with raw getter responses,
principals, exact indices and quotes, including the cached/current index mismatch.
