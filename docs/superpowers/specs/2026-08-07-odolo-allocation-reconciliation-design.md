# oDOLO Allocation Reconciliation Design

## Goal

Make the oDOLO Distribution and Claimer Behavior charts mathematically and semantically consistent with Dolomite tokenomics. The immutable lifecycle allocation is 200,000,000 oDOLO-equivalent options, while the current ERC-20 `totalSupply` is lower because exercised options are burned.

## Confirmed data model

The Distribution chart reconciles the full allocation into four mutually exclusive states:

1. `Future rewards reserve`: current oDOLO balance of the official oDOLO Claims contract.
2. `Circulating`: current token supply minus the claims reserve and oDOLO Pair Vester balance.
3. `Vester balance`: current oDOLO balance of the oDOLO Pair Vester.
4. `Redeemed & burned`: immutable allocation minus current ERC-20 total supply.

The invariant is:

`future rewards reserve + circulating + vester balance + redeemed and burned = 200,000,000 oDOLO`

Claimer Behavior has a different denominator. It describes only oDOLO actually paid from the official oDOLO Claims distributor. Its lifecycle partition remains `exercised + sent away + claim remaining = claimed to date`. The chart metadata provides the allocation context as `X claimed of 200M allocation`; it does not force unclaimed allocation into the behavior donut.

## Root cause

The generic `RewardClaimed` index currently falls back to oDOLO metadata for unresolved Berachain distributors. The legacy oDOLO compatibility payload then copies every Berachain reward-claim event. This causes DOLO airdrop, investor, strategic-investor and advisor claims to enter `generate_odolo_flows.py` as if they were oDOLO claims. The resulting claimed total can exceed the complete 200M allocation.

The official oDOLO Claims distributor is `0x79e6e932bf6686a4d357d7821e6e08835ba8a026`, and the oDOLO token is `0x02e513b5b54ee216bf836ceb471507488fc89543`. Both identities must be present before an event may supplement the canonical oDOLO transfer ledger.

## Selected approach

Use a pipeline-first, defense-in-depth correction:

- Add an explicit 200M allocation constant to the on-chain supply snapshot generator.
- Derive `redeemedAndBurned` from the allocation and current `totalSupply`, using integer/wei arithmetic before display conversion.
- Build the legacy oDOLO event payload from official oDOLO Claims events only.
- Make the oDOLO flow loader independently require both the official distributor and oDOLO token address. A misleading symbol alone is not sufficient evidence.
- Remove the Option Airdrop contract from the known oDOLO distributor set. Correct documented non-oDOLO distributor metadata where on-chain token-transfer evidence is available; unresolved generic reward distributors must not become oDOLO compatibility events.
- Keep ERC-20 transfers from the official claims contract as the primary claim source. RewardClaimed events remain a self-healing supplement, merged per wallet without double counting.

This is preferred over a UI-only clamp because generated data and downstream tables must also be correct. Full receipt verification of every generic reward event is deferred because it would materially increase RPC cost and workflow fragility; the strict oDOLO allowlist provides the required correctness for these charts.

## Generated data contract

`odolo_contract_data.json` gains:

- `allocationSupply`: `200000000`
- `redeemedAndBurned`: `allocationSupply - totalSupply`
- `allocationMethodology`: a concise source/method description

Existing supply fields remain compatible. No new generated file is introduced.

`odolo_flows.json` keeps its current schema but its claimer population and totals are regenerated from the corrected sources. Claimer metadata states the official distributor and 200M allocation context.

## UX

### oDOLO Distribution

- Add `Redeemed & burned` as the fourth donut segment and legend row.
- Use the full 200M allocation as the donut denominator and center/metadata total.
- Keep the existing Graphite + Gold visual language. The burned segment uses a restrained muted-rust tone rather than a warning-red treatment.
- Explain in the row tooltip that exercised oDOLO is burned and therefore no longer belongs to current ERC-20 supply.
- Keep the hero `Total Supply` metric as the current live ERC-20 supply; label the distribution total as `200M allocation` so the two concepts are not conflated.

### Claimer Behavior

- Keep the three non-overlapping lifecycle segments: `Claim exercised`, `Sold or sent away`, and `Claim remaining`.
- Change the metadata to `X oDOLO claimed of 200M allocation`.
- Keep `Held now` outside the donut because it is an independent current balance and can include purchased oDOLO.
- Update supporting copy so users understand that this chart analyzes paid claims, not the full tokenomics allocation.

## Validation and failure behavior

Publishing must fail when any of these conditions occurs:

- allocation components do not reconcile to 200M within the established numerical tolerance;
- current total supply exceeds the immutable allocation;
- `redeemedAndBurned` is negative;
- Claimer Behavior `total_claimed` exceeds 200M;
- a legacy oDOLO claim event has a distributor other than the official oDOLO Claims contract;
- a legacy oDOLO claim event lacks the exact oDOLO token address;
- claimer lifecycle rows or totals no longer reconcile.

The fetcher retains the existing safe behavior on RPC failure: do not overwrite a valid snapshot with placeholders.

## Test strategy

Follow TDD with focused regression tests:

1. Supply derivation tests require the four allocation components to total 200M and reject total supply above allocation.
2. Reward-index tests prove that the Option Airdrop and other non-oDOLO distributors cannot enter the legacy payload.
3. Flow-loader tests reject a wrong distributor even when its event is falsely labeled with the oDOLO token or symbol.
4. Validation tests reject claimed totals above 200M and invalid allocation reconciliation.
5. UI contract tests require the fourth distribution segment, `200M allocation` context and corrected Claimer Behavior metadata.
6. Existing oDOLO generator, validation and preview contract suites must remain green.

## Verification and deployment

- Regenerate the affected oDOLO contract, reward-claim compatibility and flow JSON files from corrected code.
- Run focused Python unit tests and `validate_data.py` checks for the affected artifacts.
- Serve the static dashboard with `python3 -m http.server` and inspect `/odolo/` at desktop and mobile widths. Verify computed chart geometry, legend wrapping, copy and absence of console errors.
- Review the diff to ensure unrelated generated data and UI are untouched.
- Commit intentionally, push the verified branch to production `master`, wait for GitHub Pages deployment, and verify the live page with a cache-busting query parameter.
