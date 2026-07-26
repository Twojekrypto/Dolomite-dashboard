# Borrow Live Impact and Workflow Reliability — Design

## Goal

Make the Borrow page honest and immediately useful when an exact 24-hour
position baseline is still being built, improve the Risk Simulator hierarchy,
remove the Liquidation History hover seam, and restore the EARN workflows that
have been failing since 2026-07-25 22:00 CEST.

## Confirmed causes

- `change24h` is intentionally `null` when no snapshot is within six hours of
  the exact 24-hour target. The UI currently converts that honest state into a
  permanently unavailable badge.
- `tests/test_borrow_ux_contracts.py` dereferences `change24h` without allowing
  `null`. That single error caused 52 producer/backfill/repair workflow failures;
  seven freshness alarms followed because those producers could not publish.
- The Liquidation History table uses a zero-width spacer column and translates
  the Date cell left by 12px. On hover this leaves an unpainted 12px vertical
  seam before Collateral.
- The simulator gives three result cards equal visual weight and labels its
  inputs only as “Scenario”, so the cause-and-effect flow is weaker than the
  underlying data.

## Position change contract

The strict `change24h` contract stays unchanged: it is present only when a
baseline is close enough to a true 24-hour comparison.

The generator additionally exposes `fallbackChange` when at least one older
non-current snapshot exists:

```json
{
  "currentCount": 1638,
  "baselineCount": 1640,
  "change": -2,
  "baselineAt": 1785003234,
  "windowSeconds": 45600
}
```

The fallback selects the retained snapshot nearest to the 24-hour target and
reports the real elapsed window. The UI prefers `change24h`; otherwise it shows
`fallbackChange` with an honest rounded label such as `13h`, plus an accessible
explanation that the 24-hour baseline is still building. It must never label a
shorter comparison as 24h.

The committed-data test accepts either an exact comparison or an honest
fallback and validates whichever contract is present.

## Institutional Live Impact simulator

The existing calculation path and element IDs remain intact. The redesign is
presentation-first:

- “Scenario” becomes “Build Scenario”.
- Each row has explicit `Asset` and `Price shock` context.
- Compact `−5%`, `−10%`, and `−25%` presets set a row’s signed shock without
  changing the existing number input or stepper behavior.
- “Scenario Result” becomes “Live Impact”.
- The primary result is a sentence: `N positions cross HF 1.0`.
- Debt and collateral exposure become quieter secondary metrics.
- A `Risk level` status reads `No impact`, `Watch`, or `High impact`.
- With all shocks at zero, the panel says `Adjust a token to simulate impact`.
  A `Scenario active` state appears only after a non-zero shock.
- Desktop preserves the two-column causal flow; mobile stacks editor then
  impact.

The Graphite + Gold visual identity remains unchanged. Risk colors retain their
existing semantic meaning.

## Liquidation History

The table becomes five columns:

1. Chain — 7.8%
2. Liquidated wallet — 19%
3. Date — 39%
4. Collateral seized — 18.2%
5. Debt repaid — 16%

The spacer header/cell and Date translation are removed. All Liquidation
History-specific `nth-child` selectors, generated rows, and empty-row colspans
are updated together. Lending Positions keeps its own spacer and is not changed.

## Workflow recovery

The Borrow contract tests are corrected instead of weakening EARN audit rules.
After local verification, the production branch is pushed and these workflows
are dispatched and monitored:

- Update Liquidation Risk Data, to publish the fallback comparison.
- Earn Audit Checks, as the shared regression gate.
- Representative canonical EARN refreshes or the existing repair dispatcher,
  to prove the formerly blocked audit step is green.
- Deploy GitHub Pages, to verify the public route.

Historical cancelled runs are not retried when they were superseded by a newer
run. Historical Pages failures already followed by a successful deployment are
recorded as resolved, not treated as current defects.

## Acceptance criteria

- The Borrow badge shows a real signed count and percentage when an exact 24h
  baseline is unavailable, and labels the actual comparison window.
- Exact 24h data still wins automatically when available.
- Risk Simulator presents Build Scenario → Live Impact, row presets work, and
  the zero/non-zero states are unambiguous.
- Liquidation History has five cells per data row and no visible hover seam.
- Targeted tests and the full EARN audit suite pass.
- Desktop and mobile browser checks confirm computed layout and interactions.
- Production `master`, required data refresh, and Pages deployment finish green.
