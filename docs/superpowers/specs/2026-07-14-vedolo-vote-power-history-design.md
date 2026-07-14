# veDOLO Vote Power History Design

## Goal

Extend the existing `Locked DOLO Over Time` card with an exact historical
`Vote Power` view. The chart must use canonical on-chain state, not a ratio or
an event-only approximation.

## UX

- Add a compact two-option segmented control to the existing card header:
  `Locked DOLO` and `Vote Power`.
- `Locked DOLO` remains the default and preserves the current chart exactly.
- Selecting `Vote Power` changes the heading to `Vote Power Over Time`, the
  y-axis labels, and the hover value to Vote Power. The date-range badge and
  brush remain in the same place.
- The selected date range is shared between modes. Switching modes preserves
  the current `from` and `to` dates whenever both series cover them; the range
  is clamped only when it falls outside a series' verified coverage.
- The active option follows the existing compact dashboard control treatment:
  gold active state, restrained hover/focus state, stable dimensions, and no
  layout shift. Controls are real buttons with `aria-pressed` and keyboard
  activation.
- The lower brush continues to show the selected metric's full history. Its
  window controls and label retain their current interaction model.

## Canonical Data Contract

A workflow generates `data/vedolo-vote-power-history.json`. It contains:

```json
{
  "schemaVersion": 1,
  "metric": "votePower",
  "chain": "berachain",
  "contract": "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4",
  "generatedAt": "ISO-8601 UTC",
  "coverage": { "from": 0, "through": 0 },
  "points": [[0, "0"]]
}
```

- `points` are daily UTC observations from contract deployment through the
  latest complete day, followed by the exact latest canonical observation.
- Values are decimal strings in the generated file. Precision is retained by
  the generator and rounded only for dashboard rendering.
- The canonical source is the veDOLO contract's verified global vote-power
  checkpoint/state at the relevant historical block. The implementation must
  first verify the exact ABI and storage semantics against the deployed
  contract. If the contract exposes global point history, use its
  bias/slope/checkpoint data. If it does not, replay the complete canonical
  lock state only when that replay proves equal to contract reads.
- `vedolo_flows.json` is useful audit evidence but is not itself an approved
  vote-power source. Its existing lock/unlock reconstruction is sufficiently
  close for the Locked DOLO chart but does not reconcile exactly to live Vote
  Power, so it must never be rescaled or relabelled as Vote Power.
- `metrics_snapshot.json` has only recent snapshots and may be used for
  regression checks, never as the all-time chart source.

## Generation And Publication

- Add a dedicated generator and run it from the veDOLO data workflow. It uses
  batched/cached RPC reads and runs in CI, never in a visitor's browser.
- The generator must pin every run to one canonical target block/time. Its
  published last point is the same observation used for validation.
- The generator must reject a result when any point is malformed, negative,
  out of chronological order, has a coverage gap, or cannot be reproduced
  from the verified contract method.
- Before publishing, compare the latest generated observation to the same
  `balanceOfNFT`-derived total used by `vedolo_stats.json`. The comparison uses
  raw precision and an explicit, documented timestamp/block tolerance. A
  mismatch is a workflow failure, not a silently degraded dashboard update.
- Add the new file to `validate_data.py` and to the workflow's explicit
  `git add` list, so a successful generator run also deploys the data.

## Frontend Integration

- Load the static history beside the existing veDOLO datasets during page
  boot. Keep `lockedHistory`, `votePowerHistory`, and `lockedChartMode` as
  separate state values.
- Generalize the chart renderer, hover renderer, and brush renderer to consume
  the active series and metric formatter. The shared brush state remains a
  date interval rather than a pixel interval.
- Do not add live RPC calls, RPC secrets, or historical calculations to the
  browser.
- If the vote-power JSON is missing, malformed, or fails client schema checks,
  keep `Locked DOLO` available and disable the Vote Power option with a
  concise unavailable state. Do not substitute an estimated series.

## Verification

- Generator tests cover point ordering, UTC sampling, zero/negative rejection,
  duplicate handling, coverage checks, and rejection on a current-total
  mismatch.
- Data validation tests cover the JSON schema, contract/chain identity,
  complete coverage, and the current vote-power cross-check.
- Workflow tests prove the new generated file is validated and staged for the
  automated commit.
- Browser verification on a served local page proves both modes draw a
  non-empty path; labels, axis units, and tooltip values change with the mode;
  brush range survives a switch; buttons work by mouse and keyboard; and the
  header remains unclipped on desktop and mobile.

## Scope Boundaries

This change adds only the second view inside the existing veDOLO chart. It
does not change the hero metrics, holder table, lock-flow data semantics, or
the calculation displayed in the existing Locked DOLO view.
