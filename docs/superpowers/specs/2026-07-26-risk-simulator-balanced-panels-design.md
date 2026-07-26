# Risk Simulator Balanced Panels Design

## Context

The desktop Risk Simulator currently uses an asymmetrical 56/44 split even
though `Build Scenario` and `Scenario Result` already share the same height.
The result header also repeats a live-state sentence, while the bottom
`Risk level` row restates impact already communicated by the threshold result.
Removing those lines without redistributing the remaining content would leave
the result panel visually underfilled.

## Goals

- Remove `Scenario active · Updated live as you edit` and its inactive
  equivalent from the visible result header.
- Remove the entire `Risk level / High impact` result row.
- Give `Build Scenario` and `Scenario Result` equal desktop width and height.
- Redistribute the remaining result cards so the panel stays intentionally and
  evenly filled.
- Preserve all simulation calculations, scenario controls, wallet details, and
  responsive behavior.

## Desktop composition

Above 980 px, the simulator uses an exact two-column 50/50 grid. Both panels
stretch to the same computed height.

`Scenario Result` keeps a single clean heading followed by:

1. the primary threshold-crossing result;
2. debt exposure;
3. collateral exposure.

The primary result occupies the upper result area. Debt and collateral form a
balanced two-card supporting row below it. The result grid stretches through
the available panel height, so removing the status and risk rows does not
create an accidental empty band.

`Build Scenario` retains its existing header actions and input model. Its
editor stretches with the panel while keeping the existing compact row sizing;
additional assets remain usable without forcing oversized controls.

## Content and behavior removal

The visible `sim-impact-state` element and the `sim-impact-risk-row` markup are
removed. Their dedicated CSS is removed or neutralized. The JavaScript no
longer looks up or updates `sim-impact-state` or `sim-risk-level`.

Impact is still communicated by the threshold count, semantic card treatment,
expanded wallet details, and existing accessible live result. No calculation
or risk-classification input changes.

## Responsive behavior

At 980 px and below, the two panels continue to stack in their current reading
order. Result cards return to a single-column layout where needed, and the
page must have no horizontal overflow at a 390 px viewport.

## Implementation boundaries

Changes remain surgical within `liquidation-preview.html` and the targeted
Borrow UX contract tests. No data files, workflow logic, dependencies, or
upstream risk methodology are changed.

## Verification and acceptance

- Contract tests prove both unwanted labels and their element IDs are absent.
- Contract tests prove the desktop grid uses equal columns.
- Existing simulator behavior tests continue to pass.
- `python3 run_earn_audit_checks.py` and `git diff --check` pass.
- Browser verification at 1440 px confirms equal panel width and height,
  filled result-card composition, and no removed copy.
- Browser verification at 390 px confirms clean stacking and no horizontal
  overflow.
- Computed bounding boxes and styles are checked in Chromium, not inferred
  only from source.
