# Borrow Hero and Institutional Scenario Desk Design

## Context

The Borrow hero currently repeats the position change as both an absolute count
and a percentage. The percentage is useful for comparison with the DOLO hero,
but the extra `positions` line makes the badge visually heavy. The latest
position-count history also contains only about 13 hours of usable baseline
data, so presenting that fallback as a full 24-hour comparison would be
misleading.

The Risk Simulator already supports multi-asset scenarios, but each row repeats
three negative presets. They consume desktop space, favor only downside
scenarios, and compete with the manual input that should be the primary
interaction.

## Goals

- Make the Borrow hero change indicator as compact and legible as the DOLO hero.
- Preserve the real comparison window instead of labeling incomplete history as
  24 hours.
- Turn the Risk Simulator into a focused institutional scenario workspace with
  manual, per-asset price shocks.
- Improve full-width desktop composition while retaining a clear stacked mobile
  layout.
- Reuse the existing calculations and data sources without changing risk
  methodology.

## Non-goals

- No changes to liquidation thresholds, position calculations, wallet routing,
  or upstream JSON generation.
- No fabricated 24-hour baseline and no interpolation of missing history.
- No new frontend dependencies.
- No changes to Borrow tables or their column layout.

## Borrow hero

The hero will show only the total position count and one compact change chip on
the same visual row. The chip will follow the established DOLO treatment:

- direction arrow;
- signed percentage;
- centered separator;
- comparison window.

Examples are `−0.12% · 13h` for the current fallback and
`−0.36% · 24h` when an exact 24-hour baseline exists. The absolute delta and
the `positions` unit will not be rendered inside the chip.

The existing data priority remains unchanged:

1. use the exact `change24h` baseline when available;
2. otherwise use the nearest valid fallback and display its real rounded window;
3. when no comparison exists, render the neutral unavailable state
   `— · awaiting history`.

The chip keeps semantic direction color and arrow behavior consistent with the
DOLO hero. Its accessible label and native title will explain whether the value
uses an exact 24-hour baseline or a shorter fallback while history is building.

## Institutional Scenario Desk

### Desktop composition

At widths above 980 px, the simulator will use the available card width as a
balanced two-panel workspace:

- the Scenario Builder occupies approximately 56% of the inner width;
- Scenario Result occupies approximately 44%;
- both panels align to the same height;
- internal spacing is compact enough to avoid a large unused lower area.

The left header contains the title and two actions: a gold `Add Asset` action
and a secondary `Reset Scenario` action. Moving these actions out of the
bottom summary card removes the disconnected footer and keeps scenario controls
where users expect them.

The result panel places the primary threshold-crossing outcome first. Debt,
collateral, aggregate risk, and the wallet action follow as supporting context,
so the reading order moves from consequence to explanation.

### Scenario rows

Each scenario row contains:

- the existing asset selector and removable asset identity;
- a clearly labeled manual percentage input;
- minus and plus controls that adjust the value by 1 percentage point;
- a subtle, non-interactive bipolar magnitude rail centered on zero.

The three repeated `−5%`, `−10%`, and `−25%` preset buttons are removed from
markup, styling, and interaction code. Downside values use a restrained red
direction treatment; upside values use the existing positive accent. The rail
is supporting feedback, not a second input.

The valid simulation domain remains `−100%` through `+500%`. Step controls move
in 1-point increments, while direct entry may include decimals. Direct values
are normalized and clamped on commit without changing the underlying simulator
formula. Typing, stepping, adding, removing, or resetting an asset updates the
result using the current live-calculation path.

Reset returns the default three assets to `0%`. Add Asset continues to use the
existing token-selection behavior.

### Responsive behavior

Below the desktop breakpoint the two panels stack in reading order. Scenario
rows may wrap their selector and value control, but the input and step buttons
remain on one usable line. Header actions may wrap without overlapping the
title, and no horizontal page overflow is allowed at a 390 px viewport.

### Accessibility

- Every numeric control retains an explicit asset-specific accessible label.
- Step and remove buttons keep descriptive labels and visible keyboard focus.
- Direction is communicated by signed text and arrow/rail position, not color
  alone.
- The result summary becomes a polite live region so meaningful scenario
  updates are announced to assistive technology.
- Reduced-motion preferences continue to disable nonessential transitions.

## Implementation boundaries

Changes should stay surgical within `liquidation-preview.html` and the targeted
Borrow UX contract tests. The final CSS override block should be extended rather
than reformatting broad legacy sections. Obsolete preset rendering,
`applyMultiAssetPreset`, active-preset synchronization, and preset click
handlers should be removed so there is only one scenario input model.

## Verification and acceptance

Regression tests will be written before the production edits and must prove:

- the hero has no absolute change or `positions` line;
- the hero percentage uses the exact or fallback window already supplied by the
  data;
- scenario presets and their handler are absent;
- row step controls use 1-point increments;
- Add Asset and Reset Scenario are in the builder header;
- the result summary exposes a polite live region;
- the existing manual input and live result path remain present.

After implementation:

- run the targeted Borrow UX contract tests;
- run `python3 run_earn_audit_checks.py`;
- run `git diff --check`;
- serve the dashboard with `python3 -m http.server`;
- verify at 1440 px that both panels use the width, align in height, show the
  threshold outcome first, and have no obsolete preset space;
- verify at 390 px that controls stack cleanly with no horizontal overflow;
- verify computed dimensions and styles in Chromium rather than relying only on
  source inspection;
- interactively test direct decimal entry, both 1-point step buttons, Add Asset,
  remove, Reset Scenario, and live result updates.
