# EARN Institutional Premium Polish Design

**Date:** 2026-07-24
**Status:** Approved
**Scope:** EARN Portfolio Value summary, Supply Assets APR/APY control, and inferred carry label

## Context

The simplified EARN summary now contains the correct information, but its presentation still feels heavier and less refined than the surrounding dashboard. The EARN APR/APY control also differs from the proven Dolomite Assets control: its option text is not vertically centered and its geometry is narrower. Finally, `Inferred Carry` does not fit comfortably in its table column.

This polish must preserve all existing portfolio, yield, reward, APR/APY, verification, and carry calculations.

## Goals

- Make the Portfolio Value summary feel like a restrained institutional finance instrument.
- Preserve the approved summary content:
  - Portfolio Value
  - supply, borrow-route, and verification counters
  - Total Yield Earned
  - Rewards
- Make the EARN APR/APY control visually and geometrically match Dolomite Assets.
- Shorten the visible `Inferred Carry` label without losing its explanatory context.
- Preserve the existing Graphite + Gold identity and mobile usability.

## Non-goals

- Changing data sources, calculations, generated JSON, or persistent state.
- Adding fonts, packages, images, APIs, or decorative dashboard components.
- Redesigning the EARN tables or changing their columns.
- Reintroducing any summary metrics removed in the previous simplification.

## Final Design

### 1. Portfolio Value — Institutional Ledger

The summary remains one connected graphite panel rather than a group of floating cards.

Desktop composition:

- The left section occupies the dominant share of the card and contains:
  - a restrained `Portfolio Value` eyebrow
  - the large monospaced portfolio value
  - the existing supporting context
  - compact position and verification counters
- A fine gold ledger rail separates the portfolio anchor from the secondary metrics.
- The right section contains `Total Yield Earned` and `Rewards` as two quiet metric zones separated by a subtle hairline.
- The secondary metrics do not receive heavy nested-card borders or competing shadows.

Visual language:

- Base graphite: `#09090B`
- Elevated panel: `#151518`
- Ledger gold: `#C9A227`
- Highlight gold: `#E4C15A`
- Primary text: `#F4F3EF`
- Positive/verified accent: `#75B87B`
- Existing Inter typography remains for labels and body copy.
- Existing JetBrains Mono remains for financial values.
- One restrained top-edge highlight and the ledger rail provide the signature detail.
- Hover may slightly strengthen the rail highlight, but no aggressive glow, scaling, or continuous animation is introduced.

Responsive behavior:

- Tablet keeps the same information hierarchy and may allow secondary metrics to wrap.
- Mobile stacks Portfolio Value before Total Yield Earned and Rewards.
- The vertical ledger rail becomes a horizontal divider on narrow screens.
- Counters wrap without clipping or horizontal scrolling.

### 2. APR/APY parity with Dolomite Assets

Reuse the existing EARN state and event hooks while matching the reference control's computed geometry:

- approximately `116px × 36px` outer control
- `3px` inner padding
- `10px` outer radius
- `52px × 28px` active slider
- each option is a flex container with centered content
- `28px` option height
- `0 14px` option padding
- `18px` line height
- slider translation uses the same fixed option width

The result must keep APR and APY visually centered in both states and retain the existing keyboard and click behavior.

### 3. Carry label

Replace the visible `Inferred Carry` label with `Carry`.

The existing tooltip remains the source of precision and explains that the value is inferred or estimated from canonical history. Internal state names and calculation logic remain unchanged.

## Data and State

This is a presentation-only refinement:

- existing Portfolio Value, Total Yield Earned, Rewards, position counters, and verification counters remain authoritative
- existing APR/APY selection state and calculations remain authoritative
- existing inferred-carry calculation and tooltip copy remain authoritative
- no new generated data file is required

## Accessibility

- APR/APY retains its radiogroup semantics, keyboard behavior, and visible active state.
- Financial hierarchy is communicated through labels and typography, not color alone.
- Text and dividers retain usable contrast against graphite surfaces.
- Motion remains subtle and respects the existing reduced-motion behavior.
- Tooltip access for `Carry` remains available by hover and keyboard focus.

## Verification

### Automated checks

- Update contract expectations from `Inferred Carry` to `Carry` where they represent visible copy.
- Preserve tests covering EARN summary content and APR/APY hooks.
- Run:
  - `npm run check:earn-audit`
  - `python3 run_earn_audit_checks.py`
  - relevant EARN contract tests
  - JavaScript syntax checks for changed generated and source files

### Browser checks

Serve the dashboard through `python3 -m http.server` and verify:

- EARN APR/APY computed dimensions and text alignment match the Dolomite Assets reference.
- APR and APY transitions keep the label centered.
- Portfolio summary hierarchy, ledger rail, metric separators, and counter wrapping at desktop and mobile widths.
- `Carry` fits its column and retains the correct tooltip.
- No clipping, horizontal overflow, or console errors.

### Publishing

If EARN source CSS or JavaScript changes:

1. rebuild the dedicated EARN bundle
2. update cache-busting versions and matching tests
3. review the scoped diff twice
4. commit only required files
5. publish the verified result to production `master`
6. confirm the live asset version and representative EARN behavior

## Acceptance Criteria

- The Portfolio Value summary reads as one calm institutional ledger panel.
- Portfolio Value remains the dominant element.
- Total Yield Earned and Rewards remain clearly readable without heavy nested tiles.
- Position and verification counters remain compact and understandable.
- EARN APR/APY geometry and centering match Dolomite Assets in browser-computed values.
- `Carry` fits the table column and retains its explanation.
- Desktop and mobile layouts do not clip or overflow.
- No financial calculation or data source changes.
- Relevant automated and browser checks pass.
- The verified change is pushed to production `master`.
