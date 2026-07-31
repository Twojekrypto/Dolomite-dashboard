# Table Hover and Filter State Parity Design

## Goal

Make the requested Rewards, Assets, Earn, and Supply interactions feel like one
Graphite + Gold product while preserving existing data, sorting, expansion, and
routing behavior.

## Alternatives Considered

1. Add isolated CSS overrides to each table and selector. This is the smallest
   patch, but it repeats fragile `last-child` rules and would leave the icon
   mismatch in the data-resolution layer.
2. Introduce a scoped interaction contract and reuse the existing canonical
   asset presentation data. This keeps the patch surgical while making hover,
   terminal-row geometry, selector states, and icons consistent. This is the
   chosen direction.
3. Replace the static table and selector markup with shared web components.
   This would improve long-term reuse, but it is too broad and risky for the
   current static dashboard.

## Chosen Direction

### Table row interaction

- DOLO Holders remains the reference for the quiet graphite hover fill and the
  two-pixel gold rail at the left edge.
- Supply Assets, Borrow Positions, and Past and routed receive the same hover
  treatment. Expanded/open rows retain their existing persistent gold state.
- Ended Programs and Dolomite Assets treat the final visible data row as the
  bottom of the table surface: the hover fill rounds at the lower left and
  lower right, and the lower end of the gold rail follows the left corner.
- Earn tables mark the final primary data row explicitly instead of relying on
  `last-child` or `last-of-type`, because detail and spacer rows are interleaved
  with primary rows. This prevents accidental rounding in the middle of a
  table.
- Tables that continue into a footer or pagination remain square at that join.
  Empty, loading, detail, and spacer rows never receive the interactive rail.

### Dolomite Earnings search

- The address input, network selector, and Search button remain one 56px control
  deck.
- The network trigger fills the complete 54px inner height; hover and focus are
  therefore applied to the full segment rather than only the 20px text line.
- Hover uses a restrained graphite lift. Open/selected network state uses a
  subtle gold wash, gold chevron/accent, and visible focus treatment without the
  browser-default blue outline.
- Network menu options use one full-width hit target, a neutral hover state, and
  one clearly selected row. Archived labels remain secondary and do not create
  a second competing highlight.
- Search remains the strongest gold action and keeps stable geometry in idle,
  hover, loading, and disabled states.

### Supply Markets selection

- Asset, network, and Apply remain one segmented 56px deck with equal vertical
  rhythm and consistent dividers.
- In the applied state, asset and network use high-contrast neutral text while
  the Applied button shows a restrained gold confirmation with a check.
- After choosing a different asset, the asset segment receives the stronger
  gold pending wash, the network receives a lighter contextual gold wash, and
  Apply becomes the strongest actionable gold segment.
- Opening either dropdown uses the same focus/open language without suggesting
  that the other value changed.
- Hover never replaces the semantic pending/applied state; it only adds a small
  brightness lift.

### Past and routed icons

- Icon resolution is address-first and then symbol-based, matching Dolomite
  Assets. Market-specific overrides such as GM/GLV wrappers therefore retain
  their correct official artwork instead of falling back to a generic symbol
  icon.
- The same full-logo framing and grayscale rules used by Dolomite Assets are
  retained where relevant.

## Accessibility and Responsive Behavior

- All actionable controls keep visible `:focus-visible` treatment and existing
  accessible names.
- Hover-only decoration does not change text color or hide data.
- Touch layouts do not depend on hover, and the 56px control decks remain large
  enough for reliable pointer and touch use.
- Dropdowns keep internal scrolling and stay inside the viewport.

## Verification

- Contract tests cover terminal-row markers/radii, hover rails on the three
  Earn tables, full-height Earn network controls, coherent Supply pending and
  applied classes, and address-first Past and routed icons.
- Browser checks on the local HTTP server inspect real computed backgrounds,
  border radii, pseudo-element radii, control heights, focus states, and asset
  image URLs.
- Desktop and mobile layouts are checked, along with console errors and the
  existing targeted test suites.
- After rebasing on current `master`, the implementation is committed, pushed
  to production `master`, and the live GitHub Pages routes are verified.
