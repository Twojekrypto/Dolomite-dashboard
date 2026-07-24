# Dolomite Supply & TVL Switcher — Design

## Goal

Replace the two consecutive `Total Supply Over Time` and `TVL Over Time` cards
with one clearer protocol-history card. The combined card must default to Total
Supply and let the user switch to TVL without losing the existing full chart,
tooltip, range badge, or mini-chart brush UX.

Remove the redundant `N supply · N borrow` sentence under the EARN Portfolio
Value amount. The existing supply, borrow-route, and verification chips remain
the only position counters.

## Product Decision

Use one metric at a time. Do not overlay both lines and do not add a third
`Compare` mode.

Total Supply and TVL are both USD-denominated, but they answer different
questions:

- Total Supply is gross user deposits.
- TVL is the net protocol value after borrowing.

Showing both by default would add legend, hover, fill, and scale ambiguity to a
card whose primary job is fast metric inspection. A two-option segmented
control keeps the interaction explicit and can be extended later only if users
demonstrate a recurring comparison need.

## Visual Direction

Preserve the existing Graphite + Gold identity.

- Graphite canvas: `#09090b`
- Elevated panel: `#141417`
- Dolomite gold: `#c9a227`
- Primary text: `#f5f5f2`
- Muted data text: `#8b8b93`

Keep Inter for labels and JetBrains Mono for controls and data. The signature
element is the compact institutional segmented control borrowed from the
`Top holders / Smaller holders` interaction: a restrained graphite capsule,
thin gold focus/active line, and no decorative animation beyond the existing
state transition.

The control is the one visual emphasis. No new gradients, icons, legends, or
badges are added.

## Layout

```text
┌─────────────────────────────────────────────────────────────────────┐
│ Dolomite Supply & TVL   [All Time]          ● drag window to zoom   │
│                                                                     │
│ [ TOTAL SUPPLY | TVL ]                                              │
│                                                                     │
│                         MAIN HISTORY CHART                          │
│                                                                     │
│                         MINI-CHART BRUSH                            │
└─────────────────────────────────────────────────────────────────────┘
```

The switch sits in a dedicated toolbar below the card title rather than
competing with the range badge or source/meta line. On narrow screens it
stretches to the available width and keeps equal options.

## Interaction Contract

- `TOTAL SUPPLY` is active on first render.
- Both buttons are native `button` elements inside a named group.
- The active button has `aria-pressed="true"`; the inactive button has
  `aria-pressed="false"`.
- Switching modes reveals the matching chart panel inside the same card.
- Each metric preserves its own brush selection when the user switches away
  and back.
- The range badge changes with the visible metric.
- The tooltip and mini-chart continue to use the same formatting and drag
  behavior as the current cards.
- No standalone `TVL Over Time` card or duplicate card title remains.

## Portfolio Value Contract

- Remove `earn-summary-portfolio-sub` from the rendered summary.
- Remove both runtime update paths that write `N supply · N borrow`.
- Keep the following chip row immediately below Portfolio Value:
  - `N supply asset(s)`
  - `N borrow route(s)`
  - verification status such as `N pending verify`
- Regenerate `earn/earn-core.html` and `earn/earn-core.js` from shared sources.
- Bump the EARN and TVL route asset versions so GitHub Pages clients receive
  the updated markup and runtime.

## Responsive and Accessibility Requirements

- The switch must remain readable without clipping at mobile widths.
- Keyboard focus must be visible in Dolomite gold.
- Hidden metric panels must not remain exposed to assistive technology.
- Existing reduced-motion and hover behavior remains unchanged.

## Acceptance Criteria

1. The TVL page renders exactly one history card titled
   `Dolomite Supply & TVL`.
2. `TOTAL SUPPLY` is active by default.
3. Selecting `TVL` changes the visible main chart, tooltip, range badge, and
   mini-chart to TVL.
4. Returning to `TOTAL SUPPLY` restores that metric and its brush state.
5. Portfolio Value contains no duplicate supply/borrow sentence.
6. Shared and generated EARN assets are synchronized.
7. Contract tests pass, JavaScript parses, browser-computed layout is aligned,
   and the requested branch is pushed.

