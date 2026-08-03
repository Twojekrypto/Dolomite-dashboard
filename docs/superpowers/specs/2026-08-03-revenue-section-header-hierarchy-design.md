# Revenue section header hierarchy

## Goal

Make the Revenue page use one clear, repeatable header hierarchy that matches the `Top users saved with current veDOLO` table: section identity and freshness first, a full-width divider second, then supporting copy and controls.

## Scope

Apply the shared header treatment to these five sections in `revenue-preview.html`:

- Protocol Revenue by Chain
- Dolomite Revenue Over Time
- Borrow Interest Over Time
- veDOLO Borrow Fee Rebates
- Current discount users

Also align the `Data updated · … ago` indicator in the Dolomite Revenue hero with the typography, color, dot, and spacing used by the DOLO hero freshness indicator.

## Approved layout

Each affected section uses two semantic rows:

1. The primary row contains the section title on the left and `Data updated · … ago` on the right.
2. A one-pixel divider spans the full content width directly below the primary row.
3. The secondary row contains the explanatory subtitle on the left and the section-specific controls on the right.
4. The chart, summary, or table content starts below the secondary row.

Controls stay with their current section:

- Protocol Revenue by Chain: date range controls.
- Dolomite Revenue Over Time: series controls and Daily/Cumulative mode.
- Borrow Interest Over Time: Daily/Cumulative mode.
- veDOLO Borrow Fee Rebates: simulation and Daily/Cumulative controls.
- Current discount users: the existing active-discount status.

All five freshness labels use the Revenue dataset generation timestamp and the existing English relative-age formatter. No data calculations or source semantics change.

## Responsive behavior

At desktop width, the title/freshness and subtitle/controls rows stay horizontally balanced. The divider spans the full panel width between them.

At mobile width, each row may stack vertically. Freshness remains visually associated with the title, controls stay below the subtitle, and the divider remains full-width without producing horizontal document overflow.

## Visual constraints

- Reuse the existing Graphite + Gold tokens; introduce no new color system.
- Use the restrained divider and freshness treatment from established dashboard surfaces.
- Do not add cards, pills, borders, or decorative accents beyond what the hierarchy requires.
- Keep current chart, brush, table, simulation, and filter behavior unchanged.

## Verification

- Add a structural regression test that asserts all five sections use the shared two-row header contract and expose a freshness target.
- Assert the divider belongs between the primary and secondary rows, rather than below all controls.
- Verify computed divider width/border and freshness typography in a local browser at desktop width.
- Verify stacking and absence of horizontal document overflow at a mobile viewport.
- Run the Revenue test suite and the dashboard audit checks before publishing.
