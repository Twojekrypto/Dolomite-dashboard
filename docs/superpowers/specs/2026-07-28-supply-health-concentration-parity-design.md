# Supply Pool Health — concentration color parity

## Goal

Remove the row-to-row “Christmas tree” effect from the `Top 10` and `Largest`
columns while preserving concentration context. The visual reference is
`Latest oDOLO Exercises`, where each numeric column keeps one stable tone and
color does not change unpredictably between rows.

## Approved direction

- Give `Top 10` and `Largest` one shared, subdued warm-gold text color in every
  data row.
- Do not encode low, moderate, and high concentration by changing the number
  color.
- Preserve the existing concentration thresholds in code and expose the
  resulting level through the dashboard's unified hover tooltip:
  - `Top 10`: low up to 40%, moderate above 40% through 60%, high above 60%.
  - `Largest`: low up to 20%, moderate above 20% through 40%, high above 40%.
- Keep the rest of the table hierarchy unchanged:
  - `Supply` and `Average` stay neutral white.
  - `Suppliers` keeps its stable participation accent.
  - `30D Change` remains the only row-dependent positive/negative color.
  - `Quality` retains its compact grade badge.

## Implementation boundary

Change only the Supply Pool Health presentation and its targeted contracts:

- replace threshold-specific concentration classes with one stable display
  class;
- convert the threshold helper into a semantic level/tooltip helper;
- remove unused low/moderate/high color variables and selectors;
- update focused JavaScript and Python contract tests;
- bump the Supply Pool Health asset versions in `tvl-preview.html` so GitHub
  Pages clients receive the new CSS and JavaScript.

No data source, score calculation, sorting, pagination, filter, or expanded-row
behavior changes.

## Interaction and accessibility

Both concentration cells will use `data-tip`, which is already handled by the
shared body-level tooltip system. The tooltip text will name the metric,
 display the exact rendered percentage, and state the semantic concentration
 level. Keyboard focus remains on the existing table row; no new clickable
 controls are introduced.

## Verification

- Run the Supply Pool Health JavaScript unit tests and Python contract tests.
- Run a JavaScript syntax check.
- Serve the static dashboard through `python3 -m http.server`.
- In Chromium, verify:
  - all visible `Top 10` and `Largest` values have the same computed color;
  - hovering both columns shows the correct low/moderate/high tooltip;
  - `30D Change` and `Quality` retain their existing semantic colors;
  - table dimensions and alignment do not change.
- Review the final diff for unrelated edits and deploy the verified commit to
  `dolomite-dashboard/master`.
