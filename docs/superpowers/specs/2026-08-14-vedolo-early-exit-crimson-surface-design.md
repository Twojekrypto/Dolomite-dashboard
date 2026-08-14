# veDOLO Early Exit Crimson Surface Design

## Objective

Make `Early Exit Analytics` and `Recent Early Exits` read as one intentional analytical suite: a restrained dark-crimson surface within the existing Graphite + Gold dashboard, with complete table corner geometry and identical freshness indicators.

## Approved visual direction

Use the recommended **Crimson Glass** treatment. The cards retain the dashboard's near-black base while adding a low-saturation burgundy wash, a quiet red border, and a fine ambient inset highlight. Red communicates loss/early-exit context without turning either card into a saturated warning panel.

## Surface hierarchy

- Both cards share the same base gradient, border color, shadow, and freshness accent.
- `Early Exit Analytics` keeps its internal panels, but its header and summary must visually belong to the same continuous card surface.
- `Recent Early Exits` uses a slightly darker table header and body layer for hierarchy, while its header, toolbar, body, and footer remain visibly part of one card.
- Neutral `.exit-table-card` parity rules must not overwrite the crimson treatment.

## Corner and overflow behavior

- All four outer corners must appear rounded at the configured card radius on desktop and mobile.
- Keep card overflow visible so the period dropdown can escape the card without clipping.
- Apply matching radii to the first and last painted child surfaces instead of hiding overflow.
- The Recent Early Exits footer owns the bottom radius; its heading owns the top radius.

## Freshness indicator

- Both `Data updated` indicators use the same muted coral-red dot and glow.
- The text remains the existing subdued mono metadata style.
- No animated or high-saturation warning treatment is introduced.

## Readability and interaction

- Body text and numeric values retain their current contrast and semantic colors.
- Search, period dropdown, sorting, row hover, pagination, and address interactions remain functionally unchanged.
- The red wash must be subtle enough that table rules, row hover, and focus rings remain legible.

## Responsive requirements

- Verify at 1440×900 and 390×844.
- No horizontal page overflow may be introduced.
- At each viewport, computed top-left/top-right/bottom-left/bottom-right radii must be non-zero on the painted Recent Early Exits header/footer surfaces.
- The dropdown must remain unclipped when opened.

## Deployment

- Add a new route cache-bust token to `vedolo/index.html` so GitHub Pages visitors receive the updated preview immediately.
- Push the verified commit to production `master`.

