# Supply Pool Health Semantic Colors Design

## Goal

Make selected Supply Pool Health values easier to scan without turning the
table into a multi-colour dashboard or implying that neutral magnitude metrics
are inherently good or bad.

## Visual semantics

- `Supply` and `Average` stay white because their magnitude has no universal
  positive or negative meaning.
- `Suppliers` uses the existing muted yield blue (`--yield`) to identify pool
  participation without assigning a risk verdict.
- `Top 10` and `Largest` use one restrained concentration scale:
  - low concentration: muted green;
  - moderate concentration: graphite-compatible gold;
  - high concentration: muted clay red.
- `30D Change` keeps the existing green/neutral/red signed-change treatment.
- `Quality` keeps the existing A–F grade colours.

## Concentration thresholds

Thresholds follow the breakpoints already used by
`generate_supply_health.py`, rather than dataset-relative percentiles:

| Metric | Low | Moderate | High |
|---|---:|---:|---:|
| Top 10 | `<= 40%` | `> 40%` and `<= 60%` | `> 60%` |
| Largest | `<= 20%` | `> 20%` and `<= 40%` | `> 40%` |

Missing values remain neutral. The raw percentage remains visible, so colour
is supporting information rather than the only carrier of meaning.

## Implementation

- Add a pure `healthConcentrationClass(metric, value)` helper to
  `tvl/supply-health.js`.
- Export the helper for focused Node tests.
- Add participation and concentration classes while rendering the five
  existing numeric cells; do not change column order, widths, sorting, data, or
  markup outside those cells.
- Define the colour palette under `#supply-health-card` and scope every new
  selector to the card.
- Bump the TVL CSS/JS asset versions and route version so GitHub Pages clients
  receive the change immediately.

## Verification

- Unit-test every threshold boundary plus missing and invalid values.
- Keep the existing Supply Pool Health contract and pagination tests green.
- Verify computed colours, centered alignment, sorting, and overflow at the
  real desktop width in a served browser.
- After deployment, repeat the computed-style and interaction checks on the
  cache-busted live TVL route.
