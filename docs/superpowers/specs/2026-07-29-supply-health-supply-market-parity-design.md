# Supply Health and Supply Market Parity Design

## Goal

Make Supply Pool Health, Dolomite Supply, and the Borrow section headers feel
like one product while preserving the existing market data and calculations.
The change clarifies chain and market identity, removes obsolete choices, and
lets a user move from a Supply Health row directly into the matching Supply
market.

## Scope

### Supply Pool Health columns

The desktop table uses this exact order:

1. Chain
2. Asset
3. Supply
4. Suppliers
5. Top 10
6. Largest
7. 30D Change
8. Quality
9. Details

`Average` is removed. Chain cells reuse the Lending Positions treatment: a
15px circular network icon followed by the readable network name. The Details
header stays visually quiet and the final cell contains the existing expansion
chevron.

Moving the expansion control must update the table header, data rows, detail
row `colspan`, empty state, spacer rows, widths, centered-column rules, mobile
rules, and every affected `nth-child` selector.

### Supply Pool Health interaction

Sortable headers follow DOLO Holders: hovering changes text color only and
does not change the header-cell background. Data rows retain the subtle
DOLO-Holders-style hover background and gold left rail. Expanded rows keep
their existing detail disclosure and keyboard behavior.

The detail action links to:

`./supply/?chain=<chain-key>&asset=<lowercase-token-address>`

The URL uses the exact row identity rather than a symbol so markets with the
same raw symbol cannot select the wrong asset.

### Dolomite Supply deep link

The Supply route reads the optional `chain` and `asset` query parameters.
When both identify a currently selectable market, it loads the requested
chain, selects the exact token address, and applies the market without a
second confirmation click. Invalid or unavailable parameters fall back to the
existing default-market behavior without breaking the page.

### Dolomite Supply market presentation

Presentation is resolved by normalized `chain + token address`, while raw
subgraph fields remain unchanged for calculations and requests.

- Known Arbitrum `dGM` wrappers display their exact GM market symbols, matching
  Supply Pool Health, such as `gmBTC-USD`, `gmETH-USD`, and `gmARB-USD`.
- Search matches the display symbol, the raw symbol, token name, and address.
- GM icons use the exact official Dolomite icon mapped to that token address.
- `dsavETH` displays as `savETH` and uses the official `savETH` icon already
  used by Dolomite Assets.
- When the active `dsavETH` isolation market is present, the obsolete base
  `savETH` duplicate is omitted from the selector.

This is a display and selection rule only. It does not rewrite token IDs,
market IDs, supply liquidity, oracle prices, interest indexes, supplier data,
or activity data.

### Obsolete market filtering

Date-coded `dPT-...-DDMMMYYYY` markets are omitted after their maturity date.
Active or future-dated markets remain selectable. Non-dated assets are not
removed solely because their current supply liquidity is zero.

The rule is deterministic code, not an environment setting. No new config or
secret is required.

### Single-select asset dropdown

The asset selector remains single-select. It removes the checkbox/checkmark
affordance that suggests multi-selection. The selected row remains clear
through restrained gold text/border treatment and accessible selected state.

### Borrow header parity

Lending Positions, Risk Simulator, and Liquidation History use the same
headline rhythm as DOLO Holders:

- 16px, weight 600 title;
- 11px monospaced count badge for table counts;
- consistent 20px/24px card-header spacing;
- 12px muted supporting description with the existing copy;
- unchanged data freshness controls and table behavior.

Count values retain their current meaning, including filtered/total counts.
Only their presentation changes.

## Accessibility and responsive behavior

- Expansion remains usable with a button and with Enter/Space on the focused
  row.
- Chain icons are decorative; the visible chain name carries meaning.
- Active dropdown options expose an accessible selected state.
- Desktop column alignment remains stable at 1440px.
- Narrow layouts may shorten the chain label visually, but must not hide the
  chain icon, asset identity, Quality, or Details control.
- Filtering and expansion must not change the ten-row table viewport height.

## Testing

Add tests before production changes for:

- the nine-column Supply Health contract and final Details cell;
- removal of Average and updated `colspan`/spacer counts;
- exact chain/address deep-link construction;
- parsing and applying a valid Supply deep link;
- fallback behavior for invalid deep links;
- unique `dGM` presentation and search by raw/display labels;
- the official `savETH` and GM icon mappings;
- removal of the obsolete `savETH` duplicate;
- expiration filtering for past and future `dPT` markets;
- absence of a checker in the single-select asset dropdown;
- DOLO-Holders-style header hover and Borrow title/count typography.

Run the targeted Node and Python contract suites, syntax checks, and a local
Playwright verification through `python3 -m http.server`. Browser verification
must inspect computed styles and bounding boxes at desktop width, exercise the
Supply Health disclosure, follow the market link, and confirm the requested
Supply asset is already selected.

## Non-goals

- No changes to Supply Health scoring, concentration methodology, supply
  history, supplier calculations, or activity calculations.
- No removal of all zero-liquidity markets.
- No redesign of the chain filter or Supply confirmation workflow outside the
  deep-link fast path.
- No cross-dashboard refactor of every token icon map.
