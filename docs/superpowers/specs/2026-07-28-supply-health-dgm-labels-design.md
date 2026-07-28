# Supply Pool Health dGM Labels Design

## Goal

Make every Dolomite GM market in Supply Pool Health immediately identifiable while preserving the raw subgraph data and all existing calculations.

## Approved UX

- Resolve a display symbol from `chain + tokenId`, for example `gmBTC-USD`, `gmETH`, or `gmSOL-USD`.
- Show `Dolomite GM Market` as the secondary label.
- Keep the raw `dGM` symbol, token address, market ID, supply, wallet, concentration, growth, and score data unchanged.
- Let search match both the specific display symbol and the raw `dGM` symbol.
- Fall back to the raw symbol and name for unknown or future markets instead of guessing.

## Architecture

The presentation mapping lives in `tvl/supply-health.js`, next to the table consumer. A pure `getSupplyHealthMarketPresentation(market)` function returns the display symbol and name without mutating the market. Filtering and rendering consume that function; row keys and calculations continue using the original `chain`, `tokenId`, and market fields.

## Validation

- Unit-test known mappings, unknown-market fallback, and search by the resolved GM market name.
- Assert that every currently published `dGM` row resolves to a unique display symbol.
- Browser-test the live table after searching for `gmBTC-USD` and `dGM`.
- Bump the Supply Pool Health script and TVL route versions to bypass GitHub Pages cache.

