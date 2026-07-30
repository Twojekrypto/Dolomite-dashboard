# Supply Health Market Dossier Design

## Goal

Turn the expanded `Details` content in Supply Pool Health into a calm,
institutional market dossier and make token presentation visually identical to
Dolomite Assets without changing any market data, scoring, filtering, or
navigation behavior.

## Design direction

The selected direction is an **institutional market dossier**. It is preferred
over:

- a minimal polish of the existing three columns, which would leave the weak
  hierarchy and disconnected surfaces in place;
- a dense technical inspector, which would expose the same data but make the
  table harder to scan.

The dossier reads from identity to judgment to evidence:

```text
┌─ gold quality spine ─────────────────────────────────────────────────┐
│ [token] ASSET / CHAIN    Market intelligence     SUPPLY   QUALITY  C │
├───────────────────────┬───────────────────────┬──────────────────────┤
│ Quality anatomy       │ Market momentum       │ Supply concentration │
│ weighted score bars   │ 30D change + signals  │ ranked suppliers      │
│                       │                       │ share bars + action    │
└───────────────────────┴───────────────────────┴──────────────────────┘
```

## Visual system

### Palette

Use the existing Graphite + Gold tokens only:

- Obsidian `#09090b`
- Graphite `#0f0f11`
- Raised graphite `#141417`
- Primary white `#f4f3ef`
- Muted stone `#a5a29a`
- Dolomite gold `#c9a227`

Green `#75b87b` and muted red `#b4796b` are reserved for signed movement.
They must not become decorative section colors.

### Typography

- Inter remains the interface and section-heading face.
- JetBrains Mono remains the utility and numeric face.
- Eyebrows use compact uppercase mono labels.
- Market identity and section titles use Inter with restrained weight.
- Values use tabular monospaced numerals.

No new font or dependency is added.

### Signature

The single signature element is the **quality spine**: a subtle gold rail that
continues from the selected parent row into the inset dossier and terminates in
the quality score/grade block. It represents continuity between the table
judgment and its evidence. All other decoration stays quiet.

## Expanded dossier

### Shell

- The detail row keeps the table background and fixed scroll viewport.
- An inset panel receives a fine top gold edge, a left quality spine, a dark
  graphite gradient, and restrained inner highlight.
- The panel must not create horizontal overflow at desktop or mobile widths.

### Identity header

The header contains:

- the canonical token icon in the same 30px frame treatment used by Dolomite
  Assets;
- canonical displayed symbol and readable market name;
- a compact chain badge with network icon;
- current supply;
- quality score and grade.

The icon resolver receives the resolved market symbol plus exact chain and
token address. The wrapper applies the same full-logo and grayscale
classification as Dolomite Assets. The same icon presentation is reused in
the table row and dossier header.

### Quality anatomy

Keep the five existing weighted score components. Improve hierarchy by showing:

- component label;
- weight;
- score bar;
- numeric score.

The bars stay gold. Missing scores remain explicitly unavailable.

### Market momentum

`30D Supply Change` is the lead signal and spans the section width. The
remaining existing signals stay in a compact two-column ledger:

- Median / Wallet
- Gini Coefficient
- 7D Supply
- 30D Wallets
- Avg Daily Move 30D
- Exit Impact

Only signed changes receive green/red text.

### Supply concentration

Show the three largest suppliers as ranked rows with:

- shortened explorer-linked address;
- exact share;
- USD value;
- a quiet share bar beneath the row.

The existing exact-address Supply link remains the final action, renamed
`Open Supply market` and given a directional arrow.

## Responsive and accessible behavior

- At wide desktop sizes the dossier uses three balanced columns.
- Below 1100px the concentration section may span the full second row.
- At 840px and below the sections stack in reading order.
- The header wraps without clipping, and the action becomes full width on
  narrow screens.
- Links and controls retain visible keyboard focus.
- Reduced-motion users receive no nonessential transitions.
- Token images are decorative because adjacent text carries the identity.

## Icon parity contract

Supply Pool Health must:

- resolve icons using the canonical displayed symbol, chain, and address;
- use the same 30px circular frame, border, object-fit, grayscale, and
  full-logo behavior as Dolomite Assets;
- reuse that exact icon presentation in both the collapsed row and expanded
  dossier;
- preserve the official savETH, dARB, GM, and other address-specific mappings
  already present on the TVL page.

This is visual/presentation parity only. It does not refactor the cross-page
icon registries or change raw token metadata.

## Testing and acceptance

- Add a pure icon-presentation helper test proving the resolved symbol and
  exact chain/address are supplied to the real resolver boundary.
- Add a renderer test proving the dossier contains identity, supply, quality,
  the three evidence sections, supplier share bars, and the exact Supply link.
- Add source contracts for the Assets-matched frame classes and responsive
  dossier layout.
- Run targeted Node and Python tests plus `node --check`.
- Serve with `python3 -m http.server`.
- In Playwright, expand a real row and inspect computed styles and bounding
  boxes at desktop and mobile widths.
- Verify the detail panel has no horizontal overflow, icon geometry is 30px,
  columns are balanced on desktop, and stacked on mobile.

## Non-goals

- No changes to Supply Health scores, weights, grades, concentration
  methodology, supplier data, or growth calculations.
- No changes to sorting, pagination, filters, row counts, or table columns.
- No new data fetch, library, configuration, or secret.
- No broad rewrite of the duplicated token icon maps across preview pages.
