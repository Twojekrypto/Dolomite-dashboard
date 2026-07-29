# Supply Pool Health Details Button Design

## Goal

Reduce visual density in Supply Pool Health by moving `30D Change` from the
collapsed table into the expanded market signals and making the `Details`
control match the established Dolomite Assets pill.

## Table contract

- The collapsed table has eight columns: Chain, Asset, Supply, Suppliers,
  Top 10, Largest, Quality, and Details.
- `30D Change` is no longer sortable because it is no longer a table column.
- All detail, empty, and spacer rows use `colspan="8"`.
- Desktop widths and every responsive `nth-child` selector are updated for the
  eight-column layout. At 840px and below, Chain, Asset, Quality, and Details
  remain visible.

## Expanded details

`Market Signals` gains a prominent `30D Supply Change` metric. The value uses
the same signed percentage formatter and positive/negative tone as the former
collapsed cell. Existing signals remain unchanged.

## Details control

The last column uses the Dolomite Assets interaction pattern:

- compact gold pill with `Details` or `Hide`,
- downward SVG chevron that rotates when expanded,
- matching height, radius, typography, hover, focus, and expanded states,
- dynamic `aria-label` and `aria-expanded`.

The entire data row remains keyboard- and pointer-expandable.

## Verification

- Contract tests prove the eight-column markup, relocated 30D metric, correct
  colspans, responsive selectors, and Assets-style button structure.
- Browser QA verifies computed button geometry, table width/overflow, visible
  columns, expansion height behavior, and placement/color of the 30D metric.
