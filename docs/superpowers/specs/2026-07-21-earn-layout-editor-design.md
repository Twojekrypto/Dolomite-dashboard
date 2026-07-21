# EARN Supply And Borrow Layout Editor

## Goal

Provide a localhost-only editor for choosing the final desktop column layouts of the EARN `Supply Assets` and `Borrow Positions` tables. The editor lets the user reorder and resize the real table columns, optionally insert one blank spacer in each table, and export the exact two-table result for a later production-only layout change.

## Entry And Scope

- The editor is enabled only when the EARN route is opened on a loopback hostname (`localhost`, `127.0.0.1`, or `::1`) with `?layoutEditor=1`.
- A public GitHub Pages host must never initialize editor controls, local-storage persistence, drag handles, or export actions, even if its URL includes `layoutEditor=1`.
- The editor operates on the real EARN `Supply Assets` and `Borrow Positions` tables and their live wallet data.
- The work includes the requested `Supply` and `Price` presentation and the shared EARN `Details` button styling.
- It does not change EARN yield calculations, data sources, address lookup, filters, or the public production layout until the user approves the exported layout.

## Supply Assets Data Layout

The required data columns are:

`token | price | supply | balance | yield | details`

- `Price` is inserted before `Supply` and uses the canonical existing `earn_getUsdPrice(symbol, tokenAddr, chainId)` result. This is the same cached price source already used for USD balance valuation; no additional RPC or pricing request is introduced.
- The header is `Price`, values are right-aligned, use the existing mono numeric presentation, and follow the `Dolomite Assets` price precision convention.
- The existing `Supply APR` header becomes `Supply`. It retains the APR/APY toggle and source breakdown but adopts the `Dolomite Assets` supply-cell hierarchy: primary rate on the right, source lines stacked beneath it on the right, green for positive rate and red for negative rate.
- `Price` is sortable. Existing supply, balance, and yield sorting continue to work after layout reordering.

## Borrow Positions Data Layout

The required data columns are:

`health | collateral | debt | pnl | details`

Existing sort behavior, E-Mode presentation, row expansion, and detail content remain unchanged.

## Local Editor Interaction

Each table gets an independent compact toolbar visible only in the local editor:

- add one blank spacer;
- remove the blank spacer;
- reset that table to its repository default;
- export both table layouts together.

Every header has a drag handle and a right-edge resize handle. Dragging moves a complete column across its `colgroup`, header, loading rows, live data rows, and empty-state handling. A visible drop target marks the position. Header sorting remains available when the user clicks outside the drag handle.

Resizing always keeps the table at exactly 100% of available width. It draws width from the other visible columns before reaching technical minimum widths. Minimums are derived from the actual content: token and collateral/debt cells protect multi-line content, numeric cells protect full amounts, and `Details` protects the full compact button. The editor never permits negative, zero-width, or overlapping columns.

Each table may contain exactly one optional `spacer`. The spacer is resizable and movable. It is visibly marked only during local editing; removing it returns its width to the adjacent data column.

## Persistence And Export

- Valid local changes are automatically persisted in `localStorage` under one versioned EARN-editor key.
- The saved document contains a schema version and both layouts, each with `order` and widths summing to 100%.
- Invalid, incomplete, duplicate, unknown, multi-spacer, or unsupported layouts are ignored and replaced by defaults.
- `Save layout` downloads `earn-layout-draft.json` using a browser-generated `Blob`; it makes no network request.
- The exported JSON is the source of truth for the later live change. That later change will copy the selected order and widths into static table configuration and omit all editor scripts, styles, controls, and storage access.

## DOM And Rendering Requirements

- Every mutable table cell, header, and `col` receives a stable `data-column` key.
- Expanded detail rows remain one full-width cell and dynamically receive the current visible-column `colspan`.
- Skeleton, empty, footer, and spacer rows update their `colspan` without being treated as reorderable data rows.
- A mutation observer reapplies the editor layout after EARN's sort, APR/APY change, dust filter, and asynchronous rerender paths.
- The existing no-yield state must remain usable. In local editing it keeps a stable editable schema, while the later production pass preserves the normal public no-yield behavior using the final selected geometry.

## Details Button

`Supply Assets` and `Borrow Positions` use a table-scoped EARN version of the `Dolomite Assets` details control:

- 24px height, compact rounded pill, centered in its column;
- maximum width 72px with clipping protection;
- gold resting, hover, and expanded states matching `Dolomite Assets`;
- text and chevron never overflow their cell;
- accessibility labels and existing row-toggle behavior remain intact.

## Error Handling

- Editor initialization failure leaves normal EARN tables usable.
- Pointer drag and resize end cleanly on pointer cancellation, Escape, and browser blur.
- A malformed saved layout cannot change the page DOM.
- The editor does not activate outside a loopback host.

## Verification

Automated checks cover:

- schema validation for both tables;
- reordering, resize conservation, technical minimums, and the one-spacer rule;
- editor localhost/query gating and public-host rejection;
- exported two-table JSON shape;
- stable column keys in EARN markup and dynamic renderers;
- `Price`, `Supply`, and compact `Details` rendering contracts.

Browser verification uses `python3 -m http.server` and a wallet with both supply and borrow data. It checks table loading, Price and Supply presentation, sort preservation, reorder, resize, spacer add/remove, export, row details, editor persistence after rerender, and the complete absence of editor controls without the local query gate.

## Success Criteria

The user can locally arrange both real EARN tables, add one optional blank gap per table, save an exact two-table layout, and review Price, Supply, and Details styling before asking for a production-only deployment. The public EARN page remains free of editor controls until that later approval.
