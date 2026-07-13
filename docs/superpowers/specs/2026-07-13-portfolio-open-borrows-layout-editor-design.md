# Portfolio Open Borrows Layout Editor

## Goal

Provide a local-only editor for choosing the final desktop column layout of the Portfolio `Open Borrows` table. The editor lets the user reorder and resize the real table columns, optionally insert one blank spacer column, and save the exact result for a later production change. The editor itself will not be pushed to production.

## Entry And Scope

- The editor is enabled only when `portfolio-preview.html` is opened with `?layoutEditor=1`.
- Without that query parameter, Portfolio must look and behave exactly as it does now.
- The editor operates on the real `Open Borrows` table and its real wallet data.
- Deposited Assets, veDOLO Position Activity, portfolio data loading, filtering, pagination, and risk calculations are out of scope.

## Column Model

The six data columns have stable keys:

`chain | account | health | emode | collateral | debt`

The editor may add exactly one optional `spacer` key. The spacer renders an identifiable header only while editing and blank cells in the body. It can be reordered and resized like every data column. Removing it restores its width to the nearest data column.

The saved layout contains:

- a schema version;
- the ordered list of visible column keys;
- a width percentage for every visible key;
- whether the optional spacer is present;
- the table width at which the layout was saved, for review context.

All saved widths must be finite positive numbers and sum to 100% within rounding tolerance. A layout with duplicate, unknown, or missing data keys is invalid.

## Editor Interaction

A compact local-only toolbar appears above `Open Borrows` with commands to add or remove the spacer, reset the layout, and save it.

Each header has a dedicated drag handle. Dragging the handle reorders the complete column across the `colgroup`, header, rendered data rows, and skeleton state. Empty and pagination spacer rows keep one cell and update only their `colspan`. A drop marker shows the target position. Header clicks outside the drag handle preserve existing table sorting.

Each header has a right-edge resize handle. Pointer movement changes the selected column and its nearest visible neighbor in the opposite direction, so the table always remains exactly 100% wide. If there is no column on the right, the left neighbor is used. Both columns respect minimum pixel widths converted to percentages from the current table width. Resizing stops when either minimum is reached.

The editor must reapply the chosen layout after data loading, sorting, filtering, pagination, and borrow-table rerenders.

## Persistence And Export

- Every valid edit is stored automatically in browser `localStorage` under a versioned editor-only key.
- Invalid or outdated stored data is ignored and replaced with the default layout.
- `Reset` restores the repository's current six-column order and widths and removes the spacer.
- `Save layout` validates the current state and downloads `open-borrows-layout-draft.json` using a browser-generated Blob. No API or network request is required.
- A compact success or validation state is shown in the local-only toolbar after saving.

The exported JSON is the source of truth for the later production implementation. The final production change will apply the selected order and widths and remove all editor controls and editor-only persistence.

## Responsive Behavior

The editor targets desktop layout selection. The table remains within its available desktop width throughout editing. Existing mobile behavior and mobile minimum table width remain unchanged; the final production pass will separately verify the chosen layout at desktop and mobile breakpoints.

## Error Handling

- Ignore malformed JSON and unsupported schema versions in `localStorage`.
- Reject layouts whose order, spacer count, widths, or total width are invalid.
- Clamp pointer resizing at column minimums instead of allowing overlap or negative widths.
- Cancel a drag or resize cleanly on pointer cancellation, Escape, or window blur.
- Keep the default table usable if editor initialization fails.

## Verification

Automated checks will cover:

- editor gating by `?layoutEditor=1`;
- valid and invalid layout normalization;
- one-spacer enforcement;
- reorder behavior;
- resize conservation of the 100% total and minimum widths;
- persistence and exported JSON shape;
- preservation of the current sorting and six-column default behavior.

Browser verification will use a real borrow wallet through a local HTTP server. It will confirm drag order, resize geometry, spacer add/remove, filtering and sorting rerenders, local persistence, JSON download, no table overflow on desktop, and unchanged behavior without the editor query parameter.

## Success Criteria

The user can open the local Portfolio, arrange the real `Open Borrows` table visually, optionally place one blank spacer, save the result as a local JSON file, refresh without losing work, and later provide that exact layout for a production-only static table update.
