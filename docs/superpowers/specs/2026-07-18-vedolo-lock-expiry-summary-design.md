# Lock Expiry Timeline Summary UX

## Goal

Reduce the vertical space used by the Lock Expiry Timeline summary while keeping the total scheduled DOLO easy to scan and consistent with the veDOLO dashboard UX.

## Layout

- Add the shared `card-meta` update indicator to the top-right of the card header.
- Source its age from `vedolo_expiry.json.timestamp` and format it through the existing `dataUpdatedLabel()` helper.
- Keep the wallet search in the existing toolbar.
- Remove the full-width `expiry-focus` panel above the chart.
- Add a compact, static summary in the top-right of the chart area with:
  - label: `Total scheduled`
  - total DOLO value
  - supporting text with the number of visible expiry buckets

## Interaction

- The summary always shows the total for the current view. A wallet search may change that total, but hovering or selecting a bar must not change it.
- Bars retain hover, focus, click highlighting, and the existing tooltip.
- The chart summary must not intercept bar interaction.

## Responsive Behavior

- On desktop, the summary sits inside the chart area at the top-right.
- On narrow screens, it becomes a compact row above the horizontally scrollable bars so it cannot cover chart content.
- Text must remain inside its container without increasing the card width.

## Verification

- Add contract tests for the metadata element, timestamp binding, static summary markup, and removal of the dynamic focus panel.
- Verify computed position, dimensions, overlap, and metadata text in a local browser on desktop and mobile.
- Run the veDOLO contract suite and the full audit checks before deployment.
