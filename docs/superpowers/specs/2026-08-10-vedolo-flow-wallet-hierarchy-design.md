# veDOLO Flow wallet hierarchy design

## Goal

Make wallet identity in both `veDOLO Flow` tables match the dashboard's established wallet hierarchy without changing the underlying flow data, sorting, filtering, pagination, or row actions.

## Approved layout

Each wallet cell in `Recent DOLO Locks` and `Recent veDOLO Unlocks` uses two visual lines:

1. The wallet label appears first in white, using the existing verified-label resolver and `Wallet` as the current generic fallback.
2. A quieter second line contains the shortened grey address, Copy action, DeBank action, date, and transaction link in that order.

The date remains on the second line so the existing stable row height is preserved. The shortened address keeps the full-address tooltip and exact-address matching attributes already used by the dashboard.

## Implementation boundary

- Change only the wallet-cell renderer and its flow-scoped CSS in `vedolo-preview.html`.
- Reuse the existing label resolver, copy handler, DeBank URL, date formatter, transaction link, icons, and color tokens.
- Add no new dependency and perform no unrelated wallet-component refactor.
- Advance the veDOLO route cache key in `vedolo/index.html` so GitHub Pages serves the new markup immediately.

## Responsive and interaction behavior

- The label must remain the primary white text on desktop and mobile.
- The address and date may truncate with an ellipsis before the action icons become clipped.
- Copy and DeBank remain distinct clickable controls and must not trigger row behavior.
- The two-line hierarchy must not increase table width, row height, or the table shell's bounding box at desktop and phone widths.
- Both lock and unlock tables must use the same hierarchy.

## Verification

1. Add a contract test that fails against the old inline label/address layout and asserts the approved DOM order, grey address class, action order, second-line date placement, and cache-key update.
2. Run the veDOLO preview contract suite and syntax/static checks.
3. Serve the static dashboard through `python3 -m http.server` and verify `vedolo-preview.html` at desktop and mobile widths.
4. Confirm computed label/address colors, visible Copy and DeBank controls, date placement, successful Copy interaction, correct DeBank target, no relevant console errors, and unchanged row/table geometry.
5. Push the reviewed change to production `master`, wait for GitHub Pages, and repeat the cache-busted live smoke check.
