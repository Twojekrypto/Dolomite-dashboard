# DOLO Holder Distribution Layout Editor

## Goal

Provide a local-only Excel-like editor for the DOLO Holder Distribution legend and its expanded wallet Details table. The user can reorder and resize columns, add one optional blank spacer to each layout, and save both layouts together for a later production-only change. Details will also gain address and label search plus an explicit Chain-first view.

## Scope And Entry

- The editor is enabled only when `dolo-preview.html` is opened with `?layoutEditor=1`.
- The production view must not load editor controls or editor persistence.
- The editor controls two independent layouts:
  - holder distribution legend: `group | balance | wallets | change | details`;
  - wallet Details table: `chain | address | dolo | change`.
- Each layout may contain at most one `spacer` column.
- The existing market-holder scope, selected date range, veDOLO toggle, chart behavior, Details panel, and explorer/copy actions remain unchanged.

## Details Search And Chain UX

- An open Details panel shows a compact search field based on Fresh 10K+ DOLO Wallets, including the visible `X` clear command.
- Search filters only the rows inside that open Details panel and matches a wallet address or known label, case-insensitively.
- The new Chain column replaces the rank (`#`) column and is placed first by default.
- Chain cells reuse the Fresh Wallets Ethereum and Berachain icon-plus-name chip treatment. A wallet on both chains shows both chips. Unknown coverage remains visibly labelled rather than inferred.
- Search state belongs to the active details bucket and is preserved while that panel rerenders for the same selected range; closing the panel clears its temporary search state.

## Column Model

Each editor layout is versioned and has this shape:

```json
{
  "version": 1,
  "order": ["chain", "address", "dolo", "change"],
  "widths": {"chain": 16, "address": 44, "dolo": 22, "change": 18}
}
```

- Every required data key must appear exactly once.
- `spacer` is optional and may appear once only.
- Every visible width is finite and positive; all widths total `100%` within a rounding tolerance.
- Per-column pixel minimums prevent text overlap. Resizing a column compensates its nearest visible neighbour and clamps at both minimums.
- Reordering moves the matching `col`, header, data cell, skeleton/empty state, and spacer cell as a single unit.
- The legend uses stable `data-column` keys on its header and every rendered row so the same model applies to its grid-based structure.

## Editor Interaction And Persistence

- Local editor toolbars appear above the holder legend and inside the open Details panel.
- Every header gets a dedicated drag handle and a right-edge resize handle. Header clicks outside the drag handle keep their existing behavior.
- Commands: `Add spacer`/`Remove spacer`, `Reset`, and `Save layout`.
- Edits persist automatically in versioned `localStorage` keys while editing.
- `Save layout` validates both layouts and downloads one browser-generated JSON document containing `holderDistribution` and `holderDetails` layouts.
- Invalid, stale, duplicate, incomplete, or over-wide saved layouts are ignored and restored to their default layout.
- The editor reapplies its layout after chart redraws, Details opening, search filtering, date-range changes, and lazy wallet-history loading.

## Responsive And Error Behavior

- The editor is for desktop layout selection. Both edited views remain within their available desktop width.
- Existing mobile single-column legend behavior and horizontally scrollable Details table behavior remain intact.
- Drag/resize cancels cleanly on Escape, pointer cancellation, or window blur.
- If editor initialization fails, the regular table/legend remains usable.
- Details rows that do not match the active search show the existing empty-state styling and do not change source data.

## Verification

- Contract tests cover query gating, default layouts, invalid-layout rejection, one-spacer enforcement, reorder behavior, resize width conservation and minimums, persistence/export shape, Details search matching, Chain-first markup, and rank removal.
- Static checks validate inline JavaScript syntax and `git diff --check`.
- Browser verification through `python3 -m http.server` confirms desktop drag/reorder, resize, spacer add/remove, search and clear, Chain chips, persistence after refresh, saved JSON, no overlap, and production behavior without `?layoutEditor=1`.

## Success Criteria

The user can open the local DOLO page with `?layoutEditor=1`, choose the order and widths of both the main distribution legend and a Details table, add one movable blank spacer to either, search current wallet Details by address or label, save one reproducible JSON layout draft, and later apply that layout to the live dashboard without exposing editing tools.
