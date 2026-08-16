# veDOLO Filter and Position Activity Table UX

## Goal

Make the veDOLO route and activity filters use the same Graphite + Gold dropdown language as the `All Chains` control in Portfolio `Deposited Assets`, while turning Position Activity into a complete standalone table.

## Approved interaction model

- Route and activity filters are single-select dropdowns because their options are mutually exclusive.
- The route dropdown offers `All routes`, `via oDOLO`, and `Direct`.
- The activity dropdown offers `All activity`, `Transfers`, `Merges`, `Splits`, and `Extensions`.
- Each trigger uses the existing 36px dropdown geometry, icon, selected-count badge, chevron, gold open/filtered state, menu header, check indicator, keyboard-safe button semantics, and outside-click dismissal.
- Selecting an option updates the label immediately, closes the menu, resets the affected page, and rerenders only its own table.

## Standalone Position Activity table

`veDOLO Position Activity` becomes a sibling card after `veDOLO Flow`, not a nested section within it. It has its own header, update metadata, toolbar, search state, action filter, stable body, footer, and pager.

The search matches wallet address, canonical wallet label, transaction hash, source position ID, and target position ID. The clear button follows the DOLO Holders search UX. Flow search and period changes no longer alter Position Activity.

The table displays 10 rows per page. Column widths are fixed and intentional: compact rank, generous wallet identity, compact action, readable position transition, then right-aligned DOLO and lock end. Narrow screens retain the card width and scroll only the table wrapper horizontally.

## Scope and validation

No blockchain data, calculations, source classification, default sort order, transaction links, or generated artifacts change. Contract tests cover standalone markup, independent state, dropdown option wiring, search behavior, and 10-row pagination. Browser QA covers dropdown interaction, search/clear, pagination height, desktop/laptop/mobile geometry, and page overflow.
