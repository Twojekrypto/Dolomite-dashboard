# Address Highlight Visibility Design

**Date:** 2026-08-04

**Status:** Approved direction, awaiting written-spec review
**Scope:** Existing repeated-wallet address highlighting in opted-in dashboard tables

## Goal

Make repeated-address recognition immediately visible without returning to a colorful or distracting table treatment. The effect must remain attached only to the rendered address text, never to the Wallet label, table cell, or row.

## Approved visual treatment

The hovered address uses the approved **Premium Strong** treatment:

- gold-highlighted address text;
- a more opaque gold wash behind the text;
- a high-contrast one-pixel gold frame;
- a restrained outer ring and soft glow for separation from dark table backgrounds.

Other visible instances of the identical canonical address in the same opted-in table retain their normal text color and receive a quieter but clearly visible gold wash and frame. Matching remains table-scoped and activates only when at least two visible address strings share the same exact canonical EVM address.

The treatment must not add padding, borders, width, or display changes. It must therefore preserve address, cell, row, and table bounding boxes on desktop and mobile.

## Cursor behavior

Address elements must no longer display the help cursor (`?`):

- clickable address links use the standard pointer cursor;
- non-clickable address text uses the standard default arrow;
- other genuine help and tooltip controls keep their existing help cursor.

This rule applies globally to canonical address wrappers (`.addr-tooltip-wrap[data-full-addr]`) so every dashboard table using the shared address component behaves consistently.

## Existing behavior preserved

- Full-address tooltips remain available.
- The hovered address is the stronger source state; identical peers use the quieter state.
- Matching never crosses table boundaries.
- Wallet labels such as `Wallet` or `Binance` are excluded.
- Unique and malformed addresses remain unhighlighted.
- Pointer exit, focus exit, scroll, resize, and window blur clear the states.
- Reduced-motion users receive the same static visual result without animation.

## Implementation boundary

The change stays in the shared address styling and its cache keys. No table markup, data pipeline, generated JSON, workflow, metric logic, or column selector changes are required.

## Verification

1. Add a failing automated style-contract test for the stronger source/peer treatment and semantic address cursors.
2. Run the existing delegated address-matching behavior tests.
3. Browser-test one known-label table and one plain-address table at desktop and mobile widths.
4. Verify with `getComputedStyle()` that source and peer styles are more visible, help cursors are absent from addresses, cells and rows receive no address-match classes, and bounding boxes do not shift.
5. Run related route/cache contract tests, push to `master`, and wait for the GitHub Pages smoke test.
