# Lessons Learned — Dolomite Dashboard

## Editing & Version Control
- **Always verify edits applied**: After a cancelled or interrupted edit, re-check the file — the edit may have been reverted.
- **Check for duplicate markup**: When modifying HTML in a large file, old markup can persist alongside new markup if edits don't target the exact right lines.

## Deployment & Verification
- **GH Pages cache**: Always add a unique timestamp/version parameter to URLs when verifying deployed changes (e.g., `?v=abc123&t=<timestamp>`).
- **Wait for GH Actions**: GitHub Pages deployment can take 30-60 seconds. Don't assume changes are live immediately after push.

## CSS & Layout
- **`text-align: center` on `<td>`**: Affects ALL child content including flex containers and breakdown text. When centering some columns, target them individually (`:nth-child(2)`, `:nth-child(3)`, etc.) rather than using ranges like `:nth-child(n+2)`.
- **Column alignment independence**: Supply APR column with breakdowns should stay `text-align: left` while other data columns can be centered.

## Data Display
- **Non-oDOLO rewards bug**: Some tokens (e.g., USD1) have rewards from external protocols (WLFI) that aren't oDOLO. These must be captured as yield sources and controlled by the Yield toggle, not ignored.
- **Stablecoin formatting**: Stablecoins should show fewer decimals (1) since their values are close to $1. Volatile tokens need more precision (6 decimals).

## Browser Subagent Tips
- **Give explicit wait times**: Subagents need clear wait durations for data-loading pages. "Wait 8 seconds" works better than "wait for data".
- **Specific click targets**: "Click the ASSETS tab" can fail if the subagent clicks the wrong element. Include position hints or element descriptions.

## GitHub Pages Deployment
- **Always push to `master`**: This repo's GitHub Pages serves from `master` branch, not `main`. Always push to both: `git push origin main && git push origin main:master`.

## Table Column Restructuring
- **Check CSS `nth-child` rules**: When removing/adding table columns, CSS `nth-child` selectors silently override inline styles. Always audit ALL `nth-child` references after changing column count.
- **`data-col` on th**: If a th contains interactive elements (search input, dropdowns), remove `data-col` to prevent sort triggers from accidental clicks.

## CI/CD & Data Pipelines
- **Always cache API receipts**: Scripts that fetch on-chain tx receipts grow linearly slower over time. Always use a cache file + GH Actions `actions/cache@v4` for incremental syncing.
- **git pull --rebase -X theirs**: For automated JSON data commits, use `-X theirs` to auto-resolve merge conflicts (our freshly generated data is always newer/correct).
- **Timeout headroom**: Set `timeout-minutes` to at least 2× expected runtime to account for RPC slowness and data growth.

## UI Overflow & Dropdowns
- **`overflow:hidden` clips dropdowns**: Parent containers with `overflow:hidden` will clip absolutely-positioned child elements (like dropdown menus). Use `overflow:visible` instead when the container has interactive dropdowns.
- **CSS `:empty` to hide unused containers**: Instead of `display:none` + JS toggle, use `:empty` pseudo-class to auto-hide containers that haven't been populated yet. Avoids timing issues with JS initialization.

## ⚠️ MANDATORY: JavaScript Verification of CSS/UI Changes
- **ALWAYS verify CSS changes via browser JS console** before considering any UI fix done. Use `getComputedStyle(el).paddingLeft`, `el.getBoundingClientRect()` etc. to confirm the actual computed values match your intent.
- **Never trust inline style values alone** — CSS rules with `!important` or higher specificity can silently override inline styles. Always run `getComputedStyle()` to see what the browser actually renders.
- **Check for `!important` overrides first**: Before changing any CSS padding/margin/position, search for `!important` rules that might target the same element (e.g. `thead th:first-child` overriding a specific th). Root cause analysis saves hours of trial-and-error.
- **Measure alignment with `getBoundingClientRect()`**: When aligning elements, use JS to get exact `.left` positions of both elements rather than guessing pixel values.
- **After removing HTML elements, search for ALL JS references**: Removing an HTML element (e.g. `#m-exit-updated`) without fixing the JS that does `getElementById('m-exit-updated').textContent = ...` causes a null reference crash that can silently break entire sections downstream (e.g. the Recent Early Exits table).

## Local Testing
- **`file://` protocol blocks `fetch()`**: Modern browsers block `fetch()` for local files due to CORS. Use `python3 -m http.server` for local testing when the page fetches JSON files.
