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
- **⚠️ `.earn-hero-outer` MUST be `overflow:visible`**: This element contains the chain selector dropdown. Using `overflow:hidden` (for gradient border glow containment) will clip the dropdown menu. Instead, use `clip-path:inset(-2px round 20px)` on the `::before` pseudo-element to contain the glow effect without affecting child overflow. Also needs `z-index:200` so dropdown appears above sibling elements below.
- **CSS `:empty` to hide unused containers**: Instead of `display:none` + JS toggle, use `:empty` pseudo-class to auto-hide containers that haven't been populated yet. Avoids timing issues with JS initialization.

## ⚠️ MANDATORY: JavaScript Verification of CSS/UI Changes
- **ALWAYS verify CSS changes via browser JS console** before considering any UI fix done. Use `getComputedStyle(el).paddingLeft`, `el.getBoundingClientRect()` etc. to confirm the actual computed values match your intent.
- **Never trust inline style values alone** — CSS rules with `!important` or higher specificity can silently override inline styles. Always run `getComputedStyle()` to see what the browser actually renders.
- **Check for `!important` overrides first**: Before changing any CSS padding/margin/position, search for `!important` rules that might target the same element (e.g. `thead th:first-child` overriding a specific th). Root cause analysis saves hours of trial-and-error.
- **Measure alignment with `getBoundingClientRect()`**: When aligning elements, use JS to get exact `.left` positions of both elements rather than guessing pixel values.
- **After removing HTML elements, search for ALL JS references**: Removing an HTML element (e.g. `#m-exit-updated`) without fixing the JS that does `getElementById('m-exit-updated').textContent = ...` causes a null reference crash that can silently break entire sections downstream (e.g. the Recent Early Exits table).
- **⚠️ grep ALL IDs before removing elements**: When converting a dropdown to pills (e.g. `risk-dropdown` → inline `risk-pills`), grep for ALL references across click handlers / toggle / close logic. Missing even 1 stale `getElementById('risk-dropdown')` will break OTHER dropdown handlers that try to close it (because null.classList throws).

## Local Testing
- **`file://` protocol blocks `fetch()`**: Modern browsers block `fetch()` for local files due to CORS. Use `python3 -m http.server` for local testing when the page fetches JSON files.

## Dolomite E-Mode & Risk Overrides
- **Never assume global liquidation ratio applies to all accounts**: Dolomite uses E-Mode (Automatic Efficiency Mode) for correlated asset pairs (e.g., stablecoin↔stablecoin). E-Mode uses a lower liq ratio (111.11% vs 115%) and zeroes margin premiums. This must be queried via RPC `getAccountRiskOverride()` on the `defaultAccountRiskOverrideSetter` contract.
- **Subgraph doesn't expose per-account risk overrides**: The `MarginAccount` entity has no `marginRatioOverride` field. E-Mode overrides live on the smart contract level only.
- **Use web3.py with proper ABI, not raw eth_call**: For struct parameters like `Account.Info`, use `w3.eth.contract(abi=...).functions.X().call()` — manual ABI encoding of struct tuples is error-prone.
- **⚠️ E-Mode RPC: use `user.id`, NOT `effectiveUser.id`**: `getAccountRiskOverride((owner, number))` requires the actual on-chain owner (vault/proxy = `user.id`), not the wallet behind it (`effectiveUser.id`). For vault accounts like iBGT isolation mode, `effectiveUser` returns marginRatioOverride=0 while `user` correctly returns 0.333. Bug pattern: all vault/proxy accounts in e-mode will have incorrect (inflated-risk) health factors if `effectiveUser` is used.
- **⚠️ NEVER use bare `except: pass` on RPC calls**: Silent exception swallowing on rate-limited RPCs caused 170+ Ethereum E-Mode positions to be missed (2/347 → 173/347). Multicall3 with 200 calls → 413 "Payload Too Large" → individual fallback → 429 rate limit → ALL silently failed. Fix: smaller batch sizes (50), retry with exponential backoff (1-3s), inter-batch delays, and proper error counting.

## Data Pipeline Resilience
- **Silent chunk failures on RPC → empty chain data**: `eth_getLogs` scanning 15M+ blocks on Berachain with rate-limited RPCs causes chunks to silently fail, producing `total_transfers=0`. Always count failed chunks and add defensive fallback: if fresh scan returns 0 but cache has data, use cached data.
- **Default period must show data**: If a table uses a configurable period (7d/30d/ALL), default to a period that shows existing data. oDOLO Exercises had latest data from 2025-08-07 but defaulted to 7 days → permanently empty. Default to ALL.
- **⚠️ Smart contracts can have MULTIPLE method IDs for the same action**: The oDOLO Vester contract has TWO exercise methods (`0xa88f8139` original, `0xf3621c90` newer). The pipeline only tracked the first, missing 51% of recent exercises. Always check for method ID variants when a data pipeline suddenly shows stale data.
- **⚠️ Cross-chain bridge transfers inflate flow data**: When the same address bridges tokens between chains (ETH↔Bera), per-chain flow calculations count it as outflow on source and inflow on destination. Must neutralize same-address opposing flows across chains — otherwise holders appear as massive sellers. Use `neutralize_cross_chain_flows()` pattern: detect opposing flows on multiple chains, cancel the min overlap.
- **⚠️ Bridge mints/burns invisible to flow calc**: `calculate_flows()` skips transfers from/to 0x0 (mints/burns). Bridges use burn→0x0 on source and mint←0x0 on destination, making one side invisible to neutralization. Fix: `calculate_bridge_flows()` extracts mint/burn amounts separately, augments raw_flows before neutralization runs, then delta is applied back to original flows. This way bridge patterns become visible without polluting overall flow semantics.
- **⚠️ Fallback data hides real sell-offs**: When balance RPC returns 0, a holders fallback was overriding with stale cached data. This made addresses that sold ALL tokens appear to still hold them. Fix: only apply fallback for addresses where RPC actually FAILED (network error), not for legitimate zero balances.
- **⚠️ Leveraged borrow positions inflate yield**: The Earn tab yield formula `yield = currentWei − netFlow` breaks for leveraged accounts. Swap deltas from borrow operations create large negative netflows on the borrowed token's market. Fix: skip **netflow-based** yield for borrow markets, but allow **snapshot-based** yield through (snapshot method is immune to swap deltas since it uses subgraph index data). The old blanket `if (isBorrowMarket)` block incorrectly hid ALL yield for borrow-market assets (e.g., USDC collateral showing "—" despite having $48+ in earned yield).
- **⚠️ Dolomite multi-account architecture**: Each address can have multiple "accounts" (account numbers) within DolomiteMargin. The RPC `getAccountBalances(addr, accountNum)` only returns balances for ONE specific account number. Borrow positions often live on different account numbers than the primary supply account (account 0). Solution: source borrow position data from the subgraph `borrowPositions` query (which spans ALL accounts for an effectiveUser), then inject synthetic borrow entries into the enriched assets array after the netflow promise resolves.

## External Rewards (oDOLO + MERKL WLFI)
- **MERKL v3/rewards API**: Use `https://api.merkl.xyz/v3/rewards?chainIds={id}&user={addr}` to get exact WLFI earned. Filter `campaignData` by reason keys starting with `Dolomite_`. The reason key format `Dolomite_{tokenAddr}` allows per-token attribution.
- **oDOLO metadata API**: `https://api.dolomite.io/liquidity-mining/odolo/metadata` returns per-token weekly allocation (`allChainWeights`). Calculate per-position oDOLO as `(userSupply / totalSupply) × weeklyAlloc × weeksActive`.
- **⚠️ WLFI blacklist**: Some wallets (e.g., `0x5be9...`) are blacklisted in MERKL campaigns. They appear in the `params.blacklist` array of campaign data and receive 0 WLFI despite supplying the eligible token.
- **⚠️ Per-position attribution**: MERKL rewards are wallet-wide, NOT per-position. Use `perToken` matching (reason key → collateral token address) to correctly attribute rewards to specific borrow positions. This prevents double-counting when a wallet has multiple positions.
- **⚠️ Summary card pattern**: Dynamic summary stats should only appear when data exists. Use `if (data > threshold) return;` early exit + `getElementById()` dedup check to prevent duplicate cards on re-render.

## Grid Layout & Inline Style Overrides
- **Inline `style.gridTemplateColumns` ALWAYS wins over CSS classes**: When JS sets `el.style.gridTemplateColumns = 'repeat(5,1fr)'`, it overrides any CSS class like `.cols-6 { repeat(3,1fr) }`. Solution: never use inline grid overrides — manage grid layout ONLY through CSS classes toggled via JS.
- **Symmetric card layouts**: For 6 cards use `repeat(3,1fr)` → 3+3 rows. For 7 use `repeat(4,1fr)` → 4+3. Centralize layout management in a single function (`earn_updateSummaryCardLayout`) that reads child count and applies the right class.

## Fixed-Position Dropdowns in Overflow Containers
- **`position:absolute` + `overflow:auto` = CLIPPED dropdowns**: When a parent container has `overflow:auto` (e.g., `.table-container`), any `position:absolute` child extending beyond the container bounds will be clipped. This is especially visible when the table has few rows (e.g., after filtering).
- **Solution: `position:fixed` + JS positioning**: Switch dropdowns from `position:absolute` to `position:fixed` and calculate `top`/`left` from `getBoundingClientRect()` of the trigger button. Use a helper like `positionPopoverFixed(triggerEl, popoverEl)`.
- **Clamp to viewport**: Always clamp fixed-position dropdowns to viewport bounds: `if (left + width > window.innerWidth - 8) left = window.innerWidth - width - 8`.

## Portfolio Value Calculation
- **Include borrow equity in Portfolio Value**: The EARN tab previously only summed supply-only assets. For users with borrow positions (margin trading), the true portfolio value = supply assets + (collateral − debt). Update the Portfolio Value card in `earn_updateSummaryDebt()` when lending positions arrive.

## Liq Monitor — Filter Popovers (March 2026)
- **Mouseleave auto-close**: Token/range filter popovers auto-close with 400-500ms delay when mouse leaves the popover. Use `mouseleave`/`mouseenter` on both the popover element AND the trigger button to cancel the timer when the mouse returns.
- **Selected tokens at top**: In `renderList()`, split the `sorted` array into `selectedItems` and `unselectedItems`, then concatenate `[...selectedItems, ...unselectedItems]`. Add a visual separator (thin cyan line) between sections.
- **Clear X buttons**: Add `.col-filter-clear` button next to each `.col-filter-btn`. Use CSS sibling selector `.col-filter-btn.has-active ~ .col-filter-clear { display: inline-flex; }` to show only when filter is active.
- **Protocol Info card**: Can be reused across pages by putting the rendering logic in an IIFE and using standalone class names (`.liq-proto-links`, `.liq-proto-contracts`) instead of sharing the same class as index.html.
- **Table container clipping**: When table has few rows (e.g., after dpglv filter), popovers can appear clipped. Add `min-height: 350px` to `.table-container` as a safety net. The `position: fixed` + `positionPopoverFixed()` pattern already handles viewport clamping.

## Earn Tab — Hide Dust Toggle
- **⚠️ Hardcoded filters ignore toggle state**: The `earn_renderResults` function had a hardcoded `a.usdValue >= 0.01` filter that always removed dust from the supply table, regardless of the `earn_showDust` toggle. The toggle correctly flipped the variable and updated CSS, but the filter never checked it. Fix: use `const dustThreshold = earn_showDust ? 0 : 1` so the threshold is dynamic. Pattern: always grep for ALL hardcoded thresholds when a filter toggle "doesn't work" — the toggle logic may be fine, but the render path may bypass it.
- **⚠️ Dust threshold = $1, not $0.01**: User expects positions below $1 to be considered dust, not $0.01. The threshold must be consistent across: main supply/borrow filter, history render, withdrawn assets, inline withdrawn section, and dust count calculation.
- **⚠️ Borrow-only addresses show "No deposits"**: When `balances.length === 0` (no supply on account 0), the code early-returned before the subgraph borrow data could be injected (borrow fetch runs in `earn_fetchNetflow()` which was started AFTER the balance check). Fix: when no supply balances, call `earn_fetchNetflow()` first to check for borrows, then proceed with borrow-only render path if found.
