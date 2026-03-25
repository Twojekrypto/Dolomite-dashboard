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
- **⚠️ Leveraged borrow positions inflate yield**: The Earn tab yield formula `yield = currentWei − netFlow` breaks for leveraged accounts. Swap deltas from borrow operations create large negative netflows on the borrowed token's market. Combined with the fact that the Earn tab only shows supply-side balances (`weiVal > 0n`), a tiny remaining supply balance minus a hugely negative netflow produces an astronomical fake yield. Fix: detect markets with active `borrowPositions` from the subgraph and skip yield display for those markets.
- **⚠️ Dolomite multi-account architecture**: Each address can have multiple "accounts" (account numbers) within DolomiteMargin. The RPC `getAccountBalances(addr, accountNum)` only returns balances for ONE specific account number. Borrow positions often live on different account numbers than the primary supply account (account 0). Solution: source borrow position data from the subgraph `borrowPositions` query (which spans ALL accounts for an effectiveUser), then inject synthetic borrow entries into the enriched assets array after the netflow promise resolves.
