# DOLO Liquidity Providers Active-Only Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a live active-only DOLO liquidity table with a permanent $10,000 pool-quality floor, focused filters, compact token amounts, explanatory statuses, and Dolomite Assets-style Details.

**Architecture:** Keep the change inside the existing static `dolo-preview.html` component and its existing generated artifact. Filter the artifact at the UI boundary with one fail-closed pool eligibility helper, derive Pair/DEX options from the same eligible pool set, and preserve precise raw token data for the expanded panel. Extend the existing Node contract suite before production edits and verify rendered behavior with local HTTP plus Playwright.

**Tech Stack:** Static HTML/CSS/JavaScript, generated JSON, Node `node:test`, Python HTTP server, Playwright, GitHub Pages.

## Global Constraints

- Preserve Graphite + Gold and reuse Dolomite Assets visual patterns.
- Do not modify generated liquidity calculations or wallet classification.
- Pool eligibility requires finite `liquidityUsd >= 10000`.
- Position dust remains independently controlled by `Hide dust` at `$10`.
- Keep ten visual row slots and nine active-position columns.
- Use official token icon registry values with explicit DOLO/paired fallbacks.
- Verify desktop and mobile before pushing to `master`.

---

### Task 1: Lock active-only filtering and interaction contracts

**Files:**
- Modify: `tests/dolo-liquidity-ui.test.js`
- Modify: `dolo-preview.html`

**Interfaces:**
- Consumes: `data.pools`, `data.activePositions`, `poolFor(row)`, and current filter state.
- Produces: `eligiblePools() -> Pool[]`, `poolPassesLiquidityFloor(pool) -> boolean`, `renderDexFilter()`, and active-only rendering.

- [ ] Write failing Node tests requiring the `$10,000` floor, removal of mode/history/low-liquidity controls, and an exclusive `All DEXes` option.
- [ ] Run `node --test tests/dolo-liquidity-ui.test.js` and confirm the new assertions fail for the intended missing behavior.
- [ ] Replace the optional low-liquidity branch with `MIN_POOL_LIQUIDITY_USD = 10000` and a finite fail-closed eligibility helper.
- [ ] Remove the History mode, its state, controls, renderer, and listeners while keeping active pagination stable.
- [ ] Make Pair and DEX option lists derive from eligible pools for the selected chain; represent All as an empty selection set.
- [ ] Run the focused test and confirm the filtering/interaction contracts pass.

### Task 2: Build compact token cells and explanatory Assets-style Details

**Files:**
- Modify: `tests/dolo-liquidity-ui.test.js`
- Modify: `dolo-preview.html`

**Interfaces:**
- Consumes: exact raw integer token amounts, `dolomite-token-icons.generated.js`, pool metadata, source metadata, and shared tooltip handling.
- Produces: `compactRawAmount(raw, decimals) -> string`, `tokenAmountCell(...) -> string`, `statusPresentation(value) -> {label, explanation}`, and the new `detailRow(row)` hierarchy.

- [ ] Add failing contracts for compact amounts with icons, tooltip explanations, a gold freshness pulse, and the Assets-style details structure.
- [ ] Run the focused test and confirm RED.
- [ ] Load the shared official icon registry and map `WBERA -> BERA` and `USDC.e -> USDC` only as icon aliases.
- [ ] Implement BigInt-safe compact token formatting and keep `exactRawAmount` in Details.
- [ ] Add plain-English explanations for `verified`, `partial`, `stale`, `unavailable`, `in range`, and `out of range` statuses.
- [ ] Replace the details grid with a header, metadata pills, four exact overview metrics, and a plain-language evidence grid.
- [ ] Render the standard gold pulse for successful freshness metadata.
- [ ] Run the focused test and confirm GREEN.

### Task 3: Verify, publish, and confirm live deployment

**Files:**
- Modify: `index.html`
- Modify: `dolo/index.html`
- Verify: `dolo-preview.html`

**Interfaces:**
- Consumes: completed active-only UI.
- Produces: refreshed route cache keys and a live GitHub Pages deployment.

- [ ] Bump the liquidity route/data cache keys in the focused contract first and confirm RED.
- [ ] Update only the matching cache references in `dolo-preview.html`, `index.html`, and `dolo/index.html`.
- [ ] Run focused Node tests, inline-script syntax compilation, related `nth-child` audit, and `git diff --check`.
- [ ] Serve with `python3 -m http.server` and verify at 1440x900 and 390x844 with Playwright: pool floor, All DEXes behavior, token icons, status tooltip, expanded Details, ten slots, and zero document overflow.
- [ ] Review the diff for correctness, maintainability, security, and unrelated changes.
- [ ] Commit only scoped files, rebase on the current remote `master`, rerun focused checks, and push `HEAD` to production `master`.
- [ ] Wait for GitHub Pages and live smoke verification before reporting completion.
