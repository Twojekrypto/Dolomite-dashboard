# Rewards search and investor wallet labels

## Scope

Add a consistent local search toolbar to the Live Programs and Ended Programs tables, and classify DOLO investor wallets from verified distribution-contract transfers. Audit the existing Core Team labels and stop investor recipients from being classified as team wallets.

## Rewards search UX

- Live Programs and Ended Programs each get an independent search field.
- Search matches the visible program name plus campaign/provider, market token, reward token, and network.
- Filtering is immediate and local; it does not refetch program data.
- A clear `×` resets the field, restores all rows, and keeps focus in the input.
- The title count shows `N programs` without a query and `N of M programs` while filtering.
- Both cards use the same header separator and toolbar hierarchy. On desktop, Live search is left and APR/APY is right; on mobile controls stack at full width.
- No-match states distinguish an empty search result from a genuinely empty program dataset.

## Investor classification

Classification is based only on Berachain DOLO `Transfer` rows whose sender is one of the official Berachain claim contracts. An identical address on another chain is not accepted as evidence:

- Strategic Investor Claims (`0x7eFD…1176`) -> `Early Investor`.
- Investor Claims (`0x3a02…6C07`) -> `Investor`.
- If a wallet received from both, `Early Investor` remains the primary label and metadata records the additional long-term investor tranche.

The claim contract is evidence of allocation provenance, not of a wallet's current owner or current DOLO balance. Existing higher-priority labels such as exchange, protocol, pool, or manually verified Core Team labels are not overwritten.

## Core Team audit

The current generator incorrectly copies every `Investor Claims` recipient into `team`. That coupling is removed and covered by a regression test. The eight existing static `Core Team 1–8` labels remain because they come from a separate token-distribution classification and are not produced by the faulty investor rule. The two `Core Team Allocation Relay` contracts also remain protocol relays, not personal-wallet labels.

Until a separately verified official team-claim source exists, the generated `team` array remains empty. The UI retains backward compatibility with the array but rejects the legacy case where `team` is identical to `investors`.

## Shared delivery

`dolo-address-labels.js` owns the merge/load logic and caches the JSON fetch. Existing wallet-table pages consume the same merged labels so the same address receives the same classification across DOLO, Earn, Borrow/Liquidation, Supply activity, oDOLO, veDOLO, and Revenue wallet views.

## Data contract and failure behavior

`vesting_investors.json` keeps the existing arrays for compatibility and adds structured wallet provenance records generated from transfer history. Invalid addresses are ignored. A failed optional label fetch leaves base labels intact and logs a warning; it never blocks primary dashboard data.

## Verification

- Unit tests for source-contract classification, overlap priority, integer DOLO totals, and empty generated team list.
- JavaScript tests for shared label merging, legacy-team guard, and search matching.
- Existing dashboard contract suites.
- Browser verification at desktop and mobile widths, including computed toolbar dimensions, clear behavior, count text, and card stability.
