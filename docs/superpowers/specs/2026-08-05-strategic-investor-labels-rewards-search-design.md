# Strategic investor labels and Rewards search parity

Date: 2026-08-05

## Goal

Make investor labels accurate, durable, and understandable across the dashboard, while making the Live Programs and Ended Programs search controls visually identical to the DOLO Holders search pattern.

## Evidence and classification

Dolomite documentation identifies these Berachain contracts:

- `0x7efd088ae500598a19a242d6d48b9f7e0d061176` — Strategic Investor Claims
- `0x3a025c7fcf7632197ea82e64acd6ff53e1c06c07` — Investor Claims

The initial funding transaction on 2025-04-24 transferred exactly:

- `13,933,333 DOLO` to Strategic Investor Claims, matching the documented 1.3933% investor tranche that vests during the first 12 months;
- `137,913,000 DOLO` to Investor Claims, matching the documented 13.7913% tranche with a one-year cliff and three-year vesting;
- `151,846,333 DOLO` in total, equal to 15.1846333% of the one-billion-token initial supply and matching the documented 15.1846% investor allocation after rounding.

The official $900K strategic round announcement is dated 2024-03-19. The documentation does not explicitly state in one sentence that the 1.3933% tranche belongs to that round. The combination of the official Strategic Investor Claims contract name, exact tranche funding, and schedule is treated as a high-confidence attribution rather than an independently documented identity claim.

The Core Team labels remain valid. The allocation flow transferred exactly `202,075,000 DOLO`, matching the documented 20.2075% Core Team allocation, to the primary team allocation wallet before distribution to the other known team wallets. Public CoinGecko tokenomics labels the same wallet cluster as Core Team.

## Label design

### Strategic tranche recipients

- Primary visible label: `Strategic Investor`
- Tooltip title: `2024 strategic round · $900K`
- Tooltip provenance: `Received DOLO from the official Strategic Investor Claims contract.`
- Attribution status: high-confidence on-chain attribution

The existing `Early Investor` label is removed because the $2.5M round from 2023 predates the 2024 strategic round and makes “early” chronologically ambiguous.

### Long-term tranche recipients

- Primary visible label: `Long-term Investor`
- Tooltip title: `3-year vesting · 1-year cliff`
- Tooltip provenance: `Received DOLO from the official Investor Claims contract.`

### Overlapping recipients

If a wallet receives DOLO from both official claim contracts:

- primary visible label remains `Strategic Investor`;
- tooltip also states `Also received the long-term investor tranche.`

### Contracts and team wallets

- The claim contract itself remains `Strategic Investor Claims`.
- The long-term contract is labeled `Investor Claims` rather than a wallet-level investor label.
- Existing confirmed `Core Team 1–8` labels remain unchanged.
- No recipient of Investor Claims is automatically classified as Core Team.
- Known protocol, treasury, admin, and claim-controller addresses are excluded from generated investor recipient lists, including internal return transfers to the Dolomite Gnosis Safe.
- Contract wallets are not excluded merely because they contain bytecode; only known internal/protocol addresses are excluded so legitimate investor multisigs remain eligible.

## Data flow

1. Read Berachain DOLO `Transfer` events from the official claim contracts.
2. Accept only transfers whose sender is one of the two configured Berachain claim contracts.
3. Exclude known protocol and internal distribution destinations.
4. Aggregate each recipient with exact integer wei arithmetic, first and last transfer blocks, transfer count, amount, and claim-source provenance.
5. Emit the current compatibility arrays and structured wallet records in `vesting_investors.json`.
6. Keep legacy JSON keys for compatibility, but expose the new user-facing labels through the shared address-label helper.
7. Propagate the shared labels to all existing consumers: DOLO, Earn, Borrow, Liquidations, Supply, oDOLO, veDOLO, and Revenue.

No wallet is classified from claim date alone, and no Ethereum transfer from an address with the same hexadecimal value is accepted as Berachain claim evidence.

## Rewards search design

Both Rewards searches reuse the DOLO Holders visual contract:

- desktop size: `280px × 36px`;
- border radius: `10px`;
- left search icon positioning and `13px` input typography match DOLO Holders;
- clear button uses the same size, border, hover, and keyboard-focus behavior;
- focus ring uses the existing Graphite + Gold token values;
- the invalid undefined `var(--sans)` font declaration is removed;
- native WebKit search decorations are neutralized so Safari and Chrome render the control consistently;
- desktop layout keeps Live search on the left and APR/APY on the right;
- Ended Programs uses the same search alignment;
- mobile layout stacks controls and gives the search the available width without horizontal overflow.

The Rewards searches keep their current local, immediate filtering, campaign/token/network matching, clear action, and visible result counts. A slash shortcut is not added because two search fields on one page would make the target ambiguous.

## Verification

### Data tests

- Strategic Investor Claims recipients receive `Strategic Investor`.
- Investor Claims recipients receive `Long-term Investor`.
- A dual-source wallet retains the strategic label and overlap metadata.
- The Dolomite Gnosis Safe and other known internal recipients are never emitted as investor wallets.
- Investor recipients are never copied into the Core Team array.
- Same-address transfers on Ethereum do not create Berachain investor labels.
- Exact integer parsing remains in use for DOLO wei values.

### UI and regression tests

- Update Rewards and vesting-label contract tests for the new wording and provenance.
- Verify computed search dimensions, font size, padding, focus state, and clear-button state against DOLO Holders.
- Browser-check Rewards at widths 1440, 1024, and 390 pixels in Chrome and Safari-compatible WebKit behavior.
- Run the targeted JavaScript tests, Python syntax checks, generated bundle checks, and the existing dashboard smoke tests.

## Deployment

After all checks pass, commit only the scoped implementation and generated outputs, push to the production `master` flow, watch GitHub Actions through completion, and verify the live GitHub Pages routes for Rewards and representative address-label tables.
