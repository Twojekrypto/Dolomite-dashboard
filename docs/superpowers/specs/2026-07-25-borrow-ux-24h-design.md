# Borrow 24h and Table UX Design

## Objective

Polish the Borrow route without changing its Graphite + Gold identity:

- add a trustworthy 24-hour change beside the monitored-position count;
- make collateral, debt, and risk values immediately distinguishable by color;
- simplify the Lending Positions and Liquidation History headers;
- align the simulator's wallet table with the Lending Positions address UX.

## Data contract

The headline count is the number of positions whose collateral plus debt is at
least `$10`, matching the existing Borrow UI dust rule. `fetch_liquidation_risk.py`
will maintain a small rolling file at
`data/liquidation-risk/position-count-history.json`.

The file contains hourly observations and a derived comparison:

```json
{
  "generatedAt": 1784990000,
  "snapshots": [
    {"timestamp": 1784903600, "count": 1649},
    {"timestamp": 1784990000, "count": 1640}
  ],
  "change24h": {
    "currentCount": 1640,
    "baselineCount": 1649,
    "change": -9,
    "baselineAt": 1784903600
  }
}
```

Snapshots are retained for 72 hours. The 24-hour baseline is the observation
closest to `current timestamp - 24 hours`, but only when it is within six hours
of that target. A missing or invalid baseline is shown honestly as unavailable;
the UI must not invent a zero change. The initial file will be seeded from
existing repository snapshots so production has a comparison immediately.

## Hero design

The main number remains the dominant element. A compact directional metric sits
on the same baseline:

```text
1640   ↓ 9
       24h change
```

- positive count change: `--up` (`#75b87b`);
- negative count change: `--down` (`#b4796b`);
- zero: neutral foreground;
- unavailable baseline: muted `—` with `24h change`.

This follows the DOLO price-change pattern without copying unrelated decoration.
The color hierarchy for the three summary values becomes:

- Total Collateral: `--up`;
- Total Debt: a restrained `--down`;
- At Risk: a stronger, higher-contrast red reserved for actual risk.

## Table headers

Remove the redundant `Position Monitor` and `Liquidation Log` kickers.
`Lending Positions (N)` and `Liquidation History (N)` use one flex-aligned title
row, tabular count figures, and a shared baseline.

The right-side metadata on both tables becomes:

```text
● Data updated · 19 min ago
```

It uses the same `generatedAtISO` value and refreshes every minute. The relative
time helper updates the hero timestamp and both table metadata labels together.
Filtering changes counts but never changes the data timestamp.

## Wallets-at-risk address table

The simulator table continues using the shared `renderDoloAddressTools` renderer.
Within this table:

- both the label (`Wallet` or known name) and shortened address have no underline;
- hover changes color only;
- copy and DeBank actions remain interactive;
- the Chain column grows slightly and the Address column receives a small
  left inset, producing a clearer Chain–Address gap;
- the spacer shrinks by the same amount so Collateral and Debt stay fixed.

Every related `nth-child` selector and the mobile widths must be audited after
the column adjustment.

## Responsive and accessibility behavior

The change chip may wrap below the number on narrow screens but may not overlap
the summary grid. Direction is communicated by sign/arrow and text, not color
alone. Existing keyboard focus and action behavior remain intact.

## Verification

- Python unit tests cover dust-consistent counting, rolling retention, exact
  24-hour selection, positive/negative/zero values, and missing baselines.
- Static UI contracts cover removed copy, the new IDs/classes, color rules, and
  table column spacing.
- Browser QA measures computed colors and title/count alignment with
  `getComputedStyle()` and `getBoundingClientRect()`.
- Browser QA covers desktop and mobile, opens “View wallets at risk”, and checks
  that address links are not underlined.
- Existing Borrow/liquidation tests, Python compilation, HTML/JS syntax checks,
  and GitHub Pages smoke tests must pass before completion.
