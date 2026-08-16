# veDOLO Activity Period, Portfolio, and History Design

## Goal

Make veDOLO position-management events easy to filter on the veDOLO page and consistently visible for a queried wallet in Portfolio and Transaction History, without treating internal NFT management as newly locked DOLO.

## Approved interaction model

- `veDOLO Position Activity` keeps its independent search and action filter.
- On desktop and laptop, the action filter sits immediately beside the search field. A period filter sits on the far right and reuses the existing `veDOLO Flow` period control and options: `24H`, `7D`, `30D`, `90D`, `180D`, and `All time`.
- The Position Activity period defaults to `7D`, matching `veDOLO Flow`. It resets only the Position Activity page and never changes the Flow filters.
- On mobile, the search remains full width and the two compact dropdowns wrap below it without widening the page.
- `veDOLO Holders` and `Expired veDOLO Ready to Claim` show at most 10 real rows per page and preserve the existing stable-height spacer and pager behavior.

## Shared event semantics

The existing `vedolo-position-activity.js` classifier remains the canonical source for `transfer`, `merge`, `split`, and `extend`. Portfolio and History consume that shared classifier rather than reimplementing deposit-type interpretation.

For a queried wallet:

- a transfer is included when the wallet is either sender or recipient;
- a merge, split, or extension is included when the wallet is the event beneficiary/owner;
- rows are deduplicated by exact transaction hash plus semantic action and position transition;
- the selected date range is applied to the event timestamp;
- merge, split, extension, and transfer are position-management events, not additional principal deposits.

Portfolio extends the existing veDOLO Position Activity route filter with the four semantic actions. Its current summary continues to count only genuine lock/exercise principal. History adds the same four semantic actions to its action filter and appends the canonical Berachain events before transaction grouping, so identical transaction hashes remain one transaction row.

## Data and scope constraints

No RPC, subgraph, generator, JSON schema, balance calculation, default table sort, or blockchain source changes. Existing published `vedolo_flows.json` supplies the raw lock and transfer rows. Internal position events receive neutral/position-management presentation and do not change portfolio totals or tax conclusions.

## Validation

Contract tests cover period filtering, wallet participation, semantic adapters, deduplication, Portfolio route wiring, History action wiring, and 10-row limits. Browser QA covers 1440px desktop, 1024px laptop, and 390px mobile for toolbar layout, dropdown interaction, pagination, stable table height, and horizontal overflow.
