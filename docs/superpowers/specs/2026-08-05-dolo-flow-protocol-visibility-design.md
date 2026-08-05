# DOLO Flow Protocol Visibility Design

## Goal

Show both sides of real DOLO transfers when a participant is a verified protocol contract or Safe, and let users include or exclude protocol addresses through the existing address-type filter.

## Scope

- Keep verified `protocol` addresses visible when dynamic contract detection builds the DOLO Flow exclusion set.
- Preserve the existing exclusions for unlabeled infrastructure contracts, explicit router/bot addresses, mint/burn endpoints, and the DOLO/oDOLO system endpoints already handled by the pipeline.
- Add `Protocol` as a first-class address type in the existing Holders and DOLO Flow type menus. The same shared type model powers both menus, while holder-distribution analytics continue to treat protocol addresses as non-market contract supply.
- Keep the current Graphite + Gold dropdown, badge, pagination, responsive toolbar, and table geometry.
- Do not add dependencies or change the generated JSON schema.

## Data flow

`generate_dolo_flows.py` loads verified labels before classifying high-flow contracts. A detected contract with `type: protocol` will no longer be added to the dynamic infrastructure exclusion set. It can therefore appear in the existing accumulator/seller arrays, with its identity resolved client-side from `dolo-address-labels.js`.

The UI maps a verified `protocol` label to a distinct `protocol` display type. `Protocol` is enabled by default alongside the other address types. Turning it off removes protocol rows from both accumulator and outflow tables without refetching data.

## UX

- Menu item: `Protocol`
- Row badge: `PROTOCOL`
- Tooltip: `Verified Dolomite protocol, treasury, rewards, or distribution address`
- Visual treatment: quiet blue-gray badge and dot, distinct from gold generic contracts and red exchanges.
- Existing multi-select behavior, checkmarks, reset control, and mobile wrapping remain unchanged.

## Correctness and tests

- A regression test will model a transfer between `Chainlink Rewards Claim` and `Ecosystem Incentives 2` and require both the seller and accumulator to survive dynamic contract filtering.
- A JavaScript behavior test will execute the shared type-classification code and require protocol labels to remain `protocol` and be included in the selectable type set.
- Browser verification will confirm the new filter, badge, and table rendering at desktop and mobile widths using the local HTTP server.

## Deployment

After local verification, push the implementation to production `master`, dispatch `Update DOLO Flows Data`, wait for its generated data commit and GitHub Pages deployment, then verify the live page with a cache-busting query parameter.
