# oDOLO Claimer Label Parity Design

## Goal

Make known wallets in **oDOLO Claimer Breakdown** use the same trusted shared identity registry as DOLO Holders, without changing claim calculations or inventing new ownership labels.

## Decisions

- Keep `dolo-address-labels.js` as the single source of truth.
- Never drop a confirmed label because its shared type is unsupported by the oDOLO page.
- Preserve `source`, `confidence`, and the original shared type when adapting a label for the oDOLO UI.
- Show a compact type badge next to known labels and expose friendly source/confidence details through the existing tooltip UX.
- Keep unknown addresses as `Wallet`; do not infer an owner.
- Render `potential` classifications explicitly as potential, never as confirmed.
- Do not change oDOLO claim, held, outflow, exercise, or remaining calculations.

## Verification

- Regression-test shared EOA, investor, protocol, multisig, watch, LP, and liquidator mappings.
- Confirm the two currently dropped ENS labels (`7bfee.eth`, `atheon.eth`) appear in Claimer Breakdown.
- Confirm known investor labels retain their classification and provenance.
- Check desktop and mobile table rendering, tooltip behavior, and horizontal overflow in a real browser.
