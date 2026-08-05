# Implementation plan: rewards search and investor wallet labels

1. Add failing generator tests covering Strategic Investor Claims, Investor Claims, overlap, provenance metadata, and the absence of derived team wallets.
2. Extend `extract_vesting_investors` to aggregate exact integer transfer evidence and produce compatibility arrays plus structured wallet records.
3. Add failing JavaScript tests for primary-label priority, non-overwrite behavior, the legacy team-duplication guard, and program-search matching.
4. Add cached vesting-label merge/load helpers to `dolo-address-labels.js`; replace duplicated page-specific loaders and wire the remaining wallet views to the shared source.
5. Add a small reusable rewards-search helper and connect independent Live/Ended queries, clear buttons, filtered counts, and no-match states.
6. Implement the shared toolbar/search styling and responsive desktop/mobile layout without changing table columns.
7. Rebuild the generated Earn bundle and bump route asset versions where needed.
8. Run targeted unit/contract checks, then the relevant broader dashboard checks.
9. Serve the static site locally and verify Live/Ended interactions and wallet labels at desktop and mobile viewport sizes using browser-computed measurements.
10. Review the diff for unrelated changes, commit, rebase on the latest production `master`, rerun checks, push `HEAD:master`, and confirm the GitHub Pages deployment.
