# CEX and Holder History Audit Fixes

> Execute inline in the existing isolated worktree, with test-first regression checks.

**Goal:** Fix the audited bot/Safe classification leak, historical CEX Details, and misleading confidence in the CEX audit without removing user-reviewed identities.

**Architecture:** Keep existing generator and preview components. Reuse cached transfer reconstruction for CEX address snapshots; do not rescan RPC history. Preserve current CEX membership, marking evidence quality separately from entity type.

**Spec:** Audit findings in the preceding user-approved conversation, 6 September 2026.

## Tasks

- [x] Add failing generator tests: liquidator/bot/MM/watch takes precedence over Safe/delegated structure; unlabeled Safe remains a user. CEX snapshots preserve historical per-address balances and never imply unknown confidence is confirmed.
- [x] Fix `holder_distribution_type` in `generate_dolo_flows.py`; preserve explicit identity metadata while loading labels.
- [x] Add bounded `walletBalances` to CEX daily history and regenerate CEX history in `--rebuild-holder-history-only`. Keep old CEX total/rounding contract unchanged.
- [x] Add JS regression tests to `tests/holder-distribution-contract.test.js`, then use the selected end-point's addresses/balances in CEX Details. Never fall back to today's balances for an older selection. Retain existing wallet renderer and styles.
- [x] Make `audit_dolo_cex_labels.py` read both canonical labels and overrides, keep unknown confidence unknown, and publish existing-label review coverage in its summary. Preserve advisory-only promotion.
- [x] Run Python/Node checks, actual-browser Details checks on desktop/mobile, and two diff-review passes. EARN checks passed; 86 targeted JS tests passed. Chromium 1440/1024/768/390 px: no table overflow or page errors. Full EARN checks initially lacked two unchanged data fixtures in the sparse worktree; restored their pinned repository versions and reran successfully.
- [ ] Publish only scoped changes to production `master`, dispatch cached history rebuild and CEX audit, verify Pages and generated JSONs.

## Acceptance

- Known bot `0xb7131fc8cdc43060a6210257f537dba5fcae6aed` never enters holder buckets solely because it is a Safe.
- Historical CEX address amounts sum to the selected snapshot within documented display rounding.
- Missing/behavioral identity evidence is not automatically upgraded to confirmed.
- Existing CEX identities, bucket thresholds, chain scope and default veDOLO toggle remain unchanged.
