# LP R2 production cutover — 2026-09-19

Scope: LP only. No credential changes, public bucket, timestamp rewriting,
Git history rewrite, or Earn/Supply migration.

## Readiness and rollback proof

- Readiness B: `52081ba0d6638a40d06eb5d1a8d81ade22209f7a5773a213244c180510796ec7`.
- Rollback A: `f047415341bc2e60c809d2c4274c9fc82561127395bf835b3ac72ffeecfaab47`.
- Successful pilot mark-ready: [35198069948](https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/35198069948).
- Read-only production gate preflights: [35436650313](https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/35436650313)
  and [35441513709](https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/35441513709), both successful.
  The latter restored the latest shadow LP (`273541e739c19be5f609123f48eead47d886285464ea6c2d8c2e5c137ae1e1a5`).

## Flows blocker repaired before cutover

- The primary explorer key was explicitly rejected as invalid. The existing
  backup key and official Berachain RPC returned the same 9 logs for blocks
  25,957,667–25,958,666 in diagnostic run [35436103167](https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/35436103167).
- Commit `f5a778ae528d252ec250934f004e8acd37b7374d` tries the existing backup only
  on invalid-key rejection, not throttling. Explorer credentials remain one
  provider family; independent exact-match quorum was not weakened.
- Local regression suite: 1588 tests, successful, one skipped.
- Full repaired Flows run [35436207866](https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/35436207866)
  succeeded, including both chain refreshes, data validation and publication.

## Production evidence

At 11:59 UTC, after confirming matching master workflows and no active LP writer:

1. Set `LP_R2_READY_DIGEST=B` (the historical readiness proof, not the active LP).
2. Set `LP_DATA_STORAGE=r2`; both variables were read back.
3. LP producer [35441573037](https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/35441573037)
   succeeded with gated R2 baseline restore, generation, validation and read-back
   verified R2 publication.
4. New LP SHA-256: `ef85d71ef47f2b44332f66617b37786a487d635a64e187ee460227f57f2e2dde`;
   `generatedAt=2026-09-19T11:59:43.465113Z`, 518454 bytes.
5. Pages [35441946694](https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/35441946694)
   restored that exact hash and passed live smoke checks at 12:13 UTC.
   A cache-busted fetch of the public LP JSON matched the hash and bytes exactly.
6. Producer reported no Git changes. The last commit touching generated LP
   remains `ff01aac6b1ef819e7542b7a3638790a63014b40a` (11:08:23 UTC, before cutover).

## Downstream status — still in progress at 12:13 UTC

Full R2-mode Flows [35442037293](https://github.com/Twojekrypto/Dolomite-dashboard/actions/runs/35442037293)
has successfully restored gated current LP attribution and is refreshing holders
and flows. Its final generation/validation/publication result is not yet confirmed.

To avoid duplicate long scans, this full run superseded pending skip-holders run
35441943220; old-mode scheduled run 35440479731 was intentionally cancelled.
No completed data publication was rolled back. Full refresh includes holders.

## Remaining limitation and rollback

Master has no branch protection/rulesets. LP Storage Checks detects generated LP
commits in `r2`, but a post-push check cannot prevent direct pushes. Broad branch
protection was not changed because existing automated data writers use direct pushes.

The tracked Git baseline, private R2 versions and rollback proof remain intact.
Follow [the rollback runbook](lp-storage-pilot.md#rollback-operacyjny); switching
back to Git requires a successful fresh LP generation/commit before publication.
Do not silently publish an aged baseline or modify `generatedAt`.
