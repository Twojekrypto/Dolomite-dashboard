# Repository storage recovery

## Incident: 2026-09-14

GitHub reported 104877817 KiB of repository storage and rejected ordinary data
pushes with `Repository is above its size quota`. Twenty failed scheduled runs
in the investigated overnight window reached this rejection. This is a repository
history/storage incident, not an RPC outage or a Git rebase race.

Generated datasets are updated frequently, including tens of thousands of EARN
wallet files. Removing files in a new commit does not remove their historical
versions. Do not weaken data validation, mark partial scans fresh, or keep retrying
a quota rejection to make Actions green.

## Recovery rules

- Back up before rewriting. Record all branch heads and workflow enablement states.
- Pause data producers during a rewrite. Keep the public site serving its previous
  audited deployment. Restore the recorded schedule states after publication works.
- Retain source, test, configuration and manual registry history. Compact old
  revisions of generated datasets only, keeping the complete current dataset.
- Compare each restored branch tip's tree SHA with its original tree SHA. This
  proves byte-identical files, including historical dates inside current JSONs.
- Use explicit per-branch force-with-lease updates; never blind `push --mirror`.
- GitHub may need to reclaim unreachable objects or remove a server-side block.
  Stop on continued quota rejection and contact Support; do not evade the quota.
- After rewriting, do not merge an old clone/branch history back into production.
  Preserve local work and transplant only the intended source patch into a fresh
  checkout. Otherwise the oversized history can become reachable again.
- Verify actual data refresh and Pages deployment independently. A local passing
  test or an accepted commit does not establish that live data was refreshed.

## Prevention

Keep heavy scan checkpoints in their existing Actions caches, not new Git commits.
Publish only the selected, validated EARN cohort; never force-add entire restored
cache directories. Avoid writes when the artifact content has not changed.

Review repository storage regularly with:

```sh
gh api repos/Twojekrypto/Dolomite-dashboard --jq '{size_kib: .size, pushed_at}'
```

Plan further retention or external data-storage changes before growth approaches
the provider quota again. Such a migration needs an explicit retention and runtime
loading design; simply moving files or deleting historical observations is not
a safe substitute.

## Independent explorer credential failure

Routescan's Berachain endpoint can return `chain not supported`. The shared
`explorer_api.py` fallback then uses Etherscan V2. An `Invalid API Key` reply must
never be interpreted as zero transactions. A distinct configured Berachain key
may be tried on credential rejection only, not to bypass throttling.

If both configured credentials are rejected, replace `ETHERSCAN_API_KEY` in
GitHub Actions secrets with a valid Etherscan V2 key. Never put credentials in
source, logs, support tickets, or chat. Changing RPC providers does not repair
an invalid explorer key.
