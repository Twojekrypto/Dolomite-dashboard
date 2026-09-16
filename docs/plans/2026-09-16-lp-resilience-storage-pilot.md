# LP resilience and durable data-storage pilot

Approved user plan: repair explorer-dependent LP refresh, then pilot private
Cloudflare R2 storage for LP before migrating large Earn/Supply datasets.

## Global Constraints

- Preserve dashboard calculations, exact integer arithmetic, wallet attribution,
  data validation, six-hour LP schedule, current URLs and all historical observations.
- Never publish partial scans as fresh or replace failed requests with zero data.
- No credentials in source, logs or chat. No rotation to bypass provider limits.
- Do not change old recovery archives or merge old pre-rewrite Git ancestry.
- Use the existing isolated repair checkout, on codex/lp-resilience-storage-pilot.
- R2 cutover requires configured credentials and an independently verified upload,
  restore and rollback. Until then production stays in Git-backed mode.
- The pilot covers LP only; full Earn/Supply migration follows a proven pilot.
- No purchases, new external accounts, deletion of current data or history rewrite.
- Write behavioral regression tests before implementation; use apply_patch for edits.

## Task 1: Repair LP explorer resilience and indexed-holder dependency

Own explorer_api.py, generate_dolo_liquidity.py, focused explorer/LP tests,
LP runtime configuration and the LP workflow's scanner cache/env steps only.
Leave R2 publication and Pages changes to Task 2.

Evidence: latest LP run 35081487471 fails with JSON-level rate limit (BrownFi),
tokenholderlist PRO denial (Kodiak), generic explorer errors (Uniswap v3/v4).
RPC had 220 successful requests, no failures/429. explorer_get raises RuntimeError
for JSON failure, while _routescan_request retries only RequestException.

Requirements:
1. Test JSON rate-limit then success, persistent rate limit, PRO/auth rejection,
   malformed payload and genuine empty results. Retry only bounded known transient
   errors, honoring Retry-After where present, with configured conservative pacing.
   Never rotate API keys for quota/rate/PRO failures or treat errors as no events.
2. Use public RPC logs when the explorer cannot provide required logs; reuse
   existing RPC client and exact normalization/deduplication. Bound request ranges
   and shrink on provider range/result limits, preserve all filters and one target
   block, fail closed on gaps. Avoid full-chain unfiltered scans.
3. Remove Kodiak Island's mandatory tokenholderlist dependency: reconstruct holder
   candidates/balances from the Island's ERC20 Transfer events (mint/burn included)
   and reconcile totalSupply/current balances at the same pinned block. Preserve
   existing farm/Infrared ownership attribution and refuse unsupported evidence.
4. Persist reusable log scan checkpoints in an ignored cache directory, not Git.
   Support recent reorg overlap and save progress after successful chunks so a
   failed refresh resumes. Wire explicit Actions cache restore/save even on failure;
   validate cached filters/coverage, never mistake cached partial work for complete.
5. Keep the existing degraded-artifact publication guard. Test RPC fallback,
   missing chunk, cache resume/reorg overlap, exact holder reconciliation.
6. Run focused suites once green, commit only your files, report test evidence and
   operational risks. Do not push or spawn other agents.

## Task 2: Private R2 LP pilot and safe publication integration

Own new scripts/data_artifact_store.py, tests/test_data_artifact_store.py,
storage-specific workflow changes, config/docs and CI guard for LP cutover.

Requirements:
1. Small S3-compatible implementation using a standard pinned SDK only if needed.
   LP snapshot payloads are immutable/content-addressed with size/hash manifest;
   validate using existing validate_data LP checks before upload and after restore.
2. Support bootstrap/publish/restore/verify and explicit version restore for rollback.
   Publish active manifest last; never overwrite a newer dataset with older source
   data. Use single-writer workflow concurrency and conditional manifest update to
   reject races. Keep prior known-good versions; no automatic deletion in pilot.
3. Credentials are R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY;
   bucket is R2_BUCKET. Feature variable LP_DATA_STORAGE is git (default), shadow,
   or r2. Shadow writes/verifies R2 but keeps Git production authoritative.
4. In r2 mode restore exact LP snapshot before generation, publish to R2 instead of
   committing generated data/dolo-liquidity.json. The manual registry remains Git.
   Pages and DOLO-flow jobs must restore the selected validated LP artifact to the
   same path; fail closed rather than silently use stale Git if R2 fails. Preserve
   existing flow refresh and deployment triggers. Gate cutover behind an explicit
   verified readiness marker and do not enable it without credentials/live proof.
5. Prevent accidental LP JSON commits after cutover. Preserve the original Git
   artifact until successful pilot; only then untrack through an explicit reviewed
   migration step. Do not broadly ignore all JSON/config/manually maintained data.
6. Tests cover local fake S3 round trip, hash mismatch, interrupted upload,
   unauthorized/missing config, failed validation, older publish, concurrent publish,
   rollback and workflow mode behavior. No secret values in exceptions/log output.
7. Document exact Cloudflare/GitHub setup and operational rollback. Alert user that
   activation and larger migration are blocked if R2 is not configured, rather than
   claiming the growth problem permanently solved.

## Task 3: Integration review and deployment

Review each implementation task, then the whole branch for data/secret safety.
Run narrow regressions, shell/Python syntax, workflow parsing and diff check.
Push source changes to master without force, start a fresh LP run and inspect
real outcomes, data validation, Pages and unchanged public path. Do not claim LP
fixed without successful refresh or R2 active without live upload/restore proof.

Completion reporting separates shipped fixes, verified live data, prepared R2
pilot, and external setup still required. All old recovery backups remain intact.
