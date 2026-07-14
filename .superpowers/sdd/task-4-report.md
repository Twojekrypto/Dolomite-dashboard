# Task 4 Report: veDOLO Chart Metric Switch

## Scope

Implemented Task 4 in the shared repository. Production ownership was limited to `vedolo-preview.html` and `tests/test_vedolo_preview_contracts.py`. The required report is stored alongside the approved SDD materials.

## Changes

- Added source-contract coverage for the accessible chart metric switch.
- Added the semantic `#lockedChartMode` group with `Locked DOLO` and disabled `Vote Power` buttons.
- Added `#lockedChartTitle` and preserved the active state with `aria-pressed="true"`.
- Added the existing tooltip contract to the disabled Vote Power button with an explicit unavailable-history explanation.
- Added scoped Graphite and Gold styling with minimum height, visible keyboard focus, active-state contrast, and narrow-screen stacking below the title/meta row.
- Added the Task 5 static-history handoff contract as a non-executable source comment only. No loading, route cache busting, series switching, or renderer behavior was added.
- Preserved the original Locked DOLO view and the existing chart/brush SVG markup byte-for-byte.

## TDD Evidence

1. Added the two requested tests before changing the HTML.
2. Ran `python3 -m unittest tests/test_vedolo_preview_contracts.py -v`: expected red state, with exactly the two new tests failing and the existing ten tests passing.
3. Added the minimal header/control markup and styles.
4. Ran the same command again: all 12 tests passed.

## Verification

- `python3 -m unittest tests/test_vedolo_preview_contracts.py -v` passed: 12/12.
- `git diff --check` passed.
- Local static server started successfully at `http://127.0.0.1:8766/` after port 8765 was already occupied.
- Server-served source inspection confirmed the new IDs, button contracts, disabled tooltip, and unchanged SVG identifiers.
- Direct comparison against `HEAD` confirmed the locked chart SVG and brush SVG blocks are unchanged.

## Concern

The repository has no installed `agent-browser`, Chromium, Google Chrome, or Playwright executable, so browser-computed styles and bounding boxes could not be collected. The source-level responsive rules and static contract checks were verified; a real-browser visual/computed-style pass remains the only residual verification gap.

## Task 4 Review Remediation

### Status

Ready for commit. Unrelated edits were preserved; the scoped work changed only `vedolo-preview.html`, `tests/test_vedolo_preview_contracts.py`, and this report.

### Changes

- Restored the `drag window below to zoom` card meta instruction in the locked-chart header.
- Kept the meta and metric control in the same responsive header and included the meta in the narrow-screen full-width layout rule.
- Moved unavailable-history `data-tip` semantics to a focusable `.locked-chart-vote-help` wrapper around the disabled Vote Power button.
- Added disabled-state synchronization so the wrapper has `tabindex="0"` and `data-tip` only while unavailable; enabling the button removes both, avoiding a second tab stop.
- Kept the actual Vote Power button semantically disabled in the unavailable state.

### TDD And Verification

- Added both source-contract tests before implementation.
- Red run: exactly the two new tests failed; the pre-existing contracts passed.
- Green run: `python3 -m unittest tests/test_vedolo_preview_contracts.py -v` passed, 14/14.
- `git diff --check` passed.

### Concern

No browser session was started for this narrow source-contract fix. The responsive relationship and tooltip/tab-stop lifecycle are covered by source contracts; a browser-computed-style pass remains a residual risk.
