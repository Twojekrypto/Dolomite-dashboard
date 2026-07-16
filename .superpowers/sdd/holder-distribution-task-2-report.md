# Holder Distribution Task 2 Report

## Scope

Implemented the client-side `Change %` holder-distribution metric in `dolo-preview.html`.
It uses the first visible point in the current brush as its baseline, keeps Balance as the
default metric and preserves the existing balance values and filled-area behavior.

## TDD

### RED

Command:

```bash
node --test tests/holder-distribution-contract.test.js
```

Result: expected failure in `holder distribution contains guarded relative-change helpers`.
The assertion for `let holderDistributionMetric = "balance"` did not match because the
new metric state and helpers had not yet been implemented. The other two tests passed.

### GREEN

Commands:

```bash
node --test tests/holder-distribution-contract.test.js
node -e 'const fs=require("fs"); const html=fs.readFileSync("dolo-preview.html", "utf8"); [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].forEach(m=>new Function(m[1])); console.log("Parsed inline scripts.");'
git diff --check
```

Results:

- Contract tests: 3 passed, 0 failed.
- Inline script parser: `Parsed inline scripts.`
- `git diff --check`: clean.

## Files Changed

- `dolo-preview.html`
  - Added `holderDistributionMetric`, guarded metric/scale/path helpers, symmetric Change % scale, a zero line, change-mode gaps, revised tooltip values, and metric control binding.
  - The area remains only in Balance mode.
- `tests/holder-distribution-contract.test.js`
  - Added the required guarded-relative-change helper contract test.
- `.superpowers/sdd/holder-distribution-task-2-report.md`
  - This report.

## Self-Review

### Correctness and regression

- Balance remains the default and follows the existing values, scale, and area rendering.
- Change % uses `model.points[0]`, the first point in the currently brushed window.
- A baseline `<= 0` returns `null`; the SVG path breaks, no dot is drawn, and the tooltip says `New / no baseline` rather than rendering a percentage.
- Change % derives a shared symmetric scale from the maximum finite absolute value and places zero at the center with a visible zero line.
- Switching metrics re-renders with `skipBrush:true`, preserving the current brush and pinned selection.
- The tooltip orders Change % as relative percentage, current balance, then absolute change.
- No bucket definitions, classifications, generated data, workflows, or address-table columns were changed.

### Maintainability and security

- Reused local helpers and chart patterns; no dependencies, config, network calls, or secrets were added.
- Only Task 2 files were staged; pre-existing local changes in `AGENTS.md` and `skills/` remain untouched.

## Concerns

- The known Node module-type warning appears during the contract test and is not a test failure.
- Browser-level smoke testing was not run because Playwright is unavailable in this workspace; the requested contract test, inline parser, and whitespace check were run successfully.

## Review Remediation

### Findings addressed

- The renderer now explicitly assigns `Change · ${holderRangeLabel}` to
  `#holder-legend-change-head` after rebuilding the legend. This runs for every chart
  render, including brush-driven renders, and therefore reflects the active brush range.
- The source-contract test now protects the concrete Change % behavior: the non-positive
  baseline guard, symmetric `min:-max`/`max` scale with zero, zero-line positioning,
  Change % area suppression, `New / no baseline`, relative/current/absolute tooltip
  order, and the dynamic legend-head assignment.

### Remediation TDD

#### RED

Command:

```bash
node --test tests/holder-distribution-contract.test.js
```

Result: expected failure in `holder distribution exposes a guarded change tooltip and
updates the legend range`. The new test could not find the explicit
`legendChangeHead.textContent = `Change · ${holderRangeLabel}`` assignment. The other
three tests passed.

#### GREEN

Commands:

```bash
node --test tests/holder-distribution-contract.test.js
node -e 'const fs=require("fs"); const html=fs.readFileSync("dolo-preview.html", "utf8"); [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].forEach(m=>new Function(m[1])); console.log("Parsed inline scripts.");'
git diff --check
```

Results:

- Contract tests: 4 passed, 0 failed.
- Inline script parser: `Parsed inline scripts.`
- `git diff --check`: clean.

### Remediation Self-Review

- The new assignment runs after `legend.innerHTML`, so it always targets the freshly
  rendered heading and has no effect on the Balance data, brush state, or pinned series.
- The test assertions target the source branches and output order rather than helper
  names alone, protecting the review findings without expanding into Task 3.
- Only `dolo-preview.html`, the focused contract test, and this existing report were
  changed; pre-existing `AGENTS.md` and `skills/` changes remain untouched.
