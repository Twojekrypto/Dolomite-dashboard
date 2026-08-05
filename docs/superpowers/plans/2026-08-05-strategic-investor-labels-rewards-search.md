# Strategic Investor Labels and Rewards Search Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace ambiguous investor labels with source-backed strategic and long-term labels, exclude internal claim-controller flows, and make both Rewards searches match DOLO Holders exactly.

**Architecture:** The Python flow generator remains the authoritative producer of claim-source provenance, while `dolo-address-labels.js` remains the single frontend normalization and propagation point for every wallet table. Rewards keeps local filtering in `rewards-search.js`; only the search control presentation changes in `rewards-preview.html`, with route cache versions bumped after verification.

**Tech Stack:** Python 3, static JavaScript, Node.js built-in test runner, unittest, HTML/CSS, Playwright browser checks, GitHub Pages.

## Global Constraints

- Classify investor wallets only from DOLO transfers sent by the official Berachain claim contracts.
- Never classify a wallet from claim date alone or from a same-address Ethereum transfer.
- Preserve exact integer wei arithmetic.
- Keep `early_investors` and `investors` JSON keys for compatibility while adding the new strategic terminology.
- Keep confirmed `Core Team 1–8` labels unchanged and never derive Core Team from Investor Claims.
- Exclude known protocol and internal destinations without excluding legitimate investor contract wallets globally.
- Use `Strategic Investor` and `Long-term Investor` as visible wallet labels.
- Show `2024 strategic round · $900K` only as a high-confidence on-chain attribution.
- Rewards search must be `280px × 36px` on desktop, use `13px` Inter text, and fill available width on mobile.
- Add no new dependencies and preserve the Graphite + Gold identity.

---

### Task 1: Harden claim-recipient generation

**Files:**
- Modify: `tests/test_generate_dolo_flows_integrity.py`
- Modify: `generate_dolo_flows.py:1226-1350`
- Modify generated output: `vesting_investors.json`

**Interfaces:**
- Consumes: `extract_vesting_investors(all_transfers: dict[str, list[tuple]])`
- Produces: schema-versioned payload containing `strategic_investors`, compatibility arrays, `team`, and structured `wallets` rows with new labels and provenance.

- [ ] **Step 1: Write failing generator tests**

Add an internal Safe recipient and assert the new labels and compatibility contract:

```python
DOLOMITE_GNOSIS_SAFE = "0xa75c21c5be284122a87a37a76cc6c4dd3e55a1d4"

payload = flows.extract_vesting_investors({
    "eth": [(STRATEGIC_INVESTOR_CLAIMS, OUTSIDE, 99 * 10**18, 101)],
    "bera": [
        (STRATEGIC_INVESTOR_CLAIMS, EARLY_ONLY, 2 * 10**18, 102),
        (INVESTOR_CLAIMS, INVESTOR_ONLY, 3 * 10**18, 103),
        (STRATEGIC_INVESTOR_CLAIMS, OVERLAP, 4 * 10**18, 201),
        (INVESTOR_CLAIMS, OVERLAP, 5 * 10**18, 202),
        (STRATEGIC_INVESTOR_CLAIMS, DOLOMITE_GNOSIS_SAFE, 166_667 * 10**18, 204),
    ],
})

self.assertEqual(payload["schemaVersion"], 3)
self.assertEqual(payload["strategic_investors"], [EARLY_ONLY, OVERLAP])
self.assertEqual(payload["early_investors"], payload["strategic_investors"])
self.assertNotIn(DOLOMITE_GNOSIS_SAFE, payload["investors"])
self.assertEqual(records[EARLY_ONLY]["label"], "Strategic Investor")
self.assertEqual(records[INVESTOR_ONLY]["label"], "Long-term Investor")
self.assertEqual(records[OVERLAP]["label"], "Strategic Investor")
self.assertEqual(records[EARLY_ONLY]["roundAttribution"]["label"], "2024 strategic round · $900K")
self.assertEqual(records[EARLY_ONLY]["roundAttribution"]["status"], "high-confidence-onchain-attribution")
```

- [ ] **Step 2: Run the focused Python test and confirm failure**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_flows_integrity.GenerateDoloFlowsIntegrityTests.test_vesting_investors_are_classified_by_official_claim_contract -v
```

Expected: failure because schema version 3, strategic labels, and internal-recipient filtering are not implemented.

- [ ] **Step 3: Implement the minimal generator change**

Add explicit constants beside the extractor:

```python
STRATEGIC_INVESTOR_CLAIMS = "0x7efd088ae500598a19a242d6d48b9f7e0d061176"
INVESTOR_CLAIMS = "0x3a025c7fcf7632197ea82e64acd6ff53e1c06c07"
INVESTOR_INTERNAL_RECIPIENTS = {
    "0xa75c21c5be284122a87a37a76cc6c4dd3e55a1d4",
    STRATEGIC_INVESTOR_CLAIMS,
    INVESTOR_CLAIMS,
}
```

Skip destinations in `INVESTOR_INTERNAL_RECIPIENTS`, emit `Strategic Investor` or `Long-term Investor`, preserve `early_investors`, add `strategic_investors`, and attach:

```python
"roundAttribution": {
    "key": "2024-strategic-900k",
    "label": "2024 strategic round · $900K",
    "status": "high-confidence-onchain-attribution",
} if is_strategic else None,
"vestingSchedule": "3-year vesting · 1-year cliff" if has_long_term else None,
```

Set methodology overlap priority to `strategic-investor` and schema version to 3.

- [ ] **Step 4: Run generator integrity tests**

Run:

```bash
python3 -m unittest tests.test_generate_dolo_flows_integrity -v
python3 -m py_compile generate_dolo_flows.py
```

Expected: all tests pass and Python compilation succeeds.

- [ ] **Step 5: Migrate the current published payload to schema 3**

The checked-in payload was generated from the current production transfer state immediately before this change. Apply the same deterministic rename, metadata, compatibility-key, and internal-recipient rules to that payload; future scheduled generator runs will reproduce it from raw transfers:

```bash
python3 - <<'PY'
import json
from pathlib import Path
path = Path("vesting_investors.json")
p = json.loads(path.read_text())
internal = {"0xa75c21c5be284122a87a37a76cc6c4dd3e55a1d4"}
p["schemaVersion"] = 3
p["methodology"]["overlapPriority"] = "strategic-investor"
p["strategic_investors"] = sorted(a for a in p.get("early_investors", []) if a not in internal)
p["early_investors"] = list(p["strategic_investors"])
p["investors"] = sorted(a for a in p.get("investors", []) if a not in internal)
p["team"] = []
p["wallets"] = [row for row in p.get("wallets", []) if row.get("address") not in internal]
for row in p["wallets"]:
    strategic = "strategic_investor_claims" in row.get("claimSources", [])
    long_term = "investor_claims" in row.get("claimSources", [])
    row["label"] = "Strategic Investor" if strategic else "Long-term Investor"
    row["roundAttribution"] = ({
        "key": "2024-strategic-900k",
        "label": "2024 strategic round · $900K",
        "status": "high-confidence-onchain-attribution",
    } if strategic else None)
    row["vestingSchedule"] = "3-year vesting · 1-year cliff" if long_term else None
path.write_text(json.dumps(p, indent=2) + "\n")
assert p["schemaVersion"] == 3
assert p["team"] == []
assert p["early_investors"] == p["strategic_investors"]
assert "0xa75c21c5be284122a87a37a76cc6c4dd3e55a1d4" not in p["strategic_investors"]
assert "0xa75c21c5be284122a87a37a76cc6c4dd3e55a1d4" not in p["investors"]
PY
```

- [ ] **Step 6: Commit the generator unit**

```bash
git add generate_dolo_flows.py tests/test_generate_dolo_flows_integrity.py vesting_investors.json
git commit -m "fix: classify strategic investor claims accurately"
```

---

### Task 2: Normalize and propagate the new labels

**Files:**
- Modify: `tests/dolo-vesting-labels.test.js`
- Modify: `tests/test_dolo_address_labels.py`
- Modify: `dolo-address-labels.js:48-55,150-220`
- Modify cache references: `dashboard-core.html`, `dolo-preview.html`, `earn/earn-core.html`, `liquidation-preview.html`, `odolo-preview.html`, `revenue-preview.html`, `vedolo-preview.html`

**Interfaces:**
- Consumes: schema 2 or schema 3 `vesting_investors.json` payloads.
- Produces: `window.mergeDoloVestingLabels(labels, payload)` entries with normalized visible labels, claim sources, descriptions, detail copy, and attribution status.

- [ ] **Step 1: Write failing frontend normalization tests**

Update the test expectations and retain legacy payload coverage:

```javascript
assert.equal(labels[EARLY].label, 'Strategic Investor');
assert.equal(labels[INVESTOR].label, 'Long-term Investor');
assert.equal(labels[OVERLAP].label, 'Strategic Investor');
assert.equal(labels[OVERLAP].labelDetail, '2024 strategic round · $900K');
assert.equal(labels[OVERLAP].attributionStatus, 'high-confidence-onchain-attribution');
assert.match(labels[OVERLAP].description, /long-term investor tranche/i);
```

Add a case where structured legacy labels `Early Investor` and `Investor` normalize to the new wording.

- [ ] **Step 2: Run frontend label tests and confirm failure**

Run:

```bash
node --test tests/dolo-vesting-labels.test.js
python3 -m unittest tests.test_dolo_address_labels -v
```

Expected: failures on the old visible names and missing detail metadata.

- [ ] **Step 3: Implement source-aware normalization**

Normalize input labels before candidate selection:

```javascript
const normalizeInvestorLabel = label => label === 'Early Investor'
  ? 'Strategic Investor'
  : label === 'Investor'
    ? 'Long-term Investor'
    : label;
```

Use `Strategic Investor` as overlap priority. Prefer `payload.strategic_investors`, falling back to `payload.early_investors`. For strategic recipients set:

```javascript
labelDetail: '2024 strategic round · $900K',
attributionStatus: 'high-confidence-onchain-attribution',
description: hasLongTerm
  ? 'Received DOLO from the official Strategic Investor Claims contract and also received the long-term investor tranche.'
  : 'Received DOLO from the official Strategic Investor Claims contract.',
```

For long-term-only recipients set `labelDetail: '3-year vesting · 1-year cliff'`. Rename the static long-term contract label from `Investor Distribution` to `Investor Claims`. Preserve existing confirmed protocol labels with `setdefault`/non-overwrite behavior.

- [ ] **Step 4: Run label and consumer tests**

Run:

```bash
node --test tests/dolo-vesting-labels.test.js
python3 -m unittest tests.test_dolo_address_labels -v
python3 -m unittest tests.test_earn_dashboard_contracts tests.test_earn_premium_ux_contracts -v
```

Expected: all tests pass, Core Team remains unchanged, and every wallet consumer still loads the shared helper.

- [ ] **Step 5: Bump the shared-label cache key**

Replace `dolo-labels-20260805-investor-provenance` with `dolo-labels-20260805-strategic-round` in every HTML consumer and verify there are no stale references:

```bash
rg -n "dolo-labels-20260805-investor-provenance" --glob '*.html'
```

Expected: no matches.

- [ ] **Step 6: Commit the frontend label unit**

```bash
git add dolo-address-labels.js tests/dolo-vesting-labels.test.js tests/test_dolo_address_labels.py dashboard-core.html dolo-preview.html earn/earn-core.html liquidation-preview.html odolo-preview.html revenue-preview.html vedolo-preview.html
git commit -m "feat: clarify strategic investor wallet labels"
```

---

### Task 3: Match Rewards search to DOLO Holders

**Files:**
- Modify: `tests/rewards-preview-contract.test.js`
- Modify: `rewards-preview.html:100-120,180-195,330-385`
- Modify: `rewards/index.html`

**Interfaces:**
- Consumes: existing `bindProgramSearch`, `rwLiveSearch`, and `rwPastSearch` behavior.
- Produces: unchanged filtering behavior with a DOLO Holders-compatible control at desktop and mobile widths.

- [ ] **Step 1: Write failing CSS contract tests**

Add assertions for the intended visual contract:

```javascript
assert(/\.program-search\{[^}]*width:280px;[^}]*height:36px/.test(html));
assert(/\.program-search\{[^}]*padding:0 14px 0 36px/.test(html));
assert(/\.program-search>svg\{[^}]*position:absolute;[^}]*left:12px;[^}]*width:14px/.test(html));
assert(/\.program-search input\{[^}]*font-size:13px/.test(html));
assert(!html.includes('var(--sans)'));
assert(html.includes('-webkit-appearance:none'));
assert(/@media \(max-width:640px\)[\s\S]*?\.program-search\{width:100%/.test(html));
```

- [ ] **Step 2: Run the Rewards contract tests and confirm failure**

Run:

```bash
node tests/rewards-preview-contract.test.js
```

Expected: failure on width, padding, icon position, font, and the invalid font variable.

- [ ] **Step 3: Implement the DOLO Holders search contract**

Replace the search CSS with the matching dimensions and explicit typography:

```css
.program-search{position:relative;display:flex;align-items:center;width:280px;height:36px;padding:0 14px 0 36px;background:var(--bg-3);border:1px solid var(--line-2);border-radius:10px;color:var(--fg-3);transition:border-color .2s var(--ease),box-shadow .2s var(--ease)}
.program-search:focus-within{border-color:var(--gold-line);box-shadow:0 0 0 3px rgba(201,162,39,.06)}
.program-search>svg{position:absolute;left:12px;width:14px;height:14px;color:var(--fg-3)}
.program-search input{flex:1;min-width:0;background:none;border:0;outline:0;color:var(--fg-1);font-family:'Inter',system-ui,sans-serif;font-size:13px;line-height:1.5;-webkit-appearance:none;appearance:none}
.program-search input::-webkit-search-cancel-button{-webkit-appearance:none;appearance:none}
```

Match the DOLO Holders clear button at `20px × 20px` with a subtle border. Keep `.program-search{width:100%}` inside the existing mobile media query.

- [ ] **Step 4: Run Rewards behavior and contract tests**

Run:

```bash
node tests/rewards-preview-contract.test.js
node --test tests/rewards-program-search.test.js
node --check rewards-search.js
```

Expected: all checks pass and filtering behavior remains unchanged.

- [ ] **Step 5: Bump the Rewards route version**

Change the route version suffix in `rewards/index.html` from `program-search-20260805` to `program-search-parity-20260805` so GitHub Pages clients do not retain the old preview.

- [ ] **Step 6: Commit the Rewards UI unit**

```bash
git add rewards-preview.html rewards/index.html tests/rewards-preview-contract.test.js
git commit -m "fix: align rewards search with holders"
```

---

### Task 4: Full verification and production deployment

**Files:**
- Verify only; modify files only if a scoped regression is found.

**Interfaces:**
- Consumes: the three committed implementation units.
- Produces: verified GitHub Pages deployment from production `master`.

- [ ] **Step 1: Run the targeted suite**

```bash
node --test tests/dolo-vesting-labels.test.js tests/rewards-program-search.test.js
node tests/rewards-preview-contract.test.js
python3 -m unittest tests.test_generate_dolo_flows_integrity tests.test_dolo_address_labels -v
python3 -m py_compile generate_dolo_flows.py
```

Expected: all tests pass.

- [ ] **Step 2: Run repository dashboard checks**

```bash
npm run check:earn-audit
git diff --check
```

Expected: checks exit successfully with no whitespace errors.

- [ ] **Step 3: Serve the static site**

Run:

```bash
python3 -m http.server 8765
```

Use the existing local server in a PTY and leave it running only for browser checks.

- [ ] **Step 4: Verify browser-computed search parity**

At widths 1440, 1024, and 390 pixels, compare `#q-holders` and both Rewards searches using `getComputedStyle()` and bounding boxes. Required assertions:

```javascript
desktop.width === 280
desktop.height === 36
desktop.fontSize === '13px'
desktop.borderRadius === '10px'
mobile.width === availableToolbarWidth
document.documentElement.scrollWidth === document.documentElement.clientWidth
```

Also type `USD1`, clear it with the visible × button, and confirm the Live count and rows return to their initial state. Repeat with Ended Programs.

- [ ] **Step 5: Review scope and synchronize production history**

```bash
git status --short
git diff dolomite-dashboard/master...HEAD --stat
git fetch dolomite-dashboard master
git rebase dolomite-dashboard/master
```

Resolve only scoped conflicts, then rerun Steps 1–2 after the rebase.

- [ ] **Step 6: Push to production**

```bash
git push dolomite-dashboard HEAD:master
```

Expected: push succeeds without force.

- [ ] **Step 7: Verify GitHub Actions and live Pages**

Watch the triggered Actions runs until completion. Confirm the production Rewards route serves the new route version and computed search dimensions, then verify representative strategic, long-term, overlapping, and Core Team addresses on live wallet-table routes.
