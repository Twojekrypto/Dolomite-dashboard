# veDOLO Green Surface and TVL Metadata Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply one continuous, subtly green veDOLO activity surface and make TVL collection recover from transient incomplete subgraph metadata without weakening validation.

**Architecture:** The UI change stays in the existing portfolio preview and keeps every child layer transparent over one parent gradient. The data fix isolates GraphQL fetching in a small retry helper that validates provenance metadata before any token or price API calls, while the existing all-active-chains guard remains responsible for refusing partial output.

**Tech Stack:** Static HTML/CSS, Python 3.11, `unittest`, Requests, GitHub Actions, GitHub Pages.

## Global Constraints

- Preserve the existing Graphite + Gold identity and the approved `#75b87b` veDOLO accent.
- Keep the veDOLO section one continuous surface; only the table header may remain darker.
- Do not relax `validate_data.py`, copy stale metadata, or fabricate provenance.
- Retry delays must remain bounded at exactly `2`, `4`, and `8` seconds.
- Do not add dependencies or change TVL calculation formulas.

---

### Task 1: Continuous Green veDOLO Activity Surface

**Files:**
- Modify: `tests/test_data_freshness_surface_contracts.py:51-82`
- Modify: `tests/test_institutional_table_market_ux_contracts.py:27-45`
- Modify: `portfolio-preview.html:1442-1449`
- Modify: `portfolio/index.html:18-22`

**Interfaces:**
- Consumes: the existing `#pf-exercises-section` parent and transparent child-surface rules.
- Produces: one green-to-graphite parent surface whose children remain transparent and whose route version invalidates the browser cache.

- [ ] **Step 1: Write the failing surface contract**

Replace the solid-background expectations in both veDOLO contract tests with:

```python
self.assertIn(
    "linear-gradient(180deg,rgba(117,184,123,.055),rgba(15,17,15,.82))",
    section,
)
self.assertIn("var(--bg-2)", section)
```

Keep the existing assertions that `.card-head`,
`.pf-exercise-summary.selected-market-rail`, `.pf-filters`, and `.tbl-foot`
use `background:transparent`, and that the table header uses
`background:var(--bg-1)`.

- [ ] **Step 2: Run the contract tests and confirm RED**

Run:

```bash
python3 -m unittest \
  tests.test_data_freshness_surface_contracts.DataFreshnessSurfaceContractsTest.test_position_activity_uses_consistent_surface_and_correct_units \
  tests.test_institutional_table_market_ux_contracts.InstitutionalTableMarketUxContracts.test_vedolo_activity_uses_one_continuous_surface
```

Expected: both tests fail because `#pf-exercises-section` still contains only
`background:var(--bg-2)`.

- [ ] **Step 3: Apply the approved parent gradient**

Change only the parent background in `portfolio-preview.html`:

```css
#pf-exercises-section{
  position:relative;
  --pf-ex-green:#75b87b;
  --pf-ex-line:rgba(117,184,123,.30);
  --pf-ex-wash:rgba(117,184,123,.10);
  border-color:rgba(117,184,123,.18);
  background:
    linear-gradient(180deg,rgba(117,184,123,.055),rgba(15,17,15,.82)),
    var(--bg-2);
}
```

Append `-green-surface-20260730` to the existing `version` value in
`portfolio/index.html`.

- [ ] **Step 4: Run the focused UI tests and confirm GREEN**

Run:

```bash
python3 -m unittest \
  tests.test_data_freshness_surface_contracts \
  tests.test_institutional_table_market_ux_contracts \
  tests.test_portfolio_preview_contracts
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit the UI change**

```bash
git add portfolio-preview.html portfolio/index.html \
  tests/test_data_freshness_surface_contracts.py \
  tests/test_institutional_table_market_ux_contracts.py
git commit -m "style: add continuous green veDOLO activity surface"
```

### Task 2: Retry Incomplete TVL Subgraph Metadata

**Files:**
- Modify: `tests/test_fetch_dolomite_tvl.py:1-65`
- Modify: `fetch_dolomite_tvl.py:14-125`
- Modify: `fetch_dolomite_tvl.py:363-389`

**Interfaces:**
- Produces: `incomplete_chain_meta_fields(data: dict) -> list[str]`.
- Produces: `fetch_subgraph_payload(chain_name: str, url: str) -> dict`.
- Consumes: `QUERY`, `requests.post`, and `time.sleep`.
- Preserves: `blocking_tvl_failures(failed_chains, chain_payloads)` and the existing snapshot builders.

- [ ] **Step 1: Write the failing retry tests**

Add two tests to `FetchDolomiteTvlTest`:

```python
def test_subgraph_payload_retries_incomplete_metadata(self):
    incomplete = Mock()
    incomplete.raise_for_status.return_value = None
    incomplete.json.return_value = {
        "data": {
            "tokens": [],
            "_meta": {
                "block": {"number": 123, "hash": None, "timestamp": 1_783_728_000},
                "deployment": "dolomite-arbitrum",
            },
        }
    }
    complete_data = {
        "tokens": [],
        "_meta": {
            "block": {
                "number": 124,
                "hash": "0xabc",
                "timestamp": 1_783_728_010,
            },
            "deployment": "dolomite-arbitrum",
        },
    }
    complete = Mock()
    complete.raise_for_status.return_value = None
    complete.json.return_value = {"data": complete_data}

    with patch.object(
        fetch_dolomite_tvl.requests,
        "post",
        side_effect=[incomplete, complete],
    ), patch.object(fetch_dolomite_tvl.time, "sleep") as sleep_mock:
        result = fetch_dolomite_tvl.fetch_subgraph_payload(
            "Arbitrum",
            "https://example.test/subgraph",
        )

    self.assertEqual(complete_data, result)
    self.assertEqual([call(2)], sleep_mock.call_args_list)

def test_subgraph_payload_refuses_persistently_incomplete_metadata(self):
    incomplete = Mock()
    incomplete.raise_for_status.return_value = None
    incomplete.json.return_value = {
        "data": {
            "tokens": [],
            "_meta": {
                "block": {"number": 123, "hash": "", "timestamp": None},
                "deployment": "",
            },
        }
    }

    with patch.object(
        fetch_dolomite_tvl.requests,
        "post",
        return_value=incomplete,
    ), patch.object(fetch_dolomite_tvl.time, "sleep"):
        with self.assertRaisesRegex(
            RuntimeError,
            r"Arbitrum subgraph metadata incomplete: "
            r"block\.hash, block\.timestamp, deployment",
        ):
            fetch_dolomite_tvl.fetch_subgraph_payload(
                "Arbitrum",
                "https://example.test/subgraph",
            )
```

- [ ] **Step 2: Run the two new tests and confirm RED**

Run:

```bash
python3 -m unittest \
  tests.test_fetch_dolomite_tvl.FetchDolomiteTvlTest.test_subgraph_payload_retries_incomplete_metadata \
  tests.test_fetch_dolomite_tvl.FetchDolomiteTvlTest.test_subgraph_payload_refuses_persistently_incomplete_metadata
```

Expected: both tests error because `fetch_subgraph_payload` does not exist.

- [ ] **Step 3: Add strict metadata validation and bounded retry**

Add next to the existing retry constants:

```python
SUBGRAPH_RETRY_DELAYS = (2, 4, 8)
```

Add before the token API fetch helpers:

```python
def incomplete_chain_meta_fields(data):
    meta = (data or {}).get("_meta", {}) or {}
    block = meta.get("block", {}) or {}
    missing = []
    for field in ("number", "timestamp"):
        value = block.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            missing.append(f"block.{field}")
    block_hash = block.get("hash")
    if not isinstance(block_hash, str) or not block_hash.strip():
        missing.append("block.hash")
    deployment = meta.get("deployment")
    if not isinstance(deployment, str) or not deployment.strip():
        missing.append("deployment")
    return missing


def fetch_subgraph_payload(chain_name, url):
    attempts = len(SUBGRAPH_RETRY_DELAYS) + 1
    for attempt in range(attempts):
        try:
            resp = requests.post(url, json={"query": QUERY}, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise RuntimeError("GraphQL response is not a JSON object")
            if payload.get("errors"):
                raise RuntimeError(str(payload["errors"]))
            data = payload.get("data")
            if not isinstance(data, dict) or not data:
                raise RuntimeError("empty GraphQL data")
            missing = incomplete_chain_meta_fields(data)
            if missing:
                raise RuntimeError(
                    f"{chain_name} subgraph metadata incomplete: {', '.join(missing)}"
                )
            return data
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            if attempt == len(SUBGRAPH_RETRY_DELAYS):
                raise
            delay = SUBGRAPH_RETRY_DELAYS[attempt]
            print(
                f"⚠️ {chain_name} subgraph response invalid ({exc}); "
                f"retrying in {delay}s ({attempt + 1}/{attempts})"
            )
            time.sleep(delay)

    raise RuntimeError(f"{chain_name} subgraph retry loop exited unexpectedly")
```

Replace the inline GraphQL request and payload parsing in `main()` with:

```python
data = fetch_subgraph_payload(chain_name, url)
```

Keep token liquidity and price requests after this call.

- [ ] **Step 4: Run the focused TVL tests and confirm GREEN**

Run:

```bash
python3 -m unittest tests.test_fetch_dolomite_tvl
python3 -m py_compile fetch_dolomite_tvl.py
```

Expected: all TVL unit tests pass and Python compilation exits successfully.

- [ ] **Step 5: Commit the workflow fix**

```bash
git add fetch_dolomite_tvl.py tests/test_fetch_dolomite_tvl.py
git commit -m "fix: retry incomplete TVL subgraph metadata"
```

### Task 3: Integrated Verification and Production Publish

**Files:**
- Verify: `portfolio-preview.html`
- Verify: `portfolio/index.html`
- Verify: `fetch_dolomite_tvl.py`
- Verify: `tests/test_fetch_dolomite_tvl.py`
- Verify: `tests/test_data_freshness_surface_contracts.py`
- Verify: `tests/test_institutional_table_market_ux_contracts.py`

**Interfaces:**
- Consumes: the two independently committed deliverables from Tasks 1 and 2.
- Produces: verified production `master` and a live GitHub Pages result.

- [ ] **Step 1: Run the complete relevant regression set**

```bash
python3 -m unittest \
  tests.test_fetch_dolomite_tvl \
  tests.test_data_freshness_surface_contracts \
  tests.test_institutional_table_market_ux_contracts \
  tests.test_portfolio_preview_contracts
python3 validate_data.py dolomite_tvl.json
git diff --check dolomite-dashboard/master...HEAD
```

Expected: all tests and validation pass, and `git diff --check` prints no
errors.

- [ ] **Step 2: Verify the real UI through a local HTTP server**

Start:

```bash
python3 -m http.server 4173
```

Open `http://127.0.0.1:4173/portfolio/` in a browser. Enter a valid wallet if
the activity section requires an active portfolio state. Verify with computed
styles and bounding boxes:

```javascript
const section = document.querySelector('#pf-exercises-section');
const head = section.querySelector('.card-head');
const summary = section.querySelector('.pf-exercise-summary');
({
  sectionBackground: getComputedStyle(section).backgroundImage,
  headBackground: getComputedStyle(head).backgroundColor,
  summaryBackground: getComputedStyle(summary).backgroundColor,
  sectionRect: section.getBoundingClientRect(),
  headRect: head.getBoundingClientRect(),
  summaryRect: summary.getBoundingClientRect(),
});
```

Expected: the parent reports the approved green gradient; child backgrounds are
transparent; header and summary align to the same section width without gaps or
separate cards. Also inspect desktop and mobile widths.

- [ ] **Step 3: Rebase over any new automated data commits**

```bash
git fetch dolomite-dashboard master
git rebase dolomite-dashboard/master
```

If the rebase changes the base, rerun Step 1 before publishing.

- [ ] **Step 4: Push the verified branch to production master**

```bash
git push dolomite-dashboard HEAD:master
```

Expected: GitHub accepts the push and starts the relevant Actions runs.

- [ ] **Step 5: Confirm Actions and the live route**

Use `gh run list` and `gh run watch` for the Pages deployment and relevant
workflow checks. Open the production portfolio route, bypass its cache, and
repeat the computed-style check from Step 2.

Expected: checks succeed and the live route reports the new version and
approved parent gradient.
