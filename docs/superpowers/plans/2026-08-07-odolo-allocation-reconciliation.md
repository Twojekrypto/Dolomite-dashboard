# oDOLO Allocation Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct oDOLO claim provenance and make the two oDOLO charts reconcile to their proper denominators: 200M allocation for Distribution and verified claims-to-date for Claimer Behavior.

**Architecture:** Harden claim identity at both the generic reward-event compatibility boundary and the oDOLO flow loader, then expose an immutable allocation reconciliation from the contract snapshot. Keep the existing static dashboard architecture and generated files, with validation gates preventing impossible totals from being published.

**Tech Stack:** Python 3 generators and `unittest`, static HTML/CSS/JavaScript, JSON artifacts, GitHub Actions, GitHub Pages.

## Global Constraints

- The immutable oDOLO lifecycle allocation is exactly `200_000_000` tokens.
- The only canonical oDOLO Claims distributor is `0x79e6e932bf6686a4d357d7821e6e08835ba8a026`.
- The only canonical oDOLO token address is `0x02e513b5b54ee216bf836ceb471507488fc89543`.
- Keep ERC-20 transfers from the official Claims distributor as the primary claimer source.
- Require both distributor and token address before a RewardClaimed event may supplement oDOLO claims.
- Keep `Held now` outside the Claimer Behavior donut partition.
- Preserve the existing Graphite + Gold visual language and introduce no dependency.
- Do not add a new generated file; existing workflow commit paths remain sufficient.
- Perform token-supply arithmetic in integer token units/wei before converting to display numbers.
- Production is GitHub Pages from `master`; do not assume pushing another branch deploys live.

---

### Task 1: Enforce canonical oDOLO claim provenance

**Files:**
- Modify: `generate_reward_claim_events.py:18-67,323-364,893-921`
- Modify: `generate_odolo_flows.py:14-30,212-263`
- Test: `tests/test_generate_reward_claim_events.py`
- Test: `tests/test_generate_odolo_flows.py`

**Interfaces:**
- Consumes: generic Berachain `RewardClaimed` event dictionaries with `chainKey`, `distributor`, `tokenAddress`, `tokenSymbol`, `amountWei`, and `user`.
- Produces: `ODOLO_CLAIMS_DISTRIBUTOR: str`, a legacy payload containing only canonical oDOLO events, and `load_reward_claims(path, min_block=None) -> dict[str, float]` that fails closed on identity mismatches.

- [ ] **Step 1: Write failing legacy-payload and token-mapping tests**

Add to `tests/test_generate_reward_claim_events.py`:

```python
    def test_legacy_odolo_payload_excludes_non_odolo_distributors(self):
        official = rce.ODOLO_CLAIMS_DISTRIBUTOR
        option_airdrop = rce.OPTION_AIRDROP_DISTRIBUTOR
        payload = {
            "generatedAt": "2026-08-07T00:00:00Z",
            "chains": {"berachain": {"eventEmitter": "0x" + "9" * 40}},
            "events": [
                {
                    "chainKey": "berachain", "distributor": official,
                    "tokenAddress": rce.ODOLO_CONTRACT, "tokenSymbol": "oDOLO",
                    "blockNumber": 100, "timestamp": 1000,
                    "user": "0x" + "1" * 40,
                    "amountWei": str(10 * 10**18),
                },
                {
                    "chainKey": "berachain", "distributor": option_airdrop,
                    "tokenAddress": rce.DOLO_CONTRACT, "tokenSymbol": "DOLO",
                    "blockNumber": 101, "timestamp": 1001,
                    "user": "0x" + "2" * 40,
                    "amountWei": str(20 * 10**18),
                },
            ],
        }

        legacy = rce.build_legacy_odolo_payload(payload)

        self.assertEqual(legacy["distributor"], official)
        self.assertEqual(legacy["distributors"], [official])
        self.assertEqual(len(legacy["events"]), 1)
        self.assertEqual(legacy["events"][0]["user"], "0x" + "1" * 40)
        self.assertEqual(legacy["token"]["address"], rce.ODOLO_CONTRACT)

    def test_documented_dolo_claim_contracts_are_not_odolo(self):
        known = rce.CHAIN_CONFIGS["berachain"]["knownDistributorTokens"]
        for distributor in rce.BERA_DOLO_DISTRIBUTORS:
            with self.subTest(distributor=distributor):
                self.assertEqual(known[distributor]["symbol"], "DOLO")
                self.assertEqual(known[distributor]["address"], rce.DOLO_CONTRACT)
```

- [ ] **Step 2: Write a failing downstream defense test**

Add to `tests/test_generate_odolo_flows.py` and update existing valid event fixtures to include `distributor: odolo_flows.REWARDS_CONTRACT`:

```python
    def test_reward_claim_loader_requires_official_distributor_and_token(self):
        wallet = "0x" + "3" * 40
        payload = {"events": [
            {"user": wallet, "distributor": "0x" + "4" * 40,
             "tokenAddress": odolo_flows.ODOLO_CONTRACT,
             "tokenSymbol": "oDOLO", "amountWei": str(100 * 10**18)},
            {"user": wallet, "distributor": odolo_flows.REWARDS_CONTRACT,
             "tokenAddress": "0x" + "5" * 40,
             "tokenSymbol": "oDOLO", "amountWei": str(200 * 10**18)},
            {"user": wallet, "distributor": odolo_flows.REWARDS_CONTRACT,
             "tokenAddress": odolo_flows.ODOLO_CONTRACT,
             "tokenSymbol": "oDOLO", "amountWei": str(30 * 10**18)},
        ]}
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            json.dump(payload, f)
            path = f.name
        try:
            claims = odolo_flows.load_reward_claims(path)
        finally:
            os.unlink(path)

        self.assertEqual(claims, {wallet: 30.0})
```

- [ ] **Step 3: Run the focused tests and confirm RED**

```bash
python3 -m unittest \
  tests.test_generate_reward_claim_events.RewardClaimTimestampReuseTests.test_legacy_odolo_payload_excludes_non_odolo_distributors \
  tests.test_generate_reward_claim_events.RewardClaimTimestampReuseTests.test_documented_dolo_claim_contracts_are_not_odolo \
  tests.test_generate_odolo_flows.GenerateOdoloFlowsTests.test_reward_claim_loader_requires_official_distributor_and_token
```

Expected: FAIL because canonical distributor constants/mapping and strict filtering do not exist.

- [ ] **Step 4: Implement canonical mappings and strict compatibility output**

In `generate_reward_claim_events.py`:

```python
DOLO_CONTRACT = "0x0f81001ef0a83ecce5ccebf63eb302c70a39a654"
ODOLO_CONTRACT = "0x02e513b5b54ee216bf836ceb471507488fc89543"
ODOLO_CLAIMS_DISTRIBUTOR = "0x79e6e932bf6686a4d357d7821e6e08835ba8a026"
OPTION_AIRDROP_DISTRIBUTOR = "0xd88f473832b0403c7736ef237af5aff8759b99ef"
REGULAR_AIRDROP_DISTRIBUTOR = "0xa3f079292cc35ba64996fe0bce3049928a838bc9"
INVESTOR_CLAIMS_DISTRIBUTOR = "0x3a025c7fcf7632197ea82e64acd6ff53e1c06c07"
STRATEGIC_INVESTOR_CLAIMS_DISTRIBUTOR = "0x7efd088ae500598a19a242d6d48b9f7e0d061176"
ADVISOR_CLAIMS_DISTRIBUTOR = "0xbd225c09e4b032e41d5e8aea5f81efff45f20f7b"

BERA_ODOLO_DISTRIBUTORS = {ODOLO_CLAIMS_DISTRIBUTOR}
BERA_DOLO_DISTRIBUTORS = {
    OPTION_AIRDROP_DISTRIBUTOR,
    REGULAR_AIRDROP_DISTRIBUTOR,
    INVESTOR_CLAIMS_DISTRIBUTOR,
    STRATEGIC_INVESTOR_CLAIMS_DISTRIBUTOR,
    ADVISOR_CLAIMS_DISTRIBUTOR,
}
BERA_KNOWN_DISTRIBUTOR_TOKENS = {
    ODOLO_CLAIMS_DISTRIBUTOR: {
        "symbol": "oDOLO", "address": ODOLO_CONTRACT, "decimals": 18,
    },
    **{
        distributor: {"symbol": "DOLO", "address": DOLO_CONTRACT, "decimals": 18}
        for distributor in BERA_DOLO_DISTRIBUTORS
    },
}
```

Use `BERA_KNOWN_DISTRIBUTOR_TOKENS` for `knownDistributorTokens`. Filter `build_legacy_odolo_payload` by normalized chain, official distributor and exact token address; compute range metadata from the filtered events and emit exact canonical `token`, `distributor`, and `distributors` values.

- [ ] **Step 5: Implement defense-in-depth in the flow loader**

In `generate_odolo_flows.py`:

```python
        distributor = normalize_address(event.get("distributor") or payload.get("distributor"))
        token = normalize_address(event.get("tokenAddress"))
        if distributor != REWARDS_CONTRACT or token != ODOLO_CONTRACT:
            continue
```

Remove the symbol-only acceptance path; keep amount parsing and per-wallet aggregation unchanged.

- [ ] **Step 6: Run the full provenance suites and confirm GREEN**

```bash
python3 -m unittest tests.test_generate_reward_claim_events tests.test_generate_odolo_flows
```

Expected: PASS.

- [ ] **Step 7: Commit the provenance correction**

```bash
git add generate_reward_claim_events.py generate_odolo_flows.py \
  tests/test_generate_reward_claim_events.py tests/test_generate_odolo_flows.py
git commit -m "fix: enforce canonical oDOLO claim provenance"
```

---

### Task 2: Add immutable allocation and burned-supply metrics

**Files:**
- Modify: `fetch_odolo_contract.py:15-55,78-125`
- Test: `tests/test_odolo_contract_supply.py`

**Interfaces:**
- Consumes: raw integer ERC-20 `totalSupply` and token `decimals` from Berachain RPC.
- Produces: `derive_allocation_metrics(total_supply_wei: int, decimals: int) -> dict` containing `allocationSupply`, `redeemedAndBurned`, and `allocationMethodology`.

- [ ] **Step 1: Write failing allocation derivation tests**

Add to `tests/test_odolo_contract_supply.py`:

```python
    def test_allocation_metrics_derive_burned_supply_from_integer_units(self):
        metrics = fetch_odolo_contract.derive_allocation_metrics(
            147_113_292 * 10**18,
            18,
        )
        self.assertEqual(metrics["allocationSupply"], 200_000_000)
        self.assertEqual(metrics["redeemedAndBurned"], 52_886_708)
        self.assertIn("allocationSupply - totalSupply", metrics["allocationMethodology"])

    def test_allocation_metrics_reject_total_supply_above_allocation(self):
        with self.assertRaises(ValueError):
            fetch_odolo_contract.derive_allocation_metrics(
                200_000_001 * 10**18,
                18,
            )
```

- [ ] **Step 2: Run the new tests and confirm RED**

```bash
python3 -m unittest \
  tests.test_odolo_contract_supply.TestOdoloContractSupply.test_allocation_metrics_derive_burned_supply_from_integer_units \
  tests.test_odolo_contract_supply.TestOdoloContractSupply.test_allocation_metrics_reject_total_supply_above_allocation
```

Expected: FAIL because `derive_allocation_metrics` does not exist.

- [ ] **Step 3: Implement integer allocation derivation**

Add to `fetch_odolo_contract.py`:

```python
ODOLO_ALLOCATION_TOKENS = 200_000_000


def derive_allocation_metrics(total_supply_wei, decimals):
    unit = 10 ** int(decimals)
    allocation_wei = ODOLO_ALLOCATION_TOKENS * unit
    if total_supply_wei > allocation_wei:
        raise ValueError("oDOLO totalSupply exceeds the immutable 200M allocation")
    burned_wei = allocation_wei - total_supply_wei
    return {
        "allocationSupply": ODOLO_ALLOCATION_TOKENS,
        "redeemedAndBurned": burned_wei / unit,
        "allocationMethodology": "allocationSupply - totalSupply; exercised oDOLO is burned",
    }
```

Store `total_supply_wei = decode_uint256(batch1[0])`, use it for `totalSupply`, and merge `derive_allocation_metrics(total_supply_wei, decimals)` before writing JSON.

- [ ] **Step 4: Run the supply tests and confirm GREEN**

```bash
python3 -m unittest tests.test_odolo_contract_supply
```

Expected: PASS.

- [ ] **Step 5: Commit allocation metrics**

```bash
git add fetch_odolo_contract.py tests/test_odolo_contract_supply.py
git commit -m "feat: reconcile oDOLO lifecycle allocation"
```

---

### Task 3: Add publication-blocking data invariants

**Files:**
- Modify: `validate_data.py:174-188,259-289,846-860,989-1013,1184-1208`
- Test: `tests/test_odolo_contract_supply.py`
- Test: `tests/test_odolo_flows_validation.py`

**Interfaces:**
- Consumes: `odolo_contract_data.json`, `odolo_flows.json`, and `data/odolo-claim-events.json` payload dictionaries.
- Produces: `_odolo_allocation_reconciles`, `_odolo_claim_total_within_allocation`, and `_odolo_claim_events_are_canonical`, registered in `FILES_TO_VALIDATE`.

- [ ] **Step 1: Write failing reconciliation and claim-ceiling tests**

Add focused tests:

```python
    def test_allocation_components_must_reconcile_to_200m(self):
        payload = {
            "allocationSupply": 200_000_000,
            "totalSupply": 147_000_000,
            "futureRewardsReserve": 126_000_000,
            "inVesterBalance": 3_000_000,
            "inCirculation": 18_000_000,
            "redeemedAndBurned": 53_000_000,
        }
        self.assertTrue(validate_data._odolo_allocation_reconciles(payload))
        payload["redeemedAndBurned"] = 52_000_000
        self.assertFalse(validate_data._odolo_allocation_reconciles(payload))
```

In `tests/test_odolo_flows_validation.py`:

```python
    def test_claimer_total_must_not_exceed_allocation(self):
        self.assertTrue(validate_data._odolo_claim_total_within_allocation({
            "claimer_behavior": {"total_claimed": 53_911_566}
        }))
        self.assertFalse(validate_data._odolo_claim_total_within_allocation({
            "claimer_behavior": {"total_claimed": 200_000_001}
        }))
```

- [ ] **Step 2: Write a failing canonical-event validation test**

```python
    def test_legacy_odolo_events_require_canonical_identity(self):
        valid = {"events": [{
            "distributor": validate_data.ODOLO_CLAIMS_DISTRIBUTOR,
            "tokenAddress": validate_data.ODOLO_TOKEN_ADDRESS,
        }]}
        self.assertTrue(validate_data._odolo_claim_events_are_canonical(valid))
        valid["events"][0]["distributor"] = "0x" + "1" * 40
        self.assertFalse(validate_data._odolo_claim_events_are_canonical(valid))
```

- [ ] **Step 3: Run the tests and confirm RED**

```bash
python3 -m unittest tests.test_odolo_contract_supply tests.test_odolo_flows_validation
```

Expected: FAIL for missing validator functions/constants.

- [ ] **Step 4: Implement and register strict validators**

In `validate_data.py`:

```python
ODOLO_ALLOCATION = 200_000_000.0
ODOLO_TOKEN_ADDRESS = "0x02e513b5b54ee216bf836ceb471507488fc89543"
ODOLO_CLAIMS_DISTRIBUTOR = "0x79e6e932bf6686a4d357d7821e6e08835ba8a026"


def _odolo_allocation_reconciles(data):
    try:
        allocation = float(data.get("allocationSupply"))
        current = float(data.get("totalSupply"))
        components = sum(float(data.get(key)) for key in (
            "futureRewardsReserve", "inVesterBalance", "inCirculation", "redeemedAndBurned"
        ))
        burned = float(data.get("redeemedAndBurned"))
    except (TypeError, ValueError):
        return False
    return (
        _nearly_equal(allocation, ODOLO_ALLOCATION, abs_tol=0.01)
        and current <= allocation and burned >= 0
        and _nearly_equal(components, allocation, abs_tol=2.0)
        and _nearly_equal(current + burned, allocation, abs_tol=2.0)
    )


def _odolo_claim_total_within_allocation(data):
    try:
        claimed = float((data.get("claimer_behavior") or {}).get("total_claimed"))
    except (TypeError, ValueError):
        return False
    return 0 <= claimed <= ODOLO_ALLOCATION


def _odolo_claim_events_are_canonical(data):
    events = data.get("events") or []
    return bool(events) and all(
        str(event.get("distributor") or "").lower() == ODOLO_CLAIMS_DISTRIBUTOR
        and str(event.get("tokenAddress") or "").lower() == ODOLO_TOKEN_ADDRESS
        for event in events
    )
```

Register these checks and require the new contract fields. Replace weaker legacy token/distributor checks with the canonical combined check.

- [ ] **Step 5: Run validation tests and confirm GREEN**

```bash
python3 -m unittest tests.test_odolo_contract_supply tests.test_odolo_flows_validation
```

Expected: PASS.

- [ ] **Step 6: Commit publication gates**

```bash
git add validate_data.py tests/test_odolo_contract_supply.py tests/test_odolo_flows_validation.py
git commit -m "test: block invalid oDOLO allocation data"
```

---

### Task 4: Rebuild chart presentation around correct denominators

**Files:**
- Modify: `odolo-preview.html:1290-1335,2258-2292,3915-4003`
- Test: `tests/test_odolo_preview_contracts.py`

**Interfaces:**
- Consumes: `LIVE.contract.allocationSupply`, `LIVE.contract.redeemedAndBurned`, and `LIVE.flows.claimer_behavior.total_claimed`.
- Produces: a four-segment Distribution donut totaling 200M and Claimer Behavior metadata reading `X oDOLO claimed of 200M allocation`.

- [ ] **Step 1: Write failing UI contract tests**

Add to `tests/test_odolo_preview_contracts.py`:

```python
    def test_distribution_reconciles_full_allocation_with_burned_segment(self):
        self.assertIn('label:"Redeemed & burned"', self.html)
        self.assertIn('finiteNum(data.allocationSupply, 200e6)', self.html)
        self.assertIn('finiteNum(data.redeemedAndBurned', self.html)
        self.assertIn('centerLDefault:"Allocation"', self.html)
        self.assertIn('`${fmtNum(allocationSupply)} allocation`', self.html)

    def test_claimer_behavior_displays_allocation_context(self):
        self.assertIn(
            'oDOLO claimed of ${fmtNum(allocationSupply)} allocation',
            self.html,
        )
```

- [ ] **Step 2: Run the UI tests and confirm RED**

```bash
python3 -m unittest tests.test_odolo_preview_contracts
```

Expected: FAIL because the fourth segment and allocation copy are absent.

- [ ] **Step 3: Implement the Distribution UI**

Update the fallback rows:

```javascript
const DIST_SEGMENTS = [
  {label:"Future rewards reserve", value:126.92e6, color:"#e4c15a", desc:"Held by the official oDOLO Claims contract for future rewards", fmt:"126.92M"},
  {label:"Circulating", value:16.79e6, color:"#a8c499", desc:"Current supply outside the rewards reserve and vester", fmt:"16.79M"},
  {label:"Vester balance", value:3.40e6, color:"#a78bfa", desc:"oDOLO currently held by the Pair Vester", fmt:"3.40M"},
  {label:"Redeemed & burned", value:52.89e6, color:"#a86f5d", desc:"Exercised oDOLO removed from current ERC-20 supply", fmt:"52.89M"},
];
```

Use `centerLDefault:"Allocation"`. In `syncLiveMetrics`, set `dist-count` from `allocationSupply`. In `syncLiveDistribution`, compute:

```javascript
  const allocationSupply = finiteNum(data.allocationSupply, 200e6);
  const redeemedAndBurned = finiteNum(
    data.redeemedAndBurned,
    Math.max(0, allocationSupply - totalSupply),
  );
```

Pass all four rows to `applyRows`, with a restrained muted-rust burned color.

- [ ] **Step 4: Implement Claimer Behavior allocation context**

```javascript
  const allocationSupply = finiteNum(LIVE.contract?.allocationSupply, 200e6);
  setHtml(
    "behav-meta",
    `<span class="pulse"></span>${fmtNum(claimed)} oDOLO claimed of ${fmtNum(allocationSupply)} allocation`,
  );
```

Keep the three lifecycle segments and current `Held now` treatment unchanged.

- [ ] **Step 5: Run the UI suite and confirm GREEN**

```bash
python3 -m unittest tests.test_odolo_preview_contracts
```

Expected: PASS.

- [ ] **Step 6: Commit chart UX changes**

```bash
git add odolo-preview.html tests/test_odolo_preview_contracts.py
git commit -m "fix: reconcile oDOLO distribution charts"
```

---

### Task 5: Regenerate artifacts, verify end to end, and deploy

**Files:**
- Modify generated: `odolo_contract_data.json`
- Modify generated: `data/odolo-claim-events.json`
- Modify generated: `data/reward-claim-events/berachain.json` only when intentionally refreshed
- Modify generated: `odolo_flows.json`
- Verify: `.github/workflows/update-odolo-data.yml`
- Verify: `.github/workflows/update-odolo-flows.yml`
- Verify: `.github/workflows/update-reward-claim-events.yml`

**Interfaces:**
- Consumes: corrected generators and live Berachain RPC/event data.
- Produces: publishable JSON, verified static UI, and a production `master` commit.

- [ ] **Step 1: Regenerate the contract snapshot**

```bash
python3 fetch_odolo_contract.py
```

Expected: `allocationSupply == 200000000`, `redeemedAndBurned >= 0`, and RPC failure preserves the previous file.

- [ ] **Step 2: Rebuild strict reward compatibility data**

Run the normal generator when RPC availability permits:

```bash
REWARD_CLAIM_CHAINS=berachain python3 generate_reward_claim_events.py
```

If a full scan cannot complete, do not publish a partial shard. Rebuild only the compatibility payload from the existing complete Berachain shard:

```bash
python3 - <<'PY'
import json
from pathlib import Path

import generate_reward_claim_events as rewards

shard = json.loads(Path("data/reward-claim-events/berachain.json").read_text())
payload = {
    "generatedAt": shard.get("generatedAt"),
    "chains": shard.get("chains") or {},
    "events": shard.get("events") or [],
}
legacy = rewards.build_legacy_odolo_payload(payload)
rewards.save_json(rewards.LEGACY_ODOLO_OUTPUT_JSON, legacy, compact=True)
PY
```

The payload must contain one distributor and one token identity matching the Global Constraints.

- [ ] **Step 3: Regenerate oDOLO flows**

```bash
python3 generate_odolo_flows.py
```

Expected: `claimer_behavior.total_claimed` is near the canonical official-claims transfer total (currently about 53.91M), is at most 200M, and no non-canonical distributor contributes a claim.

- [ ] **Step 4: Run focused regression and validation suites**

```bash
python3 -m unittest \
  tests.test_generate_reward_claim_events \
  tests.test_generate_odolo_flows \
  tests.test_odolo_contract_supply \
  tests.test_odolo_flows_validation \
  tests.test_odolo_preview_contracts
python3 validate_data.py
```

Expected: all focused tests and affected JSON validations pass.

- [ ] **Step 5: Inspect numerical reconciliation directly**

```bash
jq '{allocationSupply,totalSupply,futureRewardsReserve,inCirculation,inVesterBalance,redeemedAndBurned,total:(.futureRewardsReserve + .inCirculation + .inVesterBalance + .redeemedAndBurned)}' odolo_contract_data.json
jq '.claimer_behavior | {total_claimers,total_claimed,pct_exercised,pct_outflow,pct_claim_remaining}' odolo_flows.json
jq '[.events[].distributor] | unique' data/odolo-claim-events.json
```

Expected: distribution `total` equals 200M within display tolerance; claimed is at most 200M; distributor list contains only the official Claims address.

- [ ] **Step 6: Review generated diff scope**

```bash
git status --short
git diff --stat
git diff --check
git diff -- odolo_contract_data.json data/odolo-claim-events.json odolo_flows.json | sed -n '1,240p'
```

Expected: no unrelated generated data, workflows, tables, or layout files changed.

- [ ] **Step 7: Verify the rendered page at desktop and mobile widths**

Start the required server:

```bash
python3 -m http.server 8765
```

Open `http://127.0.0.1:8765/odolo/` at approximately `1440x1000`, `768x1024`, and `390x844`. Verify four Distribution rows, a 200M allocation center/count, current total supply in the hero, claimed-to-date allocation context, no legend clipping, no horizontal page overflow, and no console errors. Confirm geometry with bounding boxes and `getComputedStyle()`.

- [ ] **Step 8: Run verification-before-completion and commit generated artifacts**

```bash
git add odolo_contract_data.json data/odolo-claim-events.json odolo_flows.json
git add data/reward-claim-events/berachain.json  # only if intentionally refreshed
git commit -m "data: refresh corrected oDOLO metrics"
```

Re-run Step 4 after the commit.

- [ ] **Step 9: Rebase on the latest production branch and repeat verification**

```bash
git fetch dolomite-dashboard master
git rebase dolomite-dashboard/master
```

Resolve only conflicts in files owned by this plan, then repeat Steps 4-7.

- [ ] **Step 10: Push the verified commit set live**

```bash
git push dolomite-dashboard HEAD:master
```

Expected: push succeeds without force. Wait for GitHub Pages and affected update workflows, then verify `https://twojekrypto.github.io/Dolomite-dashboard/odolo/?v=<commit>`. If automation commits refreshed data afterward, confirm it preserves all new invariants.
