# 📊 veDOLO Dashboard

Real-time analytics dashboard for the [Dolomite](https://dolomite.io) protocol — tracking DOLO token, veDOLO governance, oDOLO options, and DeFi earnings across Berachain, Ethereum, Arbitrum, and more.

**🔗 [Live Dashboard](https://twojekrypto.github.io/vedolo-dashboard/)**

![Dashboard Preview](preview.png)

---

## ✨ Features

### 🪙 DOLO Tab
- Live DOLO price, market cap, FDV, 24h volume
- Total TVL breakdown by chain with interactive charts
- TVL History (7D / 30D / 90D / 180D / 1Y)
- Token composition donut chart
- Full DOLO holders ranking with contract/multisig/team labels
- DeBank & Etherscan/Berascan integration per holder

### 🏛️ veDOLO Tab
- Unique holders, active locks, DOLO locked, total vote weight
- Sortable/searchable holders table
- Detailed lock profiles with individual lock cards (amount, unlock date, vote weight)
- Early exit tracking with penalty data

### ⚡ oDOLO Tab
- Vester balance, exercised amount, revenue generated
- Average veDOLO price tracking
- Full exerciser history with per-address breakdowns
- Sticky headers and smooth scrolling modals

### 💰 Earn Tab
- Multi-chain yield tracking (Ethereum, Arbitrum, Berachain, Mantle, + more)
- Real-time balance fetching via on-chain RPC calls
- Historical yield comparison with snapshot calendar
- Smart period selection (3D / 7D / 14D / 21D / 28D)
- Optional local per-address verified ledger source (`data/earn-verified-ledger`) for higher-confidence totals

---

## ✅ Verified EARN Ledger (Phase 1)

You can precompute deterministic, per-address EARN yield files locally and let the UI use them as the preferred source for `Total Yield`.
These files are intended to stay private and should not be committed to git.

```bash
# Example: build verified files for selected addresses
python3 build_earn_verified_ledger.py \
  --chain arbitrum \
  --chain ethereum \
  --address 0xYourAddress1 \
  --address 0xYourAddress2
```

Outputs:
- `data/earn-verified-ledger/{chain}/{address}.json`
- `data/earn-verified-ledger/manifest.json`

When a file exists for the queried wallet, EARN uses it before snapshot fallback logic.

## 🔎 EARN Asset Audit

For repeatable local audits of a specific EARN asset, use:

```bash
# 1. Build a fast static cohort from snapshots + netflow
python3 audit_earn_asset.py static \
  --chain arbitrum \
  --symbol USDC

# 2. Run a live replay audit against localhost + Chrome remote debugger
python3 audit_earn_asset.py live \
  --chain arbitrum \
  --symbol USDC \
  --localhost-url 'http://127.0.0.1:8902/index.html?cb=usdc_audit' \
  --debug-json-url 'http://127.0.0.1:9555/json'

# 3. Summarize an existing live audit result
python3 audit_earn_asset.py summarize-live \
  --results /tmp/earn_audit_arbitrum_usdc_live.json

# 4. Merge a full pass with retry passes (latest row wins per wallet)
python3 audit_earn_asset.py merge-live \
  --results /tmp/usdc_full.json /tmp/usdc_timeout_retry.json /tmp/usdc_final_retry.json \
  --output /tmp/usdc_merged.json

# 5. Extract a focused retry cohort from an existing live result
python3 audit_earn_asset.py extract-live \
  --results /tmp/usdc_merged.json \
  --mode blocking \
  --chain arbitrum \
  --output /tmp/usdc_blocking_retry.json

# 6. Build a forensic report for the remaining real blockers
python3 audit_earn_asset.py forensic-live \
  --results /tmp/usdc_merged.json \
  --output /tmp/usdc_forensic.json
```

Notes:
- audit outputs default to `/tmp` and should stay local/private
- `static` is fast and good for cohort sizing plus unresolved filtering
- `live` is slower but checks the real replay/verification behavior of the UI
- `summarize-live` also groups root causes / patterns (for example timeout-heavy cases vs real snapshot mismatches)
- `merge-live` should be used after retries so the newest row for each wallet replaces older pass results
- `extract-live` is the easiest way to build timeout/blocking retry inputs without hand-editing JSON
- `forensic-live` turns the remaining real tail into a focused blocker report with root causes and current-state context

---

## ⚙️ Data Pipeline

Data is auto-updated via **GitHub Actions** workflows:

| Workflow | Schedule | Description |
|----------|----------|-------------|
| `Update veDOLO Data` | Every hour | veDOLO holders, DOLO price, DeFi Llama TVL, oDOLO contract data |
| `Update DOLO Holders` | Every hour | Incremental DOLO token holder scanning (Ethereum + Berachain) |
| `Update oDOLO Data` | Daily (3:00 UTC) | Exercised USD totals, average lock durations, exerciser profiles |

---

## 🛠️ Tech Stack

- **Frontend**: Vanilla HTML/CSS/JS — single `index.html` with zero framework dependencies
- **Data Scripts**: Python 3.11 (`requests`, `web3`)
- **APIs**: Berascan, Etherscan, DeFi Llama, Dolomite RPC
- **Hosting**: GitHub Pages
- **CI/CD**: GitHub Actions with incremental caching

---

## 🚀 Local Development

```bash
# Clone the repo
git clone https://github.com/Twojekrypto/vedolo-dashboard.git
cd vedolo-dashboard

# Serve locally
python3 -m http.server 8080
# → Open http://localhost:8080
```

---

## 📁 Project Structure

```
├── index.html                  # Main dashboard (HTML + CSS + JS)
├── preview.png                 # Dashboard preview screenshot
├── *.svg                       # Logo assets (DOLO, veDOLO, oDOLO)
│
├── update_data.py              # veDOLO holders + price fetcher
├── generate_dolo_holders.py    # DOLO holder scanner (incremental)
├── generate_exercisers.py      # oDOLO exerciser profiler
├── fetch_defillama.py          # DeFi Llama TVL data
├── fetch_odolo_contract.py     # oDOLO contract state
├── fetch_early_exits.py        # Early exit penalty tracker
├── calculate_avg_lock.py       # Average lock duration calculator
├── update_exercised_usd.py     # Exercised USD aggregator
├── scan_earn_netflow.py        # Earn tab netflow scanner
│
├── *.json / *.csv              # Auto-generated data files
├── data/                       # Earn snapshots (historical)
└── .github/workflows/          # GitHub Actions automation
```

---

## 📜 License

This project is open source. Built with ❤️ for the Dolomite community.
