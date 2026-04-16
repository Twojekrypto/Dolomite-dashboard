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
