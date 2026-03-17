# Dolomite Dashboard — Project State

> **Ostatnia aktualizacja:** 2026-03-15
> **Live:** https://twojekrypto.github.io/dolomite-dashboard/
> **Repo:** GitHub Pages (branch `master`)

---

## 🎯 Cel

Institutional-grade analytics dashboard dla **Dolomite Protocol** na Berachain.
Prezentuje metryki tokenów DOLO, oDOLO, veDOLO — flow analysis, holder tracking, early exits, liquidation risk.

## 🏗️ Architektura

```
Dolomite website/
├── index.html              ← Main dashboard (~1MB, all-in-one SPA)
├── liquidations.html       ← Liquidation risk dashboard
├── excluded_addresses.html ← Protocol addresses reference
│
├── *.py                    ← Data pipeline scripts (Python)
├── *.json                  ← Generated data files (fetched by HTML)
│
├── .github/workflows/      ← CI/CD (7 GH Actions pipelines)
├── icons/                  ← Token SVG icons
├── data/                   ← Static reference data
│
├── PROJECT_STATE.md        ← Ten plik
└── lessons.md              ← Wzorce błędów i reguły (CZYTAJ NA STARCIE!)
```

## 📊 Kluczowe Strony

| Strona | Plik | Co robi |
|---|---|---|
| **Main Dashboard** | `index.html` | veDOLO holders, DOLO/oDOLO/veDOLO flows, supply metrics, early exits, exercisers |
| **Liquidation Risk** | `liquidations.html` | Pozycje zagrożone likwidacją, health factor gauge, E-Mode detection |
| **Protocol Addresses** | `excluded_addresses.html` | Adresy kontraktowe Dolomite z opisami |

## 🐍 Data Pipeline (Python Scripts)

| Skrypt | Dane | Źródło |
|---|---|---|
| `update_data.py` | veDOLO holders, stats, expiry, DOLO price | Subgraph + RPC |
| `generate_dolo_holders.py` | DOLO holder list | Subgraph |
| `generate_dolo_flows.py` | DOLO transfer flows | Subgraph |
| `generate_vedolo_flows.py` | veDOLO lock/unlock flows | Subgraph |
| `generate_odolo_flows.py` | oDOLO flows | Subgraph |
| `generate_exercisers.py` | oDOLO→DOLO exercisers | Subgraph |
| `fetch_early_exits.py` | Early exit transactions | Subgraph + RPC (z cache!) |
| `fetch_liquidation_risk.py` | Pozycje at-risk | Subgraph + RPC (E-Mode) |
| `fetch_defillama.py` | TVL i yield data | DefiLlama API |
| `scan_earn_netflow.py` | Earn vault net flows | Subgraph |
| `fetch_odolo_contract.py` | oDOLO contract metrics | RPC |
| `calculate_avg_lock.py` | Średni czas locka veDOLO | Obliczenia lokalne |

## 🚀 Deployment

- **Hosting:** GitHub Pages z branch `master`
- **CI/CD:** 7 GitHub Actions workflows (cron-based, co 6-24h)
- **WAŻNE:** Push na **oba** branche: `git push origin main && git push origin main:master`
- **Cache:** `fetch_early_exits.py` używa `early_exits_cache.json` (GH Actions cache)

## ⚠️ Kluczowe Reguły

1. **Zawsze czytaj `lessons.md` na starcie sesji** — akumulacja bugów i fixów
2. **CSS changes → verify via `getComputedStyle()` w browser console** — nie ufaj inline styles
3. **Po zmianie kolumn tabeli → audyt WSZYSTKICH `nth-child` selektorów**
4. **E-Mode:** Używaj `user.id` (nie `effectiveUser.id`) do `getAccountRiskOverride()`
5. **GH Pages cache:** Dodaj `?v=timestamp` do URL przy weryfikacji deploymentu
6. **Local testing:** `python3 -m http.server` (bo `file://` blokuje `fetch()`)
