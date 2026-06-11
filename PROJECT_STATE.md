# Dolomite Dashboard — Project State

> **Ostatnia aktualizacja:** 2026-06-12 (po audycie — patrz `CODE_AUDIT_2026-06-11.md`)
> **Live:** https://twojekrypto.github.io/Dolomite-dashboard/
> **Repo:** `Twojekrypto/Dolomite-dashboard` GitHub Pages (branch `master`)

---

## 🎯 Cel

Institutional-grade analytics dashboard dla **Dolomite Protocol** na Berachain.
Prezentuje metryki tokenów DOLO, oDOLO, veDOLO — flow analysis, holder tracking, early exits, liquidation risk.

## 🏗️ Architektura

```
Dolomite website/
├── index.html              ← Loader (route-loader.js → dolo-preview.html)
├── <route>/index.html      ← Loadery sekcji: statyczny head (meta/OG) +
│                              loadDoloRoute(config); wspólna logika w route-loader.js
├── *-preview.html          ← Właściwe strony sekcji (design tokens w tokens.css)
├── dashboard-core.html     ← Markup Earn (0.3MB) + dashboard-core.css/.js
├── route-loader.js         ← Wspólny loader tras (jedyne źródło mobile-nav assets)
├── tokens.css              ← 25 wspólnych zmiennych Graphite+Gold
├── rpc_client.py           ← Wspólny klient RPC (endpointy, rotacja, retry)
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
- **WAŻNE:** Nowe zmiany dashboardu pushujemy do remote `dolomite-dashboard` na branch `master`
- **Cache:** `fetch_early_exits.py` używa `early_exits_cache.json` (GH Actions cache)

## ⚠️ Kluczowe Reguły

1. **Zawsze czytaj `lessons.md` na starcie sesji** — akumulacja bugów i fixów
1a. **Ciężkie dane ładowane leniwie:** `dolo_holder_wallet_history.json`, `liquidation_history.json`, sharding `rowParts` w supply-activity — patrz sekcja "Audyt 2026-06-11/12" w `lessons.md`
2. **CSS changes → verify via `getComputedStyle()` w browser console** — nie ufaj inline styles
3. **Po zmianie kolumn tabeli → audyt WSZYSTKICH `nth-child` selektorów**
4. **E-Mode:** Używaj `user.id` (nie `effectiveUser.id`) do `getAccountRiskOverride()`
5. **GH Pages cache:** Dodaj `?v=timestamp` do URL przy weryfikacji deploymentu
6. **Local testing:** `python3 -m http.server` (bo `file://` blokuje `fetch()`)
