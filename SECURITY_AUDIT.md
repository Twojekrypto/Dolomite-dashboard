# Audyt bezpieczeństwa — Dolomite Dashboard

Data: 2026-06-10 · Zakres: całe repozytorium `twojekrypto.github.io/Dolomite-dashboard` (wszystkie zakładki, skrypty, dane, workflowy). Wyłączono `node_modules/` i `_old/`.

## Podsumowanie

Znaleziono **jeden krytyczny wyciek**: w publikowanym pliku danych jest zapisany pełny adres RPC Alchemy razem z kluczem API. Reszta repozytorium jest czysta — sekrety w GitHub Actions są używane poprawnie, kod kliencki korzysta wyłącznie z publicznych, bezkluczowych endpointów, nie ma żadnych haseł, tokenów GitHub/AWS/OpenAI, e-maili ani ścieżek osobistych.

| Severity | Problem | Status |
|---|---|---|
| 🔴 KRYTYCZNY | Klucz API Alchemy w `odolo_contract_data.json` (publiczny + w historii git) | Wymaga działania |
| 🟢 OK | Klucze RPC w workflowach (`${{ secrets.* }}`) | Poprawnie |
| 🟢 OK | RPC po stronie przeglądarki (`history.js`, `dashboard-core.html`) | Tylko publiczne, bez kluczy |
| 🟢 OK | Brak `.env`, tokenów, haseł, danych osobowych | Czysto |

---

## 🔴 KRYTYCZNY — wyciek klucza API Alchemy

**Gdzie:** `odolo_contract_data.json`, linia 9
```
"rpc_source": "https://berachain-mainnet.g.alchemy.com/v2/<REDACTED_ALCHEMY_KEY>"
```

**Dlaczego to się dzieje:** `fetch_odolo_contract.py` (linia 124) zapisuje do wyniku pełny URL RPC:
```python
"rpc_source": url,   # url zawiera klucz API
```
Plik `odolo_contract_data.json` jest śledzony przez git i automatycznie commitowany przez workflow `update-odolo-data.yml` przy każdej aktualizacji, a GitHub Pages serwuje go publicznie pod:
```
https://twojekrypto.github.io/Dolomite-dashboard/odolo_contract_data.json
```

**Zasięg:** Klucz jest w repo od **52 commitów** (zawsze ta sama wartość) — czyli na stałe w historii git. Każdy może go odczytać i używać Twojego limitu zapytań Alchemy (kradzież quoty, potencjalne koszty, rate-limit blokujący Twój dashboard).

**Naprawa (kolejność ważna):**

1. **Zrotuj klucz TERAZ** — w panelu Alchemy usuń/przegeneruj ujawniony klucz. To jedyne realne zabezpieczenie, bo klucz jest już w publicznej historii git. (To akcja na Twoim koncie — zrób ją samodzielnie.)
2. **Napraw skrypt**, żeby nie zapisywał klucza (poprawkę nanoszę poniżej — zamiast URL zapisuje samą nazwę dostawcy).
3. **Wyczyść aktualny plik** z klucza i zacommituj.
4. (Opcjonalnie) wyczyść historię git narzędziem `git filter-repo` lub BFG — ale po rotacji klucz i tak jest martwy, więc to kosmetyka.

---

## 🟢 Co jest OK

**Workflowy GitHub Actions** — wszystkie klucze RPC wstrzykiwane przez `${{ secrets.ALCHEMY_BERACHAIN_RPC }}` itd. To poprawny wzorzec: sekrety nie są widoczne w repo ani w logach.

**Kod kliencki** (`history/history.js`, `dashboard-core.html`, `portfolio-preview.html`) — używa tylko publicznych endpointów bez kluczy: `eth.llamarpc.com`, `*.publicnode.com`, `*.drpc.org`, `rpc.ankr.com/botanix_mainnet`. To bezpieczne do umieszczenia w przeglądarce.

**Brak innych sekretów** — przeskanowano pod kątem tokenów GitHub (`ghp_`/`gho_`), kluczy OpenAI (`sk-`), AWS (`AKIA`), Slack (`xox`), kluczy prywatnych (`BEGIN ... PRIVATE KEY`), haseł, e-maili i ścieżek typu `/Users/...`. Nic nie znaleziono. Brak śledzonych plików `.env`.

**Uwaga drobna:** adresy portfeli i etykiety (np. „Binance Hot Wallet", „Core Team") w plikach holderów to dane publiczne on-chain — to nie jest wyciek, tylko jawne dane blockchaina.

---

## Rekomendacja na przyszłość

Dodaj prosty test/strażnika w CI, który odrzuci commit, jeśli w publikowanych plikach pojawi się wzorzec klucza (np. `g.alchemy.com/v2/`, `dkey=`, `quiknode.pro/`). Dzięki temu taki wyciek już nigdy nie trafi na produkcję.
