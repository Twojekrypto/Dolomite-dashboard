# Audyt kodu — Dolomite Dashboard

> **Status napraw (2026-06-11):** Wykonano: P0.3 (testy w gicie), P1.5 (timeout, gołe except + logowanie except-pass), P1.4/P1.6 częściowo (requirements.txt, concurrency+timeout w workflowach, usunięty Date.now() cache-busting w wszystkich 10 miejscach, no-store→no-cache), P2.7 częściowo (1 GB tmp śmieci z .git usunięty; pełny `git gc` do wykonania ręcznie), P2.8 (martwe pliki usunięte, tytuł liquidation/ naprawiony, PROJECT_STATE zaktualizowany), P0.2 częściowo (indeksy odsetkowe w fetch_liquidation_risk.py serializowane dokładnie przez Decimal; frontendowe parseFloat to ścieżki wyłącznie wyświetlania — pozostawione świadomie). Wszystkie zmiany zweryfikowane: 205 testów OK, earn-audit OK, secret-guard OK, smoke HTTP OK.
>
> **Runda 2 (2026-06-11, po południu):** Wykonano też refaktory: (1) `dolo_flows.json` odchudzony 28,3→2,9 MB — `holder_wallet_history` (25 MB) w osobnym, leniwie ładowanym `dolo_holder_wallet_history.json` (pipeline + frontend + workflow + walidacja); (2) wspólny `route-loader.js` zastąpił 12 zduplikowanych loaderów (ujednolicone wersje mobile-nav); (3) wspólny `tokens.css` z 25 zmiennymi identycznymi na 7 stronach — zweryfikowano programowo, że efektywny zestaw zmiennych per strona jest bajt w bajt zgodny z oryginałem; (4) `rpc_client.py` — wspólny klient RPC (rotacja+retry+backoff+timeout) z 11 testami; pilotażowo zmigrowany `fetch_odolo_contract.py`, pozostałe skrypty migrować pojedynczo. Weryfikacja rundy 2: 216 testów OK, earn-audit OK, smoke HTTP OK. Po deployu wymagana wizualna kontrola tras w przeglądarce.
>
> **Runda 3 (2026-06-11, wieczór):** (1) `liquidation_risk.json` 17,9→8,4 MB — `liquidationHistory` (5,0 MB) w osobnym, leniwie ładowanym `liquidation_history.json` (używała go tylko strona Liquidations; Portfolio/Earn/liq-monitor pobierały na darmo) + kompaktowa serializacja zamiast `indent=2`; (2) 13 testów jednostkowych dla `fetch_liquidation_risk.py` (matematyka HF, E-Mode, premie, klasyfikacja ryzyka, statystyki historii, serializacja Decimal); (3) audytowa pozycja "fetch bez .catch()" zweryfikowana jako fałszywie pozytywna — assets/tvl/odolo mają poprawne try/catch; znana słabość: oDOLO `Promise.all` na 5 plikach jest all-or-nothing (poprawa wymaga sesji z przeglądarką). Weryfikacja: 229 testów OK, earn-audit OK, smoke OK.
>
> **Runda 4 (2026-06-11, noc):** (1) Sharding pełnych plików supply-activity — 3 pliki >40 MB (WBERA 71,5 MB!) podzielone na części ≤33 MB z indeksem `rowParts` (generator + konsument + migracja; ryzyko limitu 100 MB GitHuba zażegnane); (2) etykiety adresów oDOLO przepięte na współdzielony `dolo-address-labels.js` z mapowaniem typów i fallbackiem inline (96 etykiet zamiast 31, zero dryfu); 2 brakujące adresy (oDOLO Token, DOLO) dodane do źródła prawdy; (3) `vedolo_flows.json` przeanalizowany i świadomie NIE cięty — pełna lista locków zasila rekonstrukcję wykresu locked-over-time, a pola beneficiary/protocol zasilają chipy źródła (lessons.md); po gzipie ~2 MB transferu. Weryfikacja: 229 testów OK, earn-audit OK, smoke OK (indeks+części odpowiadają 341 977 wierszom oryginału).
>
> **Runda 5 (2026-06-12):** (1) 7 testów dla `build_ownership` w `update_data.py` (mint/burn/transfer, sortowanie po bloku+indeksie, multi-token holders) — rdzeń pipeline'u veDOLO ma pierwsze pokrycie; (2) `fetch_early_exits.py` zmigrowany na `rpc_client` (wspólne endpointy + rotacja/retry dla pojedynczych wywołań, batch-receipty bez zmian), `update_data.py` pobiera listę endpointów ze wspólnego źródła; (3) minifikacja w pages.yml świadomie ODRZUCONA — Pages gzipuje automatycznie, główny ciężar (JSON) już ścięty, a minifikacja 39-tysięcznego HTML z inline JS to ryzyko bez proporcjonalnego zysku. Pozostałe otwarte pozycje: podział dashboard-core.html, migracja pozostałych ~13 skryptów RPC, SEO/onclick. Weryfikacja: 236 testów OK, earn-audit OK.
>
> **Runda 6 (2026-06-12):** (1) `rpc_client.CHAIN_ENV_KEYS/PUBLIC_ENDPOINTS` rozszerzone o wszystkie łańcuchy i klucze env (berachain _3, ethereum, arbitrum, mantle, xlayer, botanix); listy endpointów w `generate_vedolo_flows`, `generate_odolo_flows`, `generate_dolo_holders` (eth+bera+helpery weryfikacyjne) przepięte na wspólne źródło — logika wywołań celowo nietknięta; (2) meta description + OG/Twitter tagi we wszystkich 11 loaderach sekcji (lepsze podglądy social/SEO bez zmiany renderowania). Weryfikacja: 236 testów OK, earn-audit OK, smoke OK. Do migracji RPC zostały skrypty wielołańcuchowe (reward_claim_events, scan_earn_netflow, supply_*, earn_*) — każdy wymaga osobnej uważnej sesji.
>
> **Runda 7 (2026-06-12, Chrome QA + podział monolitu):** (1) Wizualny QA wszystkich 11 tras na produkcji — zero błędów konsoli; przy okazji znaleziony i naprawiony pre-existing wyścig `renderProto`/`setNow` ze współdzieloną stopką (null-guardy w tvl/dolo/assets/odolo). (2) **`dashboard-core.html` podzielony: 1,88 MB → 0,29 MB HTML + `dashboard-core.css` (398 KB) + `dashboard-core.js` (1,2 MB)** — zewnętrzne pliki są cache'owalne po `?v=`, więc powracający użytkownik Earn pobiera tylko ~0,3 MB HTML z rewalidacją zamiast 1,9 MB. Testy kontraktowe czytają teraz źródło łączone (html+css+js). Zweryfikowane na produkcji w Chrome: render, kolory tokenów, wszystkie skrypty załadowane, walidacja wyszukiwarki działa. Znana ciekawostka: rozszerzenia automatyzujące mogą widzieć kartę jako "loading" po `document.write` z zewnętrznym skryptem — `readyState`/load event są complete, to artefakt narzędzia, nie strony. Dalszy podział JS na moduły per sekcja = przyszła opcja, już znacznie łatwiejsza na wydzielonym pliku.
>
> **Runda 8 (2026-06-12, pełny re-audyt):** Bezpieczeństwo: CZYSTO (szczegóły w nagłówku SECURITY_AUDIT.md). Regresje: zero łamiących; znalezione i naprawione w tej rundzie: (1) `run_earn_audit_checks.py` lintuje teraz `dashboard-core.js` i `route-loader.js` (luka pokrycia po splicie); (2) walidator w `update-supply-history.yml` waliduje pliki części `rowParts` (istnienie, marker, sort, zgodność sumy z `events`) zamiast pустych inline rows; (3) `earn-audit-checks.yml` obserwuje też `route-loader.js` i `tokens.css`; (4) `.agent/workflows/ui-check.md` i tabela stron w `PROJECT_STATE.md` odświeżone z usuniętych plików na obecne trasy; (5) docstring `rpc_client.py` odzwierciedla 6 konsumentów. Zweryfikowano przekrojowo: 12/12 loaderów, 2/2 splity JSON (generator+konsument+walidator+workflow), 7/7 stron tokens.css, 6/6 konsumentów rpc_client, 16/16 workflowów z requirements.txt. Produkcja: wszystkie trasy i nowe pliki 200, dane świeże. 236 testów OK.

Data: 2026-06-11 · Zakres: frontend (HTML/JS/CSS), pipeline Python, workflowy CI, higiena repo. Uzupełnia `SECURITY_AUDIT.md` z 2026-06-10 (bezpieczeństwo nie jest tu powtarzane — wyciek klucza Alchemy jest naprawiony w plikach; **rotacja klucza w panelu Alchemy nadal wymagana**, bo klucz został w historii git).

## Podsumowanie

Projekt działa i ma mocne strony (testy kontraktowe, secret-guard w CI, staggered crony, dobra dokumentacja w `lessons.md`). Główne problemy to: **wielomegabajtowe JSON-y ładowane w przeglądarce**, **utrata precyzji na kwotach wei (float/parseFloat)**, **masywna duplikacja kodu** (12 loaderów, 16 kopii helpera RPC, 7 kopii design tokens) oraz **chore lokalne repo git (9.7 GB śmieci)**.

| Priorytet | Problem | Skala |
|---|---|---|
| 🔴 P0 | Strona główna pobiera ~43 MB JSON przy każdym wejściu | wydajność |
| 🔴 P0 | `parseFloat`/float na wei i Par — utrata precyzji (też w matematyce likwidacji) | poprawność |
| 🔴 P0 | 4 pliki testów nieśledzone przez git (łapie je wzorzec `test_*.py` w `.gitignore`) | CI nie widzi testów |
| 🟠 P1 | Cache-busting `Date.now()` wyłącza cache całkowicie (27 MB i 1.9 MB pobierane za każdym razem) | wydajność |
| 🟠 P1 | 16 skryptów Python z własną kopią helpera RPC; brak `requirements.txt` | utrzymanie |
| 🟠 P1 | Workflowy commitujące do `master` bez wspólnej grupy concurrency + `-X theirs` może gubić dane | CI race |
| 🟡 P2 | Lokalne `.git` = 9.7 GB (194 śmieciowe tmp packi); pliki śledzone do 68 MB (limit ostrzeżeń GitHub: 50 MB) | repo |
| 🟡 P2 | Martwe pliki i duplikaty loaderów; brak testów dla `fetch_liquidation_risk.py` i `update_data.py` | porządek |

---

## 🔴 P0 — Do naprawy w pierwszej kolejności

### 1. Wielomegabajtowe JSON-y w przeglądarce
- `dolo-preview.html` (strona główna): `bootLiveDoloPreview()` (l. 4866) ładuje `dolo_flows.json` (27 MB) + `vedolo_flows.json` (10 MB) + `vedolo_holders.json` (5.7 MB) + inne ≈ **~43 MB na wejście**.
- `portfolio-preview.html` ≈ ~33 MB (w tym `liquidation_risk.json` 17 MB).
- Do tego `fetchJson` (l. 4886) dokleja `?Date.now()` + `cache:"no-cache"` — **te megabajty pobierają się od nowa przy każdej nawigacji**.

**Fix:** generować po stronie pipeline'u małe pliki podsumowań do pierwszego renderu (wzorzec shardingu już istnieje: `data/supply-history/manifest.json`); duże pliki ładować leniwie/shardowane; zamienić `Date.now()` na istniejący `?v=` (wersja treści); parsowanie dużych JSON przenieść do Web Workera.

### 2. Precyzja kwot — float na wei/Par
Frontend (`parseFloat` na polach `*Wei`/`*Par` — wprost łamie regułę z `AGENTS.md:44`):
- `assets-preview.html:1877` (`supplyMaxWei`, `borrowMaxWei`), `:1888` (`supplyPar`)
- `dashboard-core.html:23623-24, 23644`
- `portfolio-preview.html:2356, 2361, 2400`

Python (`int(...,16)/1e18` → float64, traci precyzję >15-16 cyfr):
- **`fetch_liquidation_risk.py:527, 550, 685-686`** — matematyka likwidacji, najwyższa stawka; `str(borrow_raw/10**18)` serializuje już ucięty float
- `update_data.py:250, 301, 483, 675`, `fetch_early_exits.py:124, 163`, `generate_dolo_holders.py:269`, `generate_vedolo_flows.py:628, 649`, `audit_earn_asset.py:1004`

**Fix:** JS → `BigInt`; Python → liczyć w int wei, dzielić dopiero przy serializacji przez `Decimal`. Zacząć od `fetch_liquidation_risk.py`.

### 3. Testy niewidoczne dla CI
`.gitignore` zawiera wzorzec `test_*.py` z ręczną listą wyjątków — przez to **4 testy istnieją tylko lokalnie**:
`tests/test_apply_earn_subaccount_history_delta.py`, `test_materialize_earn_subaccount_history.py`, `test_plan_earn_subaccount_history_repairs.py`, `test_run_earn_data_correctness_pipeline.py`.

**Fix:** zmienić wzorzec na `/test_*.py` (tylko root) albo dodać `!tests/test_*.py`, potem `git add tests/`.

---

## 🟠 P1 — Ważne

### 4. Cache-busting i loader `document.write`
- `earn/index.html` ładuje `dashboard-core.html` (**1.9 MB**, 39 159 linii) z `cache:"no-store"` + `Date.now()` — pełne 1.9 MB przy każdej wizycie Earn.
- 12 loaderów (`index.html`, `dolo/`, `vedolo/`, …) to niemal identyczne kopie tego samego IIFE `fetch + document.write`; string `mobileNavAssets` zduplikowany bajt w bajt we wszystkich 12, już w **dwóch rozjechanych wersjach** (`mobile-nav-20260602-revenue` vs `-history-tabs`).
- `liquidation/index.html` ma tytuł `Dolomite · Borrow` (błąd copy-paste); `borrow/`, `liquidation/` i `supply/` pobierają ten sam 921 KB `liquidation-preview.html`.
- Rosnące stringi `previewVersion` (~700 znaków changelogu) → zastąpić krótkim hashem.
- `document.write` po async fetch = pusta strona dla crawlerów/botów social (SEO). Dobry wzorzec już istnieje: `history/index.html` (prawdziwy markup, aria, defer).

**Fix:** jeden parametryzowany `loader.js` + config tras; docelowo migracja na wzorzec z `history/`.

### 5. Pipeline Python — duplikacja i odporność
- Helper „rotuj RPC + retry" skopiowany w **16 skryptach** (3 różne sygnatury `rpc_call`); listy URL-i RPC wklejone w ~7 miejscach. **Fix:** jeden `rpc_client.py` + wspólna lista endpointów (świadome odstępstwo od reguły „bez nowych abstrakcji" — zgłoszone tutaj).
- **Brak `requirements.txt`** — 24 workflowy robią `pip install` bez pinowania → niereprodukowalne buildy. Fix: pinowany `requirements.txt`.
- `calculate_avg_lock.py:67` — `requests.get` **bez timeoutu** (produkcyjny, może wisieć do kill-a). Jednolinijkowy fix.
- Gołe `except:` w kodzie RPC: `scan_earn_netflow.py:262, 272` (łamie `AGENTS.md:45`; ciche dziury w danych netflow). Plus ~20 `except Exception: pass` w ścieżkach danych (`generate_odolo_flows.py:110,541,693`, `generate_dolo_flows.py:214,1784,1794,2175,2192`, …) — co najmniej logować.

### 6. Workflowy CI — race przy commitach
Wszystkie workflowy commitują do `master`, ale każdy ma **inną grupę concurrency** (15 nazw) → brak serializacji; konflikty rozwiązuje `git pull --rebase -X theirs`, które przy realnym konflikcie treści **może wyrzucić świeże dane drugiego joba**. 5 commitujących workflowów nie ma bloku concurrency wcale (`update-data`, `update-dolo-flows`, `update-liquidation-risk`, `update-earn-snapshots`, `update-assets-live`).

**Fix:** wspólne `concurrency: { group: data-commit, cancel-in-progress: false }` we wszystkich commitujących workflowach. Dodać `timeout-minutes` do `earn-audit-checks.yml`.

---

## 🟡 P2 — Porządki

### 7. Higiena repo
- **Lokalne `.git` = 9.7 GB**, w tym 194 porzucone `tmp_pack_*`/`tmp_idx_*` (przerwane gc/fetch). Fix: `rm .git/objects/pack/tmp_*` + `git gc --aggressive --prune=now` (najpierw backup).
- Śledzone pliki do **68 MB** (`data/supply-activity/berachain/0x6969….json`; GitHub ostrzega >50 MB, twardy limit 100 MB) — te pliki rosną; rozważyć sharding zanim uderzą w limit.
- 3 stashe (w tym `codex-autostash-before-risk-push`) — przejrzeć/wyczyścić.

### 8. Martwy kod
- `liquidations.html` (238 KB) i `excluded_addresses.html` (30 KB) — przez nic niereferencjonowane. Do usunięcia.
- `_old/`, `fetch_transfers.py`, `generate_early_investors_local.py`, `verify_palace_rules.py` — martwe.
- ~12 ręcznych skryptów ops (`audit_*`, `plan_*`, `explain_*`, `rerun_*`, `report_*`) zaśmieca root → przenieść do `scripts/ops/`.
- `PROJECT_STATE.md` nieaktualny (opisuje `index.html` jako 1 MB SPA; rzeczywistość to loadery + preview pages + `dashboard-core.html`). Zaktualizować.

### 9. Frontend — jakość
- Design tokens (`:root`, Graphite+Gold) wklejone w **7 preview pages** → jeden `tokens.css`.
- Mapy etykiet adresów rozsiane po stronach zamiast w `dolo-address-labels.js` (ładowany tylko przez 3 z nich) — `lessons.md` już opisuje, że to powodowało dryf etykiet.
- `fetch` bez `.catch()`: `assets-preview.html` (3 fetche, 0 catch), `tvl-preview`, `odolo-preview` → wspólny `safeFetch`.
- 235 inline `onclick` w `dashboard-core.html` (+65 w liquidation-preview) → delegacja zdarzeń.
- Brak minifikacji w `pages.yml` (rsync surowych plików) → dodać krok minify.
- Przejrzeć a11y w `tvl-preview` (3 atrybuty aria) i `assets-preview` (14) — odstają od reszty (38-67).

### 10. Testy — luki
Bez żadnego testu, mimo że krytyczne: **`fetch_liquidation_risk.py`** (57 KB, matematyka likwidacji) i **`update_data.py`** (39 KB, rdzeń danych). Dalej: `generate_dolo_holders`, `generate_odolo_flows`, `generate_supply_*`, `fetch_*`. Priorytet: dwa pierwsze.

---

## Kolejność działań (quick wins → duże)

1. `.gitignore` fix + `git add tests/` (5 min, odzyskuje 4 testy dla CI)
2. `calculate_avg_lock.py` timeout + gołe `except` w `scan_earn_netflow.py` (15 min)
3. Pinowany `requirements.txt` + wspólna grupa concurrency w workflowach (1 h)
4. Usunięcie `Date.now()` cache-bustingu (duży zysk wydajności małym kosztem)
5. Czyszczenie `.git` (9.7 GB → prawdopodobnie <1 GB)
6. Usunięcie martwych plików, fix tytułu `liquidation/index.html`
7. BigInt/Decimal na wei — zacząć od `fetch_liquidation_risk.py` + testy do niego
8. Wspólny `loader.js`, `tokens.css`, `rpc_client.py`
9. Pre-agregacja/sharding dużych JSON-ów (największy projekt, największy zysk UX)
