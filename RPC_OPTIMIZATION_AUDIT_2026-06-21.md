# RPC Optimization Audit — Dolomite Dashboard

> **Data:** 2026-06-21
> **Zakres:** zużycie JSON-RPC (Alchemy) w pipeline danych + ocena, czy darmowy tier Alchemy wystarcza.
> **Metoda:** analiza statyczna wszystkich skryptów `*.py` i workflowów `.github/workflows/`, weryfikacja kosztów CU u źródła (Alchemy docs), kontrola krzyżowa z `lessons.md`. Twierdzenia nośne zweryfikowane adwersaryjnie z cytatami `plik:linia`.
> **Pewność:** wysoka co do architektury i kierunku; szacunki wolumenu to rząd wielkości (patrz „Czego nie jestem w 100% pewien”).

---

## TL;DR — trzy odpowiedzi

1. **Czy workflow da się zoptymalizować pod kątem liczby zapytań RPC bez utraty poprawności?**
   **Tak.** Pipeline jest dojrzały (inkrementalne kursory, retry/backoff, cache receipts), ale jest kilka realnych, bezpiecznych oszczędności — głównie *deduplikacja nakładających się skanów* w EARN, *globalny dławik chunk=50 na Ethereum*, *odświeżanie tylko zmienionych adresów* i *cache niezmiennych danych*. Szczegóły niżej z „strażnikiem poprawności" przy każdej pozycji.

2. **Czy darmowy RPC Alchemy wystarczy na dashboard?**
   - **Frontend (sama strona): tak — i właściwie nie potrzebuje Alchemy w ogóle.** Dashboard czyta gotowe `*.json` z GitHub Pages, a nieliczne żywe odczyty (zakładka EARN, portfolio) idą na **publiczne** RPC (drpc/publicnode), nie na Alchemy. Obciążenie Alchemy z przeglądarki ≈ 0.
   - **Backend (pipeline GitHub Actions): w stanie ustalonym tak, ale na granicy bezpieczeństwa.** Szac. ~14–15 mln CU/mies. ≈ **~50% darmowego limitu 30 mln CU/mies**, *gdyby cały ruch szedł na jedno konto Alchemy*. Realnie jest rozłożony na kilka kluczy Alchemy + QuickNode + dRPC + publiczne, więc per-konto jest jeszcze niżej. **Wąskim gardłem nie jest miesięczny budżet CU, tylko przepustowość 500 CUPS (~8 `eth_getLogs`/s) podczas równoległych skanów i zimnych startów.**
   - **Najcięższy konsument — `fetch_liquidation_risk.py` (co godzinę) — celowo NIE używa Alchemy** (publiczne RPC + Multicall3), więc nie obciąża tieru.

3. **Czy implementacja jest w 100% pewna? Gdzie są luki?**
   Implementacja jest solidna, ale ma **policzalne luki** (poniżej, P0–P2). Nie jestem w 100% pewien dwóch rzeczy, których nie da się ustalić z kodu: (a) czy klucze `ALCHEMY_*` to jedno konto czy kilka (to zmienia, czy limit 30 mln jest współdzielony), (b) realnego bieżącego zużycia CU — to widać **tylko w dashboardzie Alchemy → Usage**. Bez tego każda liczba „wystarczy/nie wystarczy" jest szacunkiem.

---

## 1. Mapa zużycia RPC

Tylko **Alchemy JSON-RPC** liczy się do darmowego tieru. Subgraph (GraphQL) i REST (DeFiLlama, Dolomite API, Routescan, Etherscan) to osobne usługi.

| Skrypt | Kadencja | Źródło danych | Metody RPC (Alchemy?) | Inkrementalny? |
|---|---|---|---|---|
| **EARN: canonical history** (`build_/run_earn_*`, `scan_earn_subaccount_history_events`) | **co 30 min × ~7 łańcuchów** | **Alchemy** (+QuickNode/dRPC/publiczne) | `eth_getLogs`, `eth_blockNumber` | tak (checkpoint w GH cache) |
| **EARN: netflow** (`scan_earn_netflow.py`) | bera co 30 min, reszta co godz. | **Alchemy** (+fallbacky) | `eth_getLogs`, `eth_blockNumber` | tak (`data/.netflow-progress/{chain}.json`) |
| `generate_dolo_flows.py` | co 6 h | **Alchemy** (eth, bera) | `eth_getLogs`, `eth_call`(balanceOf), `eth_getCode` | tak (`dolo_flows_state.json`) |
| `generate_dolo_holders.py` | co 6 h | **Alchemy** (eth, bera) | `eth_getLogs`, `eth_call`, `eth_getCode`, `eth_getStorageAt` | tak |
| `generate_odolo_flows.py` | co 6 h | **Alchemy** (bera) | `eth_getLogs`, `eth_call` | tak (`odolo_flows_state.json`) |
| `generate_vedolo_flows.py` | co 6 h | **Alchemy** (bera) | `eth_getLogs` ×3 topiki, `eth_getTransactionReceipt`/lock | tak (`vedolo_flows_state.json`) |
| `generate_reward_claim_events.py` | co 6 h | Alchemy + subgraph (4 łańcuchy) | `eth_getLogs`, `eth_call`, `eth_getBlockByNumber` | tak (`reward_claim_events_state.json`) |
| `fetch_early_exits.py` | co 6 h | **Alchemy** (bera) | `eth_getLogs`, `eth_getTransactionReceipt` (batch 50) | tak (`early_exits_cache.json`) |
| `update_data.py` (veDOLO) | co 6 h | Etherscan REST + **Alchemy** `eth_call` | `eth_call` (locked/balanceOfNFT) | tak (limity stale-refresh) |
| `fetch_odolo_contract.py` | co 6 h | **Alchemy** `eth_call` + Routescan REST | 2 batche `eth_call` (7 wywołań) | n/d (tania) |
| `generate_exercisers.py` | co 6 h | **Routescan REST** (nie Alchemy) | `account.txlist` + receipty | **nie** (pełny rescan od bloku 0) |
| `fetch_liquidation_risk.py` | **co godzinę** | subgraph + **publiczne RPC** (web3.py, Multicall3) | `eth_call`/`aggregate3` (E-Mode, ceny, indeksy) | nie (pełny rescan historii) |
| `fetch_defillama / _tvl / _revenue` | godz./6 h | **REST/subgraph** (0 RPC) | — | n/d |
| `generate_supply_history / _activity` | co 6 h | **subgraph** (0 RPC) | — | tak (kursor subgraph) |

---

## 2. Koszty CU i darmowy limit (Alchemy, zweryfikowane u źródła)

**Darmowy tier (2026):** **30 mln CU / miesiąc**, **500 CUPS** (compute units/s) ≈ ~25 zapytań/s dla metod 20-CU, ~8 `eth_getLogs`/s. *(Uwaga: meta-opis strony Alchemy podaje „300M" — to nieaktualny tekst SEO; treść strony i niezależne źródło z czerwca 2026 zgodnie podają 30 mln.)*

| Metoda | CU |
|---|---|
| `eth_blockNumber` | 10 |
| `eth_getTransactionReceipt`, `eth_getBlockByNumber`, `eth_getBalance`, `eth_getCode`, `eth_getStorageAt` | 20 |
| `eth_call` | 26 |
| **`eth_getLogs`** | **60** |
| **batch (JSON-RPC array)** | **suma CU metod składowych** |

> ⚠️ **Najważniejszy wniosek o batchowaniu:** batch JSON-RPC **nie zmniejsza CU** — 50 `eth_call` w jednym POST kosztuje 50 × 26 = 1300 CU, tyle samo co osobno. Batch oszczędza tylko round-tripy HTTP i pomaga z limitem CUPS, **nie** z miesięcznym budżetem CU.
> **Co realnie zmniejsza CU:** (a) mniej/większe `eth_getLogs`, (b) inkrementalne kursory (brak ponownego skanu), (c) **Multicall3 `aggregate3`** — N odczytów kontraktu zwijają się do **jednego** `eth_call` (26 CU zamiast N×26), (d) cache danych niezmiennych (receipty, timestampy, rozwiązane tokeny).

### Szacunkowy budżet (gdyby CAŁY ruch szedł na jedno konto Alchemy)

- **EARN (canonical + netflow + borrow-route):** ~10–12 tys. zapytań/dobę, głównie `getLogs`(60) + `blockNumber`(10) → **~440 tys. CU/dobę ≈ ~13 mln CU/mies.**
- **Generatory flow/holders/reward/early-exits/update_data/odolo (co 6 h):** ~1 tys. zapytań/dobę, mix → **~1–1,5 mln CU/mies.**
- **Razem stan ustalony ≈ 14–15 mln CU/mies ≈ ~50% darmowego limitu.**

**Wniosek:** miesięczny limit CU **nie jest** wąskim gardłem w stanie ustalonym. Wąskie gardła to:
1. **500 CUPS** podczas równoległych skanów (canonical odpala do ~12 workerów `getLogs` naraz; nakładające się workflowy współdzielą limit konta) → ryzyko **429**.
2. **Zimny start / utrata cache** = miliony CU w jednym przebiegu + utrzymujące się 429. `lessons.md:97` opisuje „death-loop", gdy twardy timeout kasuje cache i sync startuje od zera.

---

## 3. Czy darmowy Alchemy wystarczy — werdykt warunkowy

| Scenariusz | Werdykt |
|---|---|
| **Sam dashboard (frontend)** | ✅ Wystarczy z zapasem — w praktyce Alchemy niepotrzebne (statyczne JSON + publiczne RPC w przeglądarce). |
| **Backend, stan ustalony, jedno konto Alchemy** | ⚠️ Wystarcza na CU (~50% z 30 mln), ale **napięcie na 500 CUPS** przy równoległych skanach. Bez fallbacku ryzykowne. |
| **Backend + obecny multi-provider (kilka kluczy Alchemy + QuickNode + dRPC + publiczne)** | ✅ Komfortowo — obecny projekt już rozkłada ryzyko; to jest właściwa architektura dla free tier. |
| **Zimny start / odbudowa cache na jednym koncie** | ❌ Ryzyko przekroczenia CUPS i serii 429 → wg `lessons.md` historycznie = ciche zepsucie danych. |

**Rekomendacja:** darmowy Alchemy jest **wystarczający dla stanu ustalonego**, jeśli (1) zachowane są kursory/cache, (2) współbieżność jest dławiona poniżej ~25 `getLogs`/s, (3) wdrożone są optymalizacje P0/P1 niżej. **Zostaw co najmniej jednego dostawcę zapasowego** (QuickNode/dRPC/publiczny) na zimne starty i 429 — tak jak jest teraz.

---

## 4. Optymalizacje bez utraty poprawności

Każda z „strażnikiem poprawności" — czego nie wolno przy tym złamać.

### P0 — największy zwrot, niskie ryzyko

1. **Podnieś chunk `eth_getLogs` dla Ethereum w EARN (globalny dławik = 50 bloków).**
   `scan_earn_netflow.py:53` ustawia ethereum `max_block_chunk=50` (przy domyślnym `BLOCK_CHUNK=49999`). Zweryfikowane: dławik działa na **wszystkie** endpointy, też Alchemy (rozmiar chunku liczony raz, rotacja endpointów go nie zmienia) — czyli ~6× więcej `getLogs` niż trzeba także na Alchemy. **Fix:** chunk per-endpoint (duży na Alchemy/QuickNode, mały tylko na słaby publiczny fallback).
   *Strażnik:* zachować adaptacyjne połowienie przy „range too large"/>5000 logów (`:1055,1067`); nie usuwać fallbacku na mniejszy chunk.

2. **Deduplikuj nakładające się skany head-window: netflow vs canonical-history.**
   Oba skanują ten sam kontrakt margin po tym samym oknie świeżych bloków co godzinę — netflow bez filtra (`ALL_EVENTS`), canonical z filtrem adresów. **Fix:** jeden konsumuje logi drugiego dla wspólnego okna (albo wspólny bufor logów per łańcuch/cykl).
   *Strażnik:* canonical wymaga granularności per-konto `(owner, account, market)`; nie wolno spłaszczyć do agregatu. Zachować typy zdarzeń łącznie z **waporyzacjami** (`lessons.md:137`).

3. **Rozwiąż head-block raz na cykl i przekaż `--to-block` do workerów.**
   Do ~12 workerów woła osobno `eth_blockNumber` (`scan_earn_subaccount_history_events.py`). Plan już wspiera `to_block`. **Fix:** jedno `eth_blockNumber`/cykl/łańcuch zamiast ~12. Oszczędność ~4 tys. `blockNumber`/dobę.
   *Strażnik:* wszystkie workery muszą użyć tego samego `to_block`, by nie powstały dziury.

### P1 — solidny zwrot, wymaga ostrożności

4. **balanceOf tylko dla zmienionych adresów** (`generate_dolo_flows.py`, `generate_dolo_holders.py`).
   Dziś co przebieg odpytywany jest cały top-N / top-200, choć większość sald się nie zmieniła. **Fix:** odświeżaj salda tylko adresów z niezerowym netflow w danym przebiegu; pełna weryfikacja na zimnym starcie.
   *Strażnik (krytyczny, `lessons.md:130–131`):* `balanceOf` musi zwrócić 32-bajtowy ABI-uint256; `0x0`/krótkie/brak = **nieudane wywołanie, nie zero**. Fallback do cache tylko gdy RPC *faktycznie* zawiódł — nigdy nie nadpisuj realnego zera (ukrywa wyprzedaże).

5. **Scal 3 `eth_getLogs` veDOLO w jedno (OR-topics).**
   `generate_vedolo_flows.py` robi osobne `getLogs` dla Deposit/Withdraw/Transfer per chunk (3×60 CU). **Fix:** `topics:[[deposit,withdraw,transfer]]` → 1 zapytanie (60 CU).
   *Strażnik:* zachować wykrywanie locków mint-from-0x0 i cache `odolo_receipt_checks`.

6. **Persist niezmiennych danych w `reward_claim_events_state.json`:** rozwiązane tokeny dystrybutorów i timestampy bloków (dziś re-rozwiązywane co przebieg), oraz skróć 730-dniowy lookback do małego bufora reorg po zaufaniu `lastBlock`.
   *Strażnik:* symbol tokena bierze się z dystrybutora, nie z łańcucha (`lessons.md:51`); zachować pokrycie obu wariantów method-ID.

### P2 — porządkowe / odporność

7. **`generate_exercisers.py`: dodaj kursor `last_block`/`last_tx`** — dziś pełny `txlist` od bloku 0 co przebieg (Routescan, nie Alchemy, ale marnotrawne).
8. **`generate_odolo_flows.py:397`: batch hardcoded 100 → honoruj `ODOLO_FLOW_RPC_BATCH_SIZE`; dodaj trwałe śledzenie pominiętych zakresów** (parytet z dolo_flows), by awaria chunku nie gubiła cicho zakresu bloków.
9. **`fetch_liquidation_risk.py`: cache override'ów E-Mode** — odświeżaj tylko nowe/zmienione konta + mała rotująca próbka „stale"; zamiast ~271 Multicall/godz. ~20. Plus inkrementalna historia likwidacji (`serialId_gt` zamiast `skip=0`). *(Publiczne RPC, nie Alchemy — ale odciąża i przyspiesza.)*
   *Strażnik (krytyczny):* E-Mode używa `user.id`, **nie** `effectiveUser.id` (`lessons.md:121`); nigdy `except: pass` na RPC (`:122`).

> **Świadomie zostaw `fetch_liquidation_risk.py` poza Alchemy.** Przeniesienie go na to samo darmowe konto Alchemy co EARN zjadłoby zapas 500 CUPS. Multicall3 trzyma jego koszt nisko na publicznych RPC — to dobry układ.

---

## 5. Strażnicy poprawności — czego NIE wolno złamać (z `lessons.md`)

- **Cicha awaria chunku → puste dane** (`:125`): jeśli świeży skan zwraca 0, a cache ma dane — użyj cache. Liczyć nieudane chunki, nie zerować.
- **Nigdy `except: pass` na RPC** (`:122`): pominięto 170+ pozycji E-Mode przez ciche łykanie 429.
- **`balanceOf 0x0` = błąd, nie zero** (`:130–131`).
- **E-Mode `user.id`, nie `effectiveUser.id`** (`:121`); subgraph nie eksponuje override'ów ryzyka (`:119`).
- **Konto 0 vs wszystkie konta** (`:132–134`): wybór metody liczenia netflow to przełącznik kalkulacji, nie filtr widoczności.
- **Neutralizacja przepływów cross-chain + mint/burn mostów** (`:128–129`).
- **Wiele method-ID dla tej samej akcji** (`:127`).
- **Generous `timeout-minutes` + checkpoint zamiast restartu** (`:97–98`) — inaczej death-loop kasuje cache.

---

## 6. Czego NIE jestem w 100% pewien (luki w wiedzy, nie w kodzie)

1. **Jedno konto Alchemy czy kilka?** Klucze `ALCHEMY_*_ZEN/_DANU/_TWOJE` sugerują kilka kont/dostawców. Jeśli to jedno konto — limit 30 mln CU jest **współdzielony** przez wszystkie aplikacje; jeśli kilka — każde ma własne 30 mln. **To zmienia werdykt.** Weryfikacja: Alchemy Dashboard → liczba aplikacji/kont.
2. **Realne bieżące zużycie CU/CUPS.** Moje ~14–15 mln CU/mies. to szacunek z kadencji i rozmiarów chunków. **Twarda liczba jest tylko w Alchemy → Usage** (CU/mies. i wykres CUPS z 429). To rozstrzyga pytanie „wystarczy?" w 5 minut.
3. **Tempo produkcji bloków per łańcuch** (wpływa na liczbę `getLogs`/przebieg) — użyłem rozsądnych wartości, nie pomiarów.
4. **Częstość zimnych startów** — jeśli cache jest stabilny, dominuje stan ustalony; `lessons.md` sugeruje, że death-loopy się zdarzały.

---

## 7. Plan weryfikacji (jak potwierdzić w praktyce)

1. **Alchemy Dashboard → Usage:** odczytaj CU/mies. i czy są skoki 429 na wykresie CUPS. To jedyna definitywna odpowiedź na „czy wystarczy".
2. Po każdej zmianie chunku/dedupu: `python3 validate_data.py <pliki>` + istniejące testy (`tests/test_generate_dolo_flows_rpc.py`, `test_scan_earn_netflow.py`, `test_rpc_client.py`).
3. Porównaj sumy/liczność rekordów przed/po (np. `total_transfers`, liczba holderów, pozycje E-Mode) — żadna optymalizacja nie może zmienić wyników, tylko liczbę zapytań.
4. Zmierz realny spadek: licznik zapytań RPC per przebieg w logach przed/po.

---

*Uwaga: to dokument analityczny. Nie zmieniałem kodu pipeline'u (zgodnie z AGENTS.md — zmiany wpływające na wdrożone dane wymagają zgody). Mogę wdrożyć dowolną pozycję P0–P2 osobno, z testem i porównaniem przed/po.*
