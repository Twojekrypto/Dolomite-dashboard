# EARN Verified Fast Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Przyspieszyc i ustabilizowac lookup EARN, rozdzielic swiezosc live od historycznego coverage oraz uruchomic rosnacy canonical backfill bez obnizania jakosci danych.

**Architecture:** Frontend dostaje izolowana polityke zdrowia RPC i laczy nowy wynik z ostatnim zaufanym cachem monotonicznie. Pipeline rozdziela czesty head refresh od osobnej kolejki coverage, a publiczne ledger shards moga przenosic tylko zaufany resolved interest ledger zgodny z blokiem porownawczym.

**Tech Stack:** Statyczny JavaScript, Python 3.11, JSON, GitHub Actions, `unittest`, lokalny serwer HTTP i testy przegladarkowe.

## Global Constraints

- Nie awansowac snapshot/netflow fallbacku do statusu `verified`.
- Nie uzywac `parseFloat` ani `Number` do arytmetyki wei.
- Head refresh nie moze czekac na globalny backfill.
- HTTP 401/403 i deterministyczny blad metadata nie moga uruchamiac kaskady retry.
- Rewardy nie moga blokowac pierwszego stabilnego renderu tabeli EARN.

---

### Task 1: Odporna polityka RPC

**Files:**
- Create: `earn/earn-rpc-policy.js`
- Modify: `dashboard-core.html`
- Modify: `dashboard-core.js`
- Modify: `build_earn_bundle.py`
- Test: `tests/test_earn_rpc_policy.js`
- Test: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Produces: `EarnRpcPolicy.create(endpoints)`, `next()`, `recordFailure(url, error)`, `shouldRetry(error)`, `reset()`.
- Consumes: listy `EARN_CHAINS[chain].rpcs`.

- [ ] Napisac testy, ze 403 wylacza endpoint, `metadata is not found` konczy request, a 429/timeout przechodza na kolejny endpoint.
- [ ] Uruchomic `node tests/test_earn_rpc_policy.js` i potwierdzic oczekiwane FAIL przed implementacja.
- [ ] Dodac minimalny modul UMD i podlaczyc go przed `dashboard-core.js` oraz `earn-core.js`.
- [ ] Zastapic slepa rotacje `earn_nextRpc()` polityka per lookup oraz usunac wielokrotne retry deterministic errors.
- [ ] Uruchomic test JS, `node --check dashboard-core.js` i kontrakty dashboardu.

### Task 2: Monotoniczny cache zweryfikowanego wyniku

**Files:**
- Modify: `dashboard-core.js`
- Test: `tests/test_history_tax_export_contracts.py`
- Test: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Produces: `earn_mergeTrustedLookupSnapshot(previous, candidate)` i ranking statusow weryfikacji.
- Consumes: `earn_lookupResultCache`, `earn_replayVerificationData`, `earn_resolvedTotalYieldData`, snapshot date i replay block.

- [ ] Dodac failing contract/runtime test dla `verified -> pending` w tej samej wersji danych.
- [ ] Potwierdzic FAIL przed implementacja.
- [ ] Zaimplementowac scalanie, ktore zachowuje poprzedni zaufany market podczas chwilowego bledu RPC.
- [ ] Zmienic zapis cache, aby kandydat byl scalany przed zapisem do memory i `sessionStorage`.
- [ ] Zweryfikowac, ze nowszy blok z rownym lub wyzszym zaufaniem nadal zastapi stary wynik.

### Task 3: Dwa niezalezne statusy swiezosci

**Files:**
- Modify: `dashboard-core.html`
- Modify: `dashboard-core.css`
- Modify: `dashboard-core.js`
- Test: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Produces: status `Live data · Xm ago` oraz `Historical verification · x/y wallets`.
- Consumes: `data/earn-freshness/status.json` pola `canonical.recencyStatus`, `estimatedLagMinutes` i `canonical.coverage`.

- [ ] Napisac failing test kontraktowy, ktory zabrania `Chain data syncing` przy swiezym headzie i niepelnym backfillu.
- [ ] Potwierdzic FAIL.
- [ ] Zmienic render statusu i ARIA label bez ukrywania coverage.
- [ ] Dodac responsywny separator/status copy zgodny z obecnym UX.
- [ ] Odbudowac bundle i uruchomic test kontraktowy.

### Task 4: Osobna kolejka canonical coverage

**Files:**
- Create: `.github/workflows/backfill-earn-canonical-coverage.yml`
- Modify: `select_earn_canonical_hot_addresses.py`
- Test: `tests/test_select_earn_canonical_hot_addresses.py`
- Test: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Produces: selection policy `missing-active-then-oldest-watermark` oraz bounded address file per chain.
- Consumes: snapshoty, netflow, history watermark i `run_earn_canonical_history_refresh.py`.

- [ ] Napisac failing test, ze coverage mode dodaje brakujacy wallet mimo istniejacego publicznego baseline.
- [ ] Potwierdzic FAIL.
- [ ] Dodac jawny `--coverage-backfill` i priorytet aktywnych brakujacych historii bez `--existing-history-only`.
- [ ] Dodac serializowany workflow z bounded kohorta: Mantle `200`, Arbitrum `120`, Berachain `120` na run.
- [ ] Commitowac tylko pliki selection file, manifest, ledgery i shardy przez istniejacy bezpieczny helper.
- [ ] Uruchomic testy selektora oraz kontrakty workflowu.

### Task 5: Publiczny resolved interest ledger

**Files:**
- Modify: `build_earn_verified_ledger.py`
- Modify: `build_earn_verified_ledger_shards.py`
- Modify: `dashboard-core.js`
- Test: `tests/test_build_earn_verified_ledger.py`
- Test: `tests/test_earn_ledger_shards.py`
- Test: `tests/test_history_tax_export_contracts.py`

**Interfaces:**
- Produces: opcjonalne `resolvedInterestLedger` z `comparisonBlock`, `generatedAt`, `markets[mid].earnYield`, bucketami i `strictStatus`.
- Consumes: tylko zaufany interest-ledger artefakt zgodny z aktualnym canonical history i snapshotem.

- [ ] Napisac failing test, ze zaufany resolved ledger przechodzi do publicznego marketu i shardu.
- [ ] Napisac failing test, ze stale/mismatch/inferred zrodlo jest odrzucane.
- [ ] Potwierdzic oba FAIL.
- [ ] Dodac walidowane wczytanie i publikacje resolved ledgera bez zmiany obecnych fallbackow.
- [ ] W frontendzie uzyc resolved ledgera jako fast path i replayowac tylko live delta od `comparisonBlock`.
- [ ] Uruchomic testy buildera, shardow i runtime contracts.

### Task 6: Pierwszy render bez blokowania na rewardach

**Files:**
- Modify: `dashboard-core.js`
- Test: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Produces: `earn_startDeferredSummaryFetches()` uruchamiane po stabilnym renderze pozycji.
- Consumes: istniejace fetchery oDOLO, Merkl, iBGT i dGMX.

- [ ] Dodac failing test kolejnosci: balance/ledger render przed await reward summary.
- [ ] Potwierdzic FAIL.
- [ ] Przeniesc rewardy z krytycznego `Promise.allSettled` do odroczonego zadania z bezpiecznym rerenderem.
- [ ] Zachowac skeleton/licznik rewardow do zakonczenia fetchu.
- [ ] Uruchomic kontrakty i syntax check.

### Task 7: Integracja i pelna weryfikacja

**Files:**
- Modify generated: `earn/earn-core.html`
- Modify generated: `earn/earn-core.js`
- Modify: `lessons.md`

**Interfaces:**
- Consumes: wszystkie interfejsy z Task 1-6.
- Produces: aktualny bundle EARN i udokumentowane reguly produkcyjne.

- [ ] Uruchomic `python3 build_earn_bundle.py` i `python3 build_earn_bundle.py --check`.
- [ ] Uruchomic `node --check dashboard-core.js`, `node --check earn/earn-core.js` i `python3 -m py_compile` dla zmienionych skryptow.
- [ ] Uruchomic `python3 run_earn_audit_checks.py`.
- [ ] Uruchomic reprezentatywny audyt marketow z `build_earn_representative_audit.py --check`.
- [ ] Przetestowac dwa kolejne lookupy tego samego walleta na Arbitrum oraz statusy Arbitrum, Berachain i Mantle na lokalnym HTTP.
- [ ] Sprawdzic `git diff --check`, zakres diffu i brak przypadkowych zmian danych.
