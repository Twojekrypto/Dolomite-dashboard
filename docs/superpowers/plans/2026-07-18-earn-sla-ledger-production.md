# Produkcyjne ledgery i SLA EARN - plan implementacji

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Cel:** Utrzymać aktualne dane EARN dla aktywnych portfeli w przedziale 2-6 godzin, publikować matematycznie zweryfikowany interest ledger i używać publicznego RPC wyłącznie do kontroli bieżącego stanu.

**Architektura:** Canonical backfill najpierw wybiera portfele widoczne w najnowszym snapshotcie, a zimny backlog przetwarza dopiero później i niezależnie per chain. Producent resolved ledger zapisuje tylko rynki, które dają się ściśle odtworzyć do przypiętego bloku snapshotu; UI renderuje ten ledger natychmiast i uruchamia kontrolę live w tle. Watchdog klasyfikuje 2 h jako ostrzeżenie, automatycznie dispatchuje naprawę i po 6 h kończy się alarmem krytycznym.

**Tech Stack:** Python 3, JavaScript bez frameworka, JSON, GitHub Actions, `unittest`, statyczny GitHub Pages.

## Global Constraints

- Nie obniżać rygoru `strictStatus=verified`; brak dowodu oznacza pominięcie wpisu resolved, nie inference.
- Przeliczenie `Par -> Wei` musi używać kontraktowego round-half-up, bez `parseFloat` i bez utraty precyzji.
- Dane bieżące mogą korzystać z RPC, ale pełny historyczny replay nie może blokować pierwszego renderu, gdy istnieje zweryfikowany ledger.
- Aktywne sieci mają SLA: warning po 2 h, automatyczna naprawa i critical po 6 h.
- Workflowy zapisujące nowy JSON muszą jawnie dodać i commitować ten artefakt.

---

### Task 1: Priorytet aktywnych portfeli

**Files:**
- Modify: `select_earn_canonical_hot_addresses.py`
- Modify: `.github/workflows/backfill-earn-canonical-coverage.yml`
- Test: `tests/test_select_earn_canonical_hot_addresses.py`
- Test: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Consumes: najnowszy snapshot sald, universe netflow i istniejące canonical histories.
- Produces: selection file w kolejności `priority -> active missing -> active stale -> cold missing -> cold stale` oraz metadane liczebności kohort.

- [ ] Dodać test, w którym stary pusty adres ma wcześniejszy watermark niż aktywny adres, i wymagać wyboru aktywnego adresu jako pierwszego.
- [ ] Uruchomić `python3 -m unittest tests.test_select_earn_canonical_hot_addresses tests.test_earn_dashboard_contracts` i potwierdzić czerwony test.
- [ ] Zaimplementować jawne kohorty aktywne/zimne oraz ich liczniki w metadata.
- [ ] Uruchamiać Arbitrum, Berachain i Mantle równolegle, ale z tym samym per-chain concurrency group co head refresh; harmonogram coverage ustawić co 2 h.
- [ ] Ponownie uruchomić testy selektora i kontraktów workflow.

### Task 2: Producent resolved interest ledger

**Files:**
- Create: `build_earn_resolved_interest_ledger.py`
- Modify: `generate_earn_snapshots.py`
- Modify: `build_earn_verified_ledger.py`
- Modify: canonical EARN workflows w `.github/workflows/`
- Test: `tests/test_build_earn_resolved_interest_ledger.py`
- Test: `tests/test_generate_earn_snapshots.py`
- Test: `tests/test_build_earn_verified_ledger.py`

**Interfaces:**
- Consumes: snapshot z `blockNumber` i indeksami rynku oraz canonical subaccount history zeskanowane co najmniej do tego bloku.
- Produces: `data/earn-resolved-interest-ledger/<chain>/<address>.json` z `strictMethod=interest-ledger`, pełnym `replayVerificationData` i manifestem.

- [ ] Napisać testy round-half-up, przypięcia bloku, pełnego dowodu replay oraz odrzucenia danych niepełnych/borrow-route.
- [ ] Uruchomić nowe testy i potwierdzić, że nie istnieje producent ani wymagane metadata snapshotu.
- [ ] Rozszerzyć snapshot o `_meta.block.number`, `supplyIndex` i `borrowIndex`, zachowując metadata chainów nieodświeżanych w danym runie.
- [ ] Zaimplementować producenta, który zapisuje wyłącznie rynki z kompletną canonical historią, zgodnym końcowym Par/Wei i integer-only matematyką.
- [ ] Zmienić walidator konsumenta tak, aby wymagał `canonical.lastScannedBlock >= comparisonBlock`, zamiast sztucznej równości z późniejszym headem.
- [ ] Wpiąć producenta przed budowę verified ledgerów w head refreshu, coverage i snapshot workflow; commitować tylko audytowaną kohortę oraz manifest.
- [ ] Uruchomić testy producenta, snapshotów, verified ledgerów i shardów.

### Task 3: Watchdog SLA 2 h/6 h

**Files:**
- Modify: `update_earn_freshness_status.py`
- Create: `scripts/check_earn_sla.py`
- Modify: `.github/workflows/earn-freshness-watchdog.yml`
- Test: `tests/test_update_earn_freshness_status.py`
- Create: `tests/test_check_earn_sla.py`

**Interfaces:**
- Consumes: `data/earn-freshness/status.json` wygenerowany z canonical/netflow/snapshot freshness.
- Produces: per-chain `slaStatus`, zbiorcze warning/critical chains, GitHub annotations i niezerowy exit code tylko po 6 h.

- [ ] Dodać testy granic 2 h i 6 h oraz priorytetu naprawy chainów warning/critical.
- [ ] Uruchomić testy i potwierdzić niezgodność obecnego progu 3 h.
- [ ] Dodać jawny stan `ok/warning/critical`, pozostawić auto-dispatch od warning i nadać krytycznym najwyższy priorytet.
- [ ] Dodać końcowy checker workflow: warning pozostawia zielony run, critical po dispatchu naprawy kończy run alarmem.
- [ ] Uruchomić testy freshness i parsowanie YAML.

### Task 4: Statyczny fast path i live control

**Files:**
- Modify: `earn/earn-core.js`
- Modify: `dashboard-core.html` (cache busting)
- Test: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Consumes: `resolvedInterestLedger` z publicznego verified ledger sharda.
- Produces: pierwszy render sald i historycznego yield bez czekania na pełny replay; kontrola RPC oraz delta od comparison block działają w tle.

- [ ] Dodać kontrakt, że zweryfikowany statyczny ledger jest aplikowany przed startem historycznego replayu i że pełny replay nie blokuje pierwszego renderu.
- [ ] Uruchomić kontrakty JS i potwierdzić czerwony test.
- [ ] Rozdzielić ścieżkę `published baseline` od `live verification`; zachować RPC do bieżących sald/indeksów i degradację statusu przy faktycznym mismatchu.
- [ ] Podbić wersję bundla i uruchomić `node --check earn/earn-core.js`.

### Task 5: Audyt, wdrożenie i kontrola produkcji

**Files:**
- Modify: `lessons.md`
- Verify: wszystkie zmienione skrypty, workflowy, dane i strona `/earn/`.

**Interfaces:**
- Consumes: wszystkie rezultaty wcześniejszych zadań.
- Produces: zielony audyt, commit na `master`, push oraz sprawdzone uruchomienia produkcyjne.

- [ ] Uruchomić wąskie testy, `python3 run_earn_audit_checks.py`, `python3 -m py_compile`, `node --check` i walidację YAML/bash workflowów.
- [ ] Zbudować próbny resolved ledger dla reprezentatywnych danych i sprawdzić, że wpisy niespełniające dowodu nie są publikowane.
- [ ] Zweryfikować `/earn/` lokalnie w przeglądarce, w tym pierwszy render i status live/historical coverage.
- [ ] Dopisać reguły operacyjne do `lessons.md`.
- [ ] Pobrać najnowsze automatyczne dane, zrebasować bez utraty artefaktów, commitować zmiany i wypchnąć `master`.
- [ ] Sprawdzić GitHub Actions oraz produkcyjny asset z cache-bustingiem.
