# EARN Event-Driven Publishing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automatycznie finalizowac kazdy refresh EARN przez watchdog i Pages oraz publikowac bezpieczny, przyrostowy postep backfillu Berachain.

**Architecture:** Wspolny commit helper dispatchuje monitor po pushu producenta. Monitor rozdziela tryb naprawczy od finalizujacego i dispatchuje Pages dopiero po kontroli SLA. Backfill przechowuje aktywna kohorte w cache i publikuje wyłącznie historie kompletne do przypietego target blocku.

**Tech Stack:** Bash, GitHub Actions YAML, Python 3.11, `unittest`, GitHub Pages.

## Global Constraints

- Nie publikowac canonical history przed pelnym skanem eventow od start blocku do target blocku.
- Nie tworzyc petli `monitor -> producent -> monitor -> producent`.
- Zachowac cron `*/15` jako fallback oraz progi warning 2h i critical 6h.
- Nie zmieniac sekretow ani nie dodawac kluczy RPC do repozytorium.
- Produkcja pozostaje na `master`.

---

### Task 1: Kontrakt finalizacji EARN

**Files:**
- Modify: `tests/test_earn_commit_helper.py`
- Modify: `tests/test_earn_dashboard_contracts.py`
- Modify: `tests/test_pages_workflow_contracts.py`
- Modify: `scripts/commit_with_fresh_earn_status.sh`
- Modify: `.github/workflows/monitor-earn-freshness.yml`
- Modify: `.github/workflows/update-earn-*.yml`
- Modify: `.github/workflows/backfill-earn-canonical-coverage.yml`

**Interfaces:**
- Consumes: udany push danych EARN.
- Produces: `gh workflow run monitor-earn-freshness.yml -f allow_remediation=<true|false>`, a po SLA `gh workflow run pages.yml`.

- [ ] Dodac testy oczekujace dispatchu monitora oraz braku bezposredniego deployu producenta.
- [ ] Uruchomic testy i potwierdzic oczekiwana porazke.
- [ ] Rozszerzyc helper i workflowy o finalizacje zdarzeniowa.
- [ ] Uruchomic testy i potwierdzic sukces.

### Task 2: Publikowalny postep Berachain

**Files:**
- Create: `scripts/select_earn_publishable_histories.py`
- Create: `tests/test_select_earn_publishable_histories.py`
- Modify: `.github/workflows/backfill-earn-canonical-coverage.yml`
- Modify: `tests/test_earn_dashboard_contracts.py`

**Interfaces:**
- Consumes: selection file, robocze historie, chain start block i locked target block.
- Produces: plik adresow, ktorych historie sa kompletne i bezpieczne do publikacji.

- [ ] Dodac testy dla kompletnej, stalej, blednej i brakujacej historii.
- [ ] Uruchomic testy i potwierdzic oczekiwana porazke importu.
- [ ] Zaimplementowac selektor oraz walidacje zakresu.
- [ ] Zachowac aktywna kohorte w cache i publikowac gotowy podzbior po pelnym skanie.
- [ ] Uruchomic testy selektora i kontraktu workflow.

### Task 3: Stabilne chunki canonical Berachain

**Files:**
- Modify: `tests/test_scan_earn_subaccount_history_events.py`
- Modify: `tests/test_earn_dashboard_contracts.py`
- Modify: `scan_earn_netflow.py`
- Modify: `scan_earn_subaccount_history_events.py`

**Interfaces:**
- Consumes: `CHAINS[chain].canonical_max_block_chunk`.
- Produces: poczatkowy i maksymalny chunk skanu nieprzekraczajacy 9 999 blokow na Berachain.

- [ ] Dodac test jednostkowy limitu canonical.
- [ ] Uruchomic test i potwierdzic porazke przy obecnym limicie 49 999.
- [ ] Dodac limit Berachain i uzyc go w event scannerze.
- [ ] Uruchomic testy skanera.

### Task 4: Weryfikacja i wdrozenie

**Files:**
- Verify all modified files.

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces: sprawdzony commit produkcyjny i uruchomiony backfill Berachain.

- [ ] Uruchomic testy jednostkowe i `python3 run_earn_audit_checks.py`.
- [ ] Zweryfikowac skladnie Python, Bash i YAML oraz `git diff --check`.
- [ ] Wykonac dwa przeglady: correctness/regression oraz maintainability/security.
- [ ] Commitowac, zrebase'owac na aktualny `master` i pushnac.
- [ ] Obserwowac workflow EARN -> Monitor -> Pages i sprawdzic status live z cache bustingiem.
