# Plan implementacji podsumowania Lock Expiry Timeline

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Cel:** Zastąpić wysoką, dynamiczną kartę podsumowania kompaktowym, stałym podsumowaniem w obszarze wykresu oraz dodać rzeczywisty czas aktualizacji danych w nagłówku.

**Architektura:** Istniejący payload `vedolo_expiry.json` pozostaje jedynym źródłem danych. `renderExpiry()` aktualizuje statyczne podsumowanie dla bieżącego widoku, natomiast `setExpiryFocus()` odpowiada wyłącznie za podświetlenie słupków; wspólny helper `dataUpdatedLabel()` formatuje `state.expiry.timestamp` w nagłówku.

**Tech Stack:** Statyczny HTML/CSS/JavaScript, Python `unittest`, lokalny `python3 -m http.server`, testy przeglądarkowe.

## Global Constraints

- Podsumowanie pozostaje stałe podczas hoveru i kliknięcia słupka.
- Wyszukanie portfela może zmienić sumę, bo zmienia bieżący widok danych.
- Na desktopie podsumowanie znajduje się w prawym górnym rogu obszaru wykresu.
- Na mobile podsumowanie znajduje się nad przewijanymi słupkami i nie zasłania danych.
- Etykieta aktualizacji używa `vedolo_expiry.json.timestamp`.
- Bez nowych zależności i bez zmian w pipeline danych.

---

### Zadanie 1: Kompaktowe podsumowanie i metadata

**Pliki:**
- Modyfikacja: `tests/test_vedolo_preview_contracts.py`
- Modyfikacja: `vedolo-preview.html`
- Modyfikacja: `vedolo/index.html`

**Interfejsy:**
- Consumes: `state.expiry.timestamp`, `buildExpiryView()`, `dataUpdatedLabel(timestamp)`, `fmtDolo(value)`.
- Produces: elementy `#expiry-meta`, `#expiry-summary-value`, `#expiry-summary-label` oraz wersję trasy `vedolo-expiry-summary-overlay-20260718`.

- [ ] **Krok 1: Dodaj test kontraktu, który najpierw ma się nie powieść**

```python
def test_lock_expiry_uses_static_chart_summary_and_source_update_time(self):
    section = re.search(
        r'<section class="card chart-card expiry-card">(?P<body>.*?)</section>',
        self.html,
        re.S,
    ).group("body")
    self.assertIn('id="expiry-meta"', section)
    self.assertIn('id="expiry-summary-value"', section)
    self.assertIn('id="expiry-summary-label"', section)
    self.assertNotIn('id="expiry-focus"', section)
    self.assertIn('setText("expiry-meta", dataUpdatedLabel(state.expiry?.timestamp));', self.html)
    self.assertIn('setText("expiry-summary-value", fmtDolo(total));', self.html)
    self.assertIn('setText("expiry-summary-label",', self.html)
    self.assertNotIn('focusValue.textContent', self.html)
    self.assertIn('.expiry-summary{position:absolute;top:8px;right:12px;', self.html)
    self.assertIn('.expiry-summary{position:static;', self.html)
    self.assertIn('vedolo-expiry-summary-overlay-20260718', self.route)
```

- [ ] **Krok 2: Uruchom test i potwierdź oczekiwaną porażkę**

Run: `python3 -m unittest tests.test_vedolo_preview_contracts.VeDoloPreviewContractsTest.test_lock_expiry_uses_static_chart_summary_and_source_update_time`

Expected: `FAIL`, ponieważ nowe identyfikatory i kontrakt trasy jeszcze nie istnieją.

- [ ] **Krok 3: Zmień markup i CSS na kompaktowy overlay**

W `vedolo-preview.html`:

```html
<div class="card-meta"><span class="pulse"></span><span id="expiry-meta">Loading data update...</span></div>
```

Zastąp `#expiry-focus` wrapperem wykresu:

```html
<div class="expiry-chart-shell">
  <div class="expiry-summary" aria-live="polite">
    <span class="expiry-summary-kicker">Total scheduled</span>
    <strong id="expiry-summary-value">--</strong>
    <span id="expiry-summary-label">-- buckets</span>
  </div>
  <div class="expiry-chart" id="expiry-chart">
    <div class="expiry-axis" aria-hidden="true"><span id="expiry-axis-max">--</span><span id="expiry-axis-mid">--</span><span>0</span></div>
    <div class="expiry-bars" id="expiry-bars"></div>
  </div>
</div>
```

Dodaj stabilny layout:

```css
.expiry-chart-shell{position:relative}
.expiry-summary{position:absolute;top:8px;right:12px;z-index:2;display:flex;align-items:baseline;justify-content:flex-end;gap:8px;pointer-events:none;font-family:var(--mono);font-variant-numeric:tabular-nums}
.expiry-summary-kicker{font-size:9.5px;text-transform:uppercase;color:var(--fg-4);font-weight:700}
.expiry-summary strong{font-size:17px;color:var(--fg-1)}
.expiry-summary-label{font-size:10.5px;color:var(--fg-3)}
.expiry-card .expiry-bars{padding-top:58px}
```

W breakpointcie `max-width:560px` ustaw `.expiry-summary{position:static;...}` przed przewijanym `.expiry-chart`.

- [ ] **Krok 4: Uprość logikę renderowania i podłącz timestamp**

W `syncVedoloTableMetadata()` dodaj:

```javascript
setText("expiry-meta", dataUpdatedLabel(state.expiry?.timestamp));
```

W `renderExpiry()` ustaw podsumowanie raz dla bieżącego widoku:

```javascript
setText("expiry-summary-value", fmtDolo(total));
setText("expiry-summary-label", `${fmtInt(segments.length)} expiry buckets`);
```

Usuń zapisy do `focusTitle`, `focusValue` i `focusLabel` z `setExpiryFocus()`. Funkcja ma dalej nadawać klasy `active`, `dim`, `has-active` i `aria-pressed`.

- [ ] **Krok 5: Zaktualizuj cache bust trasy**

Do `vedolo/index.html` dopisz do `version`:

```text
-vedolo-expiry-summary-overlay-20260718
```

- [ ] **Krok 6: Uruchom testy kontraktowe**

Run: `python3 -m unittest tests.test_vedolo_preview_contracts tests.test_hero_metric_chip_contracts`

Expected: wszystkie testy `OK`.

- [ ] **Krok 7: Zweryfikuj desktop i mobile w przeglądarce**

Run: `python3 -m http.server 4182 --bind 127.0.0.1`

Sprawdź `http://127.0.0.1:4182/vedolo-preview.html`:
- `#expiry-meta` pokazuje `Data updated · ... ago`;
- desktop: `.expiry-summary` znajduje się w prawym górnym rogu chart shell i nie przecina słupków;
- hover słupka nie zmienia `#expiry-summary-value` ani `#expiry-summary-label`;
- wyszukanie poprawnego portfela aktualizuje wartość;
- mobile `390x844`: summary jest nad przewijanym wykresem, bez nakładania i bez rozszerzania strony;
- brak nowych warningów i błędów konsoli.

- [ ] **Krok 8: Uruchom pełną walidację**

Run: `python3 run_earn_audit_checks.py`

Expected: wszystkie testy kończą się `OK` i skrypt raportuje `All EARN audit checks passed.`

Run: `git diff --check`

Expected: brak wyniku i kod wyjścia `0`.

- [ ] **Krok 9: Zacommituj implementację**

```bash
git add vedolo-preview.html vedolo/index.html tests/test_vedolo_preview_contracts.py docs/superpowers/plans/2026-07-18-vedolo-lock-expiry-summary.md
git commit -m "polish veDOLO lock expiry summary"
```

- [ ] **Krok 10: Wypchnij i zweryfikuj produkcję**

```bash
git push dolomite-dashboard master
```

Poczekaj na `Deploy GitHub Pages`, a następnie sprawdź publiczny `vedolo-preview.html` z cache-bustingiem pod kątem `expiry-summary` oraz wersję trasy `vedolo-expiry-summary-overlay-20260718`.
