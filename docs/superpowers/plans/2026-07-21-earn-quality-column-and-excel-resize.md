# EARN Quality Column And Excel Resize Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Umożliwić lokalnemu edytorowi EARN poszerzanie kolumn bez zwężania pozostałych oraz przenieść zwięzłe stany jakości danych do nowej kolumny `Quality` w `Supply Assets`.

**Architecture:** `earn/earn-layout-editor.js` przestaje zachowywać sumę szerokości równą 100% i wyznacza rzeczywistą szerokość tabeli jako maksimum 100% oraz zapisanej sumy kolumn. Shell tabeli zachowuje własne przewijanie poziome. `earn/earn-core.js` buduje jeden kompaktowy zestaw statusów jakości z istniejących obiektów prezentacji weryfikacji i źródła yield, dzięki czemu algorytmy yield oraz istniejące tooltipy pozostają źródłem prawdy.

**Tech Stack:** Statyczny HTML/CSS/JavaScript, Node.js `node:test`, Python `unittest`, Playwright i `python3 -m http.server`.

## Global Constraints

- Zmiany pozostają lokalne w worktree `codex/earn-layout-editor`; nie pushować ani nie publikować Pages przed zapisaniem i osobną akceptacją układu użytkownika.
- Edytor ładuje się tylko dla loopbacka z `?layoutEditor=1`; publiczna strona nie pobiera jego JavaScript ani CSS.
- Brak nowych RPC, zmian obliczeń APR/APY, yield lub danych historycznych.
- Używać istniejącego `data-tip` i Earn `#unified-tooltip`; nie dodawać natywnych `title` ani tooltipów w komórkach supporting USD.
- Po zmianie tabeli używać wyłącznie stabilnych `data-column`, a nie selektorów `nth-child` dla Supply/Borrow.
- `Quality` ma prezentować krótki stan z kolorową kropką; pełne wyjaśnienie istnieje wyłącznie po najechaniu.
- Zapisany wcześniej układ Supply bez `Quality` musi migrować bez utraty kolejności lub szerokości istniejących kolumn.

---

### Task 1: Excelowy model szerokości lokalnego edytora

**Files:**
- Modify: `earn/earn-layout-editor.js`
- Modify: `earn/earn-layout-editor.css`
- Test: `tests/earn-layout-editor.test.js`

**Interfaces:**
- Produces: `getLayoutTableWidth(name, layout)` zwracające `Math.max(100, sumWidths(order, widths))`.
- Produces: `resizeLayout(name, layout, key, deltaPx, tableWidthPx)` zmieniające wyłącznie `widths[key]` z zachowaniem minimum kolumny.
- Consumes: `SCHEMAS`, `normalizeLayout`, `currentLayout` oraz istniejący shell `.earn-section-table-shell`.

- [ ] **Step 1: Napisać test RED dla szerokości Excelowej**

```js
test('resizing grows only the dragged column and exposes horizontal table width', () => {
  const base = editor.createDefaultLayout('supply');
  const widened = editor.resizeLayout('supply', base, 'details', 300, 1000);
  assert.ok(widened.widths.details > base.widths.details);
  for (const key of base.order.filter(key => key !== 'details')) {
    assert.equal(widened.widths[key], base.widths[key]);
  }
  assert.ok(editor.getLayoutTableWidth('supply', widened) > 100);
});
```

- [ ] **Step 2: Uruchomić test RED**

Run: `node --test tests/earn-layout-editor.test.js`

Expected: FAIL, ponieważ `resizeLayout` obecnie pobiera szerokość z innych kolumn, a `getLayoutTableWidth` nie istnieje.

- [ ] **Step 3: Zmienić model oraz adapter DOM**

```js
function getLayoutTableWidth(name, layout){
  const valid = normalizeLayout(name, layout);
  return valid ? Math.max(100, sumWidths(valid.order, valid.widths)) : 100;
}

function resizeLayout(name, layout, key, deltaPx, tableWidthPx){
  const current = normalizeLayout(name, layout);
  const delta = Number(deltaPx) / Number(tableWidthPx) * 100;
  const minimum = Math.ceil(((SCHEMAS[name].minimums[key] || 16) / Number(tableWidthPx) * 100) * 1e6) / 1e6;
  const widths = {...current.widths};
  widths[key] = round(Math.max(minimum, widths[key] + delta));
  return {...current, widths};
}
```

W `normalizeLayout` zastąpić wymóg sumy 100% wymogiem dodatniej, skończonej sumy. W `applyLayout` ustawić `table.style.setProperty('width', `${getLayoutTableWidth(name, valid)}%`, 'important')`. Nie usuwać szerokości pozostałych kolumn.

W `earn-layout-editor.css` wymusić lokalnie `overflow-x:auto` na shellu oraz `min-width:100%` na edytowanej tabeli; nie wprowadzać overflow strony ani body.

- [ ] **Step 4: Uruchomić testy modelu**

Run: `node --test tests/earn-layout-editor.test.js`

Expected: PASS; istniejące testy nadal potwierdzają minimalne szerokości i jeden spacer.

- [ ] **Step 5: Commit lokalny**

```bash
git add earn/earn-layout-editor.js earn/earn-layout-editor.css tests/earn-layout-editor.test.js
git commit -m "feat: preserve EARN column widths while resizing"
```

### Task 2: Quality jako osobna kolumna Supply

**Files:**
- Modify: `earn/earn-core.html`
- Modify: `earn/earn-core.js`
- Modify: `dashboard-core.css`
- Modify: `earn/earn-draft.css`
- Modify: `earn/earn-layout-editor.js`
- Test: `tests/test_earn_layout_contracts.py`
- Test: `tests/earn-layout-editor.test.js`

**Interfaces:**
- Produces: `earn_renderSupplyQualityCell(marketId, symbol, decimals, yieldCalc)` zwracające HTML komórki z krótkimi markerami `data-tip`.
- Consumes: `earn_getVerificationPresentation`, `earn_getYieldMethodMeta`, `earn_isReplayYieldMethod`, `earn_escapeHtml` oraz stan `earn_replayVerificationReady`.
- Produces: schemat Supply `['token', 'quality', 'price', 'supply', 'balance', 'yield', 'details']`.

- [ ] **Step 1: Napisać test RED dla struktury i skróconej prezentacji**

```python
def test_supply_quality_column_and_compact_rate_labels(self):
    self.assertIn('data-column="quality"', self.html)
    self.assertLess(self.html.index('data-column="token"'), self.html.index('data-column="quality"'))
    self.assertIn('function earn_renderSupplyQualityCell(', self.js)
    self.assertIn("label: 'Interest'", self.js)
    self.assertIn("label: 'oDOLO'", self.js)
    self.assertIn('.earn-quality-marker', self.css)
```

W `tests/earn-layout-editor.test.js` dodać również migrację poprzedniego zapisu:

```js
test('legacy supply layout gains Quality without changing saved widths', () => {
  const legacy = editor.createDefaultSavedLayouts();
  legacy.supply = {
    version: 1,
    order: ['token', 'price', 'supply', 'balance', 'yield', 'details'],
    widths: { token: 32, price: 10, supply: 20, balance: 16, yield: 14, details: 8 },
  };
  const migrated = editor.normalizeSavedLayouts(legacy);
  assert.deepEqual(migrated.supply.order, ['token', 'quality', 'price', 'supply', 'balance', 'yield', 'details']);
  assert.equal(migrated.supply.widths.price, 10);
  assert.equal(migrated.supply.widths.quality, 11);
});
```

- [ ] **Step 2: Uruchomić test RED**

Run: `python3 -m unittest tests/test_earn_layout_contracts.py -v`

Expected: FAIL, ponieważ `Quality` i jej renderer nie istnieją.

- [ ] **Step 3: Dodać strukturę i renderer**

W `earn/earn-core.html` dodać `<col data-column="quality">` i nagłówek `Quality` bez sortowania po `Token` i przed `Price`.

W `earn/earn-core.js` dodać renderer korzystający z istniejących prezentacji:

```js
function earn_renderSupplyQualityCell(marketId, symbol, decimals, yieldCalc){
  const markers = [];
  if (earn_replayVerificationReady) {
    const verification = earn_getVerificationPresentation(marketId, yieldCalc, symbol, decimals);
    if (verification?.counted) markers.push({
      cls: verification.status,
      label: verification.label,
      tip: verification.title,
    });
  }
  if (yieldCalc?.hasData && !earn_isReplayYieldMethod(yieldCalc.method)) {
    const source = earn_getYieldMethodMeta(yieldCalc.method);
    const label = source.label.includes('Netflow') ? 'Netflow'
      : source.label.includes('Snapshot') ? 'Snapshot'
      : source.label.includes('Inferred') ? 'Inferred'
      : source.label.includes('Cycle') ? 'Cycle'
      : source.label.includes('Acct0') ? 'Acct0' : 'Fallback';
    markers.push({ cls: source.cls, label, tip: source.title });
  }
  return markers.length
    ? `<div class="earn-quality-cell">${markers.map(marker => `<span class="earn-quality-marker ${marker.cls}" data-tip="${earn_escapeHtml(marker.tip)}"><span class="earn-status-dot"></span>${earn_escapeHtml(marker.label)}</span>`).join('')}</div>`
    : '<span class="earn-quality-empty">—</span>';
}
```

W Supply row dodać komórkę `data-column="quality"` i usunąć `${verifyBadge}` i `${sourceBadge}` spod tokena wyłącznie dla tego wiersza. Zwiększyć `colspan` detail oraz spacer do 7.

W budowie APR użyć etykiet `Interest`, `Yield`, `GM`, `oDOLO`; każdą linię wyposażyć w obecny opis źródła przez `data-tip`.

W `earn-layout-editor.js` uzupełnić schemat Supply o `quality`, z domyślnymi szerokościami: `token:28`, `quality:11`, `price:9`, `supply:17`, `balance:16`, `yield:12`, `details:7` i minimum `quality:96`. Przed normalizacją pełnego zapisu dodać migrację legacy Supply: po `token` wstawia `quality`, zachowuje wszystkie istniejące szerokości i ustawia `quality:11`.

- [ ] **Step 4: Uzupełnić CSS o spójny, nieobciążający wygląd**

```css
.earn-asset-table [data-column="quality"] { text-align: left; }
.earn-quality-cell { display:flex; flex-direction:column; align-items:flex-start; gap:4px; }
.earn-quality-marker { display:inline-flex; align-items:center; gap:5px; color:var(--text-muted); font-size:10px; white-space:nowrap; }
.earn-quality-marker.mismatch { color:#f87171; }
.earn-quality-marker.fallback,
.earn-quality-marker.coverage_incomplete { color:#f59e0b; }
.earn-quality-marker.verified { color:#34d399; }
.earn-quality-marker.inferred,
.earn-quality-marker.snapshot { color:#60a5fa; }
```

Dodać równoważne selektory z `data-column="quality"` do `earn/earn-draft.css` i zaktualizować wszystkie szerokości Supply dla trybu normalnego oraz `no-yield`. Nie dodawać `nth-child` dla Supply.

- [ ] **Step 5: Uruchomić kontrakty tabeli**

Run: `python3 -m unittest tests/test_earn_layout_contracts.py -v && node --test tests/earn-layout-editor.test.js`

Expected: PASS; testy potwierdzają siedem kolumn i kompaktowe etykiety.

- [ ] **Step 6: Commit lokalny**

```bash
git add earn/earn-core.html earn/earn-core.js earn/earn-draft.css dashboard-core.css earn/earn-layout-editor.js tests/test_earn_layout_contracts.py tests/earn-layout-editor.test.js
git commit -m "feat: add compact EARN supply quality column"
```

### Task 3: Sprawdzenie w przeglądarce przed przekazaniem lokalnej wersji

**Files:**
- Verify: `earn/earn-core.html`
- Verify: `earn/earn-core.js`
- Verify: `earn/earn-layout-editor.js`
- Verify: `earn/earn-layout-editor.css`

**Interfaces:**
- Consumes: lokalny serwer `python3 -m http.server 4178 --bind 127.0.0.1`.
- Uses: adres `0x28da3dde285d8f1f87b2d858f89961bb8b9af180` na Arbitrum.

- [ ] **Step 1: Uruchomić pełne testy statyczne**

Run:

```bash
node --test tests/earn-layout-editor.test.js
python3 -m unittest tests/test_earn_layout_contracts.py tests/test_earn_dashboard_contracts.py tests/test_emode_ux_contracts.py -v
node --check earn/earn-layout-editor.js
node --check earn/earn-core.js
git diff --check
```

Expected: wszystkie testy PASS, bez błędów składni i bez whitespace errors.

- [ ] **Step 2: Zweryfikować rzeczywisty lookup i interakcje**

Na `http://localhost:4178/earn/?layoutEditor=1` wybrać Arbitrum i wyszukać adres testowy. Potwierdzić przez `getComputedStyle()` oraz bounding boxes:

```text
- Supply zawiera Token, Quality, Price, Supply, Current Balance, Total Yield Earned i Details.
- Poszerzenie Details nie zmniejsza Token, Quality, Price, Supply, Balance ani Yield.
- Suma ponad 100% pokazuje poziomy scrollbar tylko wewnątrz Supply shell.
- Quality pokazuje krótkie markery z kropkami; tooltip odsłania pełną przyczynę.
- APR/APY, Hide Dust, sortowanie Price i rozwinięcie Details zachowują kolejność oraz colspan 7.
- Publiczny localhost bez ?layoutEditor=1 nie ma toolbara, uchwytów ani wpisu layoutu.
```

- [ ] **Step 3: Przekazać użytkownikowi lokalną wersję bez pushowania**

Podaj dokładnie `http://localhost:4178/earn/?layoutEditor=1`, potwierdź że nic nie jest live, oraz poproś o ustawienie kolumn i kliknięcie ikony dyskietki przed kolejną prośbą o publikację.
