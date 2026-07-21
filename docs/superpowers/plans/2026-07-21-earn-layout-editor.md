# Plan Implementacji Lokalnego Edytora EARN

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Cel:** Dodać wyłącznie lokalny edytor kolejności i szerokości kolumn dla `Supply Assets` oraz `Borrow Positions`, a równocześnie wdrożyć kolumnę `Price`, nazewnictwo i wygląd `Supply` oraz kompaktowy `Details` zgodny z `Dolomite Assets`.

**Architektura:** Czysty model układu w `earn/earn-layout-editor.js` będzie niezależny od DOM i testowalny przez `node:test`. Adapter DOM dla dwóch prawdziwych tabel będzie aktywowany tylko, gdy hostname jest loopbackem oraz URL zawiera `?layoutEditor=1`; w innych przypadkach nawet nie pobierze arkusza CSS ani kodu edytora. Tabele EARN dostaną stabilne `data-column`, aby sortowanie i asynchroniczne ponowne renderowanie mogły odtworzyć wybrany układ bez zmiany obliczeń yield.

**Technologie:** Statyczny HTML/CSS/JavaScript, Node.js `node:test`, Python `unittest`, `python3 -m http.server`, kontrola lokalnej strony w przeglądarce.

## Ograniczenia Globalne

- Przed edycją kodu przenieść własne commity dokumentacji na bieżący `dolomite-dashboard/master`; nie resetować, nie usuwać ani nie nadpisywać automatycznie publikowanych danych EARN.
- Lokalny test wizualny ma używać aktualnych danych z `master`, ponieważ obecny katalog jest 39 commitów danych za live.
- Edytor może uruchomić się wyłącznie dla `localhost`, `127.0.0.1` lub `::1` i dokładnie `layoutEditor=1`.
- Publiczna strona nie może tworzyć toolbara, uchwytów, `MutationObserver`, wpisu `localStorage` ani pobierać `earn-layout-editor.js`/`.css`.
- `Supply Assets` ma wymagane klucze `token | price | supply | balance | yield | details`; `Borrow Positions` ma `health | collateral | debt | pnl | details`.
- Każda tabela dopuszcza dokładnie jeden opcjonalny `spacer`; szerokości dodatnie sumują się do 100%.
- Nie dodawać źródła cen ani zapytań RPC: `Price` korzysta z `earn_getUsdPrice` i istniejącego cache.
- Zachować sortowanie, APR/APY, Hide Dust, E-Mode, rozwijanie wierszy, stan `no-yield` i obliczenia yield.
- Nowy kod produkcyjny powstaje dopiero po teście RED dla odpowiedniego zachowania.
- Nie commitować finalnego edytora ani nie pushować go na Pages przed eksportem układu i osobną akceptacją użytkownika.

---

### Zadanie 1: Zsynchronizować bezpieczny punkt startowy i potwierdzić zgodność live

**Pliki:**
- Zmiana historii lokalnej: tylko własne commity dokumentacji.
- Sprawdzenie: `earn/index.html`, `earn/earn-core.html`, `earn/earn-core.js`, `earn/earn-draft.js`, `earn/earn-draft.css`, `earn/earn-cache-policy.js`, `earn/earn-rpc-policy.js`, `dashboard-core.css`, `route-loader.js`.

**Rezultat:** Lokalny EARN korzysta z identycznego kodu i aktualnych snapshotów danych jak `master` przed rozpoczęciem pracy nad edytorem.

- [ ] **Krok 1: Odświeżyć referencję zdalnego mastera**

```bash
git fetch dolomite-dashboard master --quiet
git status --short --branch
git log -1 --format='%H %s' dolomite-dashboard/master
```

Oczekiwany wynik: znany bieżący commit zdalny oraz tylko własne commity dokumentacji przed lokalnym `master`.

- [ ] **Krok 2: Przenieść commity dokumentacji na aktualny master bez utraty danych**

```bash
git rebase dolomite-dashboard/master
git status --short --branch
```

Oczekiwany wynik: czysty worktree, `master` nie jest za zdalnym masterem, a specyfikacja pozostaje w historii. W razie konfliktu rozwiązać wyłącznie pliki `docs/superpowers/`, wykonać `git add <plik>` oraz `git rebase --continue`.

- [ ] **Krok 3: Potwierdzić identyczność frontendowych assetów z Pages**

```bash
for file in earn/index.html earn/earn-core.html earn/earn-core.js earn/earn-draft.js earn/earn-draft.css earn/earn-cache-policy.js earn/earn-rpc-policy.js dashboard-core.css route-loader.js; do
  curl -sS --max-time 15 "https://twojekrypto.github.io/Dolomite-dashboard/$file" | shasum -a 256
  sha256sum "$file"
done
```

Oczekiwany wynik: każda para hashy jest identyczna przed wprowadzeniem edytora.

- [ ] **Krok 4: Zapisać punkt kontrolny**

```bash
git status --short
git log --oneline -3
```

Oczekiwany wynik: brak zmienionych plików aplikacji i łatwy do zidentyfikowania bieżący commit bazowy.

### Zadanie 2: Zbudować i przetestować czysty model dwóch układów

**Pliki:**
- Utwórz: `earn/earn-layout-editor.js`
- Utwórz: `tests/earn-layout-editor.test.js`

**Interfejs:**
- Eksportuje `VERSION`, `SPACER`, `SCHEMAS`, `STORAGE_KEY`, `EXPORT_NAME`.
- Eksportuje `createDefaultLayout(name)`, `normalizeLayout(name, value)`, `reorderLayout(name, layout, movedKey, targetKey, placeAfter)`, `resizeLayout(name, layout, key, deltaPx, tableWidthPx)`, `addSpacer(name, layout)`, `removeSpacer(name, layout)`, `isLocalEditorEnabled(locationLike)` oraz `normalizeSavedLayouts(value)`.
- Układ ma postać `{ version: 1, order: string[], widths: Record<string, number> }`.

- [ ] **Krok 1: Napisać test RED dla schematów, ochrony hosta oraz zachowania szerokości**

Utwórz `tests/earn-layout-editor.test.js`:

```js
const test = require('node:test');
const assert = require('node:assert/strict');
const editor = require('../earn/earn-layout-editor.js');

const total = layout => layout.order.reduce((sum, key) => sum + layout.widths[key], 0);

test('supply and borrow defaults contain all required keys and total 100%', () => {
  assert.deepEqual(editor.createDefaultLayout('supply').order, ['token', 'price', 'supply', 'balance', 'yield', 'details']);
  assert.deepEqual(editor.createDefaultLayout('borrow').order, ['health', 'collateral', 'debt', 'pnl', 'details']);
  for (const name of ['supply', 'borrow']) {
    assert.equal(Number(total(editor.createDefaultLayout(name)).toFixed(6)), 100);
  }
});

test('editor is restricted to an explicit loopback query', () => {
  assert.equal(editor.isLocalEditorEnabled({ hostname: 'localhost', search: '?layoutEditor=1' }), true);
  assert.equal(editor.isLocalEditorEnabled({ hostname: '127.0.0.1', search: '?layoutEditor=1' }), true);
  assert.equal(editor.isLocalEditorEnabled({ hostname: 'twojekrypto.github.io', search: '?layoutEditor=1' }), false);
  assert.equal(editor.isLocalEditorEnabled({ hostname: 'localhost', search: '' }), false);
});

test('reorder and one spacer preserve the complete supply schema', () => {
  const base = editor.createDefaultLayout('supply');
  const moved = editor.reorderLayout('supply', base, 'details', 'price', false);
  assert.deepEqual(moved.order, ['token', 'details', 'price', 'supply', 'balance', 'yield']);
  const added = editor.addSpacer('supply', moved);
  assert.equal(added.order.filter(key => key === 'spacer').length, 1);
  assert.deepEqual(editor.addSpacer('supply', added), added);
  assert.equal(Number(total(added).toFixed(6)), 100);
  assert.equal(Number(total(editor.removeSpacer('supply', added)).toFixed(6)), 100);
});

test('resize uses available donor width and never crosses technical minimums', () => {
  const base = editor.createDefaultLayout('borrow');
  const widened = editor.resizeLayout('borrow', base, 'details', 500, 1100);
  assert.ok(widened.widths.details > base.widths.details);
  for (const key of widened.order) {
    assert.ok(widened.widths[key] >= editor.SCHEMAS.borrow.minimums[key] / 11);
  }
  assert.equal(Number(total(widened).toFixed(6)), 100);
});

test('saved state requires both valid table layouts', () => {
  const valid = editor.normalizeSavedLayouts({
    version: 1,
    supply: editor.createDefaultLayout('supply'),
    borrow: editor.createDefaultLayout('borrow'),
  });
  assert.ok(valid);
  assert.equal(editor.normalizeSavedLayouts({ version: 1, supply: valid.supply }), null);
});
```

- [ ] **Krok 2: Uruchomić test i potwierdzić RED**

```bash
node --test tests/earn-layout-editor.test.js
```

Oczekiwany wynik: test kończy się błędem `Cannot find module '../earn/earn-layout-editor.js'`.

- [ ] **Krok 3: Dodać minimalny model bez DOM**

Utwórz `earn/earn-layout-editor.js` jako moduł browser/CommonJS. Zdefiniuj dokładnie:

```js
const SPACER = 'spacer';
const SCHEMAS = {
  supply: {
    keys: ['token', 'price', 'supply', 'balance', 'yield', 'details'],
    widths: { token: 32, price: 10, supply: 20, balance: 16, yield: 14, details: 8 },
    minimums: { token: 170, price: 78, supply: 130, balance: 132, yield: 130, details: 80, spacer: 16 },
  },
  borrow: {
    keys: ['health', 'collateral', 'debt', 'pnl', 'details'],
    widths: { health: 20, collateral: 25, debt: 25, pnl: 18, details: 12 },
    minimums: { health: 112, collateral: 150, debt: 150, pnl: 128, details: 80, spacer: 16 },
  },
};
```

`resizeLayout` konwertuje ruch kursora na procenty, oddaje szerokość kolejnym widocznym kolumnom w kolejności po prawej, a potem od początku tabeli, i nigdy nie zabiera więcej niż `width - minimum`. `addSpacer` odbiera 4% od najszerszej kolumny; `removeSpacer` przekazuje jego szerokość najbliższej kolumnie danych. Każdy wynik jest zaokrąglony do sześciu miejsc i korygowany tak, aby suma wynosiła dokładnie 100.

- [ ] **Krok 4: Uruchomić GREEN i sprawdzić składnię**

```bash
node --test tests/earn-layout-editor.test.js
node --check earn/earn-layout-editor.js
```

Oczekiwany wynik: wszystkie pięć testów przechodzi.

- [ ] **Krok 5: Zapisać samodzielny commit modelu**

```bash
git add earn/earn-layout-editor.js tests/earn-layout-editor.test.js
git commit -m "feat: add EARN table layout model"
```

### Zadanie 3: Dodać kolumny, renderowanie i testy kontraktowe Supply/Details

**Pliki:**
- Modyfikuj: `earn/earn-core.html`
- Modyfikuj: `earn/earn-core.js`
- Modyfikuj: `dashboard-core.css`
- Utwórz: `tests/test_earn_layout_contracts.py`

**Interfejs:**
- `earn_sortAssets('price')` sortuje według `earn_getUsdPrice(asset.symbol, asset.tokenAddr, chainId)`.
- `earn_formatMarketPrice(price)` zwraca `—` dla ceny niedodatniej, cenę bez miejsc dla `>= 1000`, dwa miejsca dla `>= 1` i cztery miejsca dla niższych cen.
- Każdy element w edytowalnym wierszu ma `data-column`; szczegółowy wiersz ma `data-earn-layout-detail`.

- [ ] **Krok 1: Napisać test RED kontraktu widoku**

Utwórz `tests/test_earn_layout_contracts.py`:

```python
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

class EarnLayoutContractsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (ROOT / 'earn/earn-core.html').read_text(encoding='utf-8')
        cls.js = (ROOT / 'earn/earn-core.js').read_text(encoding='utf-8')
        cls.css = (ROOT / 'dashboard-core.css').read_text(encoding='utf-8')

    def test_supply_schema_places_price_before_supply(self):
        expected = ['token', 'price', 'supply', 'balance', 'yield', 'details']
        start = self.html.index('<table class="earn-asset-table">')
        end = self.html.index('</table>', start)
        fragment = self.html[start:end]
        positions = [fragment.index(f'data-column="{key}"') for key in expected]
        self.assertEqual(positions, sorted(positions))
        self.assertIn('>Supply<', fragment)
        self.assertNotIn('>Supply APR<', fragment)

    def test_supply_price_uses_existing_canonical_price_cache(self):
        self.assertIn("earn_sortAssets('price')", self.html)
        self.assertIn("key === 'price'", self.js)
        self.assertIn('earn_getUsdPrice(a.symbol, a.tokenAddr, cid)', self.js)
        self.assertIn('function earn_formatMarketPrice(price)', self.js)

    def test_supply_and_borrow_details_share_compact_assets_geometry(self):
        self.assertIn('.earn-row-details-button {', self.css)
        self.assertIn('max-width: 72px', self.css)
        self.assertIn('margin: 0 auto', self.css)
        self.assertIn('.earn-details-cell {', self.css)

    def test_css_uses_stable_column_keys_not_supply_nth_child_alignment(self):
        self.assertIn('[data-column="price"]', self.css)
        self.assertIn('[data-column="supply"]', self.css)
        self.assertIn('[data-column="yield"]', self.css)

if __name__ == '__main__':
    unittest.main()
```

- [ ] **Krok 2: Uruchomić test i potwierdzić RED**

```bash
python3 -m unittest tests/test_earn_layout_contracts.py -v
```

Oczekiwany wynik: brak `data-column="price"`, `Supply APR` nadal istnieje i nie ma sortowania po cenie.

- [ ] **Krok 3: Zmienić stałą strukturę tabel**

W `earn/earn-core.html` zastąp `Supply Assets` przez sześć `col` i sześć stabilnych nagłówków:

```html
<colgroup>
  <col data-column="token"><col data-column="price"><col data-column="supply">
  <col data-column="balance"><col data-column="yield"><col data-column="details">
</colgroup>
...
<th data-column="price" data-sort="price" class="earn-number-header" onclick="earn_sortAssets('price')">Price <span class="earn-sort-arrow">▼</span></th>
<th data-column="supply" data-sort="apr" class="earn-number-header" onclick="earn_sortAssets('apr')">Supply <span class="earn-sort-arrow">▼</span></th>
```

Oznacz pięć nagłówków i `col` `Borrow Positions` jako `health`, `collateral`, `debt`, `pnl`, `details`. Wszystkie istniejące szczegółowe komórki otrzymają `data-earn-layout-detail`; ich `colspan` domyślnie wynosi odpowiednio `6` i `5`.

- [ ] **Krok 4: Zmienić renderer bez zmiany źródeł danych**

W `earn/earn-core.js` dodaj:

```js
function earn_formatMarketPrice(price) {
  const value = Number(price || 0);
  if (!(value > 0)) return '—';
  if (value >= 1000) return '$' + value.toLocaleString('en-US', { maximumFractionDigits: 0 });
  if (value >= 1) return '$' + value.toFixed(2);
  return '$' + value.toFixed(4);
}
```

Dodaj gałąź sortowania:

```js
} else if (key === 'price') {
  const cid = document.getElementById('earn-chain').value;
  earn_cachedAssets.sort((a, b) => {
    const diff = earn_getUsdPrice(a.symbol, a.tokenAddr, cid) - earn_getUsdPrice(b.symbol, b.tokenAddr, cid);
    return earn_sortDesc ? -diff : diff;
  });
}
```

W wierszu Supply wstaw po tokenie:

```js
<td data-column="price" class="earn-number-cell"><span class="earn-price-value">${earn_formatMarketPrice(earn_getUsdPrice(a.symbol, a.tokenAddr, cid))}</span></td>
<td data-column="supply" class="earn-number-cell">${earnSupplyCellHtml}</td>
```

Następnie oznacz istniejące saldo, yield i Details przez `data-column="balance"`, `data-column="yield"`, `data-column="details"`. Zmień nazwę lokalnej zmiennej `earnAprCellHtml` na `earnSupplyCellHtml`, zachowując wszystkie istniejące wyłączenia źródeł i APR/APY. Wszystkie wiersze Borrow otrzymują stabilne klucze w tej samej kolejności.

- [ ] **Krok 5: Dopasować CSS do Dolomite Assets i usunąć zależność od pozycji**

W `dashboard-core.css` zastąp selektory wyrównania Supply oparte o `nth-child` przez:

```css
.earn-asset-table [data-column="price"],
.earn-asset-table [data-column="supply"],
.earn-asset-table [data-column="balance"],
.earn-asset-table [data-column="yield"],
.earn-lending-table [data-column="pnl"] { text-align:right; }

.earn-price-value { color:var(--text-primary); font-family:'JetBrains Mono', monospace; }
.earn-supply-cell { display:flex; flex-direction:column; align-items:flex-end; gap:6px; min-width:0; line-height:1.1; }
.earn-supply-cell .assets-apy-breakdown { align-items:flex-end; }
.earn-asset-table tbody td { overflow:hidden; }
.earn-asset-table tbody td > * { max-width:100%; }
```

Zmień `.earn-row-details-button` na geometrię `Dolomite Assets`: `height:24px`, `width:100%`, `max-width:72px`, `min-width:0`, `padding:0 6px`, `gap:3px`, `overflow:hidden`; ustaw `.earn-details-cell` na `text-align:center` i przyciskowi `margin:0 auto`. Stan otwarcia zachowuje istniejący złoty kolor i obrót chevrona. Przepisz `no-yield` na selektory `[data-column="yield"]`; w trybie lokalnego edytora nie ukrywaj schematu yield, tylko pokaż istniejące `—`.

- [ ] **Krok 6: Uruchomić GREEN i istniejące kontrakty EARN**

```bash
python3 -m unittest tests/test_earn_layout_contracts.py -v
python3 -m unittest tests/test_earn_dashboard_contracts.py tests/test_emode_ux_contracts.py -v
node --check earn/earn-core.js
```

Oczekiwany wynik: wszystkie testy przechodzą, a `earn-core.js` nie zgłasza błędów składni.

- [ ] **Krok 7: Zapisać samodzielny commit widoku**

```bash
git add earn/earn-core.html earn/earn-core.js dashboard-core.css tests/test_earn_layout_contracts.py
git commit -m "feat: align EARN supply columns and details buttons"
```

### Zadanie 4: Podłączyć lokalny edytor do obu prawdziwych tabel

**Pliki:**
- Modyfikuj: `earn/earn-core.html`
- Modyfikuj: `earn/earn-layout-editor.js`
- Utwórz: `earn/earn-layout-editor.css`
- Modyfikuj: `tests/earn-layout-editor.test.js`
- Modyfikuj: `tests/test_earn_layout_contracts.py`

**Interfejs:**
- `window.EarnLayoutEditor.initEarnLayoutEditor()` inicjalizuje DOM wyłącznie po pozytywnej bramce host/query.
- `applyLayout('supply', table, layout)` i `applyLayout('borrow', table, layout)` aktualizują `colgroup`, nagłówki, aktualnie wyrenderowane wiersze i `colspan` szczegółów.
- Eksport ma formę `{ version: 1, supply: Layout, borrow: Layout }` i nazwę `earn-layout-draft.json`.

- [ ] **Krok 1: Rozszerzyć test RED o kontrakt bramki oraz dwa toolbary**

Dodaj do `tests/earn-layout-editor.test.js`:

```js
const fs = require('node:fs');
const core = fs.readFileSync('earn/earn-core.html', 'utf8');
const source = fs.readFileSync('earn/earn-layout-editor.js', 'utf8');

test('core loads the editor only behind loopback and query checks', () => {
  assert.match(core, /hostname === 'localhost'/);
  assert.match(core, /new URLSearchParams\(window\.location\.search\)\.get\('layoutEditor'\) === '1'/);
  assert.match(core, /earn-layout-editor\.js/);
  assert.match(core, /earn-layout-editor\.css/);
});

test('DOM adapter registers both table schemas and re-applies after DOM mutations', () => {
  assert.match(source, /earn-supply-section/);
  assert.match(source, /earn-lending-section/);
  assert.match(source, /new MutationObserver/);
  assert.match(source, /earn-layout-draft\.json/);
});
```

- [ ] **Krok 2: Uruchomić test i potwierdzić RED**

```bash
node --test tests/earn-layout-editor.test.js
```

Oczekiwany wynik: brak loadera i adaptera DOM w nowym modelu.

- [ ] **Krok 3: Dodać loader wyłącznie dla lokalnego hosta**

Na końcu `earn/earn-core.html`, po istniejącym `earn/earn-core.js`, dodaj dokładnie jeden skrypt:

```html
<script>
(() => {
  const loopback = new Set(['localhost', '127.0.0.1', '::1']);
  if (!loopback.has(window.location.hostname)) return;
  if (new URLSearchParams(window.location.search).get('layoutEditor') !== '1') return;
  const css = document.createElement('link');
  css.rel = 'stylesheet';
  css.href = 'earn/earn-layout-editor.css?v=20260721-1';
  document.head.append(css);
  const script = document.createElement('script');
  script.src = 'earn/earn-layout-editor.js?v=20260721-1';
  document.body.append(script);
})();
</script>
```

To sprawia, że publiczny host nie pobiera edytora i nie ma lokalnego stanu.

- [ ] **Krok 4: Dodać adapter DOM i toolbar**

W `earn/earn-layout-editor.js` dodaj funkcje `directColumn`, `reorderColumns`, `ensureSpacer`, `applyLayout`, `decorateHeaders`, `startColumnDrag`, `startResize`, `createToolbar`, `persistLayouts`, `downloadLayouts`, `scheduleReapply` i `initEarnLayoutEditor`.

`applyLayout` musi wykonać następujące operacje w tej kolejności:

```js
const valid = normalizeLayout(name, layout);
if (name === 'supply') table.classList.remove('no-yield');
ensureSpacer(table, valid.order.includes(SPACER));
valid.order.forEach(key => {
  const col = directColumn(table.querySelector('colgroup'), key);
  if (col) col.style.width = `${valid.widths[key]}%`;
});
reorderColumns(table.querySelector('colgroup'), valid.order);
reorderColumns(table.tHead.rows[0], valid.order);
table.querySelectorAll('tbody tr:not([data-earn-layout-detail])').forEach(row => reorderColumns(row, valid.order));
table.querySelectorAll('[data-earn-layout-detail]').forEach(cell => {
  cell.colSpan = valid.order.length;
});
```

Wstaw toolbar nad każdą sekcją tabeli, a nie do nagłówka danych: host `earn-supply-section` otrzymuje `data-earn-layout-toolbar="supply"`, `earn-lending-section` otrzymuje `data-earn-layout-toolbar="borrow"`. Przyciski mają wyłącznie ikony oraz `title`/`aria-label`: `Add blank spacer`, `Remove blank spacer`, `Reset layout`, `Save both layouts`.

- [ ] **Krok 5: Dodać lokalne style bez naruszania widoku live**

W `earn/earn-layout-editor.css` zastosuj:

```css
.earn-layout-editor-toolbar{display:inline-flex;align-items:center;gap:4px;padding:3px;border:1px solid rgba(201,162,39,.22);border-radius:8px;background:rgba(12,12,14,.92)}
.earn-layout-editor-header{position:relative;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;padding-left:25px!important;padding-right:15px!important}
.earn-layout-drag-handle{position:absolute;left:4px;top:50%;width:17px;height:20px;transform:translateY(-50%);cursor:grab}
.earn-layout-resize-handle{position:absolute;top:0;right:0;width:8px;height:100%;cursor:col-resize;touch-action:none}
.earn-layout-spacer{min-width:16px!important;background:repeating-linear-gradient(135deg,rgba(201,162,39,.07) 0 4px,transparent 4px 8px)!important;border-left:1px dashed rgba(201,162,39,.24)!important;border-right:1px dashed rgba(201,162,39,.18)!important}
@media (max-width:700px){.earn-layout-editor-toolbar,.earn-layout-drag-handle,.earn-layout-resize-handle{display:none!important}}
```

Dodaj ochronę zawartości dla edycji: `.earn-layout-editor-active td{overflow:hidden}` i `.earn-layout-editor-active td>*{max-width:100%}`. `applyLayout('supply', ...)` usuwa wyłącznie lokalnie klasę `no-yield`, aby kolumna `yield` miała zawsze stabilne miejsce do ustawienia; po wejściu bez query publiczna strona zachowuje dotychczasowe ukrywanie kolumny. Nie dodawaj tych klas ani stylów do publicznej strony.

- [ ] **Krok 6: Uruchomić GREEN oraz wszystkie celowane testy**

```bash
node --test tests/earn-layout-editor.test.js
python3 -m unittest tests/test_earn_layout_contracts.py tests/test_earn_dashboard_contracts.py tests/test_emode_ux_contracts.py -v
node --check earn/earn-layout-editor.js
```

Oczekiwany wynik: testy modelu, kontraktów i istniejącego EARN przechodzą; składnia obu plików JavaScript jest poprawna.

- [ ] **Krok 7: Zapisać samodzielny commit lokalnego edytora**

```bash
git add earn/earn-core.html earn/earn-layout-editor.js earn/earn-layout-editor.css tests/earn-layout-editor.test.js tests/test_earn_layout_contracts.py
git commit -m "feat: add local EARN table layout editor"
```

### Zadanie 5: Zweryfikować lokalny widok, interakcje i brak edytora poza trybem lokalnym

**Pliki:**
- Sprawdzenie: `earn/earn-core.html`, `earn/earn-core.js`, `dashboard-core.css`, `earn/earn-layout-editor.js`, `earn/earn-layout-editor.css`.

**Interfejs:**
- Lokalna strona: `http://localhost:4177/earn/?layoutEditor=1`.
- Adres regresyjny EARN: `0xffe4e3986d18333402564ea64f3a83fcc1907b52` z `config/earn_canonical_priority_addresses.txt`.

- [ ] **Krok 1: Uruchomić lokalny serwer na wolnym porcie**

```bash
python3 -m http.server 4177 --bind 127.0.0.1
```

Oczekiwany wynik: `http://localhost:4177/earn/?layoutEditor=1` zwraca `200` i tytuł `Dolomite · Earn`.

- [ ] **Krok 2: Sprawdzić bramkę edytora**

Otwórz kolejno:

```text
http://localhost:4177/earn/?layoutEditor=1
http://localhost:4177/earn/
https://twojekrypto.github.io/Dolomite-dashboard/earn/?layoutEditor=1
```

Oczekiwany wynik: toolbar, uchwyty i spacer są dostępne wyłącznie na pierwszym adresie. Drugi i trzeci nie mają elementów `.earn-layout-editor-toolbar`, `.earn-layout-drag-handle`, `.earn-layout-resize-handle` ani `.earn-layout-spacer`.

- [ ] **Krok 3: Sprawdzić dane oraz oba układy na rzeczywistym lookupie**

W lokalnym EARN wybierz właściwy chain dla adresu `0xffe4e3986d18333402564ea64f3a83fcc1907b52`, wklej adres i uruchom wyszukiwanie. Potwierdź w DOM oraz wizualnie:

```text
Supply Assets: Token | Price | Supply | Current Balance | Total Yield Earned | Details
Borrow Positions: Health Factor | Collateral | Debt | Net P&L | Details
```

Price i Supply są wyrównane do prawej, Supply zachowuje APR/APY i rozbicie, a przyciski Details są wyśrodkowane i w pełni widoczne.

- [ ] **Krok 4: Sprawdzić interakcje edytora**

W obu tabelach wykonaj i potwierdź po każdej czynności:

```text
1. Przeciągnij Price za Supply, a następnie przywróć Price przed Supply.
2. Poszerz Details do widocznego maksimum i zmniejsz je; żaden tekst nie może wejść w sąsiednią kolumnę.
3. Dodaj spacer, przenieś go i usuń go.
4. Kliknij sortowanie Price oraz Health Factor; po ponownym renderze układ ma pozostać bez zmian.
5. Zmień APR/APY i Hide Dust; układ ma pozostać bez zmian.
6. Rozwiń Details w Supply i Borrow; szczegółowy wiersz ma wypełniać dokładnie wszystkie aktualne kolumny.
7. Odśwież stronę; localStorage ma odtworzyć układ.
8. Kliknij Save both layouts; przeglądarka pobiera earn-layout-draft.json.
```

- [ ] **Krok 5: Wykonać końcowe przeglądy**

```bash
git diff --check
node --test tests/earn-layout-editor.test.js
python3 -m unittest tests/test_earn_layout_contracts.py tests/test_earn_dashboard_contracts.py tests/test_emode_ux_contracts.py -v
node --check earn/earn-core.js
node --check earn/earn-layout-editor.js
```

Przegląd poprawności: `Price` używa wyłącznie `earn_getUsdPrice`; wszystkie wymagane klucze są obecne; publiczna bramka jest negatywna; wszystkie `colspan` są aktualne.

Przegląd utrzymywalności i bezpieczeństwa: brak sekretów, brak nowej zależności, brak zapytań sieciowych dla edytora, brak globalnych zmian w innych tabelach, brak edytora na Pages.

- [ ] **Krok 6: Pozostawić zmiany lokalne do akceptacji układu**

```bash
git status --short
```

Oczekiwany wynik: lokalny edytor i zmiany wyglądu są gotowe do przeglądu, lecz nie są wypchnięte na Pages. Dopiero po otrzymaniu `earn-layout-draft.json` lub wyraźnej akceptacji zapisanych ustawień należy utworzyć osobny commit wdrożeniowy usuwający loader edytora i wpisujący końcowe szerokości.

## Przegląd Planu

- Pokrycie specyfikacji: Zadanie 2 obejmuje dwa modele, kolejność, szerokości, spacer, walidację i lokalną bramkę; Zadanie 3 obejmuje Price, Supply i Details; Zadanie 4 obejmuje DOM, persistence, eksport i brak pobierania na publicznym hoście; Zadanie 5 obejmuje realne sprawdzenie układu i brak publicznej dostępności.
- Brak placeholderów: wszystkie tworzone pliki, nazwy funkcji, klucze kolumn, polecenia i zachowania testowe są wskazane bez pozostawiania decyzji implementacyjnych na później.
- Spójność interfejsów: oba testy używają `supply` i `borrow`, identycznych `SCHEMAS`, formatu eksportu `{ version, supply, borrow }` oraz tego samego gate `isLocalEditorEnabled`.
