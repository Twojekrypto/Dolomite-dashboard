# DOLO Holder Distribution Layout Editor - Plan Implementacji

> **Dla agentów wykonawczych:** WYMAGANY SUB-SKILL: użyj `superpowers:subagent-driven-development` (zalecane) albo `superpowers:executing-plans`. Kroki mają checkboxy do śledzenia postępu.

**Cel:** Dodać lokalny, excelowy edytor układu dla DOLO Holder Distribution oraz jego tabeli Details, a w Details wdrożyć wyszukiwanie portfeli i kolumnę Chain zamiast rankingu.

**Architektura:** Logikę deterministycznego układu umieścić w lokalnym module `dolo-holder-layout-editor.js`, a integrację DOM i styl edytora w osobnych plikach ładowanych wyłącznie przy `?layoutEditor=1`. Stały interfejs Details (wyszukiwanie, Chain-first i jego style) pozostaje w `dolo-preview.html`, aby po późniejszym wdrożeniu był dostępny również bez edytora. Główna legenda dostanie niezależne komórki z trwałymi `data-column`, dzięki czemu jej grid i tabela Details korzystają z tego samego modelu kolejności/szerokości.

**Technologie:** Statyczny HTML/CSS/JavaScript, Node.js built-in test runner, `python3 -m http.server`, in-app Browser.

## Globalne Ograniczenia

- Nie pushować, nie scalać lokalnego edytora z produkcją i nie nadpisywać bieżących zmian w `dolo-preview.html` ani `tests/holder-distribution-contract.test.js`.
- Aktywacja edytora wyłącznie przez `http://localhost:<port>/dolo-preview.html?layoutEditor=1`; bez parametru nie ładować jego JS/CSS, kontrolek ani danych `localStorage` edytora.
- Zachować aktualny zakres market holders, logikę chartu, brush, veDOLO, lazy wallet history, akcje copy/explorer i mobilny UX.
- Dopuszczać dokładnie jeden opcjonalny `spacer` per układ; szerokości są dodatnie i łącznie zawsze wynoszą `100%`.
- Zmiana szerokości jednej kolumny kompensuje najbliższą widoczną kolumnę i respektuje minimalne szerokości; brak nakładania treści jest warunkiem zakończenia.
- Użyć wzorca `Fresh 10K+ DOLO Wallets` dla pola wyszukiwania, `X` i chipów Chain.
- Każda zmiana struktury kolumn wymaga audytu selektorów `nth-child`.

---

### Zadanie 1: Model dwóch układów i jego testy

**Pliki:**
- Utwórz: `dolo-holder-layout-editor.js`
- Utwórz: `tests/dolo-holder-layout-editor.test.js`

**Interfejsy:**
- Eksportuje `SCHEMAS`, `createDefaultLayout(name)`, `normalizeLayout(name, value)`, `reorderLayout(name, layout, movedKey, targetKey, placeAfter)`, `resizeLayout(name, layout, key, deltaPx, containerWidthPx)`, `addSpacer(name, layout)`, `removeSpacer(name, layout)`.
- `SCHEMAS.distribution.keys` to `['group','balance','wallets','change','details']`.
- `SCHEMAS.details.keys` to `['chain','address','dolo','change']`.
- Layout ma postać `{version:1, order:string[], widths:Record<string,number>}`.

- [ ] **Krok 1: Napisać testy RED dla domyślnych układów i walidacji**

```js
const test = require('node:test');
const assert = require('node:assert/strict');
const editor = require('../dolo-holder-layout-editor.js');

const total = layout => layout.order.reduce((sum, key) => sum + layout.widths[key], 0);

test('distribution and details layouts contain every required key and total 100%', () => {
  for (const name of ['distribution', 'details']) {
    const layout = editor.createDefaultLayout(name);
    assert.deepEqual(layout.order, editor.SCHEMAS[name].keys);
    assert.equal(Number(total(layout).toFixed(6)), 100);
  }
});

test('normalization rejects a missing key, duplicate key, unknown key, second spacer, and non-100% widths', () => {
  const layout = editor.createDefaultLayout('details');
  assert.equal(editor.normalizeLayout('details', {...layout, order:['chain','chain','dolo','change']}), null);
  assert.equal(editor.normalizeLayout('details', {...layout, order:['chain','address','dolo','change','unknown'], widths:{...layout.widths, unknown:1}}), null);
  assert.equal(editor.normalizeLayout('details', {...layout, order:['chain','address','dolo','change','spacer','spacer'], widths:{...layout.widths, spacer:4}}), null);
  assert.equal(editor.normalizeLayout('details', {...layout, widths:{...layout.widths, chain:30}}), null);
});
```

- [ ] **Krok 2: Uruchomić RED**

```bash
node --test tests/dolo-holder-layout-editor.test.js
```

Oczekiwany rezultat: test nie znajduje modułu `dolo-holder-layout-editor.js`.

- [ ] **Krok 3: Dopisać minimalny moduł UMD z dwoma schematami**

```js
const SCHEMAS = {
  distribution: {
    keys:['group','balance','wallets','change','details'],
    widths:{group:32,balance:27,wallets:13,change:19,details:9},
    minimums:{group:170,balance:150,wallets:88,change:112,details:76,spacer:20},
  },
  details: {
    keys:['chain','address','dolo','change'],
    widths:{chain:20,address:42,dolo:18,change:20},
    minimums:{chain:140,address:220,dolo:120,change:136,spacer:20},
  },
};
```

`normalizeLayout` odrzuca błędne dane, a `resizeLayout` używa najbliższego prawego sąsiada albo lewego na końcu. `addSpacer` odejmuje 4% od najszerszej kolumny, `removeSpacer` oddaje szerokość następnej widocznej kolumnie.

- [ ] **Krok 4: Dodać RED/GREEN dla reorder, resize i spacer**

```js
test('resize preserves total width and clamps both columns at their minima', () => {
  const base = editor.createDefaultLayout('details');
  const widened = editor.resizeLayout('details', base, 'chain', 600, 1000);
  assert.ok(widened.widths.address >= editor.SCHEMAS.details.minimums.address / 10);
  assert.equal(Number(total(widened).toFixed(6)), 100);
});

test('one spacer can move and is removed without losing width', () => {
  const base = editor.createDefaultLayout('distribution');
  const added = editor.addSpacer('distribution', base);
  const moved = editor.reorderLayout('distribution', added, 'spacer', 'change', false);
  assert.equal(moved.order.filter(key => key === 'spacer').length, 1);
  assert.equal(Number(total(editor.removeSpacer('distribution', moved)).toFixed(6)), 100);
});
```

- [ ] **Krok 5: Zweryfikować GREEN i składnię**

```bash
node --test tests/dolo-holder-layout-editor.test.js
node --check dolo-holder-layout-editor.js
```

Oczekiwany rezultat: wszystkie testy przechodzą, moduł nie ma błędów składni.

### Zadanie 2: Ustabilizować strukturę Holder Distribution i Details

**Pliki:**
- Zmień: `dolo-preview.html:312-388,2556-2815,3968-4085`
- Zmień: `tests/holder-distribution-contract.test.js`

**Interfejsy:**
- Legenda renderuje pięć komórek `data-column`: `group`, `balance`, `wallets`, `change`, `details`.
- `walletDrilldownPanelHtml(options)` przyjmuje dodatkowo `searchKey` i renderuje tabelę `data-holder-details-table`.
- Każdy wallet row ma `data-wallet-search` oraz komórki `data-column="chain|address|dolo|change"`.

- [ ] **Krok 1: Napisać kontrakty RED dla trwałego UX Details**

```js
test('holder Details replaces rank with the Fresh-style Chain column and wallet search', () => {
  assert.match(preview, /data-holder-details-search=/);
  assert.match(preview, /data-column="chain">\$\{holderWalletChainHtml\(row\)\}<\/td>/);
  assert.match(preview, /data-column="address">\$\{holderWalletAddressCell\(row\)\}<\/td>/);
  assert.doesNotMatch(preview, /<th>#<\/th>/);
  assert.match(preview, /fresh-chain-chip/);
});

test('distribution legend has independently addressable layout columns', () => {
  for (const key of ['group','balance','wallets','change','details']) {
    assert.match(preview, new RegExp(`data-column="${key}"`));
  }
});
```

- [ ] **Krok 2: Uruchomić RED**

```bash
node --test tests/holder-distribution-contract.test.js
```

Oczekiwany rezultat: nowe asercje nie znajdują wymaganych elementów.

- [ ] **Krok 3: Przekształcić legendę w niezależne komórki gridu**

W `renderHolderDistributionChart` zastąpić zbiorczy `.holder-legend-pin` pięcioma wizualnymi komórkami. Cztery pierwsze dostają ten sam `data-pin-key` i zachowują kliknięcie pinujące serię; `Details` pozostaje osobnym przyciskiem. Dodać bazowy grid z zapisanym w CSS prawym blokiem metryk, zachowując istniejące mobilne przejście do jednej kolumny.

```html
<button class="holder-legend-cell holder-legend-main" data-column="group" data-pin-key="...">...</button>
<button class="holder-legend-cell holder-legend-metric primary" data-column="balance" data-pin-key="...">...</button>
<button class="holder-legend-cell holder-legend-metric" data-column="wallets" data-pin-key="...">...</button>
<button class="holder-legend-cell holder-legend-metric delta" data-column="change" data-pin-key="...">...</button>
<button class="holder-details-btn" data-column="details" data-details-key="...">...</button>
```

- [ ] **Krok 4: Przebudować Details na Chain-first i dodać wyszukiwanie**

W `walletDrilldownPanelHtml` dodać toolbar z `label.search`, ikoną lupy i `button.search-clear`; usunąć `col`/nagłówek/cell rankingu. `holderWalletChainHtml` ma zwracać `freshChainCell({chains})`, aby bezpośrednio używać chipsów Fresh Wallets. Każdy rząd otrzymuje znormalizowany string adresu i etykiety w `data-wallet-search`.

```html
<label class="search holder-details-search">
  <svg ...></svg>
  <input data-holder-details-search="bucket-key" placeholder="Search wallet..." autocomplete="off">
  <button class="search-clear" type="button" aria-label="Clear search">...</button>
</label>
```

- [ ] **Krok 5: Zbindować filtrowanie bez ponownego pobierania historii**

`bindHolderWalletPanel(panel)` wyszukuje po `data-wallet-search`, ukrywa niepasujące rows, aktualizuje dedykowany empty state oraz przełącza klasę `has-value`. Zamknięcie danego `Details` usuwa jego wpis z `holderDetailsSearch`; lokalna edycja nie wymusza pobierania JSON ponownie.

- [ ] **Krok 6: Uruchomić kontrakty GREEN i audyt `nth-child`**

```bash
node --test tests/holder-distribution-contract.test.js
rg -n "holder-wallet-table.*nth-child|holder-legend.*nth-child" dolo-preview.html
```

Oczekiwany rezultat: testy przechodzą, a nie pozostaje selektor numerowany zależny od usuniętego rankingu.

### Zadanie 3: Lokalny adapter DOM edytora

**Pliki:**
- Rozszerz: `dolo-holder-layout-editor.js`
- Utwórz: `dolo-holder-layout-editor.css`
- Zmień: `dolo-preview.html:przed-</body>,2556-2815,3968-4085`
- Zmień: `tests/holder-distribution-contract.test.js`

**Interfejsy:**
- `initDoloHolderLayoutEditor()` uruchamia się wyłącznie przy `layoutEditor=1`.
- `applyDistributionLayout(root, layout)` ustawia grid i kolejność komórek.
- `applyDetailsLayout(table, layout)` zmienia `<colgroup>`, header i wszystkie `td[data-column]`.
- `window.DoloHolderLayoutEditor.reapply()` jest wywoływane po każdym renderze legendy i Details.

- [ ] **Krok 1: Napisać RED dla bramki edytora i zapisu obu layoutów**

```js
test('production does not statically load the local-only editor', () => {
  assert.doesNotMatch(preview, /<script[^>]+src="dolo-holder-layout-editor\.js/);
  assert.match(preview, /layoutEditor.*dolo-holder-layout-editor\.js/);
});

test('editor contract exposes the two independent saved layouts', () => {
  assert.match(editorSource, /holderDistribution/);
  assert.match(editorSource, /holderDetails/);
  assert.match(editorSource, /dolomite:dolo-holder-layout-editor:v1/);
});
```

- [ ] **Krok 2: Uruchomić RED**

```bash
node --test tests/holder-distribution-contract.test.js tests/dolo-holder-layout-editor.test.js
```

Oczekiwany rezultat: brak dynamicznego loadera oraz adaptera DOM.

- [ ] **Krok 3: Dodać dynamiczny loader tylko dla trybu lokalnego**

Przed `</body>` dodać mały inline loader, który przy `layoutEditor=1` doczepia CSS i JS. Produkcyjny URL nie tworzy elementów, nie czyta `localStorage` i nie pobiera plików lokalnego edytora.

```js
if(new URLSearchParams(location.search).get('layoutEditor') === '1'){
  const css = document.createElement('link');
  css.rel = 'stylesheet'; css.href = 'dolo-holder-layout-editor.css?v=20260717-layout-lab-1';
  document.head.append(css);
  const script = document.createElement('script');
  script.src = 'dolo-holder-layout-editor.js?v=20260717-layout-lab-1';
  document.body.append(script);
}
```

- [ ] **Krok 4: Zaimplementować adapter, toolbar i eksport**

Adapter tworzy dwa toolbary z `Add spacer`/`Remove spacer`, `Reset` i `Save layout`, uchwytami drag i resize. W trybie tabeli tworzy/usuwa komórki spacerów na wszystkich renderowanych rows i aktualizuje `colspan` pustych stanów. W trybie gridu ustawia `gridTemplateColumns` oraz `order` komórek. Każda prawidłowa zmiana zapisuje oba layouty pod `dolomite:dolo-holder-layout-editor:v1`, a zapis pobiera:

```json
{
  "version": 1,
  "holderDistribution": {"version": 1, "order": [], "widths": {}},
  "holderDetails": {"version": 1, "order": [], "widths": {}}
}
```

- [ ] **Krok 5: Podpiąć reapply po każdym dynamicznym renderze**

Po `legend.innerHTML`, po stworzeniu `walletDrilldownPanelHtml`, po filtrowaniu Details i po zakończeniu lazy `ensureHolderWalletHistory` wywołać opcjonalne `window.DoloHolderLayoutEditor?.reapply()`. Błąd edytora nie może przerwać bazowego renderu.

- [ ] **Krok 6: Uruchomić testy GREEN i sprawdzenie składni**

```bash
node --test tests/dolo-holder-layout-editor.test.js tests/holder-distribution-contract.test.js
node --check dolo-holder-layout-editor.js
node -e 'const fs=require("fs"); const h=fs.readFileSync("dolo-preview.html","utf8"); [...h.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(m=>m[1]).filter(Boolean).forEach(s=>new Function(s)); console.log("Inline scripts parsed.");'
git diff --check
```

Oczekiwany rezultat: zielone testy, poprawna składnia i brak whitespace errors.

### Zadanie 4: Weryfikacja w lokalnym interfejsie

**Pliki:**
- Zweryfikuj: `dolo-preview.html`, `dolo-holder-layout-editor.js`, `dolo-holder-layout-editor.css`

- [ ] **Krok 1: Uruchomić lokalny serwer**

```bash
python3 -m http.server 4175
```

- [ ] **Krok 2: Sprawdzić widok produkcyjny bez edytora**

Otworzyć `http://localhost:4175/dolo-preview.html`, rozwinąć Details i potwierdzić wyszukiwanie, przycisk `X`, Chain jako pierwszą kolumnę oraz brak kontrolek edycji.

- [ ] **Krok 3: Sprawdzić lokalny edytor na prawdziwych danych**

Otworzyć `http://localhost:4175/dolo-preview.html?layoutEditor=1`, rozwinąć Details. Dla obu widoków przeciągnąć kolumnę, zmienić jej szerokość, dodać/usunąć spacer, odświeżyć stronę, zapisać JSON oraz potwierdzić brak kolizji tekstu i sumę szerokości `100%`.

- [ ] **Krok 4: Sprawdzić mobilne ograniczenia i końcowy stan gita**

Potwierdzić, że legenda przechodzi do obecnego układu mobilnego, Details nadal ma poziomy scroll, i że testy oraz `git diff --check` są zielone. Pozostawić wszystkie zmiany lokalnie, bez commitu funkcjonalnego i bez pushu, aby użytkownik mógł ustawić końcowy układ.

## Kontrola Planu

- Pokrycie specyfikacji: oba lokalne edytory, jeden spacer na układ, zapis JSON, trwałe wyszukiwanie, Chain-first, walidacja, brak overlapów, production gating i weryfikacja browserowa mają osobne kroki.
- Kompletność: brak tymczasowych znaczników i nieokreślonych kroków implementacyjnych.
- Spójność: nazwy kluczy, API modułu, `localStorage`, format eksportu i punkty ponownego zastosowania układu są jednakowe w całym planie.
