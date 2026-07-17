# Neutralny stan pustej sieci Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Cel:** Zastąpić widoczne `Unknown` neutralną, dostępną kreską w kolumnie Chain, gdy wallet nie ma bieżącego salda DOLO przypisanego do aktywnej sieci.

**Architektura:** Wspólny renderer `freshChainCell()` pozostaje jedynym miejscem decydującym o wyglądzie komórki Chain. Dla modelu `unknown` zwróci osobny element prezentacyjny, natomiast modele Ethereum i Berachain zachowają obecny kod i ikony. Test kontraktowy będzie chronił tę granicę przed powrotem technicznej etykiety.

**Tech Stack:** Statyczny HTML/CSS/JavaScript, Node.js `node:test`, lokalny serwer `python3 -m http.server`.

## Global Constraints

- Nie zmieniać danych, klasyfikacji walletów ani wartości sortowania `unknown`.
- Zmiana ma obowiązywać we wspólnym widoku Chain dla Bucket wallets i Fresh 10K+ DOLO Wallets.
- W stanie pustym nie pokazywać ikon ani słowa `Unknown`.
- Element pustego stanu musi mieć `aria-label="No current chain balance"`.
- Zachować istniejący Graphite + Gold UX i nie dodawać zależności.

---

### Task 1: Neutralny renderer pustej sieci

**Files:**
- Modify: `tests/holder-distribution-contract.test.js`
- Modify: `dolo-preview.html:546-555`
- Modify: `dolo-preview.html:4569-4610`

**Interfaces:**
- Consumes: `freshChainCell(row)`, który otrzymuje `row.chains` z obiektami `{key, label}`.
- Produces: `freshChainCell()` zwracający `span.fresh-chain-chip.unknown` z neutralnym znakiem i opisem dostępności dla `key === "unknown"`.

- [ ] **Step 1: Write the failing test**

Dodaj do `tests/holder-distribution-contract.test.js` test sprawdzający renderer źródłowy:

```js
test("empty chain cells use a neutral accessible state", () => {
  const chainRenderer = preview.slice(
    preview.indexOf("function freshChainCell(row)"),
    preview.indexOf("function freshExposureHtml")
  );

  assert.match(chainRenderer, /aria-label="No current chain balance">—<\\/span>/);
  assert.doesNotMatch(chainRenderer, /escHtml\\(chain\.label \|\| "Unknown"\\)/);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
node --test tests/holder-distribution-contract.test.js
```

Expected: the new test fails because the renderer still interpolates `Unknown`.

- [ ] **Step 3: Write minimal implementation**

W `freshChainCell()` obsłuż `chain.key === "unknown"` przed ustalaniem ikony:

```js
if(chain.key === "unknown") {
  return '<span class="fresh-chain-chip unknown" aria-label="No current chain balance">—</span>';
}
```

Następnie buduj istniejący wariant ikony i etykiety tylko dla rozpoznanych sieci, używając `chain.label || chain.key` zamiast domyślnego `Unknown`. Dostosuj `.fresh-chain-chip.unknown`, aby znak był czytelny, dyskretny i nie zmieniał szerokości wiersza.

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
node --test tests/dolo-holder-layout-editor.test.js tests/holder-distribution-contract.test.js
```

Expected: all tests pass.

- [ ] **Step 5: Verify the rendered page**

Start lokalny serwer i otwórz `dolo-preview.html`. Sprawdź w narzędziach przeglądarki, że element `.fresh-chain-chip.unknown` ma widoczny znak `—`, atrybut `aria-label` i nie generuje błędów JavaScript.

- [ ] **Step 6: Commit and publish**

```bash
git add dolo-preview.html tests/holder-distribution-contract.test.js docs/superpowers/plans/2026-07-17-neutral-empty-chain.md
git commit -m "fix: show neutral empty chain state"
git push dolomite-dashboard master
```
