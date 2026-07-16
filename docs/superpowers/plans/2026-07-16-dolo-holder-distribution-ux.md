# DOLO Holder Distribution UX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (\`- [ ]\`) syntax for tracking.

**Goal:** Uczytelnić kontekst wykresu DOLO Holder Distribution, umożliwić porównanie względnej zmiany bucketów i przypinanie wybranej serii bez zmiany danych źródłowych.

**Architecture:** Całość pozostaje w \`dolo-preview.html\`: lokalny stan renderera określa aktywną metrykę i przypięty bucket. Wykres przekształca istniejące obserwacje DOLO na procentową zmianę wyłącznie w pamięci przeglądarki, względem pierwszego punktu aktualnego brush window. Legenda i linie SVG korzystają z jednego helpera przypinania, aby nie rozjechać stanu hover, szczegółów i klawiatury.

**Tech Stack:** Statyczny HTML/CSS/JavaScript, Node.js built-in test runner, Python \`http.server\`, in-app browser.

## Global Constraints

- Nie zmieniać bucketów, klasyfikacji CEX/protokół/alokacje, JSON-ów historycznych ani workflow danych.
- \`Balance\` jest stanem domyślnym i zachowuje obecne wartości DOLO oraz wypełnienie pod przypiętą serią.
- \`Change %\` liczyć od pierwszego punktu aktualnego brush window; baseline \`0\` nie może prowadzić do dzielenia przez zero ani sztucznego procentu.
- Wartości bezwzględne pozostają dostępne w tooltipie i legendzie w obu trybach.
- Kontrolki są prawdziwymi przyciskami z \`aria-pressed\`; legenda i linie SVG obsługują kliknięcie oraz klawiaturę.
- Zachować graphite/gold UX, stabilne wymiary i brak nakładania się tekstu na desktopie oraz mobile.
- Po zmianie kolumn/markup tabeli audytować selektory \`nth-child\`; w tym zakresie nie zmieniamy liczby kolumn tabeli adresów.

---

### Task 1: Kontrakt UI, kontrolki i opis metodologii

**Files:**
- Create: \`tests/holder-distribution-contract.test.js\`
- Modify: \`dolo-preview.html:170-365\` (style kontrolek i legendy)
- Modify: \`dolo-preview.html:1416-1443\` (nagłówek karty)
- Modify: \`dolo-preview.html:1926-1944\` (\`holderScopeHtml\`)

**Interfaces:**
- Consumes: istniejące \`holderWindowLabel(from, to, minX, maxX)\` oraz \`holderScopeHtml()\`.
- Produces: markup \`[data-holder-metric]\`, element \`#holder-metric-mode\`, klasy \`.holder-metric-mode\` oraz scope z \`.holder-source-exclusion\`.

- [ ] **Step 1: Write the failing test**

\`\`\`js
import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

const preview = fs.readFileSync("dolo-preview.html", "utf8");

test("holder distribution exposes accessible metric controls", () => {
  assert.match(preview, /id="holder-metric-mode"/);
  assert.match(preview, /data-holder-metric="balance"/);
  assert.match(preview, /data-holder-metric="changePct"/);
  assert.match(preview, /aria-pressed="true"/);
});

test("holder distribution explains scope and dynamic comparison period", () => {
  assert.match(preview, /holder-source-exclusion/);
  assert.match(preview, /holder-legend-change-head/);
});
\`\`\`

- [ ] **Step 2: Run test to verify it fails**

Run: \`node --test tests/holder-distribution-contract.test.js\`

Expected: FAIL, ponieważ markup metryki i dynamiczny nagłówek nie istnieją.

- [ ] **Step 3: Write minimal implementation**

Bezpośrednio po \`#holder-bucket-mode\` dodać drugi compact segmented control:

\`\`\`html
<div class="holder-metric-mode" id="holder-metric-mode" aria-label="Holder chart metric">
  <button type="button" class="active" data-holder-metric="balance" aria-pressed="true">Balance</button>
  <button type="button" data-holder-metric="changePct" aria-pressed="false">Change %</button>
</div>
\`\`\`

W \`holderScopeHtml()\` zwrócić \`Market wallets\`, widoczne \`CEX & allocations excluded\` oraz istniejącą ikonę \`?\` z pełnym opisem: wykluczenie CEX, adresów protokołu/kontraktów/LP i alokacji Team/Investor.

W headerze legendy zastąpić statyczny \`Change\` elementem:

\`\`\`html
<span id="holder-legend-change-head">Change · 30D</span>
\`\`\`

Dodać \`.holder-metric-mode\` i przyciski jako geometryczną kopię \`.holder-bucket-mode\`: graphite w spoczynku, gold dla \`.active\`, \`:hover\` i \`:focus-visible\`. Dla mobile użyć szerokości \`100%\` i dwóch równych kolumn.

- [ ] **Step 4: Run test to verify it passes**

Run: \`node --test tests/holder-distribution-contract.test.js\`

Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

\`\`\`bash
git add dolo-preview.html tests/holder-distribution-contract.test.js
git commit -m "Polish holder distribution controls"
\`\`\`

### Task 2: Metryka Change % i bezpieczna skala wykresu

**Files:**
- Modify: \`dolo-preview.html:1711-1714\` (stan renderera)
- Modify: \`dolo-preview.html:1900-1925\` (helpery metryki)
- Modify: \`dolo-preview.html:2475-2687\` (\`renderHolderDistributionChart\`)
- Modify: \`tests/holder-distribution-contract.test.js\`

**Interfaces:**
- Consumes: \`model.points\`, \`bucket.total\`, \`holderBrushSel\`, \`holderWindowLabel()\` i \`niceHolderChartMax()\`.
- Produces: \`holderDistributionMetric\`, \`holderMetricValue(point, bucketIndex, baselinePoint)\`, \`holderMetricScale(values)\` oraz \`holderMetricPath(series, xAt, yAt)\`.

- [ ] **Step 1: Write the failing test**

\`\`\`js
test("holder distribution contains guarded relative-change helpers", () => {
  assert.match(preview, /let holderDistributionMetric = "balance"/);
  assert.match(preview, /function holderMetricValue\(/);
  assert.match(preview, /if\(baseline <= 0\) return null/);
  assert.match(preview, /function holderMetricScale\(/);
  assert.match(preview, /function holderMetricPath\(/);
});
\`\`\`

- [ ] **Step 2: Run test to verify it fails**

Run: \`node --test tests/holder-distribution-contract.test.js\`

Expected: FAIL w teście metryki względnej.

- [ ] **Step 3: Write minimal implementation**

Po \`let holderDistributionActiveKey = "";\` dodać:

\`\`\`js
let holderDistributionMetric = "balance";

function holderMetricValue(point, bucketIndex, baselinePoint){
  const current = safeHolderNum(point?.buckets?.[bucketIndex]?.total);
  if(holderDistributionMetric === "balance") return current;
  const baseline = safeHolderNum(baselinePoint?.buckets?.[bucketIndex]?.total);
  if(baseline <= 0) return null;
  return (current / baseline - 1) * 100;
}

function holderMetricScale(values){
  if(holderDistributionMetric === "balance"){
    const max = niceHolderChartMax(Math.max(1, ...values.filter(Number.isFinite)) * 1.12);
    return {min:0, max, zero:0, label:value => fmtNum(value)};
  }
  const maxAbs = Math.max(1, ...values.filter(Number.isFinite).map(value => Math.abs(value)));
  const max = niceHolderChartMax(maxAbs * 1.12);
  return {min:-max, max, zero:0, label:value => value.toFixed(value % 1 ? 1 : 0) + "%"};
}

function holderMetricPath(series, xAt, yAt){
  let started = false;
  return series.reduce((path, point) => {
    if(!Number.isFinite(point.value)){ started = false; return path; }
    const segment = (started ? "L" : "M") + xAt(point.ts).toFixed(2) + " " + yAt(point.value).toFixed(2);
    started = true;
    return path + segment;
  }, "");
}
\`\`\`

W rendererze:
1. Ustalić \`baselinePoint = model.points[0]\`.
2. Zbudować serie jako \`{ts, value, total}\`, gdzie \`value\` pochodzi z \`holderMetricValue\`.
3. Użyć skali z \`min\`/ \`max\` i narysować linię \`0%\` w trybie \`changePct\`.
4. Obszar pod linią zachować wyłącznie dla \`balance\`; dla \`changePct\` ustawić pusty path.
5. W tooltipie \`changePct\` pokazać procent na pierwszej linii, następnie bieżące saldo DOLO i zmianę bezwzględną; baseline \`0\` ma tekst \`New / no baseline\`.
6. Ustawić \`#holder-legend-change-head\` na \`Change · \` + aktualny \`holderWindowLabel(...)\`.

- [ ] **Step 4: Connect the metric control**

\`\`\`js
document.querySelectorAll("[data-holder-metric]").forEach(button => {
  button.addEventListener("click", () => {
    const next = button.dataset.holderMetric;
    if(next !== "balance" && next !== "changePct") return;
    holderDistributionMetric = next;
    document.querySelectorAll("[data-holder-metric]").forEach(item => {
      const active = item.dataset.holderMetric === next;
      item.classList.toggle("active", active);
      item.setAttribute("aria-pressed", String(active));
    });
    renderHolderDistributionChart({skipBrush:true});
  });
});
\`\`\`

- [ ] **Step 5: Run test to verify it passes**

Run:

\`\`\`bash
node --test tests/holder-distribution-contract.test.js
node -e 'const fs=require("fs"); const html=fs.readFileSync("dolo-preview.html", "utf8"); [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].forEach(m=>new Function(m[1])); console.log("Parsed inline scripts.");'
\`\`\`

Expected: wszystkie testy PASS oraz \`Parsed inline scripts.\`.

- [ ] **Step 6: Commit**

\`\`\`bash
git add dolo-preview.html tests/holder-distribution-contract.test.js
git commit -m "Add holder distribution change view"
\`\`\`

### Task 3: Przypinanie serii, dostępność i weryfikacja przeglądarkowa

**Files:**
- Modify: \`dolo-preview.html:269-291\` (style SVG focus/pinned)
- Modify: \`dolo-preview.html:2553-2562\` (interaktywne ścieżki)
- Modify: \`dolo-preview.html:2591-2614\` (interaktywna legenda)
- Modify: \`dolo-preview.html:2620-2708\` (wspólny handler przypinania)
- Modify: \`tests/holder-distribution-contract.test.js\`

**Interfaces:**
- Consumes: \`holderDistributionActiveKey\`, \`holderWalletPanelKey\`, \`renderHolderDistributionChart()\`, \`holderMetricPath()\`.
- Produces: \`toggleHolderDistributionPin(key)\`, SVG paths z \`data-key\`, \`role="button"\`, \`tabindex="0"\` oraz legendę z \`aria-pressed\`.

- [ ] **Step 1: Write the failing test**

\`\`\`js
test("holder distribution supports pinned series from chart and legend", () => {
  assert.match(preview, /function toggleHolderDistributionPin\(key\)/);
  assert.match(preview, /role="button" tabindex="0"/);
  assert.match(preview, /aria-pressed=/);
  assert.match(preview, /addEventListener\("keydown"/);
});
\`\`\`

- [ ] **Step 2: Run test to verify it fails**

Run: \`node --test tests/holder-distribution-contract.test.js\`

Expected: FAIL w teście przypinania serii.

- [ ] **Step 3: Write minimal implementation**

W \`renderHolderDistributionChart()\` zdefiniować:

\`\`\`js
function toggleHolderDistributionPin(key){
  if(!bucketDefs.some(bucket => bucket.key === key)) return;
  holderDistributionActiveKey = holderDistributionActiveKey === key ? "" : key;
  renderHolderDistributionChart({skipBrush:true});
}
\`\`\`

Tworząc SVG \`<path>\`, dodać \`role="button"\`, \`tabindex="0"\`, \`aria-label\` i \`data-key\`. Po przypisaniu \`lines.innerHTML\` dodać:

\`\`\`js
line.addEventListener("click", () => toggleHolderDistributionPin(line.dataset.key));
line.addEventListener("keydown", event => {
  if(event.key !== "Enter" && event.key !== " ") return;
  event.preventDefault();
  toggleHolderDistributionPin(line.dataset.key);
});
\`\`\`

Nadać każdemu \`.holder-chart-legend-item\` \`role="button"\`, \`tabindex="0"\` i \`aria-pressed\`. Kliknięcie lub \`Enter\`/ \`Space\` ma wywoływać \`toggleHolderDistributionPin\`, z pominięciem eventów zaczynających się na \`.holder-details-btn\`.

Handler \`Details\` dalej rozwija panel adresów, a po otwarciu przypina ten bucket. Po zamknięciu czyści przypięcie tylko, gdy wskazuje dokładnie ten sam \`key\`.

Dodać style:

\`\`\`css
.holder-chart-series-line{cursor:pointer}
.holder-chart-series-line:focus{outline:none}
.holder-chart-series-line:focus-visible{stroke-width:3.2;filter:drop-shadow(0 0 9px rgba(201,162,39,.45))}
.holder-chart-legend-item[role="button"]{cursor:pointer}
.holder-chart-legend-item[aria-pressed="true"] .holder-legend-row{background:var(--gold-wash)}
\`\`\`

- [ ] **Step 4: Run test to verify it passes**

Run:

\`\`\`bash
node --test tests/holder-distribution-contract.test.js tests/fresh-wallets-preview-contract.test.js
git diff --check
python3 -m http.server 4175 --bind 127.0.0.1
\`\`\`

Expected: wszystkie testy PASS, brak błędów whitespace oraz działający lokalny serwer.

- [ ] **Step 5: Browser verification**

Na \`http://localhost:4175/dolo-preview.html\` potwierdzić:

1. \`Balance\` i \`Change %\` rysują trzy niepuste ścieżki dla obu grup bucketów.
2. Zmiana metryki nie resetuje brush window.
3. \`Change %\` ma linię \`0%\`, tooltip zawiera procent i saldo DOLO, a nie pokazuje \`Infinity\` lub \`NaN\`.
4. Kliknięcie linii oraz wiersza przypina/odpina bucket; \`Enter\` i \`Space\` robią to samo.
5. \`Details\` rozwija wyłącznie tabelę adresów właściwego bucketa.
6. Kontrolki i legenda mieszczą się na desktopie i mobile, a tooltip nie wychodzi poza kartę.

- [ ] **Step 6: Commit**

\`\`\`bash
git add dolo-preview.html tests/holder-distribution-contract.test.js
git commit -m "Add holder distribution series pinning"
\`\`\`

- [ ] **Step 7: Deploy and verify live**

\`\`\`bash
git fetch dolomite-dashboard master
git rebase dolomite-dashboard/master
git push dolomite-dashboard master
gh run list --repo Twojekrypto/Dolomite-dashboard --branch master --limit 6
\`\`\`

Expected: lokalny \`master\` jest wypchnięty bez utraty automatycznych danych, workflow \`Deploy GitHub Pages\` dla aktualnego SHA kończy się \`success\`, a na \`https://twojekrypto.github.io/Dolomite-dashboard/dolo/?v=<current-sha>\` działają kontrolki, ścieżki oraz przypinanie.

