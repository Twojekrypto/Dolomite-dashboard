# Audyt kodu — Dolomite Dashboard (2026-06-13)

> Zakres: frontend (dolo-preview.html, dashboard-core.js/html, route-loader.js, mobile-*.js, protocol-footer.js, shared JS), server.js, pipeline Python zasilający dashboard, workflowy CI, skan sekretów całego repo.
> Punkt odniesienia: `CODE_AUDIT_2026-06-11.md` i `SECURITY_AUDIT.md` — znaleziska już naprawione NIE są powtarzane; oznaczam tylko te wciąż otwarte.
> Każde znalezisko zweryfikowane bezpośrednio w kodzie (numery linii aktualne na dziś).

---

## 🔴 NAJBARDZIEJ NIEBEZPIECZNE PROBLEMY

### 1. XSS — niedomknięta luka w dekoderze symboli tokenów (Earn) — 3. kopia bez escape

- **Najbardziej niebezpieczny problem:** Kod ma TRZY kopie dekodera `symbol()` z on-chain. Dwie są zabezpieczone (`earn_getTokenInfo` :9556 ma komentarz „XSS guard: on-chain symbol() is deployer-controlled", subgraph :15692 też). Trzecia, inline w `earn_lookup`, NIE escapuje — surowy symbol trafia do `innerHTML`.
- **Możliwe konsekwencje:** Atakujący deployuje ERC-20 z `symbol() = '<img src=x onerror=...>'`, wpłaca pył na obserwowane konto — każdy, kto wyszuka ten adres w Earn, wykonuje JS atakującego (phishing overlay, redirect). Strona nie ma CSP, więc wstrzyknięty skrypt ma pełne uprawnienia.
- **Dokładne miejsce błędu:** `dashboard-core.js:16969` i `:16978` (`symbol = String.fromCharCode(...bytes)`), `:16994` (`info = { symbol, decimals, icon }` — bez escape), render: `:18856` (`<div class="earn-token-name">${a.symbol}</div>`), `:18772`, `:18786` (`alt="${a.symbol}"` — `"` wybija się z atrybutu).
- **Zalecany sposób naprawy:** Escapować symbol w punkcie ingestii (tam gdzie powstaje `info`), identycznie jak w dwóch pozostałych kopiach. Docelowo: jedna funkcja dekodująca zamiast trzech kopii.
- **Przykład kodu po poprawkach:**
```js
// dashboard-core.js, w earn_lookup po dekodzie String.fromCharCode:
symbol = earn_escapeHtml(symbol);   // ta sama reguła co w earn_getTokenInfo:9556
info = { symbol, decimals, icon };
earn_tokenCache[key] = info;
```

### 2. XSS — nieescapowane etykiety w tabelach strony głównej

- **Problem:** `renderHolders`, `renderFreshWallets` i tabela CEX interpolują `h.label`, `row.signal.label` (pochodzące z `dolo_holders.json` / `dolo_flows.json`, w tym pola `label` i `source` z pipeline'u) wprost do `innerHTML` bez `escHtml()`. Panele drill-down NA TEJ SAMEJ STRONIE poprawnie używają `escHtml()` (:3704, :3714) — reguła jest znana, ale zastosowana w 1 z 4 ścieżek renderowania. Pipeline już ingestuje etykiety ENS-reverse (`dolo-address-labels.js:104-107`), a ENS reverse to string kontrolowany przez atakującego.
- **Możliwe konsekwencje:** Stored XSS dla każdego odwiedzającego w momencie, gdy pipeline zapisze do JSON-a dowolny string pochodzenia on-chain (nazwa ENS, etykieta tokena).
- **Dokładne miejsce błędu:** `dolo-preview.html:3973` (`${h.label}`), `:3981` (`data-full-addr="${h.addr}"`), `:4194-4196`, `:4204`, `:4493-4495`, `:4519` (`${row.signal.label}`). Brak meta CSP w całym pliku.
- **Zalecany sposób naprawy:** `escHtml()` na każdym dynamicznym stringu w trzech rendererach tabel + meta CSP jako backstop.
- **Przykład kodu po poprawkach:**
```js
const nameCell = h.label
  ? `<span class="addr-name">${escHtml(h.label)}</span>`
  : `<span class="addr-name addr-generic">${TYPE_GENERIC[h.type]}</span>`;
// ...
<td><span class="fresh-signal ${row.signal.cls}">${escHtml(row.signal.label)}</span></td>
```
```html
<meta http-equiv="Content-Security-Policy" content="script-src 'self' 'unsafe-inline'">
```

### 3. Wyścig przy zmianie łańcucha (Earn) — dane starego chaina renderowane pod nowym

- **Problem:** Mechanizm inwalidacji `earn_startLookupRun()` istnieje, ale `earnChainSelect` go NIE woła (zweryfikowane: jedyne wywołanie to `earn_lookup:16515`). Lookup w locie po zmianie chaina nadal przechodzi guard `earn_isLookupRunCurrent` i renderuje stare pozycje. Dodatkowo ≥6 asynchronicznych helperów czyta `chainId`/`addr` z DOM w trakcie biegu (`:9655, :9973, :12528, :14035, :15666, :17637`) — w tym interpoluje NIEzwalidowany, w połowie wpisany adres wprost do zapytania GraphQL (`:17664`).
- **Możliwe konsekwencje:** Pozycje i salda starego chaina wyceniane kluczami nowego chaina → błędne wartości USD i APR pokazane użytkownikowi; render wyników pod cudzym/niepełnym adresem.
- **Dokładne miejsce błędu:** `dashboard-core.js:7612-7682` (`earnChainSelect` — brak inwalidacji), `:17637` + `:17664` (re-odczyt adresu z DOM + interpolacja do GraphQL).
- **Zalecany sposób naprawy:** (a) jedna linijka w `earnChainSelect`; (b) przekazywać `{runId, chainId, addr}` złapane raz w `earn_lookup` przez `opts` zamiast czytać DOM w kontynuacjach async.
- **Przykład kodu po poprawkach:**
```js
function earnChainSelect(key) {
    earn_startLookupRun();          // inwaliduje lookup w locie
    earn_activeLookupAddr = '';
    // ... reszta bez zmian
}
// earn_fetchAndRenderLendingPositions(opts):
const addr = opts.addr;             // zamiast document.getElementById('earn-address').value
```

### 4. server.js — obejście whitelisty proxy (SSRF) + bind na 0.0.0.0 + crash jednym żądaniem

- **Problem:** (a) whitelist sprawdzany przez `startsWith` — `https://api.coingecko.com.evil.com/x` i `https://api.coingecko.com@evil.com/x` przechodzą; (b) `server.listen(PORT)` bez hosta = nasłuch na wszystkich interfejsach — każdy w LAN dostaje to proxy + odczyt plików repo; (c) `decodeURIComponent(parsed.pathname)` bez try/catch — `GET /%` rzuca `URIError` i ubija proces.
- **Możliwe konsekwencje:** Otwarte GET-proxy do dowolnego hosta dostępne z sieci lokalnej; DoS dev-servera jednym żądaniem. To dev-server (produkcja = GitHub Pages statycznie), więc severity ŚREDNI, nie krytyczny — ale to trzy realne dziury w 136 liniach.
- **Dokładne miejsce błędu:** `server.js:105` (startsWith), `:131` (listen bez hosta), `:93` (decodeURIComponent), dodatkowo `:122` (prefix-check bez separatora ścieżki), `:63-67` (drugi `writeHead` po timeout → crash).
- **Zalecany sposób naprawy i przykład kodu:**
```js
// 1. Porównanie sparsowanego originu:
let parsedTarget;
try { parsedTarget = new URL(targetUrl); } catch { /* 400 */ }
const allowed = ALLOWED_ORIGINS.includes(parsedTarget.origin);
// 2. Tylko localhost:
server.listen(PORT, '127.0.0.1', () => { ... });
// 3. Bezpieczny decode:
let pathname;
try { pathname = decodeURIComponent(parsed.pathname); }
catch { res.writeHead(400); res.end('Bad request'); return; }
// 4. Prefix z separatorem:
if (filePath !== STATIC_DIR && !filePath.startsWith(STATIC_DIR + path.sep)) { /* 403 */ }
// 5. Timeout: if (!res.headersSent) res.writeHead(504, ...);
```

### 5. „Lazy loading" 24 MB, który nie jest lazy — pobiera się przy KAŻDYM wejściu

- **Problem:** Fix z audytu 2026-06-11 (wydzielenie `dolo_holder_wallet_history.json`) jest tylko ODROCZONY, nie on-demand: `fetchHeavyJson` startuje bezwarunkowo po pierwszym paincie (czeka jedną klatkę animacji), mimo że dane są potrzebne wyłącznie po otwarciu panelu Details. Do tego `holdersIncludeVeDolo: true` domyślnie → 600 ms po boot dociągają się `vedolo_flows.json` (11 MB) + `vedolo_holders.json` (5.5 MB).
- **Możliwe konsekwencje:** ~40 MB transferu i wielosetmilisekundowy blocking `JSON.parse` na main thread przy każdej wizycie strony głównej — na mobile to realny koszt danych i jank.
- **Dokładne miejsce błędu:** `dolo-preview.html:5165-5175` (bezwarunkowy fetch po paint), `:1623` (`holdersIncludeVeDolo: true`), `:4847-4851` (auto-load po 600 ms), `:4959` (`fetchHeavyJson` czeka tylko 1 rAF).
- **Zalecany sposób naprawy:** Fetch wallet-history dopiero w handlerze kliknięcia Details (pierwsze otwarcie panelu); pliki veDOLO za realną interakcją z toggle; mały per-wallet `locked` summary w `dolo_holders.json`, żeby domyślna kolumna nie potrzebowała 16.5 MB.
- **Przykład kodu po poprawkach:**
```js
let walletHistoryPromise = null;
function ensureWalletHistory(){
  if(!walletHistoryPromise){
    walletHistoryPromise = fetchHeavyJson(walletHistoryFile).then(d => {
      DOLO_HOLDER_WALLET_HISTORY = d?.holder_wallet_history || {};
    });
  }
  return walletHistoryPromise;
}
// w handlerze przycisku Details (:2628):
detailsBtn.addEventListener("click", async () => {
  await ensureWalletHistory();
  renderHolderDistributionChart();
});
```

### 6. Fałszywe dane przykładowe pokazywane jako „Live" przy awarii fetcha

- **Problem:** W pliku produkcyjnym zaszyte są zmyślone dane (vitalik.eth „18.4M DOLO", „Binance", „Wintermute"), a badge `Live · updated 2m ago` jest hardcodowany. Gdy fetch padnie (404/sieć), błąd ląduje tylko w `console.warn` — użytkownik widzi pewny siebie, całkowicie fikcyjny dashboard.
- **Możliwe konsekwencje:** Publiczny serwis analityczny pokazuje zmyślone salda przypisane realnym podmiotom z pulsującym „Live". Ryzyko reputacyjne i integralności danych.
- **Dokładne miejsce błędu:** `dolo-preview.html:1541-1558`, `:1594-1608` (fake HOLDERS/FLOWS), `:1025` (hardcodowany „Live"), `:5215` (`if(!periodData) return;` zostawia fake FLOWS), `:5301-5303`, `:5318-5320` (błędy tylko do konsoli).
- **Zalecany sposób naprawy:** Usunąć zaszyte sample (era prototypu file:// minęła) i przy błędzie pokazywać jawny stan błędu.
- **Przykład kodu po poprawkach:**
```js
.catch(error => {
  console.warn("DOLO holders load failed", error);
  HOLDERS.length = 0; renderHolders();
  document.querySelector(".card-meta").innerHTML =
    '<span class="pulse pulse-err"></span>Dane niedostępne — spróbuj później';
});
```

### 7. mobile-polish.js — samonapędzająca się pętla MutationObserver → ciągły layout thrash

- **Problem:** `updateTables()` robi `classList.remove("mobile-scrollable")` na wszystkich shellach, po czym warunkowo `add` — a MutationObserver obserwuje `attributes: ["class"]` na całym dokumencie, więc widzi własne zapisy i planuje kolejny przebieg. `updateExpiryLabels` pisze `textContent` bezwarunkowo (mutacje childList). Każdy przebieg = `querySelectorAll("table")` + odczyty `scrollWidth/clientWidth` (forced layout).
- **Możliwe konsekwencje:** Na mobile z choć jedną przewijalną tabelą — permanentna pętla ~60 fps: jank, zużycie baterii.
- **Dokładne miejsce błędu:** `mobile-polish.js:76-89` (remove+add), `:94-103` (textContent), `:167-173` (observer na class).
- **Zalecany sposób naprawy:** Mutować tylko przy realnej zmianie.
- **Przykład kodu po poprawkach:**
```js
const need = tableWidth > shellWidth + 2;
if (shell.classList.contains("mobile-scrollable") !== need)
  shell.classList.toggle("mobile-scrollable", need);
// i:
if (label.textContent !== next) label.textContent = next;
```

### 8. Cicha, trwała utrata transferów w generate_dolo_holders.py

- **Problem:** Po wyczerpaniu retry chunk bloków jest pomijany, a `last_block` i tak awansuje i zapisuje się do stanu — transfery z pominiętego zakresu nigdy nie zostaną doliczone. Bliźniaczy kod w `generate_dolo_flows.py:326-360` ma to rozwiązane (`chunks_failed`, persystowane `skipped_ranges_{chain}`, alarm >50% porażek) — holders nie.
- **Możliwe konsekwencje:** Salda w publikowanym `dolo_holders.json` dryfują w ciszy. Mitygacja częściowa: `verify_top_balances` koryguje on-chain tylko top 200 i tylko przy odchyleniu >1% — reszta holderów dryfuje bez korekty.
- **Dokładne miejsce błędu:** `generate_dolo_holders.py:153-156` (skip bez śladu), `:380, :385, :447-452` (awans i zapis stanu mimo luk).
- **Zalecany sposób naprawy:** Przenieść wzorzec z flows: rejestrować pominięte zakresy w stanie i doskanowywać je w kolejnym runie (lub nie awansować `last_block` ponad pierwszą lukę).
- **Przykład kodu po poprawkach:**
```python
if not success:
    print(f"    ⚠️ Failed at block {current}, recording gap")
    skipped_ranges.append([current, chunk_end])   # + persist w save_state()
    current = chunk_end + 1
    continue
# w save_state(): state[f"skipped_ranges_{chain}"] = skipped_ranges
# na starcie runu: najpierw doskanuj skipped_ranges, dopiero potem nowe bloki
```

---

## 🟠 WYSOKIE / ŚREDNIE (skrót — miejsce → konsekwencja → fix)

| # | Miejsce | Problem | Konsekwencja | Fix |
|---|---|---|---|---|
| 9 | `dashboard-core.js:17989, 18555, 18572, 18586, 18612, 17519-21` | Symbole z `liquidation_risk.json`/subgraph w `innerHTML` bez escape (w TYM SAMYM template `externalRewards` SĄ escapowane :18641) | XSS tą samą klasą co #1 | `earn_escapeHtml()` na każdym `${…symbol}` lub escape przy ingestii |
| 10 | `dashboard-core.js:14391` vs `:17706` | Fetch Merkl bez AbortController (każdy inny wrapper ma: :8053, :9991, :13580, :14553) | Zawieszony api.merkl.xyz blokuje całą sekcję lending — loader nigdy nie znika | Skopiować istniejący wzorzec timeout 10 s |
| 11 | `dashboard-core.js:13904-13917, 13942-13967` | Błąd sieci cache'owany jako `null` na całą sesję (nieodróżnialny od 404) | Jedna czkawka sieci = trwale „unverified" do reloadu | Cache'ować `null` tylko przy `resp.status === 404` |
| 12 | `dashboard-core.js:14591` | Pagination subgraph ucina po skip>10000 bez logu i flagi | Błędne yield bez żadnego ostrzeżenia dla adresów >11k eventów | `console.warn` + flaga `truncated` w wyniku; docelowo cursor-based (`serialId_gt`) |
| 13 | `dashboard-core.js:17750, 17796, 16737, 16744, 17001, 17017, 17035, 17053` | `parseFloat`/`Number` na `amountPar`/`wei` w ARYTMETYCE (sortowanie, dust-filter $1, sumy) — łamie regułę AGENTS.md:44; precyzyjne narzędzia istnieją obok (`earn_getTokenUsdScaled:10077`, użyte poprawnie w :18985) | Dryf alokacji/sortowania; wzorzec zaprasza do kopiowania | Użyć `earn_getTokenUsdScaled`/BigInt jak w :18985 |
| 14 | `dashboard-core.js:13092-13097` | `earn_calculateYield` (~270 linii BigInt + odczyt DOM) wołany 2× per PORÓWNANIE w komparatorze sortowania | O(n·log n) pełnych przeliczeń ledgera na klik sortowania | Precompute kluczy przed sortem (schwartzian transform) |
| 15 | `dashboard-core.js:17655-17696` | `sgPromise` (3 zapytania subgraph, Promise.all) nieobserwowany przy early return | unhandledrejection + 3 zapytania w błoto dla najczęstszego przypadku (brak pozycji) | `sgPromise.catch(()=>null)` przy utworzeniu lub lazy creation |
| 16 | `dolo-preview.html:1766-1775, 4979-4987` + dane (`dolo_flows.json` ma naiwne `2026-06-12T21:05:28.690836`, history points mają `Z`) | Timestampy bez `Z` parsowane jako czas LOKALNY widza | „Updated Xh ago" i dopasowanie punktów historii przesunięte o offset strefy widza (np. 8 h dla UTC-8) | Normalizować: dokleić `Z` gdy brak offsetu (kod w raporcie agenta) lub emitować `Z` z pipeline'u |
| 17 | `dolo-preview.html:2460-2499, 2917-2953, 3389-3411` | `innerHTML +=` w pętli w 3 rendererach wykresów — O(n²), odpalane per klatka brush-draga | ~775k re-parsowań węzłów per render przy zakresie „All"; jank brusha | Akumulować do tablicy, jedno przypisanie `join("")` |
| 18 | `dolo-preview.html:4083-4084` | `renderHolders()` przebudowuje OBA wykresy SVG przy każdym naciśnięciu klawisza w wyszukiwarce/sortowaniu/paginacji | Widocznie ciężkie pisanie w search | Usunąć wywołania wykresów z `renderHolders()` — funkcje apply-data już je wołają (:5123-5126) |
| 19 | `dolo-preview.html:5272-5287` | Awaria `vesting_investors.json` połknięta `catch(_){}` — Core Team/Investorzy klasyfikowani jako zwykłe EOA | Strukturalnie błędna analityka (zawyżone bucket'y market, puste Allocations) bez śladu; bonus: `boot():5290` serializuje wszystko ZA tym fetchem (+1 RTT na starcie) | Log + widoczna nota „labels degraded"; przenieść do `Promise.allSettled` |
| 20 | `dolo-preview.html:5094` | `data.fdv / Math.max(price,…)` gdy brak obu pól → literal „NaN" w hero stat (brak guarda w `fmtNum:1647-1653`) | „NaN" w głównej statystyce | `if(!Number.isFinite(n)) return "—";` w fmtNum + guard przed dzieleniem |
| 21 | `dolo-preview.html:2352-2418, 2813-2879, 3291-3357` | Cały subsystem brush skopiowany 3× (~130 linii każdy); ŻADNA kopia nie obsługuje touch | Brushe martwe na urządzeniach dotykowych (a strona celuje w mobile); każdy fix trzeba nanosić 3× | Jeden `createBrush({prefix,…})` na Pointer Events (`pointerdown/move/up` + `setPointerCapture`) |
| 22 | `dashboard-core.js:9805` | `e.message` (tekst z subgraph/RPC :14564, :8069) w `innerHTML` | XSS przez infrastrukturę zewnętrzną (mało prawdopodobny, ale darmowy fix) | `earn_escapeHtml(e.message)` lub `textContent` jak `earn_showError:12836` |
| 23 | `.github/workflows` — `dolo_price.json` commitowany przez `update-dolo-price.yml:45` (co 1 h) ORAZ `update-data.yml:61` (co 6 h), różne grupy concurrency, `git pull --rebase -X theirs` w 12 workflowach | Konflikt rozstrzygany na rzecz STARSZEJ ceny z długiego runu update-data | Cofnięcie świeższej ceny (samonaprawialne w ≤1 h, ale to dokładnie mechanizm z ostrzeżenia P1.6) | Usunąć `dolo_price.json` z `git add` w update-data.yml ALBO wspólna grupa `concurrency: {group: data-commit, cancel-in-progress: false}` |
| 24 | `dashboard-core.js:13474-13508` | Malformed `manifest.json` → przypisanie się udaje, każdy późniejszy `earn_buildPeriodButtons()` (np. z `earnChainSelect:7675`) rzuca uncaught TypeError | Martwe przyciski okresów po zmianie chaina | Walidacja przed przypisaniem: `if(!Array.isArray(m.dates) || !m.chains) return null;` |
| 25 | `dashboard-core.js:16824-16878` | Błąd sieci w netflow-check → twardy komunikat „No deposits found" | Mylący komunikat (definitywny) zamiast „nie udało się zweryfikować — ponów" | Flaga `netflowCheckFailed` w catch, inny komunikat |

## 🟡 NISKIE (wybór)

- `dashboard-core.js:17000` — zmyślony fallback ceny `0.01` dla nieznanych tokenów (inne ścieżki używają `0`) → fałszywe USD, niespójny dust-filter. Użyć `0` i renderować „—".
- Duplikaty w dashboard-core.js: template wiersza lending skopiowany w całości (`:17969-18148` vs `:18537-18693`, z duplikatem `FLAME_SVG` i `renderMergedTokens`); 4+ formattery USD; 3 ręczne mapy chain-id (`:12853`, `:17772`, `:14369` — Mantle/Botanix już się rozjechały). To duplikacja, która JUŻ spowodowała lukę #1 (guard naniesiony na 2 z 3 kopii).
- `dolo-preview.html:4864-4869` + `protocol-footer.js:16-21` + `dolo-address-labels.js` — lista adresów kontraktów w 3 miejscach; blok `#proto-addrs` (:4872-4912) to martwy kod (footer go usuwa).
- `dolo-preview.html:4356-4386` — `buildFreshWalletRowsFromSnapshot` nieosiągalny (dead code).
- `route-loader.js:57` — `html.replace("<head>", …)` pęknie cicho przy `<head lang=…>`; użyć `/<head[^>]*>/`.
- `index.html:25` — 700-znakowy append-only changelog jako wersja cache-bust w 12 loaderach → krótki hash/data.
- `dolo-preview.html:2658` — wykres alokacji czyta tylko stary kształt `liquid.whales/smaller`; migracja pipeline'u na `liquid.market.<view>` (który drugi reader już wspiera :2084-2089) cicho spłaszczy wykres. Reużyć `holderPrecomputedSource`.
- Pipeline: ciche `except Exception: time.sleep()` bez logowania typu wyjątku w pętlach retry (`generate_dolo_holders.py:78, 150-151, 276-277, 332-333`; `generate_dolo_flows.py:246-247, 318-319, 394-395, 2128-2129`); nieatomowy zapis stanu w holders (`:53-56` — flows ma wzorzec tmp+`os.replace` w `:225-230`); float na wei w liczbach publikowanych (`generate_dolo_holders.py:181, 255`; `generate_dolo_flows.py:412, 428, 596, 1352, 1410, 1634, 2125`; `update_data.py:244, 295, 477, 669`) — pozycja P0.2 z 2026-06-11 naprawiona dotąd tylko w fetch_liquidation_risk.py.

## ✅ CZYSTE (zweryfikowane)

- **Sekrety: ZERO** hardcodowanych kluczy/tokenów/haseł w całym repo (wzorce: alchemy/v2, quiknode, infura/v3, dkey=, api_key, ghp_, sk-, AKIA, xox, PRIVATE KEY, Bearer). Wyciek Alchemy z 2026-06-10 naprawiony (`odolo_contract_data.json` zawiera tylko hostname); `secret-guard.yml` aktywny; `rpc_client.sanitize_error` redaguje klucze z logów. Przypomnienie: stary klucz wciąż w historii gita — **rotacja w panelu Alchemy**, jeśli nie wykonana.
- Zero gołych `except:` w Pythonie (naprawione); wszystkie `requests.*` w audytowanych skryptach mają timeout; `rpc_client.py` wzorowy; `update_dolo_price.py`/`fetch_dolo_price_history.py` czyste (guardy, fallbacki, wąskie wyjątki); `update_data.py` ma dobry guard anty-regresyjny (:775-796).
- Frontend strony głównej: brak `parseFloat` w ogóle (kwoty przychodzą pre-skalowane z pipeline'u przez `safeNum`); `Promise.allSettled` zamiast all-or-nothing; `escHtml` poprawny w panelach wallet i `protocol-footer.js`; `count-up.js`, `shared-hover-tooltips.js`, `dolo-address-labels.js` czyste; `rel="noopener"` wszędzie; konwencja znaku `net_flow` sellers zweryfikowana poprawna.
- Workflowy: każdy commitujący ma już blok `concurrency` z `cancel-in-progress: false` (luka z audytu 06-11 domknięta); least-privilege permissions; brak `pull_request_target`; brak echowanych sekretów.

---

## WERDYKT PROJEKTOWY (wprost, bez uprzejmości)

**Jakość inżynierska w środku jest wysoka — architektura plików jest nie do utrzymania i już teraz aktywnie produkuje błędy.** Dowód jest w tym raporcie: XSS guard naniesiony na 2 z 3 kopii dekodera (#1), inwalidacja runId istnieje, ale zmiana chaina jej nie woła (#3), timeout-pattern jest w 5 z 6 fetch-wrapperów (#10), template lendingu w 2 kopiach, które rozjadą się przy pierwszym fixie. To nie są błędy kompetencji — to błędy KOORDYNACJI, które monolit z copy-paste-reuse generuje w nieskończoność.

Trzy strukturalne grzechy główne:

1. **dashboard-core.js: 20 906 linii, ~2 100 referencji `earn_*` w jednej globalnej przestrzeni nazw, DOM jako źródło prawdy o tożsamości lookupu.** Dopóki każdy lookup nie niesie niemutowalnego kontekstu `{runId, chainId, addr}`, każda nowa funkcja async odtworzy wyścig #3. Podział na `earn/render.js` / `earn/fetch.js` / `earn/format.js` nie wymaga bundlera — zwykła konkatenacja skryptów wystarczy, a split HTML/CSS/JS z rundy 7 już to ułatwił.
2. **dolo-preview.html: prototyp z fake danymi i „live adapter" monkey-patchujący jego funkcje współistnieją na produkcji.** Wzorzec `renderHolders = function(){ originalRenderHolders(); … }` + zaszyte sample (#6) + 3 kopie silnika brush (#21) to korzeń połowy znalezisk. Era prototypu się skończyła — sample do usunięcia, brush do ekstrakcji.
3. **`innerHTML` z escape jako opt-in.** `earn_escapeHtml` użyty 78 razy i wciąż brakuje go na najbardziej uczęszczanych interpolacjach. Jeden row-builder escapujący domyślnie (albo escape całych stringów zewnętrznych przy ingestii — wzorzec `:15692` zastosowany uniwersalnie) zamienia „znajdź każdą dziurę" w „jeden punkt kontroli".

**Kolejność napraw (koszt → zysk):**

1. #1, #2, #9, #22 — escape przy ingestii + CSP (≈20 linii, zamyka całą klasę XSS)
2. #3 — `earn_startLookupRun()` w `earnChainSelect` (1 linia) + przewleczenie `addr/chainId` przez opts
3. #4 — server.js (5 poprawek, ~15 linii)
4. #7 — mobile-polish (2 warunki)
5. #5, #6 — prawdziwy lazy-load + usunięcie fake danych
6. #8, #23 — luki w holders pipeline + własność dolo_price.json
7. Strukturalnie: ekstrakcja brush, podział dashboard-core.js, escape-by-default row builder
