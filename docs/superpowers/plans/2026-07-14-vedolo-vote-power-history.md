# Historia Vote Power veDOLO - Plan implementacji

> **Dla agentow wykonawczych:** WYMAGANY SUB-SKILL: uzyj `superpowers:subagent-driven-development` (zalecane) albo `superpowers:executing-plans`, realizujac zadania pojedynczo. Kroki maja checkboxy do sledzenia postepu.

**Cel:** Dodać do karty `Locked DOLO Over Time` dokladny, kanoniczny widok `Vote Power`, bez przyblizen i bez obciazania RPC odwiedzajacych dashboard.

**Architektura:** Nowy modul Pythona odczytuje globalne punkty veDOLO (`epoch`, `point_history`, `slope_changes`) pod jednym przypietym blokiem i wylicza dzienny przebieg Vote Power w wei. Generator publikuje lekki statyczny JSON, a obecny agregat Vote Weight jest synchronizowany z kontraktowym `totalSupply()`. Frontend wczytuje tylko ten JSON i przelacza serie w istniejacym wykresie oraz brushu.

**Stos technologiczny:** Python 3.11, `requests`, istniejacy `rpc_client.py`, `unittest`, GitHub Actions, statyczny HTML/CSS/JavaScript, Playwright.

## Ograniczenia globalne

- Nie wolno wykonywac RPC ani rekonstrukcji historii w przegladarce.
- Wszystkie wartosci wei pozostaja liczbami calkowitymi Python `int`; `parseFloat` nie moze sluzyc do obliczen on-chain.
- Generator musi przypiac `eth_blockNumber`, blok i wszystkie `eth_call` do jednego tagu bloku.
- Ostatni punkt historii musi byc identyczny w wei z `totalSupply()` odczytanym na tym samym bloku, inaczej workflow konczy sie bledem i nic nie publikuje.
- `Locked DOLO` pozostaje domyslnym widokiem, a brak poprawnego JSON nie moze tworzyc serii szacunkowej.
- Kontrolka ma zachowac istniejacy zakres brush, dzialac klawiatura i nie powodowac przesuniecia naglowka na desktopie ani mobile.
- Nowy plik generowany musi zostac zwalidowany i jawnie dodany do automatycznego commitu workflow.
- Plan oraz wszystkie przyszle plany w tym repozytorium sa pisane po polsku.

## Mapa plikow

- Utworz: `vedolo_vote_power.py` - dekodowanie kontraktowych punktow, przypiety odczyt RPC i matematyka globalnej Vote Power.
- Utworz: `generate_vedolo_vote_power_history.py` - CLI budujacy publiczny JSON oraz lokalny cache checkpointow.
- Utworz: `tests/test_vedolo_vote_power.py` - testy czystej matematyki, dekodowania, payloadu i bramki zgodnosci wei.
- Modyfikuj: `update_data.py` - synchronizacja agregatu `total_vote_weight` z kanonicznym `totalSupply()` przy odswiezaniu danych veDOLO.
- Modyfikuj: `validate_data.py` - walidacja schematu i zgodnosci opublikowanej historii Vote Power.
- Modyfikuj: `.github/workflows/update-data.yml` - cache checkpointow, generator, walidacja oraz `git add` nowego JSON.
- Modyfikuj: `.gitignore` - ignorowanie lokalnego cache generatora, jezeli nie jest juz objety wzorcem cache.
- Modyfikuj: `tests/test_update_data.py` - kontrakt kanonicznego agregatu w danych statystycznych.
- Modyfikuj: `tests/test_vedolo_preview_contracts.py` - kontrakt DOM, danych i zachowania widoku wykresu.
- Modyfikuj: `vedolo-preview.html` - kontrolka, stan, ladowanie JSON, renderer i tooltip obu serii.
- Modyfikuj: `vedolo/index.html` - cache-busting loadera po zmianie widoku.

---

### Task 1: Zadanie 1 - Matematyka globalnych punktow veDOLO

**Pliki:**
- Utworz: `tests/test_vedolo_vote_power.py`
- Utworz: `vedolo_vote_power.py`

**Interfejsy:**
- Produkuje `GlobalPoint(bias: int, slope: int, timestamp: int, block: int)`.
- Produkuje `decode_signed_word(word: str) -> int`, `decode_global_point(result: str) -> GlobalPoint` i `evaluate_vote_power_at(observation_ts: int, points: Sequence[GlobalPoint], slope_changes: Mapping[int, int], week_seconds: int = WEEK_SECONDS) -> int`.
- Kolejne zadania korzystaja z `WEEK_SECONDS = 604800` i wyniku w wei.

- [ ] **Krok 1: Napisz testy, ktore najpierw nie przechodza**

```python
from vedolo_vote_power import GlobalPoint, decode_signed_word, evaluate_vote_power_at

def test_applies_weekly_slope_change_without_float_math(self):
    point = GlobalPoint(bias=1000, slope=10, timestamp=0, block=1)
    result = evaluate_vote_power_at(
        15,
        [point],
        {10: -5},
        week_seconds=10,
    )
    self.assertEqual(result, 875)

def test_decodes_sign_extended_solidity_integer(self):
    self.assertEqual(decode_signed_word("f" * 64), -1)
```

- [ ] **Krok 2: Uruchom testy i potwierdz oczekiwana porazke**

Uruchom: `python3 -m unittest tests/test_vedolo_vote_power.py -v`

Oczekiwany wynik: blad importu `No module named 'vedolo_vote_power'` albo brak symbolu, nigdy zielony test.

- [ ] **Krok 3: Dodaj minimalny, precyzyjny modul matematyczny**

```python
WEEK_SECONDS = 7 * 24 * 60 * 60

@dataclass(frozen=True)
class GlobalPoint:
    bias: int
    slope: int
    timestamp: int
    block: int

def decode_signed_word(word: str) -> int:
    raw = int(word.removeprefix("0x"), 16)
    return raw - (1 << 256) if raw >= (1 << 255) else raw

def evaluate_vote_power_at(observation_ts, points, slope_changes, week_seconds=WEEK_SECONDS):
    anchor = max((p for p in points if p.timestamp <= observation_ts), key=lambda p: (p.timestamp, p.block))
    bias, slope, last_ts = anchor.bias, anchor.slope, anchor.timestamp
    boundary = (last_ts // week_seconds) * week_seconds
    while last_ts < observation_ts:
        boundary = min(boundary + week_seconds, observation_ts)
        bias -= slope * (boundary - last_ts)
        if boundary == observation_ts:
            break
        slope += slope_changes.get(boundary, 0)
        last_ts = boundary
    return max(0, bias)
```

`decode_global_point` musi wymagac dokladnie czterech 32-bajtowych slow i dekodowac `bias` oraz `slope` jako liczby ze znakiem. Nie moze akceptowac niepelnej odpowiedzi RPC.

- [ ] **Krok 4: Uruchom testy po implementacji**

Uruchom: `python3 -m unittest tests/test_vedolo_vote_power.py -v`

Oczekiwany wynik: wszystkie testy przechodza; wartosci sa porownywane jako `int`, bez `float` i `Decimal` w sciezce wei.

- [ ] **Krok 5: Commituj samodzielny kontrakt matematyczny**

```bash
git add vedolo_vote_power.py tests/test_vedolo_vote_power.py
git commit -m "Add canonical veDOLO vote power math"
```

### Task 2: Zadanie 2 - Przypiety odczyt kontraktu i publiczna historia

**Pliki:**
- Modyfikuj: `vedolo_vote_power.py`
- Utworz: `generate_vedolo_vote_power_history.py`
- Modyfikuj: `tests/test_vedolo_vote_power.py`
- Modyfikuj: `.gitignore`

**Interfejsy:**
- Produkuje `CanonicalSnapshot(block_number: int, timestamp: int, total_supply_wei: int, locked_supply_wei: int, epoch: int)`.
- Produkuje `fetch_canonical_snapshot() -> CanonicalSnapshot`, `fetch_global_points(snapshot) -> list[GlobalPoint]`, `build_vote_power_payload(snapshot, points, slope_changes) -> dict` i `write_vote_power_history(output_path, state_path) -> dict`.
- Publikuje `data/vedolo-vote-power-history.json`; lokalny `vedolo_vote_power_history_state.json` jest tylko cache i nie trafia do Git.

- [ ] **Krok 1: Dopisz testy payloadu i bramki zgodnosci**

```python
def test_payload_ends_with_exact_contract_total_supply(self):
    snapshot = CanonicalSnapshot(123, 15, 875, 1000, 1)
    payload = build_vote_power_payload(
        snapshot,
        [GlobalPoint(1000, 10, 0, 1)],
        {10: -5},
        day_seconds=10,
    )
    self.assertEqual(payload["lastPointWei"], "875")
    self.assertEqual(payload["points"][-1], [15, "0.000000000000000875"])

def test_payload_rejects_mismatched_contract_total_supply(self):
    with self.assertRaisesRegex(ValueError, "totalSupply"):
        build_vote_power_payload(CanonicalSnapshot(123, 15, 876, 1000, 1), [GlobalPoint(1000, 10, 0, 1)], {10: -5}, day_seconds=10)
```

- [ ] **Krok 2: Uruchom testy i potwierdz oczekiwana porazke**

Uruchom: `python3 -m unittest tests/test_vedolo_vote_power.py -v`

Oczekiwany wynik: brak `CanonicalSnapshot` albo `build_vote_power_payload`.

- [ ] **Krok 3: Zaimplementuj czytnik kontraktu oraz generator**

Uzyj potwierdzonych selektorow i przypietego tagu bloku:

```python
EPOCH_SELECTOR = "0x900cf0cf"
SUPPLY_SELECTOR = "0x047fc9aa"
TOTAL_SUPPLY_SELECTOR = "0x18160ddd"
POINT_HISTORY_SELECTOR = "0xd1febfb9"
SLOPE_CHANGES_SELECTOR = "0x71197484"

def call_at_block(data: str, block_tag: str) -> str:
    payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_call", "params": [{"to": VEDOLO_CONTRACT, "data": data}, block_tag]}
    response = rpc_single_request(get_endpoints("berachain"), payload, describe="veDOLO vote power")
    if response.get("error") or not response.get("result"):
        raise RuntimeError("Pinned veDOLO eth_call failed")
    return response["result"]
```

Najpierw pobierz `eth_blockNumber`, potem `eth_getBlockByNumber`, a nastepnie wywolaj wszystkie selektory z tym samym `block_tag`. Pobieraj punkty `0..epoch` batchami przez `rpc_batch_requests`; lokalny cache moze dopisac tylko punkty o indeksie wiekszym niz zapamietany `epoch`, ale przy niezgodnosci cache musi zostac odrzucony i zbudowany od nowa. Pobierz `slope_changes` dla wszystkich tygodni od pierwszego punktu do `snapshot.timestamp`.

Payload musi zawierac co najmniej:

```python
{
    "schemaVersion": 1,
    "metric": "votePower",
    "chain": "berachain",
    "contract": VEDOLO_CONTRACT,
    "source": "global-point-history",
    "targetBlock": snapshot.block_number,
    "targetTimestamp": snapshot.timestamp,
    "totalSupplyWei": str(snapshot.total_supply_wei),
    "lastPointWei": str(last_value_wei),
    "coverage": {"from": first_observation, "through": snapshot.timestamp},
    "points": [[timestamp, wei_to_decimal(value_wei)], ...],
}
```

Obserwacje to pierwszy timestamp punktu, kazda kolejna polnoc UTC w zakresie oraz koncowy timestamp przypietego bloku. Po zbudowaniu payloadu `lastPointWei` musi rownac sie `totalSupplyWei` dokladnie jako tekst wei, przed zapisaniem pliku atomowo przez plik tymczasowy i `replace`.

- [ ] **Krok 4: Uruchom testy po implementacji**

Uruchom: `python3 -m unittest tests/test_vedolo_vote_power.py -v`

Oczekiwany wynik: payload ma rosnace timestampy, publiczne wartosci dziesietne oraz koncowy punkt zgodny z raw wei.

- [ ] **Krok 5: Commituj generator i cache**

```bash
git add vedolo_vote_power.py generate_vedolo_vote_power_history.py tests/test_vedolo_vote_power.py .gitignore
git commit -m "Generate canonical veDOLO vote power history"
```

### Task 3: Zadanie 3 - Kanoniczny agregat, walidacja i publikacja workflow

**Pliki:**
- Modyfikuj: `update_data.py`
- Modyfikuj: `validate_data.py`
- Modyfikuj: `.github/workflows/update-data.yml`
- Modyfikuj: `tests/test_update_data.py`
- Modyfikuj: `tests/test_vedolo_vote_power.py`

**Interfejsy:**
- Produkuje `apply_canonical_vote_weight(stats: dict, snapshot: CanonicalSnapshot) -> dict`.
- `stats["total_vote_weight"]` jest kanonicznym `totalSupply()` w tokenach, a `stats["total_vote_weight_holder_sum"]` pozostaje diagnostyczna suma aktualnie odczytanych NFT.
- Validator przyjmuje `data/vedolo-vote-power-history.json` tylko z tozsama para `lastPointWei` i `totalSupplyWei`.

- [ ] **Krok 1: Napisz testy kontraktu agregatu i danych**

```python
def test_canonical_snapshot_replaces_only_aggregate_vote_weight(self):
    stats = {"total_vote_weight": 100.0}
    out = update_data.apply_canonical_vote_weight(stats, CanonicalSnapshot(44, 55, 123_450_000_000_000_000_000, 0, 3))
    self.assertEqual(out["total_vote_weight"], 123.45)
    self.assertEqual(out["total_vote_weight_holder_sum"], 100.0)
    self.assertEqual(out["total_vote_weight_source"], "contract_totalSupply")

def test_history_validation_rejects_last_point_different_from_total_supply(self):
    contract = "0xCB86B75EE6133d179a12D550b09FB3cdB1e141D4"
    self.assertFalse(validate_data._vedolo_vote_power_history_valid({
        "schemaVersion": 1, "metric": "votePower", "chain": "berachain",
        "contract": contract, "targetBlock": 1, "targetTimestamp": 2,
        "totalSupplyWei": "9", "lastPointWei": "8", "coverage": {"from": 1, "through": 2},
        "points": [[1, "0.000000000000000008"]],
    }))
```

- [ ] **Krok 2: Uruchom testy i potwierdz oczekiwana porazke**

Uruchom: `python3 -m unittest tests/test_update_data.py tests/test_vedolo_vote_power.py -v`

Oczekiwany wynik: brak helpera synchronizujacego albo walidatora historii.

- [ ] **Krok 3: Zaimplementuj synchronizacje oraz bramki CI**

`update_data.py` po zsumowaniu NFT ma zachowac te sume w `total_vote_weight_holder_sum`, odczytac `CanonicalSnapshot` i wywolac:

```python
def apply_canonical_vote_weight(stats, snapshot):
    holder_sum = stats.get("total_vote_weight", 0)
    stats["total_vote_weight_holder_sum"] = round(float(holder_sum), 4)
    stats["total_vote_weight"] = round(snapshot.total_supply_wei / 10**18, 4)
    stats["total_vote_weight_source"] = "contract_totalSupply"
    stats["total_vote_weight_block"] = snapshot.block_number
    stats["total_vote_weight_timestamp"] = snapshot.timestamp
    return stats
```

Nie wolno kontynuowac odswiezenia jako swiezego, jezeli przypiety odczyt kanoniczny nie powiedzie sie. Workflow ma wykonac generator zaraz po `python update_data.py`, dodac cache `vedolo_vote_power_history_state.json`, dolaczyc JSON do `validate_data.py ...`, a do commitu dodac `data/vedolo-vote-power-history.json`. Walidator ma wymagac poprawnego adresu, `schemaVersion == 1`, rosnacych timestampow, nieujemnych wartosci, pokrycia konczacego sie w `targetTimestamp` i dokladnej zgodnosci raw wei.

- [ ] **Krok 4: Uruchom testy i sprawdzenie skladni workflow**

Uruchom: `python3 -m unittest tests/test_update_data.py tests/test_vedolo_vote_power.py -v && python3 -m py_compile vedolo_vote_power.py generate_vedolo_vote_power_history.py update_data.py`

Oczekiwany wynik: wszystkie testy przechodza, a pliki Pythona kompilują sie bez bledu.

- [ ] **Krok 5: Commituj atomowa publikacje**

```bash
git add update_data.py validate_data.py .github/workflows/update-data.yml tests/test_update_data.py tests/test_vedolo_vote_power.py
git commit -m "Publish verified veDOLO vote power data"
```

### Task 4: Zadanie 4 - Kontrakt interfejsu przelacznika wykresu

**Pliki:**
- Modyfikuj: `tests/test_vedolo_preview_contracts.py`
- Modyfikuj: `vedolo-preview.html`

**Interfejsy:**
- Produkuje `#lockedChartMode` z przyciskami `data-locked-chart-mode="locked"` i `data-locked-chart-mode="vote"`.
- Produkuje `#lockedChartTitle`; oba przyciski maja `type="button"`, a aktywny stan ma `aria-pressed="true"`.
- Przy braku poprawnej historii przycisk Vote Power jest `disabled`, ma wyjasnienie w istniejacym systemie tooltipow i nie zmienia serii.

- [ ] **Krok 1: Dodaj testy kontraktu DOM przed zmiana HTML**

```python
def test_locked_chart_has_accessible_metric_switch(self):
    self.assertIn('id="lockedChartMode"', self.html)
    self.assertIn('data-locked-chart-mode="locked"', self.html)
    self.assertIn('data-locked-chart-mode="vote"', self.html)
    self.assertIn('id="lockedChartTitle"', self.html)
    self.assertIn('aria-pressed="true"', self.html)

def test_vote_power_view_loads_static_history_only(self):
    self.assertIn('fetchJson("data/vedolo-vote-power-history.json")', self.html)
    self.assertNotIn('rpc.berachain.com', self.html)
```

- [ ] **Krok 2: Uruchom testy i potwierdz oczekiwana porazke**

Uruchom: `python3 -m unittest tests/test_vedolo_preview_contracts.py -v`

Oczekiwany wynik: nowe asercje nie znajduja elementow i funkcji.

- [ ] **Krok 3: Dodaj semantyczny naglowek i styl kontrolki**

```html
<div class="locked-chart-heading">
  <div class="card-title">
    <h2 id="lockedChartTitle">Locked DOLO Over Time</h2>
    <span class="count-badge" id="lockedRangeBadge">All Time</span>
  </div>
  <div class="locked-chart-mode" id="lockedChartMode" role="group" aria-label="Chart metric">
    <button type="button" data-locked-chart-mode="locked" aria-pressed="true">Locked DOLO</button>
    <button type="button" data-locked-chart-mode="vote" aria-pressed="false" disabled data-tip="Verified vote-power history is unavailable.">Vote Power</button>
  </div>
</div>
```

Kontrolka ma stosowac istniejace tokeny Graphite+Gold, zachowac `min-height`, miec widoczny focus i na waskich ekranach przejsc pod meta bez nachodzenia na badge. Nie dodawaj nowej biblioteki ani osobnego komponentu.

- [ ] **Krok 4: Uruchom kontrakt UI**

Uruchom: `python3 -m unittest tests/test_vedolo_preview_contracts.py -v`

Oczekiwany wynik: kontrakt DOM przechodzi, a stary wykres nadal ma pierwotne elementy SVG i brush.

- [ ] **Krok 5: Commituj kontrolke bez zmiany obliczen renderera**

```bash
git add vedolo-preview.html tests/test_vedolo_preview_contracts.py
git commit -m "Add veDOLO chart metric switch control"
```

### Task 5: Zadanie 5 - Przelaczanie serii, brush i tooltip

**Pliki:**
- Modyfikuj: `tests/test_vedolo_preview_contracts.py`
- Modyfikuj: `vedolo-preview.html`
- Modyfikuj: `vedolo/index.html`

**Interfejsy:**
- Stan zawiera `votePowerHistory: []` i `lockedChartMode: "locked"`.
- Produkuje `parseVotePowerHistory(payload) -> Array<[number, number]>`, `activeLockedChartSeries() -> {mode, title, unit, points}` i `setLockedChartMode(mode: "locked" | "vote") -> void`.
- `state.lockedBrush` pozostaje obiektem `{from, to}` wspolnym dla obu metryk.

- [ ] **Krok 1: Dodaj testy integracji serii przed implementacja**

```python
def test_locked_chart_uses_independent_vote_power_state_and_shared_brush(self):
    self.assertIn('votePowerHistory:[]', self.html)
    self.assertIn('lockedChartMode:"locked"', self.html)
    self.assertIn('function activeLockedChartSeries()', self.html)
    self.assertIn('function setLockedChartMode(mode)', self.html)
    self.assertIn('state.lockedBrush', self.html)

def test_vedolo_route_busts_cache_for_vote_power_chart(self):
    self.assertIn('vedolo-vote-power-history-20260714', self.route)
```

- [ ] **Krok 2: Uruchom testy i potwierdz oczekiwana porazke**

Uruchom: `python3 -m unittest tests/test_vedolo_preview_contracts.py -v`

Oczekiwany wynik: brak stanu Vote Power, helperow i nowej wersji loadera.

- [ ] **Krok 3: Zaimplementuj minimalne przelaczanie bez utraty zoomu**

```javascript
function activeLockedChartSeries(){
  if(state.lockedChartMode === "vote" && state.votePowerHistory.length >= 2){
    return {mode:"vote", title:"Vote Power Over Time", unit:"Vote Power", points:state.votePowerHistory};
  }
  return {mode:"locked", title:"Locked DOLO Over Time", unit:"DOLO", points:state.lockedHistory};
}

function setLockedChartMode(mode){
  if(mode === "vote" && state.votePowerHistory.length < 2) return;
  state.lockedChartMode = mode === "vote" ? "vote" : "locked";
  qs("#lockedChartTitle").textContent = activeLockedChartSeries().title;
  qsa("[data-locked-chart-mode]").forEach(button => {
    const active = button.dataset.lockedChartMode === state.lockedChartMode;
    button.setAttribute("aria-pressed", String(active));
    button.classList.toggle("active", active);
  });
  clampLockedBrushToActiveSeries();
  renderLockedChart();
}
```

`parseVotePowerHistory` akceptuje tylko schema v1, prawidlowy kontrakt i rosnace pary `[timestamp, decimal-string]`; wartosci sa zamieniane na `Number` dopiero po sprawdzeniu skonczonosci oraz nieujemnosci. W `boot()` pobierz JSON razem z pozostalymi statycznymi danymi. Jesli pobranie lub walidacja klienta zawiedzie, pozostaw przycisk Vote Power wylaczony.

Uogolnij `renderLockedChart`, `renderLockedChartFor`, hover oraz brush tak, aby pobieraly aktywna serie i jednostke. Tooltip dla lockow pokazuje `fmtDolo(value)`, a dla Vote Power `fmtCompact(value) + " Vote Power"`. Nie rejestruj drugi raz dokumentowych listenerow brush przy przelaczeniu. Przy zmianie metryki zachowaj date range, a tylko ogranicz `from/to` do pokrycia aktywnej serii.

Zmien wersje loadera w `vedolo/index.html` na `vedolo-vote-power-history-20260714`.

- [ ] **Krok 4: Uruchom kontrakty UI po implementacji**

Uruchom: `python3 -m unittest tests/test_vedolo_preview_contracts.py -v`

Oczekiwany wynik: testy potwierdzaja dane statyczne, stan obu metryk, wspolny brush i nowy cache-busting.

- [ ] **Krok 5: Commituj renderer oraz loader**

```bash
git add vedolo-preview.html vedolo/index.html tests/test_vedolo_preview_contracts.py
git commit -m "Render canonical veDOLO vote power chart"
```

### Task 6: Zadanie 6 - Dane produkcyjne i weryfikacja calego przeplywu

**Pliki:**
- Modyfikuj: `data/vedolo-vote-power-history.json`
- Modyfikuj: pliki wygenerowane przez `update_data.py`, tylko gdy kanoniczny agregat zmienia ich wartosc.

**Interfejsy:**
- Publiczny JSON przechodzi `validate_data.py` i ma koncowy punkt raw wei identyczny z `totalSupplyWei`.
- Strona veDOLO dziala bez ostrzezen konsoli w obu trybach wykresu.

- [ ] **Krok 1: Zbuduj aktualne dane z przypietego odczytu**

Uruchom: `python3 generate_vedolo_vote_power_history.py --output data/vedolo-vote-power-history.json --state vedolo_vote_power_history_state.json`

Oczekiwany wynik: generator wypisuje blok, timestamp, liczbe punktow oraz zgodnosc `lastPointWei == totalSupplyWei`; przy niezgodnosci konczy sie kodem roznych od zera i nie nadpisuje publicznego JSON.

- [ ] **Krok 2: Uruchom komplet testow i walidacje danych**

Uruchom: `python3 -m unittest tests/test_vedolo_vote_power.py tests/test_update_data.py tests/test_vedolo_preview_contracts.py -v && python3 validate_data.py data/vedolo-vote-power-history.json vedolo_holders.json vedolo_stats.json`

Oczekiwany wynik: wszystkie testy i walidacje przechodza bez pomijania niezgodnosci.

- [ ] **Krok 3: Zweryfikuj faktyczny widok w lokalnej przegladarce**

Uruchom serwer: `python3 -m http.server 8000`

W Playwright otworz `http://127.0.0.1:8000/vedolo/?cb=vote-power-20260714`, zaczekaj na dane i sprawdz: domyslny Locked DOLO, klikniecie Vote Power, nazwe wykresu, zmiane osi i tooltipu, zachowanie brush range po przejsciu w obie strony, Enter/Space na obu przyciskach oraz brak nachodzenia naglowka przy szerokosciach 1440 px i 390 px. Zapisz screenshoty obu trybow do tymczasowej lokalizacji tylko na potrzeby kontroli.

- [ ] **Krok 4: Sprawdz diff i commituj wylacznie zweryfikowane dane**

```bash
git diff --check
git status --short
git add data/vedolo-vote-power-history.json vedolo_holders.json vedolo_stats.json metrics_snapshot.json
git commit -m "Refresh verified veDOLO vote power history"
```

Dodaj do commitu tylko pliki, ktore rzeczywiscie zmienil generator lub `update_data.py`; nie wlaczaj niezaleznych danych workflow.

## Samokontrola planu

- Przelacznik, tooltip, osie, wspolny brush i dostepnosc: Zadania 4-6.
- Dokladna historia globalna bez przyblizen i bez archive RPC: Zadania 1-2.
- Zgodnosc raw wei z kontraktem oraz kanoniczny agregat: Zadania 2-3 i 6.
- Bez runtime RPC, odporne zachowanie przy braku danych i publikacja przez GitHub Actions: Zadania 3 i 5.
- Testy danych, workflow, UI i przegladarki: Zadania 1-6.
