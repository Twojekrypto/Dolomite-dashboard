# Pilot prywatnego storage LP w Cloudflare R2

Stan wdrożenia: **kod przygotowany, R2 nieaktywne**. Bez konfiguracji repozytorium
działa tak jak wcześniej: `LP_DATA_STORAGE=git`. Nie utworzono konta/bucketu,
nie zakupiono usługi i nie przeniesiono produkcyjnych danych. Brak sekretów blokuje
aktywację. Pilot LP nie rozwiązuje dominującego wzrostu danych Earn/Supply — ich
migracja i retencja wymagają osobnego planu po rzeczywistym sprawdzeniu pilota.

## 1. Konfiguracja przez właściciela

1. W istniejącym koncie Cloudflare otwórz **R2 Object Storage**, utwórz osobny
   prywatny bucket dla pilota. Jeżeli R2 wymaga aktywacji/płatności, zatrzymaj się
   i zatwierdź koszt świadomie. Nie włączaj publicznego `r2.dev`, custom domain,
   CORS ani reguł automatycznego usuwania.
2. W panelu R2 wybierz zarządzanie tokenami API, utwórz token z uprawnieniem
   **Object Read & Write** ograniczonym do tego jednego bucketu. Zapisz Access Key
   ID, Secret Access Key i endpoint S3 podany przez panel. Nie używaj tokena
   administratora konta. Szczegóły: [uwierzytelnianie R2](https://developers.cloudflare.com/r2/api/tokens/).
3. W repozytorium GitHub: **Settings → Secrets and variables → Actions** dodaj
   sekrety `R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`.
   Endpoint ma postać `https://<ACCOUNT_ID>.r2.cloudflarestorage.com` (dla bucketu
   jurysdykcyjnego użyj dokładnie endpointu z panelu). Dodaj zmienną `R2_BUCKET`
   z nazwą bucketu. Pozostaw `LP_DATA_STORAGE` niewypełnione lub `git` oraz
   `LP_R2_READY_DIGEST` niewypełnione. [Sekrety GitHub Actions](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-secrets).

Sekrety można wprowadzić przez interaktywne `gh secret set R2_ENDPOINT_URL`
(analogicznie pozostałe nazwy); nie wklejaj wartości do historii poleceń,
repozytorium, zgłoszeń ani czatu. Kod loguje wyłącznie operację i digest.
Przeglądarka nigdy nie otrzymuje kluczy R2: Pages nadal serwuje publiczny plik
`data/dolo-liquidity.json` pod dotychczasowym URL-em.

## 2. Dowód działania, zanim cokolwiek przełączysz

W **Actions → LP Storage Pilot → Run workflow** wybieraj `master`.
Workflow ma tę samą grupę concurrency co `Update DOLO Liquidity`; niczego nie
commituje i nie zmienia zmiennych repozytorium. Zewnętrznych writerów zabezpiecza
również warunkowy zapis aktywnego manifestu.

1. Najpierw uzyskaj prawdziwy, poprawny i świeży LP z udanego `Update DOLO Liquidity`.
   Uruchom akcję `bootstrap`; zapisz digest A z logu. Nie poprawiaj ręcznie
   `generatedAt`. Jeżeli masz wyłącznie stary, poprawny strukturalnie baseline,
   `bootstrap-resume` może go zachować jako punkt odzyskiwania. Taki plik **nie**
   przejdzie walidacji publikacji Pages/Flows ani readiness.
2. Ustaw `LP_DATA_STORAGE=shadow`. Uruchom `Update DOLO Liquidity`, poczekaj na
   pełne powodzenie. Git nadal jest produkcją; R2 dostaje i odczytuje zwrotnie ten
   sam nowy artefakt. Zapisz nowy digest B. Jeśli A pochodził ze starego baseline'u,
   wykonaj jeszcze jeden prawdziwy refresh i użyj dwóch świeżych wersji do próby.
3. Uruchom `LP Storage Pilot` z `action=verify`, `digest=B`.
4. Wykonaj prawdziwy test cofnięcia: `action=rollback`, `digest=A`,
   `reason=pilot-drill-<numer-zgloszenia>`. Zapisz numer udanego runu. Następnie
   `action=republish`, `digest=B`. Publikacja B ponownie musi być świeża
   (maksymalnie 8 godzin); starszego B nie da się „odświeżyć” samym restore.
5. Uruchom `action=mark-ready`, `digest=B`, `rollback_digest=A`. Ten krok wymaga
   zapisanego dowodu rzeczywistego B→A rollbacku, aktywnego B i odczytu obu
   wersji z walidacją. Dopiero wtedy tworzy niezmienny marker readiness dla B.

W `shadow` błąd uploadu/weryfikacji blokuje publikację tego runu; nie zmienia
już wdrożonego Git/Pages. Diagnostyka jest celowo pozbawiona treści błędów SDK,
które mogłyby ujawnić endpoint lub sekret. Sprawdź komplet konfiguracji, dostęp,
świeżość/strukturę LP i ostatnie udane digesty; nie włączaj debug logów SDK z kluczami.

## 3. Osobno zatwierdzany cutover

Po przejrzeniu wszystkich runów upload → restore → rollback → republish:

Przed zmianą zmiennych uruchom `LP Storage Pilot` z `action=preflight` oraz
`digest=B` (digest istniejącego markera readiness, nie bieżącego pliku).
To tylko odczyt: w izolowanym procesie sprawdza produkcyjny gate readiness
i odtwarza świeży aktywny LP do katalogu tymczasowego runnera. Nie zmienia
zmiennych repozytorium, obiektów R2 ani danych na stronie.

1. Ustaw `LP_R2_READY_DIGEST=B`, następnie jawnie `LP_DATA_STORAGE=r2`.
   Sam string `r2` nie wystarczy: marker musi pasować do tego bucketu, endpointu,
   prefiksu `lp/v1` i dokładnego B. Kolejne poprawne publikacje nie wymagają
   zmiany B; to identyfikator dowodu pilota, nie pin aktualnego produkcyjnego pliku.
2. Uruchom `Update DOLO Liquidity`. Odtworzy wybrany aktywny baseline do skanera,
   wygeneruje i zweryfikuje świeże dane, zapisze je w R2. Git nadal publikuje ręczny
   rejestr `data/dolo-liquidity-pools.json`, ale nie generated LP JSON.
3. Sprawdź ręcznie wywołany downstream `Update DOLO Flows Data` oraz Pages.
   Flows i Pages pobierają aktywny, świeży LP z R2; przy awarii kończą się błędem,
   bez cichego fallbacku do starego Git. Harmonogram LP i istniejące triggery
   pozostają zachowane. Dla live URL użyj jednorazowego cache-bustera.
4. Ustaw check **LP Storage Checks / storage-contracts** jako wymagany przy PR
   do produkcyjnej gałęzi. Check blokuje zmiany wyłącznie
   `data/dolo-liquidity.json` w trybie `r2`; nie blokuje rejestru ani innych JSON-ów.
   Check na push wykrywa już dokonany push, nie cofa go: realne wymuszanie wymaga
   ochrony gałęzi/ruleset i ograniczenia bezpośrednich pushów.

**Oryginalny LP baseline pozostaje śledzony w Git.** Ten pilot go nie usuwa,
nie dodaje szerokiego `.gitignore` i nie przepisuje historii. Ewentualne późniejsze
`git rm --cached data/dolo-liquidity.json` i dokładny wpis ignore wymagają osobnego
review, dowodu działającego R2 i kontrolowanej migracji (check cutover normalnie
zablokuje też takie usunięcie). Hydratacja w Actions jest czyszczona wyłącznie dla
tego pliku przed istniejącymi rebase; inne zmiany pozostają nietknięte.

## Rollback operacyjny

- **R2 → starsza wersja R2:** `LP Storage Pilot`, `action=rollback`, pełny digest
  znanej poprawnej wersji i numer zgłoszenia w `reason`. W `r2` sukces jawnie
  uruchamia Flows oraz Pages. Starszy niż 8h artefakt może posłużyć do odzyskania
  skanera, ale aktualne Pages/Flows nadal odmówią jego publikacji. Wtedy wykonaj
  udany świeży LP refresh; nie obchodź freshness. `republish` pozwala wrócić do
  wybranej nowszej wersji, o ile nadal przechodzi normalną walidację i nie cofa
  aktualnych kursorów źródeł.
- **Powrót do Git:** wstrzymaj nowe wdrożenia, ustaw `LP_DATA_STORAGE=git`, uruchom
  pełny udany LP refresh/commit i dopiero potem sprawdź Flows/Pages. Zachowany
  baseline Git może być już stary — sama zmiana zmiennej nie oznacza świeżych danych.
  Po przyszłym untrackingu najpierw odtwórz i jawnie przywróć śledzenie zweryfikowanego
  LP w kontrolowanej migracji. Nie usuwaj bucketu/obiektów przy rollbacku.
- Używaj wyczyszczonego checkoutu repozytorium. **Nie merguj ani nie pushuj starej
  historii ze starych klonów sprzed czyszczenia.** Lokalne niezacommitowane zmiany
  zachowaj osobno i przenieś świadomie; starych archiwów nie modyfikuj.

## Komendy lokalne i format storage

Po bezpiecznym ustawieniu zmiennych środowiska w osobnym virtualenv:

```bash
python3 -m pip install -r requirements-storage.txt
python3 scripts/data_artifact_store.py bootstrap
LP_DATA_STORAGE=shadow python3 scripts/data_artifact_store.py publish
python3 scripts/data_artifact_store.py restore --purpose current --path /tmp/lp-current.json
python3 scripts/data_artifact_store.py restore --digest DIGEST_A --purpose rollback --path /tmp/lp-rollback.json
python3 scripts/data_artifact_store.py rollback --digest DIGEST_A --reason pilot-drill-123
LP_DATA_STORAGE=shadow python3 scripts/data_artifact_store.py publish --path /tmp/lp-current.json
python3 scripts/data_artifact_store.py verify --digest DIGEST_B --mark-ready --rollback-digest DIGEST_A
```

`DIGEST_A/B` zastąp pełnymi rzeczywistymi SHA256. Komendy `restore`, `bootstrap`,
`verify`, `rollback` są jawnymi operacjami na storage nawet w trybie `git`;
workflowowe `prepare`/`publish` są no-op w `git`. Lokalne zapisy też respektują CAS,
ale nie należą do kolejki concurrency GitHub: nie uruchamiaj ich równolegle z jobami.
Zwykły `publish` wymaga istniejącego, poprawnego aktywnego wskaźnika; jego brak
nie może wyzerować ochrony przed cofnięciem danych. Inicjalizacja pustego wskaźnika
wymaga jawnego `bootstrap`, także po awarii — najpierw ustal właściwy baseline.

Pod `lp/v1/objects/<sha256>.json` są niezmienne bajty LP,
`versions/<sha256>.json` zawiera size/hash/czas/kursory źródeł,
`active.json` jest warunkowo aktualizowanym wskaźnikiem. Readiness i dowody rollbacku
mają własne niezmienne klucze; `rollback-audit/<publicationId>.json` zachowuje
poprzedni digest, czas i identyfikator zgłoszenia także po późniejszej publikacji.
Jeśli operacja zakończy się błędem sieci po zapisie aktywnego wskaźnika, stan
może już być zmieniony: sprawdź `verify`/aktywny digest przed ponowną próbą.
Brak receipt/readiness nadal blokuje cutover. Nie ma automatycznego usuwania.
Nie polegamy na natywnym versioningu bucketu; SDK używa warunków `IfMatch`/
`IfNoneMatch`, bez opcjonalnych checksumów SDK, i zawsze weryfikuje własne SHA256
oraz size. [Zakres API R2](https://developers.cloudflare.com/r2/api/s3/api/).

Rzeczywiste koszty, trwałość tokena, zachowanie sieci i politykę retencji trzeba
potwierdzić w koncie użytkownika. Testy lokalne używają fake S3 i prawdziwego
walidatora LP; nie są dowodem połączenia z prawdziwym R2.
