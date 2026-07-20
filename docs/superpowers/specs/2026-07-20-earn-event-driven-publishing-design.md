# EARN Event-Driven Publishing Design

## Cel

Utrzymac dane EARN live bez zaleznosci od opoznionego GitHub cron, publikowac bezpieczny postep historycznego backfillu Berachain i wdrazac Pages dopiero po przeliczeniu statusu swiezosci.

## Przeplyw danych

Kazdy workflow produkujacy dane EARN po udanym pushu jawnie uruchamia `Monitor EARN Freshness`. Monitor przebudowuje `earn-freshness/status.json` oraz `earn-quality/status.json`, opcjonalnie uruchamia naprawe stalego chaina, egzekwuje SLA, a dopiero po sukcesie dispatchuje `pages.yml`. Cron monitora pozostaje zabezpieczeniem awaryjnym.

Wywolanie monitora po workflow uruchomionym przez watchdog nie moze tworzyc petli napraw. Producent przekazuje wtedy `allow_remediation=false`: monitor aktualizuje status, sprawdza SLA i wdraza Pages, ale nie dispatchuje kolejnej naprawy.

## Backfill Berachain

Aktywna kohorta jest zapisywana w checkpoint cache i wznawiana do pelnego zakonczenia, nawet jesli czesc jej portfeli zostanie juz opublikowana. Po pelnym skanie zakresu blokow workflow wybiera historie, ktore maja poprawny chain, adres, `scanRange.fromBlock` od startu protokolu oraz `lastScannedBlock`/`scanRange.toBlock` co najmniej na przypietym target blocku. Tylko te historie moga trafic do publicznego canonical i verified ledgeru.

Publikowanie gotowych portfeli nie czeka na wszystkie workery materializacji ani repair calej kohorty. Nie wolno jednak publikowac niczego przed zakonczeniem wszystkich workerow skanu eventow.

Canonical event scanner respektuje `canonical_max_block_chunk`. Dla Berachain limit wynosi 9 999 blokow, co ogranicza timeouty i koszt fallbacku z zapytania wielotematowego na pojedyncze tematy.

## Bezpieczenstwo operacyjne

- Dane klasyfikacyjne i warunki strict pozostaja w kodzie; rozmiar chunku i kohorty pozostaja jawnymi parametrami operacyjnymi.
- Sekrety RPC nadal pochodza wylacznie z GitHub Secrets.
- Concurrency monitora scala rownolegle zakonczenia producentow.
- Pages nie wdraza snapshotu, jesli kontrola SLA konczy sie bledem krytycznym.
- Cache aktywnej kohorty jest zamykany dopiero po poprawnym opublikowaniu kompletnego batcha.

## Weryfikacja

- test integracyjny helpera dispatchujacego monitor z odpowiednim `allow_remediation`;
- test kontraktu kazdego producenta EARN;
- test selekcji wyłącznie kompletnych historii;
- test zachowania aktywnej kohorty i czesciowego publikowania w workflow;
- test canonical chunk limitu Berachain;
- pelny `run_earn_audit_checks.py`, walidacja YAML i produkcyjny smoke test Pages.
