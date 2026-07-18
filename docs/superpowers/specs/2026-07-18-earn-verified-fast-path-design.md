# EARN Verified Fast Path Design

## Cel

Przyspieszyc lookup portfela EARN bez obnizania wiarygodnosci sald i yield. Biezace dane chaina, historyczny canonical backfill oraz stan weryfikacji konkretnego walleta musza byc rozdzielone, aby nie sugerowac wielogodzinnego opoznienia tam, gdzie niepelna jest tylko historia.

## Architektura

Rozwiazanie ma trzy niezalezne sciezki:

1. `head refresh` utrzymuje aktualne salda i juz opublikowane historie blisko heada;
2. osobny `coverage backfill` dodaje nowe wallety do canonical history w malej, serializowanej kolejce;
3. lookup najpierw renderuje najlepszy opublikowany i zapisany wynik, a nastepnie naklada live delta. Chwilowa awaria RPC nie moze obnizyc wyniku, ktory w tej samej wersji danych byl juz zweryfikowany.

Publiczny verified-ledger shard pozostaje podstawowym statycznym artefaktem per wallet. Zostanie rozszerzony o rozstrzygniety `resolvedInterestLedger`, ale tylko dla marketow, dla ktorych pipeline posiada zaufany wynik interest-ledger. Brak takiego wyniku pozostaje jawnie `pending` albo `inferred`; generator nie moze awansowac snapshot/netflow fallbacku do `verified`.

## Polityka RPC

- HTTP `401` i `403` wylaczaja konkretny endpoint do konca lookupu.
- Deterministyczne bledy requestu, m.in. `metadata is not found`, koncza dany request bez ponawiania na kolejnych endpointach.
- `429`, timeouty i bledy `5xx` pozostaja retryable i przechodza na kolejny zdrowy endpoint.
- Batch moze przejsc na pojedyncze wywolania tylko wtedy, gdy provider nie wspiera batcha albo odpowiedz jest czesciowa. Nie wolno uruchamiac kaskady indywidualnych retry po autorytatywnym `401/403`.
- Stan endpointow jest resetowany przy nowym lookupie i zmianie chaina.

## Stabilnosc wyniku

Cache lookupu przechowuje wynik per `chain:wallet` wraz z datami snapshotu i blokiem replayu. Podczas odswiezenia nowy market moze zastapic zapisany market tylko, gdy:

- ma nowszy snapshot albo replay block; lub
- ma co najmniej taki sam poziom zaufania w tej samej wersji danych.

Przejscie `verified -> pending/inferred` spowodowane chwilowym bledem RPC nie zastapi poprzedniej wartosci. UI pozostawi wynik i pokaze `Refreshing verification` do zakonczenia nowej proby.

## Status UI

Status EARN pokazuje dwie informacje:

- `Live data` z wiekiem canonical/netflow heada;
- `Historical verification` z `backfilledWalletCount/knownAddressCount`.

Niepelny backfill nie zmienia swiezych danych na `Chain data syncing`. Etykieta `syncing` jest zarezerwowana dla realnie starego heada.

## Coverage Backfill

Nowy workflow uruchamia sie osobno od czestych head refreshy i ma wlasna grupe concurrency. Selekcja nie uzywa `--existing-history-only`: najpierw wybiera aktywne wallety bez historii, potem najstarsze watermarki. Jeden run publikuje ograniczona kohorte i checkpointuje pozostala prace.

Kolejnosc wdrozenia chainow:

1. Mantle, bo pozostalo 1296 walletow;
2. Arbitrum i Berachain w kohortach wazonych aktywnoscia i wartoscia pozycji;
3. pozostale aktywne chainy po osiagnieciu stabilnego SLA.

Head workflow nadal odswieza wszystkie juz opublikowane historie i nie czeka na kolejke historyczna.

## Publiczny Interest Ledger

`build_earn_verified_ledger.py` zachowuje obecny strict audit i dodatkowo publikuje `resolvedInterestLedger` tylko wtedy, gdy zaufany interest-ledger zostal zapisany przez canonical refresh/audit dla tego samego lub nowszego bloku porownawczego. Shardy przenosza to pole bez degradacji schematu. Frontend zuzywa je jako fast path, a live replay liczy jedynie delta od `comparisonBlock`.

Jesli artefakt jest starszy, ma inny snapshot albo nie ma kompletnego canonical coverage dla pozycji borrow-route, frontend nie uznaje go za `verified`.

## Wydajnosc

Lookup krytyczny obejmuje: publiczny ledger shard, aktualne salda i ceny. Rewardy oraz pozostale dane dodatkowe sa pobierane po pierwszym stabilnym renderze i nie blokuja tabeli Supply Assets. Podzial duzego bundla pozostaje osobnym etapem dopiero po usunieciu opoznien RPC, poniewaz obecny transfer JS po gzip ma okolo 145 KB i nie jest glownym bottleneckiem.

## Weryfikacja

- testy jednostkowe klasyfikacji bledow i circuit breakera;
- testy TDD scalania cache bez degradacji `verified`;
- testy selektora brakujacych walletow i kontraktu osobnego workflowu;
- testy schematu `resolvedInterestLedger` i blokow zaufania;
- `python3 run_earn_audit_checks.py`;
- lokalny lookup tego samego walleta dwa razy oraz porownanie wyniku i czasu;
- kontrola statusow Arbitrum, Berachain i Mantle w przegladarce.
