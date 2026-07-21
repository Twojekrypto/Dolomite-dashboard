# EARN: Kolumna Quality i Excelowe Szerokości

## Cel

Lokalny edytor tabel EARN ma zachowywać się jak arkusz: poszerzenie jednej
kolumny nie zmniejsza innych. Nadmiar szerokości jest dostępny przez poziome
przewijanie wyłącznie wewnątrz danej tabeli. Równocześnie `Supply Assets` ma
czyściej prezentować źródła APR i stany wiarygodności danych.

## Zakres

- Dotyczy lokalnego trybu `localhost` z `?layoutEditor=1`; nie będzie
  publikowany ani wypychany przed odrębną akceptacją użytkownika.
- Edytor zwiększa szerokość przeciąganej kolumny bez odbierania szerokości
  sąsiadom. Minimalne szerokości nadal chronią treść przed nakładaniem.
- Gdy suma szerokości przekroczy kontener, tabela ma własne przewijanie w osi X.
- `Supply Assets` otrzymuje kolumnę `Quality` zaraz po `Token`.
- `Quality` zawiera krótką etykietę i kolorową kropkę: `Verified`, `Mismatch`,
  `Fallback`, `Pending`, `Inferred` albo `Checking`. Pełne znaczenie obecnych
  danych jakościowych pozostaje w istniejącym tooltipie `data-tip`.
- Dotychczasowe badge'e jakości pod nazwą assetu znikają z wiersza podstawowego,
  dzięki czemu nazwa tokena nie jest obciążona technicznymi komunikatami.
- W kolumnie `Supply` etykiety rozbicia APR są skrócone do `Interest`, `Yield`,
  `oDOLO` i `GM`. Dokładne źródło pozostaje w tooltipie.

## Dane i kompatybilność

- Bez zmian w obliczeniach APR/APY, yield, weryfikacji replay, trybie E-Mode,
  `Hide Dust`, sortowaniu lub rozwijaniu szczegółów.
- Kolumna `Quality` używa tych samych obiektów prezentacji, które obecnie
  budują badge'e `Mismatch`, `Netflow Fallback`, `Pending` i podobne stany.
- Nowy domyślny układ `Supply`: `Token | Quality | Price | Supply | Current
  Balance | Total Yield Earned | Details`.
- Zapis edytora pozostaje lokalny. Walidator układu dopuszcza sumę szerokości
  większą niż 100%, aby możliwe było przewijanie po poszerzeniu kolumny.

## Sprawdzenie

- Testy modelu potwierdzają, że poszerzenie nie zmienia szerokości innych
  kolumn i zachowuje minimalne szerokości.
- Testy kontraktowe potwierdzają pozycję `Quality`, skrócone etykiety i brak
  starych badge'ów pod nazwą tokena.
- Test w przeglądarce na rzeczywistym portfelu Arbitrum potwierdza:
  przewijanie po poszerzeniu, czytelne `Quality`, poprawne tooltipy,
  APR/APY, sortowanie i pełną szerokość wiersza Details.
