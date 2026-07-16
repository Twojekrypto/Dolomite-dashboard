# DOLO Holder Distribution UX Design

## Cel

Uspójnić kartę `DOLO Holder Distribution Over Time` z dojrzałym UX dashboardu
bez zmiany źródeł danych, klasyfikacji adresów lub sposobu liczenia sald.
Zmiana ma ułatwić odczyt zakresu czasu, metodologii oraz różnicy pomiędzy
dużymi bucketami, a także pozwolić szybko skupić się na jednej serii.

## Zakres

1. Uczytelnić kontekst danych i okres porównania.
2. Dodać przełącznik metryki `Balance / Change %`.
3. Dodać przypinanie serii przez kliknięcie wykresu lub wiersza legendy.

Nie zmieniamy bucketów, filtrowania CEX/protokół/alokacje, danych historycznych
ani rozwijanego panelu portfeli.

## UX

### Kontekst i legenda

- Badge zakresu pozostaje przy tytule i pokazuje aktualne okno: `30D`, `3M`,
  `1.0Y` lub `All`.
- Nagłówek ostatniej kolumny legendy jest dynamiczny: `Change · 30D`,
  `Change · 3M` itd. Dla pełnego zakresu używa `Change · All`.
- Pigułka metodologii otrzymuje czytelny skrót `Market wallets`; obok wprost
  pokazuje `CEX & allocations excluded`. Ikona `i` zachowuje pełen opis:
  wykluczenie CEX, adresów protokołu/kontraktów/LP oraz alokacji Team/Investor.
- `Balance` pozostaje najważniejszą wartością w wierszu. `Wallets` oraz
  `Change` są danymi pomocniczymi, wyrównanymi do prawej i w spokojniejszej
  hierarchii wizualnej.

### Przełącznik metryki

- W nagłówku obok obecnego wyboru bucketów pojawia się kompaktowy segmented
  control `Balance / Change %`. `Balance` jest stanem domyślnym.
- `Balance` zachowuje obecne wartości DOLO, skalę osi i wypełnienie pod
  aktywną serią.
- `Change %` pokazuje procentową zmianę salda każdego bucketa względem
  pierwszego punktu aktualnie wybranego okna brush. Oś Y jest procentowa,
  zawiera linię `0%`, a jej granice są symetryczne względem zera, aby wzrost i
  spadek miały tę samą wagę wizualną.
- Jeśli bucket ma zerową wartość w pierwszym punkcie, jego względna zmiana jest
  niedostępna: nie rysujemy sztucznej wartości procentowej, a tooltip pokazuje
  `New / no baseline`. Nie dzielimy przez zero i nie zmieniamy danych
  absolutnych.
- Tooltip w `Change %` pokazuje najpierw procent, pod nim aktualne saldo DOLO
  i bezwzględną zmianę. W widoku `Balance` zachowuje saldo DOLO oraz zmianę w
  ramach wybranego okna.
- Brush i wybrany okres są wspólne dla obu metryk; przełączenie nie resetuje
  zakresu ani przypiętej serii.

### Przypinanie serii i szczegóły

- Kliknięcie linii lub wiersza legendy przypina bucket: jego linia jest
  wzmocniona, inne są przygaszone, obszar wykresu i hover wskazują tę serię.
- Ponowne kliknięcie przypiętego bucketa usuwa przypięcie i wraca do widoku
  wszystkich serii.
- Wiersz legendy jest dostępny z klawiatury (`Enter` / `Space`) i dostaje
  stan `aria-pressed`; kliknięcie przycisku `Details` nie wywołuje tego samego
  handlera.
- `Details` nadal wyłącznie otwiera lub zamyka tabelę adresów i automatycznie
  przypina powiązaną serię podczas otwarcia.
- Przełączenie `Top holders / Smaller holders` czyści przypięcie wyłącznie
  wtedy, gdy przypięty bucket nie występuje w nowej grupie.

## Implementacja

- Rozszerzyć lokalny stan karty o `holderDistributionMetric` (`balance` lub
  `changePct`) oraz użyć istniejącego `holderDistributionActiveKey` jako
  przypiętego wyboru.
- Uogólnić funkcje skali osi, ścieżki, wypełnienia oraz tooltipu tak, aby
  przyjmowały aktywną metrykę. Wartości historii pozostają wyłącznie liczbami
  DOLO; procent jest pochodną renderowaną w przeglądarce dla wybranego okna.
- Zastosować istniejący gold/graphite styling dla controlu, hoveru i
  przypięcia. Nie tworzyć nowej karty ani nie zmieniać rozmiaru wykresu.
- Wykresowe ścieżki dostają semantykę interaktywną oraz obsługę kliknięcia;
  legenda dostaje jeden wspólny handler kliknięcia/klawiatury z wyłączeniem
  `Details`.

## Weryfikacja

- Dodać test kontraktowy DOM dla obecności obu metryk, dynamicznego nagłówka
  zmiany, czytelnej metodologii i interaktywnej legendy.
- Lokalnie potwierdzić: oba tryby mają niepuste ścieżki i osie; brush zachowuje
  okres po zmianie metryki; przypięcie działa z linii, wiersza oraz klawiatury;
  `Details` nadal rozwija właściwe adresy.
- Sprawdzić desktop i mobile: kontrolki mieszczą się bez nakładania, wartości
  legendy nie są obcięte, a tooltip nie wychodzi poza kartę.

## Granice

To ulepszenie nie jest audytem poprawności danych holderów. Nie dodaje nowych
źródeł, nie wykonuje RPC w przeglądarce i nie zmienia danych publikowanych przez
workflow.
