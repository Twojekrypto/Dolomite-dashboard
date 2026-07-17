# Neutralny stan pustej sieci w tabelach DOLO

## Cel

Zastąpić techniczny napis `Unknown` neutralnym znakiem `—` w kolumnie Chain,
gdy dashboard nie może wiarygodnie przypisać bieżącego salda DOLO do Ethereum
ani Berachain. Taki stan występuje między innymi po pełnym wyjściu walleta z
pozycji.

## Zakres

Zmiana obejmuje wspólny renderer `freshChainCell()` w `dolo-preview.html`.
Dzięki temu dotyczy tabeli Bucket wallets w szczegółach DOLO Holder Distribution
Over Time oraz Fresh 10K+ DOLO Wallets, wyłącznie dla wierszy bez ustalonej
sieci.

## Projekt

- Widoczna wartość pustej sieci będzie miała postać pojedynczej, stonowanej
  kreski `—`, bez ikony lub chipu sieci.
- Element otrzyma opis dostępności `No current chain balance`, aby czytniki
  ekranowe otrzymały znaczenie stanu zamiast samego znaku.
- Ethereum i Berachain pozostaną przedstawione dokładnie tak jak obecnie.
- Wartość sortowania `unknown` pozostanie bez zmian. Nie zmieniamy danych,
  klasyfikacji walletów ani zasad ustalania sieci.

## Poza zakresem

- Nie próbujemy zgadywać historycznej sieci na podstawie dawnych transferów.
- Nie zmieniamy sald, grup walletów, filtrów ani liczników holderów.

## Weryfikacja

1. Test kontraktowy będzie wymagał neutralnej kreski oraz opisu dostępności dla
   pustego zestawu sieci.
2. Test potwierdzi, że renderer nie pokaże słowa `Unknown` w tym stanie.
3. Kontrolowane sprawdzenie w przeglądarce potwierdzi style i brak błędów
   JavaScript na lokalnym serwerze HTTP.
