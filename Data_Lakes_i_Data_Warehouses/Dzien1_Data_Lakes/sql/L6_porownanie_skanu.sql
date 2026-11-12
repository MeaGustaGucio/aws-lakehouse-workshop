-- =============================================================================
-- Dzien 1, L6 - to samo pytanie biznesowe trzema drogami
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk.
--
-- WARUNEK WSTEPNY: przelacznik "Reuse query results" nad wynikami musi byc
-- WYLACZONY. Athena zwraca inaczej zapisany wynik poprzedniego identycznego
-- zapytania i pokazuje zerowy skan, co psuje cale porownanie.
--
-- Athena rozlicza sie od liczby BAJTOW ODCZYTANYCH Z S3, nie od liczby wierszy
-- ani od czasu. Data scanned jest w zakladce Query stats pod wynikiem.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Wariant 1: Bronze, CSV, ze zlaczeniem
-- -----------------------------------------------------------------------------
-- Zmierzone w Athenie v3: Data scanned = 24 119 B
-- Oczekiwane kwoty: 653531.00 i 773593.00
SELECT
  o.dt,
  ROUND(SUM(i.qty * i.unit_price_pln - i.discount_pln), 2) AS revenue_pln
FROM aurora_jk.bronze_orders o
JOIN aurora_jk.bronze_order_items i ON o.order_id = i.order_id
WHERE o.status IN ('paid', 'shipped')
GROUP BY o.dt
ORDER BY o.dt;

-- -----------------------------------------------------------------------------
-- Wariant 2: Silver, Parquet, bez zlaczenia
-- -----------------------------------------------------------------------------
-- Zmierzone w Athenie v3: Data scanned = 1 717 B  (14x mniej niz wariant 1)
-- Kwoty MUSZA byc identyczne: 653531.00 i 773593.00
SELECT
  dt,
  ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
GROUP BY dt
ORDER BY dt;

-- -----------------------------------------------------------------------------
-- Wariant 3: Silver, jedna partycja
-- -----------------------------------------------------------------------------
-- Zmierzone w Athenie v3: Data scanned = 940 B
-- Oczekiwana kwota: 773593.00
SELECT ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
WHERE dt = '2026-01-16';

-- =============================================================================
-- DLACZEGO WARIANT 2 CZYTA MNIEJ NIZ WARIANT 1 - cztery rozne mechanizmy
-- =============================================================================
-- 1. UKLAD KOLUMNOWY
--    CSV trzyma dane wierszami, wiec zeby siegnac po qty trzeba przeczytac caly
--    wiersz razem z delivery_city i reszta. Parquet trzyma kazda kolumne osobno,
--    wiec Athena pobiera z S3 tylko kolumny wystepujace w zapytaniu.
--
-- 2. KOMPRESJA I KODOWANIE
--    Kwota 1234.56 w CSV to siedem bajtow tekstu. W Parquecie to liczba, ktora
--    po SNAPPY i kodowaniu slownikowym zajmuje ulamek tego miejsca - zwlaszcza
--    dla kolumn o powtarzalnych wartosciach (status, kanal, data).
--
-- 3. BRAK ZLACZENIA
--    Wariant 1 czyta dwie tabele i laczy je po order_id. Silver ma
--    net_amount_pln policzone juz przy budowie warstwy, wiec pyta jedna tabele.
--    Ta czesc oszczednosci wynika z MODELU, nie z formatu pliku.
--
-- 4. STATYSTYKI W STOPCE PLIKU
--    Parquet zapisuje min i max kazdej kolumny dla kazdej grupy wierszy, wiec
--    Athena pomija grupy, ktore na pewno nie zawieraja szukanych wartosci
--    (predicate pushdown).
--
-- WARIANT 3 doklada piaty mechanizm: PARTITION PRUNING. Filtr dt = '2026-01-16'
-- sprawia, ze Athena w ogole nie otwiera katalogu z 15.01. Jako jedyny z tych
-- mechanizmow nie zalezy od formatu - partycje dzialalyby tak samo na CSV.
--
-- ZASTRZEZENIE O KOSZCIE: Athena zaokragla rozliczenie do 10 MB na zapytanie,
-- a caly dataset to kilobajty - rachunek jest w obu wariantach taki sam i wynosi
-- praktycznie zero. Prawdziwa jest tu PROPORCJA, nie skala. Przy 200 GB CSV ten
-- sam stosunek to roznica miedzy 1 a 14 dolarami za pojedyncze zapytanie.
-- =============================================================================
