-- =============================================================================
-- Dzien 3, W6 - SCD Type 2 na wymiarze klienta (Athena)
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela. Caly
-- dzien 3 dzieje sie w Athenie; gdyby ktores z tych zapytan mialo pojsc
-- z Query Editora v2 Redshifta, nazwy zmieniaja sie na trzyczlonowe
-- ("awsdatacatalog"."aurora_<inicjaly>"."tabela") - patrz naglowek
-- ../../Dzien2_Data_Warehouse/sql/W5_redshift.sql.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Opis krok po kroku: ../02_rozwiazania.md
--
-- IDEA SCD TYPE 2: zmiana atrybutu nie nadpisuje wiersza. Stara wersja zostaje
-- zamknieta (valid_to = dzien przed zmiana, is_current = false), a obok powstaje
-- nowa z nowym surrogate key. Dzieki temu fakt sprzed zmiany dalej wskazuje stan
-- sprzed zmiany, a raport historyczny nie zmienia sie z dnia na dzien.
--
-- Dla porownania: Type 1 nadpisuje wartosc i historia znika, Type 3 trzyma
-- poprzednia wartosc w dodatkowej kolumnie i pamieta tylko jedna zmiane wstecz.
--
-- gold_dim_customer z dnia 2 ZOSTAJE NIETKNIETY. Budujemy obok nowa tabele,
-- wiec da sie porownac oba warianty i wrocic po pomylce. W produkcji SCD2
-- nadpisuje wymiar w miejscu - ale tam istnieje backup i srodowisko przejsciowe.
--
-- Oba CREATE maja IF NOT EXISTS, wiec caly plik mozna puscic ponownie bez bledu.
-- Zastrzezenie: po DROP TABLE, gdy pliki zostaly na S3, pojawi sie
-- HIVE_PATH_ALREADY_EXISTS - IF NOT EXISTS przed tym NIE chroni.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Krok 1. Tabela zewnetrzna na pliku z aktualizacjami
-- -----------------------------------------------------------------------------
-- Plik ma te same kolumny co customers, plus valid_from z data obowiazywania
-- zmiany. Wszystkie kolumny sa string, lacznie z datami: OpenCSVSerde czyta
-- kazda kolumne jako tekst i dopiero rzutuje na typ zadeklarowany, a dla dat
-- bywa to zawodne. Konwersja na date nastepuje w CTAS-ie ponizej.

CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_customers_updates (
  customer_id string,
  email string,
  first_name string,
  last_name string,
  city string,
  voivodeship string,
  segment string,
  signup_date string,
  valid_from string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/customers_updates/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Oczekiwane: 8 wierszy, valid_from = 2026-01-20 w kazdym.
-- Kazdy customer_id z tej osemki istnieje juz w gold_dim_customer:
--   C0001 Gdansk->Bialystok   C0004 Wroclaw->Katowice   C0009 Krakow->Katowice
--   C0012 Krakow->Bialystok   C0018 Poznan->Gdansk      C0023 Poznan->Wroclaw
--   C0031 Katowice->Gdansk    C0044 Lublin->Warszawa
SELECT COUNT(*) AS updates_n FROM aurora_jk.bronze_customers_updates;

-- -----------------------------------------------------------------------------
-- Krok 2. Wymiar SCD Type 2
-- -----------------------------------------------------------------------------
-- Struktura: dwie czesci sklejone przez UNION ALL.
--
--   base      - wszystkie 80 wierszy z dnia 2, z TYM SAMYM customer_sk.
--               Osmiu zmienionym domykamy okres waznosci (valid_to = 2026-01-19,
--               is_current = false); pozostalym 72 zostaje 9999-12-31 i true.
--               Jeden LEFT JOIN obsluguje obie grupy naraz - roznia sie
--               wylacznie tym, czy zlaczenie trafilo.
--
--   new_rows  - osiem nowych wersji z NOWYM customer_sk (1000 + numer).
--
-- ZACHOWANIE STAREGO SK W WERSJI HISTORYCZNEJ JEST TU NAJWAZNIEJSZE. Fakt
-- z dnia 2 (409 wierszy) wskazuje wlasnie na te klucze i nie jest przeliczany.
-- Nowy klucz w starym wierszu oznaczalby, ze caly styczniowy fakt nie ma
-- do czego trafic.
--
-- KAZDA KOLUMNA JEST KWALIFIKOWANA ALIASEM (d. / u.). Obie tabele maja
-- customer_id, city, segment i valid_from - bez aliasu Athena zwraca
-- "Column 'customer_id' is ambiguous". To regula przy laczeniu wymiaru
-- z jego wlasna aktualizacja, nie literowka.
--
-- WARTOSCI BRZEGOWE ZAMIAST NULL-i:
--   valid_to = 9999-12-31 dla wersji otwartej, a NIE NULL. Zlaczenie faktu
--   z wymiarem to order_date BETWEEN valid_from AND valid_to, a BETWEEN
--   z NULL-em po ktorejkolwiek stronie daje NULL zamiast TRUE i NIE DOPASOWUJE
--   NICZEGO. Wiersze faktu wypadaja wtedy na INNER JOIN po cichu, bez bledu.
--   valid_from = 1900-01-01 w wierszach z dnia 2 dziala z tego samego powodu.

CREATE TABLE IF NOT EXISTS aurora_jk.gold_dim_customer_scd
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/gold/dim_customer_scd/'
) AS
WITH upd AS (
  -- Rzutowanie tekstu na date w jednym miejscu, zeby nizej operowac juz
  -- na typie date - DATE_ADD i BETWEEN na stringu daloby cichy blad.
  SELECT
    u.customer_id,
    u.first_name,
    u.last_name,
    u.city,
    u.voivodeship,
    u.segment,
    CAST(u.signup_date AS date) AS signup_date,
    CAST(u.valid_from AS date)  AS valid_from
  FROM aurora_jk.bronze_customers_updates u
),
base AS (
  -- Wiersze historyczne: te same SK co w dniu 2, wiec stary fakt dalej sie spina.
  --
  -- DATE_ADD('day', -1, u.valid_from) to skladnia Trino: jednostka jako tekst,
  -- liczba (ujemna cofa), data. Wersja zamykana konczy sie DZIEN PRZED
  -- poczatkiem nowej. Przy valid_to = valid_from nowej wersji zapytanie
  -- o 20 stycznia dopasowaloby OBIE wersje naraz i podwoilo przychod.
  --
  -- COALESCE zwraca pierwsza wartosc rozna od NULL, czyli tutaj: 9999-12-31
  -- wszedzie tam, gdzie LEFT JOIN nie znalazl aktualizacji.
  SELECT
    d.customer_sk,
    d.customer_id,
    d.first_name,
    d.last_name,
    d.city,
    d.voivodeship,
    d.segment,
    d.signup_date,
    d.valid_from,
    COALESCE(DATE_ADD('day', -1, u.valid_from), DATE '9999-12-31') AS valid_to,
    CASE WHEN u.customer_id IS NULL THEN true ELSE false END       AS is_current
  FROM aurora_jk.gold_dim_customer d
  LEFT JOIN upd u
    ON d.customer_id = u.customer_id
),
new_rows AS (
  -- Nowe wersje osemki. Przesuniecie o 1000 to najtanszy sposob na unikniecie
  -- kolizji: stare klucze zajmuja 1-80, nowe ida od 1001 do 1008.
  --
  -- CAST(... AS integer) jest obowiazkowy: ROW_NUMBER() zwraca bigint, Parquet
  -- zapisze to jako int64, a COPY do kolumny int w Redshifcie przerwie cale
  -- ladowanie na niezgodnosci typow. Ta sama pulapka co w ELT dnia 2.
  SELECT
    CAST(1000 + ROW_NUMBER() OVER (ORDER BY u.customer_id) AS integer) AS customer_sk,
    u.customer_id,
    u.first_name,
    u.last_name,
    u.city,
    u.voivodeship,
    u.segment,
    u.signup_date,
    u.valid_from,
    DATE '9999-12-31' AS valid_to,
    true              AS is_current
  FROM upd u
)
-- UNION ALL, nie UNION: UNION usuwalby duplikaty i kosztowal sortowanie,
-- a tutaj z zalozenia zaden wiersz nie powtarza sie miedzy czesciami.
SELECT * FROM base
UNION ALL
SELECT * FROM new_rows;

-- =============================================================================
-- KONTROLA
-- =============================================================================

-- Weryfikacja 1. Oczekiwane: dokladnie 8 customer_id, kazdy w 2 wersjach.
-- Wiecej niz 8 oznacza, ze czesc base weszla dwa razy.
SELECT customer_id, COUNT(*) AS wersje
FROM aurora_jk.gold_dim_customer_scd
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY customer_id;

-- Weryfikacja 2. Oczekiwane: rows_all = 88 (80 + 8), rows_current = 80.
-- Kazdy klient ma dokladnie jedna wersje aktualna, takze ta osemka.
-- rows_current = 88 oznacza, ze zamykanie starej wersji nie trafilo -
-- najczesciej INNER JOIN zamiast LEFT JOIN w czesci base.
SELECT COUNT(*) AS rows_all,
       COUNT_IF(is_current) AS rows_current
FROM aurora_jk.gold_dim_customer_scd;

-- Weryfikacja 3. Oczekiwane: ZERO wierszy.
-- Zapytanie szuka par wersji tego samego klienta, ktorych okresy waznosci
-- na siebie zachodza. Warunki a.valid_from <= b.valid_to AND b.valid_from <=
-- a.valid_to to standardowy test przeciecia dwoch przedzialow.
-- Gdyby cos zwrocilo, ten sam wiersz faktu pasowalby do dwoch wersji wymiaru
-- i przychod policzylby sie podwojnie - przy liczbie wierszy faktu
-- wygladajacej calkowicie normalnie.
SELECT a.customer_id
FROM aurora_jk.gold_dim_customer_scd a
JOIN aurora_jk.gold_dim_customer_scd b
  ON a.customer_id = b.customer_id
 AND a.customer_sk <> b.customer_sk
 AND a.valid_from <= b.valid_to
 AND b.valid_from <= a.valid_to;

-- Weryfikacja 4. Podglad obu wersji jednego klienta - najkrotsza ilustracja
-- tego, co robi SCD Type 2.
-- Oczekiwane dla C0001: sk = 1 (Gdansk, do 2026-01-19, is_current = false)
-- oraz sk = 1001 (Bialystok, od 2026-01-20, is_current = true).
SELECT customer_sk, customer_id, city, segment, valid_from, valid_to, is_current
FROM aurora_jk.gold_dim_customer_scd
WHERE customer_id = 'C0001'
ORDER BY valid_from;
