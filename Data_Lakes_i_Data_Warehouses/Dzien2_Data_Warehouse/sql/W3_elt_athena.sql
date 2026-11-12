-- =============================================================================
-- Dzien 2, W3 - ELT Silver -> Gold w Athenie
-- =============================================================================
-- SILNIK: AMAZON ATHENA. NIE Redshift - Redshift nie zna CTAS-a ze skladnia
-- WITH (format = 'PARQUET', external_location = ...).
--
-- NAZWY: dwuczlonowo, `aurora_<inicjaly>.tabela` - baza w Glue Data Catalog
-- plus nazwa tabeli. Ta sama warstwa Gold ogladana z Redshifta nazywa sie
-- "awsdatacatalog"."aurora_<inicjaly>"."gold_..." - wyjasnienie w 03_redshift.sql.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
--
-- Narzedziem jest CTAS (CREATE TABLE AS SELECT): Athena wykonuje SELECT,
-- zapisuje wynik na S3 jako nowe pliki Parquet i rejestruje tabele w Glue Data
-- Catalog. To jest litera "T" z ELT - transformacja zwyklym SQL-em, bez Glue Job
-- i bez Sparka.
--
-- Gold NICZEGO NIE FILTRUJE. Silver odfiltrowal `cancelled` w dniu 1. Gold
-- zajmuje sie wylacznie ukladem: zamienia dane ulozone pod zrodlo na dane
-- ulozone pod pytania. Gdyby Gold jeszcze cos odrzucal, znaczyloby to, ze Silver
-- nie dokonczyl roboty.
--
-- KOLEJNOSC MA ZNACZENIE: najpierw wszystkie wymiary, potem fakt. Fakt zlacza
-- sie z wymiarami po natural key i podmienia je na surrogate key, wiec
-- wymiary musza juz istniec. Wewnatrz grupy wymiarow kolejnosc jest dowolna.
--
-- Wszystkie CREATE maja IF NOT EXISTS - caly plik mozna puscic ponownie.
-- Zastrzezenie: po DROP TABLE, gdy pliki zostaly na S3, pojawi sie
-- HIVE_PATH_ALREADY_EXISTS. IF NOT EXISTS przed tym NIE chroni, bo tabeli
-- w katalogu juz nie ma - trzeba skasowac prefiks gold/ w S3 recznie.
--
-- UWAGA NA TYPY. ROW_NUMBER(), year(), month() i day() zwracaja BIGINT.
-- Bez jawnego CAST(... AS integer) lancuch wyglada tak:
--   bigint w SELECT -> int64 w pliku Parquet -> COPY do kolumny int (int32)
--   w Redshifcie (W5) przerywa cale ladowanie na niezgodnosci typow.
-- Blad pojawia sie wiec godzine pozniej, w innym narzedziu, na innym ekranie
-- niz jego przyczyna. Sprawdzone kolumna po kolumnie na realnych plikach z S3.
--
-- GDZIE LADUJA PLIKI: kazdy CTAS ma jawne external_location, wiec Parquet trafia
-- pod s3://aurora-jk-lake/gold/<nazwa>/. W5 czyta instrukcja COPY dokladnie te
-- sciezki - warto je zanotowac.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_date
-- -----------------------------------------------------------------------------
-- date_sk liczony arytmetycznie w konwencji RRRRMMDD (20260115): 2026 * 10000
-- + 1 * 100 + 15. Czytelny przy debugowaniu i stabilny miedzy przeladowaniami,
-- bo wynika z daty, a nie z kolejnosci wierszy.
--
-- date_format() oczekuje timestampa, wiec na kolumnie typu date wymaga CAST-a.
-- '%W' to pelna nazwa dnia tygodnia po angielsku (Thursday, Friday).
--
-- day_of_week(): 1 = poniedzialek ... 7 = niedziela, stad IN (6, 7) na weekend.
-- Numeracja rozni sie miedzy silnikami - w PostgreSQL 0 = niedziela - wiec przy
-- przenoszeniu tego kodu warunek trzeba sprawdzic, a nie przepisac.
--
-- Zrodlem sa daty wystepujace w pozycjach zamowien. W produkcji wymiar daty
-- generuje sie niezaleznie od faktu, na kilka lat w przod, wlasnie zeby dzien
-- bez sprzedazy mial swoj wiersz.
CREATE TABLE IF NOT EXISTS aurora_jk.gold_dim_date
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/gold/dim_date/'
) AS
SELECT
  CAST(year(d) * 10000 + month(d) * 100 + day(d) AS integer) AS date_sk,
  d AS date_actual,
  CAST(year(d) AS integer)  AS year,
  CAST(month(d) AS integer) AS month,
  CAST(day(d) AS integer)   AS day,
  date_format(CAST(d AS timestamp), '%W') AS day_name,
  CASE WHEN day_of_week(d) IN (6, 7) THEN true ELSE false END AS is_weekend
FROM (
  SELECT DISTINCT order_date AS d
  FROM aurora_jk.silver_order_items
) x;

-- Oczekiwane: 2 wiersze (2026-01-15, 2026-01-16).
-- date_sk = 20260115 i 20260116. Oba dni to czwartek i piatek, wiec
-- is_weekend = false w obu wierszach.

-- -----------------------------------------------------------------------------
-- dim_channel
-- -----------------------------------------------------------------------------
-- DISTINCT w podzapytaniu jest obowiazkowy. Bez niego wymiar mialby tyle
-- wierszy, ile zamowien (170), kazde z osobnym channel_sk - a fakt zlaczony
-- z takim wymiarem zdublowalby wiersze. To najczestsza przyczyna sytuacji
-- "Gold ma wiecej wierszy niz Silver".
CREATE TABLE IF NOT EXISTS aurora_jk.gold_dim_channel
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/gold/dim_channel/'
) AS
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY channel) AS integer) AS channel_sk,
  channel AS channel_code
FROM (SELECT DISTINCT channel FROM aurora_jk.silver_orders) s;

-- Oczekiwane: 3 wiersze - marketplace (1), mobile (2), web (3).
-- Numeracja wynika z ORDER BY channel, czyli z alfabetu.

-- -----------------------------------------------------------------------------
-- dim_customer
-- -----------------------------------------------------------------------------
-- ORDER BY w ROW_NUMBER() nie jest ozdobnikiem. Bez deterministycznej kolejnosci
-- ten sam klient przy ponownym przeliczeniu wymiaru moglby dostac inny
-- customer_sk - a fakt wskazywalby wtedy na kogos innego. W produkcji te role
-- pelni sekwencja albo IDENTITY, wlasnie po to, zeby klucz raz nadany sie
-- nie zmienil.
--
-- valid_from = 1900-01-01, a NIE NULL. Znaczy "ta wersja obowiazuje od zawsze".
-- Powod jest praktyczny i ujawnia sie dopiero jutro. Zlaczenie SCD Type 2 to:
--   ON i.customer_id = c.customer_id
--  AND i.order_date BETWEEN c.valid_from AND c.valid_to
-- BETWEEN z NULL-em po ktorejkolwiek stronie daje NULL, a nie TRUE - i NIE
-- DOPASOWUJE NICZEGO. Wiersze faktu dla takich klientow wypadaja na INNER JOIN
-- po cichu, bez bledu i bez ostrzezenia. Jedynym sygnalem jest rekoncyliacja
-- z W4 pkt 4. Ta sama zasada dotyczy valid_to = 9999-12-31.
--
-- Wartosci brzegowe zamiast NULL-i to standardowy wzorzec w SCD Type 2 i warto
-- go stosowac od pierwszego dnia, a nie dopisywac po pierwszym rozjezdzie.
CREATE TABLE IF NOT EXISTS aurora_jk.gold_dim_customer
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/gold/dim_customer/'
) AS
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY customer_id) AS integer) AS customer_sk,
  customer_id,
  first_name,
  last_name,
  city,
  voivodeship,
  segment,
  signup_date,
  DATE '1900-01-01' AS valid_from,
  DATE '9999-12-31' AS valid_to,
  true AS is_current
FROM aurora_jk.silver_customers;

-- Oczekiwane: 80 wierszy, customer_sk od 1 do 80, is_current = true wszedzie.

-- -----------------------------------------------------------------------------
-- dim_product
-- -----------------------------------------------------------------------------
-- unit_price_pln z produktu to cena KATALOGOWA, stad zmiana nazwy na
-- list_price_pln. Cena rzeczywiscie zaplacona siedzi na faktcie i moze sie od
-- niej roznic - mieszanie tych dwoch to klasyczne zrodlo rozjazdu w raportach.
-- Ta sama nazwa kolumny w dwoch miejscach o dwoch roznych znaczeniach to
-- zaproszenie do bledu, ktory wyjdzie dopiero na spotkaniu z biznesem.
CREATE TABLE IF NOT EXISTS aurora_jk.gold_dim_product
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/gold/dim_product/'
) AS
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY product_id) AS integer) AS product_sk,
  product_id,
  sku,
  product_name,
  category,
  brand,
  unit_price_pln AS list_price_pln
FROM aurora_jk.silver_products;

-- Oczekiwane: 25 wierszy, product_sk od 1 do 25, szesc roznych kategorii
-- (AGD, Akcesoria, Audio, Komputery, Telefony, TV).

-- -----------------------------------------------------------------------------
-- fact_order_item
-- -----------------------------------------------------------------------------
-- Piec zlaczen, kazde po natural key, a na fakt trafia tylko *_sk.
--
-- Czwarte zlaczenie - do silver_orders - jest jedynym, ktore nie idzie do
-- wymiaru. Kanal jest atrybutem ZAMOWIENIA, nie pozycji, a silver_order_items
-- tej kolumny nie ma. Bez tego przeskoku nie da sie dojsc do channel_code.
--
-- Wszystkie zlaczenia sa INNER JOIN, wiec kazdy brak w wymiarze po cichu usuwa
-- wiersz faktu. W produkcji stosuje sie zamiast tego wiersz nieznany
-- (*_sk = -1) i LEFT JOIN z COALESCE - wtedy fakt nigdy nie traci wiersza,
-- a problem jest widoczny w raporcie zamiast z niego znikac.
--
-- KOLUMNA PARTYCJONUJACA MUSI BYC OSTATNIA. partitioned_by = ARRAY['dt'] NIE
-- wskazuje kolumny po nazwie - Athena bierze tyle OSTATNICH kolumn z SELECT,
-- ile jest elementow w tablicy. Przy dt w srodku listy partycjonowanie poleci
-- po net_amount_pln i utworzy na S3 setki katalogow net_amount_pln=1234.56/.
-- Komunikat bledu, jesli w ogole sie pojawi, nie wskaze przyczyny.
CREATE TABLE IF NOT EXISTS aurora_jk.gold_fact_order_item
WITH (
  format            = 'PARQUET',
  partitioned_by    = ARRAY['dt'],
  external_location = 's3://aurora-jk-lake/gold/fact_order_item/'
) AS
SELECT
  i.order_id,
  d.date_sk,
  c.customer_sk,
  p.product_sk,
  ch.channel_sk,
  i.qty,
  i.unit_price_pln,
  i.discount_pln,
  i.net_amount_pln,
  i.dt              -- kolumna partycjonujaca: OSTATNIA na liscie
FROM aurora_jk.silver_order_items i
JOIN aurora_jk.gold_dim_date d     ON i.order_date  = d.date_actual
JOIN aurora_jk.gold_dim_customer c ON i.customer_id = c.customer_id
JOIN aurora_jk.gold_dim_product p  ON i.product_id  = p.product_id
JOIN aurora_jk.silver_orders o     ON i.order_id    = o.order_id
JOIN aurora_jk.gold_dim_channel ch ON o.channel     = ch.channel_code;

-- Oczekiwane: 409 wierszy w dwoch partycjach (dt=2026-01-15, dt=2026-01-16).
-- Kolumna dt zostaje w katalogu Glue, ale NIE jest zapisana w pliku Parquet -
-- siedzi w nazwie katalogu. Dlatego tabela w Redshifcie (W5) ma 9 kolumn,
-- a nie 10.

-- =============================================================================
-- KONTROLA
-- =============================================================================

-- Oczekiwane: silver_rows = gold_rows = 409.
-- Gold MNIEJSZY oznacza, ze ktorys INNER JOIN do wymiaru nie trafil i po cichu
--   usunal wiersze. Diagnostyka jest w nastepnym zapytaniu.
-- Gold WIEKSZY oznacza duplikat natural key w ktoryms wymiarze - zwykle
--   brak DISTINCT w podzapytaniu budujacym wymiar.
SELECT
  (SELECT COUNT(*) FROM aurora_jk.silver_order_items)   AS silver_rows,
  (SELECT COUNT(*) FROM aurora_jk.gold_fact_order_item) AS gold_rows;

-- Diagnostyka zgubionych wierszy: kazda kolumna ponizej powinna dac 0.
-- Wartosc rozna od zera wskazuje wymiar, w ktorym brakuje wierszy.
SELECT
  COUNT(*)                        AS wszystkie,
  COUNT(*) - COUNT(d.date_sk)     AS bez_daty,
  COUNT(*) - COUNT(c.customer_sk) AS bez_klienta,
  COUNT(*) - COUNT(p.product_sk)  AS bez_produktu
FROM aurora_jk.silver_order_items i
LEFT JOIN aurora_jk.gold_dim_date d     ON i.order_date  = d.date_actual
LEFT JOIN aurora_jk.gold_dim_customer c ON i.customer_id = c.customer_id
LEFT JOIN aurora_jk.gold_dim_product p  ON i.product_id  = p.product_id;

-- Diagnostyka duplikatow w wymiarze: oczekiwane 0 wierszy w wyniku.
SELECT customer_id, COUNT(*) AS wersje
FROM aurora_jk.gold_dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Rekoncyliacja. Oczekiwane: 1427124.00 w obu wierszach, co do grosza.
-- ORDER BY, bo UNION ALL sam nie gwarantuje kolejnosci wierszy.
SELECT 'silver' AS layer, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
UNION ALL
SELECT 'gold', ROUND(SUM(net_amount_pln), 2)
FROM aurora_jk.gold_fact_order_item
ORDER BY layer;

-- Licznosci wymiarow. Oczekiwane (kolejnosc z ORDER BY wymiar):
--   dim_channel 3 | dim_customer 80 | dim_date 2 | dim_product 25
SELECT 'dim_date' AS wymiar, COUNT(*) AS n FROM aurora_jk.gold_dim_date
UNION ALL SELECT 'dim_channel',  COUNT(*) FROM aurora_jk.gold_dim_channel
UNION ALL SELECT 'dim_customer', COUNT(*) FROM aurora_jk.gold_dim_customer
UNION ALL SELECT 'dim_product',  COUNT(*) FROM aurora_jk.gold_dim_product
ORDER BY wymiar;

-- Zarejestrowane partycje faktu. Oczekiwane: dt=2026-01-15 i dt=2026-01-16.
-- CTAS rejestruje partycje sam, wiec MSCK REPAIR nie jest tu potrzebny -
-- inaczej niz przy CREATE EXTERNAL TABLE na gotowych plikach z dnia 1.
SHOW PARTITIONS aurora_jk.gold_fact_order_item;
