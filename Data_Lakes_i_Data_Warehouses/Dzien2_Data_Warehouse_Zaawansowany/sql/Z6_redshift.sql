-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z6 - Redshift Serverless: Spectrum nad Icebergiem,
-- tabele lokalne z DISTKEY/SORTKEY, EXPLAIN, historia zapytan, MV, UNLOAD
-- =============================================================================
-- SILNIK: AMAZON REDSHIFT SERVERLESS, Query Editor v2 (polaczenie: Federated
-- user, baza dev). NIE Athena.
--
-- Do podmiany: dw_jk -> dw_<inicjaly> (lokalny schemat), aurora_jk ->
-- aurora_<inicjaly> (baza Glue), aurora-jk-lake -> wlasny bucket.
--
-- Trzy formy nazw - te same co w wersji podstawowej (W5_redshift.sql):
--   "awsdatacatalog"."aurora_jk"."dw_fact_order_item"  - Glue przez Spectrum,
--   dw_jk.fact_order_item                                - tabela lokalna,
--   aurora_jk.dw_fact_order_item                         - zapis Athenowy, NIE
--                                                          dziala w Redshifcie.
--
-- Gdy Redshift Data API jest zablokowane przez SCP - wszystko ponizej idzie
-- z Query Editora v2. Kazda zakladka QEv2 ma wlasna sesje; Run wykonuje
-- zaznaczony fragment, a bez zaznaczenia cala zakladke.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Z6a. Spectrum czyta Iceberg - bez kopiowania i bez konfiguracji
-- -----------------------------------------------------------------------------
-- awsdatacatalog to automatycznie zamontowany, tylko do odczytu obraz Glue Data
-- Catalog tego konta. Redshift rozpoznaje tabele Iceberg po table_type
-- w Glue i czyta AKTUALNY snapshot - pliki usuniete przez MERGE/DELETE nie sa
-- widoczne, choc fizycznie leza jeszcze na S3. Dlatego COPY z katalogu data/
-- tabeli Iceberg jest bledem koncepcyjnym: COPY nie zna snapshotow i wczytalby
-- takze wiersze juz skasowane.
--
-- Oczekiwane: 409 | 1427124.00 - te same liczby co w Athenie.
SELECT COUNT(*) AS rows_n, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM "awsdatacatalog"."aurora_jk"."dw_fact_order_item";

-- Ten sam star schema, z Redshifta, wprost z data lake. Oczekiwane: 9 wierszy,
-- identyczne z Z5a w Athenie (2026-01-15 marketplace 233010 ... razem 1427124).
SELECT
  COALESCE(CAST(d.date_actual AS varchar), 'razem') AS dzien,
  COALESCE(s.channel, 'razem')                       AS kanal,
  ROUND(SUM(s.revenue_pln), 2)                       AS revenue_pln
FROM "awsdatacatalog"."aurora_jk"."dw_fact_daily_sales" s
JOIN "awsdatacatalog"."aurora_jk"."dw_dim_date" d ON s.order_date_sk = d.date_sk
GROUP BY ROLLUP (d.date_actual, s.channel)
ORDER BY GROUPING(d.date_actual), d.date_actual, GROUPING(s.channel), s.channel;

-- -----------------------------------------------------------------------------
-- Z6b. Schemat lokalny i tabele z jawnym rozkladem danych
-- -----------------------------------------------------------------------------
-- DISTSTYLE decyduje, na ktorym wezle laduje wiersz:
--   ALL  - pelna kopia na kazdym wezle; dla malych wymiarow (setki-tysiace
--          wierszy) zlaczenie nie przesyla nic po sieci,
--   KEY  - wiersze o tym samym DISTKEY na tym samym wezle; dla faktu klucz
--          zlaczenia z najwiekszym wymiarem (tu customer_sk),
--   EVEN - rownomiernie, bez ogladania sie na klucze,
--   AUTO - Redshift zaczyna od ALL, przechodzi na EVEN/KEY, gdy tabela rosnie.
-- SORTKEY to fizyczna kolejnosc na dysku: filtr po order_date_sk pomija cale
-- bloki (zone maps) - odpowiednik partition pruning wewnatrz silnika.
--
-- PRIMARY KEY / FOREIGN KEY sa INFORMACYJNE: optymalizator z nich korzysta,
-- silnik ich NIE egzekwuje. Duplikaty lapia asercje z Z4, nie baza.
--
-- Typy sa dobrane do kolumn Icebergu: string -> varchar(n), double -> double
-- precision, date -> date, timestamp(6) -> timestamp. Przy INSERT ... SELECT
-- Redshift dopasowuje kolumny PO KOLEJNOSCI, jak COPY.
CREATE SCHEMA IF NOT EXISTS dw_jk;

CREATE TABLE IF NOT EXISTS dw_jk.dim_date (
  date_sk      integer PRIMARY KEY,
  date_actual  date,
  year         integer,
  quarter      integer,
  month        integer,
  month_name   varchar(16),
  day          integer,
  day_of_week  integer,
  day_name     varchar(16),
  iso_week     integer,
  is_weekend   boolean
) DISTSTYLE ALL SORTKEY (date_sk);

CREATE TABLE IF NOT EXISTS dw_jk.dim_customer (
  customer_sk  integer PRIMARY KEY,
  customer_id  varchar(16),
  first_name   varchar(64),
  last_name    varchar(64),
  city         varchar(64),
  voivodeship  varchar(64),
  segment      varchar(32),
  signup_date  date,
  signup_month varchar(7),
  valid_from   date,
  valid_to     date,
  is_current   boolean
) DISTSTYLE ALL SORTKEY (customer_sk);

CREATE TABLE IF NOT EXISTS dw_jk.dim_product (
  product_sk     integer PRIMARY KEY,
  product_id     integer,
  sku            varchar(32),
  product_name   varchar(128),
  category       varchar(64),
  brand          varchar(64),
  list_price_pln double precision,
  price_band     varchar(16)
) DISTSTYLE ALL SORTKEY (product_sk);

CREATE TABLE IF NOT EXISTS dw_jk.dim_order_status (
  order_status_sk integer PRIMARY KEY,
  status          varchar(32),
  channel         varchar(32),
  is_revenue      boolean
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dw_jk.fact_order_item (
  order_id        varchar(16),
  order_date      date,
  order_date_sk   integer REFERENCES dw_jk.dim_date (date_sk),
  customer_sk     integer REFERENCES dw_jk.dim_customer (customer_sk),
  product_sk      integer REFERENCES dw_jk.dim_product (product_sk),
  order_status_sk integer REFERENCES dw_jk.dim_order_status (order_status_sk),
  qty             integer,
  unit_price_pln  double precision,
  discount_pln    double precision,
  net_amount_pln  double precision,
  load_ts         timestamp
)
DISTKEY (customer_sk)
SORTKEY (order_date_sk);

-- -----------------------------------------------------------------------------
-- Z6c. Ladowanie: INSERT ... SELECT przez Spectrum (nie COPY)
-- -----------------------------------------------------------------------------
-- Dla tabel Iceberg to wlasciwa droga: Redshift czyta aktualny snapshot
-- tozsamoscia zalogowanego uzytkownika i zapisuje do wlasnego magazynu.
-- WHERE NOT EXISTS (SELECT 1 FROM cel) laduje tylko do PUSTEJ tabeli - drugie
-- uruchomienie nic nie dokłada (COPY tej ochrony nie ma). Realne przeladowanie:
-- TRUNCATE tabeli i ponowny INSERT.
INSERT INTO dw_jk.dim_date
SELECT date_sk, date_actual, year, quarter, month, month_name, day,
       day_of_week, day_name, iso_week, is_weekend
FROM "awsdatacatalog"."aurora_jk"."dw_dim_date"
WHERE NOT EXISTS (SELECT 1 FROM dw_jk.dim_date);

INSERT INTO dw_jk.dim_customer
SELECT customer_sk, customer_id, first_name, last_name, city, voivodeship,
       segment, signup_date, signup_month, valid_from, valid_to, is_current
FROM "awsdatacatalog"."aurora_jk"."dw_dim_customer"
WHERE NOT EXISTS (SELECT 1 FROM dw_jk.dim_customer);

INSERT INTO dw_jk.dim_product
SELECT product_sk, product_id, sku, product_name, category, brand,
       list_price_pln, price_band
FROM "awsdatacatalog"."aurora_jk"."dw_dim_product"
WHERE NOT EXISTS (SELECT 1 FROM dw_jk.dim_product);

INSERT INTO dw_jk.dim_order_status
SELECT order_status_sk, status, channel, is_revenue
FROM "awsdatacatalog"."aurora_jk"."dw_dim_order_status"
WHERE NOT EXISTS (SELECT 1 FROM dw_jk.dim_order_status);

INSERT INTO dw_jk.fact_order_item
SELECT order_id, order_date, order_date_sk, customer_sk, product_sk,
       order_status_sk, qty, unit_price_pln, discount_pln, net_amount_pln, load_ts
FROM "awsdatacatalog"."aurora_jk"."dw_fact_order_item"
WHERE NOT EXISTS (SELECT 1 FROM dw_jk.fact_order_item);

-- Oczekiwane (ORDER BY t): dim_customer 81 | dim_date 366 | dim_order_status 9 |
-- dim_product 26 | fact_order_item 409. Po drugim uruchomieniu: te same liczby.
SELECT 'dim_date' AS t, COUNT(*) AS n FROM dw_jk.dim_date
UNION ALL SELECT 'dim_customer',     COUNT(*) FROM dw_jk.dim_customer
UNION ALL SELECT 'dim_product',      COUNT(*) FROM dw_jk.dim_product
UNION ALL SELECT 'dim_order_status', COUNT(*) FROM dw_jk.dim_order_status
UNION ALL SELECT 'fact_order_item',  COUNT(*) FROM dw_jk.fact_order_item
ORDER BY t;

-- Ten sam star schema, trzeci silnik przechowywania. Oczekiwane: 9 wierszy,
-- identyczne jak w Z6a i jak w Athenie - razem 1427124.
SELECT
  COALESCE(CAST(d.date_actual AS varchar), 'razem') AS dzien,
  COALESCE(st.channel, 'razem')                      AS kanal,
  ROUND(SUM(f.net_amount_pln), 2)                    AS revenue_pln
FROM dw_jk.fact_order_item f
JOIN dw_jk.dim_date d          ON f.order_date_sk   = d.date_sk
JOIN dw_jk.dim_order_status st ON f.order_status_sk = st.order_status_sk
GROUP BY ROLLUP (d.date_actual, st.channel)
ORDER BY GROUPING(d.date_actual), d.date_actual, GROUPING(st.channel), st.channel;

-- -----------------------------------------------------------------------------
-- Z6d. EXPLAIN - jak Redshift wykonuje zlaczenie lokalne vs przez Spectrum
-- -----------------------------------------------------------------------------
-- W planie lokalnym warto wskazac: "XN Hash Join DS_DIST_ALL_NONE" - wymiar
-- z DISTSTYLE ALL nie wymaga przesylania danych (NONE). W planie Spectrum
-- pojawia sie "XN S3 Query Scan" - odczyt z S3 przez warstwe Spectrum, a
-- zlaczenie odbywa sie juz w Redshifcie. Koszty (cost=...) sa wzgledne,
-- nie sa sekundami.
EXPLAIN
SELECT p.category, SUM(f.net_amount_pln)
FROM dw_jk.fact_order_item f
JOIN dw_jk.dim_product p ON f.product_sk = p.product_sk
WHERE f.order_date_sk = 20260115
GROUP BY p.category;

EXPLAIN
SELECT p.category, SUM(f.net_amount_pln)
FROM "awsdatacatalog"."aurora_jk"."dw_fact_order_item" f
JOIN "awsdatacatalog"."aurora_jk"."dw_dim_product" p ON f.product_sk = p.product_sk
WHERE f.order_date_sk = 20260115
GROUP BY p.category;

-- -----------------------------------------------------------------------------
-- Z6e. Historia zapytan - SYS_QUERY_HISTORY (Serverless) zamiast SVL_*
-- -----------------------------------------------------------------------------
-- Widoki SVL_QUERY_SUMMARY / STL_* pochodza z klastrow provisioned; na
-- Serverless zrodlem prawdy sa widoki SYS_*. elapsed_time jest w mikrosekundach.
-- Oczekiwane: ostatnie zapytania tej tozsamosci, w tym oba EXPLAIN-y (query_type
-- UTILITY) i SELECT-y. Miedzy nimi widac "SELECT current_user;" i puste wiersze
-- typu OTHER - to Query Editor v2 sam odpytuje baze przy kazdym Run.
SELECT query_id, status, query_type,
       ROUND(elapsed_time / 1000000.0, 2) AS elapsed_s,
       returned_rows,
       LEFT(query_text, 70) AS query_text
FROM SYS_QUERY_HISTORY
WHERE user_id = current_user_id
  AND database_name = 'dev'
ORDER BY start_time DESC
LIMIT 10;

-- Spectrum osobno: co zeszlo z S3 na zapytania zewnetrzne. file_format = Iceberg
-- potwierdza, ze Redshift czytal snapshot Icebergu, a nie "goly" katalog Parquet.
-- Oczekiwane: po wierszu na kazda tabele Iceberg czytana w Z6a/Z6c, np.
-- dw_fact_order_item | Iceberg | scanned_files 2 | returned_rows 409 |
-- returned_bytes rzedu kilkunastu KB. total_partitions = -1: Iceberg ukrywa
-- partycje przed Spectrum (hidden partitioning), przycina je sam.
SELECT query_id, table_name, file_format, scanned_files, returned_rows, returned_bytes
FROM SYS_EXTERNAL_QUERY_DETAIL
ORDER BY start_time DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Z6f. Materialized view - agregat utrzymywany przez silnik
-- -----------------------------------------------------------------------------
-- To odpowiednik dw_fact_daily_sales z Atheny, tylko silnik sam wie, kiedy
-- jest nieaktualny. AUTO REFRESH NO: odswiezenie reczne (REFRESH MATERIALIZED
-- VIEW) - w klasie chcemy widziec moment, w ktorym dane sie zmieniaja.
-- Redshift nie zna IF NOT EXISTS dla MV - drugie uruchomienie zwraca
-- "relation already exists" i to jest bezpieczne.
CREATE MATERIALIZED VIEW dw_jk.mv_daily_sales
AUTO REFRESH NO
AS
SELECT f.order_date_sk, p.category, st.channel,
       COUNT(*) AS lines_n, SUM(f.qty) AS qty, SUM(f.net_amount_pln) AS revenue_pln
FROM dw_jk.fact_order_item f
JOIN dw_jk.dim_product p       ON f.product_sk      = p.product_sk
JOIN dw_jk.dim_order_status st ON f.order_status_sk = st.order_status_sk
GROUP BY f.order_date_sk, p.category, st.channel;

-- Oczekiwane: 36 | 1427124.00 - jak dw_fact_daily_sales w Athenie.
SELECT COUNT(*) AS rows_n, ROUND(SUM(revenue_pln), 2) AS revenue_pln
FROM dw_jk.mv_daily_sales;

-- -----------------------------------------------------------------------------
-- Z6g. UNLOAD - z powrotem do data lake
-- -----------------------------------------------------------------------------
-- Wynik zapytania laduje na S3 jako Parquet. To jest ostatnie ogniwo lakehouse:
-- to, co policzyl data warehouse, wraca do data lake i Athena czyta to jak kazda
-- inna tabele. IAM_ROLE default = rola namespace'u (ta sama, co przy COPY).
-- PARALLEL OFF - jeden plik zamiast pliku na slice (czytelniej w konsoli S3).
-- ALLOWOVERWRITE - drugie uruchomienie nadpisuje, zamiast konczyc sie bledem.
UNLOAD ('SELECT order_date_sk, category, channel, lines_n, qty, revenue_pln
         FROM dw_jk.mv_daily_sales')
TO 's3://aurora-jk-lake/export/daily_sales/'
IAM_ROLE default
FORMAT AS PARQUET
ALLOWOVERWRITE
PARALLEL OFF;

-- Dalszy ciag jest w ATHENIE (plik sql/Z6g_athena_po_unload.sql): tabela zewnetrzna
-- nad export/daily_sales/ i porownanie sumy z dw_fact_daily_sales.

-- -----------------------------------------------------------------------------
-- Po zajeciach
-- -----------------------------------------------------------------------------
-- DROP SCHEMA dw_jk CASCADE;   -- kasuje tabele, MV i dane w magazynie Redshifta
-- Workgroup i namespace kasuje osoba, ktora je stawiala. Tabele Iceberg i
-- eksport na S3 sprzatamy z Atheny / konsoli S3 (patrz 02_rozwiazania.md).
