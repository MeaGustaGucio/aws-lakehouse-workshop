-- =============================================================================
-- Dzien 1, L3 - warstwa Bronze w Athenie
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Opis krok po kroku: ../02_rozwiazania.md
--
-- Wszystkie CREATE maja IF NOT EXISTS, wiec caly plik mozna uruchomic ponownie
-- bez bledu. Sprawdzone w Athenie engine v3.
--
-- Tabele sa EXTERNAL: katalog Glue trzyma tylko opis, dane zostaja na S3.
-- DROP TABLE kasuje opis, plikow nie rusza.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS aurora_jk;

-- -----------------------------------------------------------------------------
-- Slowniki bez partycji
-- -----------------------------------------------------------------------------
-- bronze_customers: jeden wiersz to jeden klient ze zrodla CRM (aktualny stan).
-- signup_date jest tu string, nie date. OpenCSVSerde czyta kazda kolumne jako
-- tekst i dopiero rzutuje na typ zadeklarowany - dla dat bywa to zawodne.
-- Konwersja na date nastepuje w warstwie Silver (L5).

CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_customers (
  customer_id string,
  email string,
  first_name string,
  last_name string,
  city string,
  voivodeship string,
  segment string,
  signup_date string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/customers/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- bronze_products: jeden wiersz to jeden produkt z katalogu (sku, kategoria, cena listowa).
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_products (
  product_id int,
  sku string,
  product_name string,
  category string,
  brand string,
  unit_price_pln double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/products/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- bronze_order_items: jeden wiersz to jedna pozycja zamowienia (order_id + produkt).
-- 486 pozycji, w tym te z zamowien cancelled - Bronze nic nie filtruje.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_order_items (
  order_id string,
  product_id int,
  qty int,
  unit_price_pln double,
  discount_pln double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/order_items/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- -----------------------------------------------------------------------------
-- Tabele partycjonowane po dt (katalog dt=RRRR-MM-DD na S3)
-- -----------------------------------------------------------------------------
-- Kolumny partycjonujacej NIE wymienia sie na liscie kolumn - jest osobno,
-- w PARTITIONED BY. Jej wartosc pochodzi z nazwy katalogu, nie z pliku.

-- bronze_orders: jeden wiersz to jedno zamowienie. Partycja dt pochodzi z katalogu na S3,
-- nie z kolumny w pliku. 200 zamowien = 90 dnia 15.01 + 110 dnia 16.01 (30 cancelled zostaje).
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_orders (
  order_id string,
  customer_id string,
  order_ts string,
  status string,
  channel string,
  delivery_city string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/orders/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Pliki events to JSON Lines: jeden obiekt na linie, bez przecinkow miedzy
-- obiektami i bez nawiasow tablicy. Zwykly "ladny" JSON sie nie wczyta.
-- bronze_events: jeden wiersz to jedno zdarzenie ze sklepu (JSON Lines). 935 zdarzen.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_events (
  event_id string,
  event_ts string,
  customer_id string,
  event_type string,
  page string,
  product_id int
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://aurora-jk-lake/bronze/events/';

-- Gdy OpenX JsonSerDe zawiedzie (plan B, nie domyslny wybor):
-- Objaw: CREATE przechodzi, SELECT → HIVE_CURSOR_ERROR lub same NULL mimo poprawnych
-- plikow JSON Lines. Przyczyna: parser OpenX nie rozpoznaje linii albo odrzuca
-- znaki specjalne w polach — wtedy HCatalog JsonSerDe bywa odporniejszy.
-- Procedura: DROP TABLE, podmiana ROW FORMAT SERDE na poniższą linię, CREATE, MSCK.
--   ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
-- W Athenie engine v3 wariant OpenX działa — fallback tylko gdy SELECT faktycznie pada.

-- -----------------------------------------------------------------------------
-- Rejestracja partycji
-- -----------------------------------------------------------------------------
-- Bez tego COUNT(*) = 0 mimo poprawnie wgranych plikow. Katalog Glue nie
-- przeszukuje S3 samoczynnie - MSCK REPAIR przechodzi po prefiksach i dopisuje
-- kazda znaleziona partycje.

MSCK REPAIR TABLE aurora_jk.bronze_orders;
MSCK REPAIR TABLE aurora_jk.bronze_events;

-- -----------------------------------------------------------------------------
-- KONTROLA
-- -----------------------------------------------------------------------------
-- Oczekiwane (w kolejnosci, w jakiej zwraca je ORDER BY tabela):
--   customers 80 | events 935 | order_items 486 | orders 200 | products 25
--
-- UNION ALL nie gwarantuje kolejnosci wierszy - Athena zwraca galezie w takiej
-- kolejnosci, w jakiej skonczyly sie liczyc. Bez ORDER BY ten sam SELECT potrafi
-- dac za kazdym razem inny uklad, dlatego na koncu jest sortowanie po etykiecie.
SELECT 'customers' AS tabela, COUNT(*) AS n FROM aurora_jk.bronze_customers
UNION ALL SELECT 'products',    COUNT(*) FROM aurora_jk.bronze_products
UNION ALL SELECT 'order_items', COUNT(*) FROM aurora_jk.bronze_order_items
UNION ALL SELECT 'orders',      COUNT(*) FROM aurora_jk.bronze_orders
UNION ALL SELECT 'events',      COUNT(*) FROM aurora_jk.bronze_events
ORDER BY tabela;

-- Oczekiwane: 2026-01-15 -> 90 wierszy, 2026-01-16 -> 110 wierszy
SELECT dt, COUNT(*) AS n
FROM aurora_jk.bronze_orders
GROUP BY dt
ORDER BY dt;
