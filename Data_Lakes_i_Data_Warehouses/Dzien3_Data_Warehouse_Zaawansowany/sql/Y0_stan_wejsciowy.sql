-- =============================================================================
-- Dzien 3 (wersja zaawansowana), Y0 - upload doplywu i stan wejsciowy po dniu 2
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
--
-- Bucket s3://aurora-<inicjaly>-lake/ i baza Glue aurora_<inicjaly> JUZ ISTNIEJA
-- z dni 1-2. Ten plik NIE tworzy bucketu ani bazy. Nie ma tu CREATE DATABASE.
--
-- Zrodlo na dysku: Dzien3_Data_Warehouse_Zaawansowany/dane/ (jeden folder).
-- Prefiksy dnia 1 i bronze/shipments/ z Z0 zostaja. Wgrywamy tylko nowe:
--   dane/bronze/customers_updates/customers_updates.csv
--     -> s3://aurora-<inicjaly>-lake/bronze/customers_updates/
--   dane/bronze/orders/dt=2026-01-20/orders.csv
--     -> s3://aurora-<inicjaly>-lake/bronze/orders/dt=2026-01-20/
--   dane/bronze/order_items_20260120/order_items_2026-01-20.csv
--     -> s3://aurora-<inicjaly>-lake/bronze/order_items_20260120/
--   dane/bronze/shipments_20260121/shipments_2026-01-21.csv
--     -> s3://aurora-<inicjaly>-lake/bronze/shipments_20260121/
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Nowe tabele Bronze. bronze_orders i bronze_shipments z dni 1-2 zostaja.
-- -----------------------------------------------------------------------------
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

CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_order_items_20260120 (
  order_id string,
  product_id int,
  qty int,
  unit_price_pln double,
  discount_pln double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/order_items_20260120/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_shipments_20260121 (
  shipment_id  string,
  order_id     string,
  carrier      string,
  shipped_ts   string,
  delivered_ts string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/shipments_20260121/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Katalog Glue nie przeszukuje S3 sam. Wariant punktowy zamiast MSCK:
--   ALTER TABLE aurora_jk.bronze_orders ADD IF NOT EXISTS
--   PARTITION (dt = '2026-01-20') LOCATION 's3://aurora-jk-lake/bronze/orders/dt=2026-01-20/';
MSCK REPAIR TABLE aurora_jk.bronze_orders;

-- -----------------------------------------------------------------------------
-- Kontrola Bronze (ORDER BY tabela)
-- -----------------------------------------------------------------------------
-- Oczekiwane:
--   bronze_customers_updates      8
--   bronze_order_items_20260120  40
--   bronze_orders               240  (90 + 110 + 40; bez MSCK partycja 20.01 znika)
--   bronze_shipments_20260121    88
SELECT 'bronze_customers_updates' AS tabela, COUNT(*) AS n FROM aurora_jk.bronze_customers_updates
UNION ALL SELECT 'bronze_order_items_20260120', COUNT(*) FROM aurora_jk.bronze_order_items_20260120
UNION ALL SELECT 'bronze_orders',               COUNT(*) FROM aurora_jk.bronze_orders
UNION ALL SELECT 'bronze_shipments_20260121',   COUNT(*) FROM aurora_jk.bronze_shipments_20260121
ORDER BY tabela;

-- Oczekiwane: 2026-01-15 90 | 2026-01-16 110 | 2026-01-20 40.
SELECT dt, COUNT(*) AS n FROM aurora_jk.bronze_orders GROUP BY dt ORDER BY dt;

-- -----------------------------------------------------------------------------
-- Kontrola Gold dw_* z dnia 2 - nic jeszcze nie MERGE-ujemy
-- -----------------------------------------------------------------------------
-- Oczekiwane (ORDER BY tabela):
--   dw_dim_customer 81 - jeden wiersz to jeden klient (80 + wiersz -1), wszyscy is_current
--   dw_fact_daily_sales 36 - jeden wiersz to dzien x kategoria x kanal
--   dw_fact_order_fulfillment 170 - jeden wiersz to jedno zamowienie (paid/shipped)
--   dw_fact_order_item 409 - jeden wiersz to jedna pozycja zamowienia (nie 486:
--     cancelled odpadly w Silver dnia 1)
SELECT 'dw_dim_customer' AS tabela, COUNT(*) AS n FROM aurora_jk.dw_dim_customer
UNION ALL SELECT 'dw_fact_order_item',        COUNT(*) FROM aurora_jk.dw_fact_order_item
UNION ALL SELECT 'dw_fact_order_fulfillment', COUNT(*) FROM aurora_jk.dw_fact_order_fulfillment
UNION ALL SELECT 'dw_fact_daily_sales',       COUNT(*) FROM aurora_jk.dw_fact_daily_sales
ORDER BY tabela;
