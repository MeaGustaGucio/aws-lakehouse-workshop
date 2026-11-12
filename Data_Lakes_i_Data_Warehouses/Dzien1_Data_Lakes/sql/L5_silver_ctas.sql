-- =============================================================================
-- Dzien 1, L5 - warstwa Silver w Parquecie
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
--
-- CTAS = CREATE TABLE AS SELECT. Athena wykonuje SELECT, ZAPISUJE jego wynik
-- na S3 jako nowe pliki i rejestruje tabele w katalogu. To jest litera "T"
-- z ELT - transformacja zwyklym SQL-em, bez Glue Job i bez Sparka.
--
-- Klauzula WITH (...) przed AS opisuje JAK zapisac (format, kompresja,
-- partycje, miejsce). Sam SELECT odpowiada tylko za tresc.
--
-- IDEMPOTENCJA: Athena nigdy nie kasuje danych z S3.
--   * bez IF NOT EXISTS drugie uruchomienie -> TABLE_ALREADY_EXISTS
--   * z IF NOT EXISTS drugie uruchomienie zostaje pominiete (sprawdzone)
--   * po DROP TABLE, gdy pliki zostaly -> HIVE_PATH_ALREADY_EXISTS,
--     i tu IF NOT EXISTS juz NIE pomaga. Trzeba skasowac prefiks w S3.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- silver_orders: jeden wiersz to jedno zamowienie paid/shipped (170, nie 200).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aurora_jk.silver_orders
WITH (
  format              = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by      = ARRAY['dt'],
  external_location   = 's3://aurora-jk-lake/silver/orders/'
) AS
SELECT
  order_id,
  customer_id,
  CAST(order_ts AS timestamp) AS order_ts,
  CAST(substr(order_ts, 1, 10) AS date) AS order_date,
  status,
  channel,
  delivery_city,
  dt   -- UWAGA: kolumna partycjonujaca musi byc OSTATNIA na liscie SELECT.
       -- partitioned_by nie wskazuje kolumny po nazwie - Athena bierze tyle
       -- ostatnich kolumn, ile jest w tablicy ARRAY[...].
FROM aurora_jk.bronze_orders
WHERE status IN ('paid', 'shipped');

-- -----------------------------------------------------------------------------
-- silver_order_items: jeden wiersz to jedna pozycja zamowienia paid/shipped.
-- 409, nie 486: JOIN z orders odrzuca 77 pozycji z 30 zamowien cancelled (L4 / dzien 1).
-- -----------------------------------------------------------------------------
-- Zlaczenie z orders nie jest ozdoba. Tabela order_items nie ma ani statusu,
-- ani daty, wiec bez niego nie da sie:
--   * odfiltrowac pozycji nalezacych do zamowien anulowanych,
--   * nadac pozycjom partycji dt.
CREATE TABLE IF NOT EXISTS aurora_jk.silver_order_items
WITH (
  format              = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by      = ARRAY['dt'],
  external_location   = 's3://aurora-jk-lake/silver/order_items/'
) AS
SELECT
  i.order_id,
  i.product_id,
  i.qty,
  i.unit_price_pln,
  i.discount_pln,
  (i.qty * i.unit_price_pln - i.discount_pln) AS net_amount_pln,
  o.customer_id,
  CAST(substr(o.order_ts, 1, 10) AS date) AS order_date,
  o.dt
FROM aurora_jk.bronze_order_items i
JOIN aurora_jk.bronze_orders o ON i.order_id = o.order_id
WHERE o.status IN ('paid', 'shipped');

-- -----------------------------------------------------------------------------
-- Slowniki - bez partycji, bo nie rosna z dnia na dzien
-- -----------------------------------------------------------------------------
-- silver_customers: jeden wiersz to jeden klient; signup_date juz jako date.
CREATE TABLE IF NOT EXISTS aurora_jk.silver_customers
WITH (
  format              = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location   = 's3://aurora-jk-lake/silver/customers/'
) AS
SELECT
  customer_id,
  email,
  first_name,
  last_name,
  city,
  voivodeship,
  segment,
  CAST(signup_date AS date) AS signup_date   -- string z Bronze -> date
FROM aurora_jk.bronze_customers;

-- silver_products: jeden wiersz to jeden produkt, typy zgodne z Bronze.
CREATE TABLE IF NOT EXISTS aurora_jk.silver_products
WITH (
  format              = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location   = 's3://aurora-jk-lake/silver/products/'
) AS
SELECT
  product_id,
  sku,
  product_name,
  category,
  brand,
  unit_price_pln
FROM aurora_jk.bronze_products;

-- -----------------------------------------------------------------------------
-- KONTROLA
-- -----------------------------------------------------------------------------
-- Oczekiwane (w kolejnosci, w jakiej zwraca je ORDER BY tabela):
--   customers 80 | order_items 409 | orders 170 | products 25
--
-- 409, a nie 486, bo order_items zostaly ZLACZONE z orders, a filtr statusu
-- dziala na kolumnie z tabeli zamowien. 30 zamowien ma status cancelled;
-- zlaczenie odrzuca wszystkie 77 nalezacych do nich pozycji: 486 - 77 = 409.
-- Bronze nadal ma komplet 486 wierszy - nic nie zginelo.
--
-- ORDER BY na koncu jest potrzebny: UNION ALL sam nie gwarantuje kolejnosci.
SELECT 'orders' AS tabela, COUNT(*) AS n FROM aurora_jk.silver_orders
UNION ALL SELECT 'order_items', COUNT(*) FROM aurora_jk.silver_order_items
UNION ALL SELECT 'customers',   COUNT(*) FROM aurora_jk.silver_customers
UNION ALL SELECT 'products',    COUNT(*) FROM aurora_jk.silver_products
ORDER BY tabela;

-- Oczekiwane: 2026-01-15 -> 653531.00 | 2026-01-16 -> 773593.00
-- Razem 1427124.00 - ta sama kwota co w L4 pkt 2.
SELECT dt, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
GROUP BY dt
ORDER BY dt;
