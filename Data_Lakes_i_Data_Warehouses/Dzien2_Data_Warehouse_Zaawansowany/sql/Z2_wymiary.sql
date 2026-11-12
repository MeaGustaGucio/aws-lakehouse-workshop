-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z2 - wymiary customer, product, junk dimension
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Wymaga dw_dim_date z sql/Z1_wymiar_daty.sql (kontrola na koncu liczy tez date).
-- =============================================================================

-- dw_dim_customer: jeden wiersz to jeden klient w aktualnym stanie (is_current),
-- z surrogate key customer_sk. Kolumny SCD2 sa od razu, choc dzis kazdy ma jedna wersje.
-- -----------------------------------------------------------------------------
-- Kolumny valid_from / valid_to / is_current sa od razu, choc dzis kazdy klient
-- ma jedna wersje. Wartosci brzegowe 1900-01-01 i 9999-12-31 zamiast NULL:
-- BETWEEN z NULL-em nie dopasowuje niczego i fakt gubi wiersze po cichu.
--
-- customer_sk z ROW_NUMBER() OVER (ORDER BY customer_id) - kolejnosc jest
-- deterministyczna, wiec ten sam klient dostaje ten sam klucz przy kazdym
-- przeliczeniu. Jutro nowe wersje dostana klucze max(customer_sk) + n.
--
-- signup_month to atrybut pochodny pod analize kohort (Z5): liczy sie raz,
-- w wymiarze, a nie w kazdym zapytaniu.
CREATE TABLE IF NOT EXISTS aurora_jk.dw_dim_customer
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/dw/dim_customer/',
  is_external = false
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
  date_format(CAST(signup_date AS timestamp), '%Y-%m') AS signup_month,
  DATE '1900-01-01' AS valid_from,
  DATE '9999-12-31' AS valid_to,
  true              AS is_current
FROM aurora_jk.silver_customers;

INSERT INTO aurora_jk.dw_dim_customer
SELECT -1, 'UNKNOWN', 'Nieznany', 'Nieznany', 'Nieznany', 'Nieznany', 'Nieznany',
       DATE '1900-01-01', '1900-01', DATE '1900-01-01', DATE '9999-12-31', true
WHERE NOT EXISTS (SELECT 1 FROM aurora_jk.dw_dim_customer WHERE customer_sk = -1);

-- -----------------------------------------------------------------------------
-- dw_dim_product: jeden wiersz to jeden produkt (plus wiersz -1), z price_band.
-- -----------------------------------------------------------------------------
-- unit_price_pln z produktu to cena KATALOGOWA (list_price_pln). Cena
-- rzeczywiscie zaplacona siedzi na fakcie. price_band to klasyczny atrybut
-- "do grupowania" - biznes pyta o przedzialy cenowe, nie o kwoty.
CREATE TABLE IF NOT EXISTS aurora_jk.dw_dim_product
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/dw/dim_product/',
  is_external = false
) AS
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY product_id) AS integer) AS product_sk,
  product_id,
  sku,
  product_name,
  category,
  brand,
  unit_price_pln AS list_price_pln,
  CASE
    WHEN unit_price_pln < 200  THEN 'budget'
    WHEN unit_price_pln < 1000 THEN 'mid'
    ELSE 'premium'
  END AS price_band
FROM aurora_jk.silver_products;

INSERT INTO aurora_jk.dw_dim_product
SELECT -1, -1, 'UNKNOWN', 'Nieznany', 'Nieznany', 'Nieznany', 0.0, 'unknown'
WHERE NOT EXISTS (SELECT 1 FROM aurora_jk.dw_dim_product WHERE product_sk = -1);

-- -----------------------------------------------------------------------------
-- dw_dim_order_status: jeden wiersz to jedna para status x kanal (junk dimension).
-- -----------------------------------------------------------------------------
-- Dwa niskokardynalne atrybuty zamowienia (status, kanal) nie zasluguja na dwa
-- osobne wymiary ani na dwie kolumny tekstowe w fakcie. Junk dimension sklada
-- je w jedna tabele.
--
-- CROSS JOIN dwoch DISTINCT-ow rozpina iloczyn kartezjanski: kazdy status z
-- kazdym kanalem, takze pary, ktorych dzis nie ma w danych. Wymiar opisuje
-- dziedzine (co w ogole moze sie zdarzyc), a nie aktualny stan sprzedazy.
-- INNER JOIN po wystepujacych parach dalby dziury, gdy jutro pojawi sie nowa
-- kombinacja.
--
-- Zrodlem jest Bronze, a nie Silver: Silver odfiltrowal cancelled, a wymiar ma
-- znac wszystkie statusy. is_revenue mowi, czy status liczy sie do przychodu.
CREATE TABLE IF NOT EXISTS aurora_jk.dw_dim_order_status
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/dw/dim_order_status/',
  is_external = false
) AS
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY s.status, c.channel) AS integer) AS order_status_sk,
  s.status,
  c.channel,
  s.status IN ('paid', 'shipped') AS is_revenue
FROM (SELECT DISTINCT status  FROM aurora_jk.bronze_orders) s
CROSS JOIN (SELECT DISTINCT channel FROM aurora_jk.bronze_orders) c;

-- =============================================================================
-- KONTROLA
-- =============================================================================
-- Oczekiwane (kolejnosc z ORDER BY wymiar):
--   dw_dim_customer 81 | dw_dim_date 366 | dw_dim_order_status 9 | dw_dim_product 26
-- Kazdy wymiar poza junk dimension ma o jeden wiersz wiecej niz zrodlo - to
-- wiersz -1. Junk dimension: 3 statusy x 3 kanaly = 9.
SELECT 'dw_dim_date' AS wymiar, COUNT(*) AS n FROM aurora_jk.dw_dim_date
UNION ALL SELECT 'dw_dim_customer',     COUNT(*) FROM aurora_jk.dw_dim_customer
UNION ALL SELECT 'dw_dim_product',      COUNT(*) FROM aurora_jk.dw_dim_product
UNION ALL SELECT 'dw_dim_order_status', COUNT(*) FROM aurora_jk.dw_dim_order_status
ORDER BY wymiar;

-- Junk dimension w calosci. Oczekiwane 9 wierszy, numeracja alfabetyczna:
--   1 cancelled/marketplace | 2 cancelled/mobile | 3 cancelled/web |
--   4 paid/marketplace | 5 paid/mobile | 6 paid/web |
--   7 shipped/marketplace | 8 shipped/mobile | 9 shipped/web
-- is_revenue = false tylko dla 1-3.
SELECT order_status_sk, status, channel, is_revenue
FROM aurora_jk.dw_dim_order_status
ORDER BY order_status_sk;

-- Przedzialy cenowe. Oczekiwane (ORDER BY price_band): budget 4 | mid 6 |
-- premium 15 | unknown 1.
SELECT price_band, COUNT(*) AS n
FROM aurora_jk.dw_dim_product
GROUP BY price_band
ORDER BY price_band;

-- Co Iceberg zapisal na S3: jeden snapshot 'append' z CTAS-a (80 wierszy)
-- i drugi 'append' z INSERT-a wiersza -1 (1 wiersz). Oczekiwane: 2 wiersze,
-- oba operation = append, added_records 80 i 1. Po drugim uruchomieniu pliku
-- nadal 2 - INSERT, ktory nic nie wstawil, nie tworzy snapshotu (sprawdzone).
SELECT committed_at, operation, summary['added-records'] AS added_records
FROM "aurora_jk"."dw_dim_customer$snapshots"
ORDER BY committed_at;
