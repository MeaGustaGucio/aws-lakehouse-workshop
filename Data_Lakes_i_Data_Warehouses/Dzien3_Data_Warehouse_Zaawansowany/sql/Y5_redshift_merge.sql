-- =============================================================================
-- Dzien 3 (wersja zaawansowana), Y5 - Redshift: MERGE do tabeli lokalnej, Spectrum
-- nad Icebergiem po zmianach, odswiezenie MV, zuzycie RPU
-- =============================================================================
-- SILNIK: AMAZON REDSHIFT SERVERLESS, Query Editor v2 (Federated user, baza dev).
-- Do podmiany: dw_jk -> dw_<inicjaly>, aurora_jk -> aurora_<inicjaly>.
-- Wymaga schematu dw_<inicjaly> z dnia 2 (Z6_redshift.sql) oraz tabel
-- Iceberg po Y1-Y3 (89 wersji klientow, 449 wierszy faktu).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Y5a. Spectrum widzi zmiany z Atheny natychmiast
-- -----------------------------------------------------------------------------
-- Tabela lokalna to kopia z soboty; Iceberg w Glue to stan po dzisiejszych
-- MERGE-ach. Oczekiwane (ORDER BY zrodlo):
--   iceberg_dim_customer 89 | iceberg_fact 449 | local_dim_customer 81 | local_fact 409
SELECT 'iceberg_dim_customer' AS zrodlo, COUNT(*) AS n FROM "awsdatacatalog"."aurora_jk"."dw_dim_customer"
UNION ALL SELECT 'iceberg_fact',         COUNT(*) FROM "awsdatacatalog"."aurora_jk"."dw_fact_order_item"
UNION ALL SELECT 'local_dim_customer',   COUNT(*) FROM dw_jk.dim_customer
UNION ALL SELECT 'local_fact',           COUNT(*) FROM dw_jk.fact_order_item
ORDER BY zrodlo;

-- -----------------------------------------------------------------------------
-- Y5b. MERGE w Redshifcie - wymiar SCD2
-- -----------------------------------------------------------------------------
-- Skladnia jak w Athenie, z dwiema roznicami (sprawdzone w Redshift Serverless):
--   * Redshift wymaga OBU galezi: WHEN MATCHED i WHEN NOT MATCHED,
--   * w WHEN MATCHED nie ma dodatkowego warunku AND (...) - filtr trzeba
--     przeniesc do zrodla albo zaakceptowac UPDATE tymi samymi wartosciami.
-- Kluczem jest customer_sk (unikalny per wersja): stare wersje dostaja
-- UPDATE valid_to / is_current, nowe (sk 81-88) - INSERT. Drugie uruchomienie:
-- wszystko trafia w MATCHED i przepisuje te same wartosci - 89 bez zmian.
MERGE INTO dw_jk.dim_customer
USING "awsdatacatalog"."aurora_jk"."dw_dim_customer" s
  ON dw_jk.dim_customer.customer_sk = s.customer_sk
WHEN MATCHED THEN UPDATE SET
  city        = s.city,
  voivodeship = s.voivodeship,
  segment     = s.segment,
  valid_to    = s.valid_to,
  is_current  = s.is_current
WHEN NOT MATCHED THEN INSERT
  (customer_sk, customer_id, first_name, last_name, city, voivodeship, segment,
   signup_date, signup_month, valid_from, valid_to, is_current)
  VALUES (s.customer_sk, s.customer_id, s.first_name, s.last_name, s.city,
          s.voivodeship, s.segment, s.signup_date, s.signup_month, s.valid_from,
          s.valid_to, s.is_current);

-- Oczekiwane: 89 | biezace 81 | historyczne 8 - jak w Athenie.
SELECT COUNT(*) AS wiersze,
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END)     AS biezace,
       SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) AS historyczne
FROM dw_jk.dim_customer;

-- -----------------------------------------------------------------------------
-- Y5c. MERGE faktu - przyrost 20 stycznia do tabeli lokalnej
-- -----------------------------------------------------------------------------
-- Zrodlem jest Iceberg w Glue - dokladnie te same wiersze, ktore Athena
-- wstawila w Y2. Klucz zlozony w ON, tak samo jak w Athenie.
-- Kolumna promo_code (dodana w Y4) nie jest tu potrzebna: INSERT wypisuje
-- kolumny jawnie, wiec nowa kolumna po stronie Icebergu niczego nie psuje.
MERGE INTO dw_jk.fact_order_item
USING (SELECT * FROM "awsdatacatalog"."aurora_jk"."dw_fact_order_item"
       WHERE order_date > DATE '2026-01-16') s
  ON dw_jk.fact_order_item.order_id   = s.order_id
 AND dw_jk.fact_order_item.product_sk = s.product_sk
WHEN MATCHED THEN UPDATE SET
  qty            = s.qty,
  unit_price_pln = s.unit_price_pln,
  discount_pln   = s.discount_pln,
  net_amount_pln = s.net_amount_pln
WHEN NOT MATCHED THEN INSERT
  (order_id, order_date, order_date_sk, customer_sk, product_sk, order_status_sk,
   qty, unit_price_pln, discount_pln, net_amount_pln, load_ts)
  VALUES (s.order_id, s.order_date, s.order_date_sk, s.customer_sk, s.product_sk,
          s.order_status_sk, s.qty, s.unit_price_pln, s.discount_pln,
          s.net_amount_pln, s.load_ts);

-- Oczekiwane (ORDER BY order_date): 2026-01-15 186 | 2026-01-16 223 |
-- 2026-01-20 40 101546.00. Razem 449 - po drugim uruchomieniu tez.
SELECT order_date, COUNT(*) AS rows_n, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM dw_jk.fact_order_item
GROUP BY order_date
ORDER BY order_date;

-- -----------------------------------------------------------------------------
-- Y5d. Materialized view - nieaktualny, dopoki go nie odswiezymy
-- -----------------------------------------------------------------------------
-- Oczekiwane PRZED: 36 | 1427124.00 (stan z soboty).
SELECT COUNT(*) AS rows_n, ROUND(SUM(revenue_pln), 2) AS revenue_pln FROM dw_jk.mv_daily_sales;

REFRESH MATERIALIZED VIEW dw_jk.mv_daily_sales;

-- Oczekiwane PO: 52 | 1528670.00 (36 + 16 kombinacji kategoria x kanal
-- z 20 stycznia; 1427124 + 101546). Drugie uruchomienie: REFRESH nic nie
-- zmienia, 52 | 1528670.00.
SELECT COUNT(*) AS rows_n, ROUND(SUM(revenue_pln), 2) AS revenue_pln FROM dw_jk.mv_daily_sales;

-- Rekoncyliacja trzech silnikow po wszystkich zmianach. Oczekiwane (ORDER BY
-- zrodlo): athena_iceberg 449 1528670 | redshift_local 449 1528670 |
-- redshift_mv 52 1528670 - trzy rozne miejsca, jedna liczba.
SELECT 'athena_iceberg' AS zrodlo, COUNT(*) AS rows_n, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM "awsdatacatalog"."aurora_jk"."dw_fact_order_item"
UNION ALL SELECT 'redshift_local', COUNT(*), ROUND(SUM(net_amount_pln), 2) FROM dw_jk.fact_order_item
UNION ALL SELECT 'redshift_mv',    COUNT(*), ROUND(SUM(revenue_pln), 2)    FROM dw_jk.mv_daily_sales
ORDER BY zrodlo;

-- -----------------------------------------------------------------------------
-- Y5e. Ile to kosztowalo - RPU
-- -----------------------------------------------------------------------------
-- SYS_SERVERLESS_USAGE: zuzycie w odcinkach po 1 minucie. charged_seconds to
-- sekundy x RPU, za ktore placimy (minimum 60 s od pierwszego zapytania).
-- Oczekiwane: kilka wierszy z dzisiejszej sesji. compute_capacity to RPU,
-- z ktorymi workgroup pracowal: przy base capacity 8 - 8; na workgroupie
-- z wlaczonym "price-performance target" (AI-driven scaling, bez base
-- capacity) potrafi pokazac 128 i charged_seconds 7680 = 128 RPU x 60 s
-- za KAZDA minute z zapytaniem. Ta jedna kolumna to caly rachunek za dzien.
SELECT start_time, end_time, compute_capacity, compute_seconds, charged_seconds
FROM SYS_SERVERLESS_USAGE
WHERE start_time >= DATEADD(hour, -3, GETDATE())
ORDER BY start_time DESC
LIMIT 10;
