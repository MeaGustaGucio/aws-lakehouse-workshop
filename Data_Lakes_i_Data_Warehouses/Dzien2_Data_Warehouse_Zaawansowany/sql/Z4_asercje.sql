-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z4 - asercje jakosci danych jako jedno zapytanie
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
-- Do podmiany: aurora_jk -> aurora_<inicjaly>.
-- Wymaga faktow z sql/Z3_fakty.sql.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Asercje jakosci danych jako SQL
-- -----------------------------------------------------------------------------
-- Jedno zapytanie, jedna tabela wynikowa: nazwa testu, wartosc oczekiwana,
-- wartosc zmierzona, status. To ten sam pomysl, ktory dbt nazywa "tests",
-- a Great Expectations "expectations" - tu bez zadnego narzedzia. Wynik inny
-- niz OK we wszystkich wierszach oznacza, ze Gold nie nadaje sie do publikacji.
--
-- Oczekiwane: 8 wierszy, w kazdym status = OK (ORDER BY test).
WITH checks AS (
  SELECT 'A1 fact rows = silver rows' AS test,
         (SELECT COUNT(*) FROM aurora_jk.silver_order_items)   AS expected,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item)   AS actual
  UNION ALL
  SELECT 'A2 fact revenue = silver revenue',
         (SELECT CAST(ROUND(SUM(net_amount_pln), 2) AS bigint) FROM aurora_jk.silver_order_items),
         (SELECT CAST(ROUND(SUM(net_amount_pln), 2) AS bigint) FROM aurora_jk.dw_fact_order_item)
  UNION ALL
  SELECT 'A3 no unknown customer in fact', 0,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item WHERE customer_sk = -1)
  UNION ALL
  SELECT 'A4 no orphan product_sk', 0,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item f
          LEFT JOIN aurora_jk.dw_dim_product p ON f.product_sk = p.product_sk
          WHERE p.product_sk IS NULL)
  UNION ALL
  SELECT 'A5 grain unique (order_id, product_sk)', 0,
         (SELECT COUNT(*) FROM (
            SELECT order_id, product_sk FROM aurora_jk.dw_fact_order_item
            GROUP BY order_id, product_sk HAVING COUNT(*) > 1))
  UNION ALL
  SELECT 'A6 no negative amounts', 0,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item WHERE net_amount_pln < 0)
  UNION ALL
  SELECT 'A7 aggregate revenue = fact revenue',
         (SELECT CAST(ROUND(SUM(net_amount_pln), 2) AS bigint) FROM aurora_jk.dw_fact_order_item),
         (SELECT CAST(ROUND(SUM(revenue_pln), 2) AS bigint)    FROM aurora_jk.dw_fact_daily_sales)
  UNION ALL
  SELECT 'A8 fulfillment orders = silver orders',
         (SELECT COUNT(*) FROM aurora_jk.silver_orders),
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_fulfillment)
)
SELECT test, expected, actual,
       CASE WHEN expected = actual THEN 'OK' ELSE 'FAIL' END AS status
FROM checks
ORDER BY test;

-- Rzut oka na S3 przez metadane Icebergu: ile plikow danych ma fakt i w ilu
-- partycjach. Oczekiwane: 2 wiersze (partycje 2026-01-15 i 2026-01-16), po
-- jednym pliku kazda - CTAS zapisal jeden plik na partycje.
SELECT partition, record_count, file_count
FROM "aurora_jk"."dw_fact_order_item$partitions"
ORDER BY partition;
