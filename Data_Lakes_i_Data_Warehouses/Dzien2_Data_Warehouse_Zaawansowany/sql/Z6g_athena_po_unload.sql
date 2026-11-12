-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z6g - Athena czyta to, co Redshift wyeksportowal
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Do podmiany: aurora_jk / aurora-jk-lake.
-- Wymaga wykonanego UNLOAD z Z6_redshift.sql.
--
-- Zwykla tabela zewnetrzna nad Parquetem z UNLOAD-u. Typy wg tego, co zapisal
-- Redshift: integer -> int, varchar -> string, bigint (COUNT/SUM(int)) -> bigint,
-- double precision -> double. Parquet ma schemat w pliku, wiec niezgodnosc
-- typu wyszlaby przy pierwszym SELECT, nie przy CREATE.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.dw_export_daily_sales (
  order_date_sk int,
  category      string,
  channel       string,
  lines_n       bigint,
  qty           bigint,
  revenue_pln   double
)
STORED AS PARQUET
LOCATION 's3://aurora-jk-lake/export/daily_sales/';

-- Oczekiwane: 36 | 1427124.00 w obu wierszach (ORDER BY zrodlo):
-- athena_iceberg i redshift_unload - pelne kolo: S3 -> Athena -> Redshift -> S3
-- -> Athena, ta sama liczba na kazdym etapie.
SELECT 'athena_iceberg' AS zrodlo, COUNT(*) AS rows_n, ROUND(SUM(revenue_pln), 2) AS revenue_pln
FROM aurora_jk.dw_fact_daily_sales
UNION ALL
SELECT 'redshift_unload', COUNT(*), ROUND(SUM(revenue_pln), 2)
FROM aurora_jk.dw_export_daily_sales
ORDER BY zrodlo;
