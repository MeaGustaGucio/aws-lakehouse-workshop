-- =============================================================================
-- Dzien 1, S1 - kopia silver_orders jako tabela Apache Iceberg
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- S1. Tabela Iceberg
-- -----------------------------------------------------------------------------
-- Zwykla tabela zewnetrzna to katalog plikow - jedyna dostepna operacja to
-- "dopisz kolejny plik". Iceberg doklada nad plikami warstwe metadanych, ktora
-- pamieta, KTORE pliki tworza aktualny stan tabeli. Dzieki temu UPDATE, DELETE
-- i MERGE sprowadzaja sie do zapisania nowych plikow i przestawienia wskaznika,
-- a stare wskazniki zostaja - i to jest time travel.
--
-- Trzy roznice skladniowe wobec CTAS z L5:
--   * location zamiast external_location (tabela nie jest zewnetrzna)
--   * is_external = false - Athena zarzadza tymi plikami, wiec DROP TABLE JE
--     SKASUJE, odwrotnie niz w Bronze, gdzie DROP zostawia dane na S3
--   * partitioned_by nie dziala; Iceberg ma partitioning z transformacjami,
--     np. partitioning = ARRAY['month(order_date)']

CREATE TABLE IF NOT EXISTS aurora_jk.silver_orders_iceberg
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/silver/orders_iceberg/',
  is_external = false
) AS
SELECT * FROM aurora_jk.silver_orders;

-- Oczekiwane: 170 (tyle samo co silver_orders)
SELECT COUNT(*) AS n FROM aurora_jk.silver_orders_iceberg;

-- Oczekiwane po samym CTAS: jeden wiersz, operation = 'append'.
-- W dniu 3 dojdzie drugi, z operation = 'overwrite' po instrukcji MERGE.
SELECT committed_at, operation
FROM "aurora_jk"."silver_orders_iceberg$snapshots"
ORDER BY committed_at;

