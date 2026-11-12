-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z1 - wymiar daty jako pelny kalendarz Iceberg
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Warstwa Gold wersji zaawansowanej nazywa sie dw_* i lezy pod
-- s3://aurora-<inicjaly>-lake/dw/, zeby nie zderzac sie z gold_* z wersji
-- podstawowej.
--
-- Iceberg zamiast zwyklego CTAS do Parquetu, bo DROP TABLE kasuje takze pliki,
-- tabela przyjmuje INSERT / UPDATE / DELETE / MERGE, a kazda zmiana to snapshot.
-- Typ ladowania: pelne odswiezenie (full refresh / CTAS replace) — cala tabela od zera.
-- Trzy parametry CTAS, ktorych nie bylo w L5:
--   table_type  = 'ICEBERG'
--   location    = 's3://...'   (nie external_location)
--   is_external = false
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dw_dim_date: jeden wiersz to jeden dzien kalendarzowy 2026 (plus wiersz -1).
-- -----------------------------------------------------------------------------
-- W wersji podstawowej wymiar daty powstaje z dat wystepujacych w sprzedazy
-- (2 wiersze) i w dniu 3 trzeba go doklejac. Tu generujemy 365 dni z gory.
-- UNNEST(sequence(...)) rozpina kalendarz: sequence() buduje tablice timestampow
-- od 1 stycznia do 31 grudnia, UNNEST robi z niej wiersze. Bez tego nie ma
-- wymiaru daty niezaleznego od faktu i 20 stycznia znowu "znika" z raportu.
--
-- date_sk w konwencji RRRRMMDD (20260115) - czytelny przy debugowaniu i stabilny
-- miedzy przeladowaniami. day_of_week(): 1 = poniedzialek ... 7 = niedziela.
-- week() to numer tygodnia ISO. date_format() oczekuje timestampa, stad CAST.
--
-- PULAPKA (sprawdzona w Athenie v3): sequence(DATE, DATE, INTERVAL '1' DAY) zwraca
-- tablice TIMESTAMPOW, nie dat. Bez CAST(d AS date) kolumna date_actual
-- powstaje jako timestamp, a zlaczenie z order_date (typ date) idzie przez
-- niejawne rzutowanie i wyniki wyswietlaja sie jako 2026-01-15 00:00:00.000000.
CREATE TABLE IF NOT EXISTS aurora_jk.dw_dim_date
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/dw/dim_date/',
  is_external = false
) AS
SELECT
  CAST(year(d) * 10000 + month(d) * 100 + day(d) AS integer) AS date_sk,
  CAST(d AS date)                                AS date_actual,
  CAST(year(d) AS integer)                       AS year,
  CAST(quarter(d) AS integer)                    AS quarter,
  CAST(month(d) AS integer)                      AS month,
  date_format(d, '%M')                           AS month_name,
  CAST(day(d) AS integer)                        AS day,
  CAST(day_of_week(d) AS integer)                AS day_of_week,
  date_format(d, '%W')                           AS day_name,
  CAST(week(d) AS integer)                       AS iso_week,
  day_of_week(d) IN (6, 7)                       AS is_weekend
FROM UNNEST(sequence(DATE '2026-01-01', DATE '2026-12-31', INTERVAL '1' DAY)) AS t(d);

-- Wiersz nieznany. Fakt nigdy nie traci wiersza przez brak daty: zamowienie bez
-- wysylki dostaje ship_date_sk = -1 zamiast wypadac na INNER JOIN. Wzorzec
-- Kimballa: kazdy wymiar ma wiersz "unknown", a fakt zawsze ma do czego sie
-- zlaczyc. WHERE NOT EXISTS czyni INSERT idempotentnym.
INSERT INTO aurora_jk.dw_dim_date
SELECT -1, DATE '1900-01-01', 1900, 1, 1, 'Nieznany', 1, 1, 'Nieznany', 1, false
WHERE NOT EXISTS (SELECT 1 FROM aurora_jk.dw_dim_date WHERE date_sk = -1);

-- Oczekiwane: dni = 366 (365 + wiersz -1), weekendy = 104, pierwszy = -1,
-- ostatni = 20261231.
SELECT
  COUNT(*)                                          AS dni,
  SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END)       AS weekendy,
  MIN(date_sk)                                      AS pierwszy,
  MAX(date_sk)                                      AS ostatni
FROM aurora_jk.dw_dim_date;

-- -----------------------------------------------------------------------------
