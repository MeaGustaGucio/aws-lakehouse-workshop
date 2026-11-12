-- =============================================================================
-- Dzien 1, D5 - pseudokolumna "$path" i bucketing
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
-- NIC Z TEGO PLIKU NIE JEST POTRZEBNE W DNIU 2 ANI 3.
-- Kazdy nowy obiekt ma w nazwie _extra; pliki CTAS laduja pod extra/.
-- =============================================================================

-- D5. Pliki pod tabela - pseudokolumna "$path" i bucketing
-- -----------------------------------------------------------------------------
-- "$path" to pelna sciezka pliku, z ktorego pochodzi wiersz. Cudzyslow jest
-- konieczny, bo $ nie jest dozwolony w zwyklej nazwie kolumny.
-- Oczekiwane: 2 pliki, jeden na partycje: dt=2026-01-15 -> 186 | dt=2026-01-16 -> 223.
-- Data scanned = 0 B - odpowiedz jest w metadanych, Athena nie otwiera plikow.
SELECT "$path" AS plik, COUNT(*) AS n
FROM aurora_jk.silver_order_items
GROUP BY "$path"
ORDER BY "$path";

-- Ta sama tabela z bucketingiem: w kazdej partycji wiersze rozrzucone po 8 plikach
-- wedlug hasza customer_id. Ten sam klient trafia zawsze do tego samego pliku.
CREATE TABLE IF NOT EXISTS aurora_jk.silver_order_items_bucketed_extra
WITH (
  format              = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by      = ARRAY['dt'],
  bucketed_by         = ARRAY['customer_id'],
  bucket_count        = 8,
  external_location   = 's3://aurora-jk-lake/extra/silver_order_items_bucketed/'
) AS
SELECT * FROM aurora_jk.silver_order_items;

-- Oczekiwane: 16 plikow (8 na partycje), od 5 do 38 wierszy w pliku.
SELECT "$path" AS plik, COUNT(*) AS n
FROM aurora_jk.silver_order_items_bucketed_extra
GROUP BY "$path"
ORDER BY "$path";

-- Pytanie punktowe o jednego klienta. Oczekiwane: 7 pozycji, 29266.00.
-- Data scanned: 3 041 B na silver_order_items ...
SELECT COUNT(*) AS items, ROUND(SUM(net_amount_pln), 2) AS net_pln
FROM aurora_jk.silver_order_items
WHERE customer_id = 'C0001';

-- ... i 690 B na wersji z bucketingiem (4x mniej): Athena otwiera w kazdej
-- partycji tylko ten jeden plik, w ktorym moze byc C0001 (bucket pruning).
SELECT COUNT(*) AS items, ROUND(SUM(net_amount_pln), 2) AS net_pln
FROM aurora_jk.silver_order_items_bucketed_extra
WHERE customer_id = 'C0001';

-- Pelna suma. Oczekiwane 1427124.00 w obu.
-- Data scanned: 1 717 B na 2 plikach ...
SELECT ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items;

-- ... i 2 614 B na 16 plikach. Kazdy plik Parquet ma wlasna stopke z metadanymi;
-- przy 16 malych plikach stopki waza wiecej niz dane. To jest problem
-- "many small files" w miniaturze - bucketing (i za drobne partycje) oplaca sie
-- tylko wtedy, gdy zapytania faktycznie filtruja po tej kolumnie.
SELECT ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items_bucketed_extra;

-- =============================================================================

-- SPRZATANIE DODATKOW
-- =============================================================================
-- DROP TABLE na tabelach EXTERNAL kasuje tylko wpis w katalogu - pliki
-- bronze/events/ i bronze/orders/ zostaja, bo naleza do tabel z L3.
-- Pliki utworzone przez CTAS (prefiks extra/) trzeba usunac osobno w konsoli S3:
-- bucket -> zaznaczenie katalogu extra/ -> Delete.
-- Sprzatanie z dnia 3 (DROP DATABASE ... CASCADE + kasowanie bucketu) i tak
-- obejmie wszystko, wiec ten krok mozna zostawic na koniec kursu.
DROP TABLE IF EXISTS aurora_jk.bronze_events_extra;
DROP TABLE IF EXISTS aurora_jk.bronze_orders_extra;
DROP VIEW  IF EXISTS aurora_jk.v_revenue_by_day_extra;
DROP TABLE IF EXISTS aurora_jk.silver_revenue_by_day_extra;
DROP TABLE IF EXISTS aurora_jk.bronze_events_raw_extra;
DROP TABLE IF EXISTS aurora_jk.silver_order_items_bucketed_extra;
