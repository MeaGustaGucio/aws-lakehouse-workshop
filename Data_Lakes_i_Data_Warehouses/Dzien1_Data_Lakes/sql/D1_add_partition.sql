-- =============================================================================
-- Dzien 1, D1 - ALTER TABLE ADD PARTITION kontra MSCK REPAIR
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
-- NIC Z TEGO PLIKU NIE JEST POTRZEBNE W DNIU 2 ANI 3.
-- Kazdy nowy obiekt ma w nazwie _extra; pliki CTAS laduja pod extra/.
-- =============================================================================

-- D1. ALTER TABLE ADD PARTITION kontra MSCK REPAIR
-- -----------------------------------------------------------------------------
-- Druga tabela na TYCH SAMYCH plikach co bronze_events. Katalog Glue trzyma
-- dla kazdej tabeli osobna liste partycji, wiec nowa tabela startuje z pusta.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_events_extra (
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

-- Oczekiwane: 0 - pliki sa, partycji w katalogu nie ma.
SELECT COUNT(*) AS n FROM aurora_jk.bronze_events_extra;

-- Oczekiwane: pusty wynik.
SHOW PARTITIONS aurora_jk.bronze_events_extra;

-- Rejestracja JEDNEJ partycji. Athena nie listuje prefiksu - zapisuje w katalogu
-- to, co podano, i tyle. Tak dopisuje nowy dzien pipeline, ktory wlasnie go zapisal.
ALTER TABLE aurora_jk.bronze_events_extra
ADD IF NOT EXISTS PARTITION (dt = '2026-01-15')
LOCATION 's3://aurora-jk-lake/bronze/events/dt=2026-01-15/';

-- Oczekiwane: 434 (tylko 15.01). Data scanned = 64 616 B, czyli dokladnie
-- rozmiar pliku dt=2026-01-15/events.json - JSON czyta sie caly.
SELECT COUNT(*) AS n FROM aurora_jk.bronze_events_extra;

-- Oczekiwane: jeden wiersz, dt=2026-01-15
SHOW PARTITIONS aurora_jk.bronze_events_extra;

-- MSCK porownuje prefiks na S3 z katalogiem i dopisuje brakujace partycje.
-- Oczekiwany wynik (dwie linie):
--   Partitions not in metastore:    bronze_events_extra:dt=2026-01-16
--   Repair: Added partition to metastore bronze_events_extra:dt=2026-01-16
MSCK REPAIR TABLE aurora_jk.bronze_events_extra;

-- Oczekiwane: 2026-01-15 -> 434 | 2026-01-16 -> 501. Data scanned = 139 161 B
-- (64 616 + 74 545 - oba pliki).
SELECT dt, COUNT(*) AS n
FROM aurora_jk.bronze_events_extra
GROUP BY dt
ORDER BY dt;

-- DROP PARTITION kasuje WPIS w katalogu. Plik na S3 zostaje.
ALTER TABLE aurora_jk.bronze_events_extra DROP IF EXISTS PARTITION (dt = '2026-01-15');

-- Oczekiwane: 501 - tabela _extra widzi juz tylko 16.01 ...
SELECT COUNT(*) AS n FROM aurora_jk.bronze_events_extra;

-- ... a oryginalna bronze_events nadal 935. Te same pliki, dwa rozne katalogi.
SELECT COUNT(*) AS n FROM aurora_jk.bronze_events;

-- Przywrocenie brakujacej partycji, zeby tabela _extra byla znow kompletna.
-- Oczekiwane: "Repair: Added partition to metastore bronze_events_extra:dt=2026-01-15"
MSCK REPAIR TABLE aurora_jk.bronze_events_extra;

-- -----------------------------------------------------------------------------
