-- =============================================================================
-- Dzien 1, D2 - partition projection - partycje bez katalogu
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
-- NIC Z TEGO PLIKU NIE JEST POTRZEBNE W DNIU 2 ANI 3.
-- Kazdy nowy obiekt ma w nazwie _extra; pliki CTAS laduja pod extra/.
-- =============================================================================

-- D2. Partition projection - partycje wyliczane z reguly, bez katalogu
-- -----------------------------------------------------------------------------
-- Ten sam DDL co bronze_orders (ten sam LOCATION) plus wlasciwosci projection.*
-- Athena wylicza wszystkie daty z zakresu, buduje z nich sciezki
-- s3://.../bronze/orders/dt=<data>/ i czyta te, ktore istnieja. Zadne
-- MSCK REPAIR ani ADD PARTITION nie sa potrzebne - ani dzis, ani po nowym dniu.
--
-- Uklad katalogow dt=<wartosc> jest domyslny, wiec storage.location.template
-- nie jest tu potrzebny. Przydaje sie, gdy katalogi nazywaja sie inaczej,
-- np. .../2026/01/15/ - wtedy: 's3://aurora-jk-lake/bronze/orders/${dt}/'
-- z formatem yyyy/MM/dd.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_orders_extra (
  order_id string,
  customer_id string,
  order_ts string,
  status string,
  channel string,
  delivery_city string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/orders/'
TBLPROPERTIES (
  'skip.header.line.count'      = '1',
  'projection.enabled'          = 'true',
  'projection.dt.type'          = 'date',
  'projection.dt.range'         = '2026-01-01,NOW',
  'projection.dt.format'        = 'yyyy-MM-dd',
  'projection.dt.interval'      = '1',
  'projection.dt.interval.unit' = 'DAYS'
);

-- Oczekiwane, bez MSCK: 2026-01-15 -> 90 | 2026-01-16 -> 110.
-- Data scanned = 11 331 B (oba pliki CSV w calosci).
-- Bez filtra po dt Athena sprawdza KAZDY dzien z zakresu 2026-01-01..dzis -
-- na dwoch plikach niezauwazalne, przy tysiacach dni to koszt czasu, nie bajtow.
SELECT dt, COUNT(*) AS n
FROM aurora_jk.bronze_orders_extra
GROUP BY dt
ORDER BY dt;

-- Oczekiwane: 110. Data scanned = 6 232 B - tylko plik z 16.01.
SELECT COUNT(*) AS n FROM aurora_jk.bronze_orders_extra WHERE dt = '2026-01-16';

-- Oczekiwane: 0, bez bledu, Data scanned = 0 B. Dzien jest w zakresie reguly,
-- ale katalogu na S3 nie ma - Athena po prostu nie znajduje plikow.
SELECT COUNT(*) AS n FROM aurora_jk.bronze_orders_extra WHERE dt = '2026-02-01';

-- Oczekiwane: pusty wynik. Partycje nie istnieja w katalogu - sa wyliczane
-- w chwili zapytania. To odpowiedz na pytanie "gdzie one sa".
SHOW PARTITIONS aurora_jk.bronze_orders_extra;

-- -----------------------------------------------------------------------------
