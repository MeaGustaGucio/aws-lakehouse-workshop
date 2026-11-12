-- =============================================================================
-- Dzien 1, D4 - JSON jako jedna kolumna raw
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
-- NIC Z TEGO PLIKU NIE JEST POTRZEBNE W DNIU 2 ANI 3.
-- Kazdy nowy obiekt ma w nazwie _extra; pliki CTAS laduja pod extra/.
-- =============================================================================

-- D4. Ten sam JSON trzeci raz - jedna kolumna raw i funkcje JSON
-- -----------------------------------------------------------------------------
-- Zadnego SerDe do JSON: kazda linia pliku to jeden wiersz w kolumnie raw.
-- Separator \001 nie wystepuje w plikach, wiec linia wchodzi w calosci.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_events_raw_extra (
  raw string
)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/events/';

MSCK REPAIR TABLE aurora_jk.bronze_events_raw_extra;

-- Trzy surowe linie, dokladnie tak jak leza w pliku.
SELECT raw FROM aurora_jk.bronze_events_raw_extra LIMIT 3;

-- Oczekiwane: 935 - tyle samo co bronze_events.
SELECT COUNT(*) AS n FROM aurora_jk.bronze_events_raw_extra;

-- Odkrywanie schematu: jakie klucze sa w plikach i ile razy.
-- json_parse zamienia tekst na typ JSON, CAST na MAP daje slownik klucz -> wartosc,
-- map_keys wyciaga same klucze, UNNEST rozwija tablice kluczy na wiersze.
-- Oczekiwane: 6 kluczy, kazdy 935 razy:
--   customer_id | event_id | event_ts | event_type | page | product_id
SELECT k AS klucz, COUNT(*) AS n
FROM aurora_jk.bronze_events_raw_extra
CROSS JOIN UNNEST(map_keys(CAST(json_parse(raw) AS MAP(varchar, json)))) AS t(k)
GROUP BY k
ORDER BY k;

-- json_extract_scalar wyciaga jedna wartosc sciezka JSONPath - zawsze jako varchar.
-- Oczekiwane (event_type | n | n_with_product):
--   add_to_cart | 184 | 184
--   page_view   | 421 | 421
--   purchase    | 171 | 171
--   search      | 159 |   0
-- Czwarty typ zdarzenia, search, nie pojawil sie ani w L3, ani w S2 - i ma
-- product_id = null w kazdym wierszu. Klucz jest, wartosci nie ma.
SELECT
  json_extract_scalar(raw, '$.event_type') AS event_type,
  COUNT(*) AS n,
  COUNT(json_extract_scalar(raw, '$.product_id')) AS n_with_product
FROM aurora_jk.bronze_events_raw_extra
GROUP BY 1
ORDER BY 1;

-- Dostep "przez kropke": CAST na ROW nadaje tekstowi strukture z nazwanymi polami
-- i typami. Pola, ktorych nie ma w ROW, sa pomijane; null zostaje null-em.
-- Oczekiwane (pierwsze trzy po event_id):
--   B000801 | page_view | 117
--   B000802 | page_view | 124
--   B000803 | search    | (null)
SELECT
  e.event_id,
  e.event_type,
  e.product_id
FROM (
  SELECT CAST(json_parse(raw) AS ROW(event_id varchar, event_type varchar, product_id integer)) AS e
  FROM aurora_jk.bronze_events_raw_extra
)
ORDER BY e.event_id
LIMIT 3;

-- Kontrola: te same liczby z tabeli czytanej przez JsonSerDe.
-- Oczekiwane: add_to_cart 184 | page_view 421 | purchase 171 | search 159
SELECT event_type, COUNT(*) AS n
FROM aurora_jk.bronze_events
GROUP BY event_type
ORDER BY event_type;

-- -----------------------------------------------------------------------------
