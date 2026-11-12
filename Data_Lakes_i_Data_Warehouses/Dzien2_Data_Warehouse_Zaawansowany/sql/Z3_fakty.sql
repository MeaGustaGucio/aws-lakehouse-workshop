-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z3 - trzy fakty Iceberg z jednego Silvera
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Wymaga wymiarow z sql/Z1_wymiar_daty.sql i sql/Z2_wymiary.sql.
-- Wymaga pliku bronze/shipments/shipments.csv wgranego do wlasnego bucketu (Z0).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze_shipments: jeden wiersz to jedna wysylka (66 wierszy z pliku Z0).
-- -----------------------------------------------------------------------------
-- Plik: dane/bronze/shipments/shipments.csv -> s3://aurora-<inicjaly>-lake/bronze/shipments/
-- Ta sama procedura co w dniu 1: CSV przez OpenCSVSerde, wszystko jako string.
-- delivered_ts bywa puste (paczka w drodze) - OpenCSVSerde zwraca wtedy pusty
-- string, nie NULL. NULLIF w Gold zamienia go na NULL.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_shipments (
  shipment_id  string,
  order_id     string,
  carrier      string,
  shipped_ts   string,
  delivered_ts string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/shipments/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Oczekiwane: wysylki = 66, dostarczone = 45, w_drodze = 21.
SELECT
  COUNT(*)                                                    AS wysylki,
  SUM(CASE WHEN delivered_ts <> '' THEN 1 ELSE 0 END)         AS dostarczone,
  SUM(CASE WHEN delivered_ts =  '' THEN 1 ELSE 0 END)         AS w_drodze
FROM aurora_jk.bronze_shipments;

-- -----------------------------------------------------------------------------
-- Z3b. dw_fact_order_item - fakt transakcyjny (grain: pozycja zamowienia)
-- -----------------------------------------------------------------------------
-- Co jest inaczej niz w gold_fact_order_item z wersji podstawowej:
--   * order_id zostaje w fakcie jako degenerate dimension - identyfikator
--     zamowienia bez wlasnej tabeli wymiaru (nie ma czego o nim trzymac),
--   * status i kanal wchodza jednym kluczem order_status_sk (junk dimension),
--   * LEFT JOIN + COALESCE(..., -1): brak w wymiarze NIE usuwa wiersza faktu,
--     tylko wskazuje wiersz nieznany. Rekoncyliacja z Silver ma wtedy szanse
--     zawsze sie zgadzac, a problem widac w raporcie zamiast znikac z niego,
--   * order_date (typ date) zostaje w fakcie OBOK order_date_sk: Iceberg
--     partycjonuje po nim w sposob ukryty - partitioning = ARRAY['day(order_date)'].
--     Filtr WHERE order_date = DATE '2026-01-15' tnie partycje bez zadnej
--     kolumny dt i bez wymogu "kolumna partycjonujaca ostatnia w SELECT",
--   * load_ts to kolumna audytowa - jutro pokaze, ktore wiersze weszly
--     ladowaniem przyrostowym.
--
-- current_timestamp w Athenie ma strefe czasowa; Iceberg w Athenie przyjmuje
-- timestamp bez strefy, stad CAST(... AS timestamp(6)).
CREATE TABLE IF NOT EXISTS aurora_jk.dw_fact_order_item
WITH (
  table_type   = 'ICEBERG',
  format       = 'PARQUET',
  location     = 's3://aurora-jk-lake/dw/fact_order_item/',
  is_external  = false,
  partitioning = ARRAY['day(order_date)']
) AS
SELECT
  i.order_id,
  i.order_date,
  COALESCE(d.date_sk, -1)           AS order_date_sk,
  COALESCE(c.customer_sk, -1)       AS customer_sk,
  COALESCE(p.product_sk, -1)        AS product_sk,
  COALESCE(st.order_status_sk, -1)  AS order_status_sk,
  i.qty,
  i.unit_price_pln,
  i.discount_pln,
  i.net_amount_pln,
  CAST(current_timestamp AS timestamp(6)) AS load_ts
FROM aurora_jk.silver_order_items i
JOIN aurora_jk.silver_orders o
  ON i.order_id = o.order_id
LEFT JOIN aurora_jk.dw_dim_date d
  ON i.order_date = d.date_actual
LEFT JOIN aurora_jk.dw_dim_customer c
  ON i.customer_id = c.customer_id
 AND i.order_date BETWEEN c.valid_from AND c.valid_to
LEFT JOIN aurora_jk.dw_dim_product p
  ON i.product_id = p.product_id
LEFT JOIN aurora_jk.dw_dim_order_status st
  ON o.status = st.status AND o.channel = st.channel;

-- Oczekiwane: 409 wierszy, 1427124.00, nieznane_* = 0 wszedzie.
SELECT
  COUNT(*)                                                    AS rows_n,
  ROUND(SUM(net_amount_pln), 2)                               AS revenue_pln,
  SUM(CASE WHEN customer_sk     = -1 THEN 1 ELSE 0 END)       AS nieznany_klient,
  SUM(CASE WHEN product_sk      = -1 THEN 1 ELSE 0 END)       AS nieznany_produkt,
  SUM(CASE WHEN order_status_sk = -1 THEN 1 ELSE 0 END)       AS nieznany_status
FROM aurora_jk.dw_fact_order_item;

-- -----------------------------------------------------------------------------
-- Z3c. dw_fact_order_fulfillment - accumulating snapshot (grain: zamowienie)
-- -----------------------------------------------------------------------------
-- Jeden wiersz na zamowienie z trzema datami: zlozenia, wysylki, dostawy.
-- Wszystkie trzy wskazuja na ten sam wymiar dw_dim_date. To jest wymiar daty
-- uzywany w kilku rolach (role-playing dimension): jedna tabela, trzy aliasy
-- w zlaczeniu (od, sd, dd).
-- Zamowienie bez wysylki ma ship_date_sk = -1, a nie NULL: dzieki temu
-- INNER JOIN do wymiaru daty w raporcie nadal zwraca ten wiersz.
--
-- Miary: order_amount_pln i items_n (zsumowane z pozycji), ship_lag_days
-- i delivery_lag_days (NULL, dopoki etap nie nastapil). Fakt tego typu jest
-- z definicji AKTUALIZOWANY - jutro MERGE dopisze dostawy, ktore dzis sa w drodze.
--
-- date_diff('day', ...) zwraca bigint - CAST na integer, zeby typ byl staly.
CREATE TABLE IF NOT EXISTS aurora_jk.dw_fact_order_fulfillment
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/dw/fact_order_fulfillment/',
  is_external = false
) AS
WITH order_totals AS (
  SELECT order_id, SUM(net_amount_pln) AS order_amount_pln, COUNT(*) AS items_n
  FROM aurora_jk.silver_order_items
  GROUP BY order_id
),
ship AS (
  SELECT
    order_id,
    carrier,
    CAST(shipped_ts AS timestamp)                AS shipped_ts,
    CAST(NULLIF(delivered_ts, '') AS timestamp)  AS delivered_ts
  FROM aurora_jk.bronze_shipments
)
SELECT
  o.order_id,
  COALESCE(c.customer_sk, -1)                     AS customer_sk,
  COALESCE(st.order_status_sk, -1)                AS order_status_sk,
  COALESCE(od.date_sk, -1)                        AS order_date_sk,
  COALESCE(sd.date_sk, -1)                        AS ship_date_sk,
  COALESCE(dd.date_sk, -1)                        AS delivery_date_sk,
  COALESCE(s.carrier, 'n/a')                      AS carrier,
  t.order_amount_pln,
  CAST(t.items_n AS integer)                      AS items_n,
  CAST(date_diff('day', o.order_ts, s.shipped_ts)   AS integer) AS ship_lag_days,
  CAST(date_diff('day', o.order_ts, s.delivered_ts) AS integer) AS delivery_lag_days
FROM aurora_jk.silver_orders o
JOIN order_totals t
  ON o.order_id = t.order_id
LEFT JOIN ship s
  ON o.order_id = s.order_id
LEFT JOIN aurora_jk.dw_dim_customer c
  ON o.customer_id = c.customer_id
 AND o.order_date BETWEEN c.valid_from AND c.valid_to
LEFT JOIN aurora_jk.dw_dim_order_status st
  ON o.status = st.status AND o.channel = st.channel
LEFT JOIN aurora_jk.dw_dim_date od ON CAST(o.order_ts AS date)     = od.date_actual
LEFT JOIN aurora_jk.dw_dim_date sd ON CAST(s.shipped_ts AS date)   = sd.date_actual
LEFT JOIN aurora_jk.dw_dim_date dd ON CAST(s.delivered_ts AS date) = dd.date_actual;

-- Oczekiwane: zamowienia = 170, wyslane = 66, dostarczone = 45,
-- bez_wysylki = 104, revenue 1427124.00 (ta sama kwota co na pozycjach -
-- agregacja do zamowienia niczego nie gubi).
SELECT
  COUNT(*)                                                        AS zamowienia,
  SUM(CASE WHEN ship_date_sk     <> -1 THEN 1 ELSE 0 END)         AS wyslane,
  SUM(CASE WHEN delivery_date_sk <> -1 THEN 1 ELSE 0 END)         AS dostarczone,
  SUM(CASE WHEN ship_date_sk      = -1 THEN 1 ELSE 0 END)         AS bez_wysylki,
  ROUND(SUM(order_amount_pln), 2)                                 AS revenue_pln
FROM aurora_jk.dw_fact_order_fulfillment;

-- Ten sam wymiar daty trzy razy, kazdy alias w innej roli (role-playing dimension).
-- Wiersz -1 wymiaru daty ma date_actual = 1900-01-01 i tak wyglada
-- "jeszcze nie wyslano" w raporcie - bez NULL-i i bez zgubionych wierszy.
-- Oczekiwane: 5 wierszy, O1001 ... O1005 (ORDER BY order_id).
SELECT
  f.order_id,
  od.date_actual AS order_date,
  sd.date_actual AS ship_date,
  dd.date_actual AS delivery_date,
  f.carrier,
  f.ship_lag_days,
  f.delivery_lag_days
FROM aurora_jk.dw_fact_order_fulfillment f
JOIN aurora_jk.dw_dim_date od ON f.order_date_sk    = od.date_sk
JOIN aurora_jk.dw_dim_date sd ON f.ship_date_sk     = sd.date_sk
JOIN aurora_jk.dw_dim_date dd ON f.delivery_date_sk = dd.date_sk
ORDER BY f.order_id
LIMIT 5;

-- -----------------------------------------------------------------------------
-- Z3d. dw_fact_daily_sales - fakt agregujacy (grain: dzien x kategoria x kanal)
-- -----------------------------------------------------------------------------
-- Agregat to kopia faktu na grubszym grain, liczona RAZ, zeby najczestsze
-- pytania nie czytaly 409 (w produkcji: 400 mln) wierszy. Klucze wymiarow
-- zostaja kluczami - agregat nadal jest star schema, tylko mniejszym. Miary
-- addytywne (revenue, qty, lines_n) sumuja sie dalej; orders_n (COUNT DISTINCT)
-- juz NIE - to miara semi-addytywna i trzeba o tym pamietac przy ROLLUP-ie.
CREATE TABLE IF NOT EXISTS aurora_jk.dw_fact_daily_sales
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/dw/fact_daily_sales/',
  is_external = false
) AS
SELECT
  f.order_date_sk,
  p.category,
  st.channel,
  COUNT(*)                                  AS lines_n,
  COUNT(DISTINCT f.order_id)                AS orders_n,
  SUM(f.qty)                                AS qty,
  ROUND(SUM(f.net_amount_pln), 2)           AS revenue_pln
FROM aurora_jk.dw_fact_order_item f
JOIN aurora_jk.dw_dim_product p       ON f.product_sk      = p.product_sk
JOIN aurora_jk.dw_dim_order_status st ON f.order_status_sk = st.order_status_sk
GROUP BY f.order_date_sk, p.category, st.channel;

-- Oczekiwane: 36 wierszy (2 dni x 6 kategorii x 3 kanaly - wszystkie kombinacje
-- wystapily), revenue 1427124.00 - agregat gubi wiersze, nie zlotowki.
SELECT COUNT(*) AS rows_n, ROUND(SUM(revenue_pln), 2) AS revenue_pln
FROM aurora_jk.dw_fact_daily_sales;

-- =============================================================================
