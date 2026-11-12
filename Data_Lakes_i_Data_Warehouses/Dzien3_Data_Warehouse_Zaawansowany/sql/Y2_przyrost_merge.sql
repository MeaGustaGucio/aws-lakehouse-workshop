-- =============================================================================
-- Dzien 3 (wersja zaawansowana), Y2 - ladowanie przyrostowe dnia 2026-01-20 przez MERGE
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Wymaga: dw_* z dnia 2 oraz dw_dim_customer po SCD2 (Y1_scd2_merge.sql).
--
-- Pliki zrodlowe (dane/ tej wersji; upload i MSCK / CREATE sa w Y0):
--   dane/bronze/orders/dt=2026-01-20/orders.csv
--     -> s3://aurora-<inicjaly>-lake/bronze/orders/dt=2026-01-20/
--   dane/bronze/order_items_20260120/order_items_2026-01-20.csv
--     -> s3://aurora-<inicjaly>-lake/bronze/order_items_20260120/
--
-- CEL: dolozyc dzien BEZ przeliczania 15 i 16 stycznia i tak, zeby drugie
-- uruchomienie nie zmienilo nic (idempotencja). W wersji podstawowej oznaczalo
-- to nowa tabele faktu + widok UNION ALL + nowa wersje wymiaru daty. Tutaj:
--   * wymiar daty juz zna 20 stycznia (pelny kalendarz z Z1),
--   * fakt jest jedna tabela Iceberg, MERGE dopisuje wiersze W MIEJSCU,
--   * kluczem zlaczenia jest KLUCZ ZLOZONY (order_id, product_sk), bo pozycja
--     zamowienia nie ma wlasnego identyfikatora - to ten sam problem, ktory
--     w wersji podstawowej jest zadaniem S4.
-- Sprawdzone dwukrotnie w Athenie v3: 449 po pierwszym i po drugim uruchomieniu.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Krok 1. Bronze - nowa partycja zamowien i osobna tabela pozycji
-- -----------------------------------------------------------------------------
-- Katalog Glue nie przeszukuje S3 sam. Wariant punktowy zamiast MSCK:
--   ALTER TABLE aurora_jk.bronze_orders ADD IF NOT EXISTS
--   PARTITION (dt = '2026-01-20') LOCATION 's3://aurora-jk-lake/bronze/orders/dt=2026-01-20/';
MSCK REPAIR TABLE aurora_jk.bronze_orders;

-- Oczekiwane: 2026-01-15 90 | 2026-01-16 110 | 2026-01-20 40.
SELECT dt, COUNT(*) AS n FROM aurora_jk.bronze_orders GROUP BY dt ORDER BY dt;

-- Pozycje nowego dnia jako OSOBNA tabela, bo bronze_order_items nie jest
-- partycjonowane - plik wrzucony do bronze/order_items/ zepsulby liczby
-- kontrolne dnia 1 (486 -> 526). DDL identyczny jak w wersji podstawowej.
-- bronze_order_items_20260120: jeden wiersz to jedna pozycja z 20.01 (40 wierszy).
-- Osobna tabela, bo bronze_order_items nie jest partycjonowane.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_order_items_20260120 (
  order_id string,
  product_id int,
  qty int,
  unit_price_pln double,
  discount_pln double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/order_items_20260120/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Oczekiwane: nowe = 40, stare = 486.
SELECT (SELECT COUNT(*) FROM aurora_jk.bronze_order_items_20260120) AS items_20,
       (SELECT COUNT(*) FROM aurora_jk.bronze_order_items)          AS items_dzien1;

-- -----------------------------------------------------------------------------
-- Krok 2. Watermark - od ktorego dnia liczyc
-- -----------------------------------------------------------------------------
-- Watermark to najwieksza data juz zaladowana do faktu. Wszystko po niej jest
-- przyrostem. W produkcji trzyma sie go w osobnej tabeli sterujacej; tu
-- wystarcza MAX() z faktu, bo fakt jest jedyna tabela docelowa.
-- Typ ladowania: przyrost tylko nowszych wierszy (append-only incremental) po watermarku.
-- Korekta wczorajszego wiersza tu nie wejdzie — do tego jest zakladka (Y3).
-- Oczekiwane: 2026-01-16 (przed ladowaniem) i 2026-01-20 (po nim).
SELECT MAX(order_date) AS watermark FROM aurora_jk.dw_fact_order_item;

-- -----------------------------------------------------------------------------
-- Krok 3. MERGE - przyrost do faktu transakcyjnego
-- -----------------------------------------------------------------------------
-- USING to CALA transformacja Bronze -> Gold dla nowego dnia w jednym
-- podzapytaniu: filtr partycji (dt > watermark), filtr statusu, rzutowania,
-- wyszukanie surrogate keys. Wymiar klienta po DWU warunkach: customer_id
-- ORAZ order_date BETWEEN valid_from AND valid_to - zamowienia z 20 stycznia
-- trafiaja w NOWE wersje (sk > 80) tych osmiu klientow, ktorzy sie zmienili.
--
-- ON po kluczu zlozonym (order_id, product_sk). WHEN MATCHED aktualizuje miary
-- (korekta pozycji), WHEN NOT MATCHED wstawia. Drugie uruchomienie: kazdy
-- wiersz zrodla trafia w MATCHED z tymi samymi wartosciami -> 449 bez zmian.
--
-- WHEN NOT MATCHED THEN INSERT * NIE dziala w Athenie (skladnia Delta/Spark):
-- "mismatched input '*'". Kolumny trzeba wypisac w obu listach.
-- WHEN NOT MATCHED BY SOURCE (usuwanie wierszy nieobecnych w zrodle, skladnia
-- SQL Server / Databricks) rowniez NIE istnieje w Athenie - sprawdzone,
-- opis w 02_rozwiazania.md.
-- Typ ladowania: upsert / MERGE po (order_id, product_sk). Filtr zrodla jest watermark
-- (nie zakladka); MATCHED + NOT MATCHED daje idempotencje (449 dwa razy).
MERGE INTO aurora_jk.dw_fact_order_item t
USING (
  SELECT
    i.order_id,
    CAST(substr(o.order_ts, 1, 10) AS date)              AS order_date,
    COALESCE(d.date_sk, -1)                              AS order_date_sk,
    COALESCE(c.customer_sk, -1)                          AS customer_sk,
    COALESCE(p.product_sk, -1)                           AS product_sk,
    COALESCE(st.order_status_sk, -1)                     AS order_status_sk,
    i.qty,
    i.unit_price_pln,
    i.discount_pln,
    (i.qty * i.unit_price_pln - i.discount_pln)          AS net_amount_pln
  FROM aurora_jk.bronze_order_items_20260120 i
  JOIN aurora_jk.bronze_orders o
    ON i.order_id = o.order_id
   AND o.dt > '2026-01-16'                              -- watermark
   AND o.status IN ('paid', 'shipped')
  LEFT JOIN aurora_jk.dw_dim_date d
    ON CAST(substr(o.order_ts, 1, 10) AS date) = d.date_actual
  LEFT JOIN aurora_jk.dw_dim_customer c
    ON o.customer_id = c.customer_id
   AND CAST(substr(o.order_ts, 1, 10) AS date) BETWEEN c.valid_from AND c.valid_to
  LEFT JOIN aurora_jk.dw_dim_product p
    ON i.product_id = p.product_id
  LEFT JOIN aurora_jk.dw_dim_order_status st
    ON o.status = st.status AND o.channel = st.channel
) s
ON t.order_id = s.order_id AND t.product_sk = s.product_sk
WHEN MATCHED THEN UPDATE SET
  qty            = s.qty,
  unit_price_pln = s.unit_price_pln,
  discount_pln   = s.discount_pln,
  net_amount_pln = s.net_amount_pln
WHEN NOT MATCHED THEN INSERT (order_id, order_date, order_date_sk, customer_sk,
                              product_sk, order_status_sk, qty, unit_price_pln,
                              discount_pln, net_amount_pln, load_ts)
  VALUES (s.order_id, s.order_date, s.order_date_sk, s.customer_sk,
          s.product_sk, s.order_status_sk, s.qty, s.unit_price_pln,
          s.discount_pln, s.net_amount_pln, CAST(current_timestamp AS timestamp(6)));

-- =============================================================================
-- KONTROLA
-- =============================================================================
-- Oczekiwane (ORDER BY order_date): 2026-01-15 186 | 2026-01-16 223 |
-- 2026-01-20 40 101546.00. Razem 449. Po drugim uruchomieniu: identycznie.
SELECT order_date, COUNT(*) AS rows_n, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.dw_fact_order_item
GROUP BY order_date
ORDER BY order_date;

-- Stare dni NIETKNIETE: 409 | 1427124.00. Nowy dzien wskazuje nowe wersje
-- klientow tam, gdzie klient sie zmienil. Oczekiwane: na_nowe_wersje = 0 dla
-- 15 i 16 stycznia, 2 dla 2026-01-20 (dwie pozycje klientow z nowa wersja).
SELECT order_date,
       COUNT(*) AS rows_n,
       SUM(CASE WHEN customer_sk > 80 THEN 1 ELSE 0 END) AS na_nowe_wersje
FROM aurora_jk.dw_fact_order_item
GROUP BY order_date
ORDER BY order_date;

-- Asercje z dnia 2 - musza przejsc takze po przyroscie. Oczekiwane: 4 x OK.
WITH checks AS (
  SELECT 'B1 grain unique (order_id, product_sk)' AS test, 0 AS expected,
         (SELECT COUNT(*) FROM (SELECT order_id, product_sk FROM aurora_jk.dw_fact_order_item
                                GROUP BY order_id, product_sk HAVING COUNT(*) > 1)) AS actual
  UNION ALL
  SELECT 'B2 no unknown customer', 0,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item WHERE customer_sk = -1)
  UNION ALL
  SELECT 'B3 no unknown date', 0,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item WHERE order_date_sk = -1)
  UNION ALL
  SELECT 'B4 fact = silver d1 + bronze d20', 409 + 40,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item)
)
SELECT test, expected, actual, CASE WHEN expected = actual THEN 'OK' ELSE 'FAIL' END AS status
FROM checks ORDER BY test;

-- Partycje po przyroscie: trzecia partycja pojawila sie sama (hidden
-- partitioning po order_date). Oczekiwane po pierwszym uruchomieniu: 3 wiersze,
-- 2026-01-20 = 40 wierszy w 1 pliku. Po DRUGIM uruchomieniu: 2026-01-20 =
-- 80 wierszy w 2 plikach - $partitions liczy wiersze FIZYCZNE w plikach
-- danych; 40 z nich jest skasowanych logicznie plikiem usuniec (merge-on-read).
-- SELECT COUNT(*) nadal daje 40. To jest powod, dla ktorego istnieje OPTIMIZE (Y4).
SELECT partition, record_count, file_count
FROM "aurora_jk"."dw_fact_order_item$partitions"
ORDER BY partition;

-- Snapshoty faktu. Oczekiwane po pierwszym MERGE: append (CTAS, 409)
-- i overwrite (MERGE, added 40, position_deletes NULL - same INSERT-y).
-- Po drugim uruchomieniu dochodzi kolejny overwrite: added 40 i
-- position_deletes 40 - MERGE podmienil 40 wierszy na identyczne.
-- Idempotencja dotyczy DANYCH, nie liczby snapshotow.
SELECT committed_at, operation,
       summary['added-records']          AS added,
       summary['added-position-deletes'] AS position_deletes
FROM "aurora_jk"."dw_fact_order_item$snapshots"
ORDER BY committed_at;

-- -----------------------------------------------------------------------------
-- Krok 4. Accumulating snapshot - nowe zamowienia dochodza jako nowe wiersze
-- -----------------------------------------------------------------------------
-- Ten sam MERGE, inny grain (zamowienie). Wysylki dla 20 stycznia przyjda
-- dopiero w Y3_fulfillment_merge.sql - dzis wszystkie 40 ma ship_date_sk = -1.
-- Typ ladowania: upsert nowych zamowien do accumulating snapshot (watermark jak wyzej, nie zakladka).
-- Oczekiwane po MERGE: 210 zamowien (170 + 40), takze po drugim uruchomieniu.
MERGE INTO aurora_jk.dw_fact_order_fulfillment t
USING (
  SELECT
    o.order_id,
    COALESCE(c.customer_sk, -1)      AS customer_sk,
    COALESCE(st.order_status_sk, -1) AS order_status_sk,
    COALESCE(d.date_sk, -1)          AS order_date_sk,
    SUM(i.qty * i.unit_price_pln - i.discount_pln) AS order_amount_pln,
    CAST(COUNT(*) AS integer)        AS items_n
  FROM aurora_jk.bronze_orders o
  JOIN aurora_jk.bronze_order_items_20260120 i ON o.order_id = i.order_id
  LEFT JOIN aurora_jk.dw_dim_date d
    ON CAST(substr(o.order_ts, 1, 10) AS date) = d.date_actual
  LEFT JOIN aurora_jk.dw_dim_customer c
    ON o.customer_id = c.customer_id
   AND CAST(substr(o.order_ts, 1, 10) AS date) BETWEEN c.valid_from AND c.valid_to
  LEFT JOIN aurora_jk.dw_dim_order_status st
    ON o.status = st.status AND o.channel = st.channel
  WHERE o.dt > '2026-01-16' AND o.status IN ('paid', 'shipped')
  GROUP BY o.order_id, c.customer_sk, st.order_status_sk, d.date_sk
) s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  order_amount_pln = s.order_amount_pln,
  items_n          = s.items_n
WHEN NOT MATCHED THEN INSERT (order_id, customer_sk, order_status_sk, order_date_sk,
                              ship_date_sk, delivery_date_sk, carrier,
                              order_amount_pln, items_n, ship_lag_days, delivery_lag_days)
  VALUES (s.order_id, s.customer_sk, s.order_status_sk, s.order_date_sk,
          -1, -1, 'n/a', s.order_amount_pln, s.items_n, NULL, NULL);

-- Oczekiwane: zamowienia 210 | wyslane 66 | dostarczone 45 | revenue 1528670.00
-- (1427124 + 101546).
SELECT COUNT(*)                                                 AS zamowienia,
       SUM(CASE WHEN ship_date_sk     <> -1 THEN 1 ELSE 0 END)  AS wyslane,
       SUM(CASE WHEN delivery_date_sk <> -1 THEN 1 ELSE 0 END)  AS dostarczone,
       ROUND(SUM(order_amount_pln), 2)                          AS revenue_pln
FROM aurora_jk.dw_fact_order_fulfillment;
