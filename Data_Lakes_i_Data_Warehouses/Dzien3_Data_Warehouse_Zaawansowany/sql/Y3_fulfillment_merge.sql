-- =============================================================================
-- Dzien 3 (wersja zaawansowana), Y3 - accumulating snapshot: spozniajace sie fakty
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Do podmiany: aurora_jk / aurora-jk-lake.
-- Wymaga dw_fact_order_fulfillment po Y2_przyrost_merge.sql (210 wierszy).
--
-- Plik: dane/bronze/shipments_20260121/shipments_2026-01-21.csv
--   -> s3://aurora-<inicjaly>-lake/bronze/shipments_20260121/
-- Zawiera trzy rodzaje wierszy - i to jest cala lekcja:
--   (a) paczki, ktore w dniu 2 byly "w drodze", teraz z delivered_ts (UPDATE
--       istniejacego wiersza faktu - to samo shipment_id co wczoraj),
--   (b) spoznione wysylki zamowien z 15/16.01, ktore w dniu 2 byly 'paid'
--       (LATE-ARRIVING FACT: zdarzenie dotyczy starego dnia, a przychodzi dzis),
--   (c) wysylki zamowien z 20.01.
-- Fakt transakcyjny by tego nie uniosl bez duplikatow; accumulating snapshot
-- jest zaprojektowany wlasnie po to, zeby byc aktualizowany.
-- Typ ladowania: zakladka / late-arriving. Plik z 21.01 poprawia zamowienia z 15, 16 i 20.01;
-- MERGE po order_id aktualizuje istniejacy wiersz, nie dopisuje duplikatu.
-- =============================================================================

-- bronze_shipments_20260121: jeden wiersz to jedna wysylka z pliku 21.01 (88 wierszy).
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_shipments_20260121 (
  shipment_id  string,
  order_id     string,
  carrier      string,
  shipped_ts   string,
  delivered_ts string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/shipments_20260121/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Oczekiwane: 88 wierszy; z_dostawa 51 | bez_dostawy 37;
-- znane_wczoraj 21 (te same shipment_id co w bronze_shipments), czyli 67 nowych.
SELECT COUNT(*) AS wysylki,
       SUM(CASE WHEN delivered_ts <> '' THEN 1 ELSE 0 END) AS z_dostawa,
       SUM(CASE WHEN delivered_ts =  '' THEN 1 ELSE 0 END) AS bez_dostawy,
       SUM(CASE WHEN shipment_id IN (SELECT shipment_id FROM aurora_jk.bronze_shipments)
                THEN 1 ELSE 0 END) AS znane_wczoraj
FROM aurora_jk.bronze_shipments_20260121;

-- Stan PRZED. Oczekiwane: 210 | wyslane 66 | dostarczone 45.
SELECT COUNT(*) AS zamowienia,
       SUM(CASE WHEN ship_date_sk     <> -1 THEN 1 ELSE 0 END) AS wyslane,
       SUM(CASE WHEN delivery_date_sk <> -1 THEN 1 ELSE 0 END) AS dostarczone
FROM aurora_jk.dw_fact_order_fulfillment;

-- -----------------------------------------------------------------------------
-- MERGE - tylko UPDATE, bez INSERT
-- -----------------------------------------------------------------------------
-- Wysylka bez zamowienia w fakcie to blad danych, nie nowy fakt - dlatego nie
-- ma galezi WHEN NOT MATCHED (Athena tego nie wymaga; Redshift wymaga obu).
-- Wiersz zrodla jest zlaczony z wymiarem daty DWA razy (sd, dd) - dwie role.
-- Zamowienie z wieloma wysylkami mialoby tu problem (MERGE odrzuca zrodlo,
-- w ktorym dwa wiersze trafiaja w ten sam wiersz celu) - stad ROW_NUMBER
-- i wybor najnowszej wysylki per zamowienie.
--
-- date_diff liczy od order_ts, ktory jest w silver_orders (albo w Bronze dla
-- 20.01) - stad zlaczenie z bronze_orders, ktore ma wszystkie 240 zamowien.
MERGE INTO aurora_jk.dw_fact_order_fulfillment t
USING (
  SELECT order_id, carrier, ship_date_sk, delivery_date_sk, ship_lag_days, delivery_lag_days
  FROM (
    SELECT
      s.order_id,
      s.carrier,
      COALESCE(sd.date_sk, -1) AS ship_date_sk,
      COALESCE(dd.date_sk, -1) AS delivery_date_sk,
      CAST(date_diff('day', CAST(o.order_ts AS timestamp), CAST(s.shipped_ts AS timestamp)) AS integer) AS ship_lag_days,
      CAST(date_diff('day', CAST(o.order_ts AS timestamp),
                     CAST(NULLIF(s.delivered_ts, '') AS timestamp)) AS integer)             AS delivery_lag_days,
      ROW_NUMBER() OVER (PARTITION BY s.order_id ORDER BY s.shipped_ts DESC) AS rn
    FROM aurora_jk.bronze_shipments_20260121 s
    JOIN aurora_jk.bronze_orders o ON s.order_id = o.order_id
    LEFT JOIN aurora_jk.dw_dim_date sd ON CAST(substr(s.shipped_ts, 1, 10) AS date)   = sd.date_actual
    LEFT JOIN aurora_jk.dw_dim_date dd ON CAST(substr(NULLIF(s.delivered_ts, ''), 1, 10) AS date) = dd.date_actual
  ) x
  WHERE rn = 1
) s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  carrier           = s.carrier,
  ship_date_sk      = s.ship_date_sk,
  delivery_date_sk  = s.delivery_date_sk,
  ship_lag_days     = s.ship_lag_days,
  delivery_lag_days = s.delivery_lag_days;

-- =============================================================================
-- KONTROLA
-- =============================================================================
-- Oczekiwane po MERGE: 210 | wyslane 133 | dostarczone 96 | bez_wysylki 77.
-- (przed MERGE bylo 210 | 66 | 45 | 144). Liczba wierszy NIE ROSNIE - zmienia
-- sie tresc. Po drugim uruchomieniu: identycznie.
SELECT COUNT(*) AS zamowienia,
       SUM(CASE WHEN ship_date_sk     <> -1 THEN 1 ELSE 0 END) AS wyslane,
       SUM(CASE WHEN delivery_date_sk <> -1 THEN 1 ELSE 0 END) AS dostarczone,
       SUM(CASE WHEN ship_date_sk      = -1 THEN 1 ELSE 0 END) AS bez_wysylki
FROM aurora_jk.dw_fact_order_fulfillment;

-- Rozklad dat wysylki dla zamowien z 15/16.01 wyslanych 17.01 albo pozniej -
-- to sa wiersze, ktore fakt transakcyjny nigdy by nie "dogonil".
-- Oczekiwane (ORDER BY ship_date):
--   2026-01-17 | 50 | 0.6
--   2026-01-18 | 14 | 1.8
--   2026-01-19 | 13 | 2.8
--   2026-01-20 |  7 | 3.4
SELECT sd.date_actual AS ship_date, COUNT(*) AS zamowienia,
       ROUND(AVG(f.ship_lag_days), 1) AS sredni_lag_dni
FROM aurora_jk.dw_fact_order_fulfillment f
JOIN aurora_jk.dw_dim_date od ON f.order_date_sk = od.date_sk
JOIN aurora_jk.dw_dim_date sd ON f.ship_date_sk  = sd.date_sk
WHERE od.date_actual <= DATE '2026-01-16' AND sd.date_actual >= DATE '2026-01-17'
GROUP BY sd.date_actual
ORDER BY ship_date;

-- Raport, ktory bylby niemozliwy na fakcie transakcyjnym: czas realizacji per
-- przewoznik (wymiar daty w trzech rolach: zamowienie, wysylka, dostawa). Oczekiwane (ORDER BY
-- carrier):
--   DHL           | 23 | 18 | 1.09 | 2.78
--   DPD           | 34 | 25 | 0.97 | 2.92
--   InPost        | 41 | 28 | 0.98 | 2.61
--   Poczta Polska | 35 | 25 | 0.97 | 2.72
SELECT carrier,
       COUNT(*) AS zamowienia,
       SUM(CASE WHEN delivery_date_sk <> -1 THEN 1 ELSE 0 END) AS dostarczone,
       ROUND(AVG(ship_lag_days), 2)     AS sredni_lag_wysylki,
       ROUND(AVG(delivery_lag_days), 2) AS sredni_lag_dostawy
FROM aurora_jk.dw_fact_order_fulfillment
WHERE carrier <> 'n/a'
GROUP BY carrier
ORDER BY carrier;

-- Time travel jako audyt: ktore zamowienia dostaly wysylke DZISIAJ?
-- Porownujemy stan biezacy ze snapshotem sprzed MERGE (ten z Y2_przyrost_merge.sql,
-- operation = overwrite, added = 40). Oczekiwane snapshoty (ORDER BY committed_at):
-- append 170 (CTAS, Dzien 2) | overwrite 40 (przyrost) | overwrite 40 (drugie
-- uruchomienie Y2_przyrost_merge.sql) | overwrite 88 (ten MERGE); po drugim
-- uruchomieniu tego pliku dochodzi kolejny overwrite 88. Do time travel nizej
-- pasuje KTORYKOLWIEK z dwu 'overwrite 40' - dane sa w nich identyczne.
SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added
FROM "aurora_jk"."dw_fact_order_fulfillment$snapshots"
ORDER BY committed_at;

-- FOR VERSION AS OF przyjmuje TYLKO literal bigint (nie podzapytanie i nie
-- liczbe z zerami wiodacymi - blad "Unsupported type for table version:
-- integer"). Wklejamy snapshot_id wiersza 'overwrite 40' z wyniku powyzej.
-- Oczekiwane: nowo_wyslane 67 | nowo_dostarczone 51 (dokladnie tyle nowych
-- shipment_id / delivered_ts bylo w pliku z 21.01).
SELECT SUM(CASE WHEN o.ship_date_sk      = -1 AND f.ship_date_sk     <> -1 THEN 1 ELSE 0 END) AS nowo_wyslane,
       SUM(CASE WHEN o.delivery_date_sk  = -1 AND f.delivery_date_sk <> -1 THEN 1 ELSE 0 END) AS nowo_dostarczone
FROM aurora_jk.dw_fact_order_fulfillment f
JOIN aurora_jk.dw_fact_order_fulfillment FOR VERSION AS OF 3900263472920469512 o  -- <- snapshot_id 'overwrite 40'
  ON f.order_id = o.order_id;
