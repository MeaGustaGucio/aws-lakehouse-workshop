-- =============================================================================
-- Dzien 3 (wersja zaawansowana), Y6 - capstone: jedna liczba przez cale 9 godzin
-- + sprzatanie
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Do podmiany: aurora_jk / aurora-jk-lake.
--
-- Capstone laczy wszystko: Bronze (dzien 1) -> Silver -> Gold Iceberg (dzien 2)
-- -> przyrost i SCD2 (dzien 3). Jedno zapytanie na warstwe, jedna kwota,
-- ktora musi sie zgadzac na kazdym etapie.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Y6a. Lineage w jednym wyniku
-- -----------------------------------------------------------------------------
-- Oczekiwane (ORDER BY etap):
--   1_bronze_items   526 (486 + 40, bez filtra statusu)
--   2_silver_items   409 1427124.00 (tylko dni 15/16 - Silver nie byl
--                    przeladowywany w wersji zaawansowanej; przyrost szedl
--                    prosto Bronze -> Gold przez MERGE)
--   3_gold_fact      449 1528670.00
--   4_gold_aggregate 36  1427124.00 (agregat z soboty - NIE zostal przeliczony)
-- Ostatni wiersz to celowa "usterka": agregat jest osobna tabela i nikt go
-- nie odswiezyl. W Redshifcie MV rozwiazal to jednym REFRESH; w Athenie
-- trzeba to zrobic samemu (Y6b).
SELECT '1_bronze_items' AS etap, COUNT(*) AS rows_n, CAST(NULL AS double) AS revenue_pln
FROM (SELECT order_id FROM aurora_jk.bronze_order_items
      UNION ALL SELECT order_id FROM aurora_jk.bronze_order_items_20260120)
UNION ALL
SELECT '2_silver_items', COUNT(*), ROUND(SUM(net_amount_pln), 2) FROM aurora_jk.silver_order_items
UNION ALL
SELECT '3_gold_fact', COUNT(*), ROUND(SUM(net_amount_pln), 2) FROM aurora_jk.dw_fact_order_item
UNION ALL
SELECT '4_gold_aggregate', COUNT(*), ROUND(SUM(revenue_pln), 2) FROM aurora_jk.dw_fact_daily_sales
ORDER BY etap;

-- -----------------------------------------------------------------------------
-- Y6b. Odswiezenie agregatu przez MERGE (agregat tez jest Icebergiem)
-- -----------------------------------------------------------------------------
-- Ten sam wzorzec co przy fakcie: przeliczamy TYLKO dni po watermarku agregatu
-- i wmergujemy. Typ ladowania: przyrost agregatu po watermarku (order_date_sk > MAX), jak Y2, nie jak zakladka.
-- Oczekiwane po MERGE: 52 | 1528670.00 - tyle samo, co MV
-- w Redshifcie po REFRESH. Drugie uruchomienie: bez zmian (podzapytanie
-- MAX(order_date_sk) daje juz 20260120, zrodlo MERGE jest puste).
MERGE INTO aurora_jk.dw_fact_daily_sales t
USING (
  SELECT f.order_date_sk, p.category, st.channel,
         COUNT(*) AS lines_n, COUNT(DISTINCT f.order_id) AS orders_n,
         SUM(f.qty) AS qty, ROUND(SUM(f.net_amount_pln), 2) AS revenue_pln
  FROM aurora_jk.dw_fact_order_item f
  JOIN aurora_jk.dw_dim_product p       ON f.product_sk      = p.product_sk
  JOIN aurora_jk.dw_dim_order_status st ON f.order_status_sk = st.order_status_sk
  WHERE f.order_date_sk > (SELECT MAX(order_date_sk) FROM aurora_jk.dw_fact_daily_sales)
  GROUP BY f.order_date_sk, p.category, st.channel
) s
ON t.order_date_sk = s.order_date_sk AND t.category = s.category AND t.channel = s.channel
WHEN MATCHED THEN UPDATE SET
  lines_n = s.lines_n, orders_n = s.orders_n, qty = s.qty, revenue_pln = s.revenue_pln
WHEN NOT MATCHED THEN INSERT (order_date_sk, category, channel, lines_n, orders_n, qty, revenue_pln)
  VALUES (s.order_date_sk, s.category, s.channel, s.lines_n, s.orders_n, s.qty, s.revenue_pln);

SELECT COUNT(*) AS rows_n, ROUND(SUM(revenue_pln), 2) AS revenue_pln
FROM aurora_jk.dw_fact_daily_sales;

-- -----------------------------------------------------------------------------
-- Y6c. Pytanie biznesowe, ktore wymaga WSZYSTKIEGO z trzech dni
-- -----------------------------------------------------------------------------
-- "Przychod per wojewodztwo klienta W CHWILI ZAKUPU, per dzien, z sumami" -
-- SCD2 (wojewodztwo z wersji obowiazujacej w dniu zamowienia), pelny kalendarz,
-- przyrost, ROLLUP. Klienci, ktorzy 20.01 zmienili wojewodztwo, licza sie
-- 15/16.01 do starego, a 20.01 do nowego.
-- Oczekiwane: 34 wiersze - 10 wojewodztw + 'razem' na kazdy z 3 dni i wiersz
-- koncowy. Wiersze 'razem': 2026-01-15 74 zamowien 653531 | 2026-01-16 96
-- 773593 | 2026-01-20 40 101546 | razem 210 1528670. Pelna lista w
-- 02_rozwiazania.md. Sortowanie wewnatrz dnia: revenue_pln malejaco.
SELECT
  COALESCE(CAST(d.date_actual AS varchar), 'razem') AS dzien,
  COALESCE(c.voivodeship, 'razem')                   AS wojewodztwo,
  COUNT(DISTINCT f.order_id)                         AS zamowienia,
  ROUND(SUM(f.net_amount_pln), 2)                    AS revenue_pln
FROM aurora_jk.dw_fact_order_item f
JOIN aurora_jk.dw_dim_date d     ON f.order_date_sk = d.date_sk
JOIN aurora_jk.dw_dim_customer c ON f.customer_sk   = c.customer_sk
GROUP BY ROLLUP (d.date_actual, c.voivodeship)
ORDER BY GROUPING(d.date_actual), d.date_actual, GROUPING(c.voivodeship), revenue_pln DESC;

-- =============================================================================
-- SPRZATANIE (po zajeciach - nie w trakcie)
-- =============================================================================
-- Tabele Iceberg: DROP TABLE kasuje TAKZE pliki na S3 (data/ i metadata/).
-- To roznica wobec tabel zewnetrznych z dnia 1, gdzie DROP zostawial pliki.
-- Koszt bez sprzatania: S3 - grosze miesiecznie za kilka MB; Glue - katalog
-- jest darmowy do milionu obiektow. Realny koszt w tej wersji to Redshift
-- Serverless (RPU-godziny) - i to sprzata osoba, ktora stawiala workgroup.
--
-- DROP TABLE IF EXISTS aurora_jk.dw_fact_daily_sales;
-- DROP TABLE IF EXISTS aurora_jk.dw_fact_order_fulfillment;
-- DROP TABLE IF EXISTS aurora_jk.dw_fact_order_item;
-- DROP TABLE IF EXISTS aurora_jk.dw_dim_order_status;
-- DROP TABLE IF EXISTS aurora_jk.dw_dim_product;
-- DROP TABLE IF EXISTS aurora_jk.dw_dim_customer;
-- DROP TABLE IF EXISTS aurora_jk.dw_dim_date;
--
-- Tabele zewnetrzne (DROP nie rusza plikow; pliki kasujemy w konsoli S3):
-- DROP TABLE IF EXISTS aurora_jk.bronze_shipments;
-- DROP TABLE IF EXISTS aurora_jk.bronze_shipments_20260121;
-- DROP TABLE IF EXISTS aurora_jk.dw_export_daily_sales;      -- + prefiks export/
-- (bronze_customers_updates i bronze_order_items_20260120 sa wspolne z wersja
--  podstawowa - jesli ktos robil oba, kasuje raz.)
--
-- Redshift (QEv2):  DROP SCHEMA dw_jk CASCADE;
