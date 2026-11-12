-- =============================================================================
-- Dzien 1, D3 - widok na Silver
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
-- NIC Z TEGO PLIKU NIE JEST POTRZEBNE W DNIU 2 ANI 3.
-- Kazdy nowy obiekt ma w nazwie _extra; pliki CTAS laduja pod extra/.
-- =============================================================================

-- D3. Widok na Silver - zapisane zapytanie, nie zapisane dane
-- -----------------------------------------------------------------------------
-- ORDER BY nie wchodzi do definicji widoku: kolejnosc ustala zapytanie,
-- ktore z widoku czyta.
CREATE OR REPLACE VIEW aurora_jk.v_revenue_by_day_extra AS
SELECT dt, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
GROUP BY dt;

-- Oczekiwane: 2026-01-15 -> 653531.00 | 2026-01-16 -> 773593.00
-- Data scanned = 1 717 B - identycznie jak wariant 2 z L6. Widok nic nie skraca.
SELECT dt, revenue_pln
FROM aurora_jk.v_revenue_by_day_extra
ORDER BY dt;

-- Oczekiwane: 773593.00. Data scanned = 940 B - tyle samo co wariant 3 z L6.
-- Filtr nalozony NA WIDOK zostal przepchniety do srodka i wycial partycje.
SELECT dt, revenue_pln
FROM aurora_jk.v_revenue_by_day_extra
WHERE dt = '2026-01-16';

-- To samo bez widoku - dla porownania Data scanned (1 717 B).
SELECT dt, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
GROUP BY dt
ORDER BY dt;

-- Definicja widoku, tak jak ja pamieta katalog.
SHOW CREATE VIEW aurora_jk.v_revenue_by_day_extra;

-- Widok w katalogu Glue to tez "tabela", tylko innego typu.
-- Oczekiwane m.in.: v_revenue_by_day_extra | VIEW, reszta BASE TABLE.
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'aurora_jk'
ORDER BY table_name;

-- Ta sama tresc ZAPISANA jako dane (CTAS). Skan przy odczycie spada
-- z 1 717 B do 113 B - ale to zdjecie z chwili utworzenia: nowy dzien
-- w silver_order_items tu nie trafi, dopoki tabela nie zostanie przeliczona.
CREATE TABLE IF NOT EXISTS aurora_jk.silver_revenue_by_day_extra
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/extra/silver_revenue_by_day/'
) AS
SELECT dt, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
GROUP BY dt;

-- Oczekiwane: te same dwie kwoty, Data scanned = 113 B.
SELECT dt, revenue_pln
FROM aurora_jk.silver_revenue_by_day_extra
ORDER BY dt;

-- -----------------------------------------------------------------------------
