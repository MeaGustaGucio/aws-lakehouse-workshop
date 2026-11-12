-- =============================================================================
-- Dzien 1, L4 - pytania biznesowe na warstwie Bronze
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk.
-- Kazde zapytanie ma pod soba oczekiwany wynik. Wartosci pochodza z realnego
-- przebiegu w konsoli AWS i zgadzaja sie z tools/expected_results.py.
--
-- Przed porownywaniem "Data scanned" warto wylaczyc przelacznik
-- "Reuse query results" nad wynikami - inaczej powtorzone zapytanie pokaze 0 B.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Ile zamowien jest oplaconych lub wyslanych
-- -----------------------------------------------------------------------------
-- Oczekiwane: 170 zamowien paid/shipped (z 200 w Bronze; 30 cancelled odpada).
-- Wynik 200 oznacza pominiety warunek status - w Bronze siedza tez zamowienia
-- anulowane i nic ich za nas nie odfiltruje.
SELECT COUNT(*) AS orders_ok
FROM aurora_jk.bronze_orders
WHERE status IN ('paid', 'shipped');

-- -----------------------------------------------------------------------------
-- 2. Przychod netto wg dnia
-- -----------------------------------------------------------------------------
-- Oczekiwane: 2026-01-15 -> 653531.00 | 2026-01-16 -> 773593.00
--             razem 1427124.00
--
-- Kolejnosc dziala: qty * unit_price - discount. Rabat jest kwota NA POZYCJI,
-- nie na sztuce, wiec odejmuje sie go po pomnozeniu. Odwrotna kolejnosc
-- (qty * (unit_price - discount)) zanizy przychod.
SELECT
  o.dt,
  ROUND(SUM(i.qty * i.unit_price_pln - i.discount_pln), 2) AS revenue_pln
FROM aurora_jk.bronze_orders o
JOIN aurora_jk.bronze_order_items i ON o.order_id = i.order_id
WHERE o.status IN ('paid', 'shipped')
GROUP BY o.dt
ORDER BY o.dt;

-- -----------------------------------------------------------------------------
-- 3. Piec kategorii o najwyzszym przychodzie
-- -----------------------------------------------------------------------------
-- Oczekiwane:
--   Telefony    381977.00
--   Komputery   362605.00
--   TV          324800.00
--   AGD         261065.00
--   Audio        83254.00
--
-- Suma tej piatki to 1413701.00, a nie 1427124.00 z punktu 2. W datasecie jest
-- szesc kategorii - szosta, Akcesoria (13423.00), wypada poza LIMIT 5.
--
-- Potrzebne sa trzy tabele: status i dzien sa przy zamowieniu, kwoty przy
-- pozycji, a kategoria przy produkcie.
SELECT
  p.category,
  ROUND(SUM(i.qty * i.unit_price_pln - i.discount_pln), 2) AS revenue_pln
FROM aurora_jk.bronze_orders o
JOIN aurora_jk.bronze_order_items i ON o.order_id = i.order_id
JOIN aurora_jk.bronze_products p    ON i.product_id = p.product_id
WHERE o.status IN ('paid', 'shipped')
GROUP BY p.category
ORDER BY revenue_pln DESC
LIMIT 5;

-- -----------------------------------------------------------------------------
-- 4. Unikalni klienci w kanale mobile
-- -----------------------------------------------------------------------------
-- Oczekiwane: 35 unikalnych klientow w kanale mobile, w wsrod paid/shipped.
-- COUNT(DISTINCT ...), bo jeden klient moze miec kilka zamowien tego dnia.
SELECT COUNT(DISTINCT customer_id) AS mobile_customers
FROM aurora_jk.bronze_orders
WHERE channel = 'mobile'
  AND status IN ('paid', 'shipped');
