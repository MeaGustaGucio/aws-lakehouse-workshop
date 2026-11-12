-- =============================================================================
-- Dzien 1, S2 - lejek: koszyk bez zakupu tego samego dnia
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- S2. Lejek: koszyk bez zakupu
-- -----------------------------------------------------------------------------
-- Pytanie: ilu klientow danego dnia dodalo cos do koszyka, ale tego samego dnia
-- nie wygenerowalo zdarzenia purchase.
--
-- Oczekiwane: 2026-01-15 -> 27 klientow | 2026-01-16 -> 25 klientow
--
-- DISTINCT, bo jeden klient dodaje do koszyka kilka produktow - bez tego
-- liczylibysmy zdarzenia zamiast ludzi.
--
-- LEFT JOIN + IS NULL to standardowy wzorzec "jest w A, nie ma w B" (anti-join).
-- LEFT JOIN zachowuje wszystkie koszyki i wstawia NULL tam, gdzie nie znalazl
-- pasujacego zakupu; WHERE ... IS NULL zostawia wlasnie te wiersze.
-- Rownowaznie mozna uzyc NOT EXISTS.
--
-- Kluczem zlaczenia jest KLIENT I DZIEN RAZEM. Zlaczenie po samym customer_id
-- uznaloby za kupujacego kogos, kto porzucil koszyk 15.01, a kupil 16.01,
-- i zanizylo wynik.

WITH carts AS (
  SELECT DISTINCT customer_id, dt
  FROM aurora_jk.bronze_events
  WHERE event_type = 'add_to_cart'
),
buys AS (
  SELECT DISTINCT customer_id, dt
  FROM aurora_jk.bronze_events
  WHERE event_type = 'purchase'
)
SELECT
  c.dt,
  COUNT(*) AS abandoned_cart_customers
FROM carts c
LEFT JOIN buys b
  ON c.customer_id = b.customer_id
 AND c.dt = b.dt
WHERE b.customer_id IS NULL
GROUP BY c.dt
ORDER BY c.dt;
