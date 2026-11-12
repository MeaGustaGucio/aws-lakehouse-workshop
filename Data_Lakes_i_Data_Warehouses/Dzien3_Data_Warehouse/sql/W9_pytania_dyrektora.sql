-- =============================================================================
-- Dzien 3, W9 - pytania dyrektora handlowego
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>.
-- Wymaga gold_dim_customer_scd z W6 i v_fact_order_item_all z W7.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- W9 pkt 1. Przychod wg miasta W MOMENCIE zakupu (as-was)
-- -----------------------------------------------------------------------------
-- Zlaczenie idzie po customer_sk, czyli po kluczu ZAPISANYM W FAKCIE. Fakt
-- ze stycznia wskazuje na wersje wymiaru sprzed przeprowadzki, wiec bez zadnego
-- dodatkowego warunku dostajemy stan historyczny. Nie ma tu ani is_current,
-- ani BETWEEN - cala praca zostala wykonana w momencie ladowania faktu.
--
-- Filtr po dacie idzie przez wymiar daty, a nie po kolumnie dt na faktcie.
-- Oba warianty daja ten sam wynik; ten pokazuje, jak model wymiarowy jest
-- pomyslany - kazdy atrybut opisowy wchodzi do zapytania przez wymiar.
SELECT
  c.city,
  ROUND(SUM(f.net_amount_pln), 2) AS revenue_as_was
FROM aurora_jk.gold_fact_order_item f
JOIN aurora_jk.gold_dim_customer_scd c ON f.customer_sk = c.customer_sk
JOIN aurora_jk.gold_dim_date d         ON f.date_sk     = d.date_sk
WHERE d.date_actual BETWEEN DATE '2026-01-15' AND DATE '2026-01-16'
GROUP BY c.city
ORDER BY revenue_as_was DESC;

-- -----------------------------------------------------------------------------
-- W9 pkt 2. Przychod wg miasta AKTUALNEGO (as-is)
-- -----------------------------------------------------------------------------
-- Dwa zlaczenia do TEGO SAMEGO wymiaru, pod dwoma aliasami.
--   hist - wersja wskazywana przez fakt; jedyne, po co, to wyciagniecie
--          customer_id
--   cur  - przejscie z tego customer_id na wersje biezaca
--
-- To standardowy wzorzec dla raportu "historia wedlug dzisiejszej struktury".
-- Wraca wszedzie tam, gdzie zmienia sie podzial organizacyjny: sprzedaz sprzed
-- reorganizacji ogladana wedlug nowych regionow, koszty sprzed fuzji wedlug
-- nowych spolek.
--
-- WARUNEK cur.is_current = true JEST OBOWIAZKOWY. Bez niego dla osemki, ktora
-- sie przeprowadzila, zlaczenie trafiloby na dwie wersje naraz i przychod tych
-- klientow policzylby sie dwukrotnie - suma wyszlaby wyzsza niz 1427124.00.
SELECT
  cur.city,
  ROUND(SUM(f.net_amount_pln), 2) AS revenue_as_is
FROM aurora_jk.gold_fact_order_item f
JOIN aurora_jk.gold_dim_customer_scd hist ON f.customer_sk = hist.customer_sk
JOIN aurora_jk.gold_dim_customer_scd cur
  ON hist.customer_id = cur.customer_id
 AND cur.is_current = true
JOIN aurora_jk.gold_dim_date d ON f.date_sk = d.date_sk
WHERE d.date_actual BETWEEN DATE '2026-01-15' AND DATE '2026-01-16'
GROUP BY cur.city
ORDER BY revenue_as_is DESC;

-- -----------------------------------------------------------------------------
-- W9 pkt 3. Miasta, w ktorych liczby sie roznia
-- -----------------------------------------------------------------------------
-- Oczekiwane - siedem miast:
--   Bialystok    90937.00 ->  135235.00   (+44298.00)
--   Katowice     80287.00 ->  112591.00   (+32304.00)
--   Warszawa    228367.00 ->  239214.00   (+10847.00)
--   Lublin      151067.00 ->  140220.00   (-10847.00)
--   Gdansk      328841.00 ->  315271.00   (-13570.00)
--   Wroclaw     108517.00 ->   86023.00   (-22494.00)
--   Krakow      170570.00 ->  130032.00   (-40538.00)
--
-- SUMA OBU KOLUMN JEST TAKA SAMA: 1427124.00, a suma roznic wynosi zero.
-- Przychod sie nie zmienia, zmienia sie wylacznie to, komu go przypisujemy.
-- Krakow "traci" 40538 zl, ktorych nigdy nie stracil.
--
-- Poznan nie pojawia sie na liscie, mimo ze wyprowadzili sie z niego C0018
-- i C0023. Powod jest prozaiczny: ta dwojka nie zlozyla 15 ani 16 stycznia
-- zadnego oplaconego zamowienia, wiec nie ma czego przenosic. Brak roznicy
-- nie zawsze znaczy blad.
--
-- Gdyby pkt 1 i pkt 2 daly identyczne liczby, zlaczenie w pkt 1 poszlo
-- po is_current juz na poziomie faktu - a to kasuje cala wartosc SCD Type 2
-- i sprowadza wymiar z powrotem do Type 1.
--
-- Ponizsze zapytanie zestawia oba warianty w jednym wyniku, zeby nie trzeba bylo
-- porownywac dwoch siatek recznie. FULL JOIN, bo miasto moze wystapic tylko
-- po jednej stronie (ktos wprowadzil sie do miasta, w ktorym wczesniej nie bylo
-- ani jednego klienta).
WITH as_was AS (
  SELECT c.city, SUM(f.net_amount_pln) AS revenue_pln
  FROM aurora_jk.gold_fact_order_item f
  JOIN aurora_jk.gold_dim_customer_scd c ON f.customer_sk = c.customer_sk
  JOIN aurora_jk.gold_dim_date d         ON f.date_sk     = d.date_sk
  WHERE d.date_actual BETWEEN DATE '2026-01-15' AND DATE '2026-01-16'
  GROUP BY c.city
),
as_is AS (
  SELECT cur.city, SUM(f.net_amount_pln) AS revenue_pln
  FROM aurora_jk.gold_fact_order_item f
  JOIN aurora_jk.gold_dim_customer_scd hist ON f.customer_sk = hist.customer_sk
  JOIN aurora_jk.gold_dim_customer_scd cur
    ON hist.customer_id = cur.customer_id
   AND cur.is_current = true
  JOIN aurora_jk.gold_dim_date d ON f.date_sk = d.date_sk
  WHERE d.date_actual BETWEEN DATE '2026-01-15' AND DATE '2026-01-16'
  GROUP BY cur.city
)
SELECT
  COALESCE(w.city, i.city) AS city,
  ROUND(COALESCE(w.revenue_pln, 0), 2) AS revenue_as_was,
  ROUND(COALESCE(i.revenue_pln, 0), 2) AS revenue_as_is,
  ROUND(COALESCE(i.revenue_pln, 0) - COALESCE(w.revenue_pln, 0), 2) AS roznica
FROM as_was w
FULL JOIN as_is i ON w.city = i.city
WHERE COALESCE(w.revenue_pln, 0) <> COALESCE(i.revenue_pln, 0)
ORDER BY roznica DESC;

-- -----------------------------------------------------------------------------
-- W9 pkt 4. Koszyk bez zakupu 16 stycznia
-- -----------------------------------------------------------------------------
-- Oczekiwane: 55 klientow z add_to_cart, 14 bez zakupu, 25.5%.
--
-- DISTINCT w obu zbiorach jest konieczny: jeden klient dodaje do koszyka kilka
-- produktow i sklada kilka zamowien, a pytanie dotyczy ludzi, nie zdarzen.
--
-- COUNT(*) - COUNT(b.customer_id) to zwiezly wariant wzorca anti-join.
-- COUNT(*) liczy wszystkie wiersze po LEFT JOIN, COUNT(b.customer_id) pomija
-- NULL-e, wiec roznica to dokladnie ci, ktorzy nie mieli dopasowania.
-- Rownowaznie: WHERE b.customer_id IS NULL albo NOT EXISTS.
--
-- 100.0, a nie 100 - calkowitoliczbowe dzielenie w Trino ucieloby czesc
-- ulamkowa i procent wyszedlby liczba calkowita.
--
-- Zbior "kupil" pochodzi z ZAMOWIEN, nie ze zdarzen purchase. Porownanie
-- zdarzen ze zdarzeniami dalo by wynik bliski zera, bo byloby porownaniem
-- zbioru prawie z samym soba - zdarzenie purchase generuje ten sam system,
-- ktory generuje add_to_cart. Wynik 0% jest wiec rozpoznawalnym objawem
-- wlasnie tej pomylki.
WITH cart AS (
  SELECT DISTINCT customer_id
  FROM aurora_jk.bronze_events
  WHERE event_type = 'add_to_cart'
    AND dt = '2026-01-16'
),
buy AS (
  SELECT DISTINCT o.customer_id
  FROM aurora_jk.silver_orders o
  WHERE o.dt = '2026-01-16'
)
SELECT
  COUNT(*) AS cart_customers,
  COUNT(*) - COUNT(b.customer_id) AS no_purchase,
  ROUND(100.0 * (COUNT(*) - COUNT(b.customer_id)) / COUNT(*), 1) AS pct
FROM cart c
LEFT JOIN buy b ON c.customer_id = b.customer_id;

-- =============================================================================
-- KONTROLA KONCOWA CALEGO KURSU
-- =============================================================================
-- Oczekiwane: styczen 1427124.00 (bez zmian od dnia 1), nowy dzien 101546.00,
-- calosc 1528670.00 na 449 wierszach.
SELECT
  dt,
  COUNT(*) AS rows_n,
  ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.v_fact_order_item_all
GROUP BY dt
ORDER BY dt;
