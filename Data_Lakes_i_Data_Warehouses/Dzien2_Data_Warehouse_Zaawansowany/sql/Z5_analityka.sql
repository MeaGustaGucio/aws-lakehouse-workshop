-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z5 - SQL analityczny na star schema
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
-- Do podmiany: aurora_jk -> aurora_<inicjaly>.
-- Wymaga tabel z sql/Z1_wymiar_daty.sql, sql/Z2_wymiary.sql i sql/Z3_fakty.sql.
--
-- Kazde zapytanie ma ORDER BY, wiec wynik jest deterministyczny i da sie
-- porownac linia po linii z liczbami w komentarzach.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Z5a. ROLLUP - sumy czesciowe w jednym przebiegu
-- -----------------------------------------------------------------------------
-- GROUP BY ROLLUP (a, b) = GROUP BY (a, b) + GROUP BY (a) + suma calkowita.
-- GROUPING(kolumna) zwraca 1 w wierszu, w ktorym ta kolumna zostala "zwinieta"
-- - dzieki temu da sie odroznic NULL-a znaczacego "razem" od prawdziwego NULL-a.
-- Agregat dw_fact_daily_sales wystarcza, bo revenue jest addytywne.
--
-- Oczekiwane: 9 wierszy (ORDER BY): 2 dni x 3 kanaly = 6, 2 sumy dni,
-- 1 suma calkowita:
--   2026-01-15 marketplace 233010 | mobile 226929 | web 193592 | razem 653531
--   2026-01-16 marketplace 288582 | mobile 225598 | web 259413 | razem 773593
--   razem/razem 1427124
SELECT
  CASE WHEN GROUPING(d.date_actual) = 1 THEN 'razem' ELSE CAST(d.date_actual AS varchar) END AS dzien,
  CASE WHEN GROUPING(s.channel)     = 1 THEN 'razem' ELSE s.channel END                     AS kanal,
  ROUND(SUM(s.revenue_pln), 2) AS revenue_pln
FROM aurora_jk.dw_fact_daily_sales s
JOIN aurora_jk.dw_dim_date d ON s.order_date_sk = d.date_sk
GROUP BY ROLLUP (d.date_actual, s.channel)
ORDER BY GROUPING(d.date_actual), d.date_actual, GROUPING(s.channel), s.channel;

-- -----------------------------------------------------------------------------
-- Z5b. GROUPING SETS - dwa niezalezne raporty z jednego skanu
-- -----------------------------------------------------------------------------
-- ROLLUP to hierarchia; GROUPING SETS to dowolny zbior kombinacji. Tu: przychod
-- per kategoria ORAZ per kanal - dwa raporty, ktore inaczej wymagalyby UNION ALL
-- dwoch zapytan (i dwoch skanow).
--
-- Oczekiwane: 9 wierszy (6 kategorii + 3 kanaly), ORDER BY wymiar, klucz:
--   kanal marketplace 521592 | mobile 452527 | web 453005 |
--   kategoria AGD 261065 | Akcesoria 13423 | Audio 83254 | Komputery 362605 |
--   TV 324800 | Telefony 381977
-- TV przed Telefony, bo Trino sortuje bajtowo: wielkie 'V' < male 'e'.
SELECT
  CASE WHEN GROUPING(category) = 0 THEN 'kategoria' ELSE 'kanal' END AS wymiar,
  COALESCE(category, channel) AS klucz,
  ROUND(SUM(revenue_pln), 2)  AS revenue_pln
FROM aurora_jk.dw_fact_daily_sales
GROUP BY GROUPING SETS ((category), (channel))
ORDER BY wymiar, klucz;

-- -----------------------------------------------------------------------------
-- Z5c. Funkcje okna - udzial i ranking wewnatrz grupy
-- -----------------------------------------------------------------------------
-- SUM() OVER (PARTITION BY ...) liczy sume grupy w kazdym wierszu bez GROUP BY
-- i bez podzapytania; RANK() OVER (... ORDER BY ...) numeruje wewnatrz grupy.
-- QUALIFY nie istnieje w Trino, stad zewnetrzny SELECT z WHERE rnk <= 2.
--
-- Oczekiwane: 12 wierszy - dwa najlepsze produkty w kazdej z 6 kategorii
-- (ORDER BY category, rnk). Pierwsze trzy linie:
--   AGD 1 Lodowka Aurora 300l 102743 39.36 | AGD 2 Pralka Bosch 8kg 67043 25.68 |
--   Akcesoria 1 Klawiatura mechaniczna Aurora 5589 41.64
-- Ostatnie dwie: Telefony 1 iPhone 15 136798 35.81 | Telefony 2 Samsung Galaxy
-- S24 117099 30.66.
SELECT category, rnk, product_name, revenue_pln, udzial_pct
FROM (
  SELECT
    p.category,
    p.product_name,
    ROUND(SUM(f.net_amount_pln), 2) AS revenue_pln,
    ROUND(100.0 * SUM(f.net_amount_pln)
          / SUM(SUM(f.net_amount_pln)) OVER (PARTITION BY p.category), 2) AS udzial_pct,
    RANK() OVER (PARTITION BY p.category ORDER BY SUM(f.net_amount_pln) DESC) AS rnk
  FROM aurora_jk.dw_fact_order_item f
  JOIN aurora_jk.dw_dim_product p ON f.product_sk = p.product_sk
  GROUP BY p.category, p.product_name
) x
WHERE rnk <= 2
ORDER BY category, rnk;

-- -----------------------------------------------------------------------------
-- Z5d. Kohorty - miesiac rejestracji vs zakupy
-- -----------------------------------------------------------------------------
-- signup_month jest atrybutem WYMIARU (policzonym raz w Z2a), wiec analiza
-- kohort to zwykly GROUP BY po wymiarze. Kohorta = klienci zarejestrowani
-- w tym samym miesiacu; pytanie: ilu z nich kupilo i za ile.
--
-- Oczekiwane: 13 wierszy (ORDER BY signup_month), pierwsze trzy:
--   2025-01 klienci 11 kupujacy 8 zamowienia 23 revenue 191673 |
--   2025-02 klienci 8 kupujacy 8 zamowienia 16 revenue 101329 |
--   2025-03 klienci 4 kupujacy 4 zamowienia 10 revenue 94909
-- Ostatni: 2026-01 klienci 5 kupujacy 4 zamowienia 14 revenue 99681.
-- Suma kolumny klienci = 80, suma zamowienia = 170, suma revenue = 1427124.
SELECT
  c.signup_month,
  COUNT(DISTINCT c.customer_sk)                        AS klienci,
  COUNT(DISTINCT f.customer_sk)                        AS kupujacy,
  COUNT(DISTINCT f.order_id)                           AS zamowienia,
  ROUND(COALESCE(SUM(f.net_amount_pln), 0), 2)         AS revenue_pln
FROM aurora_jk.dw_dim_customer c
LEFT JOIN aurora_jk.dw_fact_order_item f ON c.customer_sk = f.customer_sk
WHERE c.customer_sk <> -1
GROUP BY c.signup_month
ORDER BY c.signup_month;

-- -----------------------------------------------------------------------------
-- Z5e. AOV vs srednia pozycji - dwie "srednie", ktore nie sa tym samym
-- -----------------------------------------------------------------------------
-- AOV (average order value) = przychod / liczba ZAMOWIEN. AVG(net_amount_pln)
-- na fakcie o grain pozycji to srednia POZYCJI - inna liczba, choc kolumna
-- ta sama. Roznica bierze sie z grain i jest jednym z najczestszych bledow
-- w raportach budowanych na fakcie transakcyjnym.
--
-- Oczekiwane (1 wiersz): revenue 1427124.00 | zamowienia 170 | pozycje 409 |
-- aov 8394.85 | srednia_pozycji 3489.30
SELECT
  ROUND(SUM(net_amount_pln), 2)                              AS revenue_pln,
  COUNT(DISTINCT order_id)                                   AS zamowienia,
  COUNT(*)                                                   AS pozycje,
  ROUND(SUM(net_amount_pln) / COUNT(DISTINCT order_id), 2)   AS aov_pln,
  ROUND(AVG(net_amount_pln), 2)                              AS srednia_pozycji_pln
FROM aurora_jk.dw_fact_order_item;

-- -----------------------------------------------------------------------------
-- Z5f. [po 3 h] CUBE i suma biegnaca
-- -----------------------------------------------------------------------------
-- CUBE (a, b) daje WSZYSTKIE kombinacje: (a,b), (a), (b), (). Przy dwu
-- kolumnach o 2 i 3 wartosciach: 6 + 2 + 3 + 1 = 12 wierszy.
-- Oczekiwane: 12 wierszy; ostatni (razem, razem) = 1427124.
SELECT
  COALESCE(CAST(d.date_actual AS varchar), 'razem') AS dzien,
  COALESCE(s.channel, 'razem')                       AS kanal,
  ROUND(SUM(s.revenue_pln), 2)                       AS revenue_pln
FROM aurora_jk.dw_fact_daily_sales s
JOIN aurora_jk.dw_dim_date d ON s.order_date_sk = d.date_sk
GROUP BY CUBE (d.date_actual, s.channel)
ORDER BY GROUPING(d.date_actual), d.date_actual, GROUPING(s.channel), s.channel;

-- Suma biegnaca po dniach w kazdym kanale: ROWS UNBOUNDED PRECEDING.
-- Oczekiwane: 6 wierszy (ORDER BY kanal, dzien); marketplace 233010 -> 521592,
-- mobile 226929 -> 452527, web 193592 -> 453005.
SELECT
  s.channel AS kanal,
  d.date_actual AS dzien,
  ROUND(SUM(s.revenue_pln), 2) AS revenue_pln,
  ROUND(SUM(SUM(s.revenue_pln)) OVER (PARTITION BY s.channel ORDER BY d.date_actual
        ROWS UNBOUNDED PRECEDING), 2) AS revenue_narastajaco
FROM aurora_jk.dw_fact_daily_sales s
JOIN aurora_jk.dw_dim_date d ON s.order_date_sk = d.date_sk
GROUP BY s.channel, d.date_actual
ORDER BY kanal, dzien;
