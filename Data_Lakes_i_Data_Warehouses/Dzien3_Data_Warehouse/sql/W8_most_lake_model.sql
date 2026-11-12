-- =============================================================================
-- Dzien 3, W8 - most miedzy data lake a modelem wymiarowym
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
-- Wariant Redshiftowy W8 jest opisany w komentarzu ponizej i uzywa nazw
-- trzyczlonowych - patrz naglowek ../../Dzien2_Data_Warehouse/sql/W5_redshift.sql.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>.
-- Ten plik nic nie tworzy - same zapytania. Wymaga gold_dim_customer_scd z W6
-- (sql/W6_scd2_customer.sql) i faktu z W3.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- W8. Ktora kategoria ma duzo koszykow, a malo przychodu
-- -----------------------------------------------------------------------------
-- Jedno zapytanie laczy dwa swiaty:
--   bronze_events        - JSON Lines, polstrukturalny, schemat nalozony
--                          przy ODCZYCIE (schema-on-read)
--   gold_fact_order_item - Parquet, kolumnowy, schemat ustalony przy ZAPISIE
-- Jeden silnik, jeden dialekt SQL, zero kopiowania danych miedzy systemami.
-- To jest idea lakehouse pokazana w jednym zapytaniu.
--
-- Pytanie o slaba konwersje nie da sie odpowiedziec z samego modelu - model wie
-- tylko o tym, co zostalo kupione.
--
-- LEFT JOIN, NIE INNER JOIN. Po stronie zdarzen sa produkty, ktorych nikt nie
-- kupil. Przy INNER JOIN znikneliby razem z informacja o porzuconych koszykach -
-- a to wlasnie oni sa odpowiedzia na zadane pytanie. INNER JOIN dalby tu wynik,
-- ktory wyglada poprawnie i po cichu pomija najciekawsze wiersze.
--
-- Drugi LEFT JOIN, do silver_products, dostarcza kategorie dla produktow
-- nieobecnych w faktcie. Bez niego takie wiersze mialyby category = NULL
-- i wpadlyby do osobnej grupy w GROUP BY.
--
-- COALESCE(r.revenue_pln, 0) zamienia brak sprzedazy na zero. Bez tego SUM
-- pomijalby NULL-e i kategoria bez ani jednego zakupu pokazalaby pusty przychod
-- zamiast zera, czyli brak danych zamiast informacji.
--
-- Oczekiwane (sortowanie malejaco po koszykach):
--   Akcesoria  39 koszykow     13423.00
--   AGD        38 koszykow    261065.00
--   Telefony   30 koszykow    381977.00
--   Audio      28 koszykow     83254.00
--   Komputery  25 koszykow    362605.00
--   TV         24 koszykow    324800.00
--
-- Dwie sumy kontrolne: kolumna z koszykami sumuje sie do 184 (tyle jest zdarzen
-- add_to_cart w bronze_events), a kolumna z przychodem do 1427124.00. Rozjazd
-- ktorejkolwiek oznacza, ze ktorys JOIN zdublowal albo zgubil wiersze.
--
-- ODPOWIEDZ BIZNESOWA: Akcesoria. Najwiecej koszykow w calym zestawieniu
-- i przychod o rzad wielkosci nizszy niz Telefony, ktore maja koszykow o dziewiec
-- mniej. Ktos dorzuca etui za 49 zl i nie finalizuje zamowienia.
-- Odpowiedz nie jest jednoznaczna: to moze byc problem konwersji, ale rownie
-- dobrze naturalna wlasciwosc taniego koszyka. Rozstrzygniecie wymagaloby
-- danych, ktorych nie mamy - liczby wyswietlen i wartosci koszyka w momencie
-- porzucenia.

WITH carts AS (
  SELECT e.product_id, COUNT(*) AS cart_events
  FROM aurora_jk.bronze_events e
  WHERE e.event_type = 'add_to_cart'
    AND e.product_id IS NOT NULL
  GROUP BY e.product_id
),
rev AS (
  SELECT p.product_id, p.category, SUM(f.net_amount_pln) AS revenue_pln
  FROM aurora_jk.gold_fact_order_item f
  JOIN aurora_jk.gold_dim_product p ON f.product_sk = p.product_sk
  GROUP BY p.product_id, p.category
)
SELECT
  COALESCE(r.category, p.category) AS category,
  SUM(c.cart_events) AS cart_events,
  ROUND(SUM(COALESCE(r.revenue_pln, 0)), 2) AS revenue_pln
FROM carts c
LEFT JOIN rev r                        ON c.product_id = r.product_id
LEFT JOIN aurora_jk.silver_products p  ON c.product_id = p.product_id
GROUP BY 1
ORDER BY cart_events DESC;

-- Wariant dla Redshifta: fakt lokalnie w schemacie gold_<inicjaly>, zdarzenia
-- z data lake. Dane zdarzen zostaja na S3, a Redshift doklada je do zapytania
-- w locie. Sa na to dwie drogi i roznia sie uprawnieniami, nie skladnia SQL:
--
--   a) zamontowany katalog Glue - bez zadnego DDL, wystarczy zapis trzyczlonowy
--        FROM "awsdatacatalog"."aurora_jk"."bronze_events" e
--      Czyta S3 tozsamoscia zalogowanego uzytkownika, wiec dziala bezposrednio
--      z roli PowerUserAccess. To jest droga W5A z dnia 2.
--
--   b) jawny CREATE EXTERNAL SCHEMA ... FROM DATA CATALOG ... IAM_ROLE ...
--      (klasyczny Redshift Spectrum). Czyta S3 rola IAM namespace'u, wiec bez
--      dodatkowych uprawnien S3 podlega temu samemu ograniczeniu co COPY w W5.
--
-- Nazwy tabel lokalnych i katalogowych mieszaja sie w jednym zapytaniu bez
-- problemu - to jest dokladnie ten sam lakehouse, tylko z drugiej strony.

