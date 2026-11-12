-- =============================================================================
-- Dzien 2, W4 - pytania biznesowe na star schema
-- =============================================================================
-- SILNIK: AMAZON ATHENA (Query editor w konsoli Athena). NIE Redshift.
--
-- NAZWY: dwuczlonowo, `aurora_<inicjaly>.gold_fact_order_item` - baza w Glue
-- Data Catalog plus nazwa tabeli. Athena rozmawia z katalogiem Glue wprost,
-- wiec trzeciego czlonu nie ma, a slowo awsdatacatalog w skladni Atheny
-- w ogole nie wystepuje.
--
-- Ten plik wklejony do Query Editor v2 Redshifta konczy sie bledem
--   ERROR: schema "aurora_jk" does not exist
-- i nie jest to literowka ani brak tabeli. Redshift szuka wtedy LOKALNEGO
-- schematu o tej nazwie w bazie `dev` - a baza Glue lokalnym schematem nie
-- jest. Redshiftowe odpowiedniki tych czterech pytan, razem z opisem trzech
-- form nazw, sa w 03_redshift.sql (W5).
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>.
-- Wylacznie warstwa Gold - bez wracania do Bronze i Silver.
--
-- Wspolny mianownik wszystkich czterech zapytan: fakt trzyma same klucze
-- i liczby, wiec kazdy atrybut opisowy (kategoria, miasto, kanal, data) wchodzi
-- do zapytania przez zlaczenie z wymiarem. Na tym polega star schema i to jest
-- widoczne w kazdym z ponizszych SELECT-ow.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Przychod netto wg kategorii i dnia
-- -----------------------------------------------------------------------------
-- Oczekiwane 12 wierszy: 6 kategorii x 2 dni.
--
-- 2026-01-15:
--   Telefony 199197.00 | Komputery 152821.00 | TV 142358.00
--   AGD 117153.00 | Audio 36870.00 | Akcesoria 5132.00     -> suma 653531.00
-- 2026-01-16:
--   Komputery 209784.00 | Telefony 182780.00 | TV 182442.00
--   AGD 143912.00 | Audio 46384.00 | Akcesoria 8291.00     -> suma 773593.00
-- Razem: 1427124.00 - ta sama kwota, ktora wyszla w dniu 1 na Bronze (L4 pkt 2)
-- i na Silver (L6). Trzecia niezalezna droga do tej samej liczby.
--
-- Ranking zmienia sie miedzy dniami: 15.01 prowadza Telefony, 16.01 Komputery.
-- Raport z jednego dnia zbudowalby falszywy wniosek o tym, ktora kategoria jest
-- najwazniejsza.
--
-- Dwa zlaczenia. Na znormalizowanym modelu zrodlowym (klient -> zamowienie ->
-- pozycja -> produkt -> kategoria) trzeba by ich napisac 6-8. To nie argument
-- o wydajnosci - na 409 wierszach jest ona nieodroznialna - a o liczbie miejsc,
-- w ktorych da sie pomylic kierunek zlaczenia albo zdublowac wiersze.
SELECT
  d.date_actual,
  p.category,
  ROUND(SUM(f.net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.gold_fact_order_item f
JOIN aurora_jk.gold_dim_date d    ON f.date_sk    = d.date_sk
JOIN aurora_jk.gold_dim_product p ON f.product_sk = p.product_sk
GROUP BY d.date_actual, p.category
ORDER BY d.date_actual, revenue_pln DESC;

-- -----------------------------------------------------------------------------
-- 2. Piec miast o najwyzszym przychodzie w kategorii AGD
-- -----------------------------------------------------------------------------
-- Oczekiwane:
--   Gdansk 47052.00 | Lublin 39884.00 | Warszawa 35859.00
--   Krakow 32694.00 | Lodz 23097.00
--
-- Filtr po kategorii dziala na WYMIARZE produktu, a grupowanie na WYMIARZE
-- klienta. Fakt sam z siebie nie wie ani o kategoriach, ani o miastach - trzyma
-- tylko klucze. Cala mechanika star schema jest widoczna w tym jednym zapytaniu.
--
-- Miasto pochodzi z dim_customer, wiec jest to miasto KLIENTA, nie miasto
-- dostawy (delivery_city na zamowieniu). Przy dwoch roznych miastach w danych
-- to rozroznienie ma znaczenie i wraca w dniu 3 jako pytanie "as-was czy as-is".
--
-- WHERE, nie HAVING: filtr dziala na wierszach przed agregacja. HAVING
-- na p.category bylby bledem skladniowym, bo kolumna nie wchodzi do GROUP BY.
SELECT
  c.city,
  ROUND(SUM(f.net_amount_pln), 2) AS agd_revenue_pln
FROM aurora_jk.gold_fact_order_item f
JOIN aurora_jk.gold_dim_customer c ON f.customer_sk = c.customer_sk
JOIN aurora_jk.gold_dim_product p  ON f.product_sk  = p.product_sk
WHERE p.category = 'AGD'
GROUP BY c.city
ORDER BY agd_revenue_pln DESC
LIMIT 5;

-- -----------------------------------------------------------------------------
-- 3. AOV (srednia wartosc zamowienia) wg kanalu
-- -----------------------------------------------------------------------------
-- Oczekiwane:
--   mobile      9235.24  (49 zamowien)
--   web         8236.45  (55 zamowien)
--   marketplace 7902.91  (66 zamowien)
-- Suma orders_n = 49 + 55 + 66 = 170, czyli tyle, ile silver_orders. Gdyby
-- wyszlo wiecej, znaczyloby to, ze jakies zamowienie ma pozycje przypisane
-- do dwoch kanalow.
--
-- COUNT(DISTINCT order_id), NIE COUNT(*). Grain faktu to POZYCJA zamowienia,
-- wiec jedno zamowienie zajmuje kilka wierszy - srednio 2.4, bo 409 pozycji
-- przypada na 170 zamowien. COUNT(*) policzylby pozycje i podzielil przychod
-- przez zawyzony mianownik: AOV wyszloby okolo dwuipolkrotnie nizsze
-- (3-4 tysiace zamiast 8-9) i wygladaloby zupelnie wiarygodnie. To najgorszy
-- rodzaj bledu w data warehouse - taki, ktory daje liczbe mieszczaca sie
-- w oczekiwaniach.
--
-- AOV jest miara NIEADDYTYWNA, bo jest ilorazem. Sredniej z kanalow nie wolno
-- usrednic ponownie:
--   srednia arytmetyczna z trzech AOV = 8458.20
--   AOV calosci = 1427124.00 / 170     = 8394.85
-- Roznica bierze sie z tego, ze kanaly maja rozna liczbe zamowien, a srednia
-- arytmetyczna traktuje je jak rowne. Dlatego na faktcie trzymamy
-- net_amount_pln i order_id, a nie policzone z gory AOV.
--
-- Wysokosc AOV (8-9 tysiecy zlotych) jest poprawna: Aurora jest sklepem
-- z elektronika i AGD, nie spozywczym.
SELECT
  ch.channel_code,
  ROUND(SUM(f.net_amount_pln) / COUNT(DISTINCT f.order_id), 2) AS aov_pln,
  COUNT(DISTINCT f.order_id) AS orders_n
FROM aurora_jk.gold_fact_order_item f
JOIN aurora_jk.gold_dim_channel ch ON f.channel_sk = ch.channel_sk
GROUP BY ch.channel_code
ORDER BY aov_pln DESC;

-- -----------------------------------------------------------------------------
-- 4. Rekoncyliacja Silver = Gold
-- -----------------------------------------------------------------------------
-- Oczekiwane: 1427124.00 w OBU wierszach, co do grosza.
--
-- Po co ten test istnieje: odpowiada na pytanie, na ktore nikt inny nie
-- odpowie - czy przy przekladaniu danych do nowego ukladu nic nie zginelo
-- i nic sie nie zdublowalo. Silnik tego nie sprawdzi, bo w data warehouse nie
-- ma egzekwowanych kluczy obcych (PRIMARY KEY w Redshifcie jest informacyjny,
-- Athena nie zna FOREIGN KEY w ogole). W produkcji rekoncyliacja uruchamiana
-- jest po kazdym przeladowaniu, a jej niepowodzenie wstrzymuje publikacje
-- raportow.
--
-- Kierunek rozjazdu wskazuje przyczyne:
--
-- Gold MNIEJSZY od Silver -> ktorys INNER JOIN nie trafil i po cichu usunal
--   wiersze. Najczestsza przyczyna: wymiar zbudowany z wezszego zrodla niz fakt
--   (np. gold_dim_date z silver_orders zamiast z silver_order_items) albo NULL
--   w kolumnie uzytej w BETWEEN. Diagnostyka: nastepne zapytanie.
--
-- Gold WIEKSZY od Silver -> ktorys wymiar ma duplikat natural key, wiec
--   zlaczenie zwielokrotnilo wiersze faktu. Zwykle brak DISTINCT w podzapytaniu
--   budujacym wymiar. Diagnostyka: przedostatnie zapytanie w tym pliku.
SELECT 'silver' AS layer, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
UNION ALL
SELECT 'gold', ROUND(SUM(net_amount_pln), 2)
FROM aurora_jk.gold_fact_order_item
ORDER BY layer;

-- Diagnostyka zgubionych wierszy. Kazda kolumna powinna dac 0; wartosc rozna
-- od zera wskazuje wymiar, w ktorym brakuje wierszy.
SELECT
  COUNT(*)                        AS wszystkie,
  COUNT(*) - COUNT(d.date_sk)     AS bez_daty,
  COUNT(*) - COUNT(c.customer_sk) AS bez_klienta,
  COUNT(*) - COUNT(p.product_sk)  AS bez_produktu
FROM aurora_jk.silver_order_items i
LEFT JOIN aurora_jk.gold_dim_date d     ON i.order_date  = d.date_actual
LEFT JOIN aurora_jk.gold_dim_customer c ON i.customer_id = c.customer_id
LEFT JOIN aurora_jk.gold_dim_product p  ON i.product_id  = p.product_id;

-- Diagnostyka duplikatow w wymiarze. Oczekiwane: 0 wierszy w wyniku.
-- Analogicznie dla product_id w gold_dim_product i date_actual w gold_dim_date.
-- Od dnia 3, po SCD Type 2, ten test zmienia sie w "duplikat w obrebie
-- is_current = true" - bo dwie wersje tego samego klienta beda wtedy poprawne.
SELECT customer_id, COUNT(*) AS wersje
FROM aurora_jk.gold_dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Kontrola koncowa: liczba wierszy faktu = liczba wierszy silver_order_items.
-- Oczekiwane: 409 = 409. Gold niczego nie filtruje, wiec kazda inna liczba jest
-- bledem, nawet gdy "wyglada rozsadnie".
SELECT
  (SELECT COUNT(*) FROM aurora_jk.silver_order_items)   AS silver_rows,
  (SELECT COUNT(*) FROM aurora_jk.gold_fact_order_item) AS gold_rows;
