-- =============================================================================
-- Dzien 3, W7 - ladowanie przyrostowe dnia 2026-01-20 (Athena)
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela. Gdyby
-- ktores z tych zapytan mialo pojsc z Query Editora v2 Redshifta, nazwy
-- zmieniaja sie na trzyczlonowe ("awsdatacatalog"."aurora_<inicjaly>"."tabela")
-- - patrz naglowek ../../Dzien2_Data_Warehouse/sql/W5_redshift.sql.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Opis krok po kroku: ../02_rozwiazania.md
--
-- CEL: dolozyc nowy dzien BEZ przeliczania 15 i 16 stycznia, w sposob odporny
-- na powtorne uruchomienie. Wymaganie, ktore o tym rozstrzyga, to IDEMPOTENCJA:
-- ten sam skrypt uruchomiony dwa razy ma dac ten sam stan, a nie podwojone dane.
-- Ponowienia po awarii sa norma, nie wyjatkiem.
--
-- KOLEJNOSC: Bronze -> wymiar daty -> Silver -> fakt -> widok calosci.
-- Wymiar daty musi powstac PRZED faktem, bo fakt sie z nim laczy.
--
-- IF NOT EXISTS vs realne przeliczenie. Wszystkie CTAS-y maja IF NOT EXISTS,
-- wiec caly plik da sie puscic ponownie bez bledu - ale przy drugim
-- uruchomieniu instrukcje sa po prostu POMIJANE. Zeby realnie przeliczyc dzien,
-- potrzebne sa trzy rzeczy po kolei:
--   1. DROP TABLE aurora_jk.<tabela>;
--   2. skasowanie prefiksu w S3 (konsola S3, przycisk Delete na prefiksie)
--   3. ponowny CTAS
-- Krok 2 jest tym, o ktorym sie zapomina. Athena z zalozenia nigdy nie kasuje
-- danych z S3, wiec po samym DROP ponowny CTAS konczy sie
-- HIVE_PATH_ALREADY_EXISTS, a IF NOT EXISTS przed tym NIE chroni - tabeli
-- w katalogu juz nie ma. Wariant Iceberg na koncu pliku nie ma tego ograniczenia.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Krok 1. Bronze - zamowienia jako nowa partycja
-- -----------------------------------------------------------------------------
-- Pliki leza juz na S3 pod bronze/orders/dt=2026-01-20/, ale katalog Glue nie
-- przeszukuje S3 samoczynnie. Bez MSCK REPAIR zapytanie z WHERE dt = '2026-01-20'
-- zwroci 0 wierszy przy poprawnie wgranych plikach.
--
-- Wariant punktowy, szybszy przy tysiacach partycji:
--   ALTER TABLE aurora_jk.bronze_orders
--   ADD IF NOT EXISTS PARTITION (dt = '2026-01-20')
--   LOCATION 's3://aurora-jk-lake/bronze/orders/dt=2026-01-20/';
MSCK REPAIR TABLE aurora_jk.bronze_orders;

-- Oczekiwane: trzy partycje - 2026-01-15 (90), 2026-01-16 (110), 2026-01-20 (40).
-- Razem 240 wierszy.
SELECT dt, COUNT(*) AS n
FROM aurora_jk.bronze_orders
GROUP BY dt
ORDER BY dt;

-- -----------------------------------------------------------------------------
-- Krok 2. Bronze - pozycje nowego dnia jako OSOBNA tabela
-- -----------------------------------------------------------------------------
-- Zamowienia weszly jako partycja, bo bronze_orders JEST partycjonowane po dt.
-- Pozycje wchodza jako osobna tabela, bo bronze_order_items partycjonowane
-- NIE JEST - jego LOCATION to bronze/order_items/ i Athena czyta wszystko,
-- co w tym katalogu lezy.
--
-- Gdyby pliki nowego dnia trafily do bronze/order_items/, tabela
-- bronze_order_items zaczelaby zwracac 526 wierszy zamiast 486, a razem z ta
-- liczba przestalyby sie zgadzac wszystkie liczby kontrolne dnia 1: 409 wierszy
-- Silver, 409 wierszy faktu i przychod 1427124.00. Nic by przy tym nie zglosilo
-- bledu - po prostu wszystkie wczesniejsze wyniki stalyby sie nieodtwarzalne.
--
-- W produkcji rozwiazaloby to partycjonowanie order_items po dt juz od
-- pierwszego dnia. Tutaj kosztem jednej brzydkiej nazwy tabeli zachowujemy
-- mozliwosc powrotu do stanu z dnia 1.
--
-- Struktura kolumn jest identyczna jak w bronze_order_items - ta sama kolejnosc
-- i te same typy, bo plik ma ten sam naglowek.
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

-- Oczekiwane: nowe = 40, stare = 486 (bez zmian).
SELECT
  (SELECT COUNT(*) FROM aurora_jk.bronze_order_items_20260120) AS items_20,
  (SELECT COUNT(*) FROM aurora_jk.bronze_order_items)          AS items_dzien1;

-- -----------------------------------------------------------------------------
-- Krok 3. Wymiar daty z trzecim dniem
-- -----------------------------------------------------------------------------
-- gold_dim_date zna tylko 15 i 16 stycznia. Fakt nowego dnia laczy sie
-- z wymiarem daty po order_date = date_actual, wiec bez wiersza dla 20 stycznia
-- INNER JOIN wytnie CALY nowy dzien - po cichu, bez bledu, z wynikiem zero
-- wierszy zamiast czterdziestu. To najczestsza wpadka przy pierwszym ladowaniu
-- przyrostowym, a objaw jest mylacy: tabela powstaje poprawnie, tylko jest pusta.
--
-- Tabela zewnetrzna nie przyjmuje INSERT-a (na S3 mozna dopisac plik, nie da sie
-- zmienic tabeli), wiec powstaje NOWA WERSJA wymiaru, a stara zostaje.
-- To niezgrabne i jest tego swiadomym kosztem: w produkcji wymiar daty generuje
-- sie raz, na kilka lat w przod, wlasnie po to, zeby nigdy go nie dokladac.
--
-- CAST('Tuesday' AS varchar) nie jest ozdobnikiem. Literal tekstowy w Trino ma
-- typ char(7), a day_name w gold_dim_date pochodzi z date_format() i jest
-- varchar. Bez rzutowania UNION ALL przerywa na niezgodnosci typow kolumny.
--
-- 2026-01-20 to wtorek, wiec is_weekend = false. date_sk w konwencji RRRRMMDD,
-- tak samo jak w dniu 2.
CREATE TABLE IF NOT EXISTS aurora_jk.gold_dim_date_with_20
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/gold/dim_date_with_20/'
) AS
SELECT * FROM aurora_jk.gold_dim_date
UNION ALL
SELECT
  20260120 AS date_sk,
  DATE '2026-01-20' AS date_actual,
  2026 AS year,
  1 AS month,
  20 AS day,
  CAST('Tuesday' AS varchar) AS day_name,
  false AS is_weekend;

-- Oczekiwane: 3 wiersze, date_sk = 20260115, 20260116, 20260120.
SELECT date_sk, date_actual, day_name, is_weekend
FROM aurora_jk.gold_dim_date_with_20
ORDER BY date_sk;

-- -----------------------------------------------------------------------------
-- Krok 4. Silver - tylko nowy dzien
-- -----------------------------------------------------------------------------
-- Filtr dt = '2026-01-20' robi dwie rzeczy naraz: wyznacza zakres przeliczenia
-- i pozwala Athenie w ogole nie otwierac katalogow z 15 i 16 stycznia
-- (partition pruning). Dni 15 i 16 nie sa ruszane - to jest cala idea
-- ladowania przyrostowego.
--
-- Rzutowania typow sa te same co w L5 dnia 1: order_ts na timestamp, pierwsze
-- 10 znakow na date. Bronze trzyma je jako string, bo OpenCSVSerde czyta
-- wszystko jako tekst.
--
-- Oczekiwane: 40 wierszy. Wszystkie zamowienia z 20 stycznia maja status paid
-- albo shipped, wiec filtr statusu tym razem niczego nie odrzuca - inaczej niz
-- w dniu 1, gdzie 30 zamowien bylo anulowanych i z 486 pozycji zostalo 409.
CREATE TABLE IF NOT EXISTS aurora_jk.silver_orders_20260120
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/silver/orders_20260120/'
) AS
SELECT
  order_id,
  customer_id,
  CAST(order_ts AS timestamp) AS order_ts,
  CAST(substr(order_ts, 1, 10) AS date) AS order_date,
  status,
  channel,
  delivery_city,
  dt
FROM aurora_jk.bronze_orders
WHERE dt = '2026-01-20'
  AND status IN ('paid', 'shipped');

-- -----------------------------------------------------------------------------
-- Krok 5. Fakt nowego dnia
-- -----------------------------------------------------------------------------
-- Piec zlaczen, z czego dwa wygladaja inaczej niz w dniu 2.
--
-- 1) gold_dim_date_with_20, NIE gold_dim_date. Ze starym wymiarem daty wynik
--    to 0 wierszy - INNER JOIN nie znajduje 20 stycznia i wycina wszystko.
--
-- 2) gold_dim_customer_scd po DWOCH warunkach naraz: po customer_id ORAZ po
--    przedziale waznosci wersji. NIE po is_current. Dla 20 stycznia obie drogi
--    daja ten sam wynik, bo nowe wersje sa zarazem aktualne - ale tylko pierwsza
--    zadziala przy przeliczaniu starszego dnia. Zlaczenie po is_current
--    przypisaloby styczniowe zamowienia do dzisiejszych miast, czyli zrobiloby
--    dokladnie to, przed czym SCD2 chroni.
--
--    Tu wraca decyzja z dnia 2: valid_from = DATE '1900-01-01', a nie NULL.
--    BETWEEN z NULL-em daje NULL zamiast TRUE i nie dopasowuje niczego -
--    72 klientow bez zmian wypadloby z faktu po cichu. Fakt krotszy niz
--    40 wierszy to pierwsze miejsce do sprawdzenia.
--
-- 3) silver_orders_20260120 w srodku zlaczenia. Pozycje nie maja ani daty, ani
--    kanalu, ani klienta - wszystko to jest atrybutem ZAMOWIENIA. To ten sam
--    przeskok przez warstwe Silver, ktory w dniu 2 byl potrzebny do channel_code.
--
-- KOLEJNOSC KOLUMN jest identyczna jak w gold_fact_order_item z dnia 2 - inaczej
-- widok sklejajacy obie tabele przez UNION ALL albo sie nie utworzy, albo,
-- gorzej, utworzy sie z pomieszanymi kolumnami tego samego typu.
--
-- Tabela nie jest partycjonowana. Przy jednym dniu partycja nic nie daje,
-- a dt zostaje zwykla kolumna w pliku Parquet - dzieki czemu UNION ALL z tabela
-- partycjonowana nadal zgadza sie co do liczby kolumn.
CREATE TABLE IF NOT EXISTS aurora_jk.gold_fact_order_item_20260120
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/gold/fact_order_item_20260120/'
) AS
SELECT
  i.order_id,
  d.date_sk,
  c.customer_sk,
  p.product_sk,
  ch.channel_sk,
  i.qty,
  i.unit_price_pln,
  i.discount_pln,
  (i.qty * i.unit_price_pln - i.discount_pln) AS net_amount_pln,
  o.dt
FROM aurora_jk.bronze_order_items_20260120 i
JOIN aurora_jk.silver_orders_20260120 o
  ON i.order_id = o.order_id
JOIN aurora_jk.gold_dim_date_with_20 d
  ON o.order_date = d.date_actual
JOIN aurora_jk.gold_dim_customer_scd c
  ON o.customer_id = c.customer_id
 AND o.order_date BETWEEN c.valid_from AND c.valid_to
JOIN aurora_jk.gold_dim_product p
  ON i.product_id = p.product_id
JOIN aurora_jk.gold_dim_channel ch
  ON o.channel = ch.channel_code;

-- -----------------------------------------------------------------------------
-- Krok 6. Widok calosci
-- -----------------------------------------------------------------------------
-- Widok w Athenie to zapisane zapytanie, nie dane - nic nie kosztuje w storage,
-- a CREATE OR REPLACE mozna wykonywac wielokrotnie, wiec ten fragment jest
-- idempotentny sam z siebie.
--
-- UNION ALL skleja stary fakt (409 pozycji z 15-16.01) z nowym dniem (40 pozycji),
-- bo w tej wersji nie nadpisujemy gold_fact_order_item. UNION (bez ALL) usuwalby
-- duplikaty i sortowal - tu duplikatow nie ma, a koszt sortowania nic nie daje.
--
-- Kolejnosc kolumn w obu tabelach musi byc identyczna. W gold_fact_order_item
-- dt jest ostatnie, bo to kolumna partycjonujaca; tutaj jest ostatnie, bo tak
-- zostalo wypisane w SELECT wyzej.
CREATE OR REPLACE VIEW aurora_jk.v_fact_order_item_all AS
SELECT * FROM aurora_jk.gold_fact_order_item
UNION ALL
SELECT * FROM aurora_jk.gold_fact_order_item_20260120;

-- =============================================================================
-- KONTROLA
-- =============================================================================

-- Oczekiwane: 2026-01-15 i 2026-01-16 bez zmian, 2026-01-20 = 40.
-- Razem 449 wierszy (409 + 40).
SELECT dt, COUNT(*) AS rows_n
FROM aurora_jk.v_fact_order_item_all
GROUP BY dt
ORDER BY dt;

-- Nowy dzien. Oczekiwane: 40 wierszy, przychod 101546.00.
-- Czterdziesci pozycji na czterdziesci zamowien - nowy dzien ma dokladnie jedna
-- pozycje na zamowienie, wiec grain faktu jest tu tozsamy z grain zamowienia.
--
-- MNIEJ niz 40 -> brak 2026-01-20 w wymiarze daty albo NULL w valid_from
--   lub valid_to wymiaru klienta.
-- WIECEJ niz 40 -> okresy waznosci w wymiarze SCD2 zachodza na siebie
--   (weryfikacja 3 z W6_scd2_customer.sql zwrocilaby wtedy niepusta liste).
SELECT COUNT(*) AS rows_n, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.gold_fact_order_item_20260120;

-- Stary fakt jest NIETKNIETY. Oczekiwane: 409 wierszy i 1427124.00 przychodu -
-- te same liczby co po dniu 2. Kazda inna wartosc oznacza, ze ladowanie
-- przyrostowe dotknelo danych, ktorych dotykac nie mialo.
SELECT COUNT(*) AS rows_n, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.gold_fact_order_item;

-- =============================================================================
-- WARIANT ICEBERG - ta sama operacja, bez sprzatania recznego
-- =============================================================================
-- Wymaga tabeli silver_orders_iceberg z zadania dodatkowego S1 dnia 1
-- (w Athenie engine v3 przechodzi bez problemow).
--
-- JAK CZYTAC MERGE. Instrukcja przechodzi po wierszach zrodla (USING) i dla
-- kazdego sprawdza warunek ON:
--   * wiersz PASUJE do istniejacego w tabeli docelowej -> galaz WHEN MATCHED,
--     czyli UPDATE wymienionych kolumn
--   * wiersz NIE PASUJE -> galaz WHEN NOT MATCHED, czyli INSERT
--
-- Pierwsze uruchomienie: 170 + 40 = 210, bo zadne order_id z 20 stycznia nie
-- istnieje jeszcze w tabeli docelowej. Drugie uruchomienie TEJ SAMEJ instrukcji:
-- nadal 210, bo teraz wszystkie czterdziesci wierszy trafia w WHEN MATCHED
-- i tylko nadpisuje status oraz channel tymi samymi wartosciami.
--
-- To jest cala roznica miedzy katalogiem plikow na S3 a formatem tabelarycznym.
-- Powtorzenie CTAS-a wyzej wymagalo DROP TABLE i wejscia do konsoli S3;
-- powtorzenie MERGE wymaga nacisniecia Run jeszcze raz.
--
-- SKROT "WHEN NOT MATCHED THEN INSERT *" W ATHENIE NIE DZIALA. Ta skladnia
-- pochodzi z Delta Lake i Sparka; Athena engine v3 odpowiada:
--   mismatched input '*'. Expecting: '(', 'VALUES'
-- Kolumny trzeba wypisac w obu listach - i w INSERT, i w VALUES.
-- Sprawdzone w Athenie engine v3.

MERGE INTO aurora_jk.silver_orders_iceberg t
USING aurora_jk.silver_orders_20260120 s
  ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  status  = s.status,
  channel = s.channel
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, order_ts, order_date,
                              status, channel, delivery_city, dt)
     VALUES (s.order_id, s.customer_id, s.order_ts, s.order_date,
             s.status, s.channel, s.delivery_city, s.dt);

-- Oczekiwane: 210 (170 z dnia 1 + 40 nowych), i 210 takze po drugim
-- uruchomieniu tego samego MERGE.
SELECT COUNT(*) AS rows_n FROM aurora_jk.silver_orders_iceberg;

-- Historia zmian. Oczekiwane po MERGE: dwa snapshoty - 'append' z CTAS-a
-- w dniu 1 i 'overwrite' z tego MERGE. Stary snapshot zostaje dostepny
-- i to jest caly mechanizm time travel: Iceberg nie nadpisuje plikow, tylko
-- zapisuje nowe i przestawia wskaznik.
SELECT committed_at, operation
FROM "aurora_jk"."silver_orders_iceberg$snapshots"
ORDER BY committed_at;
