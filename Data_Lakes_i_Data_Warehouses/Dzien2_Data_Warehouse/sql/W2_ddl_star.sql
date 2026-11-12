-- =============================================================================
-- Dzien 2, W2 - szkielet modelu wymiarowego (star schema)
-- =============================================================================
-- SILNIK: ZADEN. Ten plik nie jest skryptem do uruchomienia - to dokumentacja
-- modelu, zapisana w konwencji typow Hive/Athena. Nazwy tabel sa tu podane bez
-- zadnego prefiksu bazy wlasnie dlatego, ze model jest niezalezny od silnika;
-- kazdy z silnikow adresuje te same tabele inaczej:
--   Athena             aurora_<inicjaly>.gold_dim_date
--   Redshift, W5A      "awsdatacatalog"."aurora_<inicjaly>"."gold_dim_date"
--   Redshift, W5B      gold_<inicjaly>.dim_date
-- Pelne wyjasnienie tych trzech form jest w naglowku W5_redshift.sql.
--
-- Dlaczego nie da sie go uruchomic w Athenie:
--   Athena nie zna "golego" CREATE TABLE z sama lista kolumn. Tabela musi albo
--   wskazywac istniejace pliki (CREATE EXTERNAL TABLE ... LOCATION), albo
--   powstac razem z danymi (CREATE TABLE ... AS SELECT). Powod jest glebszy niz
--   skladnia: Athena nie ma wlasnego magazynu danych, wiec pusta tabela nie
--   mialaby gdzie istniec. W W3 uzywamy drugiej drogi, czyli CTAS.
--
-- Wersja tego samego modelu, ktora DA sie uruchomic, to W5_redshift.sql (W5).
-- Redshift ma wlasny magazyn, wiec pusta tabela jest tam sensownym bytem,
-- a dane dokladamy do niej pozniej instrukcja COPY.
--
-- Typy ponizej sa zapisane w konwencji Hive/Athena (string, double) - czyta sie
-- to jak projekt kolumn. Odpowiedniki Redshiftowe (varchar(n),
-- double precision) sa w W5_redshift.sql.
--
-- Wzorce dzialajacej skladni: ../../00_Sciagawka_SQL.md
--
-- W ramach W2 do uzupelnienia zostaja: grain tabeli faktow, atrybuty wymiarow
-- i lista miar. Ponizsza wersja jest punktem odniesienia po dyskusji.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Wymiar daty
-- -----------------------------------------------------------------------------
-- Jeden wiersz na dzien, takze na dzien bez sprzedazy. Powod: gdyby dzien bez
-- zamowien nie mial wiersza, raport "sprzedaz dzien po dniu" po cichu pominalby
-- ten dzien, zamiast pokazac zero. Dziura w wykresie i zero na wykresie to dwie
-- rozne informacje biznesowe.
--
-- date_sk w konwencji RRRRMMDD (20260115). To jedyny wymiar, w ktorym surrogate
-- key wolno wyliczyc z danych - daty nie zmieniaja swojej tozsamosci, wiec klucz
-- jest stabilny miedzy przeladowaniami. Dodatkowa zaleta: po samym kluczu
-- na faktcie widac, o ktory dzien chodzi.
CREATE TABLE gold_dim_date (
  date_sk     int,
  date_actual date,
  year        int,
  month       int,
  day         int,
  day_name    string,
  is_weekend  boolean
);

-- -----------------------------------------------------------------------------
-- Wymiar klienta
-- -----------------------------------------------------------------------------
-- customer_id to natural key ze zrodla; customer_sk to surrogate key nadany
-- przez data warehouse. Trzy powody, dla ktorych surrogate key istnieje:
--   1) historia - po SCD Type 2 (dzien 3) jeden customer_id ma kilka wierszy,
--      wiec przestaje jednoznacznie wskazywac wersje,
--   2) niezaleznosc od zrodla - migracja CRM zmienia format identyfikatora,
--   3) koszt zlaczenia - jeden int jest tanszy niz zlozony natural key.
--
-- Kolumny valid_from / valid_to / is_current sa tu juz teraz, mimo ze dzis kazdy
-- klient ma jedna wersje. Dzien 3 doklada druga wersje dla osob, ktore zmienily
-- miasto (SCD Type 2), i wtedy te kolumny zaczynaja cokolwiek znaczyc.
--
-- WAZNE: valid_from / valid_to nigdy nie sa NULL-em. Zamiast tego wchodza
-- wartosci brzegowe 1900-01-01 i 9999-12-31. Powod jest w 02_elt_athena.sql:
-- BETWEEN z NULL-em nie dopasowuje niczego i po cichu wycina wiersze faktu.
CREATE TABLE gold_dim_customer (
  customer_sk int,
  customer_id string,       -- natural key
  first_name  string,
  last_name   string,
  city        string,
  voivodeship string,
  segment     string,
  signup_date date,
  valid_from  date,
  valid_to    date,
  is_current  boolean
);

-- -----------------------------------------------------------------------------
-- Wymiar produktu
-- -----------------------------------------------------------------------------
-- list_price_pln, a nie unit_price_pln: to cena KATALOGOWA. Cena rzeczywiscie
-- zaplacona siedzi na faktcie i moze sie od niej roznic (promocje, negocjacje,
-- zmiany cennika). Mieszanie tych dwoch to klasyczne zrodlo rozjazdu
-- w raportach, a osobna nazwa kolumny jest najtanszym zabezpieczeniem.
--
-- Kategoria zostaje ATRYBUTEM tego wymiaru, nie osobna tabela dim_category.
-- Wydzielenie jej dalo by snowflake: dodatkowe zlaczenie w kazdym zapytaniu
-- o kategorie w zamian za oszczednosc miejsca, ktora przy 25 produktach
-- nie istnieje.
CREATE TABLE gold_dim_product (
  product_sk     int,
  product_id     int,       -- natural key
  sku            string,
  product_name   string,
  category       string,
  brand          string,
  list_price_pln double
);

-- -----------------------------------------------------------------------------
-- Wymiar kanalu
-- -----------------------------------------------------------------------------
-- Trzy wiersze, a mimo to osobna tabela. Dzieki temu da sie dolozyc atrybuty
-- kanalu (koszt pozyskania, opiekun, prowizja marketplace) bez ruszania tabeli
-- faktow - a fakt jest najwieksza tabela w modelu i kazda zmiana jego struktury
-- oznacza przepisanie calosci.
CREATE TABLE gold_dim_channel (
  channel_sk   int,
  channel_code string
);

-- -----------------------------------------------------------------------------
-- Tabela faktow
-- -----------------------------------------------------------------------------
-- GRAIN: JEDNA POZYCJA ZAMOWIENIA (order_id + product_id).
-- Grain jest decyzja nieodwracalna. Grubszy grain ("jedno zamowienie")
-- odbiera mozliwosc pytania o kategorie produktu, bo jedno zamowienie moze
-- zawierac produkty z kilku kategorii.
--
-- Fakt zawiera WYLACZNIE klucze i liczby. Nazw miast, kategorii i nazwisk tu
-- nie ma - one siedza w wymiarach. To odroznia star schema od jednej szerokiej
-- tabeli i decyduje o tym, czy zmiana nazwy kategorii wymaga przepisania
-- jednego wiersza wymiaru, czy calego faktu.
--
-- order_id nie jest kluczem obcym do wymiaru, tylko tzw. degenerate dimension:
-- identyfikatorem transakcji, ktory zostaje na faktcie, bo wszystkie jego
-- atrybuty (data, klient, kanal) sa juz osobnymi wymiarami. Bez niego przepada
-- AOV i wszystko, co wymaga liczby unikalnych zamowien.
CREATE TABLE gold_fact_order_item (
  order_id       string,    -- degenerate dimension
  date_sk        int,
  customer_sk    int,
  product_sk     int,
  channel_sk     int,
  qty            int,       -- addytywna
  unit_price_pln double,    -- NIEaddytywna: suma cen jednostkowych nie znaczy nic
  discount_pln   double,    -- addytywna
  net_amount_pln double     -- addytywna
);

-- =============================================================================
-- Kluczy obcych nie ma tu przypadkiem
-- =============================================================================
-- Athena nie zna FOREIGN KEY w ogole. Redshift przyjmuje deklaracje PRIMARY KEY
-- i FOREIGN KEY, ale ICH NIE EGZEKWUJE - sluza optymalizatorowi (pozwalaja mu
-- zalozyc unikalnosc i uproscic plan), a duplikat wejdzie do tabeli bez protestu.
--
-- Wniosek praktyczny: za spojnosc odpowiada sposob ladowania i zapytania
-- kontrolne z W4, nie silnik. Dlatego rekoncyliacja Silver = Gold nie jest
-- cwiczeniem dydaktycznym - w produkcji jest testem uruchamianym po kazdym
-- przeladowaniu, a jego niepowodzenie wstrzymuje publikacje raportow.
--
-- Oczekiwane licznosci po W3: dim_date 2 | dim_channel 3 | dim_customer 80 |
-- dim_product 25 | fact_order_item 409, suma net_amount_pln = 1427124.00.
-- =============================================================================
