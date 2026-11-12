# Dzień 2 — rozwiązania

To są rozwiązania wzorcowe do zestawienia z własną pracą — nie jedyne poprawne.
Model wymiarowy ma zwykle kilka sensownych wariantów, a różnica między nimi jest
tematem na dyskusję, nie na poprawianie.

Jedna uwaga o kolejności czytania: przeczytanie tego pliku przed samodzielną próbą
zabiera z ćwiczenia jego najcenniejszą część, czyli moment, w którym coś nie wychodzi
i trzeba dowiedzieć się dlaczego. Sekcje „Typowe pomyłki” istnieją właśnie po to,
żeby ten moment dał się skrócić bez pomijania go.

W przykładach jest prefix `aurora_jk` / `aurora-jk-lake` / `gold_jk` — do podmiany
na własny.

Wszystkie liczby poniżej to **zmierzone wyniki**, nie szacunki. Cała ścieżka Athena
została wykonana w konsoli AWS w regionie `eu-central-1`, a wartości kontrolne są
przeliczone z tych samych plików, które wgrywaliśmy do S3 w dniu 1.

Ten plik jest jedną ścieżką „jak”: pełny SQL identyczny z `sql/W2_ddl_star.sql`,
`sql/W3_elt_athena.sql`, `sql/W4_pytania_gold.sql`, `sql/W5_redshift.sql`.

---

## W1 — model wymiarowy (wzorzec)

**Proces biznesowy:** sprzedaż detaliczna online w Aurorze — konkretnie realizacja
pozycji zamówienia.

**Grain: jedna pozycja zamówienia** (`order_id` + `product_id`).

Uzasadnienie idzie przez pytania, na które model ma odpowiedzieć — i to jest jedyny
sensowny sposób wybierania grain:

| Wariant grain | Co przestaje być możliwe |
|---|---|
| jedno zamówienie | pytanie o kategorię `Telefony` — kategoria jest atrybutem produktu, a jedno zamówienie może mieć produkty z kilku kategorii. Przy tym grain produkt nie da się umieścić na faktcie bez naruszenia grain |
| jeden klient w jednym dniu | znika i produkt, i rabat na pozycji, i liczba unikalnych zamówień — czyli także AOV |
| jedna pozycja zamówienia | nic z powyższych pytań; da się z niej zagregować w górę do zamówienia i do dnia |

To jest ogólna zasada: **grain wybiera się najdrobniejszy, jaki źródło daje
i jaki da się utrzymać**. W dół nie da się zejść po fakcie, w górę zawsze da się
zagregować.

**Wymiary:**

| Wymiar | Atrybuty |
|---|---|
| Date | `date_actual`, `year`, `month`, `day`, `day_name`, `is_weekend` |
| Customer | `customer_id` (natural key), `first_name`, `last_name`, `city`, `voivodeship`, `segment`, `signup_date` |
| Product | `product_id` (natural key), `sku`, `product_name`, `category`, `brand`, `list_price_pln` |
| Channel | `channel_code` (`web` / `mobile` / `marketplace`) |

**Miary i ich addytywność:**

- **addytywne** (wolno sumować w każdym przekroju): `qty`, `discount_pln`,
  `net_amount_pln`, `gross_amount_pln`
- **nieaddytywne**: `unit_price_pln` — suma cen jednostkowych nie znaczy nic;
  AOV — to iloraz, więc liczy się go od nowa na każdym poziomie agregacji

**Star schema:** `fact_order_item` w środku, cztery wymiary dookoła, po jednym
złączeniu do każdego. Kategorii nie wydzielamy do osobnej tabeli `dim_category` —
to byłoby snowflake, dokładałoby złączenie do każdego zapytania o kategorię
i oszczędzało nieistotną ilość miejsca.

### Odpowiedź na pytanie z W1

Pierwsze pytanie („przychód kategorii `Telefony` 16 stycznia”) jest niemożliwe przy
grain „jedno zamówienie” **oraz** przy grain „klient × dzień”. W obu przypadkach
z tego samego powodu: kategoria mieszka w wymiarze produktu, a produkt nie może
znaleźć się na faktcie o grubszym grain — jedno zamówienie zawiera średnio
2,4 pozycji, potencjalnie z różnych kategorii, i nie ma sposobu przypisania go
do jednej z nich bez arbitralnej decyzji.

AOV wypada przy grain „klient × dzień”: liczba unikalnych zamówień przestaje być
odtwarzalna z faktu, bo jeden wiersz może pokrywać kilka zamówień tego samego
klienta w tym samym dniu.

---

## W2 — projekt star schema

dim_date — jeden dzień. dim_customer — jeden klient. dim_product — jeden produkt. dim_channel — jeden kanał. fact_order_item — jedna pozycja zamówienia. Ten plik nie jest do uruchomienia w Athenie.

Pełny skrypt (identyczny z `sql/W2_ddl_star.sql`):

```sql
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
```
Wzorzec: `sql/W2_ddl_star.sql`. Ten plik jest projektem modelu, nie skryptem —
Athena nie zna `CREATE TABLE` z samymi kolumnami, a wersją wykonywalną jest
`sql/W5_redshift.sql` z W5.

### Nazwy: prefiks w Athenie, schemat w Redshifcie

Baza w Glue Data Catalog nie ma schematów w środku, więc star schema w Athenie to
`aurora_jk.gold_dim_date`, `aurora_jk.gold_fact_order_item` i tak dalej — warstwę
oznacza prefiks `gold_` w nazwie tabeli. Dopiero Redshift ma prawdziwy
`CREATE SCHEMA gold_<inicjaly>` i tam nazwa tabeli to `dim_date` bez prefiksu.

Zapis `gold.dim_date` w treści W2 był więc **nazwą w modelu**, a nie literalnym SQL.
To rozróżnienie warto mieć z tyłu głowy przy czytaniu dowolnej książki
o modelowaniu wymiarowym: książki opisują model, a nie składnię konkretnego silnika.

### Surrogate key — trzy powody, w kolejności wagi

1. **Historia.** To najważniejszy powód i wraca jutro w W6. Gdy klient zmieni
   miasto, `gold_dim_customer` dostaje jego drugi wiersz. Oba mają ten sam
   `customer_id`, więc `customer_id` przestaje jednoznacznie wskazywać wiersz —
   a fakt ze stycznia musi dalej wskazywać na wersję ze starym miastem. Bez
   osobnego klucza SCD Type 2 jest niewykonalne.
2. **Niezależność od źródła.** Migracja na inny CRM zmienia format `customer_id`.
   Z surrogate key zmiana dotyczy jednej kolumny w jednym wymiarze; bez niego —
   każdego wiersza faktu.
3. **Koszt złączenia.** Natural key bywa złożony (np. `sklep_id` + `nr_paragonu`
   + `data`). Złączenie po jednym `int` jest tańsze niż po trzech kolumnach
   mieszanych typów, a fakt jest największą tabelą w modelu.

### order_id na faktcie — degenerate dimension

`order_id` zostaje na faktcie, choć nie wskazuje na żaden wymiar. Taką kolumnę
nazywa się *degenerate dimension*: identyfikator transakcji, dla którego nie ma
sensownej tabeli wymiaru, bo wszystkie jego atrybuty (data, klient, kanał) są już
osobnymi wymiarami. Zostałby z niego wymiar zawierający wyłącznie klucz.

Bez `order_id` przepada **AOV** i wszystko, co wymaga liczby zamówień: średnia
liczba pozycji w koszyku, udział zamówień z rabatem, rozkład wartości zamówienia.
Suma przychodu i liczba sztuk działałyby dalej bez niego.

### Kluczy obcych nie ma i to nie jest przeoczenie

Athena nie zna `FOREIGN KEY` w ogóle. Redshift przyjmuje deklarację, ale
**jej nie egzekwuje** — `PRIMARY KEY` i `FOREIGN KEY` służą optymalizatorowi
(pozwalają mu założyć unikalność i uprościć plan), a duplikat wejdzie do tabeli
bez protestu.

Wniosek praktyczny: w data warehouse za spójność odpowiada **sposób ładowania
i zapytania kontrolne**, nie silnik. Dlatego rekoncyliacja z W4 punkt 4 nie jest
ozdobnikiem dydaktycznym — w produkcji jest to test uruchamiany po każdym
przeładowaniu.

---

## W3 — ELT Silver → Gold w Athenie

`gold_fact_order_item` — jeden wiersz to jedna pozycja paid/shipped (**409**, nie 486).

Pełny skrypt (identyczny z `sql/W3_elt_athena.sql`):

```sql
-- =============================================================================
-- Dzien 2, W3 - ELT Silver -> Gold w Athenie
-- =============================================================================
-- SILNIK: AMAZON ATHENA. NIE Redshift - Redshift nie zna CTAS-a ze skladnia
-- WITH (format = 'PARQUET', external_location = ...).
--
-- NAZWY: dwuczlonowo, `aurora_<inicjaly>.tabela` - baza w Glue Data Catalog
-- plus nazwa tabeli. Ta sama warstwa Gold ogladana z Redshifta nazywa sie
-- "awsdatacatalog"."aurora_<inicjaly>"."gold_..." - wyjasnienie w 03_redshift.sql.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
--
-- Narzedziem jest CTAS (CREATE TABLE AS SELECT): Athena wykonuje SELECT,
-- zapisuje wynik na S3 jako nowe pliki Parquet i rejestruje tabele w Glue Data
-- Catalog. To jest litera "T" z ELT - transformacja zwyklym SQL-em, bez Glue Job
-- i bez Sparka.
--
-- Gold NICZEGO NIE FILTRUJE. Silver odfiltrowal `cancelled` w dniu 1. Gold
-- zajmuje sie wylacznie ukladem: zamienia dane ulozone pod zrodlo na dane
-- ulozone pod pytania. Gdyby Gold jeszcze cos odrzucal, znaczyloby to, ze Silver
-- nie dokonczyl roboty.
--
-- KOLEJNOSC MA ZNACZENIE: najpierw wszystkie wymiary, potem fakt. Fakt zlacza
-- sie z wymiarami po natural key i podmienia je na surrogate key, wiec
-- wymiary musza juz istniec. Wewnatrz grupy wymiarow kolejnosc jest dowolna.
--
-- Wszystkie CREATE maja IF NOT EXISTS - caly plik mozna puscic ponownie.
-- Zastrzezenie: po DROP TABLE, gdy pliki zostaly na S3, pojawi sie
-- HIVE_PATH_ALREADY_EXISTS. IF NOT EXISTS przed tym NIE chroni, bo tabeli
-- w katalogu juz nie ma - trzeba skasowac prefiks gold/ w S3 recznie.
--
-- UWAGA NA TYPY. ROW_NUMBER(), year(), month() i day() zwracaja BIGINT.
-- Bez jawnego CAST(... AS integer) lancuch wyglada tak:
--   bigint w SELECT -> int64 w pliku Parquet -> COPY do kolumny int (int32)
--   w Redshifcie (W5) przerywa cale ladowanie na niezgodnosci typow.
-- Blad pojawia sie wiec godzine pozniej, w innym narzedziu, na innym ekranie
-- niz jego przyczyna. Sprawdzone kolumna po kolumnie na realnych plikach z S3.
--
-- GDZIE LADUJA PLIKI: kazdy CTAS ma jawne external_location, wiec Parquet trafia
-- pod s3://aurora-jk-lake/gold/<nazwa>/. W5 czyta instrukcja COPY dokladnie te
-- sciezki - warto je zanotowac.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_date
-- -----------------------------------------------------------------------------
-- date_sk liczony arytmetycznie w konwencji RRRRMMDD (20260115): 2026 * 10000
-- + 1 * 100 + 15. Czytelny przy debugowaniu i stabilny miedzy przeladowaniami,
-- bo wynika z daty, a nie z kolejnosci wierszy.
--
-- date_format() oczekuje timestampa, wiec na kolumnie typu date wymaga CAST-a.
-- '%W' to pelna nazwa dnia tygodnia po angielsku (Thursday, Friday).
--
-- day_of_week(): 1 = poniedzialek ... 7 = niedziela, stad IN (6, 7) na weekend.
-- Numeracja rozni sie miedzy silnikami - w PostgreSQL 0 = niedziela - wiec przy
-- przenoszeniu tego kodu warunek trzeba sprawdzic, a nie przepisac.
--
-- Zrodlem sa daty wystepujace w pozycjach zamowien. W produkcji wymiar daty
-- generuje sie niezaleznie od faktu, na kilka lat w przod, wlasnie zeby dzien
-- bez sprzedazy mial swoj wiersz.
CREATE TABLE IF NOT EXISTS aurora_jk.gold_dim_date
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/gold/dim_date/'
) AS
SELECT
  CAST(year(d) * 10000 + month(d) * 100 + day(d) AS integer) AS date_sk,
  d AS date_actual,
  CAST(year(d) AS integer)  AS year,
  CAST(month(d) AS integer) AS month,
  CAST(day(d) AS integer)   AS day,
  date_format(CAST(d AS timestamp), '%W') AS day_name,
  CASE WHEN day_of_week(d) IN (6, 7) THEN true ELSE false END AS is_weekend
FROM (
  SELECT DISTINCT order_date AS d
  FROM aurora_jk.silver_order_items
) x;

-- Oczekiwane: 2 wiersze (2026-01-15, 2026-01-16).
-- date_sk = 20260115 i 20260116. Oba dni to czwartek i piatek, wiec
-- is_weekend = false w obu wierszach.

-- -----------------------------------------------------------------------------
-- dim_channel
-- -----------------------------------------------------------------------------
-- DISTINCT w podzapytaniu jest obowiazkowy. Bez niego wymiar mialby tyle
-- wierszy, ile zamowien (170), kazde z osobnym channel_sk - a fakt zlaczony
-- z takim wymiarem zdublowalby wiersze. To najczestsza przyczyna sytuacji
-- "Gold ma wiecej wierszy niz Silver".
CREATE TABLE IF NOT EXISTS aurora_jk.gold_dim_channel
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/gold/dim_channel/'
) AS
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY channel) AS integer) AS channel_sk,
  channel AS channel_code
FROM (SELECT DISTINCT channel FROM aurora_jk.silver_orders) s;

-- Oczekiwane: 3 wiersze - marketplace (1), mobile (2), web (3).
-- Numeracja wynika z ORDER BY channel, czyli z alfabetu.

-- -----------------------------------------------------------------------------
-- dim_customer
-- -----------------------------------------------------------------------------
-- ORDER BY w ROW_NUMBER() nie jest ozdobnikiem. Bez deterministycznej kolejnosci
-- ten sam klient przy ponownym przeliczeniu wymiaru moglby dostac inny
-- customer_sk - a fakt wskazywalby wtedy na kogos innego. W produkcji te role
-- pelni sekwencja albo IDENTITY, wlasnie po to, zeby klucz raz nadany sie
-- nie zmienil.
--
-- valid_from = 1900-01-01, a NIE NULL. Znaczy "ta wersja obowiazuje od zawsze".
-- Powod jest praktyczny i ujawnia sie dopiero jutro. Zlaczenie SCD Type 2 to:
--   ON i.customer_id = c.customer_id
--  AND i.order_date BETWEEN c.valid_from AND c.valid_to
-- BETWEEN z NULL-em po ktorejkolwiek stronie daje NULL, a nie TRUE - i NIE
-- DOPASOWUJE NICZEGO. Wiersze faktu dla takich klientow wypadaja na INNER JOIN
-- po cichu, bez bledu i bez ostrzezenia. Jedynym sygnalem jest rekoncyliacja
-- z W4 pkt 4. Ta sama zasada dotyczy valid_to = 9999-12-31.
--
-- Wartosci brzegowe zamiast NULL-i to standardowy wzorzec w SCD Type 2 i warto
-- go stosowac od pierwszego dnia, a nie dopisywac po pierwszym rozjezdzie.
CREATE TABLE IF NOT EXISTS aurora_jk.gold_dim_customer
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/gold/dim_customer/'
) AS
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY customer_id) AS integer) AS customer_sk,
  customer_id,
  first_name,
  last_name,
  city,
  voivodeship,
  segment,
  signup_date,
  DATE '1900-01-01' AS valid_from,
  DATE '9999-12-31' AS valid_to,
  true AS is_current
FROM aurora_jk.silver_customers;

-- Oczekiwane: 80 wierszy, customer_sk od 1 do 80, is_current = true wszedzie.

-- -----------------------------------------------------------------------------
-- dim_product
-- -----------------------------------------------------------------------------
-- unit_price_pln z produktu to cena KATALOGOWA, stad zmiana nazwy na
-- list_price_pln. Cena rzeczywiscie zaplacona siedzi na faktcie i moze sie od
-- niej roznic - mieszanie tych dwoch to klasyczne zrodlo rozjazdu w raportach.
-- Ta sama nazwa kolumny w dwoch miejscach o dwoch roznych znaczeniach to
-- zaproszenie do bledu, ktory wyjdzie dopiero na spotkaniu z biznesem.
CREATE TABLE IF NOT EXISTS aurora_jk.gold_dim_product
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/gold/dim_product/'
) AS
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY product_id) AS integer) AS product_sk,
  product_id,
  sku,
  product_name,
  category,
  brand,
  unit_price_pln AS list_price_pln
FROM aurora_jk.silver_products;

-- Oczekiwane: 25 wierszy, product_sk od 1 do 25, szesc roznych kategorii
-- (AGD, Akcesoria, Audio, Komputery, Telefony, TV).

-- -----------------------------------------------------------------------------
-- fact_order_item
-- -----------------------------------------------------------------------------
-- Piec zlaczen, kazde po natural key, a na fakt trafia tylko *_sk.
--
-- Czwarte zlaczenie - do silver_orders - jest jedynym, ktore nie idzie do
-- wymiaru. Kanal jest atrybutem ZAMOWIENIA, nie pozycji, a silver_order_items
-- tej kolumny nie ma. Bez tego przeskoku nie da sie dojsc do channel_code.
--
-- Wszystkie zlaczenia sa INNER JOIN, wiec kazdy brak w wymiarze po cichu usuwa
-- wiersz faktu. W produkcji stosuje sie zamiast tego wiersz nieznany
-- (*_sk = -1) i LEFT JOIN z COALESCE - wtedy fakt nigdy nie traci wiersza,
-- a problem jest widoczny w raporcie zamiast z niego znikac.
--
-- KOLUMNA PARTYCJONUJACA MUSI BYC OSTATNIA. partitioned_by = ARRAY['dt'] NIE
-- wskazuje kolumny po nazwie - Athena bierze tyle OSTATNICH kolumn z SELECT,
-- ile jest elementow w tablicy. Przy dt w srodku listy partycjonowanie poleci
-- po net_amount_pln i utworzy na S3 setki katalogow net_amount_pln=1234.56/.
-- Komunikat bledu, jesli w ogole sie pojawi, nie wskaze przyczyny.
CREATE TABLE IF NOT EXISTS aurora_jk.gold_fact_order_item
WITH (
  format            = 'PARQUET',
  partitioned_by    = ARRAY['dt'],
  external_location = 's3://aurora-jk-lake/gold/fact_order_item/'
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
  i.net_amount_pln,
  i.dt              -- kolumna partycjonujaca: OSTATNIA na liscie
FROM aurora_jk.silver_order_items i
JOIN aurora_jk.gold_dim_date d     ON i.order_date  = d.date_actual
JOIN aurora_jk.gold_dim_customer c ON i.customer_id = c.customer_id
JOIN aurora_jk.gold_dim_product p  ON i.product_id  = p.product_id
JOIN aurora_jk.silver_orders o     ON i.order_id    = o.order_id
JOIN aurora_jk.gold_dim_channel ch ON o.channel     = ch.channel_code;

-- Oczekiwane: 409 wierszy w dwoch partycjach (dt=2026-01-15, dt=2026-01-16).
-- Kolumna dt zostaje w katalogu Glue, ale NIE jest zapisana w pliku Parquet -
-- siedzi w nazwie katalogu. Dlatego tabela w Redshifcie (W5) ma 9 kolumn,
-- a nie 10.

-- =============================================================================
-- KONTROLA
-- =============================================================================

-- Oczekiwane: silver_rows = gold_rows = 409.
-- Gold MNIEJSZY oznacza, ze ktorys INNER JOIN do wymiaru nie trafil i po cichu
--   usunal wiersze. Diagnostyka jest w nastepnym zapytaniu.
-- Gold WIEKSZY oznacza duplikat natural key w ktoryms wymiarze - zwykle
--   brak DISTINCT w podzapytaniu budujacym wymiar.
SELECT
  (SELECT COUNT(*) FROM aurora_jk.silver_order_items)   AS silver_rows,
  (SELECT COUNT(*) FROM aurora_jk.gold_fact_order_item) AS gold_rows;

-- Diagnostyka zgubionych wierszy: kazda kolumna ponizej powinna dac 0.
-- Wartosc rozna od zera wskazuje wymiar, w ktorym brakuje wierszy.
SELECT
  COUNT(*)                        AS wszystkie,
  COUNT(*) - COUNT(d.date_sk)     AS bez_daty,
  COUNT(*) - COUNT(c.customer_sk) AS bez_klienta,
  COUNT(*) - COUNT(p.product_sk)  AS bez_produktu
FROM aurora_jk.silver_order_items i
LEFT JOIN aurora_jk.gold_dim_date d     ON i.order_date  = d.date_actual
LEFT JOIN aurora_jk.gold_dim_customer c ON i.customer_id = c.customer_id
LEFT JOIN aurora_jk.gold_dim_product p  ON i.product_id  = p.product_id;

-- Diagnostyka duplikatow w wymiarze: oczekiwane 0 wierszy w wyniku.
SELECT customer_id, COUNT(*) AS wersje
FROM aurora_jk.gold_dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Rekoncyliacja. Oczekiwane: 1427124.00 w obu wierszach, co do grosza.
-- ORDER BY, bo UNION ALL sam nie gwarantuje kolejnosci wierszy.
SELECT 'silver' AS layer, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
UNION ALL
SELECT 'gold', ROUND(SUM(net_amount_pln), 2)
FROM aurora_jk.gold_fact_order_item
ORDER BY layer;

-- Licznosci wymiarow. Oczekiwane (kolejnosc z ORDER BY wymiar):
--   dim_channel 3 | dim_customer 80 | dim_date 2 | dim_product 25
SELECT 'dim_date' AS wymiar, COUNT(*) AS n FROM aurora_jk.gold_dim_date
UNION ALL SELECT 'dim_channel',  COUNT(*) FROM aurora_jk.gold_dim_channel
UNION ALL SELECT 'dim_customer', COUNT(*) FROM aurora_jk.gold_dim_customer
UNION ALL SELECT 'dim_product',  COUNT(*) FROM aurora_jk.gold_dim_product
ORDER BY wymiar;

-- Zarejestrowane partycje faktu. Oczekiwane: dt=2026-01-15 i dt=2026-01-16.
-- CTAS rejestruje partycje sam, wiec MSCK REPAIR nie jest tu potrzebny -
-- inaczej niz przy CREATE EXTERNAL TABLE na gotowych plikach z dnia 1.
SHOW PARTITIONS aurora_jk.gold_fact_order_item;
```

Pełny skrypt: `sql/W3_elt_athena.sql`. Baza `aurora_<inicjaly>` istnieje od dnia 1.

Kluczowa myśl całego ćwiczenia: **nic tu nie czyścimy**. Silver odfiltrował
`cancelled` wczoraj. Gold zajmuje się wyłącznie układem — zamienia dane ułożone
pod źródło na dane ułożone pod pytania. Gdyby Gold jeszcze coś filtrował, znaczyłoby
to, że Silver nie dokończył roboty.

### Surrogate key w Athenie

```sql
CAST(ROW_NUMBER() OVER (ORDER BY customer_id) AS integer) AS customer_sk
```

`ROW_NUMBER()` bez `PARTITION BY` numeruje cały zbiór w kolejności zadanej przez
`ORDER BY` — 1, 2, 3, … Deterministyczna kolejność jest tu istotna: gdyby `ORDER BY`
nie było, przy ponownym przeliczeniu wymiaru ten sam klient mógłby dostać inny
klucz, a fakt wskazywałby wtedy na kogoś innego. W produkcji tę rolę pełni sekwencja
albo `IDENTITY` w bazie, właśnie po to, żeby klucz raz nadany już się nie zmieniał.

**`CAST(... AS integer)` jest obowiązkowy i to nie jest kosmetyka.** Łańcuch przyczyn
ma trzy ogniwa:

1. `ROW_NUMBER()` w Trino zwraca `bigint`.
2. CTAS zapisuje wynik do Parquetu, a `bigint` zapisuje się jako fizyczny typ
   `int64`.
3. W W5 `COPY ... FORMAT AS PARQUET` wkłada tę kolumnę do `customer_sk int`
   w Redshifcie — czyli do `int32`. `COPY` dopasowuje kolumny po typie i przerywa
   całe ładowanie na niezgodności, bez ładowania czegokolwiek.

Ta sama pułapka dotyczy `year()`, `month()` i `day()` w `gold_dim_date` — one też
zwracają `bigint`. Efekt jest szczególnie nieprzyjemny, bo błąd pojawia się
o godzinę później, w innym narzędziu, na innym ekranie niż jego przyczyna.

### dim_date — dlaczego osobny wymiar i dlaczego RRRRMMDD

Wymiar daty buduje się osobno, a nie z faktu. W naszym datasecie oba dni mają
sprzedaż, więc różnicy nie widać — ale w modelu z jednym dniem bez zamówień raport
„sprzedaż dzień po dniu” po cichu **pominąłby ten dzień**, zamiast pokazać zero.
Dziura w wykresie i zero na wykresie to dwie różne informacje biznesowe.

`date_sk` w konwencji `RRRRMMDD` jako liczba (`20260115`) jest czytelny przy
debugowaniu — po samym kluczu na faktcie widać, o który dzień chodzi — i stabilny
między przeładowaniami, bo wynika z daty, a nie z kolejności wierszy. To jedyny
wymiar, w którym surrogate key wolno wyliczyć z danych, właśnie dlatego, że daty
nie zmieniają swojej tożsamości.

### valid_from = DATE '1900-01-01', nie NULL

`gold_dim_customer` ma już dzisiaj kolumny `valid_from`, `valid_to` i `is_current`,
mimo że każdy klient ma jedną wersję. Wartości są sztywne: `1900-01-01`
i `9999-12-31`.

Naturalnym odruchem jest wpisanie tam `NULL` w znaczeniu „bez ograniczenia”.
To jest pułapka, która ujawnia się dopiero jutro. Złączenie w SCD Type 2 wygląda
tak:

```sql
JOIN gold_dim_customer c
  ON i.customer_id = c.customer_id
 AND i.order_date BETWEEN c.valid_from AND c.valid_to
```

`BETWEEN` z `NULL`-em po którejkolwiek stronie daje `NULL`, a nie `TRUE` —
i **nie dopasowuje niczego**. Wiersze faktu dla takich klientów wypadają
na `INNER JOIN` po cichu, bez błędu i bez ostrzeżenia. Fakt ma wtedy mniej wierszy,
niż powinien, a jedynym sygnałem jest rekoncyliacja z W4 punkt 4.

Wartości brzegowe zamiast `NULL`-i to standardowy wzorzec w SCD Type 2 właśnie
dlatego: `1900-01-01` znaczy „ta wersja obowiązuje od zawsze”, `9999-12-31` —
„i obowiązuje nadal”, a `BETWEEN` działa na nich bez wyjątków.

### dim_product — list_price kontra cena zapłacona

Cena z `silver_products` trafia do wymiaru pod nazwą `list_price_pln`, nie
`unit_price_pln`. To cena **katalogowa**. Cena faktycznie zapłacona siedzi
na faktcie i potrafi się od niej różnić (promocje, negocjacje, historia zmian
cennika).

Mieszanie tych dwóch to klasyczne źródło rozjazdu w raportach: przychód policzony
z wymiaru produktu daje inną liczbę niż przychód z faktu, a różnica jest tym
większa, im dłuższa historia. Osobna nazwa kolumny jest najtańszym możliwym
zabezpieczeniem.

### Fakt — pięć złączeń, na fakt trafiają tylko klucze

```sql
FROM aurora_jk.silver_order_items i
JOIN aurora_jk.gold_dim_date d     ON i.order_date  = d.date_actual
JOIN aurora_jk.gold_dim_customer c ON i.customer_id = c.customer_id
JOIN aurora_jk.gold_dim_product p  ON i.product_id  = p.product_id
JOIN aurora_jk.silver_orders o     ON i.order_id    = o.order_id
JOIN aurora_jk.gold_dim_channel ch ON o.channel     = ch.channel_code
```

Czwarte złączenie — do `silver_orders` — jest jedynym, które nie idzie do wymiaru.
Powód: kanał jest atrybutem **zamówienia**, nie pozycji, a `silver_order_items`
tej kolumny nie ma. Bez tego przeskoku nie da się dojść do `channel_code`
w wymiarze kanału.

Warto zauważyć, że jest to złączenie przez warstwę Silver w środku procesu
budowania Gold. Nie jest to obejście — Silver jest legalnym źródłem dla Gold,
a alternatywą byłoby dodanie `channel` do `silver_order_items` wczoraj.

### Kolumna partycjonująca musi być ostatnia

```sql
  i.net_amount_pln,
  i.dt              -- OSTATNIA na liście SELECT
```

`partitioned_by = ARRAY['dt']` **nie wskazuje kolumny po nazwie**. Athena bierze
tyle *ostatnich* kolumn z listy `SELECT`, ile jest elementów w tablicy, i traktuje
je jako partycjonujące. Przy `dt` w środku listy partycjonowanie poleci po
kolumnie, która stała na końcu — czyli po `net_amount_pln` — i utworzy na S3 setki
katalogów w rodzaju `net_amount_pln=1234.56/`. Komunikat błędu, jeśli w ogóle
się pojawi, nie wskaże przyczyny.

### Co ma wyjść po W3

| Tabela | Wierszy |
|---|---|
| `gold_dim_channel` | 3 (`marketplace`, `mobile`, `web`) |
| `gold_dim_customer` | 80 |
| `gold_dim_date` | 2 (15 i 16 stycznia) |
| `gold_dim_product` | 25 |
| `gold_fact_order_item` | **409** |

Wymiary są w kolejności, w jakiej wypisuje je zapytanie kontrolne
z `sql/W3_elt_athena.sql` (`UNION ALL` czterech `COUNT(*)` z `ORDER BY wymiar`).

Suma `net_amount_pln` na faktcie: **1 427 124,00**.

Liczba 409 nie jest przypadkowa — to dokładnie tyle, ile ma `silver_order_items`.
Gold nie filtruje, więc każda inna liczba jest błędem, nawet gdy „wygląda
rozsądnie”.

Na S3 pliki wylądują pod `s3://aurora-jk-lake/gold/dim_*/` oraz
`s3://aurora-jk-lake/gold/fact_order_item/dt=2026-01-15/` i `dt=2026-01-16/`.
W5 czyta instrukcją `COPY` dokładnie te ścieżki.

### Typowe pomyłki

- **Fakt ma mniej niż 409 wierszy.** Któryś `INNER JOIN` nie trafił. Diagnostyka:
  zamiana złączeń kolejno na `LEFT JOIN` i policzenie `NULL`-i w kolumnach `*_sk`.
  Najczęstsza przyczyna to wymiar zbudowany z węższego źródła niż fakt — na przykład
  `gold_dim_date` z `silver_orders` zamiast z `silver_order_items`.
- **Fakt ma więcej niż 409 wierszy.** Któryś wymiar ma duplikat natural key,
  więc złączenie zwielokrotniło wiersze. Zwykle brakuje `DISTINCT` w podzapytaniu
  budującym wymiar.
- **Nazwy miast i kategorii na faktcie.** Kuszące, bo skraca zapytania w W4, ale
  zamienia star schema w jedną szeroką tabelę: pierwsza zmiana nazwy kategorii
  wymaga wtedy przepisania całego faktu.
- **`gold_dim_date` z wielu źródeł.** Daty z zamówień i daty z pozycji muszą dać
  ten sam zbiór; jeśli nie dają, problem jest w warstwie Silver, nie tutaj.

### Gdy CTAS nie chce się wykonać drugi raz

| Komunikat | Co się stało | Naprawa |
|---|---|---|
| `TABLE_ALREADY_EXISTS` | tabela jest już w Glue Data Catalog | `IF NOT EXISTS` pomija ją bez błędu |
| `HIVE_PATH_ALREADY_EXISTS` | tabela skasowana przez `DROP`, ale pliki zostały pod `external_location` | usunięcie prefiksu w S3 albo inna lokalizacja w `WITH (...)` |
| `COLUMN_NOT_FOUND` przy `dt` | `partitioned_by` wymienia kolumnę, której nie ma w `SELECT` | dopisanie `dt` na końcu listy |
| `Column 'x' is ambiguous` | ta sama nazwa kolumny w dwóch złączonych tabelach | kwalifikowanie aliasem: `i.customer_id` |

Drugi przypadek zdarza się zawsze, gdy ktoś próbuje przebudować wymiar przez
`DROP TABLE` i ponowny CTAS. `IF NOT EXISTS` **nie chroni** przed nim — Athena
z założenia nie kasuje danych z S3, żeby przypadkowe `CREATE` nie zniszczyło
cudzej pracy.

---

## W4 — pytania biznesowe na star schema

Zapytania na Gold. Nic nie tworzą — czytają tabele z W3.

Pełny skrypt (identyczny z `sql/W4_pytania_gold.sql`):

```sql
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
```

Pełny skrypt z liczbami kontrolnymi: `sql/W4_pytania_gold.sql`.

**Silnik: Athena.** Nazwy są dwuczłonowe, `aurora_jk.gold_…`, bo Athena rozmawia
z Glue Data Catalog wprost. Te same cztery pytania zadane z Query Editora v2
Redshifta wymagają zapisu trzyczłonowego — wersja Redshiftowa jest w W5,
a wyjaśnienie różnicy w sekcji „Dwa silniki, trzy formy nazw” w `01_zadania.md`.

```sql
-- 1. przychód netto wg kategorii i dnia
SELECT
  d.date_actual,
  p.category,
  ROUND(SUM(f.net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.gold_fact_order_item f
JOIN aurora_jk.gold_dim_date d     ON f.date_sk = d.date_sk
JOIN aurora_jk.gold_dim_product p  ON f.product_sk = p.product_sk
GROUP BY d.date_actual, p.category
ORDER BY d.date_actual, revenue_pln DESC;

-- 2. top 5 miast w kategorii AGD
SELECT
  c.city,
  ROUND(SUM(f.net_amount_pln), 2) AS agd_revenue_pln
FROM aurora_jk.gold_fact_order_item f
JOIN aurora_jk.gold_dim_customer c ON f.customer_sk = c.customer_sk
JOIN aurora_jk.gold_dim_product p  ON f.product_sk = p.product_sk
WHERE p.category = 'AGD'
GROUP BY c.city
ORDER BY agd_revenue_pln DESC
LIMIT 5;

-- 3. AOV wg kanału
SELECT
  ch.channel_code,
  ROUND(SUM(f.net_amount_pln) / COUNT(DISTINCT f.order_id), 2) AS aov_pln,
  COUNT(DISTINCT f.order_id) AS orders_n
FROM aurora_jk.gold_fact_order_item f
JOIN aurora_jk.gold_dim_channel ch ON f.channel_sk = ch.channel_sk
GROUP BY ch.channel_code
ORDER BY aov_pln DESC;

-- 4. rekoncyliacja Silver = Gold
SELECT 'silver' AS layer, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
UNION ALL
SELECT 'gold', ROUND(SUM(net_amount_pln), 2)
FROM aurora_jk.gold_fact_order_item
ORDER BY layer;
```

### Wyniki — punkt 1, pełna siatka

| Dzień | Kategoria | Przychód PLN |
|---|---|---|
| 2026-01-15 | Telefony | 199 197,00 |
| 2026-01-15 | Komputery | 152 821,00 |
| 2026-01-15 | TV | 142 358,00 |
| 2026-01-15 | AGD | 117 153,00 |
| 2026-01-15 | Audio | 36 870,00 |
| 2026-01-15 | Akcesoria | 5 132,00 |
| 2026-01-16 | Komputery | 209 784,00 |
| 2026-01-16 | Telefony | 182 780,00 |
| 2026-01-16 | TV | 182 442,00 |
| 2026-01-16 | AGD | 143 912,00 |
| 2026-01-16 | Audio | 46 384,00 |
| 2026-01-16 | Akcesoria | 8 291,00 |

Dwanaście wierszy: sześć kategorii razy dwa dni. Suma 15.01 to **653 531,00**,
16.01 to **773 593,00** — te same kwoty, które wyszły wczoraj w L4 na warstwie
Bronze i w L6 na Silver. Trzecia niezależna droga do tej samej liczby.

Ranking zmienia się między dniami: 15.01 prowadzą Telefony, 16.01 Komputery.
Warto to zauważyć, bo raport z jednego dnia zbudowałby fałszywy wniosek o tym,
która kategoria jest najważniejsza.

### Wyniki — punkty 2, 3 i 4

| Miasto (AGD) | Przychód PLN |
|---|---|
| Gdańsk | 47 052,00 |
| Lublin | 39 884,00 |
| Warszawa | 35 859,00 |
| Kraków | 32 694,00 |
| Łódź | 23 097,00 |

| Kanał | AOV PLN | Zamówień |
|---|---|---|
| mobile | 9 235,24 | 49 |
| web | 8 236,45 | 55 |
| marketplace | 7 902,91 | 66 |

| Rekoncyliacja (`ORDER BY layer`) | Kwota |
|---|---|
| `gold` — `gold_fact_order_item` | 1 427 124,00 |
| `silver` — `silver_order_items` | 1 427 124,00 |

W punkcie 2 filtr działa na **wymiarze produktu**, a grupowanie na **wymiarze
klienta** — sam fakt nie wie ani o kategoriach, ani o miastach, bo trzyma wyłącznie
klucze. To jest cała mechanika star schema pokazana na jednym zapytaniu.

Miasto pochodzi z `dim_customer`, więc jest to miasto klienta, nie miasto dostawy.
Przy dwóch różnych miastach w danych (`delivery_city` na zamówieniu, `city`
na kliencie) to rozróżnienie ma znaczenie i wraca jutro w W9 jako pytanie
„as-was czy as-is”.

### Dlaczego COUNT(DISTINCT order_id), a nie COUNT(*)

Grain faktu to **pozycja** zamówienia. Jedno zamówienie zajmuje więc kilka wierszy —
średnio 2,4, bo 409 pozycji przypada na 170 zamówień.

`COUNT(*)` policzyłby pozycje i podzielił przychód przez zawyżony mianownik: AOV
wyszłoby około dwuipółkrotnie niższe i wyglądałoby zupełnie wiarygodnie
(3–4 tysiące zamiast 8–9). To jest najgorszy rodzaj błędu w data warehouse — taki,
który daje liczbę mieszczącą się w oczekiwaniach.

Suma `orders_n` z tabeli wyżej to 49 + 55 + 66 = **170**, czyli tyle, ile
`silver_orders`. Dobra kontrola przy okazji: gdyby wyszło więcej, znaczyłoby, że
jakieś zamówienie ma pozycje przypisane do dwóch kanałów.

### Dlaczego AOV jest miarą nieaddytywną

AOV to iloraz dwóch miar addytywnych. Ilorazu nie wolno agregować:

- średnia arytmetyczna z 9 235,24, 8 236,45 i 7 902,91 to 8 458,20
- AOV całego sklepu to 1 427 124,00 / 170 = **8 394,85**

Różnica bierze się z tego, że kanały mają różną liczbę zamówień, a średnia
arytmetyczna traktuje je jak równe. Poprawny AOV całości wymaga przeliczenia
od zera z surowych sum — i to jest praktyczny powód, dla którego na faktcie trzymamy
`net_amount_pln` i `order_id`, a nie policzone z góry AOV.

W narzędziach BI ten sam problem wraca jako różnica między miarą liczoną
w bazie a miarą liczoną po agregacji. Wiersz „Total” pokazujący średnią ze średnich
to jeden z najczęstszych błędów w dashboardach.

### Po co rekoncyliacja i co znaczy każdy kierunek rozjazdu

Rekoncyliacja odpowiada na pytanie, na które nikt inny nie odpowie: **czy przy
przekładaniu danych do nowego układu nic nie zginęło i nic się nie zdublowało.**
Silnik tego nie sprawdzi, bo w data warehouse nie ma egzekwowanych kluczy obcych.
W produkcji ten test uruchamia się po każdym przeładowaniu, a jego niepowodzenie
zatrzymuje publikację raportów.

Dwa kierunki rozjazdu mają dwie różne przyczyny i dwie różne diagnostyki:

**Gold mniejszy niż Silver** — któryś `INNER JOIN` do wymiaru nie trafił i po cichu
usunął wiersze. Znalezienie winowajcy:

```sql
SELECT
  COUNT(*)                                        AS wszystkie,
  COUNT(*) - COUNT(d.date_sk)                     AS bez_daty,
  COUNT(*) - COUNT(c.customer_sk)                 AS bez_klienta,
  COUNT(*) - COUNT(p.product_sk)                  AS bez_produktu
FROM aurora_jk.silver_order_items i
LEFT JOIN aurora_jk.gold_dim_date d     ON i.order_date  = d.date_actual
LEFT JOIN aurora_jk.gold_dim_customer c ON i.customer_id = c.customer_id
LEFT JOIN aurora_jk.gold_dim_product p  ON i.product_id  = p.product_id;
```

Kolumna z liczbą różną od zera wskazuje wymiar, w którym brakuje wierszy.
W produkcji rozwiązuje się to nie przez naprawianie wymiaru w biegu, ale przez
**wiersz nieznany** (`*_sk = -1`, `city = 'UNKNOWN'`) i `LEFT JOIN` z `COALESCE` —
wtedy fakt nigdy nie traci wiersza, a problem jest widoczny w raporcie zamiast
znikać z niego.

**Gold większy niż Silver** — któryś wymiar ma duplikat natural key, więc
złączenie zwielokrotniło wiersze faktu:

```sql
SELECT customer_id, COUNT(*)
FROM aurora_jk.gold_dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

Zapytanie powinno zwrócić zero wierszy. Analogiczne dla `product_id`
w `gold_dim_product` i `date_actual` w `gold_dim_date`. Jutro, po SCD Type 2,
ten test zmieni się w „duplikat w obrębie `is_current = true`” — bo od jutra dwie
wersje tego samego klienta są poprawne.

### Liczba złączeń — mierzalna korzyść z modelu

| Model | Złączenia w pytaniu „przychód wg kategorii i dnia” |
|---|---|
| star schema | 2 (fakt → `dim_date`, fakt → `dim_product`) |
| znormalizowany model źródłowy (3NF) | 6–8 (klient → zamówienie → pozycja → produkt → kategoria → …) |

To nie jest argument o wydajności — na 409 wierszach jest ona nieodróżnialna.
To argument o **czytelności i o liczbie miejsc, w których da się popełnić błąd**.
Sześć złączeń to sześć okazji do pomylenia kierunku, zdublowania wierszy albo
zgubienia ich na `INNER JOIN`.

### Typowe pomyłki

- **AOV około 3 500 zł** — `COUNT(*)` zamiast `COUNT(DISTINCT order_id)`.
- **Kwoty w punkcie 1 wyższe niż 1 427 124,00 w sumie** — fakt zdublowany
  przez duplikat w wymiarze; rekoncyliacja wyłapie to natychmiast.
- **Punkt 2 daje inne miasta** — grupowanie po mieście dostawy
  (`delivery_city` z `silver_orders`) zamiast po mieście klienta z wymiaru.
  Oba są poprawnymi pytaniami biznesowymi, ale odpowiadają na różne rzeczy.
  Druga możliwość: `LIMIT 5` bez `ORDER BY`, co daje pięć dowolnych miast.
- **Wracanie do `silver_order_items` po kwoty** — działa i daje tę samą liczbę,
  ale mija cel ćwiczenia: pytanie o kategorię ma być odpowiedzią z warstwy Gold.
- **AOV wygląda nierealnie wysoko** — 8–9 tysięcy złotych to poprawny wynik.
  Aurora jest sklepem z elektroniką i AGD, nie spożywczym, a koszyk ma średnio
  2,4 pozycji.

---

## W5 — ten sam model na Redshift Serverless

Lokalny `fact_order_item` to jedna pozycja zamówienia (409).

Pełny skrypt (identyczny z `sql/W5_redshift.sql`):

```sql
-- =============================================================================
-- Dzien 2, W5 - ten sam model wymiarowy na Redshift Serverless
-- =============================================================================
-- SILNIK: AMAZON REDSHIFT SERVERLESS, Query Editor v2. NIE Athena - Athena nie
-- zna ani CREATE SCHEMA, ani COPY, ani awsdatacatalog.
--
-- Do podmiany: gold_jk -> gold_<inicjaly>, aurora_jk -> aurora_<inicjaly>,
-- aurora-jk-lake -> wlasny bucket.
--
-- -----------------------------------------------------------------------------
-- TRZY FORMY NAZW - to jest miejsce, w ktorym W5 najczesciej sie wywraca
-- -----------------------------------------------------------------------------
-- 1) gold_<inicjaly>.dim_date                    <- DZIALA, czesc W5B
--    Lokalna tabela Redshifta w bazie `dev`. Dane fizycznie leza w magazynie
--    Redshifta, bo wlozyl je tam COPY. Dwa czlony: schemat . tabela.
--
-- 2) "awsdatacatalog"."aurora_<inicjaly>"."gold_dim_date"   <- DZIALA, W5A
--    TA SAMA warstwa Gold, ale czytana wprost z data lake przez katalog Glue,
--    bez kopiowania czegokolwiek. Trzy czlony: katalog . baza Glue . tabela.
--    awsdatacatalog to zamontowany automatycznie, tylko do odczytu obraz Glue
--    Data Catalog tego konta; aurora_<inicjaly> to nasza baza z Atheny.
--
-- 3) aurora_<inicjaly>.gold_dim_date             <- NIE DZIALA w Redshifcie
--    To jest zapis ATHENOWY. Redshift konczy go bledem
--      ERROR: schema "aurora_jk" does not exist
--    bo szuka wtedy lokalnego schematu o tej nazwie w bazie `dev`. Baza Glue
--    lokalnym schematem nie jest. Lokalnego schematu aurora_<inicjaly> tu NIE
--    tworzymy: przykrylby prawdziwe zrodlo wlasna kopia, ktora od pierwszego
--    przeladowania data lake zaczyna sie z nim rozjezdzac.
--
-- Zrodlem tego bledu jest prawie zawsze przeklejenie zapytania z W4
-- (W4_pytania_gold.sql, plik Athenowy) do zakladki Redshifta.
--
-- WEJSCIE: Query Editor v2 w konsoli AWS (Redshift -> Query editor v2, albo
-- przycisk Query data na stronie workgroupu). Polaczenie jako Federated user,
-- baza `dev`. Wybor Federated user nie jest tu kosmetyka: obraz awsdatacatalog
-- jest widoczny wylacznie przy tym sposobie uwierzytelnienia (na workgroupie
-- Serverless; na klastrze provisioned odpowiednikiem jest "Temporary
-- credentials using your IAM identity").
--
-- Gdy Redshift Data API jest zablokowane polityka organizacji (SCP),
-- AWS CLI, boto3 i skrypty sterujace Redshiftem nie zadzialaja. QEv2 idzie
-- inna droga i dziala normalnie.
--
-- Uwaga o edytorze: kazda zakladka QEv2 ma WLASNA, odizolowana sesje. Przycisk
-- Run wykonuje zaznaczony fragment, a gdy nic nie jest zaznaczone - cala
-- zawartosc zakladki. Przy skrypcie z wielu instrukcji ta roznica ma znaczenie.
--
-- -----------------------------------------------------------------------------
-- UKLAD PLIKU: W5A czyta data lake, W5B kopiuje go do Redshifta
-- -----------------------------------------------------------------------------
-- W5A (ponizej) idzie przez awsdatacatalog. Nie wymaga niczego poza polaczeniem
--     jako Federated user, wiec przechodzi takze wtedy, gdy COPY jest odciete
--     przez uprawnienia. Od niego zaczynamy, zeby nikt nie utknal na starcie.
-- W5B to DDL i COPY: dane trafiaja do magazynu Redshifta i dopiero wtedy widac,
--     czym data warehouse rozni sie od silnika czytajacego pliki. To jest wlasciwa
--     tresc cwiczenia i wymaga uprawnien opisanych w sekcji UPRAWNIENIA na dole.
-- Obie drogi prowadza do tej samej liczby: 1427124.00.
--
-- Dwa warunki, ktore musza byc spelnione, zanim COPY ma szanse przejsc:
--
-- 1) SCHEMAT JEST WLASNY. Cala sala pracuje na jednym workgroupie i jednej
--    bazie `dev`. Przy `CREATE SCHEMA gold` pierwsza osoba go tworzy, druga
--    dostaje "schema already exists", a trzecia wlewa dane do cudzych tabel -
--    bez zadnego bledu, po prostu z podwojonymi liczbami. Stad gold_<inicjaly>,
--    dokladnie jak bucket i baza w Athenie.
--
-- 2) ROLA IAM WORKGROUPU CZYTA TEN BUCKET. To jest krok, na ktorym W5 pada
--    najczesciej - opis bledu i warianty naprawy na dole pliku.
--
-- TYPY musza odpowiadac plikom Parquet z warstwy Gold, bo COPY FORMAT AS PARQUET
-- dopasowuje kolumny po KOLEJNOSCI I TYPIE, nie po nazwie:
--   *_sk, year, month, day -> int              (w Athenie jawny CAST AS integer)
--   kwoty                  -> double precision (w data lake sa double)
--   teksty                 -> varchar(n)       (Parquet trzyma string bez dlugosci)
-- W produkcji kwoty trzymaloby sie jako DECIMAL(12,2), zeby uniknac bledow
-- zaokraglen przy sumowaniu milionow wierszy. Rzutowanie musialoby wtedy nastapic
-- juz w CTAS-ie warstwy Silver, nie dopiero tutaj - bo typ w pliku Parquet jest
-- wczesniejszy niz typ w tabeli docelowej.
--
-- Wszystkie CREATE maja IF NOT EXISTS, wiec DDL mozna uruchomic ponownie.
-- Sam COPY idempotentny NIE JEST - dwa uruchomienia to podwojone wiersze.
-- Przed powtorka: TRUNCATE tabeli.
-- =============================================================================


-- #############################################################################
-- W5A. Warstwa Gold czytana z data lake - bez kopiowania
-- #############################################################################
-- Redshift Serverless montuje katalog Glue tego konta jako baze tylko do
-- odczytu o nazwie awsdatacatalog. Nie trzeba jej tworzyc ani konfigurowac -
-- jest to samo zrodlo, z ktorego czyta Athena, ogladane innym silnikiem.
--
-- Sprawdzenie, czy montowanie jest wlaczone na tym workgroupie:
--   SHOW data_catalog_auto_mount;
-- Wynik `on` oznacza, ze ponizsze zapytania maja szanse dzialac. Przy `off`
-- wlacza sie je jako administrator bazy:
--   ALTER SYSTEM SET data_catalog_auto_mount = on;
-- - z tym zastrzezeniem, ze na Serverless zmiana wchodzi dopiero po uspieniu
-- i wybudzeniu workgroupu, a nie natychmiast.
--
-- Lista widocznych baz Glue i tabel w naszej bazie:
--   SHOW SCHEMAS FROM DATABASE awsdatacatalog;
--   SHOW TABLES FROM SCHEMA awsdatacatalog.aurora_jk;
--
-- Cudzyslowy wokol kazdego czlonu nie sa obowiazkowe, ale sa zalecane: nazwy
-- baz Glue bywaja pisane wielka litera albo z myslnikiem, a wtedy bez
-- cudzyslowow parser Redshifta rozjezdza sie na czlonach.
--
-- Ograniczenia, ktore warto znac od razu:
--   - awsdatacatalog jest TYLKO DO ODCZYTU - zaden INSERT, UPDATE ani CTAS
--     nie zapisze niczego do bazy Glue,
--   - zapis trzyczlonowy jest OBOWIAZKOWY; `USE` i `search_path` dla tej bazy
--     nie dzialaja, wiec skrocic go do dwoch czlonow sie nie da,
--   - wymagane jest polaczenie jako Federated user.

-- To samo pytanie co W4 pkt 1, tyle ze zadane z Redshifta. Kwoty musza byc
-- identyczne jak w Athenie:
--   15.01: Telefony 199197 | Komputery 152821 | TV 142358 | AGD 117153 |
--          Audio 36870 | Akcesoria 5132        -> suma dnia 653531.00
--   16.01: Komputery 209784 | Telefony 182780 | TV 182442 | AGD 143912 |
--          Audio 46384 | Akcesoria 8291        -> suma dnia 773593.00
-- Suma calosci: 1427124.00
SELECT
  d.date_actual,
  p.category,
  ROUND(SUM(f.net_amount_pln), 2) AS revenue_pln
FROM "awsdatacatalog"."aurora_jk"."gold_fact_order_item" f
JOIN "awsdatacatalog"."aurora_jk"."gold_dim_date" d
  ON f.date_sk = d.date_sk
JOIN "awsdatacatalog"."aurora_jk"."gold_dim_product" p
  ON f.product_sk = p.product_sk
GROUP BY d.date_actual, p.category
ORDER BY d.date_actual, revenue_pln DESC;

-- Rekoncyliacja Silver = Gold, takze z data lake. Oczekiwane: 1427124.00
-- w obu wierszach. Warstwa Silver jest w tej samej bazie Glue, wiec sposob
-- adresowania jest ten sam.
SELECT 'silver' AS layer, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM "awsdatacatalog"."aurora_jk"."silver_order_items"
UNION ALL
SELECT 'gold', ROUND(SUM(net_amount_pln), 2)
FROM "awsdatacatalog"."aurora_jk"."gold_fact_order_item"
ORDER BY layer;

-- Wariant posredni, ZAMIAST calego W5B, gdy COPY jest odciety uprawnieniami:
-- Redshift czyta pliki przez katalog Glue i zapisuje wynik do wlasnej tabeli.
-- Dane laduja tam, gdzie mial je polozyc COPY, ale droga do nich idzie
-- tozsamoscia zalogowanego uzytkownika, nie rola workgroupu.
--   CREATE SCHEMA IF NOT EXISTS gold_jk;
--   CREATE TABLE gold_jk.fact_order_item AS
--   SELECT * FROM "awsdatacatalog"."aurora_jk"."gold_fact_order_item";
-- To jest ALTERNATYWA dla DDL i COPY z W5B, nie dodatek: po uruchomieniu obu
-- CREATE TABLE zderzy sie z juz istniejaca tabela. Zastrzezenie merytoryczne:
-- tak zbudowana tabela dziedziczy typy z Parquetu (miedzy innymi bigint tam,
-- gdzie DDL z W5B ma int) i nie ma SORTKEY - wiec zastepuje COPY jako droga
-- do danych, ale nie jako lekcja o typach i rozkladzie danych miedzy wezly.


-- #############################################################################
-- W5B. Warstwa Gold skopiowana do magazynu Redshifta
-- #############################################################################
-- Od tego miejsca wszystkie nazwy sa DWUCZLONOWE i wskazuja LOKALNY schemat
-- w bazie `dev`: gold_<inicjaly>.tabela. To juz nie jest data lake.

CREATE SCHEMA IF NOT EXISTS gold_jk;

-- PRIMARY KEY w Redshifcie jest INFORMACYJNY: optymalizator z niego korzysta,
-- ale silnik go NIE EGZEKWUJE. Duplikat da sie wstawic mimo deklaracji, wiec
-- za spojnosc odpowiadaja zapytania kontrolne na dole pliku.
CREATE TABLE IF NOT EXISTS gold_jk.dim_date (
  date_sk int PRIMARY KEY,
  date_actual date,
  year int,
  month int,
  day int,
  day_name varchar(20),
  is_weekend boolean
);

CREATE TABLE IF NOT EXISTS gold_jk.dim_customer (
  customer_sk int PRIMARY KEY,
  customer_id varchar(16),
  first_name varchar(64),
  last_name varchar(64),
  city varchar(64),
  voivodeship varchar(64),
  segment varchar(32),
  signup_date date,
  valid_from date,
  valid_to date,
  is_current boolean
);

CREATE TABLE IF NOT EXISTS gold_jk.dim_product (
  product_sk int PRIMARY KEY,
  product_id int,
  sku varchar(32),
  product_name varchar(128),
  category varchar(64),
  brand varchar(64),
  list_price_pln double precision
);

CREATE TABLE IF NOT EXISTS gold_jk.dim_channel (
  channel_sk int PRIMARY KEY,
  channel_code varchar(32)
);

-- DISTSTYLE AUTO - Redshift sam decyduje o rozkladzie wierszy miedzy wezly.
--   KEY = rozklad po kolumnie zlaczenia (dobry dla duzych faktow: wiersze faktu
--         i wymiaru o tym samym kluczu trafiaja na ten sam wezel, wiec zlaczenie
--         nie wymaga przesylania danych miedzy wezlami),
--   ALL = kopia calej tabeli na kazdym wezle (dobry dla malych wymiarow),
--   EVEN = rownomiernie, bez ogladania sie na klucze.
-- SORTKEY (date_sk) - fizyczna kolejnosc zapisu na dysku. Filtr po dacie moze
-- wtedy pomijac cale bloki przy odczycie; data jest tu najczesciej filtrowana
-- kolumna. To odpowiednik partycji z Atheny, tylko wewnatrz silnika.
--
-- Tabela ma 9 kolumn, nie 10. Kolumna partycjonujaca dt z Atheny NIE JEST
-- zapisana w pliku Parquet - siedzi w nazwie katalogu (dt=2026-01-15/). To
-- ogolna wlasciwosc partycjonowania w stylu Hive. Dziesiata kolumna dalaby
-- blad o liczbie kolumn przy COPY.
CREATE TABLE IF NOT EXISTS gold_jk.fact_order_item (
  order_id varchar(16),
  date_sk int,
  customer_sk int,
  product_sk int,
  channel_sk int,
  qty int,
  unit_price_pln double precision,
  discount_pln double precision,
  net_amount_pln double precision
)
DISTSTYLE AUTO
SORTKEY (date_sk);

-- ---------------------------------------------------------------------------
-- COPY - ladowanie z S3
-- ---------------------------------------------------------------------------
-- Zrodlem jest warstwa GOLD, nigdy Silver. Silver nie ma surrogate keys, a COPY
-- dopasowuje kolumny po kolejnosci - przy ladowaniu z silver/products/ kolumna
-- product_id wyladowalaby w product_sk. Bez bledu, po prostu z cichym
-- przeklamaniem danych.
--
-- COPY laduje pliki ROWNOLEGLE, na wszystkich wezlach naraz - to jest powod,
-- dla ktorego uzywa sie go zamiast INSERT-ow. Przy 409 wierszach roznicy nie ma;
-- przy 400 milionach roznica to godziny.
--
-- IAM_ROLE default oznacza role oznaczona w namespace jako domyslna. Jesli
-- workgroup ma podpieta wiecej niz jedna role, ARN podaje sie wprost (patrz
-- sekcja o Access Denied).
--
-- LICZBA URUCHOMIEN MA ZNACZENIE: COPY nie jest idempotentny. Drugie wykonanie
-- doklada te same wiersze jeszcze raz.

COPY gold_jk.dim_date
FROM 's3://aurora-jk-lake/gold/dim_date/'
IAM_ROLE default
FORMAT AS PARQUET;

COPY gold_jk.dim_channel
FROM 's3://aurora-jk-lake/gold/dim_channel/'
IAM_ROLE default
FORMAT AS PARQUET;

COPY gold_jk.dim_customer
FROM 's3://aurora-jk-lake/gold/dim_customer/'
IAM_ROLE default
FORMAT AS PARQUET;

COPY gold_jk.dim_product
FROM 's3://aurora-jk-lake/gold/dim_product/'
IAM_ROLE default
FORMAT AS PARQUET;

-- Sciezka bez wskazania partycji - COPY czyta rekurencyjnie oba katalogi
-- dt=2026-01-15/ i dt=2026-01-16/. Sama wartosc dt do tabeli nie wchodzi.
COPY gold_jk.fact_order_item
FROM 's3://aurora-jk-lake/gold/fact_order_item/'
IAM_ROLE default
FORMAT AS PARQUET;

-- ---------------------------------------------------------------------------
-- KONTROLA
-- ---------------------------------------------------------------------------
-- Oczekiwane (kolejnosc z ORDER BY t): dim_channel 3 | dim_customer 80 |
-- dim_date 2 | dim_product 25 | fact_order_item 409 - dokladnie tyle samo
-- co w Athenie.
--
-- Liczby podwojone (6 | 160 | 4 | 50 | 818) oznaczaja COPY uruchomiony dwa razy.
-- Naprawa: TRUNCATE gold_jk.<tabela>; i COPY jeszcze raz. TRUNCATE jest szybszy
-- od DELETE i zwalnia miejsce od razu, ale wykonuje niejawny COMMIT - wewnatrz
-- transakcji zachowuje sie inaczej niz DELETE.
--
-- ORDER BY na koncu, bo UNION ALL - tak samo jak w Athenie - nie gwarantuje
-- kolejnosci wierszy.
SELECT 'dim_date' AS t, COUNT(*) FROM gold_jk.dim_date
UNION ALL SELECT 'dim_channel', COUNT(*) FROM gold_jk.dim_channel
UNION ALL SELECT 'dim_customer', COUNT(*) FROM gold_jk.dim_customer
UNION ALL SELECT 'dim_product', COUNT(*) FROM gold_jk.dim_product
UNION ALL SELECT 'fact_order_item', COUNT(*) FROM gold_jk.fact_order_item
ORDER BY t;

-- To samo pytanie co W4 pkt 1. Kwoty musza byc identyczne jak w Athenie:
--   15.01: Telefony 199197 | Komputery 152821 | TV 142358 | AGD 117153 |
--          Audio 36870 | Akcesoria 5132        -> suma dnia 653531.00
--   16.01: Komputery 209784 | Telefony 182780 | TV 182442 | AGD 143912 |
--          Audio 46384 | Akcesoria 8291        -> suma dnia 773593.00
-- Suma calosci: 1427124.00
--
-- Dwa rozne silniki, dwa rozne modele przechowywania, ta sama liczba. To jest
-- dowod, ze model wymiarowy jest niezalezny od technologii, a wybor silnika jest
-- decyzja o kosztach i wzorcu uzycia, nie o poprawnosci.
SELECT d.date_actual, p.category, ROUND(SUM(f.net_amount_pln), 2) AS revenue_pln
FROM gold_jk.fact_order_item f
JOIN gold_jk.dim_date d ON f.date_sk = d.date_sk
JOIN gold_jk.dim_product p ON f.product_sk = p.product_sk
GROUP BY d.date_actual, p.category
ORDER BY d.date_actual, revenue_pln DESC;

-- ---------------------------------------------------------------------------
-- Gdy COPY zwraca "S3ServiceException: Access Denied"
-- ---------------------------------------------------------------------------
-- To nie jest literowka w sciezce ani brak pliku. Sciezka i uprawnienia to dwie
-- rozne rzeczy i warto sprawdzac je osobno.
--
-- Domyslny workgroup `default-workgroup` ma zwykle podpieta role
-- AmazonRedshift-CommandsAccessRole-*, a ta rola ma polityke
-- AmazonRedshiftAllCommandsFullAccess. Ta polityka daje dostep do S3 WYLACZNIE
-- dla bucketow, ktorych nazwa zawiera "redshift" (albo obiektow z tagiem
-- Redshift=true). Nasz bucket nazywa sie aurora-<inicjaly>-lake, wiec odmowa
-- jest zgodna z polityka - mimo ze bucket lezy w tym samym koncie i regionie.
--
-- Odblokowanie wymaga uprawnien Administratora, wiec jest robione RAZ DLA CALEJ
-- SALI, jednym z trzech sposobow:
--   a) dolozenie AmazonS3ReadOnlyAccess do roli workgroupu - najprostsze,
--   b) wlasny namespace i workgroup z rola majaca dostep do S3 - czystsze,
--      ale wymaga odczekania na provisioning (2-5 minut),
--   c) nazwa bucketu zawierajaca "redshift" (aurora-<inicjaly>-lake-redshift) -
--      brzydkie, ale nie rusza IAM w ogole.
--
-- Z roli PowerUserAccess tego kroku nie da sie wykonac samodzielnie. To jedyne
-- takie miejsce w calym kursie.
--
-- Sprawdzenie uprawnien bez zgadywania (IAM -> Policy simulator albo CLI):
--   aws iam simulate-principal-policy \
--     --policy-source-arn arn:aws:iam::123456789012:role/service-role/<rola> \
--     --action-names s3:GetObject s3:ListBucket \
--     --resource-arns arn:aws:s3:::aurora-jk-lake/gold/dim_date/x.parquet
--
-- Gdyby IAM_ROLE default nie zadzialalo mimo poprawnych uprawnien, ARN roli
-- podaje sie wprost:
--   IAM_ROLE 'arn:aws:iam::123456789012:role/service-role/<rola>'
-- ARN jest widoczny w Redshift -> Namespace -> Security and encryption.
--
-- Alternatywa bez COPY: W5A na poczatku tego pliku. Warto zauwazyc, ze te dwie
-- drogi NIE ida tymi samymi uprawnieniami, mimo ze czytaja te same pliki:
--   COPY                     -> rola IAM podpieta do namespace'u / workgroupu,
--   awsdatacatalog (W5A)     -> tozsamosc zalogowanego uzytkownika federacyjnego.
-- Domyslna rola konsolowa jest ograniczona do bucketow z "redshift" w nazwie, a druga
-- to PowerUserAccess z pelnym odczytem S3 i Glue. Stad obserwacja, ktora wyglada
-- na sprzecznosc, a sprzecznoscia nie jest: COPY zwraca Access Denied, a SELECT
-- przez awsdatacatalog z tego samego bucketu przechodzi.
--
-- Trzecia droga, wymieniona dla porzadku: jawny CREATE EXTERNAL SCHEMA (klasyczny
-- Redshift Spectrum). Ten wariant znow idzie rola IAM, wiec bez dodatkowej polityki S3 podlega temu
-- samemu ograniczeniu co COPY - i dlatego nie jest sciezka domyslna tego cwiczenia.

-- ---------------------------------------------------------------------------
-- UPRAWNIENIA - trzy warstwy, ktore latwo ze soba pomylic
-- ---------------------------------------------------------------------------
-- Kazda z nich moze zawiesc osobno i kazda daje inny komunikat. Sprawdzanie ich
-- w tej kolejnosci oszczedza najwiecej czasu.
--
-- (a) UPRAWNIENIA BAZODANOWE W REDSHIFCIE (GRANT, wykonywany w SQL)
--     Dotycza tego, co wolno roli lub uzytkownikowi WEWNATRZ bazy `dev`.
--       - odczyt katalogu Glue:
--           GRANT USAGE ON DATABASE awsdatacatalog TO "IAMR:<nazwa-roli-SSO>";
--         a gdyby uzytkownik nie istnial jeszcze w bazie:
--           CREATE USER "IAMR:<nazwa-roli-SSO>" WITH PASSWORD DISABLE;
--         Wykonuje to administrator bazy, polaczony jako Database user name
--         and password. Wlasna tozsamosc do wpisania w cudzyslow daje
--           SELECT current_user;
--       - wlasny schemat Gold: jego tworca jest wlascicielem i nie potrzebuje
--         zadnego GRANT-a. Udostepnienie go innej osobie to
--           GRANT USAGE ON SCHEMA gold_<inicjaly> TO "IAMR:<ktos>";
--           GRANT SELECT ON ALL TABLES IN SCHEMA gold_<inicjaly> TO "IAMR:<ktos>";
--     Objaw braku: "permission denied for database awsdatacatalog" albo
--     "permission denied for schema ...". Nie ma tu ani slowa o S3.
--
-- (b) ISTNIENIE OBRAZU KATALOGU GLUE W REDSHIFCIE
--     Dotyczy tego, czy baza awsdatacatalog w ogole jest widoczna.
--       SHOW data_catalog_auto_mount;      -- oczekiwane: on
--       ALTER SYSTEM SET data_catalog_auto_mount = on;   -- gdy off
--     Na Serverless zmiana wchodzi po uspieniu i wybudzeniu workgroupu.
--     Warunek drugi: polaczenie jako Federated user (baza `dev`).
--     Wariant jawny, gdy montowanie jest niedostepne albo gdy zamiast trzech
--     czlonow ma byc zwykly schemat:
--       CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_<inicjaly>
--       FROM DATA CATALOG DATABASE 'aurora_<inicjaly>'
--       IAM_ROLE 'arn:aws:iam::123456789012:role/service-role/<rola>';
--     Uwaga: ten wariant czyta S3 rola IAM, nie tozsamoscia uzytkownika, wiec
--     bez uprawnien S3 na roli workgroupu pada tak samo jak COPY.
--     Objaw braku: ERROR: database "awsdatacatalog" does not exist, albo brak
--     tej pozycji w lewym drzewie QEv2.
--
-- (c) UPRAWNIENIA IAM DO S3 I GLUE
--     Dotycza dostepu do samych plikow i metadanych, poza baza danych.
--     Potrzebne akcje: s3:GetObject i s3:ListBucket na aurora-<inicjaly>-lake
--     oraz glue:GetDatabase, glue:GetDatabases, glue:GetTable, glue:GetTables
--     i glue:GetPartitions.
--     Komu je nadac, zalezy od drogi:
--       droga W5A            -> roli SSO, ktora jest zalogowany kursant
--                               (PowerUserAccess juz je ma),
--       COPY / EXTERNAL SCHEMA -> roli IAM podpietej do namespace'u.
--     Gdyby katalog byl zarzadzany przez Lake Formation, doszlaby jeszcze
--     akcja lakeformation:GetDataAccess i grant LF na baze. W domyslnym trybie
--     IAM_ALLOWED_PRINCIPALS samo IAM wystarcza.
--     Objaw braku: S3ServiceException: Access Denied.

-- ---------------------------------------------------------------------------
-- Inne bledy, ktore moga sie tu pojawic
-- ---------------------------------------------------------------------------
--   ERROR: schema "aurora_jk" does not exist  -> zapytanie Athenowe wklejone do
--                                                Redshifta; forma 3 zamiast 1
--                                                albo 2 (patrz naglowek pliku)
--   ERROR: database "awsdatacatalog" does not
--   exist                                     -> polaczenie inne niz Federated
--                                                user albo wylaczone
--                                                data_catalog_auto_mount
--   "permission denied for database
--   awsdatacatalog"                           -> brak GRANT USAGE, warstwa (a)
--   "Column type mismatch" / blad o int64          -> brak CAST(... AS integer)
--                                                     w W3_elt_athena.sql
--   blad o liczbie kolumn na fact_order_item       -> tabela ma 10 kolumn
--                                                     zamiast 9 (kolumna dt)
--   "schema ... already exists"                    -> schemat bez inicjalow
--   liczby dokladnie dwa razy wieksze              -> COPY uruchomiony dwukrotnie
--   "relation ... does not exist" w lewym drzewie  -> drzewo QEv2 wymaga
--                                                     odswiezenia galezi

-- Po zajeciach: DROP SCHEMA gold_<inicjaly> CASCADE. Workgroup i namespace
-- kasuje osoba, ktora je stawiala - RPU licza sie same, niezaleznie od tego,
-- czy ktos jeszcze pyta.
```

DDL i `COPY`: `sql/W5_redshift.sql`.

### Dwie drogi Redshifta do warstwy Gold

Redshift dochodzi do tych samych plików Parquet na dwa sposoby, a nazwy tabel
różnią się w każdym z nich:

| | W5A — katalog Glue | W5B — `COPY` |
|---|---|---|
| Nazwa | `"awsdatacatalog"."aurora_jk"."gold_fact_order_item"` | `gold_jk.fact_order_item` |
| Gdzie leżą dane w trakcie zapytania | na S3 | w magazynie Redshifta |
| S3 czytane przez | tożsamość zalogowanego użytkownika | rolę IAM namespace'u |
| Wymaga przygotowania | nic poza połączeniem jako Federated user | schemat, DDL, `COPY` |

Trzecia forma, `aurora_jk.gold_fact_order_item`, jest **Athenowa i w Redshifcie
nie działa** — kończy się komunikatem `ERROR: schema "aurora_jk" does not exist`,
bo Redshift szuka wtedy lokalnego schematu w bazie `dev`. Lokalnego schematu
o tej nazwie nie tworzymy: przykryłby prawdziwe źródło kopią, która od pierwszego
przeładowania data lake zaczyna się z nim rozjeżdżać.

Wersja W5A pytania W4 punkt 1:

```sql
SELECT
  d.date_actual,
  p.category,
  ROUND(SUM(f.net_amount_pln), 2) AS revenue_pln
FROM "awsdatacatalog"."aurora_jk"."gold_fact_order_item" f
JOIN "awsdatacatalog"."aurora_jk"."gold_dim_date" d
  ON f.date_sk = d.date_sk
JOIN "awsdatacatalog"."aurora_jk"."gold_dim_product" p
  ON f.product_sk = p.product_sk
GROUP BY d.date_actual, p.category
ORDER BY d.date_actual, revenue_pln DESC;
```

Wynik jest co do grosza taki sam jak w Athenie — bo to dosłownie te same pliki.
`awsdatacatalog` jest tylko do odczytu, a zapis trzyczłonowy obowiązkowy:
`USE` i `search_path` dla tej bazy nie działają.

Która droga jest „tą właściwą”: **W5B**. Sens ćwiczenia leży w typach kolumn,
rozkładzie danych między węzły, `SORTKEY` i braku idempotencji `COPY` — a tego
odczyt przez katalog Glue nie pokazuje w ogóle. W5A jest po to, żeby nikt nie
utknął przed pierwszym wynikiem i żeby różnica między „silnik czyta pliki”
a „silnik trzyma dane u siebie” była widoczna na jednym ekranie.

### Droga wejścia

Na kontach organizacyjnych Redshift Data API bywa zablokowane przez SCP organizacji, więc
AWS CLI, `boto3` i wszystko, co sterowałoby Redshiftem przez Data API, może być niedostępne.
Query Editor v2 łączy się inną drogą i działa — jako **Federated user**, czyli
tożsamością z konsoli AWS. To jest cała autoryzacja, jakiej potrzeba: żadnego
hasła, żadnego sekretu w Secrets Managerze.

Baza danych: `dev`, powstaje razem z namespace'em.

Warto mieć w głowie podział, o którym była mowa w zadaniu: **namespace** to dane
(bazy, użytkownicy, uprawnienia, role IAM, klucz szyfrujący), **workgroup** to moc
(RPU, sieć, dostępność publiczna). Usunięcie workgroupu nie usuwa danych; usunięcie
namespace'u usuwa je razem z nim.

### Schemat per osoba

Cała sala pracuje na jednym workgroupie i jednej bazie `dev`, więc schemat musi być
własny: `gold_<inicjaly>`, dokładnie jak bucket i baza w Athenie.

Przy wspólnym `CREATE SCHEMA gold` pierwsza osoba tworzy schemat, druga dostaje
`schema already exists` — co jest jeszcze łagodnym scenariuszem — a trzecia
wykonuje `COPY` do cudzych tabel. Ten trzeci przypadek **nie zgłasza żadnego
błędu**: dane po prostu się dokładają, liczby przestają się zgadzać i nikt nie wie
dlaczego.

### Typy muszą odpowiadać plikom Parquet

`COPY ... FORMAT AS PARQUET` dopasowuje kolumny **po kolejności i po typie, nigdy
po nazwie**. Stąd trzy konsekwencje w DDL:

| Kolumny | Typ w Redshifcie | Skąd to wynika |
|---|---|---|
| `*_sk`, `year`, `month`, `day` | `int` | w Athenie jawny `CAST(... AS integer)`; bez niego Parquet ma `int64` i `COPY` przerywa |
| kwoty | `double precision` | w data lake są `double`, bo Silver ich nie rzutował na `DECIMAL` |
| teksty | `varchar(n)` | Parquet trzyma `string` bez długości; `n` jest decyzją po stronie Redshiftu |

W produkcji kwoty trzymałoby się jako `DECIMAL(12,2)`, żeby uniknąć błędów
zaokrągleń przy sumowaniu milionów wierszy. Rzutowanie musiałoby wtedy nastąpić
już w CTAS-ie warstwy Silver, a nie tutaj — bo typ w Parquecie jest wcześniejszy
niż typ w tabeli docelowej.

Typy w `sql/W5_redshift.sql` zostały zestawione kolumna po kolumnie z realnymi
plikami Parquet z S3 — zgadzają się wszystkie 34 kolumny pięciu tabel.

### Fakt ma 9 kolumn, nie 10

`gold_fact_order_item` w Athenie jest partycjonowany po `dt`, ale **sam plik
Parquet tej kolumny nie zawiera** — wartość siedzi w nazwie katalogu
(`dt=2026-01-15/`). To ogólna właściwość partycjonowania w stylu Hive: kolumna
partycjonująca jest metadanymi ścieżki, nie danymi pliku.

Dlatego `gold_<inicjaly>.fact_order_item` ma dziewięć kolumn i `COPY` się zgadza.
Dziesiąta kolumna dałaby błąd o liczbie kolumn — i to jest jedyny przypadek
w całym W5, w którym `COPY` zgłasza problem od razu i czytelnie.

Konsekwencja praktyczna: informacja o dniu istnieje w Redshifcie tylko przez
`date_sk` i `dim_date`. To wystarcza, bo model wymiarowy i tak pyta o datę przez
wymiar — ale gdyby `dt` było potrzebne wprost, trzeba by je dołożyć do `SELECT`
w CTAS-ie jako zwykłą kolumnę, obok partycjonującej.

### COPY nie jest idempotentny

`CREATE SCHEMA IF NOT EXISTS` i `CREATE TABLE IF NOT EXISTS` sprawiają, że DDL
da się wykonać dowolną liczbę razy. `COPY` tej właściwości **nie ma**: drugie
uruchomienie dokłada te same wiersze jeszcze raz.

Objaw jest charakterystyczny — wszystkie liczby kontrolne podwojone:

| Tabela | Poprawnie | Po dwóch `COPY` |
|---|---|---|
| `dim_channel` | 3 | 6 |
| `dim_customer` | 80 | 160 |
| `dim_date` | 2 | 4 |
| `dim_product` | 25 | 50 |
| `fact_order_item` | 409 | 818 |

Przychód wychodzi wtedy równo 2 854 248,00, czyli dokładnie dwa razy tyle, ile
powinien. Naprawa: `TRUNCATE` na tabeli i `COPY` raz jeszcze. `TRUNCATE`
w Redshifcie jest szybszy od `DELETE` i zwalnia miejsce od razu, ale wykonuje też
niejawny `COMMIT` — w transakcji zachowuje się inaczej niż `DELETE`.

Ładowanie idempotentne to osobny temat i wraca jutro: wzorzec to `DELETE`
partycji albo okresu przed `COPY`, albo `MERGE` po kluczu.

### Typowy błąd uprawnień przy domyślnej roli workgroupu

Na domyślnej roli konsolowej `COPY` zwróci `S3ServiceException: Access Denied`. To nie jest literówka w ścieżce
ani brak pliku — ścieżka i uprawnienia to dwie różne rzeczy i warto sprawdzać
je osobno.

Przyczyna: rola podpięta do `default-workgroup` (`AmazonRedshift-CommandsAccessRole-*`)
ma politykę `AmazonRedshiftAllCommandsFullAccess`, a ta daje dostęp do S3
**wyłącznie dla bucketów, których nazwa zawiera „redshift”**, albo dla obiektów
z tagiem `Redshift=true`. Nasz bucket to `aurora-<inicjaly>-lake`, więc odmowa
jest zgodna z polityką — mimo że bucket leży w tym samym koncie i regionie.

Odblokowanie wymaga uprawnień Administratora, więc jest robione **raz dla całej
sali**, jednym z trzech sposobów:

1. dołożenie `AmazonS3ReadOnlyAccess` do roli workgroupu — najprostsze, minuta pracy
2. własny namespace i workgroup z rolą mającą dostęp do S3 — czystsze, ale wymaga
   odczekania na provisioning
3. nazwa bucketu zawierająca „redshift” (`aurora-<inicjaly>-lake-redshift`) —
   brzydkie, ale nie rusza IAM w ogóle

To jedyny krok całego kursu, którego nie da się wykonać z roli **PowerUserAccess**.
Warto to wiedzieć, bo oszczędza kwadrans szukania błędu we własnym SQL.

Bloker dotyczy `COPY` i jawnego `CREATE EXTERNAL SCHEMA`, a **nie** dotyczy W5A:
odczyt przez `awsdatacatalog` idzie tożsamością zalogowanego użytkownika, nie rolą
workgroupu. Stąd obserwacja, która wygląda na sprzeczność: `COPY` z bucketu
`aurora-jk-lake` zwraca `Access Denied`, a `SELECT` z tych samych plików przez
`awsdatacatalog` przechodzi.

### Trzy warstwy uprawnień

Rozpoznanie warstwy po komunikacie skraca diagnostykę do minuty — te trzy rzeczy
zawodzą niezależnie od siebie i nie mają ze sobą nic wspólnego poza skutkiem.

| Warstwa | Czego dotyczy | Objaw braku | Kto to naprawia |
|---|---|---|---|
| **(a)** uprawnienia bazodanowe w Redshifcie: `GRANT USAGE ON DATABASE awsdatacatalog`, `GRANT USAGE`/`SELECT` na własnym schemacie | co wolno tożsamości wewnątrz bazy `dev` | `permission denied for database awsdatacatalog`, `permission denied for schema …` | administrator bazy w SQL, połączony hasłem |
| **(b)** obraz katalogu Glue: `data_catalog_auto_mount` włączone, połączenie jako Federated user; alternatywnie jawny `CREATE EXTERNAL SCHEMA … FROM DATA CATALOG` | czy baza `awsdatacatalog` w ogóle istnieje w tym połączeniu | `ERROR: database "awsdatacatalog" does not exist`, brak pozycji w drzewie QEv2 | administrator bazy (`ALTER SYSTEM`) plus właściwy wybór uwierzytelnienia |
| **(c)** IAM: `s3:GetObject`, `s3:ListBucket` na buckecie oraz `glue:GetDatabase(s)`, `glue:GetTable(s)`, `glue:GetPartitions` | dostęp do plików i metadanych poza bazą danych | `S3ServiceException: Access Denied` | administrator konta AWS, na roli SSO albo na roli namespace'u |

Warstwa (c) rozgałęzia się w zależności od drogi: W5A używa tożsamości zalogowanego użytkownika
(`PowerUserAccess` ma te uprawnienia), a `COPY` i `CREATE EXTERNAL SCHEMA` — roli
IAM podpiętej do namespace'u. Gdyby katalog był zarządzany przez Lake Formation,
doszłaby akcja `lakeformation:GetDataAccess` i grant LF na bazę; w domyślnym trybie
`IAM_ALLOWED_PRINCIPALS` samo IAM wystarcza.

Nadanie uprawnień z warstwy (a) wygląda tak:

```sql
-- jako administrator bazy, połączony jako Database user name and password
CREATE USER "IAMR:<rola-federacyjna>" WITH PASSWORD DISABLE;
GRANT USAGE ON DATABASE awsdatacatalog TO "IAMR:<rola-federacyjna>";
```

Dokładną nazwę własnej tożsamości zwraca `SELECT current_user;` uruchomione
w połączeniu federacyjnym.

Sprawdzenie uprawnień bez zgadywania, gdy jest dostęp do IAM
(**IAM → Policy simulator**, albo CLI):

```bash
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::123456789012:role/service-role/<rola> \
  --action-names s3:GetObject s3:ListBucket \
  --resource-arns arn:aws:s3:::aurora-jk-lake/gold/dim_date/x.parquet
```

Gdyby `IAM_ROLE default` nie zadziałało mimo poprawnych uprawnień, ARN roli podaje
się wprost: `IAM_ROLE 'arn:aws:iam::123456789012:role/service-role/<rola>'`.
ARN jest widoczny w **Redshift → Namespace → Security and encryption**.

### Kontrola i wynik końcowy

| Tabela w `gold_<inicjaly>` (kolejność z `ORDER BY t`) | Wierszy |
|---|---|
| `dim_channel` | 3 |
| `dim_customer` | 80 |
| `dim_date` | 2 |
| `dim_product` | 25 |
| `fact_order_item` | 409 |

Zapytanie z W4 punkt 1 daje w Redshifcie **te same kwoty** co w Athenie:
653 531,00 dla 15.01, 773 593,00 dla 16.01, w sumie 1 427 124,00 — co do grosza.

To jest puenta całego dnia. Dwa różne silniki, dwa różne modele przechowywania
(pliki na S3 kontra kolumnowy magazyn Redshiftu), dwa różne dialekty SQL — i ta
sama liczba. Dowodzi to, że **model wymiarowy jest niezależny od technologii**,
a wybór silnika jest decyzją o kosztach i wzorcu użycia, nie o poprawności.

### Redshift czy Athena

| | Athena | Redshift |
|---|---|---|
| Rozliczenie | za zeskanowane bajty | za sekundy pracy RPU |
| Najlepsza do | zapytań ad-hoc, rzadkich skanów data lake, eksploracji | powtarzalnych dashboardów, wielu równoległych odbiorców BI |
| Infrastruktura | brak | namespace + workgroup, kilka minut provisioningu |
| Optymalizacje | partycje, Parquet, kolumnowość | dodatkowo `SORTKEY`, `DISTKEY`, materialized views, cache wyników |
| Dane spoza silnika | wszystko leży na S3 | Redshift Spectrum czyta S3 przez katalog Glue, bez kopiowania |

Kryterium rozstrzygające jest zwykle liczba zapytań na godzinę. Przy kilku
zapytaniach dziennie Athena jest tańsza i nie wymaga niczego. Przy dashboardzie
odświeżanym co 15 minut przez dwudziestu odbiorców opłaca się mieć dane po stronie
silnika, który je trzyma u siebie, sortuje i cache'uje.

Na datasecie Aurory różnicy wydajności nie da się poczuć — 409 wierszy mieści się
w pamięci dowolnego narzędzia. Odczuwalna jest różnica **modelu pracy**: Athena nie
wymaga niczego przed pierwszym zapytaniem, Redshift wymaga DDL, ładowania i decyzji
o rozkładzie danych. To jest uczciwa różnica i nie warto z niej robić rankingu.

---

## Po zajęciach

Warstwa Gold w Athenie i schemat w Redshifcie zostają — dzień 3 buduje na nich SCD
Type 2 i ładowanie przyrostowe.

Sprzątanie po całym kursie: `DROP SCHEMA gold_<inicjaly> CASCADE` w Redshifcie
oraz usunięcie prefiksów w S3. Workgroup i namespace, jeśli powstały na potrzeby
zajęć, kasuje osoba, która je stawiała — RPU liczą się same, niezależnie od tego,
czy ktoś jeszcze pyta.
