# Dzień 3 — rozwiązania

To są rozwiązania wzorcowe do zestawienia z własną pracą — nie jedyne poprawne.
SCD Type 2 i ładowanie przyrostowe mają po kilka sensownych wariantów, a różnica
między nimi jest tematem na dyskusję, nie na poprawianie.

Jedna uwaga o kolejności czytania: przeczytanie tego pliku przed samodzielną próbą
zabiera z ćwiczenia jego najcenniejszą część, czyli moment, w którym coś nie wychodzi
i trzeba dowiedzieć się dlaczego. Sekcje „Typowe pomyłki” istnieją właśnie po to,
żeby ten moment dał się skrócić bez pomijania go.

W przykładach jest prefix `aurora_jk` / `aurora-jk-lake` — do podmiany na własny.

Wszystkie liczby poniżej to **zmierzone wyniki**, nie szacunki. Pochodzą z tych
samych plików, które wgrywamy do S3, i są przeliczone niezależnie od SQL-a —
te same wartości wypisuje `tools/expected_results.py`.

Ten plik jest jedną ścieżką „jak”. Pełny SQL: `sql/W6_scd2_customer.sql`,
`sql/W7_incremental.sql`, `sql/W8_most_lake_model.sql`, `sql/W9_pytania_dyrektora.sql`.

---

## Stan wejściowy

| Tabela z dnia 2 | Wierszy |
|---|---|
| `gold_dim_date` | 2 |
| `gold_dim_channel` | 3 |
| `gold_dim_customer` | 80 |
| `gold_dim_product` | 25 |
| `gold_fact_order_item` | 409 |

Przychód na faktcie: **1 427 124,00**. Ta liczba nie zmieni się dzisiaj ani razu —
dochodzi do niej osobno przychód nowego dnia, ale styczniowe 409 wierszy zostaje
nietkniętych. Każdy wynik W9, w którym suma za 15–16 stycznia jest inna, oznacza,
że po drodze coś się zdublowało albo zgubiło.

---

## W6 — SCD Type 2 na wymiarze klienta

`gold_dim_customer_scd` — jeden wiersz to jedna wersja klienta w przedziale dat (88 = 80 bieżących + 8 historycznych).

Pełny skrypt (identyczny z `sql/W6_scd2_customer.sql`):

```sql
-- =============================================================================
-- Dzien 3, W6 - SCD Type 2 na wymiarze klienta (Athena)
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela. Caly
-- dzien 3 dzieje sie w Athenie; gdyby ktores z tych zapytan mialo pojsc
-- z Query Editora v2 Redshifta, nazwy zmieniaja sie na trzyczlonowe
-- ("awsdatacatalog"."aurora_<inicjaly>"."tabela") - patrz naglowek
-- ../../Dzien2_Data_Warehouse/sql/W5_redshift.sql.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Opis krok po kroku: ../02_rozwiazania.md
--
-- IDEA SCD TYPE 2: zmiana atrybutu nie nadpisuje wiersza. Stara wersja zostaje
-- zamknieta (valid_to = dzien przed zmiana, is_current = false), a obok powstaje
-- nowa z nowym surrogate key. Dzieki temu fakt sprzed zmiany dalej wskazuje stan
-- sprzed zmiany, a raport historyczny nie zmienia sie z dnia na dzien.
--
-- Dla porownania: Type 1 nadpisuje wartosc i historia znika, Type 3 trzyma
-- poprzednia wartosc w dodatkowej kolumnie i pamieta tylko jedna zmiane wstecz.
--
-- gold_dim_customer z dnia 2 ZOSTAJE NIETKNIETY. Budujemy obok nowa tabele,
-- wiec da sie porownac oba warianty i wrocic po pomylce. W produkcji SCD2
-- nadpisuje wymiar w miejscu - ale tam istnieje backup i srodowisko przejsciowe.
--
-- Oba CREATE maja IF NOT EXISTS, wiec caly plik mozna puscic ponownie bez bledu.
-- Zastrzezenie: po DROP TABLE, gdy pliki zostaly na S3, pojawi sie
-- HIVE_PATH_ALREADY_EXISTS - IF NOT EXISTS przed tym NIE chroni.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Krok 1. Tabela zewnetrzna na pliku z aktualizacjami
-- -----------------------------------------------------------------------------
-- Plik ma te same kolumny co customers, plus valid_from z data obowiazywania
-- zmiany. Wszystkie kolumny sa string, lacznie z datami: OpenCSVSerde czyta
-- kazda kolumne jako tekst i dopiero rzutuje na typ zadeklarowany, a dla dat
-- bywa to zawodne. Konwersja na date nastepuje w CTAS-ie ponizej.

CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_customers_updates (
  customer_id string,
  email string,
  first_name string,
  last_name string,
  city string,
  voivodeship string,
  segment string,
  signup_date string,
  valid_from string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/customers_updates/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Oczekiwane: 8 wierszy, valid_from = 2026-01-20 w kazdym.
-- Kazdy customer_id z tej osemki istnieje juz w gold_dim_customer:
--   C0001 Gdansk->Bialystok   C0004 Wroclaw->Katowice   C0009 Krakow->Katowice
--   C0012 Krakow->Bialystok   C0018 Poznan->Gdansk      C0023 Poznan->Wroclaw
--   C0031 Katowice->Gdansk    C0044 Lublin->Warszawa
SELECT COUNT(*) AS updates_n FROM aurora_jk.bronze_customers_updates;

-- -----------------------------------------------------------------------------
-- Krok 2. Wymiar SCD Type 2
-- -----------------------------------------------------------------------------
-- Struktura: dwie czesci sklejone przez UNION ALL.
--
--   base      - wszystkie 80 wierszy z dnia 2, z TYM SAMYM customer_sk.
--               Osmiu zmienionym domykamy okres waznosci (valid_to = 2026-01-19,
--               is_current = false); pozostalym 72 zostaje 9999-12-31 i true.
--               Jeden LEFT JOIN obsluguje obie grupy naraz - roznia sie
--               wylacznie tym, czy zlaczenie trafilo.
--
--   new_rows  - osiem nowych wersji z NOWYM customer_sk (1000 + numer).
--
-- ZACHOWANIE STAREGO SK W WERSJI HISTORYCZNEJ JEST TU NAJWAZNIEJSZE. Fakt
-- z dnia 2 (409 wierszy) wskazuje wlasnie na te klucze i nie jest przeliczany.
-- Nowy klucz w starym wierszu oznaczalby, ze caly styczniowy fakt nie ma
-- do czego trafic.
--
-- KAZDA KOLUMNA JEST KWALIFIKOWANA ALIASEM (d. / u.). Obie tabele maja
-- customer_id, city, segment i valid_from - bez aliasu Athena zwraca
-- "Column 'customer_id' is ambiguous". To regula przy laczeniu wymiaru
-- z jego wlasna aktualizacja, nie literowka.
--
-- WARTOSCI BRZEGOWE ZAMIAST NULL-i:
--   valid_to = 9999-12-31 dla wersji otwartej, a NIE NULL. Zlaczenie faktu
--   z wymiarem to order_date BETWEEN valid_from AND valid_to, a BETWEEN
--   z NULL-em po ktorejkolwiek stronie daje NULL zamiast TRUE i NIE DOPASOWUJE
--   NICZEGO. Wiersze faktu wypadaja wtedy na INNER JOIN po cichu, bez bledu.
--   valid_from = 1900-01-01 w wierszach z dnia 2 dziala z tego samego powodu.

CREATE TABLE IF NOT EXISTS aurora_jk.gold_dim_customer_scd
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/gold/dim_customer_scd/'
) AS
WITH upd AS (
  -- Rzutowanie tekstu na date w jednym miejscu, zeby nizej operowac juz
  -- na typie date - DATE_ADD i BETWEEN na stringu daloby cichy blad.
  SELECT
    u.customer_id,
    u.first_name,
    u.last_name,
    u.city,
    u.voivodeship,
    u.segment,
    CAST(u.signup_date AS date) AS signup_date,
    CAST(u.valid_from AS date)  AS valid_from
  FROM aurora_jk.bronze_customers_updates u
),
base AS (
  -- Wiersze historyczne: te same SK co w dniu 2, wiec stary fakt dalej sie spina.
  --
  -- DATE_ADD('day', -1, u.valid_from) to skladnia Trino: jednostka jako tekst,
  -- liczba (ujemna cofa), data. Wersja zamykana konczy sie DZIEN PRZED
  -- poczatkiem nowej. Przy valid_to = valid_from nowej wersji zapytanie
  -- o 20 stycznia dopasowaloby OBIE wersje naraz i podwoilo przychod.
  --
  -- COALESCE zwraca pierwsza wartosc rozna od NULL, czyli tutaj: 9999-12-31
  -- wszedzie tam, gdzie LEFT JOIN nie znalazl aktualizacji.
  SELECT
    d.customer_sk,
    d.customer_id,
    d.first_name,
    d.last_name,
    d.city,
    d.voivodeship,
    d.segment,
    d.signup_date,
    d.valid_from,
    COALESCE(DATE_ADD('day', -1, u.valid_from), DATE '9999-12-31') AS valid_to,
    CASE WHEN u.customer_id IS NULL THEN true ELSE false END       AS is_current
  FROM aurora_jk.gold_dim_customer d
  LEFT JOIN upd u
    ON d.customer_id = u.customer_id
),
new_rows AS (
  -- Nowe wersje osemki. Przesuniecie o 1000 to najtanszy sposob na unikniecie
  -- kolizji: stare klucze zajmuja 1-80, nowe ida od 1001 do 1008.
  --
  -- CAST(... AS integer) jest obowiazkowy: ROW_NUMBER() zwraca bigint, Parquet
  -- zapisze to jako int64, a COPY do kolumny int w Redshifcie przerwie cale
  -- ladowanie na niezgodnosci typow. Ta sama pulapka co w ELT dnia 2.
  SELECT
    CAST(1000 + ROW_NUMBER() OVER (ORDER BY u.customer_id) AS integer) AS customer_sk,
    u.customer_id,
    u.first_name,
    u.last_name,
    u.city,
    u.voivodeship,
    u.segment,
    u.signup_date,
    u.valid_from,
    DATE '9999-12-31' AS valid_to,
    true              AS is_current
  FROM upd u
)
-- UNION ALL, nie UNION: UNION usuwalby duplikaty i kosztowal sortowanie,
-- a tutaj z zalozenia zaden wiersz nie powtarza sie miedzy czesciami.
SELECT * FROM base
UNION ALL
SELECT * FROM new_rows;

-- =============================================================================
-- KONTROLA
-- =============================================================================

-- Weryfikacja 1. Oczekiwane: dokladnie 8 customer_id, kazdy w 2 wersjach.
-- Wiecej niz 8 oznacza, ze czesc base weszla dwa razy.
SELECT customer_id, COUNT(*) AS wersje
FROM aurora_jk.gold_dim_customer_scd
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY customer_id;

-- Weryfikacja 2. Oczekiwane: rows_all = 88 (80 + 8), rows_current = 80.
-- Kazdy klient ma dokladnie jedna wersje aktualna, takze ta osemka.
-- rows_current = 88 oznacza, ze zamykanie starej wersji nie trafilo -
-- najczesciej INNER JOIN zamiast LEFT JOIN w czesci base.
SELECT COUNT(*) AS rows_all,
       COUNT_IF(is_current) AS rows_current
FROM aurora_jk.gold_dim_customer_scd;

-- Weryfikacja 3. Oczekiwane: ZERO wierszy.
-- Zapytanie szuka par wersji tego samego klienta, ktorych okresy waznosci
-- na siebie zachodza. Warunki a.valid_from <= b.valid_to AND b.valid_from <=
-- a.valid_to to standardowy test przeciecia dwoch przedzialow.
-- Gdyby cos zwrocilo, ten sam wiersz faktu pasowalby do dwoch wersji wymiaru
-- i przychod policzylby sie podwojnie - przy liczbie wierszy faktu
-- wygladajacej calkowicie normalnie.
SELECT a.customer_id
FROM aurora_jk.gold_dim_customer_scd a
JOIN aurora_jk.gold_dim_customer_scd b
  ON a.customer_id = b.customer_id
 AND a.customer_sk <> b.customer_sk
 AND a.valid_from <= b.valid_to
 AND b.valid_from <= a.valid_to;

-- Weryfikacja 4. Podglad obu wersji jednego klienta - najkrotsza ilustracja
-- tego, co robi SCD Type 2.
-- Oczekiwane dla C0001: sk = 1 (Gdansk, do 2026-01-19, is_current = false)
-- oraz sk = 1001 (Bialystok, od 2026-01-20, is_current = true).
SELECT customer_sk, customer_id, city, segment, valid_from, valid_to, is_current
FROM aurora_jk.gold_dim_customer_scd
WHERE customer_id = 'C0001'
ORDER BY valid_from;
```

Pełny skrypt: `sql/W6_scd2_customer.sql`.

### Krok 1 — tabela na pliku updates

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_customers_updates (
  customer_id string,
  email string,
  first_name string,
  last_name string,
  city string,
  voivodeship string,
  segment string,
  signup_date string,
  valid_from string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/customers_updates/'
TBLPROPERTIES ('skip.header.line.count'='1');
```

Wszystkie kolumny są `string`, łącznie z `valid_from` i `signup_date` — to ten
sam powód co w Bronze dnia 1: `OpenCSVSerde` czyta każdą kolumnę jako tekst
i dopiero rzutuje na typ zadeklarowany, a dla dat bywa to zawodne. Konwersja
na `date` następuje dopiero w CTAS-ie budującym wymiar, gdzie kontrolujemy format.

**Oczekiwane: 8 wierszy.** Zawartość pliku, dla orientacji przy sprawdzaniu
wyników:

| `customer_id` | miasto przed | miasto po | segment przed | segment po |
|---|---|---|---|---|
| C0001 | Gdańsk | Białystok | standard | vip |
| C0004 | Wrocław | Katowice | standard | vip |
| C0009 | Kraków | Katowice | vip | premium |
| C0012 | Kraków | Białystok | standard | vip |
| C0018 | Poznań | Gdańsk | standard | vip |
| C0023 | Poznań | Wrocław | standard | vip |
| C0031 | Katowice | Gdańsk | vip | premium |
| C0044 | Lublin | Warszawa | premium | vip |

`valid_from` we wszystkich ośmiu wierszach to `2026-01-20`.

### Krok 2 — wymiar SCD2

```sql
CREATE TABLE IF NOT EXISTS aurora_jk.gold_dim_customer_scd
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/gold/dim_customer_scd/'
) AS
WITH upd AS (
  SELECT
    u.customer_id,
    u.first_name,
    u.last_name,
    u.city,
    u.voivodeship,
    u.segment,
    CAST(u.signup_date AS date) AS signup_date,
    CAST(u.valid_from AS date)  AS valid_from
  FROM aurora_jk.bronze_customers_updates u
),
base AS (
  SELECT
    d.customer_sk,
    d.customer_id,
    d.first_name,
    d.last_name,
    d.city,
    d.voivodeship,
    d.segment,
    d.signup_date,
    d.valid_from,
    COALESCE(DATE_ADD('day', -1, u.valid_from), DATE '9999-12-31') AS valid_to,
    CASE WHEN u.customer_id IS NULL THEN true ELSE false END       AS is_current
  FROM aurora_jk.gold_dim_customer d
  LEFT JOIN upd u ON d.customer_id = u.customer_id
),
new_rows AS (
  SELECT
    CAST(1000 + ROW_NUMBER() OVER (ORDER BY u.customer_id) AS integer) AS customer_sk,
    u.customer_id,
    u.first_name,
    u.last_name,
    u.city,
    u.voivodeship,
    u.segment,
    u.signup_date,
    u.valid_from,
    DATE '9999-12-31' AS valid_to,
    true              AS is_current
  FROM upd u
)
SELECT * FROM base
UNION ALL
SELECT * FROM new_rows;
```

### Dlaczego `LEFT JOIN`, a nie dwa osobne zapytania

`base` obsługuje **obie** grupy historyczne naraz: 72 klientów bez zmian i 8 ze
zmianą. Różnią się wyłącznie tym, czy złączenie do `upd` trafiło, a to widać
w dwóch kolumnach:

| Kolumna | Klient bez zmian (`u.customer_id IS NULL`) | Klient ze zmianą |
|---|---|---|
| `valid_to` | `COALESCE(..., DATE '9999-12-31')` → 9999-12-31 | `DATE_ADD('day', -1, u.valid_from)` → 2026-01-19 |
| `is_current` | `true` | `false` |

`DATE_ADD('day', -1, ...)` to składnia Trino — pierwszy argument to jednostka
podana jako tekst, drugi to liczba (ujemna cofa), trzeci to data. Wersja zamykana
kończy się **dzień przed** początkiem nowej, a nie tego samego dnia: przy
`valid_to = 2026-01-20` zapytanie o 20 stycznia dopasowałoby obie wersje naraz
i podwoiło przychód nowego dnia.

`COALESCE` wybiera pierwszą wartość różną od `NULL`. Tutaj zastępuje brak
dopasowania w `LEFT JOIN` datą brzegową — i to jest miejsce, w którym `NULL`
zamieniamy na `9999-12-31`, zamiast zostawiać go dla „wersji otwartej”.

### Stary `customer_sk` zostaje, nowy zaczyna się od 1001

W `base` klucz pochodzi z `d.customer_sk`, czyli z wymiaru dnia 2 — nietknięty,
łącznie z wierszami zamykanymi. Dzięki temu 409 wierszy starego faktu nadal
wskazuje na wersję sprzed przeprowadzki i raport za styczeń pokazuje to, co
pokazywał wczoraj.

W `new_rows` klucz to `1000 + ROW_NUMBER()`. Przesunięcie o tysiąc jest tanim
sposobem na uniknięcie kolizji: stare klucze zajmują 1–80, nowe idą od 1001
do 1008. `CAST(... AS integer)` jest obowiązkowy z tego samego powodu co
w dniu 2 — `ROW_NUMBER()` zwraca `bigint`, a to zapisuje się do Parquetu jako
`int64` i przerywa `COPY` do kolumny `int` w Redshifcie.

W produkcji rolę tego przesunięcia pełni sekwencja albo `IDENTITY` w bazie,
właśnie po to, żeby klucz raz nadany nie mógł się powtórzyć nawet po
przeliczeniu wymiaru od zera.

### Pułapka: `Column 'customer_id' is ambiguous`

`gold_dim_customer` i `bronze_customers_updates` mają te same nazwy kolumn
(`customer_id`, `city`, `segment`, `valid_from`). Bez aliasu Athena przerywa
zapytanie tym komunikatem. W skrypcie każda kolumna jest kwalifikowana — `d.`
dla wymiaru, `u.` dla updates — i to jest reguła przy łączeniu wymiaru z jego
własną aktualizacją, a nie kwestia stylu.

### Co ma wyjść po W6

| Zapytanie | Wynik |
|---|---|
| `COUNT(*)` | **88** (80 + 8) |
| `COUNT_IF(is_current)` | **80** |
| `customer_id` z dwiema wersjami | **8**: C0001, C0004, C0009, C0012, C0018, C0023, C0031, C0044 |
| test zachodzących okresów ważności | **0 wierszy** |

```sql
-- weryfikacja 1: dokładnie 8 identyfikatorów, każdy w dwóch wersjach
SELECT customer_id, COUNT(*) AS wersje
FROM aurora_jk.gold_dim_customer_scd
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY customer_id;

-- weryfikacja 2: rows_all = 88, rows_current = 80
SELECT COUNT(*) AS rows_all, COUNT_IF(is_current) AS rows_current
FROM aurora_jk.gold_dim_customer_scd;

-- weryfikacja 3: ZERO wierszy — żadne dwie wersje tego samego klienta
-- nie mają zachodzących na siebie okresów ważności
SELECT a.customer_id
FROM aurora_jk.gold_dim_customer_scd a
JOIN aurora_jk.gold_dim_customer_scd b
  ON a.customer_id = b.customer_id
 AND a.customer_sk <> b.customer_sk
 AND a.valid_from <= b.valid_to
 AND b.valid_from <= a.valid_to;
```

Trzecia weryfikacja jest najmniej oczywista i najbardziej wartościowa. Warunki
`a.valid_from <= b.valid_to AND b.valid_from <= a.valid_to` to standardowy test
przecięcia dwóch przedziałów. Gdyby cokolwiek zwrócił, ten sam wiersz faktu
pasowałby do dwóch wersji wymiaru — a przychód policzyłby się podwójnie, przy
liczbie wierszy faktu wyglądającej całkowicie normalnie.

### Odpowiedź na pytanie z W6

Fakt z 15 stycznia ma wskazywać na **stary** `customer_sk`.

Uzasadnienie idzie przez raport. „Przychód według miast za styczeń” wysłany
31 stycznia pokazuje Kraków z kwotą 170 570,00. Ten sam raport otwarty w marcu,
po przeprowadzce dwóch krakowian, przy nowym kluczu na faktcie pokazałby
130 032,00 — i nikt nie umiałby wyjaśnić, dlaczego przeszłość się zmieniła.

To jest cały sens SCD Type 2: **fakt zapisuje stan świata z momentu zdarzenia,
a nie stan świata z momentu zapytania.** Wariant „według dzisiejszej struktury”
też jest potrzebny i też będzie dostępny — ale jako świadomy wybór w zapytaniu
(W9 punkt 2), a nie jako skutek uboczny nadpisania wymiaru.

### Typowe pomyłki

- **88 wierszy, ale 88 z `is_current = true`.** Zamykanie starej wersji poszło
  po warunku, który nie trafił — najczęściej `INNER JOIN` zamiast `LEFT JOIN`
  w `base`, przez co ósemka wypada z wyniku i pojawia się tylko raz.
- **96 wierszy zamiast 88.** `base` zbudowane z `UNION ALL` dwóch zapytań, z których
  drugie nie odfiltrowało klientów bez zmian — ósemka wchodzi trzy razy.
- **Nowe klucze kolidują ze starymi.** `ROW_NUMBER()` bez przesunięcia daje 1–8,
  czyli klucze zajęte przez klientów C0001–C0008. Fakt złączony z takim wymiarem
  przypisze styczniową sprzedaż do przypadkowych osób, nie zgłaszając błędu.
- **`valid_to = valid_from` nowej wersji.** Wersje zachodzą na siebie jednym dniem
  i weryfikacja 3 zwraca 8 wierszy. Przychód 20 stycznia wychodzi wtedy podwójny.
- **`valid_to` jako `NULL` dla wersji otwartej.** Nic nie pęka w W6 — pęka
  dopiero w W7, gdzie `BETWEEN` przestaje dopasowywać cokolwiek i fakt nowego dnia
  wychodzi pusty.

---

## W7 — ładowanie przyrostowe dnia 2026-01-20

`gold_fact_order_item_20260120` — jedna pozycja z 20.01 (40). `v_fact_order_item_all` składa 409 + 40 przez UNION ALL, bo stary fakt zostaje nietknięty.

Pełny skrypt (identyczny z `sql/W7_incremental.sql`):

```sql
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
```

Pełny skrypt: `sql/W7_incremental.sql`.

### Bronze

```sql
MSCK REPAIR TABLE aurora_jk.bronze_orders;

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
```

Zamówienia wchodzą jako **partycja** do istniejącej tabeli, bo `bronze_orders`
jest partycjonowane po `dt` i `MSCK REPAIR` wystarcza do jej zarejestrowania.
Pozycje wchodzą jako **osobna tabela**, bo `bronze_order_items` partycjonowane
nie jest.

Po tym kroku: `bronze_orders` ma **240** wierszy w trzech partycjach,
`bronze_order_items` nadal **486**, a `bronze_order_items_20260120` — **40**.

Gdyby pozycje nowego dnia trafiły do `bronze/order_items/`, `bronze_order_items`
zwracałoby **526** i liczby kontrolne dnia 1 przestałyby się zgadzać: 409 wierszy
Silver, 409 wierszy faktu, 1 427 124,00 przychodu. Nic by przy tym nie zgłosiło
błędu — po prostu wszystkie wcześniejsze wyniki stałyby się nieodtwarzalne.

### Wymiar daty

```sql
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
```

Tabela zewnętrzna nie przyjmuje `INSERT`-a, więc powstaje **nowa wersja wymiaru**,
a stara zostaje. To niezgrabne i jest tego świadomym kosztem: wymiar daty
w produkcji generuje się raz, na kilka lat w przód, właśnie po to, żeby nigdy
nie musieć go dokładać.

`CAST('Tuesday' AS varchar)` nie jest ozdobnikiem. Literał tekstowy w Trino ma typ
`char(7)`, a `day_name` w `gold_dim_date` pochodzi z `date_format()` i jest
`varchar`. Bez rzutowania `UNION ALL` przerywa na niezgodności typów kolumny.

**Oczekiwane: 3 wiersze**, `date_sk` = 20260115, 20260116, 20260120.

### Silver i fakt nowego dnia

```sql
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
```

Filtr `dt = '2026-01-20'` robi dwie rzeczy naraz: wyznacza zakres przeliczenia
i pozwala Athenie w ogóle nie otwierać katalogów z 15 i 16 stycznia
(*partition pruning*). Dni 15 i 16 nie są ruszane — to jest cała idea ładowania
przyrostowego.

**Oczekiwane: 40 wierszy.** Wszystkie zamówienia z 20 stycznia mają status
`paid` albo `shipped`, więc filtr statusu tym razem niczego nie odrzuca —
inaczej niż w dniu 1, gdzie 30 zamówień było anulowanych.

```sql
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
JOIN aurora_jk.silver_orders_20260120 o ON i.order_id = o.order_id
JOIN aurora_jk.gold_dim_date_with_20 d  ON o.order_date = d.date_actual
JOIN aurora_jk.gold_dim_customer_scd c
  ON o.customer_id = c.customer_id
 AND o.order_date BETWEEN c.valid_from AND c.valid_to
JOIN aurora_jk.gold_dim_product p  ON i.product_id = p.product_id
JOIN aurora_jk.gold_dim_channel ch ON o.channel    = ch.channel_code;
```

Kolejność kolumn jest identyczna jak w `gold_fact_order_item` z dnia 2 — inaczej
widok sklejający obie tabele przez `UNION ALL` albo się nie utworzy, albo, gorzej,
utworzy się z pomieszanymi kolumnami tego samego typu.

Tabela nowego dnia nie jest partycjonowana. Przy jednym dniu partycja nic nie
daje, a `dt` zostaje zwykłą kolumną w pliku Parquet — dzięki czemu `UNION ALL`
z tabelą partycjonowaną nadal się zgadza pod względem liczby kolumn.

### Trzy złączenia warte omówienia

**`gold_dim_date_with_20`, nie `gold_dim_date`.** To jest miejsce, w którym
najczęściej ginie cały nowy dzień. Fakt złączony ze starym wymiarem daty
zwraca **0 wierszy** — `INNER JOIN` nie znajduje 20 stycznia i wycina wszystko,
bez błędu i bez ostrzeżenia. Objaw jest mylący, bo tabela powstaje poprawnie,
tylko jest pusta.

**Wymiar SCD2 po dwóch warunkach naraz:**

```sql
ON o.customer_id = c.customer_id
AND o.order_date BETWEEN c.valid_from AND c.valid_to
```

Nie po `is_current`. Dla 20 stycznia obie drogi dają ten sam wynik, bo nowe
wersje są zarazem aktualne — ale tylko pierwsza zadziała przy przeliczaniu
starszego dnia. Złączenie po `is_current` przypisałoby styczniowe zamówienia
do dzisiejszych miast, czyli zrobiłoby dokładnie to, przed czym SCD2 chroni.

I tu wraca decyzja z dnia 2: `valid_from = DATE '1900-01-01'`, a nie `NULL`.
`BETWEEN` z `NULL`-em daje `NULL`, nie `TRUE`, i nie dopasowuje niczego —
72 klientów bez zmian wypadłoby z faktu po cichu. Fakt krótszy niż 40 wierszy
to pierwsze miejsce do sprawdzenia.

**`silver_orders_20260120` w środku złączenia.** Pozycje nie mają ani daty, ani
kanału, ani klienta — wszystko to jest atrybutem zamówienia. To ten sam przeskok
przez warstwę Silver, który w dniu 2 był potrzebny do `channel_code`.

### Widok całości

```sql
CREATE OR REPLACE VIEW aurora_jk.v_fact_order_item_all AS
SELECT * FROM aurora_jk.gold_fact_order_item
UNION ALL
SELECT * FROM aurora_jk.gold_fact_order_item_20260120;
```

Widok nic nie kosztuje w storage — to zapisane zapytanie, nie dane. `CREATE OR
REPLACE` można wykonywać wielokrotnie, więc ten fragment skryptu jest idempotentny
sam z siebie.

### Co ma wyjść po W7

| Co | Wynik |
|---|---|
| `silver_orders_20260120` | **40** |
| `gold_fact_order_item_20260120` | **40** wierszy, przychód **101 546,00** |
| `v_fact_order_item_all` wg `dt` | 2026-01-15, 2026-01-16, 2026-01-20 = 40 |
| `v_fact_order_item_all` razem | 409 + 40 = **449** |
| `gold_fact_order_item` | nadal **409** |
| przychód 15–16 stycznia | nadal **1 427 124,00** |

Czterdzieści pozycji na czterdzieści zamówień: nowy dzień ma dokładnie jedną
pozycję na zamówienie, więc grain faktu jest tu tożsamy z grain zamówienia.
W dniu 1 tak nie było — 409 pozycji przypadało na 170 zamówień.

### Idempotencja — dwa warianty i realny koszt każdego

Wszystkie CTAS-y w skrypcie mają `IF NOT EXISTS`, więc **cały plik da się puścić
ponownie bez błędu**. To jednak nie jest to samo co przeliczenie danych: przy
drugim uruchomieniu instrukcje są po prostu pomijane.

Realne przeliczenie dnia w wariancie z tabelą zewnętrzną wymaga trzech rzeczy
po kolei:

1. `DROP TABLE aurora_jk.gold_fact_order_item_20260120;`
2. usunięcie prefiksu `gold/fact_order_item_20260120/` w konsoli S3
3. ponowny CTAS

Krok 2 jest tym, o którym się zapomina. Athena z założenia nigdy nie kasuje
danych z S3, więc po samym `DROP` ponowny CTAS kończy się
`HIVE_PATH_ALREADY_EXISTS` — i `IF NOT EXISTS` przed tym **nie chroni**, bo
tabeli w katalogu już nie ma.

To jest idempotencja na piechotę i warto ją nazwać po imieniu, zamiast udawać,
że katalog plików na S3 potrafi `MERGE`.

### Wariant Iceberg

Wymaga tabeli `silver_orders_iceberg` z zadania dodatkowego S1 dnia 1 — w Athenie
engine v3 przechodzi bez problemów.

```sql
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
```

**Jak czytać `MERGE`.** Instrukcja przechodzi po wierszach źródła (`USING`)
i dla każdego sprawdza warunek `ON`:

- wiersz **pasuje** do istniejącego w tabeli docelowej → gałąź `WHEN MATCHED`,
  czyli `UPDATE` wymienionych kolumn
- wiersz **nie pasuje** → gałąź `WHEN NOT MATCHED`, czyli `INSERT`

Pierwsze uruchomienie: 170 wierszy + 40 nowych = **210**, bo żadne `order_id`
z 20 stycznia nie istnieje jeszcze w tabeli docelowej i wszystkie idą gałęzią
`WHEN NOT MATCHED`. Drugie uruchomienie **tej samej instrukcji**: nadal **210**,
bo teraz wszystkie czterdzieści wierszy trafia w `WHEN MATCHED` i tylko nadpisuje
`status` oraz `channel` tymi samymi wartościami.

To jest cała różnica między katalogiem plików na S3 a formatem tabelarycznym.
Powtórzenie CTAS-a wyżej wymagało `DROP TABLE` i wejścia do konsoli S3;
powtórzenie `MERGE` wymaga naciśnięcia *Run* jeszcze raz.

**Skrót `INSERT *` tu nie zadziała.** Składnia `WHEN NOT MATCHED THEN INSERT *`
pochodzi z Delta Lake i Sparka. Athena engine v3 odpowiada:
`mismatched input '*'. Expecting: '(', 'VALUES'`. Sprawdzone w Athenie v3 — kolumny
trzeba wypisać w obu listach.

```sql
-- oczekiwane: 210, i 210 także po drugim uruchomieniu MERGE
SELECT COUNT(*) AS rows_n FROM aurora_jk.silver_orders_iceberg;

-- historia zmian: po CTAS z dnia 1 jeden snapshot 'append',
-- po MERGE dochodzi drugi, 'overwrite'
SELECT committed_at, operation
FROM "aurora_jk"."silver_orders_iceberg$snapshots"
ORDER BY committed_at;
```

Stary snapshot zostaje dostępny i to jest cały mechanizm *time travel*:
Iceberg nie nadpisuje plików, tylko zapisuje nowe i przestawia wskaźnik.

### Typowe pomyłki

- **Fakt nowego dnia ma 0 wierszy.** Złączenie poszło do `gold_dim_date` zamiast
  do `gold_dim_date_with_20`. Objaw jest mylący, bo tabela powstaje poprawnie.
- **Fakt ma mniej niż 40 wierszy.** `NULL` w `valid_from` albo `valid_to` wymiaru
  SCD2. Diagnostyka: zamiana złączenia do wymiaru klienta na `LEFT JOIN`
  i policzenie `NULL`-i w `customer_sk`.
- **Fakt ma więcej niż 40 wierszy.** Okresy ważności w wymiarze zachodzą na siebie
  — weryfikacja 3 z W6 zwróciłaby wtedy niepustą listę.
- **`bronze_order_items` zwraca 526.** Pliki nowego dnia trafiły do starego
  prefiksu. Naprawa: usunięcie ich z `bronze/order_items/` i ponowny upload
  do `bronze/order_items_20260120/`.
- **`HIVE_PATH_ALREADY_EXISTS` przy powtórzeniu CTAS.** Tabela skasowana przez
  `DROP`, pliki zostały na S3. Naprawa opisana wyżej, w sekcji o idempotencji.
- **`mismatched input '*'`.** `INSERT *` w `MERGE` — składnia z Delta Lake.

---

## W8 — most między data lake a modelem wymiarowym

Zapytanie nic nie tworzy. LEFT JOIN zdarzeń do faktu, żeby kategorie bez sprzedaży nie zniknęły.

Pełny skrypt (identyczny z `sql/W8_most_lake_model.sql`):

```sql
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
```

Pełny skrypt W9 jest w następnym bloku (`sql/W9_pytania_dyrektora.sql`).

```sql
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
```

### Co ma wyjść

| Kategoria | Koszyki | Przychód PLN |
|---|---|---|
| Akcesoria | 39 | 13 423,00 |
| AGD | 38 | 261 065,00 |
| Telefony | 30 | 381 977,00 |
| Audio | 28 | 83 254,00 |
| Komputery | 25 | 362 605,00 |
| TV | 24 | 324 800,00 |

Suma kolumny z koszykami to **184** — dokładnie tyle, ile zdarzeń `add_to_cart`
jest w `bronze_events`. Suma przychodu to **1 427 124,00**, czyli ta sama liczba
co zawsze. Obie sumy są dobrymi testami: rozjazd znaczy, że któryś `JOIN`
zdublował albo zgubił wiersze.

### Odpowiedź biznesowa

**Akcesoria.** Najwięcej koszyków w całym zestawieniu i przychód o rząd wielkości
niższy niż Telefony, które mają koszyków o dziewięć mniej. Ktoś dorzuca etui
za 49 zł i nie finalizuje zamówienia.

Warto się przy tym zatrzymać, bo odpowiedź nie jest jednoznaczna: to może być
problem konwersji, ale równie dobrze naturalna właściwość taniego koszyka —
akcesoria dokłada się impulsywnie i równie impulsywnie usuwa. Obie interpretacje
są dobre, jeśli padnie za nimi uzasadnienie, a rozstrzygnięcie wymagałoby danych,
których nie mamy: liczby wyświetleń i wartości koszyka w momencie porzucenia.

### Dlaczego `LEFT JOIN`, a nie `INNER JOIN`

Po stronie zdarzeń są produkty, których nikt nie kupił. Przy `INNER JOIN`
zniknęłyby razem z informacją o porzuconych koszykach — a to właśnie one są
odpowiedzią na zadane pytanie. `INNER JOIN` dałby tu wynik, który wygląda
poprawnie i po cichu pomija te wiersze, które najbardziej interesują biznes.

Drugie `LEFT JOIN`, do `silver_products`, dostarcza kategorię dla produktów
nieobecnych w faktcie. Bez niego takie wiersze miałyby `category = NULL`
i wpadłyby do osobnej grupy w `GROUP BY`. `COALESCE(r.category, p.category)`
bierze kategorię stamtąd, skąd jest dostępna.

`COALESCE(r.revenue_pln, 0)` zamienia brak sprzedaży na zero. Bez tego `SUM`
pomijałby `NULL`-e i kategoria bez ani jednego zakupu pokazałaby pusty przychód
zamiast zera — czyli brak danych zamiast informacji.

### Dlaczego to jest lakehouse

W jednym zapytaniu:

- `bronze_events` — JSON Lines, półstrukturalny, schemat nałożony przy odczycie
- `gold_fact_order_item` i `gold_dim_product` — Parquet, kolumnowy, schemat
  ustalony przy zapisie

Jeden silnik, jeden dialekt SQL, zero kopiowania danych między systemami.
W klasycznym data warehouse zdarzenia trzeba by najpierw zamodelować i załadować —
a nikt by tego nie zrobił, bo w momencie zbierania logów pytanie o porzucone
koszyki jeszcze nie istniało.

Wariant dwusilnikowy wygląda tak samo z punktu widzenia SQL: fakt lokalnie
w schemacie `gold_<inicjaly>`, zdarzenia z data lake. Zmienia się tylko sposób
adresowania tabel katalogowych — trzy człony zamiast dwóch:

```sql
FROM "awsdatacatalog"."aurora_jk"."bronze_events" e
```

Ta droga (automatyczne zamontowanie katalogu Glue, część W5A z dnia 2) czyta S3
tożsamością zalogowanego użytkownika, więc przechodzi bezpośrednio z roli
`PowerUserAccess`. Klasyczny Redshift Spectrum, czyli jawny
`CREATE EXTERNAL SCHEMA … FROM DATA CATALOG … IAM_ROLE …`, czyta S3 rolą IAM
namespace'u i podlega temu samemu blokerowi, który w W5 zatrzymywał `COPY`.

---

## W9 — zadanie końcowe

As-was idzie po customer_sk zapisanym w fakcie. As-is dogania bieżącą wersję po customer_id. Suma obu to 1 427 124 zł — zmienia się miasto, nie przychód.

Pełny skrypt (identyczny z `sql/W9_pytania_dyrektora.sql`):

```sql
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
```

### Punkt 1 — miasto w momencie zakupu (as-was)

```sql
SELECT
  c.city,
  ROUND(SUM(f.net_amount_pln), 2) AS revenue_as_was
FROM aurora_jk.gold_fact_order_item f
JOIN aurora_jk.gold_dim_customer_scd c ON f.customer_sk = c.customer_sk
JOIN aurora_jk.gold_dim_date d         ON f.date_sk     = d.date_sk
WHERE d.date_actual BETWEEN DATE '2026-01-15' AND DATE '2026-01-16'
GROUP BY c.city
ORDER BY revenue_as_was DESC;
```

Złączenie idzie po `customer_sk`, czyli po kluczu **zapisanym w faktcie**. Fakt
ze stycznia wskazuje na wersję wymiaru sprzed przeprowadzki, więc bez żadnego
dodatkowego warunku dostajemy stan historyczny. Nie ma tu ani `is_current`, ani
`BETWEEN` — cała praca została wykonana w momencie ładowania faktu.

### Punkt 2 — miasto aktualne (as-is)

```sql
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
```

Dwa złączenia do **tego samego wymiaru**, pod dwoma aliasami. Pierwsze (`hist`)
sięga po wersję wskazywaną przez fakt — jedyne, po co, to wyciągnięcie
`customer_id`. Drugie (`cur`) przechodzi z tego `customer_id` na wersję bieżącą.

To jest standardowy wzorzec dla raportu „historia według dzisiejszej struktury”
i wraca wszędzie tam, gdzie zmienia się podział organizacyjny: sprzedaż sprzed
reorganizacji oglądana według nowych regionów, koszty sprzed fuzji według nowych
spółek.

Warunek `cur.is_current = true` jest tu **obowiązkowy**. Bez niego dla ósemki,
która się przeprowadziła, złączenie trafiłoby na dwie wersje naraz i przychód
tych klientów policzyłby się dwukrotnie — suma wyszłaby wyższa niż 1 427 124,00.

### Punkt 3 — gdzie liczby się różnią

| Miasto | as-was | as-is | Różnica |
|---|---|---|---|
| Białystok | 90 937,00 | 135 235,00 | **+44 298,00** |
| Katowice | 80 287,00 | 112 591,00 | +32 304,00 |
| Warszawa | 228 367,00 | 239 214,00 | +10 847,00 |
| Lublin | 151 067,00 | 140 220,00 | −10 847,00 |
| Gdańsk | 328 841,00 | 315 271,00 | −13 570,00 |
| Wrocław | 108 517,00 | 86 023,00 | −22 494,00 |
| Kraków | 170 570,00 | 130 032,00 | **−40 538,00** |

Siedem miast. Pozostałe mają w obu wariantach tę samą kwotę, a **suma obu kolumn
to te same 1 427 124,00**.

Przychód się nie zmienia — zmienia się wyłącznie to, komu go przypisujemy.
**Kraków „traci” 40 538 zł, których nigdy nie stracił.** To jest zdanie warte
zapamiętania z całego dnia 3.

Poznań nie pojawia się na liście, mimo że wyprowadzili się z niego C0018 i C0023.
Powód jest prozaiczny: ci dwaj klienci nie złożyli 15 ani 16 stycznia żadnego
opłaconego zamówienia, więc nie ma czego przenosić. Warto to sprawdzić przy
okazji — brak różnicy nie zawsze znaczy błąd.

Gdyby punkty 1 i 2 dały identyczne liczby, złączenie w punkcie 1 poszło po
`is_current` już na poziomie faktu. To błąd, który kasuje całą wartość SCD Type 2
i sprowadza wymiar z powrotem do Type 1.

### Punkt 4 — koszyk bez zakupu 16 stycznia

```sql
WITH cart AS (
  SELECT DISTINCT customer_id
  FROM aurora_jk.bronze_events
  WHERE event_type = 'add_to_cart' AND dt = '2026-01-16'
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
```

**Co ma wyjść:** **55** klientów z `add_to_cart` 16 stycznia, z tego **14 bez
zakupu**, czyli **25,5%**.

`DISTINCT` w obu zbiorach jest konieczny: jeden klient dodaje do koszyka kilka
produktów i składa kilka zamówień, a pytanie dotyczy ludzi, nie zdarzeń.

`COUNT(*) - COUNT(b.customer_id)` to zwięzły wariant wzorca *anti-join*.
`COUNT(*)` liczy wszystkie wiersze po `LEFT JOIN`, `COUNT(b.customer_id)` pomija
`NULL`-e, więc różnica to dokładnie ci, którzy nie mieli dopasowania.
Równoważnie: `WHERE b.customer_id IS NULL` albo `NOT EXISTS`.

`100.0`, a nie `100` — całkowitoliczbowe dzielenie w Trino ucięłoby część
ułamkową i procent wyszedłby zerem albo liczbą całkowitą.

Zbiór „kupił” pochodzi z **zamówień**, nie ze zdarzeń `purchase`. Porównanie
zdarzeń ze zdarzeniami dałoby wynik bliski zera, bo byłoby porównaniem zbioru
prawie z samym sobą — zdarzenie `purchase` jest generowane przez ten sam system,
który generuje `add_to_cart`. Wynik 0% jest więc rozpoznawalnym objawem właśnie
tej pomyłki.

### Typowe pomyłki

- **Sumy w punktach 1 i 2 się różnią.** Brak `cur.is_current = true` w punkcie 2 —
  ósemka policzona podwójnie.
- **Punkty 1 i 2 dają identyczne liczby.** Punkt 1 złączony po `is_current`
  zamiast po `customer_sk` z faktu.
- **Więcej niż siedem miast z różnicą.** Wymiar SCD2 ma zachodzące okresy
  ważności albo zduplikowane `customer_id` w obrębie `is_current = true`.
- **Punkt 4 daje 0%.** Zbiór „kupił” wzięty ze zdarzeń `purchase`.
- **Punkt 4 daje procent całkowity.** `100` zamiast `100.0` w liczniku.

---

## W10 — sprzątanie

Kolejność wynika z zależności między zasobami, nie z wygody:

1. **Redshift Serverless** — najpierw workgroup, potem namespace. Namespace nie
   da się usunąć, dopóki wisi na nim workgroup. Przy usuwaniu namespace'u konsola
   proponuje *Create final snapshot* — na kursie zbędny, bo sam kosztuje
   za przechowywanie.
2. **Athena** — `DROP DATABASE aurora_<inicjaly> CASCADE;`. `CASCADE` znaczy
   „razem z tabelami w środku”; bez niego Athena odmawia usunięcia niepustej bazy.
   Tabele są w większości zewnętrzne, więc kasowany jest wyłącznie opis w Glue —
   pliki na S3 zostają do kroku 3. Wyjątkiem jest `silver_orders_iceberg`
   (`is_external = false`), której pliki znikają razem z tabelą.
3. **S3** — najpierw *Empty* (bucket z choćby jednym obiektem nie da się usunąć),
   potem *Delete*. Obie operacje wymagają wpisania potwierdzenia: odpowiednio
   `permanently delete` i nazwy bucketu.
4. **`athena-results/`** — znika razem z bucketem. Osobna pozycja tylko wtedy, gdy
   wyniki zapytań szły do innego bucketu niż własny.

Zasada zakresu: **wyłącznie własne zasoby, bez ruszania cudzych prefiksów
i cudzych baz.** Ewentualny `default-workgroup` oraz inne bazy Glue na koncie
zostają nietknięte — usuwamy tylko obiekty z własnym prefiksem `aurora_<inicjaly>`.

Kontrola końcowa: brak bucketu na liście S3, brak bazy `aurora_<inicjaly>`
w Athenie, brak workgroupu, namespace'u i snapshotu z własnymi inicjałami
w konsoli Redshiftu.

W chmurze i w środowisku wieloosobowym to standardowa higiena inżynierska —
pozostawione zasoby generują niepotrzebny koszt.

---

## Liczby całego kursu w jednym miejscu

| Warstwa | Co | Wartość |
|---|---|---|
| Bronze | customers · products · orders · order_items · events | 80 · 25 · 200 · 486 · 935 |
| Silver | orders · order_items · przychód | 170 · 409 · 1 427 124,00 |
| Gold (dzień 2) | dim_date · dim_channel · dim_customer · dim_product · fakt | 2 · 3 · 80 · 25 · 409 |
| Gold (dzień 3) | `gold_dim_customer_scd` · fakt 20.01 · widok całości | 88 · 40 · 449 |
| Dzień 3 | przychód 20.01 · Iceberg po `MERGE` | 101 546,00 · 210 |

Trzy dni, dwa silniki, jedna liczba przychodu za styczeń — i to jest najkrótszy
dowód, że model jest poprawny.
