# Dzień 1 — rozwiązania

Rozwiązania wzorcowe do zadań z `01_zadania.md`. Największą wartość mają jako materiał
do **porównania z własnym rozwiązaniem** — przeczytane przed pierwszą próbą zabierają
z ćwiczenia to, co w nim najciekawsze. Pełny SQL każdego ćwiczenia jest w tym pliku i — identycznie — w `sql/L3_bronze_ddl.sql` itd.

W przykładach jest prefix `aurora_jk` / `aurora-jk-lake` — do podmiany na własny.

Cały dzień 1 (L3–L6 oraz S1 i S2) został wykonany w konsoli AWS w regionie `eu-central-1`.
Liczby poniżej to zmierzone wyniki, nie szacunki — pochodzą z tych samych plików,
które wgrywamy do S3 w L2.

| Tabela Bronze | COUNT |
|---|---|
| `bronze_customers` | 80 |
| `bronze_events` | 935 |
| `bronze_order_items` | 486 |
| `bronze_orders` | 200 (90 dnia 15.01 + 110 dnia 16.01) |
| `bronze_products` | 25 |

Wiersze są w takiej kolejności, w jakiej zwraca je zapytanie kontrolne
z `sql/L3_bronze_ddl.sql` — pięć `COUNT(*)` sklejonych `UNION ALL` i posortowanych
`ORDER BY tabela`. Samo `UNION ALL` kolejności nie gwarantuje: bez `ORDER BY`
Athena wypisuje gałęzie w takim porządku, w jakim skończyły się liczyć, i przy
każdym uruchomieniu może on być inny.

---

## L1–L2 — struktura na S3

Po uploadzie katalogu `bronze` bucket wygląda tak:

```text
s3://aurora-jk-lake/
  bronze/customers/customers.csv
  bronze/events/dt=2026-01-15/events.json
  bronze/events/dt=2026-01-16/events.json
  bronze/order_items/order_items.csv
  bronze/orders/dt=2026-01-15/orders.csv
  bronze/orders/dt=2026-01-16/orders.csv
  bronze/products/products.csv
```

`silver/`, `gold/` i `athena-results/` pojawią się same — pierwsze dwa przy CTAS,
trzecie przy pierwszym zapytaniu.

### Odpowiedź na pytanie o trzy warstwy

Pytanie brzmiało: skoro dla S3 to tylko fragment nazwy klucza, co daje rozdzielenie?

Warstwa jest **kontraktem między ludźmi**, nie funkcją AWS. Nazwa prefiksu odpowiada
na pytanie „na ile mogę zaufać temu plikowi”:

- **Bronze** — dokładnie to, co przyszło ze źródła. Wolno tu mieć duplikaty, puste
  pola i anulowane zamówienia. Nikt nie buduje na tym raportu.
- **Silver** — typy się zgadzają, śmieci odfiltrowane, jeden wiersz znaczy jedną rzecz.
  Analityk może na tym pracować.
- **Gold** — liczby, które wolno pokazać zarządowi.

Bez tego podziału po miesiącu w bucketcie leży `orders_final_v2_FIXED.csv` i nikt
nie wie, czy przychód liczyć z niego, czy z `orders.csv`. To jest właśnie moment,
w którym data lake zamienia się w data swamp.

Drugi, praktyczny argument: warstwa Bronze zostaje nietknięta, więc każdy błąd
w transformacji da się naprawić przeliczeniem Silver od zera. Gdyby czyszczenie
nadpisywało oryginał, pomyłka byłaby nieodwracalna.

Najczęstsza pierwsza odpowiedź to „porządek”. Warto ją dociągnąć do końca: porządek
dla kogo i co konkretnie się psuje bez niego. Dopiero to jest odpowiedź.

### Odpowiedź na pytanie o `dt=`

Wynik zapytania byłby identyczny. Traci się dwie rzeczy:

1. **Możliwość pominięcia odczytu.** Z partycjami filtr `WHERE dt = '2026-01-16'`
   sprawia, że Athena nie otwiera plików z 15.01. Bez nich każde zapytanie o jeden
   dzień czyta całą historię i za nią płaci.
2. **Możliwość dołożenia dnia bez ruszania reszty.** Dzień 3 dokłada
   `dt=2026-01-20` jako nowy katalog. Przy jednym wspólnym pliku trzeba by go
   pobrać, dopisać i wgrać z powrotem — a to znaczy przepisanie całości.

---

## L3 — DDL warstwy Bronze

`bronze_customers` — jeden wiersz to jeden klient ze źródła. `bronze_orders` — jedno zamówienie. `bronze_order_items` — jedna pozycja (486, w tym cancelled). `bronze_events` — jedno zdarzenie. `bronze_products` — jeden produkt.

Pełny skrypt (identyczny z `sql/L3_bronze_ddl.sql`):

```sql
-- =============================================================================
-- Dzien 1, L3 - warstwa Bronze w Athenie
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Opis krok po kroku: ../02_rozwiazania.md
--
-- Wszystkie CREATE maja IF NOT EXISTS, wiec caly plik mozna uruchomic ponownie
-- bez bledu. Sprawdzone w Athenie engine v3.
--
-- Tabele sa EXTERNAL: katalog Glue trzyma tylko opis, dane zostaja na S3.
-- DROP TABLE kasuje opis, plikow nie rusza.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS aurora_jk;

-- -----------------------------------------------------------------------------
-- Slowniki bez partycji
-- -----------------------------------------------------------------------------
-- bronze_customers: jeden wiersz to jeden klient ze zrodla CRM (aktualny stan).
-- signup_date jest tu string, nie date. OpenCSVSerde czyta kazda kolumne jako
-- tekst i dopiero rzutuje na typ zadeklarowany - dla dat bywa to zawodne.
-- Konwersja na date nastepuje w warstwie Silver (L5).

CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_customers (
  customer_id string,
  email string,
  first_name string,
  last_name string,
  city string,
  voivodeship string,
  segment string,
  signup_date string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/customers/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- bronze_products: jeden wiersz to jeden produkt z katalogu (sku, kategoria, cena listowa).
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_products (
  product_id int,
  sku string,
  product_name string,
  category string,
  brand string,
  unit_price_pln double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/products/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- bronze_order_items: jeden wiersz to jedna pozycja zamowienia (order_id + produkt).
-- 486 pozycji, w tym te z zamowien cancelled - Bronze nic nie filtruje.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_order_items (
  order_id string,
  product_id int,
  qty int,
  unit_price_pln double,
  discount_pln double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/order_items/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- -----------------------------------------------------------------------------
-- Tabele partycjonowane po dt (katalog dt=RRRR-MM-DD na S3)
-- -----------------------------------------------------------------------------
-- Kolumny partycjonujacej NIE wymienia sie na liscie kolumn - jest osobno,
-- w PARTITIONED BY. Jej wartosc pochodzi z nazwy katalogu, nie z pliku.

-- bronze_orders: jeden wiersz to jedno zamowienie. Partycja dt pochodzi z katalogu na S3,
-- nie z kolumny w pliku. 200 zamowien = 90 dnia 15.01 + 110 dnia 16.01 (30 cancelled zostaje).
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_orders (
  order_id string,
  customer_id string,
  order_ts string,
  status string,
  channel string,
  delivery_city string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/orders/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Pliki events to JSON Lines: jeden obiekt na linie, bez przecinkow miedzy
-- obiektami i bez nawiasow tablicy. Zwykly "ladny" JSON sie nie wczyta.
-- bronze_events: jeden wiersz to jedno zdarzenie ze sklepu (JSON Lines). 935 zdarzen.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_events (
  event_id string,
  event_ts string,
  customer_id string,
  event_type string,
  page string,
  product_id int
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://aurora-jk-lake/bronze/events/';

-- Gdy OpenX JsonSerDe zawiedzie (plan B, nie domyslny wybor):
-- Objaw: CREATE przechodzi, SELECT → HIVE_CURSOR_ERROR lub same NULL mimo poprawnych
-- plikow JSON Lines. Przyczyna: parser OpenX nie rozpoznaje linii albo odrzuca
-- znaki specjalne w polach — wtedy HCatalog JsonSerDe bywa odporniejszy.
-- Procedura: DROP TABLE, podmiana ROW FORMAT SERDE na poniższą linię, CREATE, MSCK.
--   ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
-- W Athenie engine v3 wariant OpenX działa — fallback tylko gdy SELECT faktycznie pada.

-- -----------------------------------------------------------------------------
-- Rejestracja partycji
-- -----------------------------------------------------------------------------
-- Bez tego COUNT(*) = 0 mimo poprawnie wgranych plikow. Katalog Glue nie
-- przeszukuje S3 samoczynnie - MSCK REPAIR przechodzi po prefiksach i dopisuje
-- kazda znaleziona partycje.

MSCK REPAIR TABLE aurora_jk.bronze_orders;
MSCK REPAIR TABLE aurora_jk.bronze_events;

-- -----------------------------------------------------------------------------
-- KONTROLA
-- -----------------------------------------------------------------------------
-- Oczekiwane (w kolejnosci, w jakiej zwraca je ORDER BY tabela):
--   customers 80 | events 935 | order_items 486 | orders 200 | products 25
--
-- UNION ALL nie gwarantuje kolejnosci wierszy - Athena zwraca galezie w takiej
-- kolejnosci, w jakiej skonczyly sie liczyc. Bez ORDER BY ten sam SELECT potrafi
-- dac za kazdym razem inny uklad, dlatego na koncu jest sortowanie po etykiecie.
SELECT 'customers' AS tabela, COUNT(*) AS n FROM aurora_jk.bronze_customers
UNION ALL SELECT 'products',    COUNT(*) FROM aurora_jk.bronze_products
UNION ALL SELECT 'order_items', COUNT(*) FROM aurora_jk.bronze_order_items
UNION ALL SELECT 'orders',      COUNT(*) FROM aurora_jk.bronze_orders
UNION ALL SELECT 'events',      COUNT(*) FROM aurora_jk.bronze_events
ORDER BY tabela;

-- Oczekiwane: 2026-01-15 -> 90 wierszy, 2026-01-16 -> 110 wierszy
SELECT dt, COUNT(*) AS n
FROM aurora_jk.bronze_orders
GROUP BY dt
ORDER BY dt;
```


Poniżej rzeczy, które w tym DDL są nieoczywiste.

### Dlaczego `signup_date` jest `string`

`OpenCSVSerde` czyta **każdą** kolumnę jako tekst i dopiero rzutuje na zadeklarowany
typ. Dla `date` bywa to zawodne — CSV nie niesie metadanych typu, a w źródle mogą
mieszać się formaty (`2026-01-15` vs `15.01.2026`). Bezpieczniej zostawić `string`
w Bronze i skonwertować w Silver — to jest sens podziału na warstwy: Bronze przyjmuje
dane takimi, jakie są, a naprawą zajmuje się następna warstwa. Ten sam wzorzec
dotyczy `order_ts` w `bronze_orders`.

W `bronze_events` kolumna `product_id int` ma sens, bo JsonSerDe czyta natywny typ
JSON (liczba bez cudzysłowu). To nie dotyczy CSV — tam nawet liczby w pliku wracają
jako string.

### JsonSerDe — jeden wariant, jeden fallback

W skrypcie jest **tylko** `org.openx.data.jsonserde.JsonSerDe` — to domyślny wybór.
Zamiennik `org.apache.hive.hcatalog.data.JsonSerDe` wchodzi **wyłącznie**, gdy OpenX
zawiedzie w rozpoznawalny sposób:

- `CREATE TABLE` przechodzi bez błędu,
- `SELECT` kończy się `HIVE_CURSOR_ERROR` albo zwraca same `NULL` mimo poprawnych
  plików JSON Lines na S3.

**Dlaczego OpenX zawodzi:** parser HCatalog bywa odporniejszy na nietypowe znaki
w polach tekstowych albo na linie, które OpenX źle interpretuje. W Athenie engine v3
OpenX działa na tym datasecie bez zarzutu — fallback to plan B, nie równoległa opcja do wyboru.

Procedura wymiany: `DROP TABLE aurora_jk.bronze_events`, podmiana nazwy SerDe,
ponowne `CREATE`, ponowny `MSCK REPAIR`. Dane na S3 pozostają nietknięte.

### Dlaczego `IF NOT EXISTS`

Skrypt niemal zawsze uruchamia się dwa razy — z ciekawości albo dlatego, że pierwsze
wykonanie wyglądało na nieudane. Bez tego dodatku drugie kończy się
`AlreadyExistsException` i szukaniem błędu tam, gdzie go nie ma. Sprawdzone w Athenie:
z `IF NOT EXISTS` instrukcja przechodzi i nic nie zmienia.

### MSCK REPAIR

```sql
MSCK REPAIR TABLE aurora_jk.bronze_orders;
MSCK REPAIR TABLE aurora_jk.bronze_events;
```

Bez tego `COUNT(*)` = 0 przy poprawnie wgranych plikach. To pierwszy moment dnia,
w którym widać, że **katalog i dane to dwie osobne rzeczy** — pliki na S3 istnieją,
ale Glue o nich nie wie. Warto zrobić to celowo: najpierw `COUNT(*)`, dopiero potem
`MSCK REPAIR` i `COUNT(*)` jeszcze raz.

---

## L4 — SQL na warstwie Bronze

Cztery pytania na Bronze. 170 to zamówienia paid/shipped (z 200; 30 cancelled).

Pełny skrypt (identyczny z `sql/L4_pytania_biznesowe.sql`):

```sql
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
```

Pełny skrypt z liczbami kontrolnymi w komentarzach: `sql/L4_pytania_biznesowe.sql`.

```sql
-- 1. zamówienia opłacone lub wysłane
SELECT COUNT(*) AS orders_ok
FROM aurora_jk.bronze_orders
WHERE status IN ('paid', 'shipped');

-- 2. przychód netto wg dnia
SELECT
  o.dt,
  ROUND(SUM(i.qty * i.unit_price_pln - i.discount_pln), 2) AS revenue_pln
FROM aurora_jk.bronze_orders o
JOIN aurora_jk.bronze_order_items i ON o.order_id = i.order_id
WHERE o.status IN ('paid', 'shipped')
GROUP BY o.dt
ORDER BY o.dt;
```

Punkty 3 i 4 są w pliku `.sql`.

### Wyniki

| Pytanie | Wynik |
|---|---|
| 1. zamówienia paid/shipped | **170** (z 200; 30 ma status `cancelled`) |
| 2. przychód 2026-01-15 | **653 531,00** |
| 2. przychód 2026-01-16 | **773 593,00** |
| 4. unikalni klienci mobile | **35** |

Punkt 3 — pięć kategorii wg przychodu:

| Kategoria | Przychód PLN |
|---|---|
| Telefony | 381 977,00 |
| Komputery | 362 605,00 |
| TV | 324 800,00 |
| AGD | 261 065,00 |
| Audio | 83 254,00 |

Suma tych pięciu = 1 413 701,00. To **nie jest** przychód całkowity z punktu 2
(1 427 124,00). W datasecie jest sześć kategorii — szósta, **Akcesoria**, ma
13 423,00 i wypada poza `LIMIT 5`. Warto to pokazać, gdy ktoś zacznie sumować
kolumnę w głowie i zgłosi, że „coś się nie zgadza”.

### Typowe pomyłki

- **200 zamiast 170** w punkcie 1 — pominięty warunek `status`.
- **Przychód wyższy niż w tabeli** — policzone razem z `cancelled`.
- **Przychód niższy** — rabat odjęty przed pomnożeniem przez `qty`.
  Poprawna kolejność: `qty * unit_price − discount`, bo rabat jest kwotą na pozycję,
  nie na sztukę.

To jest dobry moment na zdanie: **Bronze niczego za nas nie filtruje.** Poprawność
zależy wyłącznie od tego, czy analityk pamiętał o `WHERE`.

---

## L5 — Silver w Parquecie

`silver_orders` — jedno zamówienie paid/shipped (170). `silver_order_items` — jedna pozycja (**409**, nie 486: JOIN z orders odrzuca 77 pozycji z 30 cancelled — L5 / dzień 1).

Pełny skrypt (identyczny z `sql/L5_silver_ctas.sql`):

```sql
-- =============================================================================
-- Dzien 1, L5 - warstwa Silver w Parquecie
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
--
-- CTAS = CREATE TABLE AS SELECT. Athena wykonuje SELECT, ZAPISUJE jego wynik
-- na S3 jako nowe pliki i rejestruje tabele w katalogu. To jest litera "T"
-- z ELT - transformacja zwyklym SQL-em, bez Glue Job i bez Sparka.
--
-- Klauzula WITH (...) przed AS opisuje JAK zapisac (format, kompresja,
-- partycje, miejsce). Sam SELECT odpowiada tylko za tresc.
--
-- IDEMPOTENCJA: Athena nigdy nie kasuje danych z S3.
--   * bez IF NOT EXISTS drugie uruchomienie -> TABLE_ALREADY_EXISTS
--   * z IF NOT EXISTS drugie uruchomienie zostaje pominiete (sprawdzone)
--   * po DROP TABLE, gdy pliki zostaly -> HIVE_PATH_ALREADY_EXISTS,
--     i tu IF NOT EXISTS juz NIE pomaga. Trzeba skasowac prefiks w S3.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- silver_orders: jeden wiersz to jedno zamowienie paid/shipped (170, nie 200).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aurora_jk.silver_orders
WITH (
  format              = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by      = ARRAY['dt'],
  external_location   = 's3://aurora-jk-lake/silver/orders/'
) AS
SELECT
  order_id,
  customer_id,
  CAST(order_ts AS timestamp) AS order_ts,
  CAST(substr(order_ts, 1, 10) AS date) AS order_date,
  status,
  channel,
  delivery_city,
  dt   -- UWAGA: kolumna partycjonujaca musi byc OSTATNIA na liscie SELECT.
       -- partitioned_by nie wskazuje kolumny po nazwie - Athena bierze tyle
       -- ostatnich kolumn, ile jest w tablicy ARRAY[...].
FROM aurora_jk.bronze_orders
WHERE status IN ('paid', 'shipped');

-- -----------------------------------------------------------------------------
-- silver_order_items: jeden wiersz to jedna pozycja zamowienia paid/shipped.
-- 409, nie 486: JOIN z orders odrzuca 77 pozycji z 30 zamowien cancelled (L4 / dzien 1).
-- -----------------------------------------------------------------------------
-- Zlaczenie z orders nie jest ozdoba. Tabela order_items nie ma ani statusu,
-- ani daty, wiec bez niego nie da sie:
--   * odfiltrowac pozycji nalezacych do zamowien anulowanych,
--   * nadac pozycjom partycji dt.
CREATE TABLE IF NOT EXISTS aurora_jk.silver_order_items
WITH (
  format              = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by      = ARRAY['dt'],
  external_location   = 's3://aurora-jk-lake/silver/order_items/'
) AS
SELECT
  i.order_id,
  i.product_id,
  i.qty,
  i.unit_price_pln,
  i.discount_pln,
  (i.qty * i.unit_price_pln - i.discount_pln) AS net_amount_pln,
  o.customer_id,
  CAST(substr(o.order_ts, 1, 10) AS date) AS order_date,
  o.dt
FROM aurora_jk.bronze_order_items i
JOIN aurora_jk.bronze_orders o ON i.order_id = o.order_id
WHERE o.status IN ('paid', 'shipped');

-- -----------------------------------------------------------------------------
-- Slowniki - bez partycji, bo nie rosna z dnia na dzien
-- -----------------------------------------------------------------------------
-- silver_customers: jeden wiersz to jeden klient; signup_date juz jako date.
CREATE TABLE IF NOT EXISTS aurora_jk.silver_customers
WITH (
  format              = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location   = 's3://aurora-jk-lake/silver/customers/'
) AS
SELECT
  customer_id,
  email,
  first_name,
  last_name,
  city,
  voivodeship,
  segment,
  CAST(signup_date AS date) AS signup_date   -- string z Bronze -> date
FROM aurora_jk.bronze_customers;

-- silver_products: jeden wiersz to jeden produkt, typy zgodne z Bronze.
CREATE TABLE IF NOT EXISTS aurora_jk.silver_products
WITH (
  format              = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location   = 's3://aurora-jk-lake/silver/products/'
) AS
SELECT
  product_id,
  sku,
  product_name,
  category,
  brand,
  unit_price_pln
FROM aurora_jk.bronze_products;

-- -----------------------------------------------------------------------------
-- KONTROLA
-- -----------------------------------------------------------------------------
-- Oczekiwane (w kolejnosci, w jakiej zwraca je ORDER BY tabela):
--   customers 80 | order_items 409 | orders 170 | products 25
--
-- 409, a nie 486, bo order_items zostaly ZLACZONE z orders, a filtr statusu
-- dziala na kolumnie z tabeli zamowien. 30 zamowien ma status cancelled;
-- zlaczenie odrzuca wszystkie 77 nalezacych do nich pozycji: 486 - 77 = 409.
-- Bronze nadal ma komplet 486 wierszy - nic nie zginelo.
--
-- ORDER BY na koncu jest potrzebny: UNION ALL sam nie gwarantuje kolejnosci.
SELECT 'orders' AS tabela, COUNT(*) AS n FROM aurora_jk.silver_orders
UNION ALL SELECT 'order_items', COUNT(*) FROM aurora_jk.silver_order_items
UNION ALL SELECT 'customers',   COUNT(*) FROM aurora_jk.silver_customers
UNION ALL SELECT 'products',    COUNT(*) FROM aurora_jk.silver_products
ORDER BY tabela;

-- Oczekiwane: 2026-01-15 -> 653531.00 | 2026-01-16 -> 773593.00
-- Razem 1427124.00 - ta sama kwota co w L4 pkt 2.
SELECT dt, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
GROUP BY dt
ORDER BY dt;
```

Skrypt: `sql/L5_silver_ctas.sql`.

### Jak wytłumaczyć CTAS

Trzy zdania, zanim ktokolwiek zacznie pisać:

1. `CREATE TABLE ... AS SELECT` uruchamia `SELECT` i **zapisuje wynik jako nowe pliki
   na S3**, a potem rejestruje tabelę w katalogu. Zwykły `CREATE EXTERNAL TABLE`
   z L3 tylko opisywał pliki, które już tam były — tutaj powstają nowe.
2. To jest litera „T” z ELT. Transformacja nie wymaga Glue Job ani Sparka, wystarczy SQL.
3. Klauzula `WITH (...)` przed `AS` opisuje **jak zapisać**: format pliku, kompresję,
   partycjonowanie i miejsce. Sam `SELECT` odpowiada tylko za treść.

```sql
CREATE TABLE IF NOT EXISTS aurora_jk.silver_orders
WITH (
  format              = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by      = ARRAY['dt'],
  external_location   = 's3://aurora-jk-lake/silver/orders/'
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
WHERE status IN ('paid', 'shipped');
```

### Kolumna partycjonująca na końcu

`partitioned_by = ARRAY['dt']` nie wskazuje kolumny po nazwie — Athena bierze
**tyle ostatnich kolumn z `SELECT`, ile jest w tablicy**. Przy `dt` w środku listy
partycjonowanie poleci po niewłaściwej kolumnie albo zapytanie zwróci błąd.
Warto to powiedzieć wprost, bo komunikat błędu nie jest oczywisty.

### Wyniki

| Tabela Silver | COUNT |
|---|---|
| `silver_customers` | 80 |
| `silver_order_items` | **409** |
| `silver_orders` | 170 |
| `silver_products` | 25 |

Kolejność jak w zapytaniu kontrolnym z `sql/L5_silver_ctas.sql` (`ORDER BY tabela`).

Suma `net_amount_pln` w `silver_order_items` = **1 427 124,00**
(653 531,00 + 773 593,00 — zgadza się z L4 pkt 2).

### Skąd 409 zamiast 486

To jest efekt **złączenia**, nie osobnego filtra na pozycjach.

`silver_order_items` powstaje z `bronze_order_items` **złączonych** z `bronze_orders`
po `order_id`, a warunek `WHERE o.status IN ('paid','shipped')` działa na kolumnie
z tabeli zamówień. Skoro 30 zamówień ma status `cancelled`, to złączenie odrzuca
wszystkie należące do nich pozycje — 77 wierszy. Zostaje 486 − 77 = **409**.

Sama tabela `order_items` nie ma kolumny `status`; gdyby nie złączenie, nie byłoby
jak odróżnić pozycji anulowanej od opłaconej.

Warto zatrzymać się tu na chwilę: **to pierwszy moment, w którym Silver coś usuwa.**
Bronze miał 486 wierszy i nadal je ma — nic nie zginęło, po prostu warstwa wyżej
zawęziła zbiór do tego, co znaczy „sprzedaż”.

### Gdy CTAS nie chce się uruchomić drugi raz

Dwa różne komunikaty, dwie różne przyczyny:

| Komunikat | Co się stało | Naprawa |
|---|---|---|
| `TABLE_ALREADY_EXISTS` | tabela jest w katalogu | `IF NOT EXISTS` pomija ją bez błędu |
| `HIVE_PATH_ALREADY_EXISTS` | tabela skasowana, ale pliki zostały na S3 | usunąć prefiks `silver/orders/` albo zmienić lokalizację |

Drugi przypadek zdarza się, gdy ktoś zrobi `DROP TABLE` i uruchomi CTAS ponownie.
`IF NOT EXISTS` **nie chroni** przed nim — sprawdzone. Athena z założenia nie kasuje
danych z S3, żeby przypadkowe `CREATE` nie zniszczyło cudzej pracy.

---

## L6 — porównanie skanu

To samo pytanie (przychód wg dnia) trzema drogami. Kwoty 653 531 i 773 593; różni się Data scanned.

Pełny skrypt (identyczny z `sql/L6_porownanie_skanu.sql`):

```sql
-- =============================================================================
-- Dzien 1, L6 - to samo pytanie biznesowe trzema drogami
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk.
--
-- WARUNEK WSTEPNY: przelacznik "Reuse query results" nad wynikami musi byc
-- WYLACZONY. Athena zwraca inaczej zapisany wynik poprzedniego identycznego
-- zapytania i pokazuje zerowy skan, co psuje cale porownanie.
--
-- Athena rozlicza sie od liczby BAJTOW ODCZYTANYCH Z S3, nie od liczby wierszy
-- ani od czasu. Data scanned jest w zakladce Query stats pod wynikiem.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Wariant 1: Bronze, CSV, ze zlaczeniem
-- -----------------------------------------------------------------------------
-- Zmierzone w Athenie v3: Data scanned = 24 119 B
-- Oczekiwane kwoty: 653531.00 i 773593.00
SELECT
  o.dt,
  ROUND(SUM(i.qty * i.unit_price_pln - i.discount_pln), 2) AS revenue_pln
FROM aurora_jk.bronze_orders o
JOIN aurora_jk.bronze_order_items i ON o.order_id = i.order_id
WHERE o.status IN ('paid', 'shipped')
GROUP BY o.dt
ORDER BY o.dt;

-- -----------------------------------------------------------------------------
-- Wariant 2: Silver, Parquet, bez zlaczenia
-- -----------------------------------------------------------------------------
-- Zmierzone w Athenie v3: Data scanned = 1 717 B  (14x mniej niz wariant 1)
-- Kwoty MUSZA byc identyczne: 653531.00 i 773593.00
SELECT
  dt,
  ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
GROUP BY dt
ORDER BY dt;

-- -----------------------------------------------------------------------------
-- Wariant 3: Silver, jedna partycja
-- -----------------------------------------------------------------------------
-- Zmierzone w Athenie v3: Data scanned = 940 B
-- Oczekiwana kwota: 773593.00
SELECT ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
WHERE dt = '2026-01-16';

-- =============================================================================
-- DLACZEGO WARIANT 2 CZYTA MNIEJ NIZ WARIANT 1 - cztery rozne mechanizmy
-- =============================================================================
-- 1. UKLAD KOLUMNOWY
--    CSV trzyma dane wierszami, wiec zeby siegnac po qty trzeba przeczytac caly
--    wiersz razem z delivery_city i reszta. Parquet trzyma kazda kolumne osobno,
--    wiec Athena pobiera z S3 tylko kolumny wystepujace w zapytaniu.
--
-- 2. KOMPRESJA I KODOWANIE
--    Kwota 1234.56 w CSV to siedem bajtow tekstu. W Parquecie to liczba, ktora
--    po SNAPPY i kodowaniu slownikowym zajmuje ulamek tego miejsca - zwlaszcza
--    dla kolumn o powtarzalnych wartosciach (status, kanal, data).
--
-- 3. BRAK ZLACZENIA
--    Wariant 1 czyta dwie tabele i laczy je po order_id. Silver ma
--    net_amount_pln policzone juz przy budowie warstwy, wiec pyta jedna tabele.
--    Ta czesc oszczednosci wynika z MODELU, nie z formatu pliku.
--
-- 4. STATYSTYKI W STOPCE PLIKU
--    Parquet zapisuje min i max kazdej kolumny dla kazdej grupy wierszy, wiec
--    Athena pomija grupy, ktore na pewno nie zawieraja szukanych wartosci
--    (predicate pushdown).
--
-- WARIANT 3 doklada piaty mechanizm: PARTITION PRUNING. Filtr dt = '2026-01-16'
-- sprawia, ze Athena w ogole nie otwiera katalogu z 15.01. Jako jedyny z tych
-- mechanizmow nie zalezy od formatu - partycje dzialalyby tak samo na CSV.
--
-- ZASTRZEZENIE O KOSZCIE: Athena zaokragla rozliczenie do 10 MB na zapytanie,
-- a caly dataset to kilobajty - rachunek jest w obu wariantach taki sam i wynosi
-- praktycznie zero. Prawdziwa jest tu PROPORCJA, nie skala. Przy 200 GB CSV ten
-- sam stosunek to roznica miedzy 1 a 14 dolarami za pojedyncze zapytanie.
-- =============================================================================
```

```sql
-- Bronze, CSV, ze złączeniem
SELECT o.dt, ROUND(SUM(i.qty * i.unit_price_pln - i.discount_pln), 2) AS revenue_pln
FROM aurora_jk.bronze_orders o
JOIN aurora_jk.bronze_order_items i ON o.order_id = i.order_id
WHERE o.status IN ('paid', 'shipped')
GROUP BY o.dt ORDER BY o.dt;

-- Silver, Parquet, bez złączenia
SELECT dt, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
GROUP BY dt ORDER BY dt;

-- Silver, jedna partycja
SELECT ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
WHERE dt = '2026-01-16';
```

Kwoty muszą wyjść identyczne: 653 531,00 i 773 593,00.

### Zmierzone w Athenie v3

| Wariant | Data scanned |
|---|---|
| Bronze CSV, `JOIN` orders × order_items | 24 119 B |
| Silver Parquet, oba dni | 1 717 B |
| Silver Parquet, jedna partycja `dt='2026-01-16'` | 940 B |

Czternastokrotna różnica między pierwszym a drugim wariantem.

### Dlaczego Parquet czyta mniej — cztery mechanizmy

Warto rozłożyć to na czynniki, bo „Parquet jest szybszy” nie jest wyjaśnieniem.

1. **Układ kolumnowy.** CSV trzyma dane wierszami, więc żeby dostać się do `qty`,
   trzeba przeczytać cały wiersz razem z `delivery_city` i resztą. Parquet trzyma
   każdą kolumnę osobno, więc Athena pobiera z S3 wyłącznie te kolumny, które
   pojawiły się w zapytaniu. Tu: dwie z dziewięciu.
2. **Kompresja i kodowanie.** Kwota `1234.56` w CSV to siedem bajtów tekstu;
   w Parquecie to ośmiobajtowa liczba, ale po kompresji SNAPPY i kodowaniu
   słownikowym powtarzalne wartości (statusy, kanały, daty) zajmują ułamek tego,
   co w tekście.
3. **Brak złączenia.** Wariant Bronze czyta dwie tabele i łączy je po `order_id`.
   Silver ma `net_amount_pln` policzone przy tworzeniu warstwy, więc pyta jedną
   tabelę. To jest ta część oszczędności, która wynika z **modelu**, nie z formatu.
4. **Statystyki w stopce pliku.** Parquet zapisuje minimum i maksimum każdej kolumny
   dla każdej grupy wierszy. Athena potrafi pominąć grupę, o której z góry wiadomo,
   że nie zawiera szukanych wartości (*predicate pushdown*).

Trzeci wariant dokłada piąty mechanizm — **partition pruning**. Filtr `dt = '2026-01-16'`
sprawia, że Athena w ogóle nie otwiera katalogu z 15.01. To jedyna z tych oszczędności,
która nie zależy od formatu pliku: partycje działałyby tak samo na CSV.

### Uczciwe zastrzeżenie o koszcie

Athena zaokrągla rozliczenie do **10 MB na zapytanie**, a nasz dataset to kilobajty —
rachunek jest w obu wariantach identyczny i wynosi praktycznie zero. Nie warto
udawać, że sala właśnie coś zaoszczędziła.

Zdanie, które to ratuje: *proporcja jest tu prawdziwa, skala nie*. Przy 200 GB CSV
ten sam stosunek to różnica między 1 a 14 dolarami za pojedyncze zapytanie —
i między sekundami a minutami czekania. Dashboard odpytujący dane co 15 minut robi
to 96 razy dziennie.

### Pułapka: Reuse query results

Athena ma domyślnie włączone ponowne użycie wyników (do 60 minut). Powtórzone
identyczne zapytanie zwraca wtedy zapisany wynik i pokazuje **zerowy skan** —
co całkowicie psuje to ćwiczenie.

Przełącznik jest nad wynikami, po prawej — wyłączamy go już w L3. Jeśli mimo tego
`Data scanned` pokazuje 0 B, to jest pierwsze miejsce do sprawdzenia.

---

## S1 — Iceberg

`silver_orders_iceberg` — jeden wiersz to jedno zamówienie, kopia Silver jako Iceberg (170).

Pełny skrypt (identyczny z `sql/S1_tabela_iceberg.sql`):

```sql
-- =============================================================================
-- Dzien 1, S1 - kopia silver_orders jako tabela Apache Iceberg
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- S1. Tabela Iceberg
-- -----------------------------------------------------------------------------
-- Zwykla tabela zewnetrzna to katalog plikow - jedyna dostepna operacja to
-- "dopisz kolejny plik". Iceberg doklada nad plikami warstwe metadanych, ktora
-- pamieta, KTORE pliki tworza aktualny stan tabeli. Dzieki temu UPDATE, DELETE
-- i MERGE sprowadzaja sie do zapisania nowych plikow i przestawienia wskaznika,
-- a stare wskazniki zostaja - i to jest time travel.
--
-- Trzy roznice skladniowe wobec CTAS z L5:
--   * location zamiast external_location (tabela nie jest zewnetrzna)
--   * is_external = false - Athena zarzadza tymi plikami, wiec DROP TABLE JE
--     SKASUJE, odwrotnie niz w Bronze, gdzie DROP zostawia dane na S3
--   * partitioned_by nie dziala; Iceberg ma partitioning z transformacjami,
--     np. partitioning = ARRAY['month(order_date)']

CREATE TABLE IF NOT EXISTS aurora_jk.silver_orders_iceberg
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/silver/orders_iceberg/',
  is_external = false
) AS
SELECT * FROM aurora_jk.silver_orders;

-- Oczekiwane: 170 (tyle samo co silver_orders)
SELECT COUNT(*) AS n FROM aurora_jk.silver_orders_iceberg;

-- Oczekiwane po samym CTAS: jeden wiersz, operation = 'append'.
-- W dniu 3 dojdzie drugi, z operation = 'overwrite' po instrukcji MERGE.
SELECT committed_at, operation
FROM "aurora_jk"."silver_orders_iceberg$snapshots"
ORDER BY committed_at;
```

```sql
CREATE TABLE IF NOT EXISTS aurora_jk.silver_orders_iceberg
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/silver/orders_iceberg/',
  is_external = false
) AS
SELECT * FROM aurora_jk.silver_orders;
```

Sprawdzone w Athenie v3: przechodzi i daje **170** wierszy.

### Co powiedzieć o Iceberg w dwóch zdaniach

Zwykła tabela zewnętrzna to katalog plików — jedyna dostępna operacja to „dopisz
kolejny plik”. Iceberg dokłada nad plikami warstwę metadanych, która pamięta, **które
pliki tworzą aktualny stan tabeli**; dzięki temu `UPDATE`, `DELETE` i `MERGE` sprowadzają
się do zapisania nowych plików i przestawienia wskaźnika. Stare wskaźniki zostają,
i to jest właśnie time travel.

Trzy różnice składniowe wobec CTAS z L5, warte wskazania palcem:

- `location` zamiast `external_location` — bo tabela nie jest zewnętrzna
- `is_external = false` — Athena zarządza tymi plikami, więc `DROP TABLE` je skasuje
  (odwrotnie niż w Bronze, gdzie `DROP` zostawia dane)
- `partitioned_by` nie zadziała; Iceberg ma `partitioning` z transformacjami
  w rodzaju `month(order_date)`

Historia snapshotów:

```sql
SELECT committed_at, operation
FROM "aurora_jk"."silver_orders_iceberg$snapshots"
ORDER BY committed_at;
```

Po CTAS jest jeden snapshot z operacją `append`. W dniu 3 dojdzie drugi, z `overwrite`
po `MERGE` — i to jest najkrótsza możliwa demonstracja, po co komu lakehouse.

---

## S2 — lejek: koszyk bez zakupu

Ilu klientów danego dnia miało add_to_cart i nie miało purchase tego samego dnia (27 / 25). LEFT JOIN + IS NULL, bo to anti-join.

Pełny skrypt (identyczny z `sql/S2_lejek_koszyk.sql`):

```sql
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
```

```sql
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
  ON c.customer_id = b.customer_id AND c.dt = b.dt
WHERE b.customer_id IS NULL
GROUP BY c.dt
ORDER BY c.dt;
```

**Wynik: 27 klientów 15.01 i 25 klientów 16.01.**

### Trzy rzeczy do wyjaśnienia

**Dlaczego `DISTINCT`.** Jeden klient dodaje do koszyka kilka produktów, więc bez
tego liczylibyśmy zdarzenia, a nie ludzi. Pytanie brzmi „ilu klientów”.

**Dlaczego `LEFT JOIN` plus `IS NULL`.** To standardowy wzorzec „jest w A, nie ma w B”
(*anti-join*). `LEFT JOIN` zachowuje wszystkie koszyki i dokłada `NULL` tam, gdzie nie
znalazł pasującego zakupu; `WHERE b.customer_id IS NULL` zostawia właśnie te wiersze.
Równoważnie dałoby się użyć `NOT EXISTS`.

**Dlaczego klucz to klient i dzień.** Złączenie po samym `customer_id` uznałoby
za kupującego kogoś, kto porzucił koszyk 15.01, a kupił 16.01 — i zaniżyło wynik.
Warto przewidzieć, w którą stronę zmieni się liczba, przed uruchomieniem wariantu
bez daty w kluczu.

### Puenta dnia

Te dane to JSON ze strumienia zdarzeń. W klasycznym data warehouse trzeba by
zaprojektować dla nich model, zanim cokolwiek by się zapisało — i najprawdopodobniej
nikt by tego nie zrobił, bo w momencie zbierania logów nikt nie wiedział, że powstanie
pytanie o porzucone koszyki.

Tu wystarczyło je odłożyć i nałożyć schemat dopiero przy odczycie. To jest cała
teza dnia 1 pokazana pojedynczym zapytaniem.

---

## Dodatki po zajęciach (opcjonalne — nie są potrzebne w dniu 2 ani 3)

Skrypty dodatków: `sql/D1_add_partition.sql` … `sql/D5_path_bucketing.sql` (sprzątanie na końcu D5)
— zestaw został wykonany od początku do końca w Athenie engine v3 (`eu-central-1`).
Wszystkie liczby poniżej — także **Data scanned** — pochodzą z tego przebiegu.
Bajty są powtarzalne co do jednego, bo pliki są te same, które wgrywamy w L2.

Żaden z sześciu obiektów `_extra` nie jest używany w dniu 2 ani 3.

### D1 — `ADD PARTITION` kontra `MSCK REPAIR`

`bronze_events_extra` — jeden wiersz to jedno zdarzenie; te same pliki co bronze_events, własna lista partycji.

Pełny skrypt (identyczny z `sql/D1_add_partition.sql`):

```sql
-- =============================================================================
-- Dzien 1, D1 - ALTER TABLE ADD PARTITION kontra MSCK REPAIR
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
-- NIC Z TEGO PLIKU NIE JEST POTRZEBNE W DNIU 2 ANI 3.
-- Kazdy nowy obiekt ma w nazwie _extra; pliki CTAS laduja pod extra/.
-- =============================================================================

-- D1. ALTER TABLE ADD PARTITION kontra MSCK REPAIR
-- -----------------------------------------------------------------------------
-- Druga tabela na TYCH SAMYCH plikach co bronze_events. Katalog Glue trzyma
-- dla kazdej tabeli osobna liste partycji, wiec nowa tabela startuje z pusta.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_events_extra (
  event_id string,
  event_ts string,
  customer_id string,
  event_type string,
  page string,
  product_id int
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://aurora-jk-lake/bronze/events/';

-- Oczekiwane: 0 - pliki sa, partycji w katalogu nie ma.
SELECT COUNT(*) AS n FROM aurora_jk.bronze_events_extra;

-- Oczekiwane: pusty wynik.
SHOW PARTITIONS aurora_jk.bronze_events_extra;

-- Rejestracja JEDNEJ partycji. Athena nie listuje prefiksu - zapisuje w katalogu
-- to, co podano, i tyle. Tak dopisuje nowy dzien pipeline, ktory wlasnie go zapisal.
ALTER TABLE aurora_jk.bronze_events_extra
ADD IF NOT EXISTS PARTITION (dt = '2026-01-15')
LOCATION 's3://aurora-jk-lake/bronze/events/dt=2026-01-15/';

-- Oczekiwane: 434 (tylko 15.01). Data scanned = 64 616 B, czyli dokladnie
-- rozmiar pliku dt=2026-01-15/events.json - JSON czyta sie caly.
SELECT COUNT(*) AS n FROM aurora_jk.bronze_events_extra;

-- Oczekiwane: jeden wiersz, dt=2026-01-15
SHOW PARTITIONS aurora_jk.bronze_events_extra;

-- MSCK porownuje prefiks na S3 z katalogiem i dopisuje brakujace partycje.
-- Oczekiwany wynik (dwie linie):
--   Partitions not in metastore:    bronze_events_extra:dt=2026-01-16
--   Repair: Added partition to metastore bronze_events_extra:dt=2026-01-16
MSCK REPAIR TABLE aurora_jk.bronze_events_extra;

-- Oczekiwane: 2026-01-15 -> 434 | 2026-01-16 -> 501. Data scanned = 139 161 B
-- (64 616 + 74 545 - oba pliki).
SELECT dt, COUNT(*) AS n
FROM aurora_jk.bronze_events_extra
GROUP BY dt
ORDER BY dt;

-- DROP PARTITION kasuje WPIS w katalogu. Plik na S3 zostaje.
ALTER TABLE aurora_jk.bronze_events_extra DROP IF EXISTS PARTITION (dt = '2026-01-15');

-- Oczekiwane: 501 - tabela _extra widzi juz tylko 16.01 ...
SELECT COUNT(*) AS n FROM aurora_jk.bronze_events_extra;

-- ... a oryginalna bronze_events nadal 935. Te same pliki, dwa rozne katalogi.
SELECT COUNT(*) AS n FROM aurora_jk.bronze_events;

-- Przywrocenie brakujacej partycji, zeby tabela _extra byla znow kompletna.
-- Oczekiwane: "Repair: Added partition to metastore bronze_events_extra:dt=2026-01-15"
MSCK REPAIR TABLE aurora_jk.bronze_events_extra;

-- -----------------------------------------------------------------------------
```

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_events_extra (
  event_id string, event_ts string, customer_id string,
  event_type string, page string, product_id int
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://aurora-jk-lake/bronze/events/';

ALTER TABLE aurora_jk.bronze_events_extra
ADD IF NOT EXISTS PARTITION (dt = '2026-01-15')
LOCATION 's3://aurora-jk-lake/bronze/events/dt=2026-01-15/';

MSCK REPAIR TABLE aurora_jk.bronze_events_extra;

ALTER TABLE aurora_jk.bronze_events_extra DROP IF EXISTS PARTITION (dt = '2026-01-15');
```

| Krok | `COUNT(*)` na `_extra` | `SHOW PARTITIONS` | Data scanned |
|---|---|---|---|
| tuż po `CREATE` | 0 | pusty wynik | 0 B |
| po `ADD PARTITION (dt='2026-01-15')` | 434 | `dt=2026-01-15` | 64 616 B |
| po `MSCK REPAIR` | 434 + 501 = 935 (`GROUP BY dt ORDER BY dt`) | oba dni | 139 161 B |
| po `DROP PARTITION (dt='2026-01-15')` | 501 | `dt=2026-01-16` | 74 545 B |
| `bronze_events` w tym samym momencie | **935** | oba dni | 139 161 B |

`MSCK REPAIR` wypisuje, co zrobił — po `ADD PARTITION` jednego dnia są to dwie linie:

```
Partitions not in metastore:    bronze_events_extra:dt=2026-01-16
Repair: Added partition to metastore bronze_events_extra:dt=2026-01-16
```

Trzy rzeczy do zauważenia:

- **64 616 B i 74 545 B to dokładnie rozmiary plików** `events.json` z 15.01 i 16.01.
  JSON czyta się w całości — nie ma tu kolumn do pominięcia. Suma 139 161 B wraca
  w każdym zapytaniu, które dotyka obu dni.
- **Katalog jest per tabela.** `bronze_events` i `bronze_events_extra` wskazują ten
  sam prefiks, a mają niezależne listy partycji. Po `DROP PARTITION` na jednej druga
  nadal widzi 935 wierszy, a plik z 15.01 leży na S3 nietknięty.
- **`ADD PARTITION` nie listuje S3.** Zapisuje w katalogu to, co podano — nawet gdyby
  katalogu nie było. To operacja O(1), w przeciwieństwie do `MSCK`, który rośnie
  z liczbą katalogów pod prefiksem. Dlatego pipeline, który właśnie zapisał dzień,
  woła `ADD PARTITION` dla tego dnia, a `MSCK REPAIR` zostaje do jednorazowego
  „nadrobienia” po ręcznym uploadzie — jak w L3.

Ostatni `MSCK REPAIR` w skrypcie przywraca `dt=2026-01-15`, żeby tabela `_extra`
była znów kompletna.

### D2 — partition projection

`bronze_orders_extra` — jeden wiersz to jedno zamówienie; partycje dt wylicza Athena z reguły.

Pełny skrypt (identyczny z `sql/D2_partition_projection.sql`):

```sql
-- =============================================================================
-- Dzien 1, D2 - partition projection - partycje bez katalogu
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
-- NIC Z TEGO PLIKU NIE JEST POTRZEBNE W DNIU 2 ANI 3.
-- Kazdy nowy obiekt ma w nazwie _extra; pliki CTAS laduja pod extra/.
-- =============================================================================

-- D2. Partition projection - partycje wyliczane z reguly, bez katalogu
-- -----------------------------------------------------------------------------
-- Ten sam DDL co bronze_orders (ten sam LOCATION) plus wlasciwosci projection.*
-- Athena wylicza wszystkie daty z zakresu, buduje z nich sciezki
-- s3://.../bronze/orders/dt=<data>/ i czyta te, ktore istnieja. Zadne
-- MSCK REPAIR ani ADD PARTITION nie sa potrzebne - ani dzis, ani po nowym dniu.
--
-- Uklad katalogow dt=<wartosc> jest domyslny, wiec storage.location.template
-- nie jest tu potrzebny. Przydaje sie, gdy katalogi nazywaja sie inaczej,
-- np. .../2026/01/15/ - wtedy: 's3://aurora-jk-lake/bronze/orders/${dt}/'
-- z formatem yyyy/MM/dd.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_orders_extra (
  order_id string,
  customer_id string,
  order_ts string,
  status string,
  channel string,
  delivery_city string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/orders/'
TBLPROPERTIES (
  'skip.header.line.count'      = '1',
  'projection.enabled'          = 'true',
  'projection.dt.type'          = 'date',
  'projection.dt.range'         = '2026-01-01,NOW',
  'projection.dt.format'        = 'yyyy-MM-dd',
  'projection.dt.interval'      = '1',
  'projection.dt.interval.unit' = 'DAYS'
);

-- Oczekiwane, bez MSCK: 2026-01-15 -> 90 | 2026-01-16 -> 110.
-- Data scanned = 11 331 B (oba pliki CSV w calosci).
-- Bez filtra po dt Athena sprawdza KAZDY dzien z zakresu 2026-01-01..dzis -
-- na dwoch plikach niezauwazalne, przy tysiacach dni to koszt czasu, nie bajtow.
SELECT dt, COUNT(*) AS n
FROM aurora_jk.bronze_orders_extra
GROUP BY dt
ORDER BY dt;

-- Oczekiwane: 110. Data scanned = 6 232 B - tylko plik z 16.01.
SELECT COUNT(*) AS n FROM aurora_jk.bronze_orders_extra WHERE dt = '2026-01-16';

-- Oczekiwane: 0, bez bledu, Data scanned = 0 B. Dzien jest w zakresie reguly,
-- ale katalogu na S3 nie ma - Athena po prostu nie znajduje plikow.
SELECT COUNT(*) AS n FROM aurora_jk.bronze_orders_extra WHERE dt = '2026-02-01';

-- Oczekiwane: pusty wynik. Partycje nie istnieja w katalogu - sa wyliczane
-- w chwili zapytania. To odpowiedz na pytanie "gdzie one sa".
SHOW PARTITIONS aurora_jk.bronze_orders_extra;

-- -----------------------------------------------------------------------------
```

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_orders_extra (
  order_id string, customer_id string, order_ts string,
  status string, channel string, delivery_city string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/orders/'
TBLPROPERTIES (
  'skip.header.line.count'      = '1',
  'projection.enabled'          = 'true',
  'projection.dt.type'          = 'date',
  'projection.dt.range'         = '2026-01-01,NOW',
  'projection.dt.format'        = 'yyyy-MM-dd',
  'projection.dt.interval'      = '1',
  'projection.dt.interval.unit' = 'DAYS'
);
```

| Zapytanie (bez żadnego `MSCK REPAIR`) | Wynik | Data scanned |
|---|---|---|
| `SELECT dt, COUNT(*) … GROUP BY dt ORDER BY dt` | `2026-01-15` → 90, `2026-01-16` → 110 | 11 331 B |
| `COUNT(*) WHERE dt = '2026-01-16'` | 110 | 6 232 B |
| `COUNT(*) WHERE dt = '2026-02-01'` | 0 — bez błędu | 0 B |
| `SHOW PARTITIONS` | pusty wynik | — |

`SHOW PARTITIONS` jest pusty, bo partycje **nie istnieją w katalogu** — Athena wylicza
je z reguły w chwili zapytania. Dzień `2026-02-01` mieści się w zakresie reguły,
więc Athena buduje ścieżkę `…/bronze/orders/dt=2026-02-01/`, nie znajduje pod nią
plików i zwraca 0. Żadnego `MSCK`, żadnego `ADD PARTITION`, także po każdym nowym dniu.

Cena: przy zapytaniu **bez** filtra po `dt` Athena musi sprawdzić każdą datę z zakresu
`2026-01-01,NOW` — dziś to ok. 250 ścieżek, za rok ponad 600. Na dwóch plikach nie
widać tego wcale (11 331 B to oba CSV w całości, tyle samo co na `bronze_orders`);
przy dużym zakresie koszt jest w czasie, nie w bajtach. Zakres warto więc trzymać
możliwie ciasny, a zapytania bez filtra po dacie traktować jak sygnał ostrzegawczy.

Dwa domyślne ustawienia, które przeszły niezauważone: `projection.dt.interval = 1`
z jednostką `DAYS` (Athena przyjęłaby też np. `1 HOURS`), a układ katalogów
`dt=<wartość>` jest domyślny — właściwość `storage.location.template` byłaby potrzebna
tylko przy katalogach nazwanych inaczej, np. `…/2026/01/15/`.

### D3 — widok na Silver

`v_revenue_by_day_extra` to zapisane zapytanie. `silver_revenue_by_day_extra` — jeden wiersz to jeden dzień z przychodem.

Pełny skrypt (identyczny z `sql/D3_widok_silver.sql`):

```sql
-- =============================================================================
-- Dzien 1, D3 - widok na Silver
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
-- NIC Z TEGO PLIKU NIE JEST POTRZEBNE W DNIU 2 ANI 3.
-- Kazdy nowy obiekt ma w nazwie _extra; pliki CTAS laduja pod extra/.
-- =============================================================================

-- D3. Widok na Silver - zapisane zapytanie, nie zapisane dane
-- -----------------------------------------------------------------------------
-- ORDER BY nie wchodzi do definicji widoku: kolejnosc ustala zapytanie,
-- ktore z widoku czyta.
CREATE OR REPLACE VIEW aurora_jk.v_revenue_by_day_extra AS
SELECT dt, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
GROUP BY dt;

-- Oczekiwane: 2026-01-15 -> 653531.00 | 2026-01-16 -> 773593.00
-- Data scanned = 1 717 B - identycznie jak wariant 2 z L6. Widok nic nie skraca.
SELECT dt, revenue_pln
FROM aurora_jk.v_revenue_by_day_extra
ORDER BY dt;

-- Oczekiwane: 773593.00. Data scanned = 940 B - tyle samo co wariant 3 z L6.
-- Filtr nalozony NA WIDOK zostal przepchniety do srodka i wycial partycje.
SELECT dt, revenue_pln
FROM aurora_jk.v_revenue_by_day_extra
WHERE dt = '2026-01-16';

-- To samo bez widoku - dla porownania Data scanned (1 717 B).
SELECT dt, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
GROUP BY dt
ORDER BY dt;

-- Definicja widoku, tak jak ja pamieta katalog.
SHOW CREATE VIEW aurora_jk.v_revenue_by_day_extra;

-- Widok w katalogu Glue to tez "tabela", tylko innego typu.
-- Oczekiwane m.in.: v_revenue_by_day_extra | VIEW, reszta BASE TABLE.
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'aurora_jk'
ORDER BY table_name;

-- Ta sama tresc ZAPISANA jako dane (CTAS). Skan przy odczycie spada
-- z 1 717 B do 113 B - ale to zdjecie z chwili utworzenia: nowy dzien
-- w silver_order_items tu nie trafi, dopoki tabela nie zostanie przeliczona.
CREATE TABLE IF NOT EXISTS aurora_jk.silver_revenue_by_day_extra
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/extra/silver_revenue_by_day/'
) AS
SELECT dt, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
GROUP BY dt;

-- Oczekiwane: te same dwie kwoty, Data scanned = 113 B.
SELECT dt, revenue_pln
FROM aurora_jk.silver_revenue_by_day_extra
ORDER BY dt;

-- -----------------------------------------------------------------------------
```

```sql
CREATE OR REPLACE VIEW aurora_jk.v_revenue_by_day_extra AS
SELECT dt, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
GROUP BY dt;

CREATE TABLE IF NOT EXISTS aurora_jk.silver_revenue_by_day_extra
WITH (
  format            = 'PARQUET',
  external_location = 's3://aurora-jk-lake/extra/silver_revenue_by_day/'
) AS
SELECT dt, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items
GROUP BY dt;
```

Wszystkie cztery odczyty dają te same kwoty: 653 531,00 (15.01) i 773 593,00 (16.01),
w kolejności `ORDER BY dt`.

| Odczyt | Data scanned |
|---|---|
| `SELECT … FROM v_revenue_by_day_extra ORDER BY dt` | 1 717 B |
| to samo z `WHERE dt = '2026-01-16'` | 940 B |
| L6 wariant 2 wpisany wprost, bez widoku | 1 717 B |
| `SELECT … FROM silver_revenue_by_day_extra ORDER BY dt` (CTAS) | **113 B** |

1 717 B i 940 B to **dokładnie** liczby z L6 (wariant 2 i 3). Widok jest zapisanym
tekstem zapytania: przy każdym odczycie Athena wkleja go w miejsce nazwy i wykonuje
od zera, więc skan jest identyczny jak bez widoku. Filtr `WHERE dt` nałożony na widok
został przepchnięty do środka (*predicate pushdown* przez widok) i wyciął partycję —
tak samo, jak zrobiłby to na tabeli.

CTAS to odwrotny kompromis: 113 B, bo czytany jest gotowy wynik (dwa wiersze
w Parquecie), ale jest to zdjęcie z chwili utworzenia — trzeci dzień w
`silver_order_items` nie pojawi się w `silver_revenue_by_day_extra`, dopóki tabela
nie zostanie przeliczona. Widok „widzi” go od razu.

Dwie rzeczy z katalogu:

- `SHOW CREATE VIEW` pokazuje zapytanie tak, jak zapamiętał je katalog — przeformatowane,
  z aliasami bez `AS`, bez `ORDER BY` (jego w definicji widoku nie ma; kolejność ustala
  zapytanie czytające).
- `information_schema.tables` dla `aurora_jk` zwraca `v_revenue_by_day_extra | VIEW`,
  a wszystko inne jako `BASE TABLE` — widok w Glue jest wpisem tego samego rodzaju,
  z innym typem.

### D4 — JSON jako `raw string`

`bronze_events_raw_extra` — jeden wiersz to jedna linia JSON w kolumnie raw. CROSS JOIN UNNEST rozwija listę kluczy JSON na wiersze, żeby zobaczyć schemat z danych.

Pełny skrypt (identyczny z `sql/D4_json_raw.sql`):

```sql
-- =============================================================================
-- Dzien 1, D4 - JSON jako jedna kolumna raw
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
-- NIC Z TEGO PLIKU NIE JEST POTRZEBNE W DNIU 2 ANI 3.
-- Kazdy nowy obiekt ma w nazwie _extra; pliki CTAS laduja pod extra/.
-- =============================================================================

-- D4. Ten sam JSON trzeci raz - jedna kolumna raw i funkcje JSON
-- -----------------------------------------------------------------------------
-- Zadnego SerDe do JSON: kazda linia pliku to jeden wiersz w kolumnie raw.
-- Separator \001 nie wystepuje w plikach, wiec linia wchodzi w calosci.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_events_raw_extra (
  raw string
)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/events/';

MSCK REPAIR TABLE aurora_jk.bronze_events_raw_extra;

-- Trzy surowe linie, dokladnie tak jak leza w pliku.
SELECT raw FROM aurora_jk.bronze_events_raw_extra LIMIT 3;

-- Oczekiwane: 935 - tyle samo co bronze_events.
SELECT COUNT(*) AS n FROM aurora_jk.bronze_events_raw_extra;

-- Odkrywanie schematu: jakie klucze sa w plikach i ile razy.
-- json_parse zamienia tekst na typ JSON, CAST na MAP daje slownik klucz -> wartosc,
-- map_keys wyciaga same klucze, UNNEST rozwija tablice kluczy na wiersze.
-- Oczekiwane: 6 kluczy, kazdy 935 razy:
--   customer_id | event_id | event_ts | event_type | page | product_id
SELECT k AS klucz, COUNT(*) AS n
FROM aurora_jk.bronze_events_raw_extra
CROSS JOIN UNNEST(map_keys(CAST(json_parse(raw) AS MAP(varchar, json)))) AS t(k)
GROUP BY k
ORDER BY k;

-- json_extract_scalar wyciaga jedna wartosc sciezka JSONPath - zawsze jako varchar.
-- Oczekiwane (event_type | n | n_with_product):
--   add_to_cart | 184 | 184
--   page_view   | 421 | 421
--   purchase    | 171 | 171
--   search      | 159 |   0
-- Czwarty typ zdarzenia, search, nie pojawil sie ani w L3, ani w S2 - i ma
-- product_id = null w kazdym wierszu. Klucz jest, wartosci nie ma.
SELECT
  json_extract_scalar(raw, '$.event_type') AS event_type,
  COUNT(*) AS n,
  COUNT(json_extract_scalar(raw, '$.product_id')) AS n_with_product
FROM aurora_jk.bronze_events_raw_extra
GROUP BY 1
ORDER BY 1;

-- Dostep "przez kropke": CAST na ROW nadaje tekstowi strukture z nazwanymi polami
-- i typami. Pola, ktorych nie ma w ROW, sa pomijane; null zostaje null-em.
-- Oczekiwane (pierwsze trzy po event_id):
--   B000801 | page_view | 117
--   B000802 | page_view | 124
--   B000803 | search    | (null)
SELECT
  e.event_id,
  e.event_type,
  e.product_id
FROM (
  SELECT CAST(json_parse(raw) AS ROW(event_id varchar, event_type varchar, product_id integer)) AS e
  FROM aurora_jk.bronze_events_raw_extra
)
ORDER BY e.event_id
LIMIT 3;

-- Kontrola: te same liczby z tabeli czytanej przez JsonSerDe.
-- Oczekiwane: add_to_cart 184 | page_view 421 | purchase 171 | search 159
SELECT event_type, COUNT(*) AS n
FROM aurora_jk.bronze_events
GROUP BY event_type
ORDER BY event_type;

-- -----------------------------------------------------------------------------
```

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_events_raw_extra (raw string)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/events/';

MSCK REPAIR TABLE aurora_jk.bronze_events_raw_extra;

-- 2. jakie klucze są w plikach
SELECT k AS klucz, COUNT(*) AS n
FROM aurora_jk.bronze_events_raw_extra
CROSS JOIN UNNEST(map_keys(CAST(json_parse(raw) AS MAP(varchar, json)))) AS t(k)
GROUP BY k
ORDER BY k;

-- 3. rozkład event_type i ile zdarzeń ma product_id
SELECT
  json_extract_scalar(raw, '$.event_type') AS event_type,
  COUNT(*) AS n,
  COUNT(json_extract_scalar(raw, '$.product_id')) AS n_with_product
FROM aurora_jk.bronze_events_raw_extra
GROUP BY 1
ORDER BY 1;

-- 4. dostęp przez kropkę
SELECT e.event_id, e.event_type, e.product_id
FROM (
  SELECT CAST(json_parse(raw) AS ROW(event_id varchar, event_type varchar, product_id integer)) AS e
  FROM aurora_jk.bronze_events_raw_extra
)
ORDER BY e.event_id
LIMIT 3;
```

`COUNT(*)` = **935**, tyle samo co `bronze_events`. Separator `\001` nie występuje
w plikach, więc każda linia wchodzi w całości jako jedna wartość `raw`.

Klucze (`ORDER BY k`): `customer_id`, `event_id`, `event_ts`, `event_type`, `page`,
`product_id` — każdy **935** razy. Każdy wiersz ma komplet kluczy; braki są w
wartościach, nie w kluczach.

| `event_type` | n | `n_with_product` |
|---|---|---|
| `add_to_cart` | 184 | 184 |
| `page_view` | 421 | 421 |
| `purchase` | 171 | 171 |
| `search` | 159 | **0** |

Suma 184 + 421 + 171 + 159 = 935. To samo zapytanie na `bronze_events`
(`GROUP BY event_type ORDER BY event_type`) daje identyczne cztery liczby.

Pierwsze trzy rekordy po `event_id`: `B000801 | page_view | 117`,
`B000802 | page_view | 124`, `B000803 | search | NULL`.

Trzy rzeczy warte zauważenia:

- **Czwarty typ zdarzenia.** L3 i S2 mówią o `page_view`, `add_to_cart` i `purchase`.
  `search` jest w plikach od początku — 159 wierszy — i nikt go nie potrzebował, więc
  nikt go nie zauważył. To jest schema-on-read w praktyce: dane leżą, pytanie decyduje,
  co z nich wyjąć.
- **`null` w `product_id` jest tylko przy `search`** — wyszukiwanie nie dotyczy
  konkretnego produktu. `COUNT(kolumna)` pomija `null`, stąd 0 przy 159 wierszach.
  Klucz w JSON-ie jest, wartości nie ma; `JsonSerDe` w L3 zrobił z tego po prostu
  `NULL` w kolumnie `int`.
- **Trzy narzędzia, trzy zastosowania.** `json_extract_scalar` to szybki dostęp do
  jednej wartości (zawsze jako `varchar` — stąd `CAST`, jeśli potrzebna liczba);
  `CAST … AS MAP` odkrywa nieznany schemat; `CAST … AS ROW` nadaje rekordowi nazwane,
  typowane pola i pozwala pisać `e.product_id` jak w zwykłej tabeli. Pola nieobecne
  w `ROW` są pomijane, pola z `null` zostają `null`.

Każde zapytanie na `raw` skanuje 139 161 B — całe oba pliki, tak samo jak przez
`JsonSerDe`. Sposób czytania JSON-a nie zmienia rachunku; zmienia go dopiero Parquet.

### D5 — `"$path"` i bucketing

`silver_order_items_bucketed_extra` ma ten sam grain co Silver pozycji (409), pliki pocięte haszem customer_id.

Pełny skrypt (identyczny z `sql/D5_path_bucketing.sql`):

```sql
-- =============================================================================
-- Dzien 1, D5 - pseudokolumna "$path" i bucketing
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk / aurora-jk-lake.
-- NIC Z TEGO PLIKU NIE JEST POTRZEBNE W DNIU 2 ANI 3.
-- Kazdy nowy obiekt ma w nazwie _extra; pliki CTAS laduja pod extra/.
-- =============================================================================

-- D5. Pliki pod tabela - pseudokolumna "$path" i bucketing
-- -----------------------------------------------------------------------------
-- "$path" to pelna sciezka pliku, z ktorego pochodzi wiersz. Cudzyslow jest
-- konieczny, bo $ nie jest dozwolony w zwyklej nazwie kolumny.
-- Oczekiwane: 2 pliki, jeden na partycje: dt=2026-01-15 -> 186 | dt=2026-01-16 -> 223.
-- Data scanned = 0 B - odpowiedz jest w metadanych, Athena nie otwiera plikow.
SELECT "$path" AS plik, COUNT(*) AS n
FROM aurora_jk.silver_order_items
GROUP BY "$path"
ORDER BY "$path";

-- Ta sama tabela z bucketingiem: w kazdej partycji wiersze rozrzucone po 8 plikach
-- wedlug hasza customer_id. Ten sam klient trafia zawsze do tego samego pliku.
CREATE TABLE IF NOT EXISTS aurora_jk.silver_order_items_bucketed_extra
WITH (
  format              = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by      = ARRAY['dt'],
  bucketed_by         = ARRAY['customer_id'],
  bucket_count        = 8,
  external_location   = 's3://aurora-jk-lake/extra/silver_order_items_bucketed/'
) AS
SELECT * FROM aurora_jk.silver_order_items;

-- Oczekiwane: 16 plikow (8 na partycje), od 5 do 38 wierszy w pliku.
SELECT "$path" AS plik, COUNT(*) AS n
FROM aurora_jk.silver_order_items_bucketed_extra
GROUP BY "$path"
ORDER BY "$path";

-- Pytanie punktowe o jednego klienta. Oczekiwane: 7 pozycji, 29266.00.
-- Data scanned: 3 041 B na silver_order_items ...
SELECT COUNT(*) AS items, ROUND(SUM(net_amount_pln), 2) AS net_pln
FROM aurora_jk.silver_order_items
WHERE customer_id = 'C0001';

-- ... i 690 B na wersji z bucketingiem (4x mniej): Athena otwiera w kazdej
-- partycji tylko ten jeden plik, w ktorym moze byc C0001 (bucket pruning).
SELECT COUNT(*) AS items, ROUND(SUM(net_amount_pln), 2) AS net_pln
FROM aurora_jk.silver_order_items_bucketed_extra
WHERE customer_id = 'C0001';

-- Pelna suma. Oczekiwane 1427124.00 w obu.
-- Data scanned: 1 717 B na 2 plikach ...
SELECT ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items;

-- ... i 2 614 B na 16 plikach. Kazdy plik Parquet ma wlasna stopke z metadanymi;
-- przy 16 malych plikach stopki waza wiecej niz dane. To jest problem
-- "many small files" w miniaturze - bucketing (i za drobne partycje) oplaca sie
-- tylko wtedy, gdy zapytania faktycznie filtruja po tej kolumnie.
SELECT ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.silver_order_items_bucketed_extra;

-- =============================================================================

-- SPRZATANIE DODATKOW
-- =============================================================================
-- DROP TABLE na tabelach EXTERNAL kasuje tylko wpis w katalogu - pliki
-- bronze/events/ i bronze/orders/ zostaja, bo naleza do tabel z L3.
-- Pliki utworzone przez CTAS (prefiks extra/) trzeba usunac osobno w konsoli S3:
-- bucket -> zaznaczenie katalogu extra/ -> Delete.
-- Sprzatanie z dnia 3 (DROP DATABASE ... CASCADE + kasowanie bucketu) i tak
-- obejmie wszystko, wiec ten krok mozna zostawic na koniec kursu.
DROP TABLE IF EXISTS aurora_jk.bronze_events_extra;
DROP TABLE IF EXISTS aurora_jk.bronze_orders_extra;
DROP VIEW  IF EXISTS aurora_jk.v_revenue_by_day_extra;
DROP TABLE IF EXISTS aurora_jk.silver_revenue_by_day_extra;
DROP TABLE IF EXISTS aurora_jk.bronze_events_raw_extra;
DROP TABLE IF EXISTS aurora_jk.silver_order_items_bucketed_extra;
```

```sql
SELECT "$path" AS plik, COUNT(*) AS n
FROM aurora_jk.silver_order_items
GROUP BY "$path"
ORDER BY "$path";

CREATE TABLE IF NOT EXISTS aurora_jk.silver_order_items_bucketed_extra
WITH (
  format              = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by      = ARRAY['dt'],
  bucketed_by         = ARRAY['customer_id'],
  bucket_count        = 8,
  external_location   = 's3://aurora-jk-lake/extra/silver_order_items_bucketed/'
) AS
SELECT * FROM aurora_jk.silver_order_items;
```

`silver_order_items` ma **2 pliki**, jeden na partycję: `dt=2026-01-15/…` → 186 wierszy,
`dt=2026-01-16/…` → 223 (razem 409). Wersja z bucketingiem ma **16 plików** — po 8 na
partycję, nazwanych `000000_0_…` do `000007_0_…`, od 5 do 38 wierszy w pliku
(hasz `customer_id` rozkłada klientów nierówno). Oba zapytania o `"$path"` pokazują
**0 B** skanu — odpowiedź jest w metadanych, Athena nie otwiera plików.

| Zapytanie | `silver_order_items` (2 pliki) | `…_bucketed_extra` (16 plików) |
|---|---|---|
| `WHERE customer_id = 'C0001'` → 7 pozycji, 29 266,00 | 3 041 B | **690 B** |
| `SUM(net_amount_pln)` bez filtra → 1 427 124,00 | **1 717 B** | 2 614 B |

Ten sam wynik, przeciwne rachunki:

- **Pytanie punktowe** czyta z bucketingiem 4,4× mniej. Athena liczy hasz z `'C0001'`,
  wie, w którym z ośmiu plików każdej partycji mógłby być ten klient, i otwiera tylko
  te dwa (*bucket pruning*). Bez bucketingu czyta kolumny `customer_id`
  i `net_amount_pln` z obu plików w całości.
- **Pełna suma** czyta z bucketingiem 1,5× więcej. Każdy plik Parquet ma własną stopkę
  z metadanymi i statystykami; przy 16 plikach po kilkadziesiąt wierszy stopki ważą
  więcej niż dane. To problem *many small files* w miniaturze — tu 900 B, na produkcji
  tysiące plików po kilka KB, każdy z osobnym żądaniem do S3.

Bucketing (tak jak zbyt drobne partycje) opłaca się tylko wtedy, gdy zapytania
faktycznie filtrują po tej kolumnie — i to często. Dla dashboardu „przychód wg dnia”
z L6 dwa pliki na partycję są lepsze niż szesnaście.

Nazwy plików pod `silver/order_items/` (`20260830_112603_00358_cnw2k_…`) to
identyfikatory zapytań CTAS z L5 — każdy przebieg CTAS zostawia inny.

### D6 — Glue Crawler: sprawdzone, że się nie da

Próba utworzenia crawlera w roli `PowerUserAccess` (`Create crawler` z rolą
`AWSGlueServiceRole`, która w koncie istnieje) kończy się:

```
AccessDeniedException: User: arn:aws:sts::123456789012:assumed-role/PowerUserAccess/session
is not authorized to perform: iam:PassRole on resource: arn:aws:iam::123456789012:role/AWSGlueServiceRole
```

Utworzenie własnej roli (`iam:CreateRole`) jest odcięte tak samo. Dlatego D6 jest
tylko do przeczytania — a przy okazji dobrze pokazuje, dlaczego dzień 1 stoi na
jawnym DDL zamiast na crawlerze: DDL działa w każdej roli, która ma Athenę i Glue,
i jest kodem, który da się przeczytać i wersjonować.

### Sprzątanie

Skrypt kończy się sześcioma `DROP … IF EXISTS`. `DROP TABLE` na tabelach zewnętrznych
usuwa wpis w katalogu i **nie** kasuje plików — `bronze/events/` i `bronze/orders/`
zostają dla tabel z L3. Pliki dwóch CTAS-ów pod `extra/` (17 obiektów) trzeba usunąć
w konsoli S3 osobno. Po pełnym przebiegu i sprzątaniu zapytanie kontrolne z L3
nadal daje 80 | 935 | 486 | 200 | 25, a Silver 80 | 409 | 170 | 25 — sprawdzone.
