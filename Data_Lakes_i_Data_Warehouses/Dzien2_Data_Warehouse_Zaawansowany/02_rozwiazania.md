# Dzień 2 — Data Warehouse, wersja zaawansowana (rozwiązania i liczby kontrolne)

Wszystkie wyniki poniżej pochodzą z rzeczywistego przebiegu w konsoli AWS
(Athena engine v3, Redshift Serverless, `eu-central-1`), wykonanego w kolejności
Z0 → Z6, dwukrotnie tam, gdzie plik deklaruje idempotencję. Kolejność wierszy
jest dokładnie taka, jaką zwraca `ORDER BY` w zapytaniu.

Konwencja: ten plik jest jedną ścieżką czytania „jak”. Każde ćwiczenie ma
zdanie, co powstało, pełny SQL (identyczny z plikiem w `sql/`) i liczby
kontrolne z etykietą. Paste 1:1: `sql/Z0_stan_wejsciowy.sql` …
`sql/Z6_redshift.sql`, `sql/Z6g_athena_po_unload.sql`.

---

## Liczby kontrolne w jednym miejscu

| Co | Wartość |
|---|---|
| `dw_dim_date` — jeden wiersz to jeden dzień kalendarzowy | 366 wierszy (365 dni roku 2026 + wiersz nieznany -1), 104 weekendy, `date_sk` od -1 do 20261231 |
| `dw_dim_customer` — jeden wiersz to jeden klient (aktualny stan, `is_current`) | 81 (80 klientów + wiersz nieznany -1), wszystkie `is_current = true`, `customer_sk` 1–80 |
| `dw_dim_product` — jeden wiersz to jeden produkt | 26 (25 + wiersz -1); price_band: budget 4, mid 6, premium 15, unknown 1 |
| `dw_dim_order_status` — jeden wiersz to para status × kanał | 9 (3 statusy × 3 kanały), `is_revenue = false` dla sk 1–3 (cancelled) |
| `bronze_shipments` — jeden wiersz to jedna wysyłka | 66 wysyłek, 45 dostarczonych, 21 w drodze (puste `delivered_ts`) |
| `dw_fact_order_item` — jeden wiersz to jedna pozycja zamówienia | 409 pozycji (nie 486: cancelled odpadły w Silver dnia 1), 1 427 124,00 zł, zero kluczy -1; partycje 15.01 = 186 pozycji, 16.01 = 223 |
| `dw_fact_order_fulfillment` — jeden wiersz to jedno zamówienie | 170 zamówień, 66 wysłanych, 45 dostarczonych, 104 bez wysyłki, 1 427 124,00 zł |
| `dw_fact_daily_sales` — jeden wiersz to dzień × kategoria × kanał | 36 wierszy, 1 427 124,00 zł (ta sama kwota co 409 pozycji) |
| Asercje Z4 | 8 × OK |
| ROLLUP dzień × kanał | 15.01: marketplace 233 010 · mobile 226 929 · web 193 592 · razem 653 531; 16.01: 288 582 · 225 598 · 259 413 · razem 773 593; razem 1 427 124 |
| GROUPING SETS | kanały: marketplace 521 592 · mobile 452 527 · web 453 005; kategorie: AGD 261 065 · Akcesoria 13 423 · Audio 83 254 · Komputery 362 605 · TV 324 800 · Telefony 381 977 |
| AOV | 8 394,85 (170 zamówień) vs średnia pozycji 3 489,30 (409 pozycji) |
| Redshift lokalnie | 81 / 366 / 9 / 26 / 409; MV 36 wierszy, 1 427 124,00 |

---

## Z0. Stan wejściowy

`silver_orders` — jeden wiersz to jedno zamówienie paid/shipped. `silver_order_items` — jeden wiersz to jedna pozycja takiego zamówienia (409, nie 486).

Pełny skrypt (identyczny z `sql/Z0_stan_wejsciowy.sql`):

```sql
-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z0 - stan wejsciowy Silver z dnia 1
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>.
-- Te cztery liczby to punkt odniesienia na caly dzien. Kazda tabela Gold ma
-- sie do nich rekoncyliowac. 409 (nie 486) to pozycje po odfiltrowaniu
-- zamowien cancelled w Silver - ten sam JOIN co w L5 dnia 1.
-- =============================================================================

SELECT 'orders' AS tabela, COUNT(*) AS n FROM aurora_jk.silver_orders
UNION ALL SELECT 'order_items', COUNT(*) FROM aurora_jk.silver_order_items
UNION ALL SELECT 'customers',   COUNT(*) FROM aurora_jk.silver_customers
UNION ALL SELECT 'products',    COUNT(*) FROM aurora_jk.silver_products
ORDER BY tabela;

-- Oczekiwane (ORDER BY tabela):
--   customers 80  - silver_customers: jeden wiersz to jeden klient
--   order_items 409 - silver_order_items: jeden wiersz to jedna pozycja paid/shipped
--   orders 170 - silver_orders: jeden wiersz to jedno zamowienie paid/shipped
--   products 25 - silver_products: jeden wiersz to jeden produkt
```

```text
tabela      | n
customers   | 80
order_items | 409
orders      | 170
products    | 25
```

Inne liczby oznaczają, że Silver z dnia 1 jest inny niż zakładamy (np. brak
filtra statusu w `silver_order_items` → 486 pozycji zamiast 409). Wtedy wszystkie
dzisiejsze liczby kontrolne będą inne, ale *spójne* — rekoncyliacja Silver = Gold
nadal ma się zgadzać. 409 kontra 486 to ten sam JOIN co w L5 dnia 1.

---

## Z1. `dw_dim_date`

`dw_dim_date` to wymiar daty: jeden wiersz to jeden dzień kalendarzowy 2026, plus wiersz nieznany (`date_sk = -1`). CTAS jest pełnym odświeżeniem (full refresh): tabela powstaje od zera.

Pełny skrypt (identyczny z `sql/Z1_wymiar_daty.sql`):

```sql
-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z1 - wymiar daty jako pelny kalendarz Iceberg
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Warstwa Gold wersji zaawansowanej nazywa sie dw_* i lezy pod
-- s3://aurora-<inicjaly>-lake/dw/, zeby nie zderzac sie z gold_* z wersji
-- podstawowej.
--
-- Iceberg zamiast zwyklego CTAS do Parquetu, bo DROP TABLE kasuje takze pliki,
-- tabela przyjmuje INSERT / UPDATE / DELETE / MERGE, a kazda zmiana to snapshot.
-- Typ ladowania: pelne odswiezenie (full refresh / CTAS replace) — cala tabela od zera.
-- Trzy parametry CTAS, ktorych nie bylo w L5:
--   table_type  = 'ICEBERG'
--   location    = 's3://...'   (nie external_location)
--   is_external = false
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dw_dim_date: jeden wiersz to jeden dzien kalendarzowy 2026 (plus wiersz -1).
-- -----------------------------------------------------------------------------
-- W wersji podstawowej wymiar daty powstaje z dat wystepujacych w sprzedazy
-- (2 wiersze) i w dniu 3 trzeba go doklejac. Tu generujemy 365 dni z gory.
-- UNNEST(sequence(...)) rozpina kalendarz: sequence() buduje tablice timestampow
-- od 1 stycznia do 31 grudnia, UNNEST robi z niej wiersze. Bez tego nie ma
-- wymiaru daty niezaleznego od faktu i 20 stycznia znowu "znika" z raportu.
--
-- date_sk w konwencji RRRRMMDD (20260115) - czytelny przy debugowaniu i stabilny
-- miedzy przeladowaniami. day_of_week(): 1 = poniedzialek ... 7 = niedziela.
-- week() to numer tygodnia ISO. date_format() oczekuje timestampa, stad CAST.
--
-- PULAPKA (sprawdzona w Athenie v3): sequence(DATE, DATE, INTERVAL '1' DAY) zwraca
-- tablice TIMESTAMPOW, nie dat. Bez CAST(d AS date) kolumna date_actual
-- powstaje jako timestamp, a zlaczenie z order_date (typ date) idzie przez
-- niejawne rzutowanie i wyniki wyswietlaja sie jako 2026-01-15 00:00:00.000000.
CREATE TABLE IF NOT EXISTS aurora_jk.dw_dim_date
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/dw/dim_date/',
  is_external = false
) AS
SELECT
  CAST(year(d) * 10000 + month(d) * 100 + day(d) AS integer) AS date_sk,
  CAST(d AS date)                                AS date_actual,
  CAST(year(d) AS integer)                       AS year,
  CAST(quarter(d) AS integer)                    AS quarter,
  CAST(month(d) AS integer)                      AS month,
  date_format(d, '%M')                           AS month_name,
  CAST(day(d) AS integer)                        AS day,
  CAST(day_of_week(d) AS integer)                AS day_of_week,
  date_format(d, '%W')                           AS day_name,
  CAST(week(d) AS integer)                       AS iso_week,
  day_of_week(d) IN (6, 7)                       AS is_weekend
FROM UNNEST(sequence(DATE '2026-01-01', DATE '2026-12-31', INTERVAL '1' DAY)) AS t(d);

-- Wiersz nieznany. Fakt nigdy nie traci wiersza przez brak daty: zamowienie bez
-- wysylki dostaje ship_date_sk = -1 zamiast wypadac na INNER JOIN. Wzorzec
-- Kimballa: kazdy wymiar ma wiersz "unknown", a fakt zawsze ma do czego sie
-- zlaczyc. WHERE NOT EXISTS czyni INSERT idempotentnym.
INSERT INTO aurora_jk.dw_dim_date
SELECT -1, DATE '1900-01-01', 1900, 1, 1, 'Nieznany', 1, 1, 'Nieznany', 1, false
WHERE NOT EXISTS (SELECT 1 FROM aurora_jk.dw_dim_date WHERE date_sk = -1);

-- Oczekiwane: dni = 366 (365 + wiersz -1), weekendy = 104, pierwszy = -1,
-- ostatni = 20261231.
SELECT
  COUNT(*)                                          AS dni,
  SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END)       AS weekendy,
  MIN(date_sk)                                      AS pierwszy,
  MAX(date_sk)                                      AS ostatni
FROM aurora_jk.dw_dim_date;

-- -----------------------------------------------------------------------------
```

```text
dni | weekendy | pierwszy | ostatni
366 | 104      | -1       | 20261231
```

Drugie uruchomienie pliku: identyczny wynik. `CREATE TABLE IF NOT EXISTS` pomija
CTAS, `INSERT ... WHERE NOT EXISTS` wstawia 0 wierszy — i, co ciekawe, **nie
tworzy nowego snapshotu** (sprawdzone w `$snapshots`).

### Pułapki

- **`sequence()` zwraca timestampy.** `typeof(d)` dla
  `UNNEST(sequence(DATE ..., DATE ..., INTERVAL '1' DAY))` to `timestamp(0)`.
  Bez `CAST(d AS date)` kolumna `date_actual` powstaje jako `timestamp(6)`,
  złączenia z `order_date` działają przez niejawne rzutowanie, ale każda data
  w raportach wyświetla się jako `2026-01-15 00:00:00.000000`. Pierwsza wersja
  materiału miała ten błąd — naprawa to `DROP TABLE` (Iceberg kasuje pliki) i
  ponowny CTAS.
- **`is_external = false` jest obowiązkowe** przy CTAS do Icebergu; z `true`
  Athena odrzuca zapytanie.
- **`location`, nie `external_location`.** Nazwy parametrów różnią się od
  hive'owego CTAS-a z dnia 1.
- Iceberg w Athenie zapisuje domyślnie **ZSTD** (widoczne w
  `SHOW TBLPROPERTIES`: `write_compression ZSTD`), mimo `format = 'PARQUET'` —
  Parquet to kontener, ZSTD to kodek.

---

## Z2. Wymiary

`dw_dim_customer` — jeden wiersz to jeden klient w aktualnym stanie (`is_current`), z surrogate key `customer_sk`. `dw_dim_product` — jeden wiersz to jeden produkt. `dw_dim_order_status` — jeden wiersz to jedna para status × kanał.

Pełny skrypt (identyczny z `sql/Z2_wymiary.sql`):

```sql
-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z2 - wymiary customer, product, junk dimension
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Wymaga dw_dim_date z sql/Z1_wymiar_daty.sql (kontrola na koncu liczy tez date).
-- =============================================================================

-- dw_dim_customer: jeden wiersz to jeden klient w aktualnym stanie (is_current),
-- z surrogate key customer_sk. Kolumny SCD2 sa od razu, choc dzis kazdy ma jedna wersje.
-- -----------------------------------------------------------------------------
-- Kolumny valid_from / valid_to / is_current sa od razu, choc dzis kazdy klient
-- ma jedna wersje. Wartosci brzegowe 1900-01-01 i 9999-12-31 zamiast NULL:
-- BETWEEN z NULL-em nie dopasowuje niczego i fakt gubi wiersze po cichu.
--
-- customer_sk z ROW_NUMBER() OVER (ORDER BY customer_id) - kolejnosc jest
-- deterministyczna, wiec ten sam klient dostaje ten sam klucz przy kazdym
-- przeliczeniu. Jutro nowe wersje dostana klucze max(customer_sk) + n.
--
-- signup_month to atrybut pochodny pod analize kohort (Z5): liczy sie raz,
-- w wymiarze, a nie w kazdym zapytaniu.
CREATE TABLE IF NOT EXISTS aurora_jk.dw_dim_customer
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/dw/dim_customer/',
  is_external = false
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
  date_format(CAST(signup_date AS timestamp), '%Y-%m') AS signup_month,
  DATE '1900-01-01' AS valid_from,
  DATE '9999-12-31' AS valid_to,
  true              AS is_current
FROM aurora_jk.silver_customers;

INSERT INTO aurora_jk.dw_dim_customer
SELECT -1, 'UNKNOWN', 'Nieznany', 'Nieznany', 'Nieznany', 'Nieznany', 'Nieznany',
       DATE '1900-01-01', '1900-01', DATE '1900-01-01', DATE '9999-12-31', true
WHERE NOT EXISTS (SELECT 1 FROM aurora_jk.dw_dim_customer WHERE customer_sk = -1);

-- -----------------------------------------------------------------------------
-- dw_dim_product: jeden wiersz to jeden produkt (plus wiersz -1), z price_band.
-- -----------------------------------------------------------------------------
-- unit_price_pln z produktu to cena KATALOGOWA (list_price_pln). Cena
-- rzeczywiscie zaplacona siedzi na fakcie. price_band to klasyczny atrybut
-- "do grupowania" - biznes pyta o przedzialy cenowe, nie o kwoty.
CREATE TABLE IF NOT EXISTS aurora_jk.dw_dim_product
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/dw/dim_product/',
  is_external = false
) AS
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY product_id) AS integer) AS product_sk,
  product_id,
  sku,
  product_name,
  category,
  brand,
  unit_price_pln AS list_price_pln,
  CASE
    WHEN unit_price_pln < 200  THEN 'budget'
    WHEN unit_price_pln < 1000 THEN 'mid'
    ELSE 'premium'
  END AS price_band
FROM aurora_jk.silver_products;

INSERT INTO aurora_jk.dw_dim_product
SELECT -1, -1, 'UNKNOWN', 'Nieznany', 'Nieznany', 'Nieznany', 0.0, 'unknown'
WHERE NOT EXISTS (SELECT 1 FROM aurora_jk.dw_dim_product WHERE product_sk = -1);

-- -----------------------------------------------------------------------------
-- dw_dim_order_status: jeden wiersz to jedna para status x kanal (junk dimension).
-- -----------------------------------------------------------------------------
-- Dwa niskokardynalne atrybuty zamowienia (status, kanal) nie zasluguja na dwa
-- osobne wymiary ani na dwie kolumny tekstowe w fakcie. Junk dimension sklada
-- je w jedna tabele.
--
-- CROSS JOIN dwoch DISTINCT-ow rozpina iloczyn kartezjanski: kazdy status z
-- kazdym kanalem, takze pary, ktorych dzis nie ma w danych. Wymiar opisuje
-- dziedzine (co w ogole moze sie zdarzyc), a nie aktualny stan sprzedazy.
-- INNER JOIN po wystepujacych parach dalby dziury, gdy jutro pojawi sie nowa
-- kombinacja.
--
-- Zrodlem jest Bronze, a nie Silver: Silver odfiltrowal cancelled, a wymiar ma
-- znac wszystkie statusy. is_revenue mowi, czy status liczy sie do przychodu.
CREATE TABLE IF NOT EXISTS aurora_jk.dw_dim_order_status
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/dw/dim_order_status/',
  is_external = false
) AS
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY s.status, c.channel) AS integer) AS order_status_sk,
  s.status,
  c.channel,
  s.status IN ('paid', 'shipped') AS is_revenue
FROM (SELECT DISTINCT status  FROM aurora_jk.bronze_orders) s
CROSS JOIN (SELECT DISTINCT channel FROM aurora_jk.bronze_orders) c;

-- =============================================================================
-- KONTROLA
-- =============================================================================
-- Oczekiwane (kolejnosc z ORDER BY wymiar):
--   dw_dim_customer 81 | dw_dim_date 366 | dw_dim_order_status 9 | dw_dim_product 26
-- Kazdy wymiar poza junk dimension ma o jeden wiersz wiecej niz zrodlo - to
-- wiersz -1. Junk dimension: 3 statusy x 3 kanaly = 9.
SELECT 'dw_dim_date' AS wymiar, COUNT(*) AS n FROM aurora_jk.dw_dim_date
UNION ALL SELECT 'dw_dim_customer',     COUNT(*) FROM aurora_jk.dw_dim_customer
UNION ALL SELECT 'dw_dim_product',      COUNT(*) FROM aurora_jk.dw_dim_product
UNION ALL SELECT 'dw_dim_order_status', COUNT(*) FROM aurora_jk.dw_dim_order_status
ORDER BY wymiar;

-- Junk dimension w calosci. Oczekiwane 9 wierszy, numeracja alfabetyczna:
--   1 cancelled/marketplace | 2 cancelled/mobile | 3 cancelled/web |
--   4 paid/marketplace | 5 paid/mobile | 6 paid/web |
--   7 shipped/marketplace | 8 shipped/mobile | 9 shipped/web
-- is_revenue = false tylko dla 1-3.
SELECT order_status_sk, status, channel, is_revenue
FROM aurora_jk.dw_dim_order_status
ORDER BY order_status_sk;

-- Przedzialy cenowe. Oczekiwane (ORDER BY price_band): budget 4 | mid 6 |
-- premium 15 | unknown 1.
SELECT price_band, COUNT(*) AS n
FROM aurora_jk.dw_dim_product
GROUP BY price_band
ORDER BY price_band;

-- Co Iceberg zapisal na S3: jeden snapshot 'append' z CTAS-a (80 wierszy)
-- i drugi 'append' z INSERT-a wiersza -1 (1 wiersz). Oczekiwane: 2 wiersze,
-- oba operation = append, added_records 80 i 1. Po drugim uruchomieniu pliku
-- nadal 2 - INSERT, ktory nic nie wstawil, nie tworzy snapshotu (sprawdzone).
SELECT committed_at, operation, summary['added-records'] AS added_records
FROM "aurora_jk"."dw_dim_customer$snapshots"
ORDER BY committed_at;
```

```text
wymiar              | n
dw_dim_customer     | 81
dw_dim_date         | 366
dw_dim_order_status | 9
dw_dim_product      | 26
```

```text
order_status_sk | status    | channel     | is_revenue
1               | cancelled | marketplace | false
2               | cancelled | mobile      | false
3               | cancelled | web         | false
4               | paid      | marketplace | true
5               | paid      | mobile      | true
6               | paid      | web         | true
7               | shipped   | marketplace | true
8               | shipped   | mobile      | true
9               | shipped   | web         | true
```

```text
price_band | n
budget     | 4
mid        | 6
premium    | 15
unknown    | 1
```

Snapshoty `dw_dim_customer` po Z2 (i po drugim uruchomieniu — bez zmian):

```text
committed_at                | operation | added_records
2026-09-03 08:21:28.170 UTC | append    | 80
2026-09-03 08:21:29.939 UTC | append    | 1
```

### Na co zwrócić uwagę

- Wiersz `-1` ma `valid_from 1900-01-01 / valid_to 9999-12-31 / is_current true`,
  więc złączenie SCD2 po `BETWEEN` trafia w niego tak samo jak w prawdziwych
  klientów. To ważne jutro.
- `ROW_NUMBER() OVER (ORDER BY customer_id)` daje klucze 1–80 w kolejności
  `C0001`…`C0080`. Jutro nowe wersje dostaną 81–88.

---

## Z3. Fakty

`bronze_shipments` — jeden wiersz to jedna wysyłka. `dw_fact_order_item` — jeden wiersz to jedna pozycja zamówienia. `dw_fact_order_fulfillment` — jeden wiersz to jedno zamówienie. `dw_fact_daily_sales` — jeden wiersz to dzień × kategoria × kanał.

Pełny skrypt (identyczny z `sql/Z3_fakty.sql`):

```sql
-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z3 - trzy fakty Iceberg z jednego Silvera
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Wymaga wymiarow z sql/Z1_wymiar_daty.sql i sql/Z2_wymiary.sql.
-- Wymaga pliku bronze/shipments/shipments.csv wgranego do wlasnego bucketu (Z0).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze_shipments: jeden wiersz to jedna wysylka (66 wierszy z pliku Z0).
-- -----------------------------------------------------------------------------
-- Plik: dane/bronze/shipments/shipments.csv -> s3://aurora-<inicjaly>-lake/bronze/shipments/
-- Ta sama procedura co w dniu 1: CSV przez OpenCSVSerde, wszystko jako string.
-- delivered_ts bywa puste (paczka w drodze) - OpenCSVSerde zwraca wtedy pusty
-- string, nie NULL. NULLIF w Gold zamienia go na NULL.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_shipments (
  shipment_id  string,
  order_id     string,
  carrier      string,
  shipped_ts   string,
  delivered_ts string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/shipments/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Oczekiwane: wysylki = 66, dostarczone = 45, w_drodze = 21.
SELECT
  COUNT(*)                                                    AS wysylki,
  SUM(CASE WHEN delivered_ts <> '' THEN 1 ELSE 0 END)         AS dostarczone,
  SUM(CASE WHEN delivered_ts =  '' THEN 1 ELSE 0 END)         AS w_drodze
FROM aurora_jk.bronze_shipments;

-- -----------------------------------------------------------------------------
-- Z3b. dw_fact_order_item - fakt transakcyjny (grain: pozycja zamowienia)
-- -----------------------------------------------------------------------------
-- Co jest inaczej niz w gold_fact_order_item z wersji podstawowej:
--   * order_id zostaje w fakcie jako degenerate dimension - identyfikator
--     zamowienia bez wlasnej tabeli wymiaru (nie ma czego o nim trzymac),
--   * status i kanal wchodza jednym kluczem order_status_sk (junk dimension),
--   * LEFT JOIN + COALESCE(..., -1): brak w wymiarze NIE usuwa wiersza faktu,
--     tylko wskazuje wiersz nieznany. Rekoncyliacja z Silver ma wtedy szanse
--     zawsze sie zgadzac, a problem widac w raporcie zamiast znikac z niego,
--   * order_date (typ date) zostaje w fakcie OBOK order_date_sk: Iceberg
--     partycjonuje po nim w sposob ukryty - partitioning = ARRAY['day(order_date)'].
--     Filtr WHERE order_date = DATE '2026-01-15' tnie partycje bez zadnej
--     kolumny dt i bez wymogu "kolumna partycjonujaca ostatnia w SELECT",
--   * load_ts to kolumna audytowa - jutro pokaze, ktore wiersze weszly
--     ladowaniem przyrostowym.
--
-- current_timestamp w Athenie ma strefe czasowa; Iceberg w Athenie przyjmuje
-- timestamp bez strefy, stad CAST(... AS timestamp(6)).
CREATE TABLE IF NOT EXISTS aurora_jk.dw_fact_order_item
WITH (
  table_type   = 'ICEBERG',
  format       = 'PARQUET',
  location     = 's3://aurora-jk-lake/dw/fact_order_item/',
  is_external  = false,
  partitioning = ARRAY['day(order_date)']
) AS
SELECT
  i.order_id,
  i.order_date,
  COALESCE(d.date_sk, -1)           AS order_date_sk,
  COALESCE(c.customer_sk, -1)       AS customer_sk,
  COALESCE(p.product_sk, -1)        AS product_sk,
  COALESCE(st.order_status_sk, -1)  AS order_status_sk,
  i.qty,
  i.unit_price_pln,
  i.discount_pln,
  i.net_amount_pln,
  CAST(current_timestamp AS timestamp(6)) AS load_ts
FROM aurora_jk.silver_order_items i
JOIN aurora_jk.silver_orders o
  ON i.order_id = o.order_id
LEFT JOIN aurora_jk.dw_dim_date d
  ON i.order_date = d.date_actual
LEFT JOIN aurora_jk.dw_dim_customer c
  ON i.customer_id = c.customer_id
 AND i.order_date BETWEEN c.valid_from AND c.valid_to
LEFT JOIN aurora_jk.dw_dim_product p
  ON i.product_id = p.product_id
LEFT JOIN aurora_jk.dw_dim_order_status st
  ON o.status = st.status AND o.channel = st.channel;

-- Oczekiwane: 409 wierszy, 1427124.00, nieznane_* = 0 wszedzie.
SELECT
  COUNT(*)                                                    AS rows_n,
  ROUND(SUM(net_amount_pln), 2)                               AS revenue_pln,
  SUM(CASE WHEN customer_sk     = -1 THEN 1 ELSE 0 END)       AS nieznany_klient,
  SUM(CASE WHEN product_sk      = -1 THEN 1 ELSE 0 END)       AS nieznany_produkt,
  SUM(CASE WHEN order_status_sk = -1 THEN 1 ELSE 0 END)       AS nieznany_status
FROM aurora_jk.dw_fact_order_item;

-- -----------------------------------------------------------------------------
-- Z3c. dw_fact_order_fulfillment - accumulating snapshot (grain: zamowienie)
-- -----------------------------------------------------------------------------
-- Jeden wiersz na zamowienie z trzema datami: zlozenia, wysylki, dostawy.
-- Wszystkie trzy wskazuja na ten sam wymiar dw_dim_date. To jest wymiar daty
-- uzywany w kilku rolach (role-playing dimension): jedna tabela, trzy aliasy
-- w zlaczeniu (od, sd, dd).
-- Zamowienie bez wysylki ma ship_date_sk = -1, a nie NULL: dzieki temu
-- INNER JOIN do wymiaru daty w raporcie nadal zwraca ten wiersz.
--
-- Miary: order_amount_pln i items_n (zsumowane z pozycji), ship_lag_days
-- i delivery_lag_days (NULL, dopoki etap nie nastapil). Fakt tego typu jest
-- z definicji AKTUALIZOWANY - jutro MERGE dopisze dostawy, ktore dzis sa w drodze.
--
-- date_diff('day', ...) zwraca bigint - CAST na integer, zeby typ byl staly.
CREATE TABLE IF NOT EXISTS aurora_jk.dw_fact_order_fulfillment
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/dw/fact_order_fulfillment/',
  is_external = false
) AS
WITH order_totals AS (
  SELECT order_id, SUM(net_amount_pln) AS order_amount_pln, COUNT(*) AS items_n
  FROM aurora_jk.silver_order_items
  GROUP BY order_id
),
ship AS (
  SELECT
    order_id,
    carrier,
    CAST(shipped_ts AS timestamp)                AS shipped_ts,
    CAST(NULLIF(delivered_ts, '') AS timestamp)  AS delivered_ts
  FROM aurora_jk.bronze_shipments
)
SELECT
  o.order_id,
  COALESCE(c.customer_sk, -1)                     AS customer_sk,
  COALESCE(st.order_status_sk, -1)                AS order_status_sk,
  COALESCE(od.date_sk, -1)                        AS order_date_sk,
  COALESCE(sd.date_sk, -1)                        AS ship_date_sk,
  COALESCE(dd.date_sk, -1)                        AS delivery_date_sk,
  COALESCE(s.carrier, 'n/a')                      AS carrier,
  t.order_amount_pln,
  CAST(t.items_n AS integer)                      AS items_n,
  CAST(date_diff('day', o.order_ts, s.shipped_ts)   AS integer) AS ship_lag_days,
  CAST(date_diff('day', o.order_ts, s.delivered_ts) AS integer) AS delivery_lag_days
FROM aurora_jk.silver_orders o
JOIN order_totals t
  ON o.order_id = t.order_id
LEFT JOIN ship s
  ON o.order_id = s.order_id
LEFT JOIN aurora_jk.dw_dim_customer c
  ON o.customer_id = c.customer_id
 AND o.order_date BETWEEN c.valid_from AND c.valid_to
LEFT JOIN aurora_jk.dw_dim_order_status st
  ON o.status = st.status AND o.channel = st.channel
LEFT JOIN aurora_jk.dw_dim_date od ON CAST(o.order_ts AS date)     = od.date_actual
LEFT JOIN aurora_jk.dw_dim_date sd ON CAST(s.shipped_ts AS date)   = sd.date_actual
LEFT JOIN aurora_jk.dw_dim_date dd ON CAST(s.delivered_ts AS date) = dd.date_actual;

-- Oczekiwane: zamowienia = 170, wyslane = 66, dostarczone = 45,
-- bez_wysylki = 104, revenue 1427124.00 (ta sama kwota co na pozycjach -
-- agregacja do zamowienia niczego nie gubi).
SELECT
  COUNT(*)                                                        AS zamowienia,
  SUM(CASE WHEN ship_date_sk     <> -1 THEN 1 ELSE 0 END)         AS wyslane,
  SUM(CASE WHEN delivery_date_sk <> -1 THEN 1 ELSE 0 END)         AS dostarczone,
  SUM(CASE WHEN ship_date_sk      = -1 THEN 1 ELSE 0 END)         AS bez_wysylki,
  ROUND(SUM(order_amount_pln), 2)                                 AS revenue_pln
FROM aurora_jk.dw_fact_order_fulfillment;

-- Ten sam wymiar daty trzy razy, kazdy alias w innej roli (role-playing dimension).
-- Wiersz -1 wymiaru daty ma date_actual = 1900-01-01 i tak wyglada
-- "jeszcze nie wyslano" w raporcie - bez NULL-i i bez zgubionych wierszy.
-- Oczekiwane: 5 wierszy, O1001 ... O1005 (ORDER BY order_id).
SELECT
  f.order_id,
  od.date_actual AS order_date,
  sd.date_actual AS ship_date,
  dd.date_actual AS delivery_date,
  f.carrier,
  f.ship_lag_days,
  f.delivery_lag_days
FROM aurora_jk.dw_fact_order_fulfillment f
JOIN aurora_jk.dw_dim_date od ON f.order_date_sk    = od.date_sk
JOIN aurora_jk.dw_dim_date sd ON f.ship_date_sk     = sd.date_sk
JOIN aurora_jk.dw_dim_date dd ON f.delivery_date_sk = dd.date_sk
ORDER BY f.order_id
LIMIT 5;

-- -----------------------------------------------------------------------------
-- Z3d. dw_fact_daily_sales - fakt agregujacy (grain: dzien x kategoria x kanal)
-- -----------------------------------------------------------------------------
-- Agregat to kopia faktu na grubszym grain, liczona RAZ, zeby najczestsze
-- pytania nie czytaly 409 (w produkcji: 400 mln) wierszy. Klucze wymiarow
-- zostaja kluczami - agregat nadal jest star schema, tylko mniejszym. Miary
-- addytywne (revenue, qty, lines_n) sumuja sie dalej; orders_n (COUNT DISTINCT)
-- juz NIE - to miara semi-addytywna i trzeba o tym pamietac przy ROLLUP-ie.
CREATE TABLE IF NOT EXISTS aurora_jk.dw_fact_daily_sales
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://aurora-jk-lake/dw/fact_daily_sales/',
  is_external = false
) AS
SELECT
  f.order_date_sk,
  p.category,
  st.channel,
  COUNT(*)                                  AS lines_n,
  COUNT(DISTINCT f.order_id)                AS orders_n,
  SUM(f.qty)                                AS qty,
  ROUND(SUM(f.net_amount_pln), 2)           AS revenue_pln
FROM aurora_jk.dw_fact_order_item f
JOIN aurora_jk.dw_dim_product p       ON f.product_sk      = p.product_sk
JOIN aurora_jk.dw_dim_order_status st ON f.order_status_sk = st.order_status_sk
GROUP BY f.order_date_sk, p.category, st.channel;

-- Oczekiwane: 36 wierszy (2 dni x 6 kategorii x 3 kanaly - wszystkie kombinacje
-- wystapily), revenue 1427124.00 - agregat gubi wiersze, nie zlotowki.
SELECT COUNT(*) AS rows_n, ROUND(SUM(revenue_pln), 2) AS revenue_pln
FROM aurora_jk.dw_fact_daily_sales;

-- =============================================================================
```

### Z3a. `bronze_shipments`

```text
wysylki | dostarczone | w_drodze
66      | 45          | 21
```

### Z3b. `dw_fact_order_item`

```text
rows_n | revenue_pln | nieznany_klient | nieznany_produkt | nieznany_status
409    | 1427124.0   | 0               | 0                | 0
```

`DESCRIBE aurora_jk.dw_fact_order_item` pokazuje na dole sekcję *Partition spec*:
`order_date_day | day | order_date`. To jest hidden partitioning: Iceberg tnie
pliki po kolumnie `order_date`, choć w tabeli zostaje zwykłą datą. Na S3 katalogi
wyglądają jak `data/order_date_day=2026-01-15/`, ale tabela nie ma kolumny o tej
nazwie.

### Z3c. `dw_fact_order_fulfillment`

```text
zamowienia | wyslane | dostarczone | bez_wysylki | revenue_pln
170        | 66      | 45          | 104         | 1427124.0
```

Wymiar daty w kilku rolach — pierwsze pięć zamówień (`ORDER BY order_id`):

```text
order_id | order_date | ship_date  | delivery_date | carrier | ship_lag_days | delivery_lag_days
O1001    | 2026-01-15 | 2026-01-16 | 2026-01-18    | DPD     | 0             | 3
O1002    | 2026-01-15 | 1900-01-01 | 1900-01-01    | n/a     |               |
O1003    | 2026-01-15 | 1900-01-01 | 1900-01-01    | n/a     |               |
O1004    | 2026-01-15 | 1900-01-01 | 1900-01-01    | n/a     |               |
O1005    | 2026-01-15 | 1900-01-01 | 1900-01-01    | n/a     |               |
```

`ship_lag_days = 0` dla O1001 nie jest błędem: zamówienie 15.01 08:23, wysyłka
16.01 04:23 — mniej niż 24 h, więc `date_diff('day', ...)` daje 0. To dobry
moment na pytanie „czy opóźnienie (`ship_lag_days`) liczymy w dobach kalendarzowych, czy w pełnych 24 h” —
odpowiedź jest decyzją biznesową, nie techniczną.

### Z3d. `dw_fact_daily_sales`

```text
rows_n | revenue_pln
36     | 1427124.0
```

### Pułapki Z3

- **`current_timestamp` ma strefę czasową**, Iceberg w Athenie przyjmuje
  `timestamp` bez strefy — stąd `CAST(current_timestamp AS timestamp(6))`.
  Bez rzutowania CTAS kończy się błędem o typie kolumny.
- **`date_diff` zwraca bigint** → `CAST(... AS integer)`, żeby typ w Redshifcie
  (Z6) był `integer`, nie `bigint`.
- Zamówienie z **wieloma wysyłkami** zdublowałoby wiersz faktu przez `LEFT JOIN
  ship`. W dzisiejszym pliku każde zamówienie ma jedną — jutro (spóźnione
  wysyłki) chroni przed tym `ROW_NUMBER()` w źródle `MERGE`.

---

## Z4. Asercje

Wynik to osiem testów, nie osiem tabel. `OK` znaczy: Gold wolno publikować.

Pełny skrypt (identyczny z `sql/Z4_asercje.sql`):

```sql
-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z4 - asercje jakosci danych jako jedno zapytanie
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
-- Do podmiany: aurora_jk -> aurora_<inicjaly>.
-- Wymaga faktow z sql/Z3_fakty.sql.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Asercje jakosci danych jako SQL
-- -----------------------------------------------------------------------------
-- Jedno zapytanie, jedna tabela wynikowa: nazwa testu, wartosc oczekiwana,
-- wartosc zmierzona, status. To ten sam pomysl, ktory dbt nazywa "tests",
-- a Great Expectations "expectations" - tu bez zadnego narzedzia. Wynik inny
-- niz OK we wszystkich wierszach oznacza, ze Gold nie nadaje sie do publikacji.
--
-- Oczekiwane: 8 wierszy, w kazdym status = OK (ORDER BY test).
WITH checks AS (
  SELECT 'A1 fact rows = silver rows' AS test,
         (SELECT COUNT(*) FROM aurora_jk.silver_order_items)   AS expected,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item)   AS actual
  UNION ALL
  SELECT 'A2 fact revenue = silver revenue',
         (SELECT CAST(ROUND(SUM(net_amount_pln), 2) AS bigint) FROM aurora_jk.silver_order_items),
         (SELECT CAST(ROUND(SUM(net_amount_pln), 2) AS bigint) FROM aurora_jk.dw_fact_order_item)
  UNION ALL
  SELECT 'A3 no unknown customer in fact', 0,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item WHERE customer_sk = -1)
  UNION ALL
  SELECT 'A4 no orphan product_sk', 0,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item f
          LEFT JOIN aurora_jk.dw_dim_product p ON f.product_sk = p.product_sk
          WHERE p.product_sk IS NULL)
  UNION ALL
  SELECT 'A5 grain unique (order_id, product_sk)', 0,
         (SELECT COUNT(*) FROM (
            SELECT order_id, product_sk FROM aurora_jk.dw_fact_order_item
            GROUP BY order_id, product_sk HAVING COUNT(*) > 1))
  UNION ALL
  SELECT 'A6 no negative amounts', 0,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item WHERE net_amount_pln < 0)
  UNION ALL
  SELECT 'A7 aggregate revenue = fact revenue',
         (SELECT CAST(ROUND(SUM(net_amount_pln), 2) AS bigint) FROM aurora_jk.dw_fact_order_item),
         (SELECT CAST(ROUND(SUM(revenue_pln), 2) AS bigint)    FROM aurora_jk.dw_fact_daily_sales)
  UNION ALL
  SELECT 'A8 fulfillment orders = silver orders',
         (SELECT COUNT(*) FROM aurora_jk.silver_orders),
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_fulfillment)
)
SELECT test, expected, actual,
       CASE WHEN expected = actual THEN 'OK' ELSE 'FAIL' END AS status
FROM checks
ORDER BY test;

-- Rzut oka na S3 przez metadane Icebergu: ile plikow danych ma fakt i w ilu
-- partycjach. Oczekiwane: 2 wiersze (partycje 2026-01-15 i 2026-01-16), po
-- jednym pliku kazda - CTAS zapisal jeden plik na partycje.
SELECT partition, record_count, file_count
FROM "aurora_jk"."dw_fact_order_item$partitions"
ORDER BY partition;
```

```text
test                                    | expected | actual  | status
A1 fact rows = silver rows              | 409      | 409     | OK
A2 fact revenue = silver revenue        | 1427124  | 1427124 | OK
A3 no unknown customer in fact          | 0        | 0       | OK
A4 no orphan product_sk                 | 0        | 0       | OK
A5 grain unique (order_id, product_sk)  | 0        | 0       | OK
A6 no negative amounts                  | 0        | 0       | OK
A7 aggregate revenue = fact revenue     | 1427124  | 1427124 | OK
A8 fulfillment orders = silver orders   | 170      | 170     | OK
```

```text
partition                     | record_count | file_count
{order_date_day=2026-01-15}   | 186          | 1
{order_date_day=2026-01-16}   | 223          | 1
```

Kwoty w asercjach są rzutowane na `bigint` po `ROUND(..., 2)`: porównanie dwu
`double` na równość jest kruche (błędy reprezentacji), porównanie liczb
całkowitych — nie. Przy kwotach z groszami należałoby porównywać
`CAST(ROUND(x * 100) AS bigint)`.

---

## Z5. SQL analityczny

Pięć zapytań na gotowym star schema. Żadne nic nie tworzy — czytają `dw_fact_*` i wymiary.

Pełny skrypt (identyczny z `sql/Z5_analityka.sql`):

```sql
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
```

### Z5a. ROLLUP

```text
dzien      | kanal       | revenue_pln
2026-01-15 | marketplace | 233010.0
2026-01-15 | mobile      | 226929.0
2026-01-15 | web         | 193592.0
2026-01-15 | razem       | 653531.0
2026-01-16 | marketplace | 288582.0
2026-01-16 | mobile      | 225598.0
2026-01-16 | web         | 259413.0
2026-01-16 | razem       | 773593.0
razem      | razem       | 1427124.0
```

`ORDER BY GROUPING(...)` na początku listy sortowania jest tym, co trzyma
wiersze „razem” na końcu każdej grupy — bez tego `NULL`-e z ROLLUP-u trafiają
tam, gdzie akurat silnik je posortuje.

### Z5b. GROUPING SETS

```text
wymiar    | klucz       | revenue_pln
kanal     | marketplace | 521592.0
kanal     | mobile      | 452527.0
kanal     | web         | 453005.0
kategoria | AGD         | 261065.0
kategoria | Akcesoria   | 13423.0
kategoria | Audio       | 83254.0
kategoria | Komputery   | 362605.0
kategoria | TV          | 324800.0
kategoria | Telefony    | 381977.0
```

`TV` przed `Telefony` — Trino sortuje bajtowo, wielkie `V` (0x56) jest przed
małym `e` (0x65). To samo zapytanie w Redshifcie da tę samą kolejność (też
sortowanie binarne), ale w bazach z collation językowym już nie.

### Z5c. Okna

```text
category  | rnk | product_name                  | revenue_pln | udzial_pct
AGD       | 1   | Lodówka Aurora 300l           | 102743.0    | 39.36
AGD       | 2   | Pralka Bosch 8kg              | 67043.0     | 25.68
Akcesoria | 1   | Klawiatura mechaniczna Aurora | 5589.0      | 41.64
Akcesoria | 2   | Ładowarka 65W Aurora          | 4146.0      | 30.89
Audio     | 1   | Sony WH-1000XM5               | 38450.0     | 46.18
Audio     | 2   | Kamera sportowa Aurora 4K     | 28149.0     | 33.81
Komputery | 1   | Laptop Aurora 16" i7          | 153101.0    | 42.22
Komputery | 2   | Laptop Aurora 14" i5          | 108126.0    | 29.82
TV        | 1   | Telewizor Aurora 65" OLED     | 134560.0    | 41.43
TV        | 2   | Telewizor Sony 50" LED        | 101199.0    | 31.16
Telefony  | 1   | iPhone 15                     | 136798.0    | 35.81
Telefony  | 2   | Samsung Galaxy S24            | 117099.0    | 30.66
```

`SUM(SUM(x)) OVER (...)` — okno na agregacie — to poprawna, choć na pierwszy
rzut oka dziwna składnia: wewnętrzny `SUM` należy do `GROUP BY`, zewnętrzny do
okna. Trino nie ma `QUALIFY`, więc filtr po `rnk` wymaga zewnętrznego `SELECT`.

### Z5d. Kohorty

```text
signup_month | klienci | kupujacy | zamowienia | revenue_pln
2025-01      | 11      | 8        | 23         | 191673.0
2025-02      | 8       | 8        | 16         | 101329.0
2025-03      | 4       | 4        | 10         | 94909.0
2025-04      | 4       | 4        | 10         | 119148.0
2025-05      | 4       | 4        | 11         | 81978.0
2025-06      | 2       | 1        | 2          | 13820.0
2025-07      | 8       | 8        | 21         | 189157.0
2025-08      | 8       | 6        | 16         | 172367.0
2025-09      | 5       | 4        | 10         | 84421.0
2025-10      | 4       | 3        | 8          | 53484.0
2025-11      | 7       | 6        | 9          | 93379.0
2025-12      | 10      | 8        | 20         | 131778.0
2026-01      | 5       | 4        | 14         | 99681.0
```

Suma `klienci` = 80, `zamowienia` = 170, `revenue_pln` = 1 427 124 — kohorty
są rozłączne i pokrywają całość, więc to trzecia niezależna rekoncyliacja.

### Z5e. AOV

```text
revenue_pln | zamowienia | pozycje | aov_pln | srednia_pozycji_pln
1427124.0   | 170        | 409     | 8394.85 | 3489.3
```

### Z5f. CUBE i suma biegnąca `[po 3 h]`

CUBE: 12 wierszy — 9 z ROLLUP-u plus `razem | marketplace 521592`,
`razem | mobile 452527`, `razem | web 453005`. Suma biegnąca:

```text
kanal       | dzien      | revenue_pln | revenue_narastajaco
marketplace | 2026-01-15 | 233010.0    | 233010.0
marketplace | 2026-01-16 | 288582.0    | 521592.0
mobile      | 2026-01-15 | 226929.0    | 226929.0
mobile      | 2026-01-16 | 225598.0    | 452527.0
web         | 2026-01-15 | 193592.0    | 193592.0
web         | 2026-01-16 | 259413.0    | 453005.0
```

---

## Z6. Redshift Serverless

`dw_jk.fact_order_item` (po podmianie inicjałów) to lokalna kopia faktu transakcyjnego: jeden wiersz to jedna pozycja zamówienia, rozłożona `DISTKEY (customer_sk)`.

Pełny skrypt (identyczny z `sql/Z6_redshift.sql`):

```sql
-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z6 - Redshift Serverless: Spectrum nad Icebergiem,
-- tabele lokalne z DISTKEY/SORTKEY, EXPLAIN, historia zapytan, MV, UNLOAD
-- =============================================================================
-- SILNIK: AMAZON REDSHIFT SERVERLESS, Query Editor v2 (polaczenie: Federated
-- user, baza dev). NIE Athena.
--
-- Do podmiany: dw_jk -> dw_<inicjaly> (lokalny schemat), aurora_jk ->
-- aurora_<inicjaly> (baza Glue), aurora-jk-lake -> wlasny bucket.
--
-- Trzy formy nazw - te same co w wersji podstawowej (W5_redshift.sql):
--   "awsdatacatalog"."aurora_jk"."dw_fact_order_item"  - Glue przez Spectrum,
--   dw_jk.fact_order_item                                - tabela lokalna,
--   aurora_jk.dw_fact_order_item                         - zapis Athenowy, NIE
--                                                          dziala w Redshifcie.
--
-- Gdy Redshift Data API jest zablokowane przez SCP - wszystko ponizej idzie
-- z Query Editora v2. Kazda zakladka QEv2 ma wlasna sesje; Run wykonuje
-- zaznaczony fragment, a bez zaznaczenia cala zakladke.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Z6a. Spectrum czyta Iceberg - bez kopiowania i bez konfiguracji
-- -----------------------------------------------------------------------------
-- awsdatacatalog to automatycznie zamontowany, tylko do odczytu obraz Glue Data
-- Catalog tego konta. Redshift rozpoznaje tabele Iceberg po table_type
-- w Glue i czyta AKTUALNY snapshot - pliki usuniete przez MERGE/DELETE nie sa
-- widoczne, choc fizycznie leza jeszcze na S3. Dlatego COPY z katalogu data/
-- tabeli Iceberg jest bledem koncepcyjnym: COPY nie zna snapshotow i wczytalby
-- takze wiersze juz skasowane.
--
-- Oczekiwane: 409 | 1427124.00 - te same liczby co w Athenie.
SELECT COUNT(*) AS rows_n, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM "awsdatacatalog"."aurora_jk"."dw_fact_order_item";

-- Ten sam star schema, z Redshifta, wprost z data lake. Oczekiwane: 9 wierszy,
-- identyczne z Z5a w Athenie (2026-01-15 marketplace 233010 ... razem 1427124).
SELECT
  COALESCE(CAST(d.date_actual AS varchar), 'razem') AS dzien,
  COALESCE(s.channel, 'razem')                       AS kanal,
  ROUND(SUM(s.revenue_pln), 2)                       AS revenue_pln
FROM "awsdatacatalog"."aurora_jk"."dw_fact_daily_sales" s
JOIN "awsdatacatalog"."aurora_jk"."dw_dim_date" d ON s.order_date_sk = d.date_sk
GROUP BY ROLLUP (d.date_actual, s.channel)
ORDER BY GROUPING(d.date_actual), d.date_actual, GROUPING(s.channel), s.channel;

-- -----------------------------------------------------------------------------
-- Z6b. Schemat lokalny i tabele z jawnym rozkladem danych
-- -----------------------------------------------------------------------------
-- DISTSTYLE decyduje, na ktorym wezle laduje wiersz:
--   ALL  - pelna kopia na kazdym wezle; dla malych wymiarow (setki-tysiace
--          wierszy) zlaczenie nie przesyla nic po sieci,
--   KEY  - wiersze o tym samym DISTKEY na tym samym wezle; dla faktu klucz
--          zlaczenia z najwiekszym wymiarem (tu customer_sk),
--   EVEN - rownomiernie, bez ogladania sie na klucze,
--   AUTO - Redshift zaczyna od ALL, przechodzi na EVEN/KEY, gdy tabela rosnie.
-- SORTKEY to fizyczna kolejnosc na dysku: filtr po order_date_sk pomija cale
-- bloki (zone maps) - odpowiednik partition pruning wewnatrz silnika.
--
-- PRIMARY KEY / FOREIGN KEY sa INFORMACYJNE: optymalizator z nich korzysta,
-- silnik ich NIE egzekwuje. Duplikaty lapia asercje z Z4, nie baza.
--
-- Typy sa dobrane do kolumn Icebergu: string -> varchar(n), double -> double
-- precision, date -> date, timestamp(6) -> timestamp. Przy INSERT ... SELECT
-- Redshift dopasowuje kolumny PO KOLEJNOSCI, jak COPY.
CREATE SCHEMA IF NOT EXISTS dw_jk;

CREATE TABLE IF NOT EXISTS dw_jk.dim_date (
  date_sk      integer PRIMARY KEY,
  date_actual  date,
  year         integer,
  quarter      integer,
  month        integer,
  month_name   varchar(16),
  day          integer,
  day_of_week  integer,
  day_name     varchar(16),
  iso_week     integer,
  is_weekend   boolean
) DISTSTYLE ALL SORTKEY (date_sk);

CREATE TABLE IF NOT EXISTS dw_jk.dim_customer (
  customer_sk  integer PRIMARY KEY,
  customer_id  varchar(16),
  first_name   varchar(64),
  last_name    varchar(64),
  city         varchar(64),
  voivodeship  varchar(64),
  segment      varchar(32),
  signup_date  date,
  signup_month varchar(7),
  valid_from   date,
  valid_to     date,
  is_current   boolean
) DISTSTYLE ALL SORTKEY (customer_sk);

CREATE TABLE IF NOT EXISTS dw_jk.dim_product (
  product_sk     integer PRIMARY KEY,
  product_id     integer,
  sku            varchar(32),
  product_name   varchar(128),
  category       varchar(64),
  brand          varchar(64),
  list_price_pln double precision,
  price_band     varchar(16)
) DISTSTYLE ALL SORTKEY (product_sk);

CREATE TABLE IF NOT EXISTS dw_jk.dim_order_status (
  order_status_sk integer PRIMARY KEY,
  status          varchar(32),
  channel         varchar(32),
  is_revenue      boolean
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dw_jk.fact_order_item (
  order_id        varchar(16),
  order_date      date,
  order_date_sk   integer REFERENCES dw_jk.dim_date (date_sk),
  customer_sk     integer REFERENCES dw_jk.dim_customer (customer_sk),
  product_sk      integer REFERENCES dw_jk.dim_product (product_sk),
  order_status_sk integer REFERENCES dw_jk.dim_order_status (order_status_sk),
  qty             integer,
  unit_price_pln  double precision,
  discount_pln    double precision,
  net_amount_pln  double precision,
  load_ts         timestamp
)
DISTKEY (customer_sk)
SORTKEY (order_date_sk);

-- -----------------------------------------------------------------------------
-- Z6c. Ladowanie: INSERT ... SELECT przez Spectrum (nie COPY)
-- -----------------------------------------------------------------------------
-- Dla tabel Iceberg to wlasciwa droga: Redshift czyta aktualny snapshot
-- tozsamoscia zalogowanego uzytkownika i zapisuje do wlasnego magazynu.
-- WHERE NOT EXISTS (SELECT 1 FROM cel) laduje tylko do PUSTEJ tabeli - drugie
-- uruchomienie nic nie dokłada (COPY tej ochrony nie ma). Realne przeladowanie:
-- TRUNCATE tabeli i ponowny INSERT.
INSERT INTO dw_jk.dim_date
SELECT date_sk, date_actual, year, quarter, month, month_name, day,
       day_of_week, day_name, iso_week, is_weekend
FROM "awsdatacatalog"."aurora_jk"."dw_dim_date"
WHERE NOT EXISTS (SELECT 1 FROM dw_jk.dim_date);

INSERT INTO dw_jk.dim_customer
SELECT customer_sk, customer_id, first_name, last_name, city, voivodeship,
       segment, signup_date, signup_month, valid_from, valid_to, is_current
FROM "awsdatacatalog"."aurora_jk"."dw_dim_customer"
WHERE NOT EXISTS (SELECT 1 FROM dw_jk.dim_customer);

INSERT INTO dw_jk.dim_product
SELECT product_sk, product_id, sku, product_name, category, brand,
       list_price_pln, price_band
FROM "awsdatacatalog"."aurora_jk"."dw_dim_product"
WHERE NOT EXISTS (SELECT 1 FROM dw_jk.dim_product);

INSERT INTO dw_jk.dim_order_status
SELECT order_status_sk, status, channel, is_revenue
FROM "awsdatacatalog"."aurora_jk"."dw_dim_order_status"
WHERE NOT EXISTS (SELECT 1 FROM dw_jk.dim_order_status);

INSERT INTO dw_jk.fact_order_item
SELECT order_id, order_date, order_date_sk, customer_sk, product_sk,
       order_status_sk, qty, unit_price_pln, discount_pln, net_amount_pln, load_ts
FROM "awsdatacatalog"."aurora_jk"."dw_fact_order_item"
WHERE NOT EXISTS (SELECT 1 FROM dw_jk.fact_order_item);

-- Oczekiwane (ORDER BY t): dim_customer 81 | dim_date 366 | dim_order_status 9 |
-- dim_product 26 | fact_order_item 409. Po drugim uruchomieniu: te same liczby.
SELECT 'dim_date' AS t, COUNT(*) AS n FROM dw_jk.dim_date
UNION ALL SELECT 'dim_customer',     COUNT(*) FROM dw_jk.dim_customer
UNION ALL SELECT 'dim_product',      COUNT(*) FROM dw_jk.dim_product
UNION ALL SELECT 'dim_order_status', COUNT(*) FROM dw_jk.dim_order_status
UNION ALL SELECT 'fact_order_item',  COUNT(*) FROM dw_jk.fact_order_item
ORDER BY t;

-- Ten sam star schema, trzeci silnik przechowywania. Oczekiwane: 9 wierszy,
-- identyczne jak w Z6a i jak w Athenie - razem 1427124.
SELECT
  COALESCE(CAST(d.date_actual AS varchar), 'razem') AS dzien,
  COALESCE(st.channel, 'razem')                      AS kanal,
  ROUND(SUM(f.net_amount_pln), 2)                    AS revenue_pln
FROM dw_jk.fact_order_item f
JOIN dw_jk.dim_date d          ON f.order_date_sk   = d.date_sk
JOIN dw_jk.dim_order_status st ON f.order_status_sk = st.order_status_sk
GROUP BY ROLLUP (d.date_actual, st.channel)
ORDER BY GROUPING(d.date_actual), d.date_actual, GROUPING(st.channel), st.channel;

-- -----------------------------------------------------------------------------
-- Z6d. EXPLAIN - jak Redshift wykonuje zlaczenie lokalne vs przez Spectrum
-- -----------------------------------------------------------------------------
-- W planie lokalnym warto wskazac: "XN Hash Join DS_DIST_ALL_NONE" - wymiar
-- z DISTSTYLE ALL nie wymaga przesylania danych (NONE). W planie Spectrum
-- pojawia sie "XN S3 Query Scan" - odczyt z S3 przez warstwe Spectrum, a
-- zlaczenie odbywa sie juz w Redshifcie. Koszty (cost=...) sa wzgledne,
-- nie sa sekundami.
EXPLAIN
SELECT p.category, SUM(f.net_amount_pln)
FROM dw_jk.fact_order_item f
JOIN dw_jk.dim_product p ON f.product_sk = p.product_sk
WHERE f.order_date_sk = 20260115
GROUP BY p.category;

EXPLAIN
SELECT p.category, SUM(f.net_amount_pln)
FROM "awsdatacatalog"."aurora_jk"."dw_fact_order_item" f
JOIN "awsdatacatalog"."aurora_jk"."dw_dim_product" p ON f.product_sk = p.product_sk
WHERE f.order_date_sk = 20260115
GROUP BY p.category;

-- -----------------------------------------------------------------------------
-- Z6e. Historia zapytan - SYS_QUERY_HISTORY (Serverless) zamiast SVL_*
-- -----------------------------------------------------------------------------
-- Widoki SVL_QUERY_SUMMARY / STL_* pochodza z klastrow provisioned; na
-- Serverless zrodlem prawdy sa widoki SYS_*. elapsed_time jest w mikrosekundach.
-- Oczekiwane: ostatnie zapytania tej tozsamosci, w tym oba EXPLAIN-y (query_type
-- UTILITY) i SELECT-y. Miedzy nimi widac "SELECT current_user;" i puste wiersze
-- typu OTHER - to Query Editor v2 sam odpytuje baze przy kazdym Run.
SELECT query_id, status, query_type,
       ROUND(elapsed_time / 1000000.0, 2) AS elapsed_s,
       returned_rows,
       LEFT(query_text, 70) AS query_text
FROM SYS_QUERY_HISTORY
WHERE user_id = current_user_id
  AND database_name = 'dev'
ORDER BY start_time DESC
LIMIT 10;

-- Spectrum osobno: co zeszlo z S3 na zapytania zewnetrzne. file_format = Iceberg
-- potwierdza, ze Redshift czytal snapshot Icebergu, a nie "goly" katalog Parquet.
-- Oczekiwane: po wierszu na kazda tabele Iceberg czytana w Z6a/Z6c, np.
-- dw_fact_order_item | Iceberg | scanned_files 2 | returned_rows 409 |
-- returned_bytes rzedu kilkunastu KB. total_partitions = -1: Iceberg ukrywa
-- partycje przed Spectrum (hidden partitioning), przycina je sam.
SELECT query_id, table_name, file_format, scanned_files, returned_rows, returned_bytes
FROM SYS_EXTERNAL_QUERY_DETAIL
ORDER BY start_time DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Z6f. Materialized view - agregat utrzymywany przez silnik
-- -----------------------------------------------------------------------------
-- To odpowiednik dw_fact_daily_sales z Atheny, tylko silnik sam wie, kiedy
-- jest nieaktualny. AUTO REFRESH NO: odswiezenie reczne (REFRESH MATERIALIZED
-- VIEW) - w klasie chcemy widziec moment, w ktorym dane sie zmieniaja.
-- Redshift nie zna IF NOT EXISTS dla MV - drugie uruchomienie zwraca
-- "relation already exists" i to jest bezpieczne.
CREATE MATERIALIZED VIEW dw_jk.mv_daily_sales
AUTO REFRESH NO
AS
SELECT f.order_date_sk, p.category, st.channel,
       COUNT(*) AS lines_n, SUM(f.qty) AS qty, SUM(f.net_amount_pln) AS revenue_pln
FROM dw_jk.fact_order_item f
JOIN dw_jk.dim_product p       ON f.product_sk      = p.product_sk
JOIN dw_jk.dim_order_status st ON f.order_status_sk = st.order_status_sk
GROUP BY f.order_date_sk, p.category, st.channel;

-- Oczekiwane: 36 | 1427124.00 - jak dw_fact_daily_sales w Athenie.
SELECT COUNT(*) AS rows_n, ROUND(SUM(revenue_pln), 2) AS revenue_pln
FROM dw_jk.mv_daily_sales;

-- -----------------------------------------------------------------------------
-- Z6g. UNLOAD - z powrotem do data lake
-- -----------------------------------------------------------------------------
-- Wynik zapytania laduje na S3 jako Parquet. To jest ostatnie ogniwo lakehouse:
-- to, co policzyl data warehouse, wraca do data lake i Athena czyta to jak kazda
-- inna tabele. IAM_ROLE default = rola namespace'u (ta sama, co przy COPY).
-- PARALLEL OFF - jeden plik zamiast pliku na slice (czytelniej w konsoli S3).
-- ALLOWOVERWRITE - drugie uruchomienie nadpisuje, zamiast konczyc sie bledem.
UNLOAD ('SELECT order_date_sk, category, channel, lines_n, qty, revenue_pln
         FROM dw_jk.mv_daily_sales')
TO 's3://aurora-jk-lake/export/daily_sales/'
IAM_ROLE default
FORMAT AS PARQUET
ALLOWOVERWRITE
PARALLEL OFF;

-- Dalszy ciag jest w ATHENIE (plik sql/Z6g_athena_po_unload.sql): tabela zewnetrzna
-- nad export/daily_sales/ i porownanie sumy z dw_fact_daily_sales.

-- -----------------------------------------------------------------------------
-- Po zajeciach
-- -----------------------------------------------------------------------------
-- DROP SCHEMA dw_jk CASCADE;   -- kasuje tabele, MV i dane w magazynie Redshifta
-- Workgroup i namespace kasuje osoba, ktora je stawiala. Tabele Iceberg i
-- eksport na S3 sprzatamy z Atheny / konsoli S3 (patrz 02_rozwiazania.md).
```

Wyniki poniżej pochodzą z Redshift Serverless (workgroup z podpiętą rolą IAM posiadającą uprawnienia do S3),
Query Editor v2, połączenie **Federated user**, baza `dev`. Tożsamość federacyjna w bazie
ma postać `IAMR:<rola-federacyjna>` (`SELECT current_user;`). Każda
instrukcja z `sql/Z6_redshift.sql` została wykonana dokładnie tą drogą, którą
idzie Query Editor v2 po naciśnięciu *Run*, dwa razy.

### Z6a. Spectrum nad Icebergiem

```text
rows_n | revenue_pln
409    | 1427124.00
```

ROLLUP przez Spectrum (`dw_fact_daily_sales` + `dw_dim_date` z Glue): 9 wierszy,
identyczne z Z5a w Athenie — `2026-01-15 marketplace 233010.0` …
`razem razem 1427124.0`. Redshift wypisuje kwoty jako `233010.0`, Athena tak
samo; różni się tylko nagłówek kolumn (Redshift zawsze małymi literami).

### Z6b–Z6c. Schemat lokalny i ładowanie

```text
t                | n
dim_customer     | 81
dim_date         | 366
dim_order_status | 9
dim_product      | 26
fact_order_item  | 409
```

Drugie uruchomienie: `CREATE SCHEMA/TABLE IF NOT EXISTS` przechodzą bez błędu,
`INSERT … WHERE NOT EXISTS (SELECT 1 FROM cel)` wstawia 0 wierszy, liczby te same.
ROLLUP na tabelach lokalnych: te same 9 wierszy co w Z6a.

### Z6d. EXPLAIN

Plan lokalny (skrócony do linii, o które chodzi):

```text
XN HashAggregate  (cost=0.41..0.41 rows=1 width=122)
  ->  XN Hash Join DS_DIST_ALL_NONE  (cost=0.33..0.40 rows=1 width=122)
        Hash Cond: ("outer".product_sk = "inner".product_sk)
        ->  XN Seq Scan on fact_order_item f  (cost=0.00..0.03 rows=3 width=12)
              Filter: (order_date_sk = 20260115)
        ->  XN Hash  (cost=0.26..0.26 rows=26 width=118)
              ->  XN Seq Scan on dim_product p  (cost=0.00..0.26 rows=26 width=118)
----- Tables missing statistics: dim_product, fact_order_item -----
----- Update statistics by running the ANALYZE command on these tables -----
```

Plan przez Spectrum:

```text
XN HashAggregate  (cost=6003.24..6003.25 rows=3 width=32)
  ->  XN Hash Join DS_BCAST_INNER  (cost=2.88..6003.23 rows=3 width=32)
        Hash Cond: ("outer".product_sk = "inner".product_sk)
        Remarks: Derives subplan 1
        ->  XN Seq Scan awsdatacatalog.aurora_jk.dw_dim_product p location:"s3://aurora-jk-lake/dw/dim_product" format:ICEBERG  (cost=0.00..0.26 rows=26 width=28)
              Filter: (subplan 1: (product_sk = $1))
        ->  XN Hash  (cost=2.83..2.83 rows=3 width=12)
              ->  XN Seq Scan awsdatacatalog.aurora_jk.dw_fact_order_item f location:"s3://aurora-jk-lake/dw/fact_order_item" format:ICEBERG  (cost=0.00..2.83 rows=3 width=12)
                    Filter: (order_date_sk = 20260115)
```

Trzy rzeczy do pokazania palcem: `DS_DIST_ALL_NONE` (wymiar `DISTSTYLE ALL`,
zero ruchu sieciowego) vs `DS_BCAST_INNER` (tabela zewnętrzna musi zostać
rozesłana), `format:ICEBERG` w węźle skanu (Redshift *wie*, że czyta Iceberg —
nie katalog Parquet) oraz koszt `6003` vs `0.41` — liczby względne, ale różnica
rzędów wielkości jest realna. Komunikat o `ANALYZE` jest normalny dla świeżo
załadowanych tabel; `ANALYZE dw_jk.fact_order_item;` go usuwa.

Plan Spectrum **nie jest deterministyczny** między przejazdami. Drugi, czysty
przejazd tego samego skryptu dał `DS_DIST_BOTH` z kluczami
`Outer Dist Key: f.product_sk` / `Inner Dist Key: p.product_sk` i kosztem
`42631` (planer oszacował `rows=186` na skanie faktu zamiast `rows=3`). Lekcja
jest ta sama — obie tabele zewnętrzne trzeba przesłać, a plan lokalny nie
przesyła nic — więc na sali czytamy **rodzaj** kroku dystrybucji
(`DS_DIST_ALL_NONE` vs cokolwiek innego) i rząd wielkości kosztu, nie konkretną
liczbę.

### Z6e. Historia zapytań

`SYS_QUERY_HISTORY` (10 ostatnich): oba `EXPLAIN`-y mają `query_type = UTILITY`,
`SELECT`-y `SELECT`, a między nimi powtarza się `SELECT current_user;` i puste
wiersze `OTHER` — to Query Editor v2 przy każdym *Run* sprawdza tożsamość
i ustawia sesję. `elapsed_s` dla wszystkiego poniżej 1 s.

`SYS_EXTERNAL_QUERY_DETAIL` (po Z6a–Z6c, 10 ostatnich):

```text
table_name                                  | file_format | scanned_files | returned_rows | returned_bytes
awsdatacatalog.aurora_jk.dw_fact_order_item | Iceberg     | 2             | 409           | ~27 000
awsdatacatalog.aurora_jk.dw_dim_order_status| Iceberg     | 1             | 9             | 237
awsdatacatalog.aurora_jk.dw_dim_product     | Iceberg     | 2             | 26            | 2177
awsdatacatalog.aurora_jk.dw_dim_customer    | Iceberg     | 2             | 81            | ~8 000
awsdatacatalog.aurora_jk.dw_dim_date        | Iceberg     | 2             | 366           | 19874
awsdatacatalog.aurora_jk.dw_fact_daily_sales| Iceberg     | 1             | 36            | 816
```

`total_partitions = -1` dla każdej tabeli: Iceberg nie wystawia partycji
Spectrum — przycina je sam w metadanych (hidden partitioning z drugiej strony).

### Z6f. Materialized view

```text
rows_n | revenue_pln
36     | 1427124.00
```

Drugie uruchomienie `CREATE MATERIALIZED VIEW`: `ERROR: relation "mv_daily_sales"
already exists` — bezpieczne, MV nie ma `IF NOT EXISTS`.

### Z6g. UNLOAD → Athena

`dw_export_daily_sales` to tabela zewnętrzna nad Parquetem z UNLOAD-u: jeden wiersz to dzień × kategoria × kanał, ten sam grain co agregat Iceberg.

Pełny skrypt (identyczny z `sql/Z6g_athena_po_unload.sql`):

```sql
-- =============================================================================
-- Dzien 2 (wersja zaawansowana), Z6g - Athena czyta to, co Redshift wyeksportowal
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Do podmiany: aurora_jk / aurora-jk-lake.
-- Wymaga wykonanego UNLOAD z Z6_redshift.sql.
--
-- Zwykla tabela zewnetrzna nad Parquetem z UNLOAD-u. Typy wg tego, co zapisal
-- Redshift: integer -> int, varchar -> string, bigint (COUNT/SUM(int)) -> bigint,
-- double precision -> double. Parquet ma schemat w pliku, wiec niezgodnosc
-- typu wyszlaby przy pierwszym SELECT, nie przy CREATE.
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.dw_export_daily_sales (
  order_date_sk int,
  category      string,
  channel       string,
  lines_n       bigint,
  qty           bigint,
  revenue_pln   double
)
STORED AS PARQUET
LOCATION 's3://aurora-jk-lake/export/daily_sales/';

-- Oczekiwane: 36 | 1427124.00 w obu wierszach (ORDER BY zrodlo):
-- athena_iceberg i redshift_unload - pelne kolo: S3 -> Athena -> Redshift -> S3
-- -> Athena, ta sama liczba na kazdym etapie.
SELECT 'athena_iceberg' AS zrodlo, COUNT(*) AS rows_n, ROUND(SUM(revenue_pln), 2) AS revenue_pln
FROM aurora_jk.dw_fact_daily_sales
UNION ALL
SELECT 'redshift_unload', COUNT(*), ROUND(SUM(revenue_pln), 2)
FROM aurora_jk.dw_export_daily_sales
ORDER BY zrodlo;
```

`UNLOAD` z `IAM_ROLE default` zapisuje jeden plik `export/daily_sales/000.parquet`
(~2,5 KB). W Athenie po `CREATE EXTERNAL TABLE dw_export_daily_sales`:

```text
zrodlo          | rows_n | revenue_pln
athena_iceberg  | 36     | 1427124.0
redshift_unload | 36     | 1427124.0
```

---

## Uwagi operacyjne z weryfikacji w konsoli AWS

- **Gdy Redshift Data API jest zablokowane przez SCP** — wszystko idzie przez
  Query Editor v2 (a ściślej: przez backend `sqlworkbench`, który QEv2
  woła po *Run*). Oznacza to brak zewnętrznego skryptu CLI do Redshifta w takich środowiskach.
- **Tożsamość federacyjna ≠ osoba.** Przy logowaniu przez IAM Identity Center `current_user` to
  `IAMR:<rola-federacyjna>` — jeden użytkownik bazy dla
  *wszystkich* osób z danym zestawem uprawnień. Gdy ta tożsamość ma na workgroupie
  uprawnienia w bazie `dev`, `CREATE SCHEMA` i `awsdatacatalog` działają bez dodatkowych
  grantów. Logowanie inną rolą (np. `AdministratorAccess`) tworzy w Redshifcie *inną*
  tożsamość `IAMR:...`, która bez jawnych grantów zwraca `permission denied
  for database dev` / `permission denied for database awsdatacatalog`.
- **Jedna tożsamość → per-osoba schemat.** W środowisku wieloosobowym dzielącym jedną rolę federacyjną
  izolację daje wyłącznie nazwa schematu `dw_<inicjaly>`.
  `DROP SCHEMA ... CASCADE` cudzego schematu przejdzie bez pytania.
- **Query Editor v2 limituje sesje do 3** (*Isolated session* włączone —
  każda zakładka to sesja). Komunikat: `The current limit of 3 sessions has been
  reached. Close another session or use a non-isolated session`. Zamknięcie
  zakładek albo wyłączenie *Isolated session* rozwiązuje sprawę.
- **`SYS_EXTERNAL_QUERY_DETAIL` nie ma kolumny `scanned_bytes`**.
  Właściwe kolumny: `scanned_files`, `returned_rows`,
  `returned_bytes`, `total_partitions`, `qualified_partitions`. `source_type`
  jest typu `char` z dopełnieniem spacjami i wartością `S3` (wielkie litery) —
  filtr `= 's3'` zwraca 0 wierszy, stąd w pliku nie ma filtra.
- **`UNLOAD` z `IAM_ROLE default` na bucket `aurora-<inicjaly>-lake`** wymaga,
  żeby rola podpięta do namespace'u miała uprawnienia zapisu S3 na tym buckecie.
  Domyślna rola konsolowa `AmazonRedshift-CommandsAccessRole-*` widzi tylko buckety
  z „redshift” w nazwie — bez rozszerzenia uprawnień `UNLOAD`/`COPY` skończą się
  `S3ServiceException: Access Denied` i wtedy Z6g zostaje jako blok dodatkowy po zajęciach.
- **Koszt trybu *price-performance target* (AI-driven scaling).** Workgroup z włączonym
  *price-performance target* bez sztywnego *base capacity* potrafi przydzielić **128 RPU**
  (`SYS_SERVERLESS_USAGE.compute_capacity = 128`, `charged_seconds = 7680` za
  każdą minutę z zapytaniem). Do ćwiczeń warsztatowych warto ustawić jawne *Base capacity* = 8 RPU.
- **Publiczny endpoint Redshift Serverless** wymaga sprawnego Internet Gateway w tablicy routingu VPC —
  w izolowanych VPC organizacyjnych bezpieczniej zostawić workgroup jako niepubliczny
  i korzystać z Query Editora v2 bezpośrednio w konsoli AWS.
