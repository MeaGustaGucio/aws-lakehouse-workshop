# Dzień 3 — SCD2, przyrost i lakehouse, wersja zaawansowana (rozwiązania i liczby kontrolne)

Wszystkie wyniki poniżej pochodzą z rzeczywistego przebiegu w konsoli AWS
(Athena engine v3, Redshift Serverless, `eu-central-1`), wykonanego w kolejności
Y0 → Y6 na stanie po wersji zaawansowanej dnia 2, dwukrotnie tam, gdzie plik
deklaruje idempotencję. Kolejność wierszy jest dokładnie taka, jaką zwraca
`ORDER BY` w zapytaniu. Identyfikatory snapshotów są z tego przebiegu — u każdego
są inne, dlatego w plikach SQL stoją z komentarzem `<- własny snapshot_id`.

Konwencja: ten plik jest jedną ścieżką czytania „jak”. Paste 1:1: `sql/Y0_stan_wejsciowy.sql` … `sql/Y6_capstone_i_sprzatanie.sql`.

---

## Liczby kontrolne w jednym miejscu

| Co | Wartość |
|---|---|
| Y0 stan wejściowy | Bronze: updates 8 · items_20 40 · orders 240 · shipments_21 88; `dw_dim_customer` 81 · `dw_fact_daily_sales` 36 · `dw_fact_order_fulfillment` 170 · `dw_fact_order_item` 409 |
| `bronze_customers_updates` | 8 zmian, wszystkie `valid_from = 2026-01-20` |
| Y1 SCD2 | po MERGE 81 / 73 bieżących → po INSERT **89 / 81 / 8 hist. / max_sk 88**; C0001: sk 1 Gdańsk standard → sk 81 Białystok vip |
| Y1 `$snapshots` wymiaru | append 80 · append 1 · overwrite 8 (position_deletes 8) · append 8; `total-records` 97 |
| Y1 time travel | `FOR VERSION AS OF <append 1>` → 81 / 81, C0001 Gdańsk standard |
| Y2 Bronze | `bronze_orders` 90 / 110 / **40**; `bronze_order_items_20260120` 40; `bronze_order_items` 486 |
| Y2 watermark | 2026-01-16 przed, 2026-01-20 po |
| Y2 fakt po MERGE | 15.01 186 · 653 531 / 16.01 223 · 773 593 / **20.01 40 · 101 546** = **449 / 1 528 670,00**; `na_nowe_wersje` 0 / 0 / 2; asercje 4 × OK |
| Y2 `$partitions` | po 1. uruchomieniu 20.01 = 40 w 1 pliku; po 2. = **80 w 2 plikach** (COUNT(*) nadal 40) |
| Y2 fulfillment | **210 zamówień** (170 z dnia 2 + 40 z 20.01; to nie minuty), 66 wysłanych, 45 dostarczonych, 1 528 670,00 zł |
| Y3 `bronze_shipments_20260121` | 88 / 51 z dostawą / 37 bez / 21 znanych wczoraj |
| Y3 po MERGE | **210 zamówień** / 133 wysłane / 96 dostarczone / 77 bez wysyłki; per przewoźnik DHL 23 · DPD 34 · InPost 41 · Poczta Polska 35 |
| Y3 time travel audyt | `nowo_wyslane 67 · nowo_dostarczone 51` |
| Y4 `$files` przed OPTIMIZE | 4 pliki (186 / 223 / 40 / 40); po OPTIMIZE **3 / 449 / 10 696 B** |
| Y4 promo_code | RABAT-100 67 · RABAT-20 69 · RABAT-50 79 · NULL 234 |
| Y4 VACUUM (próg 60 s) | 5 → **2 snapshoty**, 449 wierszy |
| Y5 Redshift | Spectrum 89 / 449 vs lokalnie 81 / 409 → po MERGE 89 / 81 / 8 i 449; MV 36 → **52 / 1 528 670,00** |
| Y5 rekoncyliacja | athena_iceberg 449 · redshift_local 449 · redshift_mv 52 — wszystkie 1 528 670,00 |
| Y6 lineage | bronze 526 · silver 409 / 1 427 124 · gold_fact 449 / 1 528 670 · gold_aggregate 36 / 1 427 124 → po MERGE **52 / 1 528 670** |
| Y6 ROLLUP | 34 wiersze; `razem`: 15.01 74 zamówienia · 653 531 / 16.01 96 · 773 593 / 20.01 40 · 101 546 / razem **210 zamówień** · 1 528 670 zł |

---

## Y0. Upload dopływu i stan wejściowy

Bucket `aurora-<inicjaly>-lake` i baza `aurora_<inicjaly>` już są z dni 1–2 —
bez *Create bucket*, bez `CREATE DATABASE`. Jeden folder na dysku:
`Dzien3_Data_Warehouse_Zaawansowany/dane/`. Prefiksy dnia 1 i `bronze/shipments/`
z Z0 zostają. Po wgraniu czterech nowych prefiksów paste `sql/Y0_stan_wejsciowy.sql`
rejestruje trzy nowe tabele Bronze i robi `MSCK REPAIR` na `bronze_orders`.
Cztery tabele `dw_*` z dnia 2 jeszcze się nie zmieniają. 409 pozycji to nie 486
z Bronze — cancelled odpadły w L5.

Pełny skrypt (identyczny z `sql/Y0_stan_wejsciowy.sql`):

```sql
-- =============================================================================
-- Dzien 3 (wersja zaawansowana), Y0 - upload doplywu i stan wejsciowy po dniu 2
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
--
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
--
-- Bucket s3://aurora-<inicjaly>-lake/ i baza Glue aurora_<inicjaly> JUZ ISTNIEJA
-- z dni 1-2. Ten plik NIE tworzy bucketu ani bazy. Nie ma tu CREATE DATABASE.
--
-- Zrodlo na dysku: Dzien3_Data_Warehouse_Zaawansowany/dane/ (jeden folder).
-- Prefiksy dnia 1 i bronze/shipments/ z Z0 zostaja. Wgrywamy tylko nowe:
--   dane/bronze/customers_updates/customers_updates.csv
--     -> s3://aurora-<inicjaly>-lake/bronze/customers_updates/
--   dane/bronze/orders/dt=2026-01-20/orders.csv
--     -> s3://aurora-<inicjaly>-lake/bronze/orders/dt=2026-01-20/
--   dane/bronze/order_items_20260120/order_items_2026-01-20.csv
--     -> s3://aurora-<inicjaly>-lake/bronze/order_items_20260120/
--   dane/bronze/shipments_20260121/shipments_2026-01-21.csv
--     -> s3://aurora-<inicjaly>-lake/bronze/shipments_20260121/
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Nowe tabele Bronze. bronze_orders i bronze_shipments z dni 1-2 zostaja.
-- -----------------------------------------------------------------------------
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

CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_shipments_20260121 (
  shipment_id  string,
  order_id     string,
  carrier      string,
  shipped_ts   string,
  delivered_ts string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/shipments_20260121/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Katalog Glue nie przeszukuje S3 sam. Wariant punktowy zamiast MSCK:
--   ALTER TABLE aurora_jk.bronze_orders ADD IF NOT EXISTS
--   PARTITION (dt = '2026-01-20') LOCATION 's3://aurora-jk-lake/bronze/orders/dt=2026-01-20/';
MSCK REPAIR TABLE aurora_jk.bronze_orders;

-- -----------------------------------------------------------------------------
-- Kontrola Bronze (ORDER BY tabela)
-- -----------------------------------------------------------------------------
-- Oczekiwane:
--   bronze_customers_updates      8
--   bronze_order_items_20260120  40
--   bronze_orders               240  (90 + 110 + 40; bez MSCK partycja 20.01 znika)
--   bronze_shipments_20260121    88
SELECT 'bronze_customers_updates' AS tabela, COUNT(*) AS n FROM aurora_jk.bronze_customers_updates
UNION ALL SELECT 'bronze_order_items_20260120', COUNT(*) FROM aurora_jk.bronze_order_items_20260120
UNION ALL SELECT 'bronze_orders',               COUNT(*) FROM aurora_jk.bronze_orders
UNION ALL SELECT 'bronze_shipments_20260121',   COUNT(*) FROM aurora_jk.bronze_shipments_20260121
ORDER BY tabela;

-- Oczekiwane: 2026-01-15 90 | 2026-01-16 110 | 2026-01-20 40.
SELECT dt, COUNT(*) AS n FROM aurora_jk.bronze_orders GROUP BY dt ORDER BY dt;

-- -----------------------------------------------------------------------------
-- Kontrola Gold dw_* z dnia 2 - nic jeszcze nie MERGE-ujemy
-- -----------------------------------------------------------------------------
-- Oczekiwane (ORDER BY tabela):
--   dw_dim_customer 81 - jeden wiersz to jeden klient (80 + wiersz -1), wszyscy is_current
--   dw_fact_daily_sales 36 - jeden wiersz to dzien x kategoria x kanal
--   dw_fact_order_fulfillment 170 - jeden wiersz to jedno zamowienie (paid/shipped)
--   dw_fact_order_item 409 - jeden wiersz to jedna pozycja zamowienia (nie 486:
--     cancelled odpadly w Silver dnia 1)
SELECT 'dw_dim_customer' AS tabela, COUNT(*) AS n FROM aurora_jk.dw_dim_customer
UNION ALL SELECT 'dw_fact_order_item',        COUNT(*) FROM aurora_jk.dw_fact_order_item
UNION ALL SELECT 'dw_fact_order_fulfillment', COUNT(*) FROM aurora_jk.dw_fact_order_fulfillment
UNION ALL SELECT 'dw_fact_daily_sales',       COUNT(*) FROM aurora_jk.dw_fact_daily_sales
ORDER BY tabela;
```

```text
tabela                       | n
bronze_customers_updates     | 8
bronze_order_items_20260120  | 40
bronze_orders                | 240
bronze_shipments_20260121    | 88
```

```text
dt         | n
2026-01-15 | 90
2026-01-16 | 110
2026-01-20 | 40
```

```text
tabela                    | n
dw_dim_customer           | 81
dw_fact_daily_sales       | 36
dw_fact_order_fulfillment | 170
dw_fact_order_item        | 409
```

Inne liczby na `dw_*` oznaczają, że sobotnia wersja zaawansowana nie została dokończona —
`sql/Z1_wymiar_daty.sql`, `sql/Z2_wymiary.sql` i `sql/Z3_fakty.sql` z dnia 2 są
idempotentne, więc można je puścić ponownie przed startem (razem ok. 3 min).
Inne liczby na Bronze oznaczają, że plik wylądował pod złym prefiksem albo
`MSCK REPAIR` nie zobaczył `dt=2026-01-20`.

---

## Y1. SCD2 przez MERGE

`bronze_customers_updates` — jeden wiersz to jedna zmiana atrybutu z 20.01. `dw_dim_customer` po SCD2: jeden wiersz to jedna wersja klienta w przedziale dat (89 wierszy = 81 bieżących + 8 historycznych). `MERGE` zamyka starą wersję (upsert na wymiarze); `INSERT` dopisuje nową — to nie jest ten sam typ ładowania co przyrost faktu w Y2.

Pełny skrypt (identyczny z `sql/Y1_scd2_merge.sql`):

```sql
-- =============================================================================
-- Dzien 3 (wersja zaawansowana), Y1 - SCD Type 2 przez MERGE INTO na Icebergu
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Wymaga dw_dim_customer z dnia 2 (81 wierszy: 80 klientow + wiersz -1).
--
-- Plik zrodlowy: dane/bronze/customers_updates/customers_updates.csv
--   -> s3://aurora-<inicjaly>-lake/bronze/customers_updates/
-- (upload i CREATE EXTERNAL TABLE sa w Y0_stan_wejsciowy.sql; IF NOT EXISTS
-- tu jest ten sam, idempotentny. Te same 8 zmian, co w wersji podstawowej.)
--
-- SCD2 w dwoch krokach. Jedna instrukcja MERGE nie moze dla jednego wiersza
-- zrodla zrobic dwu rzeczy naraz (zamknac stara wersje I wstawic nowa), bo
-- kazdy wiersz zrodla trafia w dokladnie jedna galaz WHEN. Stad:
--   krok 1: MERGE   - zamyka biezaca wersje (valid_to, is_current = false),
--   krok 2: INSERT  - dopisuje nowa wersje z nowym surrogate key.
-- Oba kroki sa idempotentne: krok 1 dotyka tylko wierszy, ktore REALNIE sie
-- roznia; krok 2 wstawia tylko tam, gdzie nie ma juz wersji biezacej.
-- Drugie uruchomienie calego pliku nie zmienia nic. Sprawdzone dwukrotnie.
--
-- W wersji podstawowej ten sam efekt wymagal CTAS-a nowej tabeli
-- gold_dim_customer_scd (UNION ALL) i zostawial stara. Tu wymiar zmienia sie
-- W MIEJSCU, a stara wersja danych zyje dalej jako snapshot (time travel nizej).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Krok 0. Bronze - plik zmian
-- -----------------------------------------------------------------------------
-- bronze_customers_updates: jeden wiersz to jedna zmiana atrybutu klienta z 20.01
-- (8 klientow, miasto lub segment). To nie jest pelny wymiar - tylko delta.
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

-- Oczekiwane: 8 wierszy, wszystkie valid_from = 2026-01-20.
SELECT COUNT(*) AS zmiany, MIN(valid_from) AS od, MAX(valid_from) AS do_
FROM aurora_jk.bronze_customers_updates;

-- Stan PRZED: 81 wierszy, 81 biezacych. Numer snapshotu warto zapisac -
-- przyda sie do time travel na koncu pliku.
SELECT COUNT(*) AS wiersze,
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS biezace
FROM aurora_jk.dw_dim_customer;

-- -----------------------------------------------------------------------------
-- Krok 1. MERGE - zamkniecie wersji, ktore sie zmienily
-- -----------------------------------------------------------------------------
-- ON laczy po natural key i TYLKO z wersja biezaca. Dodatkowy warunek w
-- WHEN MATCHED AND (...) sprawdza, czy cokolwiek sie zmienilo - bez niego
-- drugie uruchomienie zamknaloby wlasnie wstawione nowe wersje.
-- valid_to = dzien przed valid_from nowej wersji: przedzialy nie zachodza
-- na siebie, a BETWEEN w fakcie trafia zawsze w dokladnie jedna wersje.
-- Typ ladowania: upsert / MERGE. SCD2 zamyka stara wersje; nowa wejdzie INSERT-em w nastepnym kroku.
MERGE INTO aurora_jk.dw_dim_customer t
USING aurora_jk.bronze_customers_updates s
  ON t.customer_id = s.customer_id
 AND t.is_current = true
WHEN MATCHED AND (t.city <> s.city OR t.segment <> s.segment
                  OR t.voivodeship <> s.voivodeship)
  THEN UPDATE SET
    valid_to   = date_add('day', -1, CAST(s.valid_from AS date)),
    is_current = false;

-- Oczekiwane po kroku 1: 81 wierszy, 73 biezace (8 zamknietych, jeszcze bez
-- nowych wersji). Po drugim uruchomieniu pliku: 89 / 81 - krok 1 niczego
-- juz nie zamyka, bo nowe wersje maja te same atrybuty co zrodlo.
SELECT COUNT(*) AS wiersze,
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS biezace
FROM aurora_jk.dw_dim_customer;

-- -----------------------------------------------------------------------------
-- Krok 2. INSERT - nowe wersje z nowymi kluczami
-- -----------------------------------------------------------------------------
-- Surrogate key: max(customer_sk) + ROW_NUMBER(). To odpowiednik sekwencji -
-- klucz nigdy nie wraca do uzytku, stare fakty dalej wskazuja stare wersje.
-- WHERE NOT EXISTS: wstawiamy tylko klientom, ktorzy NIE MAJA wersji biezacej
-- (czyli tym, ktorych krok 1 wlasnie zamknal). Drugie uruchomienie -> 0 wierszy.
-- Typ ladowania: INSERT nowej wersji SCD2 (nie append-only faktu); WHERE NOT EXISTS daje idempotencje.
INSERT INTO aurora_jk.dw_dim_customer
SELECT
  CAST((SELECT MAX(customer_sk) FROM aurora_jk.dw_dim_customer)
       + ROW_NUMBER() OVER (ORDER BY s.customer_id) AS integer) AS customer_sk,
  s.customer_id,
  s.first_name,
  s.last_name,
  s.city,
  s.voivodeship,
  s.segment,
  CAST(s.signup_date AS date)                                   AS signup_date,
  date_format(CAST(s.signup_date AS timestamp), '%Y-%m')        AS signup_month,
  CAST(s.valid_from AS date)                                    AS valid_from,
  DATE '9999-12-31'                                             AS valid_to,
  true                                                          AS is_current
FROM aurora_jk.bronze_customers_updates s
WHERE NOT EXISTS (
  SELECT 1 FROM aurora_jk.dw_dim_customer c
  WHERE c.customer_id = s.customer_id AND c.is_current = true
);

-- =============================================================================
-- KONTROLA
-- =============================================================================
-- Oczekiwane: 89 wierszy, 81 biezacych, 8 historycznych, max_sk = 88
-- (80 + 8 nowych; wiersz -1 nie podnosi maksimum). Po drugim uruchomieniu
-- pliku: identycznie.
SELECT COUNT(*)                                          AS wiersze,
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END)       AS biezace,
       SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END)   AS historyczne,
       MAX(customer_sk)                                  AS max_sk
FROM aurora_jk.dw_dim_customer;

-- Historia jednego klienta. Oczekiwane: 2 wiersze C0001 (ORDER BY valid_from):
--   sk 1  Gdansk  standard 1900-01-01 .. 2026-01-19 false
--   sk 81 Bialystok vip    2026-01-20 .. 9999-12-31 true
SELECT customer_sk, customer_id, city, segment, valid_from, valid_to, is_current
FROM aurora_jk.dw_dim_customer
WHERE customer_id = 'C0001'
ORDER BY valid_from;

-- Test przedzialow: zaden klient nie ma dwu wersji obowiazujacych tego samego
-- dnia. Oczekiwane: 0 wierszy.
SELECT a.customer_id
FROM aurora_jk.dw_dim_customer a
JOIN aurora_jk.dw_dim_customer b
  ON a.customer_id = b.customer_id AND a.customer_sk < b.customer_sk
 AND a.valid_from <= b.valid_to AND b.valid_from <= a.valid_to
ORDER BY a.customer_id;

-- Fakt z dnia 2 jest NIETKNIETY i nadal wskazuje stare wersje - to jest sens
-- SCD2: styczniowe zamowienia zostaja przy styczniowym miescie.
-- Oczekiwane: 409 wierszy, 0 wierszy wskazujacych nowe wersje (sk > 80).
SELECT COUNT(*) AS rows_n,
       SUM(CASE WHEN customer_sk > 80 THEN 1 ELSE 0 END) AS na_nowe_wersje
FROM aurora_jk.dw_fact_order_item;

-- -----------------------------------------------------------------------------
-- Time travel - wymiar sprzed MERGE
-- -----------------------------------------------------------------------------
-- Kazda zmiana Icebergu to snapshot. $snapshots pokazuje historie tabeli:
-- Oczekiwane (ORDER BY committed_at): append 80 (CTAS dnia 2), append 1
-- (wiersz -1), overwrite added 8 / position_deletes 8 (MERGE), append 8 (INSERT).
-- Przy drugim uruchomieniu pliku nie dochodzi zaden snapshot.
--
-- MERGE w Athenie dziala w trybie merge-on-read: nie edytuje starego pliku,
-- tylko dopisuje NOWY plik z 8 zmienionymi wierszami i PLIK USUNIEC pozycyjnych
-- (position delete file) wskazujacy 8 starych wierszy. total-records w summary
-- rosnie do 97 (89 + 8 skasowanych logicznie) - fizycznie stare wiersze leza
-- dalej, az do OPTIMIZE (jutro w Y4).
SELECT snapshot_id, committed_at, operation,
       summary['added-records']          AS added,
       summary['added-position-deletes'] AS position_deletes,
       summary['total-records']          AS total_records
FROM "aurora_jk"."dw_dim_customer$snapshots"
ORDER BY committed_at;

-- Odczyt stanu z chwili PRZED MERGE: FOR VERSION AS OF <snapshot_id>, gdzie
-- <snapshot_id> to liczba z DRUGIEGO wiersza wyniku powyzej (append, added 1).
-- Wariant czasowy: FOR TIMESTAMP AS OF TIMESTAMP '2026-09-03 08:21:30 UTC'
-- (dowolna chwila miedzy drugim snapshotem a MERGE).
--
-- PULAPKI (sprawdzone w Athenie v3):
--   * wyrazenie po AS OF musi byc STALA - podzapytanie wybierajace snapshot
--     z $snapshots konczy sie bledem EXPRESSION_NOT_CONSTANT: Constant
--     expression cannot contain a subquery. Liczbe trzeba wkleic recznie,
--   * snapshot_id to bigint; krotka liczba (np. 0) daje NOT_SUPPORTED:
--     Unsupported type for table version: integer.
--
-- Oczekiwane: 81 | 81 - wymiar sprzed zmian, choc tabela ma juz 89 wierszy.
-- Ponizsza liczba pochodzi z przebiegu weryfikacyjnego - u kazdego jest INNA.
SELECT COUNT(*) AS wiersze,
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS biezace
FROM aurora_jk.dw_dim_customer FOR VERSION AS OF 5713768167270614691;  -- <- wlasny snapshot_id

-- Ten sam klient w dwu chwilach: Gdansk/standard w starym snapshocie,
-- Bialystok/vip w biezacym. Oczekiwane: 1 wiersz, C0001 | Gdansk | standard.
SELECT customer_id, city, segment
FROM aurora_jk.dw_dim_customer FOR VERSION AS OF 5713768167270614691   -- <- wlasny snapshot_id
WHERE customer_id = 'C0001';
```

### Y1a. Bronze i stan przed

```text
zmiany | od         | do_
8      | 2026-01-20 | 2026-01-20
```

```text
wiersze | biezace
81      | 81
```

### Y1b. Po kroku 1 (MERGE)

```text
wiersze | biezace
81      | 73
```

### Y1c. Po kroku 2 (INSERT)

```text
wiersze | biezace | historyczne | max_sk
89      | 81      | 8           | 88
```

```text
customer_sk | customer_id | city      | segment  | valid_from | valid_to   | is_current
1           | C0001       | Gdańsk    | standard | 1900-01-01 | 2026-01-19 | false
81          | C0001       | Białystok | vip      | 2026-01-20 | 9999-12-31 | true
```

Test przedziałów: **0 wierszy**. Kontrola faktu: `409 | 0` — żaden sobotni wiersz
nie wskazuje nowych wersji.

**Drugie uruchomienie pliku:** stan przed `89 | 81` → MERGE nic nie zamyka (`89 | 81`)
→ INSERT wstawia 0 → `89 | 81 | 8 | 88`. `$snapshots` nie dostaje nowego wiersza
ani po MERGE, ani po INSERT, które nic nie zmieniły — Athena nie tworzy pustych
snapshotów.

### Y1d. `$snapshots` i time travel

```text
snapshot_id         | committed_at                | operation | added | position_deletes | total_records
615027321704658062  | 2026-09-03 08:21:28.170 UTC | append    | 80    |                  | 80
5713768167270614691 | 2026-09-03 08:21:29.939 UTC | append    | 1     |                  | 81
1204079255956138494 | 2026-09-03 08:48:46.962 UTC | overwrite | 8     | 8                | 97
5018054558704411011 | 2026-09-03 08:48:53.476 UTC | append    | 8     |                  | 105
```

`total-records` liczy wiersze fizyczne we wszystkich plikach danych snapshotu:
81 + 8 nowych plikiem MERGE (stare 8 dalej leżą, tylko są wskazane w pliku
usunięć) = 97, + 8 z INSERT = 105. `COUNT(*)` daje 89.

```text
wiersze | biezace
81      | 81
```

```text
customer_id | city   | segment
C0001       | Gdańsk | standard
```

`FOR TIMESTAMP AS OF TIMESTAMP '2026-09-03 08:30:00 UTC'` (chwila między drugim
snapshotem a MERGE) zwraca to samo: 81.

**Pułapki (sprawdzone):**

- `FOR VERSION AS OF (SELECT MIN(snapshot_id) FROM "…$snapshots")` →
  `EXPRESSION_NOT_CONSTANT: Constant expression cannot contain a subquery`.
  Numer trzeba wkleić ręcznie.
- `FOR VERSION AS OF 0` (albo dowolna liczba mieszcząca się w `integer`) →
  `NOT_SUPPORTED: Unsupported type for table version: integer`. Prawdziwe
  identyfikatory są 19-cyfrowe, więc w praktyce błąd pojawia się tylko przy
  wpisaniu „na próbę”.
- `summary['deleted-records']` przy MERGE jest **puste** — Athena zapisuje
  usunięcia jako `added-position-deletes` (merge-on-read). To ta kolumna
  pokazuje, ile wierszy zostało zastąpionych.

---

## Y2. Przyrost 20 stycznia

`bronze_order_items_20260120` — jeden wiersz to jedna pozycja z 20.01. `dw_fact_order_item` po MERGE: jeden wiersz to jedna pozycja (409 z 15–16.01 + 40 z 20.01 = 449). Filtr `dt > watermark` to przyrost tylko nowszych wierszy; sam `MERGE` po `(order_id, product_sk)` to upsert, dlatego drugie uruchomienie zostaje na 449.

Pełny skrypt (identyczny z `sql/Y2_przyrost_merge.sql`):

```sql
-- =============================================================================
-- Dzien 3 (wersja zaawansowana), Y2 - ladowanie przyrostowe dnia 2026-01-20 przez MERGE
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Nazwy dwuczlonowe: aurora_<inicjaly>.tabela.
-- Do podmiany: aurora_jk -> aurora_<inicjaly>, aurora-jk-lake -> wlasny bucket.
-- Wymaga: dw_* z dnia 2 oraz dw_dim_customer po SCD2 (Y1_scd2_merge.sql).
--
-- Pliki zrodlowe (dane/ tej wersji; upload i MSCK / CREATE sa w Y0):
--   dane/bronze/orders/dt=2026-01-20/orders.csv
--     -> s3://aurora-<inicjaly>-lake/bronze/orders/dt=2026-01-20/
--   dane/bronze/order_items_20260120/order_items_2026-01-20.csv
--     -> s3://aurora-<inicjaly>-lake/bronze/order_items_20260120/
--
-- CEL: dolozyc dzien BEZ przeliczania 15 i 16 stycznia i tak, zeby drugie
-- uruchomienie nie zmienilo nic (idempotencja). W wersji podstawowej oznaczalo
-- to nowa tabele faktu + widok UNION ALL + nowa wersje wymiaru daty. Tutaj:
--   * wymiar daty juz zna 20 stycznia (pelny kalendarz z Z1),
--   * fakt jest jedna tabela Iceberg, MERGE dopisuje wiersze W MIEJSCU,
--   * kluczem zlaczenia jest KLUCZ ZLOZONY (order_id, product_sk), bo pozycja
--     zamowienia nie ma wlasnego identyfikatora - to ten sam problem, ktory
--     w wersji podstawowej jest zadaniem S4.
-- Sprawdzone dwukrotnie w Athenie v3: 449 po pierwszym i po drugim uruchomieniu.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Krok 1. Bronze - nowa partycja zamowien i osobna tabela pozycji
-- -----------------------------------------------------------------------------
-- Katalog Glue nie przeszukuje S3 sam. Wariant punktowy zamiast MSCK:
--   ALTER TABLE aurora_jk.bronze_orders ADD IF NOT EXISTS
--   PARTITION (dt = '2026-01-20') LOCATION 's3://aurora-jk-lake/bronze/orders/dt=2026-01-20/';
MSCK REPAIR TABLE aurora_jk.bronze_orders;

-- Oczekiwane: 2026-01-15 90 | 2026-01-16 110 | 2026-01-20 40.
SELECT dt, COUNT(*) AS n FROM aurora_jk.bronze_orders GROUP BY dt ORDER BY dt;

-- Pozycje nowego dnia jako OSOBNA tabela, bo bronze_order_items nie jest
-- partycjonowane - plik wrzucony do bronze/order_items/ zepsulby liczby
-- kontrolne dnia 1 (486 -> 526). DDL identyczny jak w wersji podstawowej.
-- bronze_order_items_20260120: jeden wiersz to jedna pozycja z 20.01 (40 wierszy).
-- Osobna tabela, bo bronze_order_items nie jest partycjonowane.
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

-- Oczekiwane: nowe = 40, stare = 486.
SELECT (SELECT COUNT(*) FROM aurora_jk.bronze_order_items_20260120) AS items_20,
       (SELECT COUNT(*) FROM aurora_jk.bronze_order_items)          AS items_dzien1;

-- -----------------------------------------------------------------------------
-- Krok 2. Watermark - od ktorego dnia liczyc
-- -----------------------------------------------------------------------------
-- Watermark to najwieksza data juz zaladowana do faktu. Wszystko po niej jest
-- przyrostem. W produkcji trzyma sie go w osobnej tabeli sterujacej; tu
-- wystarcza MAX() z faktu, bo fakt jest jedyna tabela docelowa.
-- Typ ladowania: przyrost tylko nowszych wierszy (append-only incremental) po watermarku.
-- Korekta wczorajszego wiersza tu nie wejdzie — do tego jest zakladka (Y3).
-- Oczekiwane: 2026-01-16 (przed ladowaniem) i 2026-01-20 (po nim).
SELECT MAX(order_date) AS watermark FROM aurora_jk.dw_fact_order_item;

-- -----------------------------------------------------------------------------
-- Krok 3. MERGE - przyrost do faktu transakcyjnego
-- -----------------------------------------------------------------------------
-- USING to CALA transformacja Bronze -> Gold dla nowego dnia w jednym
-- podzapytaniu: filtr partycji (dt > watermark), filtr statusu, rzutowania,
-- wyszukanie surrogate keys. Wymiar klienta po DWU warunkach: customer_id
-- ORAZ order_date BETWEEN valid_from AND valid_to - zamowienia z 20 stycznia
-- trafiaja w NOWE wersje (sk > 80) tych osmiu klientow, ktorzy sie zmienili.
--
-- ON po kluczu zlozonym (order_id, product_sk). WHEN MATCHED aktualizuje miary
-- (korekta pozycji), WHEN NOT MATCHED wstawia. Drugie uruchomienie: kazdy
-- wiersz zrodla trafia w MATCHED z tymi samymi wartosciami -> 449 bez zmian.
--
-- WHEN NOT MATCHED THEN INSERT * NIE dziala w Athenie (skladnia Delta/Spark):
-- "mismatched input '*'". Kolumny trzeba wypisac w obu listach.
-- WHEN NOT MATCHED BY SOURCE (usuwanie wierszy nieobecnych w zrodle, skladnia
-- SQL Server / Databricks) rowniez NIE istnieje w Athenie - sprawdzone,
-- opis w 02_rozwiazania.md.
-- Typ ladowania: upsert / MERGE po (order_id, product_sk). Filtr zrodla jest watermark
-- (nie zakladka); MATCHED + NOT MATCHED daje idempotencje (449 dwa razy).
MERGE INTO aurora_jk.dw_fact_order_item t
USING (
  SELECT
    i.order_id,
    CAST(substr(o.order_ts, 1, 10) AS date)              AS order_date,
    COALESCE(d.date_sk, -1)                              AS order_date_sk,
    COALESCE(c.customer_sk, -1)                          AS customer_sk,
    COALESCE(p.product_sk, -1)                           AS product_sk,
    COALESCE(st.order_status_sk, -1)                     AS order_status_sk,
    i.qty,
    i.unit_price_pln,
    i.discount_pln,
    (i.qty * i.unit_price_pln - i.discount_pln)          AS net_amount_pln
  FROM aurora_jk.bronze_order_items_20260120 i
  JOIN aurora_jk.bronze_orders o
    ON i.order_id = o.order_id
   AND o.dt > '2026-01-16'                              -- watermark
   AND o.status IN ('paid', 'shipped')
  LEFT JOIN aurora_jk.dw_dim_date d
    ON CAST(substr(o.order_ts, 1, 10) AS date) = d.date_actual
  LEFT JOIN aurora_jk.dw_dim_customer c
    ON o.customer_id = c.customer_id
   AND CAST(substr(o.order_ts, 1, 10) AS date) BETWEEN c.valid_from AND c.valid_to
  LEFT JOIN aurora_jk.dw_dim_product p
    ON i.product_id = p.product_id
  LEFT JOIN aurora_jk.dw_dim_order_status st
    ON o.status = st.status AND o.channel = st.channel
) s
ON t.order_id = s.order_id AND t.product_sk = s.product_sk
WHEN MATCHED THEN UPDATE SET
  qty            = s.qty,
  unit_price_pln = s.unit_price_pln,
  discount_pln   = s.discount_pln,
  net_amount_pln = s.net_amount_pln
WHEN NOT MATCHED THEN INSERT (order_id, order_date, order_date_sk, customer_sk,
                              product_sk, order_status_sk, qty, unit_price_pln,
                              discount_pln, net_amount_pln, load_ts)
  VALUES (s.order_id, s.order_date, s.order_date_sk, s.customer_sk,
          s.product_sk, s.order_status_sk, s.qty, s.unit_price_pln,
          s.discount_pln, s.net_amount_pln, CAST(current_timestamp AS timestamp(6)));

-- =============================================================================
-- KONTROLA
-- =============================================================================
-- Oczekiwane (ORDER BY order_date): 2026-01-15 186 | 2026-01-16 223 |
-- 2026-01-20 40 101546.00. Razem 449. Po drugim uruchomieniu: identycznie.
SELECT order_date, COUNT(*) AS rows_n, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.dw_fact_order_item
GROUP BY order_date
ORDER BY order_date;

-- Stare dni NIETKNIETE: 409 | 1427124.00. Nowy dzien wskazuje nowe wersje
-- klientow tam, gdzie klient sie zmienil. Oczekiwane: na_nowe_wersje = 0 dla
-- 15 i 16 stycznia, 2 dla 2026-01-20 (dwie pozycje klientow z nowa wersja).
SELECT order_date,
       COUNT(*) AS rows_n,
       SUM(CASE WHEN customer_sk > 80 THEN 1 ELSE 0 END) AS na_nowe_wersje
FROM aurora_jk.dw_fact_order_item
GROUP BY order_date
ORDER BY order_date;

-- Asercje z dnia 2 - musza przejsc takze po przyroscie. Oczekiwane: 4 x OK.
WITH checks AS (
  SELECT 'B1 grain unique (order_id, product_sk)' AS test, 0 AS expected,
         (SELECT COUNT(*) FROM (SELECT order_id, product_sk FROM aurora_jk.dw_fact_order_item
                                GROUP BY order_id, product_sk HAVING COUNT(*) > 1)) AS actual
  UNION ALL
  SELECT 'B2 no unknown customer', 0,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item WHERE customer_sk = -1)
  UNION ALL
  SELECT 'B3 no unknown date', 0,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item WHERE order_date_sk = -1)
  UNION ALL
  SELECT 'B4 fact = silver d1 + bronze d20', 409 + 40,
         (SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item)
)
SELECT test, expected, actual, CASE WHEN expected = actual THEN 'OK' ELSE 'FAIL' END AS status
FROM checks ORDER BY test;

-- Partycje po przyroscie: trzecia partycja pojawila sie sama (hidden
-- partitioning po order_date). Oczekiwane po pierwszym uruchomieniu: 3 wiersze,
-- 2026-01-20 = 40 wierszy w 1 pliku. Po DRUGIM uruchomieniu: 2026-01-20 =
-- 80 wierszy w 2 plikach - $partitions liczy wiersze FIZYCZNE w plikach
-- danych; 40 z nich jest skasowanych logicznie plikiem usuniec (merge-on-read).
-- SELECT COUNT(*) nadal daje 40. To jest powod, dla ktorego istnieje OPTIMIZE (Y4).
SELECT partition, record_count, file_count
FROM "aurora_jk"."dw_fact_order_item$partitions"
ORDER BY partition;

-- Snapshoty faktu. Oczekiwane po pierwszym MERGE: append (CTAS, 409)
-- i overwrite (MERGE, added 40, position_deletes NULL - same INSERT-y).
-- Po drugim uruchomieniu dochodzi kolejny overwrite: added 40 i
-- position_deletes 40 - MERGE podmienil 40 wierszy na identyczne.
-- Idempotencja dotyczy DANYCH, nie liczby snapshotow.
SELECT committed_at, operation,
       summary['added-records']          AS added,
       summary['added-position-deletes'] AS position_deletes
FROM "aurora_jk"."dw_fact_order_item$snapshots"
ORDER BY committed_at;

-- -----------------------------------------------------------------------------
-- Krok 4. Accumulating snapshot - nowe zamowienia dochodza jako nowe wiersze
-- -----------------------------------------------------------------------------
-- Ten sam MERGE, inny grain (zamowienie). Wysylki dla 20 stycznia przyjda
-- dopiero w Y3_fulfillment_merge.sql - dzis wszystkie 40 ma ship_date_sk = -1.
-- Typ ladowania: upsert nowych zamowien do accumulating snapshot (watermark jak wyzej, nie zakladka).
-- Oczekiwane po MERGE: 210 zamowien (170 + 40), takze po drugim uruchomieniu.
MERGE INTO aurora_jk.dw_fact_order_fulfillment t
USING (
  SELECT
    o.order_id,
    COALESCE(c.customer_sk, -1)      AS customer_sk,
    COALESCE(st.order_status_sk, -1) AS order_status_sk,
    COALESCE(d.date_sk, -1)          AS order_date_sk,
    SUM(i.qty * i.unit_price_pln - i.discount_pln) AS order_amount_pln,
    CAST(COUNT(*) AS integer)        AS items_n
  FROM aurora_jk.bronze_orders o
  JOIN aurora_jk.bronze_order_items_20260120 i ON o.order_id = i.order_id
  LEFT JOIN aurora_jk.dw_dim_date d
    ON CAST(substr(o.order_ts, 1, 10) AS date) = d.date_actual
  LEFT JOIN aurora_jk.dw_dim_customer c
    ON o.customer_id = c.customer_id
   AND CAST(substr(o.order_ts, 1, 10) AS date) BETWEEN c.valid_from AND c.valid_to
  LEFT JOIN aurora_jk.dw_dim_order_status st
    ON o.status = st.status AND o.channel = st.channel
  WHERE o.dt > '2026-01-16' AND o.status IN ('paid', 'shipped')
  GROUP BY o.order_id, c.customer_sk, st.order_status_sk, d.date_sk
) s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  order_amount_pln = s.order_amount_pln,
  items_n          = s.items_n
WHEN NOT MATCHED THEN INSERT (order_id, customer_sk, order_status_sk, order_date_sk,
                              ship_date_sk, delivery_date_sk, carrier,
                              order_amount_pln, items_n, ship_lag_days, delivery_lag_days)
  VALUES (s.order_id, s.customer_sk, s.order_status_sk, s.order_date_sk,
          -1, -1, 'n/a', s.order_amount_pln, s.items_n, NULL, NULL);

-- Oczekiwane: zamowienia 210 | wyslane 66 | dostarczone 45 | revenue 1528670.00
-- (1427124 + 101546).
SELECT COUNT(*)                                                 AS zamowienia,
       SUM(CASE WHEN ship_date_sk     <> -1 THEN 1 ELSE 0 END)  AS wyslane,
       SUM(CASE WHEN delivery_date_sk <> -1 THEN 1 ELSE 0 END)  AS dostarczone,
       ROUND(SUM(order_amount_pln), 2)                          AS revenue_pln
FROM aurora_jk.dw_fact_order_fulfillment;
```

### Y2a. Bronze

`MSCK REPAIR TABLE` przy pierwszym uruchomieniu:

```text
Partitions not in metastore:	bronze_orders:dt=2026-01-20
Repair: Added partition to metastore bronze_orders:dt=2026-01-20
```

Drugie uruchomienie — pusty wynik (nic do dodania).

```text
dt         | n
2026-01-15 | 90
2026-01-16 | 110
2026-01-20 | 40
```

```text
items_20 | items_dzien1
40       | 486
```

### Y2b. Watermark i MERGE

```text
watermark
2026-01-16
```

Po MERGE (i po drugim uruchomieniu — identycznie):

```text
order_date | rows_n | revenue_pln
2026-01-15 | 186    | 653531.0
2026-01-16 | 223    | 773593.0
2026-01-20 | 40     | 101546.0
```

```text
order_date | rows_n | na_nowe_wersje
2026-01-15 | 186    | 0
2026-01-16 | 223    | 0
2026-01-20 | 40     | 2
```

Dwie pozycje z 20.01 należą do klientów, którzy tego dnia zmienili miasto —
`BETWEEN valid_from AND valid_to` trafiło w wersje sk 81–88. Bez tego warunku
`LEFT JOIN` dałby dwa wiersze per pozycja i asercja B1 (grain) by nie przeszła.

### Y2c. Asercje i metadane

```text
test                                    | expected | actual | status
B1 grain unique (order_id, product_sk)  | 0        | 0      | OK
B2 no unknown customer                  | 0        | 0      | OK
B3 no unknown date                      | 0        | 0      | OK
B4 fact = silver d1 + bronze d20        | 449      | 449    | OK
```

`$partitions` po pierwszym uruchomieniu:

```text
partition                   | record_count | file_count
{order_date_day=2026-01-15} | 186          | 1
{order_date_day=2026-01-16} | 223          | 1
{order_date_day=2026-01-20} | 40           | 1
```

po drugim:

```text
partition                   | record_count | file_count
{order_date_day=2026-01-15} | 186          | 1
{order_date_day=2026-01-16} | 223          | 1
{order_date_day=2026-01-20} | 80           | 2
```

`$snapshots` po drugim uruchomieniu:

```text
committed_at                | operation | added | position_deletes
2026-09-03 08:22:06.760 UTC | append    | 409   |
2026-09-03 08:50:52.962 UTC | overwrite | 40    |
2026-09-03 08:51:17.018 UTC | overwrite | 40    | 40
```

Pierwszy MERGE to same `INSERT`-y (brak usunięć), drugi podmienił 40 wierszy na
identyczne — 40 dodanych, 40 skasowanych pozycyjnie. To jest moment, w którym
warto powiedzieć, że **idempotencja dotyczy danych, nie plików** — i zapowiedzieć
`OPTIMIZE`.

### Y2d. Accumulating snapshot

```text
zamowienia | wyslane | dostarczone | revenue_pln
210        | 66      | 45          | 1528670.0
```

210 to **zamówienia** (170 z dnia 2 + 40 z 20.01), nie minuty i nie pozycje faktu (449).

**Pułapki (sprawdzone):**

- `WHEN NOT MATCHED THEN INSERT *` → `mismatched input '*'. Expecting: '(', 'VALUES'`.
  Kolumny trzeba wypisać w obu listach.
- `WHEN NOT MATCHED BY SOURCE THEN DELETE` → `mismatched input 'BY'. Expecting:
  'AND', 'THEN'`. Athena zna tylko `WHEN MATCHED [AND …] THEN UPDATE/DELETE` i
  `WHEN NOT MATCHED [AND …] THEN INSERT`. Usunięcie wierszy nieobecnych w źródle
  to osobny `DELETE FROM t WHERE NOT EXISTS (…)`.
- Wrzucenie `order_items_2026-01-20.csv` do `bronze/order_items/` zamiast do
  osobnego prefiksu podnosi `bronze_order_items` do 526 i psuje asercję B4 oraz
  liczby kontrolne dnia 1. Przy pomyłce: skasować plik w S3 — tabela zewnętrzna
  nie ma cache'u, następny `SELECT` znów da 486.

---

## Y3. Late-arriving: spóźnione wysyłki

`bronze_shipments_20260121` — jeden wiersz to jedna wysyłka z pliku 21.01. `dw_fact_order_fulfillment` po MERGE nadal ma grain zamówienia: 210 wierszy to **210 zamówień**. To jest zakładka w tym warsztacie: plik z 21.01 poprawia zamówienia z 15, 16 i 20 stycznia, zamiast dopisywać duplikaty.

Pełny skrypt (identyczny z `sql/Y3_fulfillment_merge.sql`):

```sql
-- =============================================================================
-- Dzien 3 (wersja zaawansowana), Y3 - accumulating snapshot: spozniajace sie fakty
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Do podmiany: aurora_jk / aurora-jk-lake.
-- Wymaga dw_fact_order_fulfillment po Y2_przyrost_merge.sql (210 wierszy).
--
-- Plik: dane/bronze/shipments_20260121/shipments_2026-01-21.csv
--   -> s3://aurora-<inicjaly>-lake/bronze/shipments_20260121/
-- Zawiera trzy rodzaje wierszy - i to jest cala lekcja:
--   (a) paczki, ktore w dniu 2 byly "w drodze", teraz z delivered_ts (UPDATE
--       istniejacego wiersza faktu - to samo shipment_id co wczoraj),
--   (b) spoznione wysylki zamowien z 15/16.01, ktore w dniu 2 byly 'paid'
--       (LATE-ARRIVING FACT: zdarzenie dotyczy starego dnia, a przychodzi dzis),
--   (c) wysylki zamowien z 20.01.
-- Fakt transakcyjny by tego nie uniosl bez duplikatow; accumulating snapshot
-- jest zaprojektowany wlasnie po to, zeby byc aktualizowany.
-- Typ ladowania: zakladka / late-arriving. Plik z 21.01 poprawia zamowienia z 15, 16 i 20.01;
-- MERGE po order_id aktualizuje istniejacy wiersz, nie dopisuje duplikatu.
-- =============================================================================

-- bronze_shipments_20260121: jeden wiersz to jedna wysylka z pliku 21.01 (88 wierszy).
CREATE EXTERNAL TABLE IF NOT EXISTS aurora_jk.bronze_shipments_20260121 (
  shipment_id  string,
  order_id     string,
  carrier      string,
  shipped_ts   string,
  delivered_ts string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://aurora-jk-lake/bronze/shipments_20260121/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Oczekiwane: 88 wierszy; z_dostawa 51 | bez_dostawy 37;
-- znane_wczoraj 21 (te same shipment_id co w bronze_shipments), czyli 67 nowych.
SELECT COUNT(*) AS wysylki,
       SUM(CASE WHEN delivered_ts <> '' THEN 1 ELSE 0 END) AS z_dostawa,
       SUM(CASE WHEN delivered_ts =  '' THEN 1 ELSE 0 END) AS bez_dostawy,
       SUM(CASE WHEN shipment_id IN (SELECT shipment_id FROM aurora_jk.bronze_shipments)
                THEN 1 ELSE 0 END) AS znane_wczoraj
FROM aurora_jk.bronze_shipments_20260121;

-- Stan PRZED. Oczekiwane: 210 | wyslane 66 | dostarczone 45.
SELECT COUNT(*) AS zamowienia,
       SUM(CASE WHEN ship_date_sk     <> -1 THEN 1 ELSE 0 END) AS wyslane,
       SUM(CASE WHEN delivery_date_sk <> -1 THEN 1 ELSE 0 END) AS dostarczone
FROM aurora_jk.dw_fact_order_fulfillment;

-- -----------------------------------------------------------------------------
-- MERGE - tylko UPDATE, bez INSERT
-- -----------------------------------------------------------------------------
-- Wysylka bez zamowienia w fakcie to blad danych, nie nowy fakt - dlatego nie
-- ma galezi WHEN NOT MATCHED (Athena tego nie wymaga; Redshift wymaga obu).
-- Wiersz zrodla jest zlaczony z wymiarem daty DWA razy (sd, dd) - dwie role.
-- Zamowienie z wieloma wysylkami mialoby tu problem (MERGE odrzuca zrodlo,
-- w ktorym dwa wiersze trafiaja w ten sam wiersz celu) - stad ROW_NUMBER
-- i wybor najnowszej wysylki per zamowienie.
--
-- date_diff liczy od order_ts, ktory jest w silver_orders (albo w Bronze dla
-- 20.01) - stad zlaczenie z bronze_orders, ktore ma wszystkie 240 zamowien.
MERGE INTO aurora_jk.dw_fact_order_fulfillment t
USING (
  SELECT order_id, carrier, ship_date_sk, delivery_date_sk, ship_lag_days, delivery_lag_days
  FROM (
    SELECT
      s.order_id,
      s.carrier,
      COALESCE(sd.date_sk, -1) AS ship_date_sk,
      COALESCE(dd.date_sk, -1) AS delivery_date_sk,
      CAST(date_diff('day', CAST(o.order_ts AS timestamp), CAST(s.shipped_ts AS timestamp)) AS integer) AS ship_lag_days,
      CAST(date_diff('day', CAST(o.order_ts AS timestamp),
                     CAST(NULLIF(s.delivered_ts, '') AS timestamp)) AS integer)             AS delivery_lag_days,
      ROW_NUMBER() OVER (PARTITION BY s.order_id ORDER BY s.shipped_ts DESC) AS rn
    FROM aurora_jk.bronze_shipments_20260121 s
    JOIN aurora_jk.bronze_orders o ON s.order_id = o.order_id
    LEFT JOIN aurora_jk.dw_dim_date sd ON CAST(substr(s.shipped_ts, 1, 10) AS date)   = sd.date_actual
    LEFT JOIN aurora_jk.dw_dim_date dd ON CAST(substr(NULLIF(s.delivered_ts, ''), 1, 10) AS date) = dd.date_actual
  ) x
  WHERE rn = 1
) s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  carrier           = s.carrier,
  ship_date_sk      = s.ship_date_sk,
  delivery_date_sk  = s.delivery_date_sk,
  ship_lag_days     = s.ship_lag_days,
  delivery_lag_days = s.delivery_lag_days;

-- =============================================================================
-- KONTROLA
-- =============================================================================
-- Oczekiwane po MERGE: 210 | wyslane 133 | dostarczone 96 | bez_wysylki 77.
-- (przed MERGE bylo 210 | 66 | 45 | 144). Liczba wierszy NIE ROSNIE - zmienia
-- sie tresc. Po drugim uruchomieniu: identycznie.
SELECT COUNT(*) AS zamowienia,
       SUM(CASE WHEN ship_date_sk     <> -1 THEN 1 ELSE 0 END) AS wyslane,
       SUM(CASE WHEN delivery_date_sk <> -1 THEN 1 ELSE 0 END) AS dostarczone,
       SUM(CASE WHEN ship_date_sk      = -1 THEN 1 ELSE 0 END) AS bez_wysylki
FROM aurora_jk.dw_fact_order_fulfillment;

-- Rozklad dat wysylki dla zamowien z 15/16.01 wyslanych 17.01 albo pozniej -
-- to sa wiersze, ktore fakt transakcyjny nigdy by nie "dogonil".
-- Oczekiwane (ORDER BY ship_date):
--   2026-01-17 | 50 | 0.6
--   2026-01-18 | 14 | 1.8
--   2026-01-19 | 13 | 2.8
--   2026-01-20 |  7 | 3.4
SELECT sd.date_actual AS ship_date, COUNT(*) AS zamowienia,
       ROUND(AVG(f.ship_lag_days), 1) AS sredni_lag_dni
FROM aurora_jk.dw_fact_order_fulfillment f
JOIN aurora_jk.dw_dim_date od ON f.order_date_sk = od.date_sk
JOIN aurora_jk.dw_dim_date sd ON f.ship_date_sk  = sd.date_sk
WHERE od.date_actual <= DATE '2026-01-16' AND sd.date_actual >= DATE '2026-01-17'
GROUP BY sd.date_actual
ORDER BY ship_date;

-- Raport, ktory bylby niemozliwy na fakcie transakcyjnym: czas realizacji per
-- przewoznik (wymiar daty w trzech rolach: zamowienie, wysylka, dostawa). Oczekiwane (ORDER BY
-- carrier):
--   DHL           | 23 | 18 | 1.09 | 2.78
--   DPD           | 34 | 25 | 0.97 | 2.92
--   InPost        | 41 | 28 | 0.98 | 2.61
--   Poczta Polska | 35 | 25 | 0.97 | 2.72
SELECT carrier,
       COUNT(*) AS zamowienia,
       SUM(CASE WHEN delivery_date_sk <> -1 THEN 1 ELSE 0 END) AS dostarczone,
       ROUND(AVG(ship_lag_days), 2)     AS sredni_lag_wysylki,
       ROUND(AVG(delivery_lag_days), 2) AS sredni_lag_dostawy
FROM aurora_jk.dw_fact_order_fulfillment
WHERE carrier <> 'n/a'
GROUP BY carrier
ORDER BY carrier;

-- Time travel jako audyt: ktore zamowienia dostaly wysylke DZISIAJ?
-- Porownujemy stan biezacy ze snapshotem sprzed MERGE (ten z Y2_przyrost_merge.sql,
-- operation = overwrite, added = 40). Oczekiwane snapshoty (ORDER BY committed_at):
-- append 170 (CTAS, Dzien 2) | overwrite 40 (przyrost) | overwrite 40 (drugie
-- uruchomienie Y2_przyrost_merge.sql) | overwrite 88 (ten MERGE); po drugim
-- uruchomieniu tego pliku dochodzi kolejny overwrite 88. Do time travel nizej
-- pasuje KTORYKOLWIEK z dwu 'overwrite 40' - dane sa w nich identyczne.
SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added
FROM "aurora_jk"."dw_fact_order_fulfillment$snapshots"
ORDER BY committed_at;

-- FOR VERSION AS OF przyjmuje TYLKO literal bigint (nie podzapytanie i nie
-- liczbe z zerami wiodacymi - blad "Unsupported type for table version:
-- integer"). Wklejamy snapshot_id wiersza 'overwrite 40' z wyniku powyzej.
-- Oczekiwane: nowo_wyslane 67 | nowo_dostarczone 51 (dokladnie tyle nowych
-- shipment_id / delivered_ts bylo w pliku z 21.01).
SELECT SUM(CASE WHEN o.ship_date_sk      = -1 AND f.ship_date_sk     <> -1 THEN 1 ELSE 0 END) AS nowo_wyslane,
       SUM(CASE WHEN o.delivery_date_sk  = -1 AND f.delivery_date_sk <> -1 THEN 1 ELSE 0 END) AS nowo_dostarczone
FROM aurora_jk.dw_fact_order_fulfillment f
JOIN aurora_jk.dw_fact_order_fulfillment FOR VERSION AS OF 3900263472920469512 o  -- <- snapshot_id 'overwrite 40'
  ON f.order_id = o.order_id;
```

```text
wysylki | z_dostawa | bez_dostawy | znane_wczoraj
88      | 51        | 37          | 21
```

Przed MERGE `210 | 66 | 45`. Po MERGE (i po drugim uruchomieniu):

```text
zamowienia | wyslane | dostarczone | bez_wysylki
210        | 133     | 96          | 77
```

Spóźnione wysyłki zamówień z 15/16.01:

```text
ship_date  | zamowienia | sredni_lag_dni
2026-01-17 | 50         | 0.6
2026-01-18 | 14         | 1.8
2026-01-19 | 13         | 2.8
2026-01-20 | 7          | 3.4
```

Per przewoźnik:

```text
carrier       | zamowienia | dostarczone | sredni_lag_wysylki | sredni_lag_dostawy
DHL           | 23         | 18          | 1.09               | 2.78
DPD           | 34         | 25          | 0.97               | 2.92
InPost        | 41         | 28          | 0.98               | 2.61
Poczta Polska | 35         | 25          | 0.97               | 2.72
```

`$snapshots` faktu accumulating (po drugim uruchomieniu; drugi `overwrite 40` to
drugie uruchomienie `sql/Y2_przyrost_merge.sql`):

```text
snapshot_id         | committed_at                | operation | added
7211110241235132088 | 2026-09-03 08:22:11.930 UTC | append    | 170
3900263472920469512 | 2026-09-03 08:51:04.644 UTC | overwrite | 40
6444421570142062683 | 2026-09-03 08:51:26.562 UTC | overwrite | 40
6093568069735017090 | 2026-09-03 08:52:27.516 UTC | overwrite | 88
9184816431724199852 | 2026-09-03 08:52:42.470 UTC | overwrite | 88
```

Audyt przez time travel (`FOR VERSION AS OF 3900263472920469512`):

```text
nowo_wyslane | nowo_dostarczone
67           | 51
```

67 = 88 − 21 znanych wczoraj; 51 = wszystkie `delivered_ts` z pliku (21 dostaw
wczorajszych paczek + 30 dostaw nowych wysyłek). Bronze i Gold zgadzają się co
do wiersza.

**Pułapka:** `MERGE` odrzuca źródło, w którym dwa wiersze trafiają w ten sam
wiersz celu (`One MERGE target table row matched more than one source row`).
`ROW_NUMBER() … WHERE rn = 1` w `USING` jest po to; w tym pliku każde
zamówienie ma jedną wysyłkę, więc błąd nie występuje, ale w produkcji to
pierwszy komunikat po włączeniu prawdziwych danych.

---

## Y4. Iceberg od środka

Te instrukcje nie zmieniają grain faktu: nadal jeden wiersz to jedna pozycja zamówienia (449). `OPTIMIZE` skleja małe pliki — historia zostaje. `VACUUM` kasuje stare snapshoty — wejrzenie wstecz do nich znika. To dwa różne polecenia.

Pełny skrypt (identyczny z `sql/Y4_iceberg_operacje.sql`):

```sql
-- =============================================================================
-- Dzien 3 (wersja zaawansowana), Y4 - Iceberg od srodka: metadane, ewolucja
-- schematu, OPTIMIZE, VACUUM
-- =============================================================================
-- SILNIK: AMAZON ATHENA (engine v3). Do podmiany: aurora_jk / aurora-jk-lake.
-- Wymaga dw_fact_order_item po Y2_przyrost_merge.sql (449 wierszy, kilka
-- snapshotow).
--
-- Tabele metadanych czyta sie skladnia "baza"."tabela$nazwa" - cudzyslowy sa
-- obowiazkowe, bo $ nie jest dozwolony w zwyklym identyfikatorze.
-- Dostepne w Athenie v3: $snapshots, $history, $files,
-- $manifests, $partitions, $refs, $properties.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Y4a. Co Iceberg wie o tabeli
-- -----------------------------------------------------------------------------
-- $history: lancuch snapshotow (parent_id) i to, ktory jest biezacy.
-- Oczekiwane: tyle wierszy, ile bylo zapisow do faktu (CTAS + kazdy MERGE).
SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor
FROM "aurora_jk"."dw_fact_order_item$history"
ORDER BY made_current_at;

-- $files: pliki danych AKTUALNEGO snapshotu. Po CTAS byly 2 (jeden na
-- partycje); kazdy MERGE dopisuje nowe pliki zamiast edytowac stare.
-- $files nie ma kolumny partition - partycje odczytujemy ze sciezki. Sciezka
-- ma postac data/<hash>/order_date_day=2026-01-20/<plik>.parquet: Athena
-- wlacza write.object-storage.enabled, czyli losowy prefiks przed katalogiem
-- partycji (rozklada obciazenie S3); dlatego Icebergu nie da sie czytac
-- "po katalogach" jak tabeli Hive.
-- Oczekiwane po Y2_przyrost_merge.sql (dwa uruchomienia): 4 pliki - po jednym
-- na 15 i 16 stycznia (186 / 223 wierszy) i DWA na 20 stycznia (40 + 40 -
-- drugi MERGE dopisal kopie, stara jest skasowana logicznie). W produkcji to
-- jest problem "small files", ktory rozwiazuje OPTIMIZE nizej.
SELECT regexp_extract(file_path, 'order_date_day=[^/]+') AS partycja,
       record_count, file_size_in_bytes,
       regexp_extract(file_path, '[^/]+$') AS file_name
FROM "aurora_jk"."dw_fact_order_item$files"
ORDER BY partycja, file_name;

-- $partitions: statystyki per partycja - to, co Athena wykorzystuje do
-- partition pruning bez zagladania do plikow. record_count liczy wiersze
-- FIZYCZNE (z plikow danych), wiec 2026-01-20 pokazuje 80 w 2 plikach, choc
-- SELECT COUNT(*) da 40 - 40 wierszy jest skasowanych plikiem usuniec.
-- Oczekiwane (ORDER BY partition):
--   {order_date_day=2026-01-15} | 186 | 1 | ~3.7 KB
--   {order_date_day=2026-01-16} | 223 | 1 | ~3.9 KB
--   {order_date_day=2026-01-20} |  80 | 2 | ~5.1 KB
SELECT partition, record_count, file_count, total_size
FROM "aurora_jk"."dw_fact_order_item$partitions"
ORDER BY partition;

-- -----------------------------------------------------------------------------
-- Y4b. Ewolucja schematu - ALTER TABLE ADD COLUMNS bez przepisywania danych
-- -----------------------------------------------------------------------------
-- Iceberg trzyma schemat w metadanych i mapuje kolumny po ID, nie po pozycji.
-- Nowa kolumna to zmiana JSON-a w metadata/, zero dotknietych plikow Parquet;
-- stare wiersze zwracaja NULL. Drugie uruchomienie ALTER konczy sie bledem
-- "Column ... already exists" - to jedyna nieidempotentna instrukcja w pliku,
-- i jest to bezpieczny blad.
ALTER TABLE aurora_jk.dw_fact_order_item ADD COLUMNS (promo_code string);

-- UPDATE na tabeli Iceberg - wypelnienie nowej kolumny dla czesci wierszy.
-- Dotyka 215 wierszy z rabatem (discount_pln > 0); drugie uruchomienie
-- nadpisuje te same wartosci (idempotentne w danych, nowy snapshot).
UPDATE aurora_jk.dw_fact_order_item
SET promo_code = 'RABAT-' || CAST(CAST(discount_pln AS integer) AS varchar)
WHERE discount_pln > 0;

-- Oczekiwane (ORDER BY promo_code; NULL-e Athena sortuje na koncu):
--   RABAT-100 |  67 | 6700.00
--   RABAT-20  |  69 | 1380.00
--   RABAT-50  |  79 | 3950.00
--   (NULL)    | 234 |    0.00
SELECT promo_code, COUNT(*) AS n, ROUND(SUM(discount_pln), 2) AS rabaty_pln
FROM aurora_jk.dw_fact_order_item
GROUP BY promo_code
ORDER BY promo_code;

-- Time travel po zmianie schematu: stary snapshot czyta sie STARYM schematem
-- (bez promo_code). <snapshot_id> = pierwszy wiersz z $history.
-- SELECT * FROM aurora_jk.dw_fact_order_item FOR VERSION AS OF <snapshot_id> LIMIT 3;

-- -----------------------------------------------------------------------------
-- Y4c. OPTIMIZE - sklej male pliki; historia zostaje
-- -----------------------------------------------------------------------------
-- Kazdy MERGE i UPDATE dopisal nowe pliki w data/. Zapytanie otwiera je
-- wszystkie. OPTIMIZE ... REWRITE DATA USING BIN_PACK skleja male pliki
-- w wieksze (jak karteczki do jednego zeszytu). Aktualne zapytanie czyta
-- mniej plikow. Stare zdjecia (snapshoty) dalej istnieja - wejrzenie wstecz
-- nadal widzi stare male pliki. Nic nie jest skasowane z historii.
-- Dane: 449 wierszy przed i po. Liczba plikow PRZED (z $files wyzej) i PO
-- to cala lekcja. OPTIMIZE != VACUUM.
OPTIMIZE aurora_jk.dw_fact_order_item REWRITE DATA USING BIN_PACK;

-- Kontrola, etykiety: przed OPTIMIZE 4 pliki (186 / 223 / 40 / 40);
-- po OPTIMIZE 3 | 449 | 10696 B (jeden plik na partycje).
SELECT COUNT(*) AS pliki, SUM(record_count) AS wiersze, SUM(file_size_in_bytes) AS bajty
FROM "aurora_jk"."dw_fact_order_item$files";

-- Te same 449 pozycji i 1 528 670 zl. Przy 449 wierszach roznica "Data scanned"
-- jest w KB; przy 449 mln - w GB i w zlotowkach.
SELECT COUNT(*) AS rows_n, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM aurora_jk.dw_fact_order_item;

-- -----------------------------------------------------------------------------
-- Y4d. [po 3 h] VACUUM - wyrzuc historie, ktorej juz nie potrzebujesz
-- -----------------------------------------------------------------------------
-- To NIE jest "OPTIMIZE jeszcze raz". OPTIMIZE skleil pliki; zdjecia zostaly.
-- VACUUM kasuje snapshoty starsze niz vacuum_max_snapshot_age_seconds
-- (domyslnie 432000 s = 5 dni) i pliki, do ktorych ZADEN snapshot juz nie
-- wskazuje. Po VACUUM AS OF starego snapu przestaje dzialac.
-- Domyslnie dzis NIC nie skasuje (snapshoty sa sprzed minut).
-- Zeby zobaczyc efekt, obnizamy prog do 60 s. CENA: mniej S3, mniej
-- wejrzenia wstecz. vacuum_min_snapshots_to_keep (domyslnie 1) pilnuje,
-- zeby zawsze zostal choc biezacy snapshot.
ALTER TABLE aurora_jk.dw_fact_order_item
SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds' = '60');

VACUUM aurora_jk.dw_fact_order_item;

-- Kontrola: 5 snapshotow -> 2 (UPDATE i OPTIMIZE sprzed chwili). Wiersze: 449.
-- Tabela dw_fact_order_fulfillment (Y3) jest nietknieta.
SELECT COUNT(*) AS snapshoty FROM "aurora_jk"."dw_fact_order_item$snapshots";
SELECT COUNT(*) AS rows_n FROM aurora_jk.dw_fact_order_item;

-- Test wygaslego snapshotu (swiadomy blad; nalezy wkleic WLASNY snapshot_id z Y4a):
-- SELECT COUNT(*) FROM aurora_jk.dw_fact_order_item
-- FOR VERSION AS OF 188806921387989439;
-- Oczekiwane: INVALID_ARGUMENTS: Iceberg snapshot ID does not exists
-- OPTIMIZE != VACUUM: sklejanie zostawia historie; sprzatanie ja kasuje.

-- Przywrocenie domyslnego progu (5 dni), zeby time travel z Y5 dzialal
-- normalnie do konca zajec.
ALTER TABLE aurora_jk.dw_fact_order_item
SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds' = '432000');

-- -----------------------------------------------------------------------------
-- Y4e. [po 3 h] Co Athena nie potrafi (a Spark/Trino potrafia)
-- -----------------------------------------------------------------------------
-- Partition evolution (ALTER TABLE ... ADD PARTITION FIELD month(order_date))
-- - w Athenie v3 brak takiej instrukcji: "mismatched input 'PARTITION'.
-- Expecting: '.', 'ADD'". Zmiana partycjonowania = nowa tabela + INSERT.
-- Branching/tagging snapshotow ($refs pokazuje tylko 'main' typu BRANCH) - brak DDL.
-- MERGE ... WHEN NOT MATCHED BY SOURCE - brak: "mismatched input 'BY'.
-- Expecting: 'AND', 'THEN'" (patrz Y2_przyrost_merge.sql).
-- MERGE ... WHEN NOT MATCHED THEN INSERT * - brak: "mismatched input '*'.
-- Expecting: '(', 'VALUES'" - kolumny trzeba wypisac.
-- Szczegoly: 02_rozwiazania.md.
SELECT name, type, snapshot_id FROM "aurora_jk"."dw_fact_order_item$refs";
```

### Y4a. Metadane

`$history` po Y2 (dwa uruchomienia MERGE):

```text
made_current_at             | snapshot_id         | parent_id           | is_current_ancestor
2026-09-03 08:22:06.760 UTC | 188806921387989439  |                     | true
2026-09-03 08:50:52.962 UTC | 1389930826611884803 | 188806921387989439  | true
2026-09-03 08:51:17.018 UTC | 1771748402267456295 | 1389930826611884803 | true
```

`$files` — 4 pliki: 186 / 223 wierszy dla 15 i 16 stycznia (po ok. 3,7–3,9 KB)
i dwa po 40 dla 20 stycznia (po ok. 2,5 KB). Ścieżki mają postać
`s3://…/dw_fact_order_item/data/<hash>/order_date_day=2026-01-20/<query_id>-<uuid>.parquet`.

`$partitions` z rozmiarem:

```text
partition                   | record_count | file_count | total_size
{order_date_day=2026-01-15} | 186          | 1          | 3669
{order_date_day=2026-01-16} | 223          | 1          | 3931
{order_date_day=2026-01-20} | 80           | 2          | 5065
```

### Y4b. Ewolucja schematu i UPDATE

`ALTER TABLE … ADD COLUMNS` — OK, 0 bajtów przeskanowanych. `UPDATE` — 215 wierszy.

```text
promo_code | n   | rabaty_pln
RABAT-100  | 67  | 6700.0
RABAT-20   | 69  | 1380.0
RABAT-50   | 79  | 3950.0
           | 234 | 0.0
```

Drugie uruchomienie `ALTER`: `Cannot add column, name already exists: promo_code`
— jedyna nieidempotentna instrukcja dnia; kolejne instrukcje w pliku działają
normalnie. `UPDATE` drugi raz: te same wartości, nowy snapshot.

### Y4c. OPTIMIZE — sklej pliki, historia zostaje

Przed `OPTIMIZE` (to samo `$files` co w Y4a): **4 pliki** (186 / 223 / 40 / 40).
Po sklejeniu aktualne zapytanie czyta **3** — stare zdjęcia (snapshoty) dalej
istnieją, wejrzenie wstecz nadal widzi stare małe pliki.

```text
pliki | wiersze | bajty
3     | 449     | 10696
```

```text
rows_n | revenue_pln
449    | 1528670.0
```

Po `OPTIMIZE` `$partitions` pokazuje `2026-01-20 | 40 | 1 | 2644` — plik
usunięć został „skonsumowany”, została jedna kopia danych. Rozmiar plików 15/16.01
wzrósł o ok. 200 B (nowa kolumna `promo_code`). Dane bez zmian: **449** i
**1 528 670,00** zł.

### Y4d. `[po 3 h]` VACUUM — kasujesz stare snapshoty

To nie jest „OPTIMIZE jeszcze raz”. `OPTIMIZE` skleił pliki; historia została.
`VACUUM` wyrzuca snapshoty starsze niż próg i pliki, do których żaden snapshot
już nie wskazuje.

```text
snapshoty
2
```

```text
rows_n
449
```

Z 5 snapshotów (CTAS, MERGE, MERGE, UPDATE, OPTIMIZE) zostały dwa młodsze niż
60 s. Przy drugim uruchomieniu pliku (kilka minut później) zostały 4 — poprzednie
UPDATE/OPTIMIZE zmieściły się w oknie. W drugim, czystym przejeździe całego dnia
(skrypty puszczone jeden po drugim, bez przerw) po pierwszym `VACUUM` zostały
**3** — bo MERGE z Y2 był młodszy niż 60 s. Liczba po `VACUUM` zależy od tempa
klikania; sens jest ten sam: *stare zniknęły, dane zostały*. Na sali, gdzie od
Y2 do Y4 mija ponad godzina, wynik to 2.

Kontrola po VACUUM: `SELECT COUNT(*) … FOR VERSION AS OF 188806921387989439`
(snapshot CTAS) → `INVALID_ARGUMENTS: Iceberg snapshot ID does not exists`.
Wejrzenie wstecz do tego snapu **przestaje działać**. `OPTIMIZE` ≠ `VACUUM`.

### Y4e. Czego Athena nie umie (komunikaty z Atheny v3)

| Instrukcja | Komunikat |
|---|---|
| `ALTER TABLE … ADD PARTITION FIELD month(order_date)` | `mismatched input 'PARTITION'. Expecting: '.', 'ADD'` |
| `MERGE … WHEN NOT MATCHED BY SOURCE THEN DELETE` | `mismatched input 'BY'. Expecting: 'AND', 'THEN'` |
| `MERGE … WHEN NOT MATCHED THEN INSERT *` | `mismatched input '*'. Expecting: '(', 'VALUES'` |
| `FOR VERSION AS OF (SELECT …)` | `EXPRESSION_NOT_CONSTANT` |
| branching / tagging | brak DDL; `$refs` zwraca jeden wiersz `main | BRANCH | <snapshot_id>` |

`$manifests` i `$properties` działają; `$properties` pokazuje właściwości
tabeli (m.in. `vacuum_max_snapshot_age_seconds` po zmianie w Y4d).

---

## Y5. Redshift

Lokalny `dim_customer` po MERGE: jeden wiersz to jedna wersja klienta (89 = 81 bieżących + 8 historycznych). Lokalny fakt: jeden wiersz to jedna pozycja (449).

Pełny skrypt (identyczny z `sql/Y5_redshift_merge.sql`):

```sql
-- =============================================================================
-- Dzien 3 (wersja zaawansowana), Y5 - Redshift: MERGE do tabeli lokalnej, Spectrum
-- nad Icebergiem po zmianach, odswiezenie MV, zuzycie RPU
-- =============================================================================
-- SILNIK: AMAZON REDSHIFT SERVERLESS, Query Editor v2 (Federated user, baza dev).
-- Do podmiany: dw_jk -> dw_<inicjaly>, aurora_jk -> aurora_<inicjaly>.
-- Wymaga schematu dw_<inicjaly> z dnia 2 (Z6_redshift.sql) oraz tabel
-- Iceberg po Y1-Y3 (89 wersji klientow, 449 wierszy faktu).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Y5a. Spectrum widzi zmiany z Atheny natychmiast
-- -----------------------------------------------------------------------------
-- Tabela lokalna to kopia z soboty; Iceberg w Glue to stan po dzisiejszych
-- MERGE-ach. Oczekiwane (ORDER BY zrodlo):
--   iceberg_dim_customer 89 | iceberg_fact 449 | local_dim_customer 81 | local_fact 409
SELECT 'iceberg_dim_customer' AS zrodlo, COUNT(*) AS n FROM "awsdatacatalog"."aurora_jk"."dw_dim_customer"
UNION ALL SELECT 'iceberg_fact',         COUNT(*) FROM "awsdatacatalog"."aurora_jk"."dw_fact_order_item"
UNION ALL SELECT 'local_dim_customer',   COUNT(*) FROM dw_jk.dim_customer
UNION ALL SELECT 'local_fact',           COUNT(*) FROM dw_jk.fact_order_item
ORDER BY zrodlo;

-- -----------------------------------------------------------------------------
-- Y5b. MERGE w Redshifcie - wymiar SCD2
-- -----------------------------------------------------------------------------
-- Skladnia jak w Athenie, z dwiema roznicami (sprawdzone w Redshift Serverless):
--   * Redshift wymaga OBU galezi: WHEN MATCHED i WHEN NOT MATCHED,
--   * w WHEN MATCHED nie ma dodatkowego warunku AND (...) - filtr trzeba
--     przeniesc do zrodla albo zaakceptowac UPDATE tymi samymi wartosciami.
-- Kluczem jest customer_sk (unikalny per wersja): stare wersje dostaja
-- UPDATE valid_to / is_current, nowe (sk 81-88) - INSERT. Drugie uruchomienie:
-- wszystko trafia w MATCHED i przepisuje te same wartosci - 89 bez zmian.
MERGE INTO dw_jk.dim_customer
USING "awsdatacatalog"."aurora_jk"."dw_dim_customer" s
  ON dw_jk.dim_customer.customer_sk = s.customer_sk
WHEN MATCHED THEN UPDATE SET
  city        = s.city,
  voivodeship = s.voivodeship,
  segment     = s.segment,
  valid_to    = s.valid_to,
  is_current  = s.is_current
WHEN NOT MATCHED THEN INSERT
  (customer_sk, customer_id, first_name, last_name, city, voivodeship, segment,
   signup_date, signup_month, valid_from, valid_to, is_current)
  VALUES (s.customer_sk, s.customer_id, s.first_name, s.last_name, s.city,
          s.voivodeship, s.segment, s.signup_date, s.signup_month, s.valid_from,
          s.valid_to, s.is_current);

-- Oczekiwane: 89 | biezace 81 | historyczne 8 - jak w Athenie.
SELECT COUNT(*) AS wiersze,
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END)     AS biezace,
       SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) AS historyczne
FROM dw_jk.dim_customer;

-- -----------------------------------------------------------------------------
-- Y5c. MERGE faktu - przyrost 20 stycznia do tabeli lokalnej
-- -----------------------------------------------------------------------------
-- Zrodlem jest Iceberg w Glue - dokladnie te same wiersze, ktore Athena
-- wstawila w Y2. Klucz zlozony w ON, tak samo jak w Athenie.
-- Kolumna promo_code (dodana w Y4) nie jest tu potrzebna: INSERT wypisuje
-- kolumny jawnie, wiec nowa kolumna po stronie Icebergu niczego nie psuje.
MERGE INTO dw_jk.fact_order_item
USING (SELECT * FROM "awsdatacatalog"."aurora_jk"."dw_fact_order_item"
       WHERE order_date > DATE '2026-01-16') s
  ON dw_jk.fact_order_item.order_id   = s.order_id
 AND dw_jk.fact_order_item.product_sk = s.product_sk
WHEN MATCHED THEN UPDATE SET
  qty            = s.qty,
  unit_price_pln = s.unit_price_pln,
  discount_pln   = s.discount_pln,
  net_amount_pln = s.net_amount_pln
WHEN NOT MATCHED THEN INSERT
  (order_id, order_date, order_date_sk, customer_sk, product_sk, order_status_sk,
   qty, unit_price_pln, discount_pln, net_amount_pln, load_ts)
  VALUES (s.order_id, s.order_date, s.order_date_sk, s.customer_sk, s.product_sk,
          s.order_status_sk, s.qty, s.unit_price_pln, s.discount_pln,
          s.net_amount_pln, s.load_ts);

-- Oczekiwane (ORDER BY order_date): 2026-01-15 186 | 2026-01-16 223 |
-- 2026-01-20 40 101546.00. Razem 449 - po drugim uruchomieniu tez.
SELECT order_date, COUNT(*) AS rows_n, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM dw_jk.fact_order_item
GROUP BY order_date
ORDER BY order_date;

-- -----------------------------------------------------------------------------
-- Y5d. Materialized view - nieaktualny, dopoki go nie odswiezymy
-- -----------------------------------------------------------------------------
-- Oczekiwane PRZED: 36 | 1427124.00 (stan z soboty).
SELECT COUNT(*) AS rows_n, ROUND(SUM(revenue_pln), 2) AS revenue_pln FROM dw_jk.mv_daily_sales;

REFRESH MATERIALIZED VIEW dw_jk.mv_daily_sales;

-- Oczekiwane PO: 52 | 1528670.00 (36 + 16 kombinacji kategoria x kanal
-- z 20 stycznia; 1427124 + 101546). Drugie uruchomienie: REFRESH nic nie
-- zmienia, 52 | 1528670.00.
SELECT COUNT(*) AS rows_n, ROUND(SUM(revenue_pln), 2) AS revenue_pln FROM dw_jk.mv_daily_sales;

-- Rekoncyliacja trzech silnikow po wszystkich zmianach. Oczekiwane (ORDER BY
-- zrodlo): athena_iceberg 449 1528670 | redshift_local 449 1528670 |
-- redshift_mv 52 1528670 - trzy rozne miejsca, jedna liczba.
SELECT 'athena_iceberg' AS zrodlo, COUNT(*) AS rows_n, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM "awsdatacatalog"."aurora_jk"."dw_fact_order_item"
UNION ALL SELECT 'redshift_local', COUNT(*), ROUND(SUM(net_amount_pln), 2) FROM dw_jk.fact_order_item
UNION ALL SELECT 'redshift_mv',    COUNT(*), ROUND(SUM(revenue_pln), 2)    FROM dw_jk.mv_daily_sales
ORDER BY zrodlo;

-- -----------------------------------------------------------------------------
-- Y5e. Ile to kosztowalo - RPU
-- -----------------------------------------------------------------------------
-- SYS_SERVERLESS_USAGE: zuzycie w odcinkach po 1 minucie. charged_seconds to
-- sekundy x RPU, za ktore placimy (minimum 60 s od pierwszego zapytania).
-- Oczekiwane: kilka wierszy z dzisiejszej sesji. compute_capacity to RPU,
-- z ktorymi workgroup pracowal: przy base capacity 8 - 8; na workgroupie
-- z wlaczonym "price-performance target" (AI-driven scaling, bez base
-- capacity) potrafi pokazac 128 i charged_seconds 7680 = 128 RPU x 60 s
-- za KAZDA minute z zapytaniem. Ta jedna kolumna to caly rachunek za dzien.
SELECT start_time, end_time, compute_capacity, compute_seconds, charged_seconds
FROM SYS_SERVERLESS_USAGE
WHERE start_time >= DATEADD(hour, -3, GETDATE())
ORDER BY start_time DESC
LIMIT 10;
```

Logowanie jak w dniu 2: rola z uprawnieniami w bazie (np. **PowerUserAccess**), Query Editor v2,
*Federated user*, baza `dev`. Inna tożsamość federacyjna bez nadanych grantów w bazie
zwróci `permission denied for database dev` — szczegóły w rozwiązaniach dnia 2.

### Y5a. Spectrum vs lokalnie (przed MERGE)

```text
zrodlo               | n
iceberg_dim_customer | 89
iceberg_fact         | 449
local_dim_customer   | 81
local_fact           | 409
```

Redshift czyta tabelę Iceberg **po zmianie schematu** (kolumna `promo_code` z Y4)
bez żadnej akcji po stronie Redshifta — Spectrum bierze schemat z Glue przy
każdym zapytaniu.

### Y5b–Y5c. MERGE

```text
wiersze | biezace | historyczne
89      | 81      | 8
```

```text
order_date | rows_n | revenue_pln
2026-01-15 | 186    | 653531.0
2026-01-16 | 223    | 773593.0
2026-01-20 | 40     | 101546.0
```

Drugie uruchomienie: Y5a pokazuje już `89 / 449 / 89 / 449`, oba `MERGE`
przechodzą (wszystko w `WHEN MATCHED`), liczby bez zmian. Czas: ~4 s każdy.

**Pułapki (sprawdzone):**

- `MERGE` bez gałęzi `WHEN NOT MATCHED` → Redshift odrzuca składnię; obie gałęzie
  są wymagane (w Athenie nie są).
- `WHEN MATCHED AND (…) THEN UPDATE` → błąd składni. Filtr idzie do `USING (…)`.
- W pliku cel jest wskazywany pełną nazwą (`dw_jk.dim_customer.customer_sk`),
  a alias ma tylko źródło — tak wygląda składnia w dokumentacji Redshifta i tak
  była testowana.

### Y5d. Materialized view

Przed `REFRESH`:

```text
rows_n | revenue_pln
36     | 1427124.0
```

Po `REFRESH MATERIALIZED VIEW` (~5 s):

```text
rows_n | revenue_pln
52     | 1528670.0
```

52 = 36 + 16 nowych kombinacji kategoria × kanał dla 20 stycznia (na 18
możliwych — dwie kombinacje nie miały sprzedaży). Drugie `REFRESH`: bez zmian.

```text
zrodlo         | rows_n | revenue_pln
athena_iceberg | 449    | 1528670.0
redshift_local | 449    | 1528670.0
redshift_mv    | 52     | 1528670.0
```

### Y5e. RPU

```text
start_time          | end_time            | compute_capacity | compute_seconds | charged_seconds
2026-09-03 09:22:00 | 2026-09-03 09:23:00 | 128.0            | 2146.0          | 7680
2026-09-03 09:21:00 | 2026-09-03 09:22:00 | 128.0            | 679.0           | 0
2026-09-03 09:18:00 | 2026-09-03 09:19:00 | 0.0              | 0.0             | 7680
2026-09-03 09:17:00 | 2026-09-03 09:18:00 | 128.0            | 1.0             | 7680
2026-09-03 09:16:00 | 2026-09-03 09:17:00 | 128.0            | 337.0           | 7680
…
```

Workgroup z włączonym *price-performance target* zamiast
stałej base capacity przydzielił tu **128 RPU** i nalicza
`128 × 60 = 7680` RPU-sekund za każdą minutę z aktywnością (wiersze z `0`
w `charged_seconds` to minuty rozliczone w sąsiednim wierszu). Przy stawce
~0,4 USD za RPU-godzinę to ok. **0,85 USD za każdą minutę** działania — argument
za tym, żeby własny workgroup miał ustawioną base capacity 8 i żeby po zajęciach
zniknął.

---

## Y6. Capstone

`dw_fact_daily_sales` po MERGE — jeden wiersz to dzień × kategoria × kanał, już z 20 stycznia. Lineage pokazuje, na którym etapie kwota ma prawo się różnić. `MERGE` agregatu idzie po watermarku (`order_date_sk > MAX`), jak Y2, nie jak zakładka.

Pełny skrypt (identyczny z `sql/Y6_capstone_i_sprzatanie.sql`):

```sql
-- =============================================================================
-- Dzien 3 (wersja zaawansowana), Y6 - capstone: jedna liczba przez cale 9 godzin
-- + sprzatanie
-- =============================================================================
-- SILNIK: AMAZON ATHENA. Do podmiany: aurora_jk / aurora-jk-lake.
--
-- Capstone laczy wszystko: Bronze (dzien 1) -> Silver -> Gold Iceberg (dzien 2)
-- -> przyrost i SCD2 (dzien 3). Jedno zapytanie na warstwe, jedna kwota,
-- ktora musi sie zgadzac na kazdym etapie.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Y6a. Lineage w jednym wyniku
-- -----------------------------------------------------------------------------
-- Oczekiwane (ORDER BY etap):
--   1_bronze_items   526 (486 + 40, bez filtra statusu)
--   2_silver_items   409 1427124.00 (tylko dni 15/16 - Silver nie byl
--                    przeladowywany w wersji zaawansowanej; przyrost szedl
--                    prosto Bronze -> Gold przez MERGE)
--   3_gold_fact      449 1528670.00
--   4_gold_aggregate 36  1427124.00 (agregat z soboty - NIE zostal przeliczony)
-- Ostatni wiersz to celowa "usterka": agregat jest osobna tabela i nikt go
-- nie odswiezyl. W Redshifcie MV rozwiazal to jednym REFRESH; w Athenie
-- trzeba to zrobic samemu (Y6b).
SELECT '1_bronze_items' AS etap, COUNT(*) AS rows_n, CAST(NULL AS double) AS revenue_pln
FROM (SELECT order_id FROM aurora_jk.bronze_order_items
      UNION ALL SELECT order_id FROM aurora_jk.bronze_order_items_20260120)
UNION ALL
SELECT '2_silver_items', COUNT(*), ROUND(SUM(net_amount_pln), 2) FROM aurora_jk.silver_order_items
UNION ALL
SELECT '3_gold_fact', COUNT(*), ROUND(SUM(net_amount_pln), 2) FROM aurora_jk.dw_fact_order_item
UNION ALL
SELECT '4_gold_aggregate', COUNT(*), ROUND(SUM(revenue_pln), 2) FROM aurora_jk.dw_fact_daily_sales
ORDER BY etap;

-- -----------------------------------------------------------------------------
-- Y6b. Odswiezenie agregatu przez MERGE (agregat tez jest Icebergiem)
-- -----------------------------------------------------------------------------
-- Ten sam wzorzec co przy fakcie: przeliczamy TYLKO dni po watermarku agregatu
-- i wmergujemy. Typ ladowania: przyrost agregatu po watermarku (order_date_sk > MAX), jak Y2, nie jak zakladka.
-- Oczekiwane po MERGE: 52 | 1528670.00 - tyle samo, co MV
-- w Redshifcie po REFRESH. Drugie uruchomienie: bez zmian (podzapytanie
-- MAX(order_date_sk) daje juz 20260120, zrodlo MERGE jest puste).
MERGE INTO aurora_jk.dw_fact_daily_sales t
USING (
  SELECT f.order_date_sk, p.category, st.channel,
         COUNT(*) AS lines_n, COUNT(DISTINCT f.order_id) AS orders_n,
         SUM(f.qty) AS qty, ROUND(SUM(f.net_amount_pln), 2) AS revenue_pln
  FROM aurora_jk.dw_fact_order_item f
  JOIN aurora_jk.dw_dim_product p       ON f.product_sk      = p.product_sk
  JOIN aurora_jk.dw_dim_order_status st ON f.order_status_sk = st.order_status_sk
  WHERE f.order_date_sk > (SELECT MAX(order_date_sk) FROM aurora_jk.dw_fact_daily_sales)
  GROUP BY f.order_date_sk, p.category, st.channel
) s
ON t.order_date_sk = s.order_date_sk AND t.category = s.category AND t.channel = s.channel
WHEN MATCHED THEN UPDATE SET
  lines_n = s.lines_n, orders_n = s.orders_n, qty = s.qty, revenue_pln = s.revenue_pln
WHEN NOT MATCHED THEN INSERT (order_date_sk, category, channel, lines_n, orders_n, qty, revenue_pln)
  VALUES (s.order_date_sk, s.category, s.channel, s.lines_n, s.orders_n, s.qty, s.revenue_pln);

SELECT COUNT(*) AS rows_n, ROUND(SUM(revenue_pln), 2) AS revenue_pln
FROM aurora_jk.dw_fact_daily_sales;

-- -----------------------------------------------------------------------------
-- Y6c. Pytanie biznesowe, ktore wymaga WSZYSTKIEGO z trzech dni
-- -----------------------------------------------------------------------------
-- "Przychod per wojewodztwo klienta W CHWILI ZAKUPU, per dzien, z sumami" -
-- SCD2 (wojewodztwo z wersji obowiazujacej w dniu zamowienia), pelny kalendarz,
-- przyrost, ROLLUP. Klienci, ktorzy 20.01 zmienili wojewodztwo, licza sie
-- 15/16.01 do starego, a 20.01 do nowego.
-- Oczekiwane: 34 wiersze - 10 wojewodztw + 'razem' na kazdy z 3 dni i wiersz
-- koncowy. Wiersze 'razem': 2026-01-15 74 zamowien 653531 | 2026-01-16 96
-- 773593 | 2026-01-20 40 101546 | razem 210 1528670. Pelna lista w
-- 02_rozwiazania.md. Sortowanie wewnatrz dnia: revenue_pln malejaco.
SELECT
  COALESCE(CAST(d.date_actual AS varchar), 'razem') AS dzien,
  COALESCE(c.voivodeship, 'razem')                   AS wojewodztwo,
  COUNT(DISTINCT f.order_id)                         AS zamowienia,
  ROUND(SUM(f.net_amount_pln), 2)                    AS revenue_pln
FROM aurora_jk.dw_fact_order_item f
JOIN aurora_jk.dw_dim_date d     ON f.order_date_sk = d.date_sk
JOIN aurora_jk.dw_dim_customer c ON f.customer_sk   = c.customer_sk
GROUP BY ROLLUP (d.date_actual, c.voivodeship)
ORDER BY GROUPING(d.date_actual), d.date_actual, GROUPING(c.voivodeship), revenue_pln DESC;

-- =============================================================================
-- SPRZATANIE (po zajeciach - nie w trakcie)
-- =============================================================================
-- Tabele Iceberg: DROP TABLE kasuje TAKZE pliki na S3 (data/ i metadata/).
-- To roznica wobec tabel zewnetrznych z dnia 1, gdzie DROP zostawial pliki.
-- Koszt bez sprzatania: S3 - grosze miesiecznie za kilka MB; Glue - katalog
-- jest darmowy do milionu obiektow. Realny koszt w tej wersji to Redshift
-- Serverless (RPU-godziny) - i to sprzata osoba, ktora stawiala workgroup.
--
-- DROP TABLE IF EXISTS aurora_jk.dw_fact_daily_sales;
-- DROP TABLE IF EXISTS aurora_jk.dw_fact_order_fulfillment;
-- DROP TABLE IF EXISTS aurora_jk.dw_fact_order_item;
-- DROP TABLE IF EXISTS aurora_jk.dw_dim_order_status;
-- DROP TABLE IF EXISTS aurora_jk.dw_dim_product;
-- DROP TABLE IF EXISTS aurora_jk.dw_dim_customer;
-- DROP TABLE IF EXISTS aurora_jk.dw_dim_date;
--
-- Tabele zewnetrzne (DROP nie rusza plikow; pliki kasujemy w konsoli S3):
-- DROP TABLE IF EXISTS aurora_jk.bronze_shipments;
-- DROP TABLE IF EXISTS aurora_jk.bronze_shipments_20260121;
-- DROP TABLE IF EXISTS aurora_jk.dw_export_daily_sales;      -- + prefiks export/
-- (bronze_customers_updates i bronze_order_items_20260120 sa wspolne z wersja
--  podstawowa - jesli ktos robil oba, kasuje raz.)
--
-- Redshift (QEv2):  DROP SCHEMA dw_jk CASCADE;
```

### Y6a. Lineage (przed odświeżeniem agregatu)

```text
etap             | rows_n | revenue_pln
1_bronze_items   | 526    |
2_silver_items   | 409    | 1427124.0
3_gold_fact      | 449    | 1528670.0
4_gold_aggregate | 36     | 1427124.0
```

### Y6b. Po MERGE agregatu

```text
rows_n | revenue_pln
52     | 1528670.0
```

Drugie uruchomienie: lineage pokazuje już `4_gold_aggregate 52 | 1528670.0`,
`MERGE` przechodzi ze źródłem pustym, wynik bez zmian.

### Y6c. ROLLUP: województwo w chwili zakupu × dzień

```text
dzien      | wojewodztwo        | zamowienia | revenue_pln
2026-01-15 | pomorskie          | 20         | 195913.0
2026-01-15 | lubelskie          | 11         | 106754.0
2026-01-15 | mazowieckie        | 10         | 91107.0
2026-01-15 | dolnośląskie       | 8          | 86273.0
2026-01-15 | małopolskie        | 5          | 57800.0
2026-01-15 | wielkopolskie      | 6          | 46514.0
2026-01-15 | łódzkie            | 5          | 29461.0
2026-01-15 | śląskie            | 6          | 22602.0
2026-01-15 | zachodniopomorskie | 2          | 12221.0
2026-01-15 | podlaskie          | 1          | 4886.0
2026-01-15 | razem              | 74         | 653531.0
2026-01-16 | mazowieckie        | 18         | 137260.0
2026-01-16 | pomorskie          | 12         | 132928.0
2026-01-16 | małopolskie        | 9          | 112770.0
2026-01-16 | podlaskie          | 9          | 86051.0
2026-01-16 | łódzkie            | 9          | 71635.0
2026-01-16 | wielkopolskie      | 12         | 62226.0
2026-01-16 | śląskie            | 8          | 57685.0
2026-01-16 | zachodniopomorskie | 6          | 46481.0
2026-01-16 | lubelskie          | 7          | 44313.0
2026-01-16 | dolnośląskie       | 6          | 22244.0
2026-01-16 | razem              | 96         | 773593.0
2026-01-20 | pomorskie          | 10         | 29814.0
2026-01-20 | śląskie            | 5          | 18422.0
2026-01-20 | małopolskie        | 5          | 18192.0
2026-01-20 | lubelskie          | 3          | 12795.0
2026-01-20 | mazowieckie        | 6          | 10001.0
2026-01-20 | wielkopolskie      | 2          | 7856.0
2026-01-20 | zachodniopomorskie | 3          | 2825.0
2026-01-20 | łódzkie            | 3          | 1245.0
2026-01-20 | dolnośląskie       | 2          | 267.0
2026-01-20 | podlaskie          | 1          | 129.0
2026-01-20 | razem              | 40         | 101546.0
razem      | razem              | 210        | 1528670.0
```

34 wiersze. Suma `zamowienia` w wierszach `razem` to 210 = 170 + 40 zamówień
`paid`/`shipped` — ta sama liczba, co `COUNT(*)` z `dw_fact_order_fulfillment`.
Rekoncyliacja przez trzy fakty o różnym grain, jedna kwota.

---

## Sprzątanie — co zostaje i ile kosztuje

Sekcja sprzątania w `sql/Y6_capstone_i_sprzatanie.sql` (zakomentowana).
`DROP TABLE` na Icebergu w Athenie kasuje także pliki z S3 (`data/` i
`metadata/` pod lokalizacją tabeli). Tabele zewnętrzne (`bronze_*`,
`dw_export_daily_sales`) zostawiają pliki — prefiksy do skasowania ręcznie:
`bronze/customers_updates/`, `bronze/orders/dt=2026-01-20/`,
`bronze/order_items_20260120/`, `bronze/shipments/`, `bronze/shipments_20260121/`,
`export/`. Redshift: `DROP SCHEMA dw_<inicjaly> CASCADE`; workgroup postawiony
na potrzeby tej wersji — kasuje osoba, która go tworzyła (RPU to jedyny realny koszt).

Sprawdzone w konsoli AWS po drugim przejeździe: dziesięć `DROP TABLE IF EXISTS`
z sekcji przechodzi bez błędu; po siedmiu `DROP` na Icebergu prefiks `dw/` na S3
jest **pusty** (0 obiektów), po `DROP` na `bronze_shipments` plik CSV pod
`bronze/shipments/` **zostaje** (1 obiekt). `DROP SCHEMA dw_<inicjaly> CASCADE`
w Redshifcie kasuje razem z MV. Reszta (Bronze, Silver dnia 1, bucket) to
sprzątanie W10 z wersji podstawowej — to samo dla obu wersji.
