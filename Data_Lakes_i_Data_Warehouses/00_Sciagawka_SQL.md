# Ściągawka SQL — Athena i Redshift

Wzorce składni na wszystkie trzy dni. Przykłady są celowo na innych tabelach niż
ćwiczenia — chodzi o kształt instrukcji, kolumny trzeba dopasować samodzielnie.

Athena mówi dialektem **Trino** (dawniej PrestoSQL), a nie T-SQL czy PL/pgSQL.
Stąd `date_format` zamiast `TO_CHAR`, `ARRAY[...]` w nawiasach kwadratowych
i brak `TOP n` (jest `LIMIT n`).

---

## 1. Baza w Glue Data Catalog

```sql
CREATE DATABASE IF NOT EXISTS moja_baza;
```

Baza w Athenie nie ma schematów w środku. Warstwy rozdziela się prefiksem w nazwie
tabeli: `bronze_orders`, `silver_orders`, `gold_dim_date`.

---

## 2. Tabela zewnętrzna na plikach CSV

`EXTERNAL` znaczy: katalog trzyma tylko opis, dane zostają na S3. `DROP TABLE`
kasuje opis, pliki zostają nietknięte.

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS moja_baza.bronze_przyklad (
  kolumna_tekstowa  string,
  kolumna_calkowita int,
  kolumna_kwotowa   double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://moj-bucket/bronze/przyklad/'
TBLPROPERTIES ('skip.header.line.count'='1');
```

Co tu jest istotne:

| Element | Znaczenie |
|---|---|
| `IF NOT EXISTS` | drugie uruchomienie skryptu nie wywala się błędem |
| kolejność kolumn | musi odpowiadać kolejności w pliku — dopasowanie idzie po pozycji, nie po nazwie |
| `SERDE` | *Serializer/Deserializer* — biblioteka, która zamienia bajty pliku na wiersze |
| `OpenCSVSerde` | radzi sobie z cudzysłowami i przecinkami w wartościach |
| `LOCATION` | **katalog**, nigdy pojedynczy plik; Athena czyta wszystko, co w nim leży |
| `skip.header.line.count` | bez tego wiersz nagłówka wejdzie do wyników jako dane |

Pułapka `OpenCSVSerde`: wszystkie kolumny czyta jako tekst i dopiero rzutuje
na zadeklarowany typ. Daty najbezpieczniej deklarować jako `string` i konwertować
dopiero w warstwie Silver.

---

## 3. Tabela na plikach JSON

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS moja_baza.bronze_zdarzenia (
  event_id   string,
  event_ts   string,
  event_type string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://moj-bucket/bronze/zdarzenia/';
```

Format to **JSON Lines** — jeden obiekt JSON na linię, bez przecinków między
obiektami i bez nawiasów tablicy wokół całości. Zwykły „ładny” JSON z tablicą
w Athenie się nie wczyta.

**Domyślny SerDe:** `org.openx.data.jsonserde.JsonSerDe` (OpenX). Typy w DDL
(`int`, `double`, `string`) mapują się na typy natywne w JSON — np. `product_id int`
działa, bo w pliku jest liczba, nie tekst w cudzysłowie.

**Zamiennik — tylko gdy OpenX zawiedzie.** Objaw: `CREATE TABLE` przechodzi,
ale `SELECT` zwraca `HIVE_CURSOR_ERROR` albo same `NULL` (parser nie rozpoznaje
formatu linii albo odrzuca znaki specjalne). Wtedy:

1. `DROP TABLE` (pliki na S3 zostają),
2. podmień SerDe na `org.apache.hive.hcatalog.data.JsonSerDe`,
3. `CREATE EXTERNAL TABLE` ponownie,
4. `MSCK REPAIR TABLE` jeśli tabela jest partycjonowana.

| SerDe | Kiedy |
|---|---|
| `org.openx.data.jsonserde.JsonSerDe` | **Zawsze zaczynaj od tego** — wybacza brakujące pola, mapuje typy JSON |
| `org.apache.hive.hcatalog.data.JsonSerDe` | Gdy OpenX zwraca błąd kursora przy poprawnych plikach JSON Lines |

---

## 4. Partycje

Partycja to katalog w konwencji `nazwa=wartosc`, np. `orders/dt=2026-01-15/`.
Athena traktuje `dt` jak zwykłą kolumnę, ale filtr po niej pozwala **pominąć odczyt**
pozostałych katalogów (*partition pruning*) — stąd mniejszy rachunek.

Katalog nie wykrywa nowych partycji sam:

```sql
MSCK REPAIR TABLE moja_baza.bronze_zdarzenia;
```

Bez tego `SELECT COUNT(*)` zwraca **0**, choć pliki leżą na S3.

Wariant punktowy, szybszy przy dużej liczbie partycji:

```sql
ALTER TABLE moja_baza.bronze_zdarzenia
ADD IF NOT EXISTS PARTITION (dt = '2026-01-20')
LOCATION 's3://moj-bucket/bronze/zdarzenia/dt=2026-01-20/';
```

Podgląd zarejestrowanych partycji:

```sql
SHOW PARTITIONS moja_baza.bronze_zdarzenia;
```

---

## 5. CTAS — tworzenie tabeli z wyniku zapytania

`CREATE TABLE AS SELECT` wykonuje `SELECT`, **zapisuje wynik na S3 jako nowe pliki**
i rejestruje tabelę w katalogu. To podstawowe narzędzie transformacji w ELT.

```sql
CREATE TABLE IF NOT EXISTS moja_baza.silver_przyklad
WITH (
  format              = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by      = ARRAY['dt'],
  external_location   = 's3://moj-bucket/silver/przyklad/'
) AS
SELECT
  id,
  CAST(kwota AS double)          AS kwota,
  CAST(substr(ts, 1, 10) AS date) AS data_zdarzenia,
  dt                              -- kolumna partycjonująca ZAWSZE na końcu
FROM moja_baza.bronze_przyklad
WHERE status IN ('paid', 'shipped');
```

Właściwości w `WITH (...)`:

| Właściwość | Znaczenie |
|---|---|
| `format` | `PARQUET`, `ORC`, `AVRO`, `JSON`, `TEXTFILE`. Domyślnie `PARQUET` |
| `parquet_compression` | `SNAPPY` (szybka) lub `GZIP` (mniejsze pliki, wolniejsza) |
| `partitioned_by` | lista kolumn partycjonujących; muszą być ostatnie w `SELECT` |
| `external_location` | gdzie zapisać pliki. Bez tego Athena wybiera katalog w lokalizacji wyników |
| `bucketed_by` / `bucket_count` | rozrzuca dane po N plikach wg haszu kolumny; przydatne przy dużych złączeniach |

**Idempotencja.** Athena nigdy nie kasuje danych z S3. Bez `IF NOT EXISTS` drugie
uruchomienie kończy się `TABLE_ALREADY_EXISTS`. Z `IF NOT EXISTS` zostanie pominięte.

Jeden przypadek, którego `IF NOT EXISTS` nie ratuje: tabela skasowana przez `DROP`,
ale pliki nadal leżą pod `external_location`. Wtedy pojawia się
`HIVE_PATH_ALREADY_EXISTS` i trzeba usunąć prefiks w S3 albo wskazać inną ścieżkę.

---

## 6. CTAS dla tabeli Iceberg

```sql
CREATE TABLE IF NOT EXISTS moja_baza.silver_przyklad_iceberg
WITH (
  table_type  = 'ICEBERG',
  format      = 'PARQUET',
  location    = 's3://moj-bucket/silver/przyklad_iceberg/',
  is_external = false
) AS
SELECT * FROM moja_baza.silver_przyklad;
```

Różnice wobec zwykłego CTAS:

- `location` zamiast `external_location`
- `is_external = false` — Athena zarządza plikami i metadanymi tej tabeli
- `partitioned_by` nie działa; Iceberg używa `partitioning = ARRAY['month(data)']`
  (transformacje `day(...)`, `month(...)`, `year(...)`, `bucket(n, kol)`,
  `truncate(n, kol)` — *hidden partitioning*, kolumna partycjonująca zostaje
  zwykłą kolumną, filtr po niej i tak przycina pliki)

Pełny zestaw operacji na Icebergu (MERGE z wieloma gałęziami, time travel,
tabele metadanych, `OPTIMIZE`, `VACUUM`) jest w sekcji 12 — wersja zaawansowana.

Historia zmian (każda operacja tworzy snapshot):

```sql
SELECT committed_at, operation
FROM "moja_baza"."silver_przyklad_iceberg$snapshots"
ORDER BY committed_at;
```

---

## 7. MERGE — tylko na tabelach Iceberg

Na zwykłej tabeli zewnętrznej `UPDATE`, `DELETE` i `MERGE` nie działają: na S3
można dopisać plik, nie da się zmienić wiersza. Iceberg to odblokowuje.

```sql
MERGE INTO moja_baza.docelowa t
USING moja_baza.zrodlowa s
  ON t.id = s.id
WHEN MATCHED THEN UPDATE SET
  status = s.status
WHEN NOT MATCHED THEN INSERT (id, status, dt)
     VALUES (s.id, s.status, s.dt);
```

Athena **nie zna** skrótu `WHEN NOT MATCHED THEN INSERT *` (to składnia Delta Lake
i Sparka) — zwraca `mismatched input '*'. Expecting: '(', 'VALUES'`. Kolumny trzeba
wypisać.

`MERGE` jest idempotentny: to samo uruchomione dwa razy nie zdubluje wierszy.

---

## 8. Widoki

```sql
CREATE OR REPLACE VIEW moja_baza.v_przyklad AS
SELECT id, kwota FROM moja_baza.silver_przyklad;
```

Widok w Athenie to zapisane zapytanie, nie dane. Nic nie kosztuje w storage,
a `CREATE OR REPLACE` można uruchamiać wielokrotnie.

---

## 9. Funkcje przydatne w tych ćwiczeniach

| Zadanie | Athena / Trino |
|---|---|
| tekst → data | `CAST(substr(ts, 1, 10) AS date)` |
| tekst → znacznik czasu | `CAST(ts AS timestamp)` |
| części daty | `year(d)`, `month(d)`, `day(d)` — zwracają **bigint** |
| nazwa dnia tygodnia | `date_format(CAST(d AS timestamp), '%W')` |
| numer dnia tygodnia | `day_of_week(d)` — 1 = poniedziałek, 7 = niedziela |
| zaokrąglenie kwoty | `ROUND(suma, 2)` |
| surrogate key | `CAST(ROW_NUMBER() OVER (ORDER BY id) AS integer)` |
| pierwsza wartość nie-NULL | `COALESCE(a, b)` |
| liczba unikalnych | `COUNT(DISTINCT kolumna)` |

**Uwaga na typy.** `ROW_NUMBER()`, `year()`, `month()` i `day()` zwracają `bigint`.
Jeżeli wynik ma potem trafić przez `COPY` do kolumny `int` w Redshifcie, trzeba
je jawnie rzutować: `CAST(... AS integer)`. Inaczej `COPY` przerwie na niezgodności
typów, bo Parquet zapisze `int64` zamiast `int32`.

---

## 10. Redshift — składnia inna niż w Athenie

### Jak nazywa się tabela — trzy formy, każda dla innego przypadku

To jest pierwsza rzecz do ustalenia przy przechodzeniu między silnikami
i najczęstsze źródło błędu dnia 2.

| Forma | Silnik | Znaczenie |
|---|---|---|
| `aurora_xx.gold_dim_date` | **tylko Athena** | baza w Glue Data Catalog + tabela |
| `"awsdatacatalog"."aurora_xx"."gold_dim_date"` | **tylko Redshift** | ta sama tabela w tym samym katalogu Glue, oglądana z Redshifta: katalog · baza Glue · tabela |
| `gold_xx.dim_date` | **tylko Redshift** | lokalna tabela w bazie `dev`, załadowana instrukcją `COPY` |

`awsdatacatalog` to nazwa zarezerwowana. Redshift Serverless montuje pod nią
katalog Glue całego konta w trybie **tylko do odczytu**, więc żaden `INSERT`,
`UPDATE` ani `CTAS` nie zapisze przez nią niczego. Warunki, żeby ta baza była
widoczna: połączenie w Query Editorze v2 jako **Federated user** do bazy `dev`
oraz `SHOW data_catalog_auto_mount;` zwracające `on`.

Zapis trzyczłonowy jest przy tej bazie obowiązkowy — `USE` i `search_path`
dla `awsdatacatalog` nie działają.

```sql
SHOW data_catalog_auto_mount;              -- oczekiwane: on
SHOW SCHEMAS FROM DATABASE awsdatacatalog; -- widoczne bazy Glue
SHOW TABLES FROM SCHEMA awsdatacatalog.aurora_xx;

SELECT COUNT(*) FROM "awsdatacatalog"."aurora_xx"."gold_fact_order_item";
```

Wariant jawny, gdy zamiast trzech członów ma być zwykły schemat (klasyczny
Redshift Spectrum):

```sql
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_xx
FROM DATA CATALOG DATABASE 'aurora_xx'
IAM_ROLE 'arn:aws:iam::<konto>:role/<rola>';
```

Różnica jest w uprawnieniach, nie w wyniku: external schema i `COPY` czytają S3
**rolą IAM** podpiętą do namespace'u, a automatyczne montowanie —
**tożsamością zalogowanego użytkownika**.

### Schematy i tabele lokalne

Redshift ma prawdziwe schematy wewnątrz bazy i klasyczny `CREATE TABLE` z typami.

```sql
CREATE SCHEMA IF NOT EXISTS gold_xx;

CREATE TABLE IF NOT EXISTS gold_xx.dim_przyklad (
  klucz_sk    int PRIMARY KEY,
  nazwa       varchar(128),
  kwota       double precision,
  data_od     date,
  czy_aktualny boolean
)
DISTSTYLE AUTO
SORTKEY (klucz_sk);
```

- `DISTSTYLE AUTO` — Redshift sam decyduje, jak rozłożyć wiersze między węzły.
  Ręcznie ustawia się `KEY` (rozkład po kolumnie złączenia) lub `ALL` (kopia całej
  tabeli na każdym węźle — dobre dla małych wymiarów).
- `SORTKEY` — fizyczna kolejność zapisu. Kolumna filtrowana najczęściej (zwykle data)
  pozwala pomijać całe bloki przy odczycie.

Ładowanie danych z S3 — **równolegle, instrukcją `COPY`**, nie `INSERT`-ami:

```sql
COPY gold_xx.dim_przyklad
FROM 's3://moj-bucket/gold/dim_przyklad/'
IAM_ROLE default
FORMAT AS PARQUET;
```

`FORMAT AS PARQUET` dopasowuje kolumny **po kolejności i po typie**, nie po nazwie.
Liczba i typy kolumn w `CREATE TABLE` muszą odpowiadać plikom.

Kolumna partycjonująca (np. `dt`) **nie jest zapisana w pliku Parquet** — siedzi
w ścieżce S3. Tabela w Redshifcie ma więc o tę kolumnę mniej.

---

## 11. Błędy, które pojawiają się najczęściej

| Komunikat | Przyczyna | Co zrobić |
|---|---|---|
| `No output location provided` | brak lokalizacji wyników Atheny | Query settings → Manage → `s3://…/athena-results/` |
| `COUNT(*)` zwraca 0 mimo plików | partycje niezarejestrowane | `MSCK REPAIR TABLE` |
| `TABLE_ALREADY_EXISTS` | CTAS uruchomiony drugi raz | dopisać `IF NOT EXISTS` albo `DROP TABLE` |
| `HIVE_PATH_ALREADY_EXISTS` | tabela skasowana, pliki zostały | usunąć prefiks w S3 lub zmienić `external_location` |
| `HIVE_CURSOR_ERROR` na JSON | niewłaściwy SerDe lub JSON nie w formacie JSON Lines | podmienić SerDe na wariant HCatalog |
| wiersz nagłówka w wynikach | brak `skip.header.line.count` | dodać do `TBLPROPERTIES` |
| `Column 'x' is ambiguous` | ta sama nazwa w dwóch złączonych tabelach | kwalifikować aliasem: `o.customer_id` |
| `Data scanned: 0 KB` przy powtórzeniu | włączone *Reuse query results* | wyłączyć przełącznik nad wynikami |
| `mismatched input '*'` w `MERGE` | `INSERT *` nie istnieje w Athenie | wypisać kolumny i `VALUES` |
| `ERROR: schema "aurora_xx" does not exist` w Redshifcie | zapytanie Athenowe (nazwa dwuczłonowa) wklejone do Query Editora v2; Redshift szuka wtedy lokalnego schematu w bazie `dev` | dopisać człon katalogu: `"awsdatacatalog"."aurora_xx"."gold_…"`. Nigdy przez utworzenie lokalnego schematu `aurora_xx` |
| `ERROR: database "awsdatacatalog" does not exist` | połączenie inne niż *Federated user* albo `data_catalog_auto_mount = off` | połączenie jako *Federated user* do bazy `dev`; `ALTER SYSTEM SET data_catalog_auto_mount = on` po stronie administratora |
| `permission denied for database awsdatacatalog` | brak `GRANT USAGE` dla własnej tożsamości | `GRANT USAGE ON DATABASE awsdatacatalog TO "IAMR:<rola>"` po stronie administratora bazy |
| `S3ServiceException: Access Denied` przy `COPY` | rola IAM workgroupu nie czyta tego bucketu | uprawnienia S3 dla roli namespace'u; obejście na dziś to odczyt przez `awsdatacatalog` |
| `EXPRESSION_NOT_CONSTANT` przy `FOR VERSION AS OF` | podzapytanie po `$snapshots` zamiast liczby | wkleić `snapshot_id` jako literał |
| `Unsupported type for table version: integer` | za krótka liczba po `AS OF` (prawdziwe id mają 19 cyfr) | wkleić pełny `snapshot_id` |
| `Iceberg snapshot ID does not exists` | snapshot skasowany przez `VACUUM` | time travel sięga tylko tak daleko, jak pozwala `vacuum_max_snapshot_age_seconds` |
| `Cannot add column, name already exists` | `ALTER TABLE … ADD COLUMNS` uruchomiony drugi raz | bezpieczny błąd — pominąć |
| `mismatched input 'BY'` w `MERGE` | `WHEN NOT MATCHED BY SOURCE` nie istnieje w Athenie | osobny `DELETE … WHERE NOT EXISTS (…)` |
| `mismatched input 'PARTITION'` w `ALTER TABLE` | partition evolution (`ADD PARTITION FIELD`) nie istnieje w Athenie | nowa tabela z innym `partitioning` + `INSERT INTO … SELECT` |
| `permission denied for database dev` w Redshifcie | tożsamość federacyjna bez uprawnień w bazie (np. inna rola SSO niż skonfigurowana w bazie) | zalogować się rolą posiadającą uprawnienia w bazie (np. `PowerUserAccess`) lub nadać `GRANT` jako administrator |
| `The current limit of 3 sessions has been reached` w QEv2 | za dużo otwartych zakładek z *Isolated session* | zamknąć zakładki albo wyłączyć *Isolated session* |
| `MERGE` w Redshifcie: błąd składni przy braku `WHEN NOT MATCHED` | Redshift wymaga **obu** gałęzi | dopisać `WHEN NOT MATCHED THEN INSERT (…) VALUES (…)` |

---

## 12. Iceberg — wersja zaawansowana

Wszystko niżej działa **tylko** na tabelach z `table_type = 'ICEBERG'` i zostało
sprawdzone w Athenie engine v3.

### Tabela od zera (bez CTAS) i partycjonowanie ukryte

```sql
CREATE TABLE IF NOT EXISTS moja_baza.dw_fakt (
  id          bigint,
  data_zdarz  date,
  kwota       double,
  load_ts     timestamp
)
PARTITIONED BY (day(data_zdarz))
LOCATION 's3://moj-bucket/dw/fakt/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'PARQUET');

-- ten sam efekt przez CTAS (tak robimy w ćwiczeniach):
CREATE TABLE IF NOT EXISTS moja_baza.dw_fakt
WITH (table_type = 'ICEBERG', format = 'PARQUET', is_external = false,
      location = 's3://moj-bucket/dw/fakt/',
      partitioning = ARRAY['day(data_zdarz)'])
AS SELECT … ;
```

Wiersze z nowego dnia tworzą partycję **same** — nie ma `MSCK REPAIR` ani
`ADD PARTITION`. `INSERT INTO … SELECT` dopisuje dane i tworzy snapshot.
`current_timestamp` ma strefę czasową, a kolumna `timestamp` Icebergu nie —
stąd `CAST(current_timestamp AS timestamp(6))` przy `load_ts`.

### MERGE — pełna składnia

```sql
MERGE INTO moja_baza.dw_wymiar t
USING (SELECT … FROM moja_baza.bronze_zmiany) s
  ON t.natural_key = s.natural_key
 AND t.is_current = true
WHEN MATCHED AND (t.miasto <> s.miasto)          -- warunek dodatkowy: co MA się zmienić
  THEN UPDATE SET valid_to = date_add('day', -1, s.valid_from), is_current = false
WHEN MATCHED AND s.usunac = true
  THEN DELETE
WHEN NOT MATCHED
  THEN INSERT (natural_key, miasto, valid_from, valid_to, is_current)
       VALUES (s.natural_key, s.miasto, s.valid_from, DATE '9999-12-31', true);
```

- każdy wiersz źródła trafia w **dokładnie jedną** gałąź `WHEN` — dlatego SCD2 to
  `MERGE` (zamknięcie starej wersji) **plus** `INSERT … WHERE NOT EXISTS` (nowa wersja)
- klucz złożony w `ON`: `ON t.order_id = s.order_id AND t.product_sk = s.product_sk`
- gałęzie są opcjonalne — `MERGE` z samym `WHEN MATCHED THEN UPDATE` jest legalny
- brak: `INSERT *`, `WHEN NOT MATCHED BY SOURCE`
- dwa wiersze źródła na jeden wiersz celu = błąd; w `USING` odfiltrować przez
  `ROW_NUMBER() OVER (PARTITION BY klucz ORDER BY ts DESC) = 1`

### UPDATE, DELETE, ewolucja schematu

```sql
UPDATE moja_baza.dw_fakt SET kwota = kwota * 1.23 WHERE data_zdarz = DATE '2026-01-20';
DELETE FROM moja_baza.dw_fakt WHERE id IN (SELECT id FROM moja_baza.do_usuniecia);

ALTER TABLE moja_baza.dw_fakt ADD COLUMNS (promo_code string);   -- 0 bajtów przepisanych
ALTER TABLE moja_baza.dw_fakt DROP COLUMN promo_code;            -- wg dokumentacji Atheny; w ćwiczeniach nieużywane
ALTER TABLE moja_baza.dw_fakt CHANGE COLUMN kwota kwota_pln double;  -- zmiana nazwy, j.w.
```

Iceberg mapuje kolumny po ID, nie po pozycji — stare pliki czyta się dalej,
nowe kolumny zwracają `NULL` dla starych wierszy. Partition evolution
(`ADD PARTITION FIELD`) w Athenie **nie działa**.

### Time travel

```sql
SELECT * FROM moja_baza.dw_fakt FOR VERSION AS OF 5713768167270614691;   -- snapshot_id z $snapshots
SELECT * FROM moja_baza.dw_fakt FOR TIMESTAMP AS OF TIMESTAMP '2026-09-03 08:30:00 UTC';

-- audyt: co zmieniło się od snapshotu X
SELECT COUNT(*) FROM moja_baza.dw_fakt f
JOIN moja_baza.dw_fakt FOR VERSION AS OF 5713768167270614691 o ON f.id = o.id
WHERE f.kwota <> o.kwota;
```

Po `AS OF` musi stać **literał** — podzapytanie daje `EXPRESSION_NOT_CONSTANT`.

### Tabele metadanych

```sql
SELECT * FROM "moja_baza"."dw_fakt$snapshots"  ORDER BY committed_at;  -- snapshot_id, operation, summary[...]
SELECT * FROM "moja_baza"."dw_fakt$history"    ORDER BY made_current_at;
SELECT * FROM "moja_baza"."dw_fakt$files";        -- pliki bieżącego snapshotu (file_path, record_count, file_size_in_bytes)
SELECT * FROM "moja_baza"."dw_fakt$partitions";   -- record_count, file_count, total_size per partycja
SELECT * FROM "moja_baza"."dw_fakt$manifests";
SELECT * FROM "moja_baza"."dw_fakt$refs";         -- gałęzie/tagi; w Athenie zawsze tylko 'main'
SELECT * FROM "moja_baza"."dw_fakt$properties";
```

Cudzysłowy wokół bazy i tabeli są obowiązkowe (`$` nie jest dozwolony w zwykłym
identyfikatorze). Przydatne klucze `summary`: `added-records`,
`added-position-deletes` (tak Athena zapisuje usunięcia — *merge-on-read*),
`total-records` (wiersze fizyczne, także skasowane logicznie). `$partitions`
i `$files` też liczą wiersze **fizyczne** — po `MERGE` bywają większe niż
`COUNT(*)`.

### OPTIMIZE i VACUUM

```sql
OPTIMIZE moja_baza.dw_fakt REWRITE DATA USING BIN_PACK;          -- sklej małe pliki; historia zostaje
OPTIMIZE moja_baza.dw_fakt REWRITE DATA USING BIN_PACK WHERE data_zdarz >= DATE '2026-01-20';

ALTER TABLE moja_baza.dw_fakt SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds' = '86400');
VACUUM moja_baza.dw_fakt;                                        -- kasuje stare snapshoty; AS OF pada
```

`OPTIMIZE` skleja małe pliki w większe; dane i snapshoty zostają — wejrzenie
wstecz nadal działa. `VACUUM` kasuje wygasłe snapshoty i pliki, do których żaden
snapshot już nie wskazuje; po nim `AS OF` starego snapu pada
(`Iceberg snapshot ID does not exists`). `OPTIMIZE` ≠ `VACUUM`.
`VACUUM` domyślnie zostawia snapshoty z 5 dni (`432000` s) i zawsze minimum
`vacuum_min_snapshots_to_keep` (1).

`DROP TABLE` na tabeli Iceberg (`is_external = false`) **kasuje też pliki na S3**
— inaczej niż przy tabelach zewnętrznych.

---

## 13. Redshift — wersja zaawansowana

### Rozkład i sortowanie jawnie

```sql
CREATE TABLE IF NOT EXISTS dw_xx.fact_przyklad (
  order_id       varchar(16),
  order_date_sk  int,
  customer_sk    int,
  kwota          double precision
)
DISTSTYLE KEY
DISTKEY (customer_sk)             -- wiersze o tym samym kluczu na tym samym węźle → złączenie bez przesyłania
SORTKEY (order_date_sk);          -- fizyczna kolejność → filtr po dacie pomija bloki

CREATE TABLE IF NOT EXISTS dw_xx.dim_przyklad (…) DISTSTYLE ALL;   -- mały wymiar: kopia na każdym węźle
```

`DISTSTYLE AUTO` (domyślne) zaczyna od `ALL` dla małych tabel i przechodzi na
`EVEN`/`KEY` w miarę wzrostu — to, co Redshift wybrał, widać w
`SVV_TABLE_INFO` (`diststyle`, `sortkey1`).

### Ładowanie z Glue bez COPY

```sql
INSERT INTO dw_xx.dim_przyklad
SELECT … FROM "awsdatacatalog"."aurora_xx"."dw_dim_przyklad";     -- czyta Iceberg/Parquet przez Spectrum
```

Alternatywa dla `COPY`, gdy rola IAM workgroupu nie czyta bucketu — dane idą
tożsamością zalogowanego użytkownika.

### Plan zapytania i historia

```sql
EXPLAIN SELECT … ;                                    -- DS_DIST_NONE = bez przesyłania; DS_BCAST_INNER = rozgłaszanie; S3 Seq Scan = Spectrum

SELECT query_id, status, elapsed_time / 1000 AS ms, returned_rows, query_text
FROM SYS_QUERY_HISTORY
WHERE user_id = current_user_id AND query_type = 'SELECT'
ORDER BY start_time DESC LIMIT 10;

SELECT query_id, source_type, scanned_files, returned_rows, returned_bytes
FROM SYS_EXTERNAL_QUERY_DETAIL ORDER BY start_time DESC LIMIT 10;   -- tylko zapytania Spectrum

SELECT start_time, compute_capacity, charged_seconds
FROM SYS_SERVERLESS_USAGE ORDER BY start_time DESC LIMIT 10;         -- RPU × sekundy = rachunek
```

`SVL_QUERY_SUMMARY` / `STL_*` to widoki z klasycznego Redshifta — w Serverless
są puste; używa się `SYS_*`.

### MERGE

```sql
MERGE INTO dw_xx.dim_przyklad
USING "awsdatacatalog"."aurora_xx"."dw_dim_przyklad" s
  ON dw_xx.dim_przyklad.klucz_sk = s.klucz_sk
WHEN MATCHED THEN UPDATE SET nazwa = s.nazwa, is_current = s.is_current
WHEN NOT MATCHED THEN INSERT (klucz_sk, nazwa, is_current)
     VALUES (s.klucz_sk, s.nazwa, s.is_current);
```

Dwie różnice wobec Atheny: **obie** gałęzie są wymagane, a `WHEN MATCHED` nie
przyjmuje dodatkowego `AND (…)` — filtr trafia do `USING (SELECT … WHERE …)`.

### UNLOAD — z powrotem do data lake

```sql
UNLOAD ('SELECT * FROM dw_xx.fact_przyklad')
TO 's3://moj-bucket/export/fact_przyklad/'
IAM_ROLE default
FORMAT AS PARQUET
ALLOWOVERWRITE
PARALLEL OFF;
```

`PARALLEL OFF` = jeden plik (łatwiej podpiąć w Athenie), domyślnie jeden plik
na slice. `IAM_ROLE default` to rola namespace'u — czy widzi dany bucket, zależy
od uprawnień podpiętych do roli workgroupu (gdy rola ma uprawnienia S3 do
`aurora-<inicjaly>-lake`, `UNLOAD` działa od razu; domyślna rola konsolowa widzi
tylko buckety z „redshift” w nazwie).

### Widok zmaterializowany

```sql
CREATE MATERIALIZED VIEW dw_xx.mv_przyklad
AUTO REFRESH NO AS
SELECT order_date_sk, SUM(kwota) AS kwota FROM dw_xx.fact_przyklad GROUP BY 1;

REFRESH MATERIALIZED VIEW dw_xx.mv_przyklad;
```

Bez `IF NOT EXISTS` (Redshift go tu nie ma) — drugi `CREATE` zwraca
`already exists`. Po `MERGE` do tabeli bazowej MV jest nieaktualny do `REFRESH`.

---

## 14. SQL analityczny na poziomie data warehouse

### GROUPING SETS / ROLLUP / CUBE

```sql
SELECT COALESCE(CAST(dzien AS varchar), 'razem') AS dzien,
       COALESCE(kanal, 'razem')                 AS kanal,
       SUM(kwota)                               AS kwota
FROM …
GROUP BY ROLLUP (dzien, kanal)                 -- (dzien, kanal), (dzien), ()
ORDER BY GROUPING(dzien), dzien, GROUPING(kanal), kanal;

GROUP BY CUBE (dzien, kanal)                   -- wszystkie kombinacje, także (kanal)
GROUP BY GROUPING SETS ((kanal), (kategoria))  -- dwa niezależne podsumowania w jednym wyniku
```

`GROUPING(kol)` = 1 dla wiersza sumy — daje deterministyczny `ORDER BY`
(sumy zawsze na końcu) i pozwala odróżnić prawdziwy `NULL` od „razem”.
Ta sama składnia w Athenie i w Redshifcie.

### Funkcje okna

```sql
ROW_NUMBER() OVER (PARTITION BY klient ORDER BY ts DESC)              -- najnowszy wiersz per klient (= 1)
SUM(kwota)   OVER (PARTITION BY klient ORDER BY dzien ROWS UNBOUNDED PRECEDING)  -- suma narastająca
LAG(kwota)   OVER (PARTITION BY klient ORDER BY dzien)                -- poprzednia wartość
SUM(kwota)   OVER ()                                                  -- suma całości → udział procentowy
```

### Asercje jakości danych jako SQL

```sql
WITH checks AS (
  SELECT 'grain unique' AS test, 0 AS expected,
         (SELECT COUNT(*) FROM (SELECT order_id, product_sk FROM f GROUP BY 1, 2 HAVING COUNT(*) > 1)) AS actual
  UNION ALL
  SELECT 'no unknown customer', 0, (SELECT COUNT(*) FROM f WHERE customer_sk = -1)
  UNION ALL
  SELECT 'fact = silver', (SELECT COUNT(*) FROM silver), (SELECT COUNT(*) FROM f)
)
SELECT test, expected, actual, CASE WHEN expected = actual THEN 'OK' ELSE 'FAIL' END AS status
FROM checks ORDER BY test;
```

Wiersz `-1` w każdym wymiarze (*unknown member*) + `COALESCE(sk, -1)` w fakcie =
brak `NULL`-i w kluczach, a asercja `= -1` wyłapuje sieroty.
