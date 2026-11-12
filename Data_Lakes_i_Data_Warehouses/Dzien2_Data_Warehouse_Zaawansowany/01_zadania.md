# Dzień 2 — Data Warehouse, wersja zaawansowana (zadania)

**Czas:** 3 godziny  
**Format:** praca w konsoli z weryfikacją na liczbach kontrolnych  
**Gdzie:** AWS Console · region **eu-central-1** · Athena (Z0–Z5) + Redshift Serverless (Z6)  
**Wejście:** stan po dniu 1 — baza `aurora_<inicjaly>`, bucket `aurora-<inicjaly>-lake`, Bronze + Silver  
**Prefix:** ten sam `aurora_<inicjaly>`; nowe tabele nazywają się `dw_*`, w Redshifcie schemat `dw_<inicjaly>`

Ta wersja **zastępuje** podstawową `../Dzien2_Data_Warehouse/`, nie dokłada się do
niej. Zakłada, że SQL i idea Bronze/Silver/Gold są już oswojone, i idzie dalej:
model wymiarowy Kimballa (wymiar daty używany w kilku rolach, junk dimension,
accumulating snapshot, agregat), warstwa Gold od razu jako **Apache Iceberg**,
Redshift z jawnym rozkładem danych i planem zapytania. Jutro (wersja zaawansowana
dnia 3) SCD2 i przyrost idą na tych samych tabelach przez `MERGE`.

Składnia jest zebrana w `../00_Sciagawka_SQL.md` (sekcje *Iceberg*, *GROUPING SETS*,
*Redshift — DISTKEY/SORTKEY/EXPLAIN/MERGE/UNLOAD*). Warto mieć ją otwartą obok.

### Jak czytać materiały — bez skakania między trzema plikami

- **Ten plik** mówi, **co** zrobić: które obiekty, jaki grain, skąd dane, jaka
  kontrola. Nie ma tu pełnego SQL do skopiowania — najpierw własna próba.
- **`02_rozwiazania.md`** to jedna ścieżka czytania „jak”: zdanie, co powstało
  i co znaczy wynik, **pełny SQL** w bloku, liczby kontrolne z etykietą.
- **`sql/Z0_stan_wejsciowy.sql`**, `sql/Z1_wymiar_daty.sql`, … — ten sam SQL co
  w rozwiązaniach, jeden plik = jedno ćwiczenie, do otwarcia w edytorze Atheny
  albo Query Editor v2. Nazwa zaczyna się od identyfikatora ćwiczenia.

---

## Plan na 3 godziny

| Blok | Treść | min | Razem |
|---|---|---|---|
| Z0 | Upload wysyłek, stan wejściowy, Iceberg | 15 | 15 |
| Z1 | Kimball: grain i bus matrix (gotowa tabela); `dw_dim_date` jako pełny kalendarz | 25 | 40 |
| Z2 | Wymiary: customer (pod SCD2), product (price_band), junk dimension status × channel, wiersze -1 | 20 | 60 |
| Z3 | Fakty: transakcyjny, accumulating snapshot, agregat | 30 | 90 |
| Z4 | Asercje jakości danych jako jedno zapytanie | 15 | 105 |
| — | przerwa | 10 | 115 |
| Z5 | SQL analityczny: ROLLUP, GROUPING SETS, okna, kohorty, AOV | 25 | 140 |
| Z6 | Redshift: Spectrum nad Icebergiem, DISTKEY/SORTKEY, INSERT…SELECT, EXPLAIN | 30 | 170 |
| Z7 | Podsumowanie, co zostaje na jutro | 10 | 180 |

Z0–Z4 i Z6a–Z6c są potrzebne jutro i zostają w planie.

## Po trzech godzinach (jeśli zostanie czas)

| Blok | Treść |
|---|---|
| Z5f | CUBE i suma biegnąca |
| Z6e | widoki `SYS_*` |
| Z6f | materialized view |
| Z6g | `UNLOAD` z powrotem do data lake |

---

## Z0. Upload wysyłek, stan wejściowy i Iceberg (15 min)

**Dlaczego:** wszystko, co dzisiaj powstanie, czyta Silver z dnia 1. Jedna
kontrola oszczędza godzinę szukania błędu we własnej tabeli. Dodatkowo wersja
zaawansowana dokłada **wysyłki** — bez tego pliku w *własnym* buckecie Z3 nie
ma źródła na accumulating snapshot.

**Konsola:** AWS Management Console → rola **PowerUserAccess** → region
**eu-central-1**.

### Upload — tylko nowy prefiks, tylko własny bucket

Katalogi `bronze/`, `silver/` w buckecie `aurora-<inicjaly>-lake` **już istnieją**
z dnia 1. Nie wgrywamy ponownie klientów ani zamówień. Nie ruszamy cudzego
prefiksu `aurora-<czyjeś_inicjaly>-`.

W tym folderze dnia 2 jest jeden nowy plik, poukładany tak, jak ma leżeć na S3:

```text
dane/bronze/
  shipments/shipments.csv    66 wysyłek zamówień shipped z 15–16.01
                             (45 z delivered_ts, 21 w drodze — puste delivered_ts)
```

**Do zrobienia:** S3 → własny bucket `aurora-<inicjaly>-lake` → katalog `bronze/`
(ten z dnia 1) → *Upload* → *Add folder* → wskazujemy lokalny katalog
`dane/bronze/shipments`. Konsola odwzorowuje 1:1: plik ląduje pod
`bronze/shipments/shipments.csv`.

Kontrola w S3: pod własnym `bronze/` widać dotychczasowe katalogi z dnia 1
**oraz** nowy `shipments/`.

Tabela `bronze_shipments` powstanie w Z3; dziś wystarczy, że plik leży we
właściwym prefiksie.

### Stan Silver

Athena → *Query editor*. Baza `aurora_<inicjaly>` (filtr `aurora` skraca listę).
*Reuse query results* wyłączone — inaczej część liczb będzie pochodzić z cache.

**Do zrobienia:** `SELECT` z `UNION ALL` czterech `COUNT(*)` na
`silver_orders`, `silver_order_items`, `silver_customers`, `silver_products`,
z `ORDER BY` po etykiecie tabeli. Kształt jak w dniu 1 przy kontroli Silver.

Oczekiwane (kolejność alfabetyczna etykiet): `customers 80 | order_items 409 |
orders 170 | products 25`.

`409` to pozycje zamówienia w Silver **po odfiltrowaniu anulowanych** — w Bronze
pozycji jest 486. Ten sam JOIN co w L5 dnia 1; bez tego filtra dzisiejszy fakt
nie zejdzie się z 1 427 124 zł.

Paste 1:1: `sql/Z0_stan_wejsciowy.sql`.

### Iceberg w pięć minut (dla tych, którzy nie robili S1 w dniu 1)

Tabela Iceberg to katalog na S3 z dwoma podkatalogami — `data/` (Parquet) i
`metadata/` (JSON + Avro). Metadane mówią, *które* pliki składają się na aktualny
stan tabeli. Zapis = nowe pliki + nowy wpis w metadanych (snapshot). Stąd:
`MERGE`, `UPDATE`, `DELETE`, time travel i `DROP TABLE`, który kasuje także dane.

Dla Atheny różnica w składni CTAS to trzy parametry w `WITH (...)`:
`table_type = 'ICEBERG'`, `location = ...` (nie `external_location`),
`is_external = false`. Wzorzec jest w ściągawce SQL.

**Na co patrzeć:** cztery liczby z dnia 1 to od dzisiaj *liczby kontrolne* —
każda tabela Gold ma się do nich rekoncyliować.

---

## Z1. Kimball i wymiar daty (25 min)

**Dlaczego:** model wymiarowy to zestaw *decyzji*, nie tabel. Zanim powstanie
pierwszy CTAS, decyzje są spisane. Na zajęciach online (3h, bez tablicy) bus
matrix Aurory jest już wypełniona poniżej — omawiamy gotową tabelę.

### Z1a. Decyzje architektoniczne (12 min)

| Decyzja | Aurora |
|---|---|
| **Grain** faktu głównego | jeden wiersz to jedna pozycja zamówienia (`order_id` + produkt) |
| **Wymiary konformowane** | `dw_dim_date`, `dw_dim_customer`, `dw_dim_product` — te same tabele dla każdego faktu |
| **Wymiar daty w kilku rolach** | jedna fizyczna `dw_dim_date`; trzy obce klucze: data zamówienia, wysyłki i dostawy (role-playing dimension) |
| **Degenerate dimension** | `order_id` zostaje w fakcie, bo nie ma własnych atrybutów na osobną tabelę |
| **Junk dimension** | `dw_dim_order_status` zbiera status × kanał (9 kombinacji) do jednej tabeli |
| **Wiersz nieznany** | każdy wymiar ma `*_sk = -1`; fakt używa `LEFT JOIN` + `COALESCE(..., -1)`, żeby żaden wiersz nie znikał na JOIN |
| **Fakty** | transakcyjny (`dw_fact_order_item`), accumulating snapshot (`dw_fact_order_fulfillment`), agregat (`dw_fact_daily_sales`) |
| **Addytywność** | `qty` i `net_amount_pln` wolno sumować przez dowolny wymiar. `orders_n` (COUNT DISTINCT) nie sumuje się dalej, bo jedno zamówienie ma pozycje w kilku kategoriach |

Bus matrix — **gotowa tabela**, ten sam układ, który w podręczniku Kimballa
jest kratką procesów i wymiarów. Wiersz to proces (fakt), kolumna to wymiar.
Krzyżyk albo słowo w komórce znaczy: ten fakt złącza się z tym wymiarem.
To jest dokument projektowy, nie tabela, którą ktoś robi `CREATE TABLE`.
Sens słowa *konformowany* widać od razu: te same wymiary obsługują więcej niż
jeden fakt.

| Proces (fakt) | `dw_dim_date` | `dw_dim_customer` | `dw_dim_product` | `dw_dim_order_status` | `order_id` w fakcie |
|---|---|---|---|---|---|
| Sprzedaż — `dw_fact_order_item` (grain: pozycja) | data zamówienia | tak | tak | status × kanał | degenerate dimension |
| Realizacja — `dw_fact_order_fulfillment` (grain: zamówienie) | data zamówienia, wysyłki i dostawy (trzy role, jedna tabela) | tak | — | status × kanał | tak |
| Agregat — `dw_fact_daily_sales` (grain: dzień × kategoria × kanał) | data zamówienia | — | kategoria z wymiaru produktu | kanał z junk dimension | — |

Sprzedaż ma grain pozycji, więc bez produktu i klienta nie ma wiersza, a data
jest jedna — data zamówienia. Realizacja ma grain zamówienia: produkt nie jest
potrzebny, za to data występuje w trzech rolach (zamówienie, wysyłka, dostawa)
jako trzy obce klucze do tej samej `dw_dim_date`. Agregat jest już po zgrupowaniu,
więc nie ma klienta ani `order_id` — pytanie „który klient” nie ma na tym fakcie
kolumny. Wiersza nieznanego `-1` ta kratka nie pokazuje; on jest w tabeli decyzji
powyżej.

Te cztery decyzje — wiersz `-1`, 409 kontra 486, trzy daty do jednej
`dw_dim_date`, `orders_n` które się nie sumuje — wracają w dniu 3 przy ładowaniu przyrostowym (Y2).

### Z1b. `dw_dim_date` — pełny kalendarz 2026 jako Iceberg (13 min)

**Dlaczego:** wymiar daty generowany z faktu (w wersji podstawowej: 2 wiersze)
trzeba doklejać przy każdym nowym dniu. Kalendarz z góry na cały rok kończy ten
problem, zanim się zacznie.

**Konsola:** Athena → *Query editor*, nowa zakładka. Paste po własnej próbie:
`sql/Z1_wymiar_daty.sql`.

**Do zbudowania:** tabela Iceberg `dw_dim_date`.

- **Grain:** jeden wiersz to jeden dzień kalendarzowy 2026; dodatkowo wiersz
  nieznany `date_sk = -1` (data `1900-01-01`).
- **Źródło:** nie Silver. Generator: `UNNEST(sequence(DATE '2026-01-01', DATE '2026-12-31', INTERVAL '1' DAY))`.
- **Kształt SQL:** CTAS Iceberg z `table_type = 'ICEBERG'`, `location` (nie
  `external_location`) pod `s3://aurora-<inicjaly>-lake/dw/dim_date/`,
  `is_external = false`. `date_sk` w konwencji RRRRMMDD. Kolumna `date_actual`
  jako `date` — `sequence()` z interwałem dniowym zwraca timestampy, więc
  potrzebny jest `CAST(d AS date)`.
- Potem `INSERT` wiersza `-1` z `WHERE NOT EXISTS`, żeby drugie uruchomienie
  nic nie zdublowało.

**Kontrola:** `COUNT(*)` dni, liczba weekendów (`is_weekend`), `MIN(date_sk)`,
`MAX(date_sk)`. Oczekiwane: **366** wierszy (365 dni roku przestępnego 2026 +
wiersz nieznany), **104** weekendy, pierwszy klucz **-1**, ostatni **20261231**.

**Na co patrzeć:** po CTAS-ie w S3 pod `dw/dim_date/` widać `data/` oraz
`metadata/`. Bez rzutowania na `date` każda data w raporcie dostaje
`00:00:00.000000` (sprawdzone w Athenie v3).

---

## Z2. Wymiary: customer, product, junk dimension (20 min)

**Dlaczego:** trzy różne rodzaje wymiarów w jednym bloku — zwykły z historią
(customer), zwykły z atrybutem pochodnym (product) i junk dimension, który
skleja dwa niskokardynalne atrybuty zamówienia.

**Konsola:** ta sama zakładka Atheny. Paste: `sql/Z2_wymiary.sql`.

**Do zbudowania** — trzy tabele Iceberg, każda CTAS z `table_type = 'ICEBERG'`,
`location` pod `s3://…/dw/…`, `is_external = false`, plus `INSERT` wiersza `-1`
(`WHERE NOT EXISTS`) tam, gdzie wymiar ma wiersz nieznany.

### `dw_dim_customer`

- **Grain:** jeden wiersz to jeden klient w aktualnym stanie (`is_current`),
  z surrogate key `customer_sk`.
- **Źródło:** `silver_customers` (80 klientów).
- **Kształt:** `ROW_NUMBER() OVER (ORDER BY customer_id)` jako `customer_sk`;
  kolumny SCD2 od razu: `valid_from = 1900-01-01`, `valid_to = 9999-12-31`,
  `is_current = true`; `signup_month` jako atrybut pochodny pod kohorty.
- Wiersz `-1` z `customer_id = 'UNKNOWN'`.

### `dw_dim_product`

- **Grain:** jeden wiersz to jeden produkt (plus wiersz nieznany).
- **Źródło:** `silver_products` (25 produktów).
- **Kształt:** `product_sk` z `ROW_NUMBER()`; `list_price_pln` ze źródłowej ceny
  katalogowej; `price_band` z `CASE` (`budget` / `mid` / `premium`).
- Wiersz `-1`.

### `dw_dim_order_status` (junk dimension)

- **Grain:** jeden wiersz to jedna para status × kanał. **Bez** wiersza `-1` —
  komplet kombinacji z dziedziny wystarcza; fakt i tak ma `COALESCE` do `-1`,
  gdyby pary zabrakło.
- **Źródło:** `bronze_orders` (nie Silver — Silver odciął `cancelled`).
- **Kształt:** `CROSS JOIN` dwóch `SELECT DISTINCT` (status × kanał), flaga
  `is_revenue` dla `paid` i `shipped`. `CROSS JOIN` jest po to, żeby powstał
  iloczyn kartezjański dziedziny, nie tylko pary widziane dziś w sprzedaży.

**Kontrola:** `UNION ALL` czterech `COUNT(*)` z `ORDER BY` etykiety.
Oczekiwane: `dw_dim_customer` **81** (80 + wiersz nieznany) · `dw_dim_date`
**366** · `dw_dim_order_status` **9** (3 statusy × 3 kanały) · `dw_dim_product`
**26** (25 + wiersz nieznany).

Drugie uruchomienie całego pliku niczego nie zmienia (`IF NOT EXISTS` +
`WHERE NOT EXISTS`).

---

## Z3. Fakty: trzy typy z jednego Silvera (30 min)

**Konsola:** Athena. Paste: `sql/Z3_fakty.sql`. Plik wysyłek ma już leżeć w
S3 po Z0.

### Z3a. `bronze_shipments` (5 min)

**Do zbudowania:** tabela zewnętrzna `bronze_shipments` nad prefiksem
`s3://aurora-<inicjaly>-lake/bronze/shipments/`.

- **Grain:** jeden wiersz to jedna wysyłka.
- **Źródło:** CSV wgrany w Z0. DDL jak w L3: `OpenCSVSerde`, kolumny jako
  `string`, `skip.header.line.count = 1`, `LOCATION` wskazuje **katalog**, nie plik.

**Kontrola:** liczba wysyłek, ile ma niepusty `delivered_ts`, ile pusty string.
Oczekiwane: **66** wysyłek · **45** dostarczonych · **21** w drodze.
Porównanie z `''`, nie z `NULL` — OpenCSVSerde tak czyta puste pole CSV.

### Z3b. `dw_fact_order_item` — fakt transakcyjny (10 min)

**Do zbudowania:** tabela Iceberg `dw_fact_order_item`.

- **Grain:** jeden wiersz to jedna pozycja zamówienia (ten sam grain co Silver).
- **Źródło:** `silver_order_items` JOIN `silver_orders`, potem `LEFT JOIN` do
  czterech wymiarów.
- **Kształt:** CTAS Iceberg, `partitioning = ARRAY['day(order_date)']` (hidden
  partitioning — kolumna `order_date` zostaje zwykłą datą). `order_id` zostaje
  w fakcie jako degenerate dimension. Klucze wymiarów przez `LEFT JOIN` +
  `COALESCE(..., -1)`, nie przez `INNER JOIN`. Klient: `customer_id` **oraz**
  `order_date BETWEEN valid_from AND valid_to`. Kolumna `load_ts` (timestamp
  bez strefy: `CAST(current_timestamp AS timestamp(6))`).

**Kontrola:** liczba wierszy, suma `net_amount_pln`, ile kluczy `customer_sk` /
`product_sk` / `order_status_sk` równa się `-1`.
Oczekiwane: **409** pozycji (nie 486 — cancelled odpadły w Silver) ·
**1 427 124,00** zł · zero wierszy z kluczem nieznanym.

### Z3c. `dw_fact_order_fulfillment` — accumulating snapshot (10 min)

**Do zbudowania:** tabela Iceberg `dw_fact_order_fulfillment`.

- **Grain:** jeden wiersz to jedno zamówienie; wiersz *żyje* — jutro `MERGE`
  dopisze dostawy, które dziś są w drodze.
- **Źródło:** `silver_orders`, agregat pozycji (`SUM` / `COUNT` po `order_id`),
  `LEFT JOIN` do `bronze_shipments` i do wymiarów. Trzy złączenia do
  `dw_dim_date` pod trzema aliasami: data zamówienia, wysyłki, dostawy
  (wymiar daty w kilku rolach).
- **Kształt:** `NULLIF(delivered_ts, '')` zanim rzutowanie na timestamp;
  `COALESCE(date_sk, -1)` na każdej roli daty; `carrier` z `COALESCE(..., 'n/a')`;
  `date_diff('day', ...)` rzutowane na `integer`.

**Kontrola:** liczba zamówień, ile ma `ship_date_sk <> -1`, ile
`delivery_date_sk <> -1`, ile bez wysyłki, suma `order_amount_pln`.
Oczekiwane: **170** zamówień · **66** wysłanych · **45** dostarczonych ·
**104** bez wysyłki · **1 427 124,00** zł (ta sama kwota co na pozycjach).

Drugie zapytanie: pierwsze pięć zamówień z trzema datami z wymiaru (aliasy
`od` / `sd` / `dd`), `ORDER BY order_id`. `O1002`–`O1005` mają `1900-01-01`
w rolach wysyłki i dostawy — to wiersz `-1`, nie zgubiony JOIN.

### Z3d. `dw_fact_daily_sales` — agregat (8 min)

**Do zbudowania:** tabela Iceberg `dw_fact_daily_sales`.

- **Grain:** jeden wiersz to dzień × kategoria × kanał.
- **Źródło:** `dw_fact_order_item` złąccony z `dw_dim_product` i
  `dw_dim_order_status`, `GROUP BY` trzech kluczy.
- **Kształt:** miary `lines_n`, `orders_n` (`COUNT DISTINCT order_id`), `qty`,
  `revenue_pln`. Agregat nadal jest star schema — kluczami są klucze wymiarów.

**Kontrola:** liczba wierszy i suma przychodu. Oczekiwane: **36** wierszy
(2 dni × 6 kategorii × 3 kanały) · **1 427 124,00** zł. Suma `orders_n` po
kategoriach *nie* da liczby zamówień: jedno zamówienie ma pozycje z kilku
kategorii.

---

## Z4. Asercje jakości danych jako SQL (15 min)

**Dlaczego:** rekoncyliacja „na oko” nie skaluje się. Jedno zapytanie, które
zwraca tabelę testów ze statusem, to to samo, co robią dbt tests albo Great
Expectations — tylko bez narzędzia.

**Konsola:** Athena. Paste: `sql/Z4_asercje.sql`.

**Do zbudowania:** jeden `WITH checks AS (` … `UNION ALL` ośmiu testów `)`,
na zewnątrz `CASE WHEN expected = actual THEN 'OK' ELSE 'FAIL' END`,
`ORDER BY test`.

Osiem asercji, słowami:

| Id | Co porównujemy | Oczekiwane |
|---|---|---|
| A1 | liczba wierszy faktu transakcyjnego = Silver pozycji | 409 = 409 |
| A2 | przychód faktu = przychód Silver (po `ROUND` na `bigint`) | 1 427 124 |
| A3 | zero wierszy z `customer_sk = -1` | 0 |
| A4 | zero osieroconych `product_sk` (LEFT JOIN do wymiaru, `IS NULL`) | 0 |
| A5 | grain unikalny: zero par `(order_id, product_sk)` z `HAVING COUNT(*) > 1` | 0 |
| A6 | zero ujemnych `net_amount_pln` | 0 |
| A7 | przychód agregatu = przychód faktu | 1 427 124 |
| A8 | liczba wierszy fulfillment = Silver zamówień | 170 = 170 |

**Kontrola:** 8 wierszy, każdy `OK`. Pełny wynik z etykietami jest w
`02_rozwiazania.md`.

Dodatkowo metadane Icebergu: `SELECT partition, record_count, file_count FROM
"aurora_<inicjaly>"."dw_fact_order_item$partitions" ORDER BY partition`.
Oczekiwane: partycja 15.01 — **186** pozycji w 1 pliku; 16.01 — **223** w 1
pliku (186 + 223 = 409). Tabele `$partitions` / `$snapshots` / `$files` czyta
się w cudzysłowach, bo `$` nie jest dozwolony w zwykłym identyfikatorze.

---

## Z5. SQL analityczny na star schema (25 min)

**Dlaczego:** star schema istnieje po to, żeby pytania biznesowe były jednym
`SELECT`-em. Pięć wzorców, które wracają w każdym data warehouse.

**Konsola:** Athena. Paste: `sql/Z5_analityka.sql`.

Każde zapytanie ma `ORDER BY`, żeby wynik dało się porównać linia po linii.

### Z5a. ROLLUP — sumy częściowe (7 min)

**Pytanie:** przychód z agregatu `dw_fact_daily_sales` po dniu i kanale, z
sumami częściowymi dnia i sumą całkowitą.

**Kształt:** `GROUP BY ROLLUP (data, kanał)`, `GROUPING()` do etykiety `razem`
zamiast `NULL`. Źródło: agregat + `dw_dim_date`.

**Kontrola:** 9 wierszy. Suma 15.01 **653 531** · suma 16.01 **773 593** ·
razem **1 427 124**. Kwoty per kanał są w rozwiązaniach.

### Z5b. GROUPING SETS — dwa raporty z jednego skanu (5 min)

**Pytanie:** przychód per kategoria **oraz** per kanał, bez dwóch osobnych
zapytań.

**Kształt:** `GROUP BY GROUPING SETS ((category), (channel))`. To nie hierarchia
(ROLLUP), tylko dwa niezależne cięcia jednego skanu — stąd `GROUPING SETS`,
a nie `UNION ALL` dwóch `GROUP BY`.

**Kontrola:** 9 wierszy (6 kategorii + 3 kanały). Kanały: marketplace **521 592**
· mobile **452 527** · web **453 005**. Kategorie od Akcesoria **13 423** do
Telefony **381 977**.

### Z5c. Okna — udział i ranking w grupie (6 min)

**Pytanie:** dwa produkty o najwyższym przychodzie w każdej kategorii, z
udziałem procentowym w kategorii.

**Kształt:** `SUM() OVER (PARTITION BY category)` i `RANK() OVER (...)`; filtr
`rnk <= 2` w zewnętrznym `SELECT` (Trino nie ma `QUALIFY`). Źródło: fakt
transakcyjny + `dw_dim_product`.

**Kontrola:** 12 wierszy. Pierwszy: AGD · 1 · Lodówka Aurora 300l · **102 743** ·
**39,36 %**.

### Z5d. Kohorty po miesiącu rejestracji (6 min)

**Pytanie:** ilu klientów z danego `signup_month` kupiło i za ile.

**Kształt:** `GROUP BY signup_month` na wymiarze; `LEFT JOIN` do faktu, żeby
kohorta bez zakupów nie zniknęła. `WHERE customer_sk <> -1`.

**Kontrola:** 13 wierszy (miesiące 2025-01 … 2026-01). Suma kolumny klienci =
**80**, suma zamówień = **170**, suma przychodu = **1 427 124**.

### Z5e. AOV vs średnia pozycji (6 min)

**Pytanie:** średnia wartość zamówienia kontra średnia pozycja na tym samym
fakcie.

**Kształt:** `SUM / COUNT DISTINCT order_id` versus `AVG(net_amount_pln)`.
Różnica to grain, nie błąd arytmetyczny.

**Kontrola:** przychód **1 427 124,00** · **170** zamówień · **409** pozycji ·
AOV **8 394,85** · średnia pozycji **3 489,30**.

### Z5f. `[po 3 h]` CUBE i suma biegnąca

W `sql/Z5_analityka.sql`, sekcja Z5f: `GROUP BY CUBE` (12 wierszy) i
`SUM() OVER (... ROWS UNBOUNDED PRECEDING)` (6 wierszy). Do pokazania, jeśli
Z1–Z5e zamknęły się przed 150. minutą.

---

## Z6. Redshift Serverless (30 min)

**Dlaczego:** ten sam star schema na silniku data warehouse. Trzy rzeczy, których
Athena nie ma: rozkład danych między węzły (`DISTKEY`/`SORTKEY`), plan zapytania
(`EXPLAIN`) i utrzymywany przez silnik agregat (materialized view). Plus
Spectrum, które czyta Iceberg z Glue bez kopiowania.

**Konsola:** (zalogowani rolą z uprawnieniami w bazie, np. **PowerUserAccess**) Redshift → *Serverless dashboard* →
workgroup → *Query data* (Query Editor v2). Połączenie:
**Federated user**, baza `dev`. Bez Federated user nie ma `awsdatacatalog`.
Gdy Redshift Data API jest zablokowane przez SCP organizacji — wszystko idzie z QEv2.
QEv2 pozwala na 3 sesje naraz (*Isolated session*) — jedna zakładka na osobę
wystarcza. Paste: `sql/Z6_redshift.sql`.

Nazwy trzyczłonowe: `"awsdatacatalog"."aurora_<inicjaly>"."dw_fact_order_item"`.
Zapis Athenowy `aurora_<inicjaly>.dw_…` w Redshifcie kończy się
`schema "aurora_…" does not exist`.

### Z6a. Spectrum czyta Iceberg (5 min)

**Do zrobienia:** `COUNT(*)` i suma `net_amount_pln` na fakcie Iceberg przez
Spectrum (trzy człony, bez `COPY`).

**Kontrola:** **409** wierszy · **1 427 124,00** zł — te same liczby co w Athenie.
`COPY` z katalogu `data/` tabeli Iceberg byłby błędem koncepcyjnym: `COPY` nie
zna snapshotów.

### Z6b–Z6c. Schemat lokalny, DISTKEY/SORTKEY, ładowanie (15 min)

**Do zbudowania:**

1. `CREATE SCHEMA IF NOT EXISTS dw_<inicjaly>`.
2. Cztery wymiary `DISTSTYLE ALL SORTKEY (*_sk)` i fakt
   `DISTKEY (customer_sk) SORTKEY (order_date_sk)`. `PRIMARY KEY` / `REFERENCES`
   są informacyjne — silnik ich nie egzekwuje.
3. `INSERT … SELECT` z `"awsdatacatalog"."aurora_<inicjaly>"."dw_…"` z
   `WHERE NOT EXISTS (SELECT 1 FROM cel)` — drugie uruchomienie nic nie dokłada.

**Kontrola:** `UNION ALL` pięciu `COUNT(*)` z `ORDER BY`. Oczekiwane:
`dim_customer` **81** · `dim_date` **366** · `dim_order_status` **9** ·
`dim_product` **26** · `fact_order_item` **409**.

### Z6d. EXPLAIN (8 min)

**Do zrobienia:** dwa `EXPLAIN` tego samego pytania (przychód per kategoria
15 stycznia) — raz na tabelach lokalnych, raz na Spectrum. Szukamy w planie
`DS_DIST_ALL_NONE` (wymiar `ALL`, zero ruchu sieciowego) kontra skan `S3` /
`format:ICEBERG`. Dokładne plany z konsoli AWS są w `02_rozwiazania.md`.

### Z6e. `[po 3 h]` Historia zapytań: `SYS_QUERY_HISTORY`, `SYS_EXTERNAL_QUERY_DETAIL`

W `sql/Z6_redshift.sql`, sekcja Z6e. `elapsed_time` jest w mikrosekundach.

### Z6f. `[po 3 h]` Materialized view (6 min)

**Do zbudowania:** `CREATE MATERIALIZED VIEW dw_<inicjaly>.mv_daily_sales
AUTO REFRESH NO` — ten sam grain co `dw_fact_daily_sales`. Redshift nie zna
`IF NOT EXISTS` dla MV; drugie uruchomienie kończy się `relation already exists`.

**Kontrola:** **36** wierszy · **1 427 124,00** zł.

### Z6g. `[po 3 h]` UNLOAD — z powrotem do data lake (6 min)

**Do zrobienia:** `UNLOAD` wyniku MV do `s3://aurora-<inicjaly>-lake/export/daily_sales/`
(`IAM_ROLE default`, Parquet, `PARALLEL OFF`, `ALLOWOVERWRITE`). Potem w
**Athenie** (`sql/Z6g_athena_po_unload.sql`) tabela zewnętrzna nad tym prefiksem
i porównanie sumy z `dw_fact_daily_sales`.

**Kontrola:** oba źródła **36** wierszy · **1 427 124,00** zł — pełne koło
S3 → Athena → Redshift → S3 → Athena.

---

## Z7. Podsumowanie i sprzątanie (10 min)

Co zostaje na jutro (wersja zaawansowana dnia 3): `MERGE` do `dw_dim_customer`
(SCD2), przyrost 20 stycznia do `dw_fact_order_item` przez `MERGE` z kluczem
złożonym, spóźnione wysyłki do `dw_fact_order_fulfillment`, `OPTIMIZE`/`VACUUM`,
`MERGE` w Redshifcie. **Tabel z dzisiaj nie kasujemy** — jutro na nich pracujemy.

| Zasób | Kasowanie | Koszt, jeśli zostanie |
|---|---|---|
| tabele `dw_*` (Iceberg) | `DROP TABLE` kasuje także pliki na S3 | grosze/mies. za kilka MB |
| `bronze_shipments`, `dw_export_daily_sales` | `DROP TABLE` + prefiks w S3 | grosze |
| schemat `dw_<inicjaly>` w Redshifcie | `DROP SCHEMA dw_<inicjaly> CASCADE` | storage Redshifta, pomijalny |
| workgroup / namespace Redshift Serverless | kasuje osoba, która stawiała | **RPU-godziny — jedyny realny koszt** |

Pełna lista instrukcji sprzątających: `../Dzien3_Data_Warehouse_Zaawansowany/sql/Y6_capstone_i_sprzatanie.sql`
(sekcja sprzątania) — po dniu 3.
