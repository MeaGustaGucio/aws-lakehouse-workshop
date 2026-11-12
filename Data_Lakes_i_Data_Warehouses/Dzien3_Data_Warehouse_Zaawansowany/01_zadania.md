# Dzień 3 — SCD2, przyrost i lakehouse, wersja zaawansowana (zadania)

**Czas:** 3 godziny  
**Format:** praca w konsoli z weryfikacją na liczbach kontrolnych  
**Gdzie:** AWS Console · region **eu-central-1** · Athena (Y0–Y4, Y6) + Redshift Serverless (Y5)  
**Wejście:** stan po wersji zaawansowanej dnia 2 — tabele `dw_*` (Iceberg) w `aurora_<inicjaly>`, schemat `dw_<inicjaly>` w Redshifcie

Ta wersja **zastępuje** podstawową `../Dzien3_Data_Warehouse/`. Tam SCD2 i przyrost
oznaczały nowe tabele (`CTAS` + `UNION ALL` + widok). Tu wymiar i fakty są tabelami
Iceberg z wczoraj i zmieniają się **w miejscu** przez `MERGE INTO`, a każda zmiana
zostaje jako snapshot.

Składnia: `../00_Sciagawka_SQL.md`, sekcje *Iceberg — MERGE / time travel /
OPTIMIZE / VACUUM* i *Redshift — MERGE*.

### Jak czytać materiały

- **Ten plik** mówi, **co** zrobić. Nie ma tu pełnego SQL do skopiowania.
- **`02_rozwiazania.md`** to jedna ścieżka „jak”: co powstało, pełny SQL, liczby
  z etykietą.
- **`sql/Y0_stan_wejsciowy.sql`**, `sql/Y1_scd2_merge.sql`, … — ten sam SQL co w
  rozwiązaniach, jeden plik = jedno ćwiczenie.

---

## Plan na 3 godziny

| Blok | Treść | min | Razem |
|---|---|---|---|
| Y0 | Wprowadzenie architektoniczne; upload dopływu, stan wejściowy | 25 | 25 |
| Y1 | SCD Type 2 przez `MERGE` + `INSERT`; test przedziałów; wejrzenie wstecz w konsoli | 25 | 50 |
| Y2 | Przyrost 20.01: typy ładowania na głos, watermark, `MERGE` z kluczem złożonym, idempotencja, asercje | 35 | 85 |
| Y3 | Late-arriving: spóźnione wysyłki do accumulating snapshot | 15 | 100 |
| — | przerwa | 10 | 110 |
| Y4 | Iceberg od środka: metadane, `ALTER TABLE ADD COLUMNS`, `UPDATE`, `OPTIMIZE` | 30 | 140 |
| Y5 | Redshift: Spectrum widzi zmiany, `MERGE` do wymiaru i faktu, `REFRESH MATERIALIZED VIEW` | 20 | 160 |
| Y6 | Capstone: lineage Bronze → Silver → Gold, `MERGE` agregatu, jedno pytanie; sprzątanie | 20 | 180 |

Y1, Y2 i Y6a są rdzeniem dnia i zostają w planie.

## Po trzech godzinach (jeśli zostanie czas)

| Blok | Treść |
|---|---|
| Y3 | audyt przez time travel (nowo wysłane / nowo dostarczone) |
| Y4d | `VACUUM` |
| Y4e | czego Athena nie umie |
| Y5e | RPU z `SYS_SERVERLESS_USAGE` |

---

## Y0. Upload dopływu, stan wejściowy (po briefingu)

**Dlaczego:** każdy `MERGE` dzisiaj zakłada konkretny stan wczorajszych tabel.
Jedna kontrola na start, zero szukania błędu w złym miejscu. Pliki z 20 i 21
stycznia muszą leżeć we **własnym** buckecie, zanim Y1–Y3 zaczną czytać Bronze.

**Konsola:** AWS Management Console → rola **PowerUserAccess** → **eu-central-1**.

### Co już jest — nic z tego nie tworzymy

| Zasób | Wzorzec | Status |
|---|---|---|
| Bucket S3 | `s3://aurora-<inicjaly>-lake/` | **już istnieje** z dnia 1. Lista bucketów, nie *Create bucket*. |
| Baza Glue | `aurora_<inicjaly>` | **już istnieje** z dnia 1. W Athenie wybieramy ją z listy, **nie** `CREATE DATABASE`. |
| Prefiksy dnia 1 | `bronze/customers/`, `bronze/orders/dt=2026-01-15/`, `bronze/orders/dt=2026-01-16/`, `bronze/order_items/`, … | zostają |
| Prefiks dnia 2 (Z0) | `bronze/shipments/` | zostaje — **nie** wgrywamy ponownie `shipments.csv` |

### Upload — jeden folder, tylko nowe prefiksy

Źródło na dysku: **`Dzien3_Data_Warehouse_Zaawansowany/dane/`**. Tego jednego.
Folderu `Dzien3_Data_Warehouse/dane/` nie ruszamy — te same pliki przyrostu
są już skopiowane tutaj.

| Lokalnie w `dane/` | Prefiks w istniejącym buckecie |
|---|---|
| `bronze/customers_updates/customers_updates.csv` | `s3://aurora-<inicjaly>-lake/bronze/customers_updates/` — 8 zmian (miasto / segment / województwo), `valid_from = 2026-01-20` |
| `bronze/orders/dt=2026-01-20/orders.csv` | `s3://aurora-<inicjaly>-lake/bronze/orders/dt=2026-01-20/` — 40 zamówień; Hive `dt=` obok 15.01 i 16.01 |
| `bronze/order_items_20260120/order_items_2026-01-20.csv` | `s3://aurora-<inicjaly>-lake/bronze/order_items_20260120/` — 40 pozycji; osobny prefiks, bo `bronze_order_items` nie jest partycjonowane (wrzutka do `bronze/order_items/` podniosłaby 486 z dnia 1 do 526) |
| `bronze/shipments_20260121/shipments_2026-01-21.csv` | `s3://aurora-<inicjaly>-lake/bronze/shipments_20260121/` — 88 wysyłek z 21.01; obok wczorajszego `shipments/`, nie do środka |

**Do zrobienia:** S3 → własny bucket `aurora-<inicjaly>-lake` (ten z listy) →
*Upload* → *Add folder* → lokalny `dane/bronze/`. S3 **dokłada** obiekty do
istniejących prefiksów. Albo po kolei:

1. Folder `customers_updates` → `bronze/customers_updates/`.
2. Wejście w istniejący `bronze/orders/` → *Upload* folderu `dt=2026-01-20`
   (żeby obok `dt=2026-01-15` i `dt=2026-01-16` powstał trzeci dzień, a nie
   `bronze/orders/orders/dt=…`).
3. Folder `order_items_20260120` → `bronze/order_items_20260120/`.
4. Folder `shipments_20260121` → `bronze/shipments_20260121/`.

Kontrola w S3: pod własnym `bronze/` widać dotychczasowe katalogi **oraz** cztery
nowe prefiksy z tabeli.

### Rejestracja Bronze i stan `dw_*`

Athena → *Query editor*, baza `aurora_<inicjaly>` (ta z listy). *Reuse query
results* wyłączone.

Paste 1:1: `sql/Y0_stan_wejsciowy.sql`. W pliku, w tej kolejności:

1. `CREATE EXTERNAL TABLE IF NOT EXISTS` — `bronze_customers_updates`,
   `bronze_order_items_20260120`, `bronze_shipments_20260121` (nad nowymi
   prefiksami). `bronze_orders` i `bronze_shipments` z dni 1–2 zostają.
2. `MSCK REPAIR TABLE aurora_<inicjaly>.bronze_orders` — katalog Glue ma
   zobaczyć partycję `dt=2026-01-20`.
3. `UNION ALL` z etykietą `tabela` (Bronze, potem `dw_*`) oraz `GROUP BY dt`
   na `bronze_orders`.

**Kontrola Bronze:** `bronze_customers_updates` **8** ·
`bronze_order_items_20260120` **40** · `bronze_orders` **240** (90 + 110 + 40) ·
`bronze_shipments_20260121` **88**. Po `dt`: 15.01 **90** · 16.01 **110** ·
20.01 **40**.

**Kontrola Gold (`dw_*`):** `dw_dim_customer` **81** (80 klientów + wiersz
nieznany, wszyscy `is_current`) · `dw_fact_daily_sales` **36** ·
`dw_fact_order_fulfillment` **170 zamówień** · `dw_fact_order_item` **409**
pozycji (nie 486 — cancelled odpadły w Silver dnia 1).

Inne liczby na `dw_*` oznaczają, że dzień 2 nie został dokończony. Skrypty
Z1–Z3 z dnia 2 są idempotentne i można je puścić ponownie (ok. 3 min). Inne
liczby na Bronze — zły prefiks albo `MSCK REPAIR` nie zobaczył 20.01.

### Kontekst architektoniczny przed startem

Przegląd zagadnień dnia 3: czym jest SCD i kiedy stosuje się typy 0–3, architektura lakehouse (Silver / Gold / UNLOAD), oś czasu klienta `C0001`, dwa kroki zamknięcia wersji w Icebergu oraz odczyt historyczny przez time travel. Y0 poniżej to upload plików dopływu, rejestracja w warstwie Bronze i kontrolny `COUNT`.

---

## Y1. SCD2 przez MERGE na Icebergu (35 min)

**Dlaczego:** 8 klientów zmieniło miasto lub segment 20 stycznia. Historia ma
zostać, styczniowe zamówienia mają nadal wskazywać styczniowe miasto.

**Konsola:** Athena. Paste: `sql/Y1_scd2_merge.sql`. Plik i tabela
`bronze_customers_updates` są po Y0 (`CREATE EXTERNAL TABLE` w
`sql/Y0_stan_wejsciowy.sql`; paste Y1 ma ten sam `IF NOT EXISTS`).

`dw_dim_customer` to wymiar klienta: jeden wiersz to jedna **wersja** klienta
w przedziale dat (`valid_from`–`valid_to`); bieżąca ma `is_current` i
`valid_to = 9999-12-31`.

> **Typy ładowania.** Przy tym `MERGE` wraca pojęcie upsert
> na wymiarze oraz SCD2 jako wyspecjalizowany `MERGE`. Ćwiczenie zostaje tym
> samym SQL: krok 1 zamyka wersję, krok 2 robi `INSERT` nowej. Athena przypisuje
> wiersz źródła do jednej gałęzi `WHEN`, więc obu rzeczy nie da się złożyć
> w jednej instrukcji. Pozostałe wzorce (pełne odświeżenie, przyrost, zakładka, CDC)
> wracają w Y2.

### Y1a. Bronze i stan przed (5 min)

**Do zbudowania:** tabela zewnętrzna `bronze_customers_updates` nad
`s3://…/bronze/customers_updates/` (`OpenCSVSerde`, `skip.header.line.count = 1`).
DDL jest w Y0; paste Y1 ma ten sam `IF NOT EXISTS`.
**Grain:** jeden wiersz to jedna zmiana atrybutu z 20.01 — to delta, nie pełny wymiar.

**Kontrola:** `COUNT` / `MIN(valid_from)` / `MAX(valid_from)` → **8** zmian, wszystkie
**2026-01-20**. Na wymiarze: **81** wierszy, **81** bieżących.

### Y1b. Krok 1 — MERGE zamyka wersje (10 min)

**Kształt:** `MERGE INTO dw_dim_customer t USING bronze_customers_updates s ON
customer_id AND t.is_current = true WHEN MATCHED AND (city/segment/voivodeship
się różni) THEN UPDATE SET valid_to = dzień przed valid_from źródła,
is_current = false`. Bez gałęzi `INSERT`.

`AND t.is_current = true` w `ON` wybiera *którą* wersję porównywać; `WHEN MATCHED
AND (…)` decyduje, *czy* cokolwiek się zmieniło. Bez drugiego warunku drugie
uruchomienie zamknęłoby świeżo wstawione wersje.

**Kontrola:** **81** wierszy, **73** bieżące — 8 wersji zamkniętych, jeszcze bez
nowych.

### Y1c. Krok 2 — INSERT nowych wersji (8 min)

**Kształt:** `INSERT … SELECT` z `MAX(customer_sk) + ROW_NUMBER()`,
`valid_from` ze źródła, `valid_to = 9999-12-31`, `is_current = true`,
`WHERE NOT EXISTS` wersji bieżącej tego `customer_id`.

**Kontrola:** **89** wierszy · **81** bieżących · **8** historycznych ·
`max_sk` **88**. Klient `C0001`: dwa wiersze — Gdańsk / standard do 19.01
(`is_current` false) i Białystok / vip od 20.01. Test przedziałów: **0** wierszy
zachodzących. Fakt z wczoraj: **409** pozycji, **0** wskazujących nowe wersje
(sk > 80).

Drugie uruchomienie całego pliku: MERGE nic nie zamyka, INSERT wstawia 0.

### Y1d. Time travel — wymiar sprzed MERGE (12 min)

**Do zrobienia:** odczyt `"dw_dim_customer$snapshots"` (kolumny `snapshot_id`,
`committed_at`, `operation`, `summary['added-records']`,
`summary['added-position-deletes']`, `summary['total-records']`), `ORDER BY
committed_at`. Numer z **drugiego** wiersza (`append` 1 wiersza, stan 81)
wklejamy jako literał do `FOR VERSION AS OF`.

**Kontrola:** cztery snapshoty (append 80 · append 1 · overwrite 8 z
position_deletes 8 · append 8). `AS OF` drugiego: **81 / 81** i C0001 Gdańsk
standard — wymiar sprzed zmian, choć tabela ma już 89 wierszy.

`FOR VERSION AS OF` wymaga **stałej**: podzapytanie po `$snapshots` daje
`EXPRESSION_NOT_CONSTANT`. `total-records` rośnie powyżej 89, bo MERGE w Athenie
to merge-on-read: stary plik zostaje, dochodzi plik usunięć pozycyjnych.

---

## Y2. Przyrost 20 stycznia przez MERGE (35 min)

**Dlaczego:** dołożyć dzień bez przeliczania 15 i 16 stycznia, tak, żeby drugie
uruchomienie nie zmieniło nic. Wymiar daty zna już cały 2026, fakt jest jedną
tabelą Iceberg — przyrost to jeden `MERGE`.

**Konsola:** Athena. Paste: `sql/Y2_przyrost_merge.sql`. Partycja zamówień,
plik pozycji i `MSCK REPAIR` są po Y0.

`dw_fact_order_item` to fakt transakcyjny: jeden wiersz to jedna pozycja
zamówienia.

> **Typy ładowania.** Sześć wzorców: pełne odświeżenie (full
> refresh) — CTAS wymiarów z dnia 2; przyrost tylko nowych wierszy (append-only
> incremental) — filtr `dt > watermark`; zakładka (incremental with lookback
> window) — korekty „nowych” faktów przez N dni; upsert (`MERGE`); CDC
> (log-based); dzienny snapshot. Ćwiczenie zostaje tym
> samym `MERGE`. Filtr
> `dt > '2026-01-16'` jest przyrostem tylko nowszych wierszy; sam `MERGE` po
> `(order_id, product_sk)` jest upsertem, dlatego drugie uruchomienie zostaje
> na 449. Gdyby korekta wysyłki z 16 stycznia przyszła 21, ten watermark by
> jej nie wziął — stąd zakładka, pokazana na fulfillment w Y3. N na tych
> zajęciach to dni, na które są jeszcze pliki (15, 16, 20 i plik z 21), nie
> umowne siedem dni z SLA.
>
> **Cztery przypadki grainu.** Wiersz `-1`:
> `dw_dim_date` ma 366 wierszy (365 + `-1`); `LEFT JOIN` + `COALESCE` sadza fakt
> na nieznanym, `INNER JOIN` go kasuje. 409, nie 486: 77 cancelled odpadło w JOIN
> z dnia 1 (L5). Trzy daty do jednej `dw_dim_date`: 104 z 170 ma `ship_date_sk = -1`.
> `orders_n` się nie sumuje: 361 po kategoriach, 170 zamówień.

### Y2a. Bronze: partycja i osobna tabela pozycji (7 min)

**Do zrobienia:** paste Y2 powtarza to, co Y0 już zrobił (`IF NOT EXISTS` /
`MSCK REPAIR` są idempotentne):

1. `MSCK REPAIR TABLE bronze_orders` (albo punktowe `ALTER TABLE … ADD IF NOT
   EXISTS PARTITION` — wzorzec w komentarzu skryptu).
2. Tabela zewnętrzna `bronze_order_items_20260120` nad
   `s3://…/bronze/order_items_20260120/`. **Grain:** jeden wiersz to jedna
   pozycja z 20.01.

**Kontrola:** `bronze_orders` po `dt`: 15.01 **90** · 16.01 **110** · 20.01
**40**. Pozycje nowego dnia **40**; `bronze_order_items` dnia 1 nadal **486**
(nie 526).

### Y2b. Watermark i MERGE (15 min)

**Do zrobienia:** `SELECT MAX(order_date)` z faktu — oczekiwane **2026-01-16**.
Potem `MERGE INTO dw_fact_order_item` ze źródłem, które:

- czyta `bronze_order_items_20260120` JOIN `bronze_orders` z filtrem
  `dt > '2026-01-16'` (watermark) i `status IN ('paid', 'shipped')`,
- wylicza `net_amount_pln` i klucze wymiarów (`LEFT JOIN` + `COALESCE` do `-1`),
- klienta łączy po `customer_id` **i** `BETWEEN valid_from AND valid_to`
  (zamówienia z 20.01 trafiają w nowe wersje SCD2).

`ON` po **kluczu złożonym** `(order_id, product_sk)` — pozycja nie ma własnego
id. `WHEN MATCHED THEN UPDATE` ilości i kwot, `WHEN NOT MATCHED THEN INSERT`
z `load_ts`.

**Kontrola:** przychód i liczba pozycji per `order_date`, `ORDER BY order_date`.
Oczekiwane: 15.01 **186** pozycji / **653 531** zł · 16.01 **223** / **773 593** ·
20.01 **40** / **101 546**. Razem **449** pozycji, **1 528 670,00** zł.
Drugie uruchomienie MERGE: te same trzy wiersze.

### Y2c. Asercje i metadane po przyroście (8 min)

W skrypcie asercje B1–B4 (grain unikalny, brak `-1`, `409 + 40 = 449`).
`$partitions`: po dwóch MERGE partycja 20.01 pokazuje **80** rekordów w **2**
plikach przy `COUNT(*) = 40` — `$partitions` liczy wiersze fizyczne;
idempotencja dotyczy danych, nie plików. Stąd `OPTIMIZE` w Y4.

### Y2d. Accumulating snapshot — nowe zamówienia (5 min)

Ten sam wzorzec, grain zamówienia: `MERGE` do `dw_fact_order_fulfillment`.

**Kontrola:** **210 zamówień** (170 z dnia 2 + 40 z 20.01 — to liczba zamówień,
nie minut) · **66** wysłanych · **45** dostarczonych · przychód **1 528 670,00**.
Czterdzieści nowych zamówień ma `ship_date_sk = -1`.

---

## Y3. Late-arriving: spóźnione wysyłki (15 min)

**Dlaczego:** fakt transakcyjny nigdy nie „dogoni” zdarzeń, które przychodzą po
fakcie. Accumulating snapshot jest po to, żeby te zdarzenia *dopisywać do
istniejących wierszy*.

**Konsola:** Athena. Paste: `sql/Y3_fulfillment_merge.sql`. Plik i tabela
`bronze_shipments_20260121` są po Y0.

`dw_fact_order_fulfillment` to accumulating snapshot: jeden wiersz to jedno
zamówienie, aktualizowany gdy pojawia się wysyłka albo dostawa.

**Do zbudowania:** tabela zewnętrzna `bronze_shipments_20260121` (DDL w Y0;
paste Y3 ma ten sam `IF NOT EXISTS`). **Grain:**
jeden wiersz to jedna wysyłka z pliku 21.01.

**Kontrola Bronze:** **88** wysyłek · **51** z dostawą · **37** bez · **21**
znanych wczoraj (to samo `shipment_id` co w `bronze_shipments`). 67 nowych
`shipment_id`.

**Kształt MERGE:** `USING` z `ROW_NUMBER() … PARTITION BY order_id` (najnowsza
wysyłka, gdyby było ich kilka), `LEFT JOIN` do `dw_dim_date` w dwóch rolach
(wysyłka, dostawa), `ON t.order_id = s.order_id`, **tylko** `WHEN MATCHED THEN
UPDATE` — bez `INSERT`, bo zamówienie bez faktu byłoby błędem danych.
`COALESCE` dat do `-1`.

**Kontrola Gold:** **210 zamówień** · **133** wysłanych · **96** dostarczonych ·
**77** bez wysyłki (przed MERGE: 210 / 66 / 45 / 144). Liczba wierszy nie
rośnie — zmienia się treść. Drugie uruchomienie: identycznie.

Raport per przewoźnik (wymiar daty w trzech rolach): 4 wiersze —
DHL 23/18 · DPD 34/25 · InPost 41/28 · Poczta Polska 35/25; średnie opóźnienia
w rozwiązaniach.

**Audyt przez time travel** (jeśli czas): JOIN tabeli z samą sobą
`FOR VERSION AS OF` snapshotu sprzed MERGE. Oczekiwane: **67** nowo wysłanych
zamówień · **51** nowo dostarczonych.

---

## Y4. Iceberg od środka (30 min)

**Dlaczego:** każdy `MERGE` i `UPDATE` dokłada **nowe** pliki w `data/`, zamiast
poprawiać stare. Wierszy jest nadal 449, ale zapytanie otwiera ich coraz więcej.
To nie to samo co „za długa historia”: najpierw **sklejamy pliki** (`OPTIMIZE`),
a **wyrzucanie starych zdjęć** (`VACUUM`) zostaje `[po 3 h]`.

**Konsola:** Athena. Paste: `sql/Y4_iceberg_operacje.sql`.

### Y4a. Metadane (8 min)

**Do zrobienia:** odczyt `$history` (łańcuch `parent_id`) i `$files`
(ścieżka, `record_count`, rozmiar) faktu transakcyjnego.

**Kontrola:** `$history` — 3 wiersze (CTAS, MERGE, MERGE). `$files` — 4 pliki:
186 / 223 / 40 / 40 wierszy. Ścieżka ma losowy prefiks przed
`order_date_day=` (`write.object-storage.enabled`) — Icebergu nie czyta się
„po katalogach” jak tabeli Hive.

### Y4b. Ewolucja schematu i UPDATE (8 min)

**Do zrobienia:** `ALTER TABLE … ADD COLUMNS (promo_code string)`, potem
`UPDATE` ustawiający `RABAT-` + rabat dla `discount_pln > 0`.

**Kontrola:** `GROUP BY promo_code`: RABAT-100 **67** · RABAT-20 **69** ·
RABAT-50 **79** · `NULL` **234** (pozycje bez rabatu). Drugie `ALTER`:
`Cannot add column, name already exists` — jedyna nieidempotentna instrukcja
dnia, bezpieczny błąd. `ADD COLUMNS` nie dotyka plików Parquet.

### Y4c. OPTIMIZE — sklej pliki, historia zostaje (8 min)

> Iceberg po `MERGE` zostawia **dużo małych plików** (każdy zapis ≈ nowe pliki
> w `data/`). Zapytanie otwiera je wszystkie.
>
> **`OPTIMIZE` = sklej małe pliki w większe.** Jak zbieranie karteczek do jednego
> zeszytu. Aktualne zapytanie czyta np. 3 pliki zamiast 80. **Stare zdjęcia
> (snapshoty) dalej istnieją** — wejrzenie wstecz nadal widzi stare małe pliki.
> Nic nie jest skasowane z historii.
>
> **`VACUUM` = wyrzuć historię, której już nie potrzebujesz.** To jest Y4d
> `[po 3 h]`: kasuje wygasłe snapshoty i pliki, do których **żaden** snapshot
> już nie wskazuje. Po `VACUUM` `AS OF` starego snapu **przestaje działać**.
> Płacisz: mniej S3, mniej wejrzenia wstecz.

**Do zrobienia:** w bloku Y4c wykonujemy

```sql
OPTIMIZE aurora_<inicjaly>.dw_fact_order_item REWRITE DATA USING BIN_PACK;
```

Potem to samo `$files` co w Y4a oraz `COUNT(*)` i sumę przychodu. Najpierw spójrz
na liczbę plików **przed** sklejeniem (Y4a).

**Kontrola (Athena v3, etykiety):** przed `OPTIMIZE` — **4 pliki** (186 / 223 / 40 / 40
wierszy). Po `OPTIMIZE` — **3 pliki** · **449** wierszy · **10 696 B**. Przychód
nadal **449** pozycji i **1 528 670,00** zł. Partycja 20.01: z **80 wierszy w 2
plikach** na **40 w 1**. Dane bez zmian. Stare małe pliki leżą na S3, bo stare
snapshoty ich jeszcze potrzebują.

### Y4d. `[po 3 h]` VACUUM — kasujesz stare snapshoty (6 min)

To nie jest „OPTIMIZE jeszcze raz”. `OPTIMIZE` skleił pliki; zdjęcia zostały.
`VACUUM` **wyrzuca** snapshoty starsze niż próg i pliki, do których żaden
snapshot już nie wskazuje.

**Do zrobienia:** chwilowo `vacuum_max_snapshot_age_seconds = 60`, `VACUUM`,
kontrola liczby snapshotów i `COUNT(*)`, próg z powrotem na 5 dni (`432000`).
Potem `FOR VERSION AS OF` ze starym snapshotem **tej** tabeli (pierwszy wiersz
`$history` z Y4a — własny numer wygenerowany w bieżącym przebiegu).

**Kontrola (Athena v3):** snapshoty **5 → 2**; wiersze nadal **449**.
`SELECT COUNT(*) … FOR VERSION AS OF 188806921387989439` →
`INVALID_ARGUMENTS: Iceberg snapshot ID does not exists`.

**`OPTIMIZE` ≠ `VACUUM`:** sklejanie plików zostawia historię; sprzątanie snapshotów ją kasuje.

### Y4e. `[po 3 h]` Czego Athena nie umie (5 min)

Komunikaty z Atheny v3 są w `02_rozwiazania.md`: brak partition evolution, brak
branching/tagging, brak `WHEN NOT MATCHED BY SOURCE`, brak `INSERT *`.

---

## Y5. Redshift: MERGE i odświeżenie (20 min)

**Dlaczego:** wczorajsza kopia star schema w Redshifcie jest już nieaktualna.

**Konsola:** (**PowerUserAccess**) Redshift → *Query data* → Query Editor v2,
Federated user, `dev`. Paste: `sql/Y5_redshift_merge.sql`. Nazwy Iceberga:
`"awsdatacatalog"."aurora_<inicjaly>"."dw_…"`.

### Y5a. Spectrum widzi zmiany natychmiast (4 min)

**Kontrola:** Iceberg wymiar **89** · Iceberg fakt **449** · lokalny wymiar
**81** · lokalny fakt **409**.

### Y5b–Y5c. MERGE do wymiaru i faktu (14 min)

**Kształt wymiaru:** `MERGE INTO dw_<inicjaly>.dim_customer USING` Spectrum
`ON customer_sk`; obie gałęzie (`WHEN MATCHED` i `WHEN NOT MATCHED`) — Redshift
wymaga obu i nie ma `WHEN MATCHED AND (…)` (filtr idzie do źródła).

**Kontrola wymiaru:** **89** wierszy · **81** bieżących · **8** historycznych.

**Kształt faktu:** `MERGE` po `(order_id, product_sk)`, źródło z
`WHERE order_date > DATE '2026-01-16'`.

**Kontrola faktu:** 15.01 **186** · 16.01 **223** · 20.01 **40** / **101 546**.
Drugie uruchomienie obu: bez zmian. Kolumna `promo_code` z Y4 niczego nie
psuje, bo `INSERT` wypisuje kolumny jawnie.

### Y5d. REFRESH MATERIALIZED VIEW (7 min)

**Kontrola:** MV przed **36** / **1 427 124,00** → po `REFRESH` **52** /
**1 528 670,00**. Rekoncyliacja trzech silników: Athena Iceberg **449** =
Redshift lokalnie **449** = MV **52** wierszy agregatu, wszystkie
**1 528 670,00** zł.

### Y5e. `[po 3 h]` RPU (5 min)

`SYS_SERVERLESS_USAGE` w skrypcie: `compute_capacity` i `charged_seconds` na
minutę.

---

## Y6. Capstone i sprzątanie (20 min)

**Dlaczego:** jedna kwota, która musi się zgadzać na każdym etapie trzech dni.

**Konsola:** Athena. Paste: `sql/Y6_capstone_i_sprzatanie.sql`.

### Y6a. Lineage (8 min)

**Pytanie:** ile wierszy i jaki przychód na etapie Bronze (dzień 1 ∪ 20.01),
Silver, fakt Gold, agregat Gold.

**Kontrola:** Bronze pozycji **526** (486 + 40) · Silver **409** / **1 427 124**
(przyrost szedł Bronze → Gold, Silver dnia 1 nie był przeładowany) · fakt
**449** / **1 528 670** · agregat nadal **36** / **1 427 124** — celowa usterka:
agregatu nikt nie odświeżył.

### Y6b. MERGE agregatu (7 min)

**Kształt:** `MERGE` do `dw_fact_daily_sales` z watermarkem
`order_date_sk > (SELECT MAX(order_date_sk) FROM agregat)`.

**Kontrola:** **52** wiersze · **1 528 670,00** zł. Drugie uruchomienie: źródło
puste.

### Y6c. Pytanie, które wymaga wszystkiego (5 min)

Przychód per województwo klienta *w chwili zakupu*, per dzień, z sumami —
`ROLLUP` + SCD2 + pełny kalendarz + przyrost.

**Kontrola:** 34 wiersze; `razem` per dzień: 15.01 **74** zamówienia /
**653 531** · 16.01 **96** / **773 593** · 20.01 **40** / **101 546** · razem
**210 zamówień** / **1 528 670**. Klient, który 20.01 zmienił województwo,
liczy się 15/16.01 do starego, a 20.01 do nowego.

### Sprzątanie (5 min)

| Zasób | Kasowanie | Koszt, jeśli zostanie |
|---|---|---|
| tabele `dw_*` (Iceberg) | `DROP TABLE` kasuje **także pliki na S3** (`data/` i `metadata/`) | grosze/mies. |
| `bronze_shipments*`, `bronze_customers_updates`, `bronze_order_items_20260120`, `dw_export_daily_sales` | `DROP TABLE` + prefiksy w konsoli S3 | grosze |
| schemat `dw_<inicjaly>` w Redshifcie | `DROP SCHEMA dw_<inicjaly> CASCADE` | pomijalny |
| workgroup / namespace Redshift Serverless | osoba, która stawiała | **RPU-godziny — jedyny realny koszt** |

Instrukcje `DROP` są w `sql/Y6_capstone_i_sprzatanie.sql`, sekcja sprzątania
(zakomentowane — odkomentować po zajęciach, nie w trakcie).
