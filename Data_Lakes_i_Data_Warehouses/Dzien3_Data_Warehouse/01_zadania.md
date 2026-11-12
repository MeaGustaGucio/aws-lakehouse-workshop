# Dzień 3 — SCD, ładowanie przyrostowe, lakehouse (zadania)

**Czas:** 3 godziny  
**Gdzie:** AWS Console · konto warsztatowe / sandbox AWS · S3 + Athena (W6–W9), sprzątanie także w Redshifcie (W10)  
**Wejście:** warstwa Gold z dnia 2 i bucket z dnia 1  
**Prefix (izolacja zasobów):** ten sam `aurora-<inicjaly>-`; baza w Athenie `aurora_<inicjaly>`

Dzień 2 zbudował model na danych, które się nie zmieniają: dwa dni sprzedaży,
osiemdziesięciu klientów, każdy w jednej wersji. Dzisiaj zmieniają się dwie rzeczy
naraz — **przychodzi nowy dzień sprzedaży** (20 stycznia) i **ośmiu klientów
zmienia miasto**. To jest moment, w którym większość modeli zaczyna kłamać, i to
w sposób, którego nie widać w żadnym komunikacie błędu.

Składnia SQL, której będziemy używać, jest zebrana w `../00_Sciagawka_SQL.md`.
Warto mieć ten plik otwarty obok — wracają z niego wzorce CTAS, `MERGE`
i tabeli zewnętrznej.

Rozwiązania są w `02_rozwiazania.md` (pełny SQL i liczby z etykietą). Katalog `sql/`
trzyma ten sam SQL 1:1: `sql/W6_scd2_customer.sql`, `sql/W7_incremental.sql`,
`sql/W8_most_lake_model.sql`, `sql/W9_pytania_dyrektora.sql`. Warto zaglądać
tam **po** własnej próbie.

Ten folder to **wersja podstawowa** dnia 3 — kontynuacja `../Dzien2_Data_Warehouse/`.
Alternatywą jest wersja zaawansowana `../Dzien3_Data_Warehouse_Zaawansowany/` (SCD2
i przyrost przez `MERGE` na tabelach Iceberg z `../Dzien2_Data_Warehouse_Zaawansowany/`);
grupa idzie tą wersją, którą trener wybrał po dniu 1. Pliki z `dane/bronze/` w tym
folderze są wspólne dla obu wersji.

---

## Co dzisiaj powstaje

| Ćwiczenie | Temat | Co zostaje po nim |
|---|---|---|
| W6 | SCD Type 2 na wymiarze klienta | `gold_dim_customer_scd` — 88 wierszy zamiast 80 |
| W7 | ładowanie przyrostowe dnia 20.01 | fakt nowego dnia, widok całości, wariant Iceberg |
| W8 | most między data lake a modelem | jedno zapytanie łączące Parquet z JSON-em |
| W9 | zadanie końcowe | cztery odpowiedzi dla dyrektora handlowego |
| W10 | sprzątanie | puste konto i zerowy rachunek |

W6 i W7 są ze sobą związane: fakt nowego dnia łączy się z wymiarem SCD2 zbudowanym
w W6. W8 i W9 to zapytania na tym, co już istnieje. W10 domyka całość: konto jest
wspólne i płaci za nie ktoś inny, więc rozłożenie własnego środowiska jest
tą samą robotą co jego postawienie — tylko w drugą stronę.

---

## Stan wejściowy z dnia 2 (10 min)

**Cel:** upewnienie się, że wszystko, na czym dzisiaj budujemy, nadal istnieje.

Logowanie do AWS Management Console (np. przez IAM Identity Center lub własny sandbox AWS, rola typu **PowerUserAccess**). Region w prawym górnym rogu: **`eu-central-1` (Frankfurt)** —
ten sam co poprzednio, bo S3, Glue, Athena i Redshift są usługami regionalnymi.

Pięć rzeczy do potwierdzenia, w tej kolejności:

| # | Co | Gdzie | Czego oczekujemy |
|---|---|---|---|
| 1 | bucket `aurora-<inicjaly>-lake` | S3 → *Buckets* | istnieje, w środku `bronze/`, `silver/`, `gold/` |
| 2 | lokalizacja wyników Atheny | Athena → *Query settings* | `s3://aurora-<inicjaly>-lake/athena-results/` |
| 3 | tabele Gold | Athena → baza `aurora_<inicjaly>` | `gold_dim_date`, `gold_dim_channel`, `gold_dim_customer`, `gold_dim_product`, `gold_fact_order_item` |
| 4 | tabele Silver | ta sama baza | `silver_orders`, `silver_order_items`, `silver_customers`, `silver_products` |
| 5 | *Reuse query results* | przełącznik nad wynikami | wyłączony |

### Kontrola

```sql
SELECT
  (SELECT COUNT(*) FROM aurora_<inicjaly>.gold_dim_customer)     AS dim_customer,
  (SELECT COUNT(*) FROM aurora_<inicjaly>.gold_fact_order_item)  AS fact_rows,
  (SELECT ROUND(SUM(net_amount_pln), 2)
     FROM aurora_<inicjaly>.gold_fact_order_item)                AS revenue_pln;
```

Oczekiwane: **80 · 409 · 1 427 124,00**. Te trzy liczby są punktem odniesienia
dla całego dnia — po W7 fakt urośnie do 449 wierszy, ale 409 z nich ma pozostać
nietkniętych, a przychód ze stycznia ma się nie zmienić ani o grosz.

Gdyby zasobów z dnia 2 nie było (np. po wcześniejszym sprzątaniu środowiska), do odtworzenia zostaje cała ścieżka z plików
`Dzien1_Data_Lakes/dane/bronze/` oraz skryptów `Dzien1_Data_Lakes/sql/` i
`Dzien2_Data_Warehouse/sql/W3_elt_athena.sql`. Zajmuje to około 25 minut
i wystarcza na cały dzień 3.

---

## Dane wejściowe — dopływ na dzień 3

W folderze `dane/` leży jeden katalog `bronze` o zawartości:

```text
bronze/
  customers_updates/customers_updates.csv           8 klientów zmienia miasto i segment
  orders/dt=2026-01-20/orders.csv                  40 zamówień z 20 stycznia
  order_items_20260120/order_items_2026-01-20.csv  40 pozycji nowego dnia
```

To jest **dopływ**, a nie nowy komplet danych. Pliki z dnia 1 zostają na S3
dokładnie tam, gdzie były, i nic ich nie nadpisuje.

**Do zrobienia:** S3 → bucket `aurora-<inicjaly>-lake` → *Upload* → *Add folder*
→ wskazanie katalogu `bronze` → *Upload*. S3 **dokłada** obiekty do istniejących
prefiksów, więc `orders/dt=2026-01-20/` wyląduje obok partycji z 15 i 16 stycznia,
a nie zamiast nich. Ekran uploadu jest ten sam co w dniu 1 i wszystkie jego opcje
zostają domyślne:

| Opcja | Zostaje | Dlaczego tak i kiedy inaczej |
|---|---|---|
| Storage class | `Standard` | klasy Glacier są tańsze za GB, ale Athena nie odczyta ich w locie — obiekt trzeba najpierw przywrócić |
| Server-side encryption | `Use bucket settings` | dziedziczymy SSE-S3 ustawione przy tworzeniu bucketu |
| Checksums | domyślne | S3 liczy sumę kontrolną sam; jawny SHA-256 przydaje się, gdy trzeba udowodnić integralność przesyłu |

### Dlaczego pozycje nowego dnia mają osobny prefiks

Zamówienia z 20 stycznia trafiają do `bronze/orders/dt=2026-01-20/`, czyli
do **istniejącej tabeli, jako nowa partycja**. Pozycje trafiają do zupełnie
osobnego prefiksu `bronze/order_items_20260120/`, a nie do `bronze/order_items/`.

Powód jest konkretny i mierzalny. Tabela `bronze_order_items` z dnia 1 nie jest
partycjonowana — jej `LOCATION` to `bronze/order_items/` i Athena czyta **wszystko**,
co w tym katalogu leży. Dołożenie tam czterdziestu nowych pozycji sprawiłoby, że
`SELECT COUNT(*)` zwróciłby **526** zamiast 486, a razem z tą liczbą przestałyby
się zgadzać wszystkie liczby kontrolne dnia 1: 409 wierszy Silver, 409 wierszy
faktu i przychód 1 427 124,00.

Osobny prefiks pozwala policzyć nowy dzień niezależnie i zostawia dzień 1
w stanie, w którym da się do niego wrócić. W produkcji rozwiązałoby to
partycjonowanie `order_items` po `dt` już od pierwszego dnia — i to jest właśnie
lekcja, którą ten prefiks ilustruje kosztem jednej brzydkiej nazwy tabeli.

**Pytanie do przemyślenia:** gdyby `bronze_order_items` było od początku
partycjonowane po `dt`, czego byśmy dzisiaj nie musieli obchodzić — i co
w zamian trzeba by zrobić inaczej w dniu 1?

---

## W6. SCD Type 2 na wymiarze klienta (40 min)

**Cel:** wymiar, który pamięta, kim klient był **w momencie zakupu**, a nie tylko
kim jest dzisiaj.

### Czym jest Slowly Changing Dimension

Atrybuty wymiaru zmieniają się rzadko, ale się zmieniają: klient się przeprowadza,
produkt wędruje do innej kategorii, sprzedawca dostaje nowy region. Pytanie
brzmi, co zrobić z historią — i odpowiedzi są ponumerowane, bo Ralph Kimball
opisał je jako typy.

| Typ | Co się dzieje przy zmianie | Historia | Kiedy się stosuje |
|---|---|---|---|
| 0 | wartość nigdy się nie zmienia | z definicji brak zmian | data założenia konta, numer seryjny |
| 1 | nadpisanie w miejscu | znika bezpowrotnie | literówki i poprawki błędów, gdzie stara wartość była po prostu zła |
| 2 | stary wiersz zamknięty, nowy otwarty | pełna | zmiany, po których stary raport ma zostać taki, jaki był |
| 3 | poprzednia wartość w dodatkowej kolumnie | jedna zmiana wstecz | reorganizacje, gdy trzeba na chwilę pokazać „stara struktura obok nowej” |

Dzisiaj budujemy **Type 2**. Klient, który przeprowadził się z Krakowa
do Białegostoku, dostaje w wymiarze **dwa wiersze**: jeden opisujący go
do 19 stycznia, drugi od 20 stycznia. Fakt ze stycznia dalej wskazuje na ten
pierwszy — i dzięki temu raport za styczeń nie zmienia się z dnia na dzień
tylko dlatego, że ktoś zmienił adres.

Type 1 na tych samych danych byłby jednym `UPDATE` i po zmianie przychód
ze stycznia „przeprowadziłby się” razem z klientem. Kraków straciłby 40 tysięcy
złotych, których nigdy nie stracił, i nikt nie miałby jak tego zauważyć.

### Nowe kolumny i ich znaczenie

`gold_dim_customer` ma je już od wczoraj, choć każdy klient miał jedną wersję:

| Kolumna | Typ | Znaczenie |
|---|---|---|
| `valid_from` | `date` | od kiedy ta wersja opisuje klienta |
| `valid_to` | `date` | do kiedy włącznie |
| `is_current` | `boolean` | czy to wersja aktualna |

Dwie konwencje wartości brzegowych, które warto rozumieć, zanim wejdą do kodu:

- **`valid_to = DATE '9999-12-31'`, nie `NULL`.** Wersja otwarta „obowiązuje
  nadal”. Naturalnym odruchem jest wpisanie tam `NULL` w znaczeniu „bez
  ograniczenia”, ale złączenie z faktem wygląda tak:
  `order_date BETWEEN valid_from AND valid_to`, a `BETWEEN` z `NULL`-em
  po którejkolwiek stronie daje `NULL`, nie `TRUE` — i **nie dopasowuje niczego**.
  Wiersze faktu wypadają wtedy na `INNER JOIN` po cichu, bez błędu i bez
  ostrzeżenia.
- **`valid_from = DATE '1900-01-01'` w wierszach z dnia 2.** Znaczy „ta wersja
  obowiązuje od zawsze”. Data jest umowna i celowo wcześniejsza niż jakakolwiek
  data w danych — dzięki temu ten sam warunek `BETWEEN` działa również dla
  siedemdziesięciu dwóch klientów, którzy się nigdzie nie przeprowadzili.
  Bez tej wartości ich wiersze faktu wypadłyby dokładnie tak samo cicho.

`is_current` jest kolumną **nadmiarową**: da się ją wyliczyć z dat
(`valid_to = DATE '9999-12-31'`). Trzyma się ją dla wygody i szybkości
najczęstszego zapytania, czyli „stan na dziś”. To świadoma denormalizacja,
za którą płaci się obowiązkiem utrzymania spójności obu zapisów.

### Do zrobienia

**1. Rejestracja tabeli `bronze_customers_updates`** na wgranym pliku.
Plik ma te same kolumny co `customers`, plus jedną nową — `valid_from` z datą
obowiązywania zmiany. Wzorzec `CREATE EXTERNAL TABLE` dla CSV jest w ściągawce;
kolumny idą w kolejności z nagłówka pliku, wszystkie jako `string`, bo
`OpenCSVSerde` i tak czyta każdą jako tekst.

Kontrola: **8 wierszy**, a każdy `customer_id` z tej ósemki istnieje już
w `gold_dim_customer`.

**2. Zbudowanie nowej tabeli `gold_dim_customer_scd`** instrukcją CTAS.

Wymiar z dnia 2 **zostaje nietknięty**. Nie dlatego, że nie da się go przebudować,
tylko dlatego, że obok siebie oba warianty da się porównać, a po pomyłce jest
dokąd wrócić. W produkcji SCD2 nadpisuje wymiar w miejscu — ale tam istnieje też
backup i test na środowisku przejściowym.

Nowa tabela składa się z **trzech grup wierszy sklejonych `UNION ALL`**:

| Grupa | Ilu wierszy | `customer_sk` | `valid_from` | `valid_to` | `is_current` |
|---|---|---|---|---|---|
| klienci bez zmian | 72 | ten sam co w dniu 2 | `1900-01-01` | `9999-12-31` | `true` |
| **stare** wersje ósemki | 8 | **ten sam co w dniu 2** | `1900-01-01` | `2026-01-19` | `false` |
| **nowe** wersje ósemki | 8 | **nowy** | `2026-01-20` | `9999-12-31` | `true` |

Dwie pierwsze grupy da się zbudować jednym `SELECT`-em z `LEFT JOIN` do updates —
różnią się tylko wartościami `valid_to` i `is_current`, a te wynikają z tego,
czy złączenie trafiło.

**Zachowanie starego `customer_sk` w wersji historycznej jest tu najważniejsze.**
Fakt z dnia 2 nie jest ruszany, a on wskazuje właśnie na ten klucz. Nowy klucz
w starym wierszu oznaczałby, że 409 wierszy faktu nagle nie ma do czego trafić.

`valid_to` wersji zamykanej to **dzień przed** początkiem nowej, czyli
2026-01-19. Nie ten sam dzień — przy `valid_to = valid_from` nowej wersji
zapytanie o 20 stycznia dopasowałoby obie wersje naraz i podwoiło przychód.

Nowy `customer_sk` musi być unikalny w całej tabeli. Najprostszy sposób
w Athenie to przesunięcie zakresu, na przykład `1000 + ROW_NUMBER()`: stare
klucze zajmują 1–80, nowe zaczynają się od 1001 i nie mają szans się zderzyć.
W produkcji tę rolę pełni sekwencja albo `IDENTITY`, właśnie po to, żeby klucz
raz nadany już nigdy się nie powtórzył.

### Pułapka składniowa, na której wykłada się większość sali

`gold_dim_customer` i `bronze_customers_updates` mają **te same nazwy kolumn** —
`customer_id`, `city`, `segment`, `valid_from`. Bez aliasu Athena odpowiada
`Column 'customer_id' is ambiguous` i przerywa. W skrypcie `sql/W6_scd2_customer.sql`
każda kolumna jest kwalifikowana (`d.` dla wymiaru, `u.` dla updates). To jest
reguła przy łączeniu wymiaru z jego własną aktualizacją, a nie literówka.

### Kontrola

| Zapytanie | Czego oczekujemy |
|---|---|
| `COUNT(*)` na `gold_dim_customer_scd` | **88** (80 + 8) |
| `COUNT_IF(is_current)` | **80** — każdy klient ma dokładnie jedną wersję aktualną |
| `customer_id` z więcej niż jedną wersją | **8** identyfikatorów, każdy w dwóch wersjach |
| pary wersji o zachodzących okresach ważności | **0 wierszy** |

Ostatni test jest najważniejszy i najmniej oczywisty:

```sql
SELECT a.customer_id
FROM aurora_<inicjaly>.gold_dim_customer_scd a
JOIN aurora_<inicjaly>.gold_dim_customer_scd b
  ON a.customer_id = b.customer_id
 AND a.customer_sk <> b.customer_sk
 AND a.valid_from <= b.valid_to
 AND b.valid_from <= a.valid_to;
```

Zapytanie szuka dwóch wersji tego samego klienta, których okresy ważności
na siebie zachodzą. Gdyby cokolwiek zwróciło, ten sam wiersz faktu pasowałby
do dwóch wersji wymiaru i przychód policzyłby się podwójnie — a suma kontrolna
z W9 przestałaby wychodzić.

**Pytanie do przemyślenia:** fakt z 15 stycznia ma wskazywać na stary czy nowy
`customer_sk`? Warto uzasadnić przez pryzmat raportu „przychód według miast
za styczeń”, wysłanego zarządowi 31 stycznia i otwartego ponownie w marcu.

---

## W7. Ładowanie przyrostowe — 20 stycznia (40 min)

**Cel:** dołożenie nowego dnia bez przeliczania całości i bez zepsucia liczb,
które już raz wyszły.

**Incremental load** to dołożenie nowego okresu do istniejącego modelu.
Wymaganie, które o nim rozstrzyga, to **idempotencja**: ten sam skrypt uruchomiony
dwa razy ma dać ten sam stan, a nie podwojone dane. Potok, który tego nie
gwarantuje, przy każdym ponowieniu po awarii cicho psuje raport — a ponowienia
po awarii są normą, nie wyjątkiem.

### Kolejność kroków

Ta sama, co w każdym ELT: najpierw źródło, potem wymiary, na końcu fakt.

| # | Warstwa | Co powstaje | Uwaga |
|---|---|---|---|
| 1 | Bronze | partycja `dt=2026-01-20` w `bronze_orders` | rejestracja przez `MSCK REPAIR` |
| 2 | Bronze | `bronze_order_items_20260120` | osobna tabela na osobnym prefiksie |
| 3 | Gold | `gold_dim_date_with_20` | wymiar daty z trzecim dniem |
| 4 | Silver | `silver_orders_20260120` | tylko nowy dzień, bez ruszania 15 i 16 |
| 5 | Gold | `gold_fact_order_item_20260120` | fakt nowego dnia |
| 6 | Gold | widok `v_fact_order_item_all` | stary fakt `UNION ALL` nowy |

### Krok 1 — partycja w Bronze

Pliki są już na S3, ale katalog Glue jeszcze o nich nie wie:

```sql
MSCK REPAIR TABLE aurora_<inicjaly>.bronze_orders;
```

To ta sama instrukcja, co w L3 dnia 1, i z tego samego powodu: katalog nie
przeszukuje S3 samoczynnie. Bez niej `SELECT COUNT(*) ... WHERE dt = '2026-01-20'`
zwróci **0** przy poprawnie wgranych plikach. Po niej `bronze_orders` ma 240
wierszy w trzech partycjach.

Wariant punktowy, szybszy przy tysiącach partycji, jest w ściągawce:
`ALTER TABLE ... ADD IF NOT EXISTS PARTITION (dt = '2026-01-20') LOCATION ...`.

### Krok 2 — pozycje nowego dnia

Nowa tabela zewnętrzna `bronze_order_items_20260120` na prefiksie
`bronze/order_items_20260120/`, o dokładnie tej samej strukturze kolumn co
`bronze_order_items`. Powód osobnej tabeli jest opisany wyżej, w sekcji o danych
wejściowych: wspólny prefiks zepsułby liczbę 486.

Kontrola: **40 wierszy**, a `bronze_order_items` nadal **486**.

### Krok 3 — wymiar daty

`gold_dim_date` zna tylko 15 i 16 stycznia. Fakt nowego dnia łączy się z wymiarem
daty po `order_date = date_actual`, więc bez wiersza dla 20 stycznia `INNER JOIN`
wytnie **cały** nowy dzień — po cichu, bez błędu, z wynikiem zero wierszy zamiast
czterdziestu. To najczęstsza wpadka przy pierwszym ładowaniu przyrostowym i warto
ją zobaczyć celowo: fakt zbudowany przed rozszerzeniem wymiaru daty jest pusty.

Tabela zewnętrzna nie przyjmuje `INSERT`-a — na S3 można dopisać plik, nie da się
zmienić tabeli. Powstaje więc nowa wersja wymiaru, `gold_dim_date_with_20`, jako
`UNION ALL` starej tabeli i jednego wiersza literałów:

```sql
SELECT * FROM aurora_<inicjaly>.gold_dim_date
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

Typy po obu stronach `UNION ALL` muszą się zgadzać co do kolumny. `CAST(...
AS varchar)` przy `day_name` nie jest ozdobnikiem: literał tekstowy w Trino ma typ
`char(7)`, a kolumna z `date_format()` jest `varchar` — bez rzutowania Athena
odpowiada błędem o niezgodności typów w `UNION`.

To jest zarazem argument za tym, żeby w produkcji wymiar daty generować raz,
na kilka lat w przód, i nigdy go nie dokładać.

### Krok 4 i 5 — Silver i fakt nowego dnia

`silver_orders_20260120` powstaje z `bronze_orders` filtrowanego po
`dt = '2026-01-20'` i po statusie, z tymi samymi rzutowaniami typów co w L5
dnia 1. Dni 15 i 16 nie są przeliczane.

`gold_fact_order_item_20260120` łączy pozycje nowego dnia z wymiarami. Pięć
złączeń, tak jak wczoraj, ale dwa z nich wyglądają inaczej: wymiar daty to
`gold_dim_date_with_20`, a wymiar klienta dochodzi po dwóch warunkach naraz.

### Złączenie z wymiarem SCD2

Nowe fakty łączą się z klientem po **dwóch warunkach naraz**:

```sql
JOIN aurora_<inicjaly>.gold_dim_customer_scd c
  ON o.customer_id = c.customer_id
 AND o.order_date BETWEEN c.valid_from AND c.valid_to
```

Nie po `is_current`. Dla 20 stycznia obie drogi dają ten sam wynik, bo tego dnia
wersje nowe są zarazem aktualne — ale tylko pierwsza zadziała, gdy trzeba będzie
przeliczyć starszy dzień po awarii albo po zmianie w logice. Złączenie po
`is_current` przypisałoby wtedy styczniowe zamówienia do dzisiejszych miast, czyli
zrobiłoby dokładnie to, przed czym SCD2 ma chronić.

Ten warunek jest zarazem miejscem, w którym mści się `NULL` w `valid_from`
albo `valid_to`. Fakt krótszy niż 40 wierszy to pierwsze miejsce do sprawdzenia.

### Dwa sposoby na idempotencję

| | **Tabela zewnętrzna (CTAS)** | **Iceberg (`MERGE INTO`)** |
|---|---|---|
| Powtórne uruchomienie | `IF NOT EXISTS` pomija tabelę — dane się **nie** przeliczają | `MERGE` przelicza i nie dubluje |
| Realne przeliczenie dnia | `DROP TABLE` **plus** ręczne usunięcie prefiksu w S3 | ta sama instrukcja jeszcze raz |
| Układ tabel | osobna tabela na każdy dzień, sklejone widokiem `UNION ALL` | jedna tabela na wszystko |
| Co się psuje przy pominięciu kroku | `HIVE_PATH_ALREADY_EXISTS` przy ponownym CTAS | nic |

Wariant pierwszy to idempotencja **na piechotę** i warto ją nazwać po imieniu,
zamiast udawać, że katalog plików na S3 potrafi `MERGE`. Athena nie zna `DELETE`
ani `UPDATE` na tabeli zewnętrznej, bo na S3 jedyną dostępną operacją jest
„dopisz plik”.

Wariant drugi wymaga tabeli Iceberg z zadania dodatkowego S1 dnia 1 (w Athenie engine v3
przechodzi bez problemów). Warto wykonać oba i zestawić, ile pracy kosztuje
powtórzenie każdego z nich — to jest najkrótsze uzasadnienie, po co w ogóle
powstały formaty lakehouse.

### Jak czytać MERGE

```sql
MERGE INTO aurora_<inicjaly>.silver_orders_iceberg t
USING aurora_<inicjaly>.silver_orders_20260120 s
  ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  status  = s.status,
  channel = s.channel
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, order_ts, order_date,
                              status, channel, delivery_city, dt)
     VALUES (s.order_id, s.customer_id, s.order_ts, s.order_date,
             s.status, s.channel, s.delivery_city, s.dt);
```

Instrukcja przechodzi po wierszach źródła (`USING`) i dla każdego sprawdza warunek
`ON`:

- wiersz **pasuje** do istniejącego w tabeli docelowej → gałąź `WHEN MATCHED`,
  czyli aktualizacja wymienionych kolumn
- wiersz **nie pasuje** → gałąź `WHEN NOT MATCHED`, czyli wstawienie nowego

I tu jest pointa dnia: 170 wierszy + 40 nowych = **210**, a po **drugim**
uruchomieniu dokładnie tej samej instrukcji nadal **210** — bo za drugim razem
wszystkie czterdzieści wierszy trafia w gałąź `WHEN MATCHED`. Ta sama operacja
na tabeli zewnętrznej wymaga `DROP TABLE`, wejścia do konsoli S3 i usunięcia
prefiksu.

**Skrót `INSERT *` w Athenie nie działa.** Zapis
`WHEN NOT MATCHED THEN INSERT *` pochodzi z Delta Lake i Sparka; Athena engine v3
odpowiada `mismatched input '*'. Expecting: '(', 'VALUES'`. Kolumny trzeba
wypisać — obie listy, i tę w `INSERT`, i tę w `VALUES`.

Warto po `MERGE` zajrzeć w listę snapshotów tabeli Iceberg
(`"…"."silver_orders_iceberg$snapshots"`). Po CTAS z dnia 1 był tam jeden wpis
z operacją `append`; po `MERGE` dochodzi drugi, `overwrite`. Stary snapshot
zostaje dostępny — i to jest cały mechanizm *time travel* pokazany na jednym
zapytaniu.

### Kontrola

| Co | Czego oczekujemy |
|---|---|
| `silver_orders_20260120` | **40** — wszystkie zamówienia z 20.01 mają status `paid` lub `shipped` |
| `gold_fact_order_item_20260120` | **40** wierszy, przychód **101 546,00** |
| `v_fact_order_item_all` wg `dt` | 15.01 i 16.01 bez zmian, 20.01 = 40; razem **449** |
| `gold_fact_order_item` | nadal **409** — stary fakt jest nietknięty |
| `silver_orders_iceberg` po `MERGE` | **210**, i **210** także po drugim uruchomieniu |

Nowy dzień ma dokładnie 40 pozycji, bo każde z czterdziestu zamówień ma jedną
pozycję i żadne nie jest anulowane. Dzień 1 był pod tym względem trudniejszy —
tam z 486 pozycji zostało 409.

**Pytanie do przemyślenia:** widok `v_fact_order_item_all` skleja dwie tabele.
Co się stanie po dołożeniu dnia 21, 22 i 23 stycznia — i po ilu dniach ten wzorzec
przestanie być znośny? Warto zestawić to z kosztem jednorazowego przejścia
na Iceberg.

---

## W8. Most między data lake a modelem wymiarowym (20 min)

**Cel:** jedno zapytanie, które odpowiada na pytanie biznesowe, sięgając naraz
po dane modelowane i niemodelowane.

**Lakehouse** to nazwa na architekturę, w której jeden silnik SQL czyta zarówno
uporządkowane tabele modelu wymiarowego, jak i surowe pliki data lake — bez
kopiowania jednych do drugich. U nas ten silnik to Athena, a dwa światy wyglądają
tak:

| | **Gold** | **Bronze** |
|---|---|---|
| Co to jest | `gold_fact_order_item` i wymiary | `bronze_events` |
| Format | Parquet, kolumnowy, ze schematem | JSON Lines, półstrukturalny |
| Kiedy powstał schemat | przy zapisie (CTAS) | przy odczycie (DDL nałożony na gotowe pliki) |
| Kto go zaprojektował | my, w dniu 2 | nikt — to log ze sklepu |

Zdarzenia (`page_view`, `add_to_cart`, `purchase`, `search`) to dane, które
w klasycznym data warehouse najczęściej w ogóle by nie wylądowały: w momencie
zbierania logów nikt nie wiedział, że powstanie pytanie o porzucone koszyki.

### Pytanie biznesowe

**Która kategoria produktów ma dużo koszyków, a mało przychodu?** To jest pytanie
o słabą konwersję i nie da się na nie odpowiedzieć z samego modelu — model wie
tylko o tym, co zostało kupione.

### Konstrukcja

Trzy elementy do złożenia:

1. liczba zdarzeń `add_to_cart` w rozbiciu na `product_id` — z `bronze_events`
2. przychód w rozbiciu na `product_id` i kategorię — z faktu złączonego
   z `gold_dim_product`
3. złączenie obu i agregacja do poziomu kategorii

Kluczowy szczegół: po stronie zdarzeń są produkty, **których nikt nie kupił**.
Przy `INNER JOIN` zniknęłyby razem z informacją o porzuconych koszykach — a to
właśnie one są odpowiedzią na pytanie. Stąd `LEFT JOIN` od strony koszyków
i `COALESCE` na przychodzie, żeby brak sprzedaży dał zero, a nie `NULL`.

Kategoria dla takich produktów musi przyjść skądinąd niż z faktu — najbliżej jest
`silver_products` albo `gold_dim_product` złączony po `product_id`.

### Kontrola

Suma kolumny z koszykami to **184** — dokładnie tyle, ile jest zdarzeń
`add_to_cart` w `bronze_events`. Wynik posortowany malejąco po koszykach zaczyna
się od **Akcesoriów: 39 koszyków, 13 423,00 przychodu**, a suma kolumny
z przychodem to 1 427 124,00.

Odpowiedzią są Akcesoria: najwięcej koszyków w całym zestawieniu i przychód
o rząd wielkości niższy niż Telefony, które mają koszyków mniej.

### Wariant dla Redshifta

Ten sam pomysł da się zrealizować na dwóch silnikach naraz: fakt lokalnie
w schemacie `gold_<inicjaly>`, zdarzenia prosto z data lake. Dane zdarzeń zostają
na S3, a Redshift dokłada je do zapytania w locie. Nazwy lokalne i katalogowe
mieszają się w jednym `FROM` bez żadnego problemu — to jest ten sam lakehouse,
tylko oglądany z drugiej strony.

Drogi są dwie i różnią się **uprawnieniami, nie składnią SQL** — dokładnie tak,
jak w W5 z dnia 2:

| Droga | Zapis | Kto czyta S3 | Typowe zachowanie |
|---|---|---|---|
| zamontowany katalog Glue | `"awsdatacatalog"."aurora_<inicjaly>"."bronze_events"` | tożsamość zalogowanego użytkownika | działa z `PowerUserAccess` |
| jawny Redshift Spectrum | `CREATE EXTERNAL SCHEMA … FROM DATA CATALOG … IAM_ROLE …` | rola IAM namespace'u | wymaga uprawnień S3 na roli (to samo ograniczenie co przy `COPY`) |

Żadna z tych dróg nie jest do W8 potrzebna — całe ćwiczenie dzieje się w Athenie.
Warto je znać jako fakt architektoniczny.

**Pytanie do przemyślenia:** liczba koszyków pochodzi z całych dwóch dni,
a przychód również. Co by się zmieniło w tym zestawieniu, gdyby zdarzenia
i sprzedaż liczyć osobno dla każdego dnia — i czy odpowiedź na pytanie
biznesowe byłaby ta sama?

---

## W9. Zadanie końcowe — pytania dyrektora handlowego (25 min)

**Cel:** sprawdzenie, czy model mówi prawdę, gdy ktoś zmieni adres.

Cztery pytania, wyłącznie na tym, co już istnieje. To nie są nowe techniki —
to sprawdzian, czy poprzednie trzy ćwiczenia zostały złożone poprawnie.

1. **Przychód netto 15–16 stycznia według miasta, w którym klient mieszkał
   w momencie zakupu.** Miasto pochodzi z wersji wymiaru wskazywanej przez
   `customer_sk` zapisany w faktcie.
2. **Ten sam przychód według miasta aktualnego**, czyli po przeprowadzce.
   Droga jest dłuższa: z faktu do wersji historycznej po `customer_sk`, z niej
   do `customer_id`, a z `customer_id` do wersji z `is_current = true`.
3. **Czy wyniki 1 i 2 się różnią?** Dla których miast i o ile?
4. **Jaki procent klientów, którzy 16 stycznia dodali coś do koszyka, nie ma
   tego dnia zakupu?**

### Co warto zapisać przy każdej odpowiedzi

Przy punktach 1–3 jedno zdanie o tym, **na które pytanie biznesowe odpowiada
dany wariant**. Oba są poprawne — pod warunkiem, że wiadomo, o który zapytał
dyrektor. „Jak nam szło w styczniu w Krakowie” i „ile dzisiejsi klienci
z Krakowa kupili w styczniu” to dwa różne pytania i dwie różne liczby.

Przy punkcie 4 jedno zdanie o **grain**: zbiór „kupił” pochodzi z zamówień,
nie ze zdarzeń `purchase`. Porównanie zdarzeń ze zdarzeniami dałoby 0%, bo byłoby
porównaniem zbioru z samym sobą.

### Kontrola

| Pytanie | Czego oczekujemy |
|---|---|
| 1 i 2 | suma obu wariantów identyczna: **1 427 124,00** |
| 3 | różnice w **siedmiu** miastach; największa strata **Kraków −40 538,00**, największy zysk **Białystok +44 298,00** |
| 3 | dalej: Katowice **+32 304,00**, Warszawa **+10 847,00**, Lublin **−10 847,00**, Gdańsk **−13 570,00**, Wrocław **−22 494,00** |
| 4 | **55** klientów z `add_to_cart` 16 stycznia, z tego **14 bez zakupu**, czyli **25,5%** |

Suma różnic z punktu 3 wynosi zero i to nie jest przypadek: przychód się nie
zmienia, zmienia się wyłącznie to, komu go przypisujemy. **Kraków „traci”
40 tysięcy, których nigdy nie stracił.**

Gdyby wyniki 1 i 2 wyszły identyczne, złączenie w punkcie 1 poszło po
`is_current` już na poziomie faktu — a to kasuje całą wartość SCD2 i zamienia
Type 2 z powrotem w Type 1.

Poznań nie pojawia się na liście różnic, mimo że dwoje klientów się z niego
wyprowadziło. Powód jest prozaiczny: ci dwaj klienci nie złożyli 15 ani 16 stycznia
żadnego opłaconego zamówienia, więc nie ma czego przenosić.

**Pytanie do przemyślenia:** dyrektor prosi o raport „przychód według miasta
za styczeń, według dzisiejszej struktury klientów”, a za miesiąc o ten sam raport
jeszcze raz. Czy obie wersje pokażą te same liczby — i co to znaczy dla zdania
„raport historyczny się nie zmienia”?

---

## W10. Sprzątanie (15 min)

**Cel:** zerowy rachunek i konto w stanie, w jakim zastaliśmy je w dniu 1.

W chmurze każde pozostawione środowisko generuje bieżący koszt. Redshift
Serverless nalicza RPU przy aktywności (a snapshoty przez całą dobę); S3 nalicza
za każdy GB przez całą dobę; wyniki zapytań Atheny zostają na S3 jako pliki CSV
i też zajmują miejsce. W środowisku wieloosobowym nieposprzątane zasoby zostają
na wspólnym rachunku i w katalogu Glue.

Całą trudnością tego ćwiczenia jest zakres: **wyłącznie własne zasoby, bez
ruszania cudzych prefiksów i cudzych baz.** Reszta to kilka kliknięć w konsoli —
i jednocześnie najlepszy test tego, czy wiadomo, co przez trzy dni powstało
i gdzie to leży. Kto potrafi rozłożyć własne środowisko po elementach, ten je
rozumie; w pracy to jest ta sama umiejętność, tylko droższa w razie pomyłki.

### Kolejność ma znaczenie

| # | Usługa | Do usunięcia | Dlaczego w tej kolejności |
|---|---|---|---|
| 1 | Redshift Serverless | najpierw **workgroup**, potem **namespace** | namespace nie da się usunąć, dopóki wisi na nim workgroup; workgroup to moc obliczeniowa i to on generuje rachunek |
| 2 | Athena / Glue | `DROP DATABASE aurora_<inicjaly> CASCADE` | tabele znikają z katalogu, ale **pliki na S3 zostają** — dlatego S3 jest po tym, a nie przed |
| 3 | S3 | opróżnienie, a potem usunięcie bucketu `aurora-<inicjaly>-lake` | bucket z choćby jednym obiektem nie da się usunąć |
| 4 | S3 | prefiks `athena-results/` | znika razem z bucketem; osobna pozycja tylko wtedy, gdy wyniki szły do cudzego bucketu |

### Redshift

Konsola **Amazon Redshift** → **Redshift Serverless** → *Serverless dashboard*.

Workgroup: strona workgroupu → *Actions* → **Delete**. Konsola prosi o wpisanie
nazwy workgroupu jako potwierdzenie — to standardowe zabezpieczenie AWS przed
usunięciem nie tego zasobu.

Namespace: to samo miejsce, *Actions* → **Delete**. Tutaj dochodzi pytanie
**„Create final snapshot”**, domyślnie zaznaczone. Snapshot jest po to, żeby dane
przeżyły infrastrukturę — przy migracji albo przy tymczasowym wyłączeniu
środowiska. Na kursie jest zbędny i sam kosztuje za przechowywanie, więc
odznaczenie tego pola jest tu świadomą decyzją, a nie skrótem.

`default-workgroup` oraz inne wcześniej istniejące workgroupy w koncie **zostają** — usuwamy wyłącznie własne zasoby z prefiksem `aurora-<inicjaly>-`.

### Athena

```sql
DROP DATABASE aurora_<inicjaly> CASCADE;
```

`CASCADE` znaczy „razem ze wszystkimi tabelami w środku”. Bez niego Athena
odmawia usunięcia niepustej bazy. Tabele w tej bazie są w większości
**zewnętrzne**, więc kasowany jest wyłącznie opis w Glue Data Catalog — pliki
Parquet i CSV zostają na S3 do kroku 3.

Jeden wyjątek: `silver_orders_iceberg` z zadania S1 **nie jest** tabelą
zewnętrzną (`is_external = false`), więc jej pliki Athena skasuje razem z tabelą.
Ta asymetria jest dobrym pytaniem kontrolnym i wynika wprost z tego, kto zarządza
plikami.

Lista baz w Athenie jest długa — na koncie zostało kilkadziesiąt baz Glue
po poprzednich grupach. Filtr i wpisane `aurora` skraca ją do własnych. Nazwa
bazy w instrukcji `DROP` to jedyne miejsce dzisiaj, w którym literówka jest
nieodwracalna.

### S3

Bucket → zakładka **Objects** → *Empty*. Konsola prosi o wpisanie słowa
`permanently delete` — po tym potwierdzeniu obiekty znikają bez kosza.
Dopiero pusty bucket da się usunąć: lista bucketów → zaznaczenie → *Delete*
→ wpisanie nazwy bucketu.

Przy dużej liczbie obiektów operacja *Empty* trwa i pokazuje pasek postępu;
alternatywą w produkcji jest reguła lifecycle, która kasuje obiekty starsze
niż X dni.

Gdyby na koncie był wspólny bucket demo albo cudze prefiksy w tym samym
bucketcie — zostają nietknięte. Warto zanotować, co zostaje świadomie, i podać
to trenerowi na koniec.

### Kontrola

| Gdzie | Czego oczekujemy |
|---|---|
| S3 → *Buckets* | brak bucketu `aurora-<inicjaly>-lake` |
| Athena → lista baz | brak bazy `aurora_<inicjaly>` |
| Redshift → *Serverless dashboard* | brak workgroupu i namespace'u z własnymi inicjałami |
| Redshift → *Snapshots* | brak snapshotu z własną nazwą |

**Pytanie do przemyślenia:** `DROP DATABASE ... CASCADE` skasował opis tabel,
a pliki zostały na S3. Gdyby ktoś zatrzymał się na tym kroku i wyszedł z sali —
co dokładnie kosztowałoby dalej i ile trzeba by zrobić, żeby te dane odzyskać?

---

## Zadania dodatkowe (dla tych, którzy skończą wcześniej)

Numeracja `S…` kontynuuje zadania dodatkowe z dnia 1, gdzie skończyła się
na S1 (Iceberg) i S2 (lejek koszykowy).

### S3. Type 1 obok Type 2

Zbudowanie `gold_dim_customer_scd1` — tego samego wymiaru, ale z historią
nadpisaną: 80 wierszy, każdy z aktualnym miastem. Następnie policzenie przychodu
za styczeń według miast na obu wymiarach.

Wynik z Type 1 jest identyczny z wariantem „as-is” z W9, a różni się od „as-was”
o te same siedem miast. Wniosek do sformułowania jednym zdaniem: Type 1 nie jest
uproszczeniem Type 2 — to inna odpowiedź na inne pytanie, tyle że podana bez
możliwości wyboru.

### S4. Fakt na tabeli Iceberg

Przeniesienie `gold_fact_order_item` do tabeli Iceberg i dołożenie dnia 20.01
przez `MERGE` zamiast osobnej tabeli i widoku. Klucz złączenia musi być tu
złożony: `order_id` sam nie wystarczy, bo grain faktu to **pozycja** zamówienia,
a jedno zamówienie ma ich średnio 2,4.

To ćwiczenie pokazuje różnicę między idempotencją po **natural key** a idempotencją
po partycji — i to, dlaczego wybór grain z dnia 2 wraca w tym miejscu jako decyzja
techniczna.

---

## Po kursie

Trzy rzeczy, które zostają po tych dziewięciu godzinach, nawet gdy konto już
jest posprzątane:

1. **Model.** Grain, wymiary, surrogate key i SCD Type 2 nie zależą od AWS —
   te same decyzje wyglądają tak samo w Snowflake, BigQuery i w bazie
   on-premise.
2. **Nawyk liczb kontrolnych.** 486 → 409 → 449 i 1 427 124,00 przez trzy dni
   i dwa silniki. Rekoncyliacja po każdym przeładowaniu jest tańsza niż tłumaczenie
   biznesowi, skąd wzięła się różnica.
3. **Pliki.** Katalogi `sql/` z trzech dni razem ze ściągawką składają się
   na kompletny, działający potok ELT — od `CREATE EXTERNAL TABLE` na CSV
   po `MERGE` na tabeli Iceberg.
