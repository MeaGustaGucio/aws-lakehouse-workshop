# Dzień 1 — Data Lakes (zadania)

**Czas:** 3 godziny  
**Gdzie:** AWS Console · konto warsztatowe / sandbox AWS · S3 + Athena  
**Prefix (izolacja zasobów):** `aurora-<twoje_inicjaly>-`, np. `aurora-jk-lake`

W środowisku wieloosobowym każdy zasób dostaje własne inicjały w nazwie — pozwala to
odróżnić własne tabele od cudzych, a przy sprzątaniu uniknąć skasowania cudzej pracy
(a w S3 zapewnia globalną unikalność nazwy bucketu).

Bucket zostaje po zajęciach — dzień 2 czyta dokładnie te same dane.

Składnia SQL, której będziemy używać, jest zebrana w `../00_Sciagawka_SQL.md`.
Warto mieć ten plik otwarty obok.

Rozwiązania są w tym samym folderze. `02_rozwiazania.md` to jedna ścieżka czytania
„jak”: zdanie, co powstało, pełny SQL i liczby z etykietą. Katalog `sql/` trzyma
**ten sam** SQL, jeden plik na ćwiczenie (`sql/L3_bronze_ddl.sql`, `sql/L4_pytania_biznesowe.sql`,
`sql/L5_silver_ctas.sql`, `sql/L6_porownanie_skanu.sql`, `sql/S1_tabela_iceberg.sql`,
`sql/S2_lejek_koszyk.sql`). Warto zaglądać tam **po** własnej próbie.

---

## L0. Wejście do konsoli (10 min)

1. Logowanie do AWS Management Console (np. przez IAM Identity Center na koncie warsztatowym lub własny sandbox AWS).
2. Rola **PowerUserAccess** wystarczy na cały dzień 1.
   Obok bywa `AdministratorAccess` — nie jest potrzebna, a w środowisku wieloosobowym im mniej
   uprawnień, tym mniej przypadkowych szkód.
3. Region w prawym górnym rogu: **`eu-central-1` (Frankfurt)**.
   Wszystkie usługi używane w warsztacie działają w jednym regionie. S3, Glue i Athena są regionalne — bucket
   utworzony we Frankfurcie nie pojawi się na liście, gdy konsola pokazuje Irlandię.
   To najczęstsza przyczyna „zniknął mi bucket”.
4. Do zanotowania: region, własny prefix, Account ID (np. `123456789012`).

---

## L1. Bucket na data lake (15 min)

**Cel:** miejsce, w którym wyląduje cały data lake.

S3 → *Create bucket*. Ekran ma kilkanaście opcji; poniżej te, które zmieniamy,
i te, które zostają domyślne — razem z powodem.

### Ustawiamy

| Opcja | Wartość | Dlaczego |
|---|---|---|
| Bucket name | `aurora-<inicjaly>-lake` | nazwy bucketów są **globalne** w całym AWS |
| AWS Region | `eu-central-1` | ten sam, co konsola i Athena |

Jeśli pojawi się `BucketAlreadyExists`, nazwa jest zajęta przez kogoś na świecie.
Wtedy dopisujemy numer konta: `aurora-<inicjaly>-lake-123456789012`. Ta sama nazwa
musi potem wejść do każdego `LOCATION` w SQL.

### Zostawiamy domyślne — i po co te opcje istnieją

- **Bucket type: General purpose.** Druga opcja, *Directory bucket*, to S3 Express
  One Zone: dane w jednej strefie dostępności, kilkukrotnie niższe opóźnienia, inny
  model nazewnictwa i wyższa cena za GB. Sensowna przy tysiącach drobnych odczytów
  na sekundę, nie przy analityce wsadowej.
- **Object Ownership: ACLs disabled.** ACL to stary mechanizm uprawnień „per obiekt”,
  sprzed polityk IAM. AWS rekomenduje wyłączenie od 2021 roku. ACL włącza się dziś
  praktycznie tylko wtedy, gdy pliki wrzuca do naszego bucketu inne konto AWS
  i chce zachować własność obiektów.
- **Block Public Access: wszystkie cztery zaznaczone.** Zdejmuje się je świadomie
  przy hostowaniu statycznej strony z S3. Data lake z danymi klientów nigdy nie
  powinien być publiczny — a odznaczenie tego pola to najczęstsza przyczyna wycieków
  danych z S3, o jakich czyta się w mediach.
- **Bucket Versioning: Disable.** Włączone wersjonowanie trzyma każdą nadpisaną
  i skasowaną wersję obiektu, co ratuje po błędnym `DELETE` — ale na kursie
  podwoiłoby liczbę obiektów do posprzątania i utrudniło ponowne uruchomienie CTAS.
  W produkcyjnym data lake zwykle jest włączone, razem z regułą lifecycle,
  która stare wersje kasuje po X dniach.
- **Default encryption: SSE-S3 (Amazon S3 managed keys).** Szyfrowanie w spoczynku
  jest włączone zawsze i nic nie kosztuje. Alternatywa SSE-KMS daje własny klucz,
  osobny audyt użycia i możliwość odcięcia dostępu przez politykę klucza, ale kosztuje
  za każde żądanie i wymaga, żeby Athena oraz rola Redshiftu miały `kms:Decrypt`.
  To częsty powód „Access Denied”, mimo poprawnych uprawnień do S3.
- **Bucket Key: Enable.** Ma znaczenie tylko przy SSE-KMS — ogranicza liczbę wywołań
  KMS i przez to rachunek. Przy SSE-S3 nie robi nic.
- **Tags.** Puste. W firmie służą do rozliczania kosztów (`cost-center`, `owner`)
  i do polityk IAM warunkowanych tagiem.
- **Object Lock: Disable.** Tryb WORM — obiektu nie da się skasować przed upływem
  retencji, nawet z konta root. Wymóg regulacyjny w finansach, na kursie zablokowałby
  sprzątanie.

### Folderów nie tworzymy ręcznie

Struktura `bronze/`, `silver/`, `gold/` powstanie sama przy uploadzie i przy pierwszym
CTAS. Na S3 „folder” nie jest bytem — to tylko prefiks w nazwie klucza. Obiekt
`bronze/customers/customers.csv` ma jedną nazwę, a konsola rysuje z niej drzewko.

**Pytanie do przemyślenia:** skoro Bronze, Silver i Gold to dla S3 wyłącznie fragment
nazwy pliku i nic ich technicznie nie odróżnia, to co realnie daje ich rozdzielenie?
Inaczej: co się psuje w zespole, gdy wszystkie pliki leżą obok siebie w jednym miejscu?
Odpowiedź zbierzemy wspólnie po L2.

---

## L2. Bronze — upload danych Aurory (10 min)

Dane są już poukładane dokładnie tak, jak mają leżeć na S3. Nie ma potrzeby
odtwarzania struktury klikaniem.

W folderze `dane/` jest jeden katalog `bronze`. Poniżej jego zawartość
w kolejności, w jakiej będziemy z niej korzystać — najpierw słowniki, potem
zamówienia, na końcu strumień zdarzeń:

```text
bronze/
  customers/customers.csv                     80 klientów
  products/products.csv                       25 produktów
  orders/dt=2026-01-15/orders.csv             90 zamówień
  orders/dt=2026-01-16/orders.csv            110 zamówień
  order_items/order_items.csv                486 pozycji
  events/dt=2026-01-15/events.json           zdarzenia z 15.01
  events/dt=2026-01-16/events.json           zdarzenia z 16.01
```

Eksplorator plików i konsola S3 posortują te katalogi alfabetycznie — to normalne,
kolejność katalogów nie ma dla S3 żadnego znaczenia.

**Do zrobienia:** wchodzimy do bucketu, *Upload* → *Add folder* → wskazujemy `bronze`
→ *Upload*. Konsola przeciągnięty katalog odwzorowuje 1:1 na prefiksy, więc siedem
plików ląduje we właściwych miejscach za jednym razem.

Na ekranie uploadu też jest kilka opcji; wszystkie zostają domyślne:

- **Storage class: Standard.** Klasy Glacier są tańsze za GB, ale odczyt wymaga
  przywrócenia obiektu (minuty do godzin) — Athena nie odczyta ich w locie.
  *Intelligent-Tiering* ma sens dla data lake, gdzie część danych stygnie: AWS sam
  przenosi rzadko czytane obiekty do tańszej warstwy.
- **Server-side encryption: Use bucket settings.** Dziedziczymy SSE-S3 z L1.
- **Checksums / Additional checksums.** Domyślnie S3 liczy sumę kontrolną sam.
  Jawny wybór (np. SHA-256) przydaje się, gdy trzeba udowodnić integralność przesyłu.

### Kontrola

Po uploadzie w bucketcie widać `bronze/` z pięcioma podkatalogami, a wewnątrz
`orders/` i `events/` są katalogi `dt=2026-01-15` i `dt=2026-01-16`.

Zapis `dt=` nie jest ozdobą — to konwencja Hive, którą Athena rozpoznaje jako
**partycję**. Wrócimy do niej w L3.

**Pytanie do przemyślenia:** co by się zmieniło, gdyby oba dni zamówień trafiły
do jednego pliku `orders.csv` bez katalogów `dt=`? Wynik zapytania byłby ten sam —
co więc tracimy?

---

## L3. Athena — schema-on-read (25 min)

**Cel:** te same pliki CSV i JSON, które przed chwilą wgraliśmy, zaczynają być
tabelami SQL — bez przenoszenia i bez przepisywania danych.

### Czym jest Query editor

**Amazon Athena** to silnik SQL bez serwera: nie ma klastra do postawienia ani bazy,
do której trzeba ładować dane. Athena czyta pliki leżące na S3 i płaci się za liczbę
zeskanowanych bajtów.

**Query editor** to okno SQL Atheny w konsoli AWS — miejsce, gdzie wpisujemy
zapytania, oglądamy wyniki i podgląd listy tabel. Odpowiednik DBeavera czy pgAdmina,
tyle że w przeglądarce i bez konfiguracji połączenia.

Definicje tabel nie trafiają do Atheny, tylko do **AWS Glue Data Catalog** — wspólnego
katalogu metadanych (jakie są bazy, tabele, kolumny, gdzie na S3 leżą pliki).
Dlatego tę samą tabelę zobaczy potem Redshift i Spark. Athena jest silnikiem,
Glue jest spisem treści.

### Wejście

Athena po otwarciu pokazuje ekran powitalny z dwiema opcjami:

- *Query your data in Amazon SageMaker Unified Studio* — nowe, szersze środowisko
  do danych i AI. Nie korzystamy z niego; wymaga osobnej konfiguracji domeny.
- *Query your data in Athena console* — klasyczny Query editor. **Ta opcja.**

Trzeba zaznaczyć drugą pozycję, bo domyślnie wybrana jest pierwsza i przycisk na dole
pokazuje wtedy *Open Amazon SageMaker Unified Studio*. Po zaznaczeniu przycisk zmienia
się na *Launch query editor*.

Konta, na których Athena była już używana, wchodzą do edytora od razu, z pominięciem
tego ekranu.

### Query result location

Athena każdy wynik zapisuje jako plik CSV na S3 i dopiero potem pokazuje go w tabelce.
Bez wskazanego miejsca zapisu pierwsze zapytanie kończy się błędem
**„No output location provided”**. Na świeżym koncie AWS workgroup `primary` tej lokalizacji
nie ma ustawionej, więc dotyczy to pierwszego uruchomienia.

Zakładka **Query settings** → przycisk **Manage** → pole *Location of query result*:

```text
s3://aurora-<inicjaly>-lake/athena-results/
```

Pozostałe pola w tym oknie zostają puste lub domyślne:

- **Expected bucket owner** — Account ID właściciela bucketu. Zabezpiecza przed
  zapisaniem wyników do bucketu, który w międzyczasie zmienił właściciela. Ma sens
  w konfiguracjach wielokontowych.
- **Assign bucket owner full control over query results** — dotyczy wyłącznie
  bucketów z włączonymi ACL i zapisu między kontami. U nas ACL są wyłączone.
- **Encrypt query results** — pozwala zaszyfrować *same wyniki* innym kluczem niż
  reszta bucketu (np. SSE-KMS). Bez zaznaczenia wyniki i tak dziedziczą szyfrowanie
  bucketu, czyli SSE-S3.

Sekcja **Execution controls** obok pozostaje pusta. *Data scanned limit* potrafi
przerwać zapytanie, które chce przeskanować więcej niż podany limit — to najprostsza
ochrona przed rachunkiem za `SELECT *` na wielu terabajtach. *Min/Max DPU per query*
dotyczy pojemności rezerwowanej i nie działa w trybie rozliczania za skan.

Napis **Workgroup: primary** u góry: workgroup grupuje zapytania i ustawienia —
pozwala rozdzielić zespoły, narzucić im wspólną lokalizację wyników, limit skanu
i osobno raportować koszty. Zostajemy na `primary`.

### Wyłączamy Reuse query results

Nad wynikami jest przełącznik **Reuse query results** (domyślnie włączony, „up to
60 minutes ago”). Athena zwraca wtedy zapisany wynik poprzedniego identycznego
zapytania, zamiast czytać dane jeszcze raz. Oszczędza pieniądze, ale w L4 i L6
porównujemy **Data scanned** — a powtórzone zapytanie pokaże wtedy zero.

Na czas ćwiczeń przełącznik zostaje wyłączony.

### Baza i tabele

```sql
CREATE DATABASE IF NOT EXISTS aurora_<inicjaly>;
```

„Baza” w Athenie to wpis w Glue Data Catalog grupujący tabele. Nie ma w niej
schematów w rozumieniu PostgreSQL, więc warstwy rozdzielamy prefiksem w nazwie
tabeli (`bronze_`, `silver_`, `gold_`).

Do zbudowania jest pięć tabel **zewnętrznych** (`EXTERNAL`) — takich, które opisują
pliki leżące na S3 i nie kopiują danych. `DROP TABLE` na tabeli zewnętrznej kasuje
sam opis, pliki zostają:

| # | Tabela | Źródło | Uwagi |
|---|---|---|---|
| 1 | `bronze_customers` | `bronze/customers/` | CSV z nagłówkiem |
| 2 | `bronze_products` | `bronze/products/` | CSV z nagłówkiem |
| 3 | `bronze_orders` | `bronze/orders/` | CSV, **partycja** `dt string` |
| 4 | `bronze_order_items` | `bronze/order_items/` | CSV z nagłówkiem |
| 5 | `bronze_events` | `bronze/events/` | JSON, **partycja** `dt string` |

Wzorce `CREATE EXTERNAL TABLE` dla CSV i dla JSON są w `../00_Sciagawka_SQL.md`.
Do uzupełnienia zostają: nazwy i typy kolumn (muszą odpowiadać nagłówkowi pliku
co do kolejności) oraz `LOCATION` z własną nazwą bucketu.

### CSV (OpenCSVSerde) vs JSON (JsonSerDe)

To dwa różne mechanizmy odczytu — wybór zależy od formatu pliku, nie od preferencji.

**CSV — `OpenCSVSerde`**

- Plik to wiersze tekstowe rozdzielone przecinkami; SerDe **nie zna typów z nagłówka**.
- Każda kolumna wraca jako **string**, nawet gdy w pliku widać liczby (`1234.56`).
- Dlatego `signup_date`, `order_ts` i podobne pola deklarujemy jako `string` w Bronze.
- Konwersja na `date` / `timestamp` następuje dopiero w Silver (CTAS), gdzie kontrolujemy
  format i odrzucamy błędne wartości.
- W DDL muszą być: `escapeChar`, `STORED AS TEXTFILE` oraz `skip.header.line.count`
  (nagłówek CSV nie może trafić do wyników).

**JSON — `org.openx.data.jsonserde.JsonSerDe`**

- Pliki w `bronze/events/` to **JSON Lines** — jeden obiekt JSON na linię, bez tablicy
  wokół całości i bez przecinków między obiektami.
- JsonSerDe mapuje typy z JSON na typy z DDL: `"product_id": 42` → kolumna `int`,
  `"event_type": "page_view"` → `string`.
- Dlatego w `bronze_events` widać `product_id int`, a w tabelach CSV — same `string`.

**Gdy JsonSerDe zawiedzie:** `CREATE TABLE` przechodzi bez błędu, ale `SELECT` kończy się
`HIVE_CURSOR_ERROR` albo zwraca same `NULL`. Wtedy `DROP TABLE`, podmiana SerDe na
`org.apache.hive.hcatalog.data.JsonSerDe`, ponowne `CREATE` i `MSCK REPAIR`. Pliki na S3
zostają — zmieniasz tylko opis w katalogu.

Warto pisać `CREATE EXTERNAL TABLE IF NOT EXISTS`. Przy drugim uruchomieniu tego
samego skryptu Athena wtedy po prostu pominie istniejącą tabelę, zamiast przerwać
błędem `AlreadyExistsException`.

`LOCATION` wskazuje **katalog, nie plik**. Athena czyta wszystko, co w nim leży.
Ścieżka do pojedynczego pliku jest częstym błędem i kończy się pustą tabelą.

### Partycje

Po utworzeniu tabel partycjonowanych katalog nie wie jeszcze, jakie wartości `dt`
istnieją na S3 — `SELECT COUNT(*)` zwróci **0** mimo poprawnie wgranych plików:

```sql
MSCK REPAIR TABLE aurora_<inicjaly>.bronze_orders;
MSCK REPAIR TABLE aurora_<inicjaly>.bronze_events;
```

`MSCK REPAIR` przechodzi po prefiksach i dopisuje do katalogu każdą znalezioną
partycję. Alternatywy: `ALTER TABLE ... ADD PARTITION` (pojedynczo, szybciej przy
tysiącach partycji) oraz *partition projection* (Athena wylicza partycje z reguły,
bez odpytywania S3).

### Kontrola

`SELECT COUNT(*)` na każdej z pięciu tabel. Liczby warto zapisać — wrócimy do nich
w L5, gdy Silver zacznie odrzucać część wierszy.

---

## L4. Pierwsze pytania biznesowe (20 min)

Zapytania na warstwie Bronze. Przy każdym warto zanotować **Data scanned**
z paska pod wynikiem — w L6 porównamy te wartości z Parquetem.

1. Ile zamówień ma status `paid` lub `shipped` (czyli bez `cancelled`)?
2. Jaki jest przychód **netto** w dniu `2026-01-15` i `2026-01-16`?
   Brutto to `qty × unit_price_pln`; netto to brutto minus `discount_pln`.
   Tak samo nazwiemy tę miarę w tabeli faktów dnia 2.
3. Pięć kategorii o najwyższym przychodzie.
4. Ilu unikalnych klientów złożyło zamówienie w kanale `mobile`?

Punkty 2 i 3 wymagają `JOIN` — zamówienia trzymają status i kanał, pozycje trzymają
kwoty, a kategoria jest przy produkcie.

`JOIN` na plikach CSV jest wolny i to jest część lekcji: warstwa Bronze nie służy
do raportowania.

---

## L5. Silver — Parquet i partycje (25 min)

**Cel:** z surowych CSV powstaje warstwa, którą da się analizować — inny format
plików, uporządkowane typy, odfiltrowane śmieci.

Robimy to instrukcją **CTAS** (`CREATE TABLE AS SELECT`): Athena wykonuje `SELECT`
i **zapisuje jego wynik na S3 jako nowe pliki**, a przy okazji rejestruje w katalogu
nową tabelę. To jest właśnie „T” z ELT — transformacja zwykłym SQL-em, bez
osobnego narzędzia.

Pełny wzorzec składni CTAS wraz z opisem każdej właściwości `WITH (...)` jest
w `../00_Sciagawka_SQL.md`.

### Do zbudowania

**`silver_orders`** — z `bronze_orders`:

- tylko wiersze o statusie `paid` lub `shipped`
- `order_ts` przekonwertowane ze `string` na `timestamp`
- dodatkowa kolumna `order_date` typu `date` (pierwsze 10 znaków `order_ts`)
- format `PARQUET`, partycjonowanie po `dt`
- `external_location`: `s3://aurora-<inicjaly>-lake/silver/orders/`

**`silver_order_items`** — z `bronze_order_items` złączonych z `bronze_orders`:

- złączenie po `order_id`; z zamówienia bierzemy `customer_id`, `order_date` i `dt`
- ten sam filtr statusu — pozycja anulowanego zamówienia nie ma trafić do Silver
- wyliczona kolumna `net_amount_pln` = `qty × unit_price_pln − discount_pln`
- partycjonowanie po `dt`, `external_location`: `s3://…/silver/order_items/`

Sens złączenia: `order_items` nie zawiera ani daty, ani statusu. Bez sięgnięcia
do `orders` nie da się ani odfiltrować anulowanych, ani nadać pozycjom partycji `dt`.

**`silver_customers`** i **`silver_products`** — przepisane 1:1 z Bronze, z jedną
zmianą: `signup_date` jako `date` zamiast `string`. Bez partycjonowania, bo to małe
słowniki, które nie rosną z dnia na dzień.

### Trzy rzeczy, o które łatwo się potknąć

1. **Kolumna partycjonująca musi być ostatnia** na liście `SELECT`. Athena bierze
   nazwy z `partitioned_by` i oczekuje ich na końcu; przy innej kolejności zgłosi
   błąd albo — gorzej — spartycjonuje po niewłaściwej kolumnie.
2. **CTAS nie nadpisuje.** Athena nigdy nie kasuje danych z S3. Drugie uruchomienie
   kończy się `TABLE_ALREADY_EXISTS`. Zapis `CREATE TABLE IF NOT EXISTS ... AS SELECT`
   sprawia, że powtórzone wykonanie zostanie pominięte bez błędu.
   Uwaga: jeśli tabela została skasowana przez `DROP`, a pliki na S3 zostały,
   `IF NOT EXISTS` nie pomoże — pojawi się `HIVE_PATH_ALREADY_EXISTS` i trzeba
   usunąć prefiks w S3 albo wskazać inną lokalizację.
3. **Typ kolumny** wynika z `CAST`-a w `SELECT`. To, co tu ustalimy, dzień 2 zastanie
   w plikach Parquet — a Redshift przy `COPY` dopasowuje kolumny po typie.

### Kontrola

`SELECT COUNT(*)` na czterech tabelach Silver oraz suma `net_amount_pln`.
Liczba pozycji będzie **mniejsza** niż w Bronze — warto sprawdzić w S3, że pod
`silver/orders/` pojawiły się katalogi `dt=…` z plikami `.parquet`.

---

## L6. CSV kontra Parquet — rachunek (15 min)

**Cel:** zobaczyć na własnym zapytaniu, za co się w Athenie płaci.

Athena rozlicza się od **liczby bajtów odczytanych z S3**, nie od liczby wierszy
ani czasu. Warstwa plików decyduje więc o rachunku.

**Do wykonania — to samo pytanie biznesowe (przychód wg dnia) trzema drogami:**

1. na tabelach **Bronze** (CSV, `JOIN` zamówień z pozycjami) — jak w L4 pkt 2
2. na tabeli **Silver** (Parquet, `net_amount_pln` jest już policzone, bez `JOIN`-a)
3. na tabeli **Silver z filtrem jednej partycji**, np. `WHERE dt = '2026-01-16'`

Dla każdego przebiegu notujemy **Data scanned** i czas z zakładki *Query stats*.

Warunek poprawności: kwoty w wariantach 1 i 2 muszą być identyczne. Jeśli się różnią,
najczęstsza przyczyna to brak filtra statusu w jednym z zapytań.

Przełącznik *Reuse query results* musi być wyłączony, inaczej drugi przebieg
zwróci wynik z pamięci i pokaże zerowy skan.

**Pytania do przemyślenia:**

- Dlaczego wariant 2 czyta mniej bajtów niż wariant 1, choć zwraca ten sam wynik?
  Składają się na to trzy różne mechanizmy — warto spróbować je nazwać.
- Dlaczego wariant 3 czyta mniej niż wariant 2?
- Która warstwa powinna zasilać dashboard odświeżany co 15 minut i dlaczego?

---

## Zadania dodatkowe (dla tych, którzy skończą wcześniej)

### S1. Tabela Iceberg

**Apache Iceberg** to otwarty format tabeli: nad plikami Parquet dokłada warstwę
metadanych, dzięki której zbiór plików na S3 zachowuje się jak prawdziwa tabela —
z transakcjami, `UPDATE`, `DELETE`, `MERGE` i historią zmian. Bez niego jedyną
operacją na data lake jest „dopisz plik”.

**Do zrobienia:** utworzenie kopii `silver_orders` jako tabeli Iceberg
(`silver_orders_iceberg`) instrukcją CTAS. Różnice wobec CTAS z L5:

- dochodzi `table_type = 'ICEBERG'`
- lokalizację podaje się właściwością `location`, nie `external_location`
- dochodzi `is_external = false` — tabela Iceberg nie jest tabelą zewnętrzną,
  bo Athena zarządza jej plikami i metadanymi
- `partitioned_by` nie działa; Iceberg ma własną składnię `partitioning`

Wzorzec jest w ściągawce SQL.

**Sprawdzenie:** liczba wierszy zgodna z `silver_orders`, a dodatkowo lista snapshotów:

```sql
SELECT committed_at, operation
FROM "aurora_<inicjaly>"."silver_orders_iceberg$snapshots"
ORDER BY committed_at;
```

Każda zmiana tabeli tworzy nowy snapshot, a stare pozostają dostępne — na tym polega
*time travel*. Ta tabela wraca w dniu 3 jako jedyny wariant, w którym `MERGE` działa.

### S2. Lejek: koszyk bez zakupu

`bronze_events` to strumień zdarzeń ze sklepu (`page_view`, `add_to_cart`,
`purchase`) — dane półstrukturalne, które w klasycznym data warehouse zwykle
w ogóle by nie wylądowały.

**Pytanie:** ilu klientów danego dnia dodało coś do koszyka (`add_to_cart`),
ale tego samego dnia nie wygenerowało zdarzenia `purchase`?

**Podpowiedź do konstrukcji:** przydają się dwa zbiory pomocnicze (`WITH`):
unikalne pary klient + dzień ze zdarzeniem `add_to_cart` oraz analogiczne pary
ze zdarzeniem `purchase`. Wynikiem jest `LEFT JOIN` pierwszego do drugiego
z warunkiem, że po prawej stronie nic się nie znalazło.

Klucz złączenia to **klient i dzień razem** — sam `customer_id` policzyłby jako
„kupił” kogoś, kto porzucił koszyk w poniedziałek, a kupił w środę.

---

## Bilet wstępu na dzień 2

Zrzut ekranu z dwóch rzeczy:

1. drzewo bucketu z warstwami i katalogami `dt=`
2. dowolne zapytanie na warstwie Silver wraz z wynikiem

Bucket **zostaje** — dzień 2 buduje model wymiarowy na tych tabelach Silver.

---

## Dodatki po zajęciach (opcjonalne — nie są potrzebne w dniu 2 ani 3)

Sześć krótkich ćwiczeń do samodzielnej pracy po zajęciach, dla tych, którzy chcą
zostać przy tematach dnia 1 trochę dłużej. Każde zajmuje 10–20 minut, każde mieści
się w przeglądarce i w roli PowerUserAccess, każde pogłębia coś, co dziś już się
pojawiło: partycje, katalog Glue, widoki, JSON, układ plików na S3.

Dwie rzeczy do ustalenia na wejściu:

- **Nic z tej sekcji nie jest potrzebne dalej.** Dzień 2 i dzień 3 — także w wariancie
  zaawansowanym — zaczynają od tabel Bronze i Silver z L3–L5 i niczego stąd nie
  zakładają. Pominięcie całej sekcji nic nie zmienia.
- **Każdy nowy obiekt ma w nazwie `_extra`** (`bronze_events_extra`,
  `v_revenue_by_day_extra`, …), a pliki, które powstają, lądują pod prefiksem `extra/`
  w bucketcie. Dzięki temu od razu widać, co jest dodatkiem, a sprzątanie sprowadza
  się do `DROP` obiektów z tym sufiksem i usunięcia jednego prefiksu. Tabele z L3–L5
  i ich pliki zostają nietknięte.

Rozwiązania z liczbami kontrolnymi są w `02_rozwiazania.md` (sekcja o tej samej
nazwie), gotowe skrypty `sql/D1_add_partition.sql`, `sql/D2_partition_projection.sql`,
`sql/D3_widok_silver.sql`, `sql/D4_json_raw.sql`, `sql/D5_path_bucketing.sql`
(sprzątanie obiektów `_extra` jest na końcu D5). Wszystko zostało zweryfikowane od
początku do końca w Athenie engine v3.

### D1. `ALTER TABLE ADD PARTITION` kontra `MSCK REPAIR` (15 min)

W L3 partycje zarejestrowało `MSCK REPAIR TABLE`. Ta instrukcja przechodzi po
**całym** prefiksie tabeli na S3 i porównuje go z katalogiem — przy dwóch dniach
trwa sekundy, przy kilku latach dziennych partycji potrafi trwać minuty i skończyć
się przekroczeniem limitu czasu. Pipeline, który dopisuje jeden dzień, robi to
inaczej: rejestruje tylko ten jeden katalog, bez listowania reszty.

**Do zrobienia:** druga tabela na tych samych plikach zdarzeń, `bronze_events_extra`,
z tym samym DDL co `bronze_events` (ten sam `LOCATION`). Potem, w tej kolejności:

1. `SELECT COUNT(*)` i `SHOW PARTITIONS` — zanim cokolwiek zostanie zarejestrowane.
2. `ALTER TABLE … ADD IF NOT EXISTS PARTITION (dt = '2026-01-15') LOCATION 's3://…/bronze/events/dt=2026-01-15/'`,
   ponownie `COUNT(*)` i `SHOW PARTITIONS`.
3. `MSCK REPAIR TABLE` — i przeczytanie tego, co Athena wypisze jako wynik.
4. `ALTER TABLE … DROP IF EXISTS PARTITION (dt = '2026-01-15')`, `COUNT(*)` na tabeli
   `_extra`, a potem `COUNT(*)` na oryginalnej `bronze_events`.
5. Jeszcze raz `MSCK REPAIR TABLE`, żeby tabela `_extra` była znów kompletna.

Wzorzec `ADD PARTITION` jest w ściągawce SQL (sekcja 4).

**Na co zwrócić uwagę:** dwie tabele opisują te same pliki, a każda ma własną listę
partycji. `DROP PARTITION` zmienia wyłącznie wpis w katalogu — plik
`dt=2026-01-15/events.json` leży na S3 dalej i `bronze_events` nadal go widzi.

### D2. Partition projection — partycje bez katalogu (15 min)

`MSCK REPAIR` i `ADD PARTITION` mają wspólną słabość: ktoś musi je uruchomić po
każdym nowym dniu. *Partition projection* usuwa ten krok — w `TBLPROPERTIES` zapisuje
się **regułę**, z której Athena sama wylicza możliwe wartości `dt`, buduje z nich
ścieżki na S3 i czyta te katalogi, które istnieją. Katalog Glue nie jest o partycje
pytany w ogóle.

**Do zrobienia:** `bronze_orders_extra` — DDL jak `bronze_orders` (ten sam `LOCATION`),
z rozszerzonym `TBLPROPERTIES`:

```sql
TBLPROPERTIES (
  'skip.header.line.count'      = '1',
  'projection.enabled'          = 'true',
  'projection.dt.type'          = 'date',
  'projection.dt.range'         = '2026-01-01,NOW',
  'projection.dt.format'        = 'yyyy-MM-dd',
  'projection.dt.interval'      = '1',
  'projection.dt.interval.unit' = 'DAYS'
)
```

Bez żadnego `MSCK REPAIR`: `SELECT dt, COUNT(*) … GROUP BY dt ORDER BY dt`, potem
`COUNT(*)` z filtrem `dt = '2026-01-16'`, potem z filtrem `dt = '2026-02-01'`
(dzień, którego na S3 nie ma), na koniec `SHOW PARTITIONS`.

**Na co zwrócić uwagę:** co zwraca `SHOW PARTITIONS` i dlaczego zapytanie o dzień,
którego nie ma, nie kończy się błędem. Warto też pomyśleć, co Athena musi zrobić
przy zapytaniu **bez** filtra po `dt`, gdy zakres reguły to `2026-01-01,NOW`.

### D3. Widok na Silver — zapisane zapytanie, nie zapisane dane (15 min)

**Do zrobienia:** widok `v_revenue_by_day_extra` z zapytaniem „przychód wg dnia”
na `silver_order_items` (to samo, co wariant 2 z L6), a potem trzy zapytania
z zanotowanym **Data scanned**:

1. `SELECT dt, revenue_pln FROM v_revenue_by_day_extra ORDER BY dt`
2. to samo z `WHERE dt = '2026-01-16'`
3. zapytanie z L6 wariant 2 wpisane wprost, bez widoku

Dla porównania ta sama treść zapisana CTAS-em jako mała tabela
`silver_revenue_by_day_extra` (Parquet, `external_location` pod `extra/`) i `SELECT`
z niej — także z **Data scanned**.

Do podejrzenia: `SHOW CREATE VIEW` oraz
`SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'aurora_<inicjaly>' ORDER BY table_name`.

**Na co zwrócić uwagę:** widok nie zmniejsza skanu ani o bajt, a filtr po `dt`
nałożony **na widok** i tak wycina partycję. CTAS skanuje ułamek — ale jest zdjęciem
z chwili utworzenia.

### D4. Ten sam JSON trzeci raz — schema-on-read dosłownie (20 min)

`bronze_events` czyta pliki JSON przez SerDe, który zna kolumny z DDL. Da się
inaczej: tabela z **jedną kolumną** `raw string`, w której każda linia pliku jest
jednym wierszem, a kolumny wyciąga się dopiero w `SELECT` — funkcjami JSON Trino.

**Do zrobienia:** `bronze_events_raw_extra` (jedna kolumna `raw string`, partycja `dt`,
`ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'`, `STORED AS TEXTFILE`, `LOCATION`
jak `bronze_events`) i `MSCK REPAIR`. Potem:

1. `SELECT raw … LIMIT 3` i `COUNT(*)`.
2. Które klucze występują w plikach i ile razy —
   `map_keys(CAST(json_parse(raw) AS MAP(varchar, json)))` rozwinięte przez
   `CROSS JOIN UNNEST(…) AS t(k)`.
3. Rozkład `event_type` przez `json_extract_scalar(raw, '$.event_type')`, razem
   z liczbą zdarzeń, które mają niepusty `product_id`.
4. Dostęp „przez kropkę”:
   `CAST(json_parse(raw) AS ROW(event_id varchar, event_type varchar, product_id integer))`
   i wybranie pól tego rekordu.

**Na co zwrócić uwagę:** w plikach jest czwarty typ zdarzenia, o którym ani L3, ani S2
nie wspominają — i klucz, który w całej jednej grupie wierszy ma wartość `null`.
Schemat wynika z pytania, nie z pliku.

### D5. Pliki pod tabelą — `"$path"` i bucketing (20 min)

Athena rozlicza bajty, ale czyta **pliki**. Ile ich jest pod tabelą i jak są ułożone,
pokazuje pseudokolumna `"$path"` (w cudzysłowie — `$` nie jest dozwolony w zwykłej
nazwie kolumny).

**Do zrobienia:**

1. `SELECT "$path", COUNT(*) FROM silver_order_items GROUP BY "$path" ORDER BY "$path"`.
2. CTAS `silver_order_items_bucketed_extra` — jak `silver_order_items`, z dodatkiem
   `bucketed_by = ARRAY['customer_id']`, `bucket_count = 8` i `external_location`
   pod `extra/`. Ponownie `"$path"`.
3. To samo pytanie punktowe na obu tabelach, z zanotowanym **Data scanned**:
   `SELECT COUNT(*), ROUND(SUM(net_amount_pln), 2) … WHERE customer_id = 'C0001'`.
4. Pełna suma `net_amount_pln` na obu tabelach — również z **Data scanned**.

**Na co zwrócić uwagę:** bucketing pomaga jednemu rodzajowi zapytań i szkodzi
drugiemu. Który wariant ma więcej plików, który czyta mniej bajtów — i przy jakim
pytaniu.

### D6. Glue Crawler kontra ręczny DDL (tylko do przeczytania)

Glue ma narzędzie, które tworzy tabele automatycznie: **Crawler** czyta próbkę
plików pod prefiksem, zgaduje kolumny i typy, rozpoznaje katalogi `dt=…` jako
partycje i dopisuje tabelę do katalogu. Konsola: *AWS Glue → Data Catalog →
Crawlers → Create crawler*.

W standardowej roli `PowerUserAccess` (bez uprawnień `iam:*`) ten krok się nie uda i warto wiedzieć dlaczego.
Crawler działa w imieniu roli IAM, którą trzeba mu przekazać przy tworzeniu, a
PowerUserAccess nie pozwala ani utworzyć roli, ani przekazać istniejącej —
próba kończy się komunikatem
`AccessDeniedException: … is not authorized to perform: iam:PassRole on resource: arn:aws:iam::…:role/AWSGlueServiceRole`.
To celowe ograniczenie konta szkoleniowego, nie błąd konfiguracji.

Warto za to porównać w myślach oba podejścia. Crawler oszczędza pisanie DDL, ale
typy zgaduje z próbki: kolumna z samymi cyframi w CSV staje się `bigint`, choć jest
identyfikatorem; `signup_date` zapisane tekstem może wyjść jako `string` lub jako
`date`, zależnie od próbki; wiersz nagłówka bywa rozpoznany, a bywa wczytany jako
dane. Ręczny DDL z L3 jest dłuższy, ale jednoznaczny, powtarzalny i trafia do
repozytorium razem z resztą kodu — dlatego dzień 1 na nim stoi.

**Na co zwrócić uwagę:** crawler nie zastępuje decyzji o schemacie, tylko podsuwa
propozycję. Na produkcji często i tak zostaje przepisana na jawny DDL.

### Sprzątanie po dodatkach

`DROP TABLE` / `DROP VIEW` na sześciu obiektach z sufiksem `_extra` (lista w
`sql/D5_path_bucketing.sql`, sekcja sprzątania) i usunięcie w konsoli S3 katalogu `extra/` z bucketu. `DROP` na
tabelach zewnętrznych nie kasuje plików pod `bronze/` — te należą do tabel z L3.
Sprzątanie z dnia 3 (`DROP DATABASE … CASCADE` i kasowanie bucketu) i tak obejmie
wszystko, więc ten krok można zostawić na koniec kursu.
