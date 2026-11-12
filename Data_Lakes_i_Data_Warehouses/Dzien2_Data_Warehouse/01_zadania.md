# Dzień 2 — Data Warehouse (zadania)

**Czas:** 3 godziny  
**Gdzie:** AWS Console · konto warsztatowe / sandbox AWS · Athena (W1–W4) + Redshift Serverless (W5)  
**Wejście:** działający data lake z dnia 1, warstwa Silver  
**Prefix (izolacja zasobów):** ten sam `aurora-<inicjaly>-`; baza w Athenie `aurora_<inicjaly>`

Dzień 1 skończył się na warstwie Silver: dane czyste, typy uporządkowane, ale
ułożone tak, jak przyszły ze źródła. Dzisiaj układamy je pod **pytania**,
a nie pod źródło — i to jest cała różnica między data lake a data warehouse.

Składnia SQL, której będziemy używać, jest zebrana w `../00_Sciagawka_SQL.md`.
Warto mieć ten plik otwarty obok.

Ten folder to **wersja podstawowa** dni 2–3. Obok leży wersja zaawansowana
(`../Dzien2_Data_Warehouse_Zaawansowany/` — Gold od razu jako Iceberg, Redshift
z własnym DDL); o tym, którą wersją idzie grupa, decyduje trener po dniu 1 i wybór
jest jeden dla całej sali. Obie wersje startują z tego samego stanu po dniu 1
(`../Dzien1_Data_Lakes/`: Bronze + Silver, 170 / 409 / 1 427 124,00) — wystarczy
folder tej wersji, która została wybrana.

Rozwiązania są w `02_rozwiazania.md` (pełny SQL i liczby z etykietą). Katalog `sql/`
trzyma ten sam SQL 1:1: `sql/W2_ddl_star.sql`, `sql/W3_elt_athena.sql`,
`sql/W4_pytania_gold.sql`, `sql/W5_redshift.sql`. Warto zaglądać tam po własnej próbie.

---

## Zanim zaczniemy — typowe ograniczenia na koncie organizacyjnym AWS

Na kontach warsztatowych zarządzanych przez AWS Organizations często obowiązują polityki bezpieczeństwa (SCP) i ograniczenia ról IAM. Trzy rzeczy są znane z góry i **nie są niczyim błędem** — warto je poznać teraz, żeby w połowie W5 nie szukać literówki tam, gdzie jej nie ma.

**1. Redshift Data API bywa zablokowane polityką organizacji (SCP).**
Gdy w organizacji wyłączone jest `redshift-data:*`, skrypty w AWS CLI lub `boto3` sterujące Redshiftem zwrócą odmowę dostępu. Niezawodna droga do zapytań to **Query Editor v2** w konsoli — łączy się z Redshiftem bezpośrednio w przeglądarce i działa niezależnie od Data API. Samo `redshift-serverless:*` również działa normalnie.

**2. `COPY` z naszego bucketu zwróci `S3ServiceException: Access Denied`.**
Nie z powodu literówki w ścieżce. Rola IAM podpięta do workgroupu ma politykę
`AmazonRedshiftAllCommandsFullAccess`, a ta daje dostęp do S3 **wyłącznie dla
bucketów, których nazwa zawiera „redshift”** (albo dla obiektów z tagiem
`Redshift=true`). Nasz bucket to `aurora-<inicjaly>-lake`, więc pada odmowa —
mimo że bucket leży w tym samym koncie i tym samym regionie.

Odblokowanie wymaga uprawnień Administratora, więc jest ustawiane raz dla całej
sali. Z poziomu roli **PowerUserAccess** nie da się tego naprawić samodzielnie
i nie ma sensu próbować — to jedyny krok całego kursu, który tak wygląda.

Ten bloker dotyczy wyłącznie `COPY` i jawnego `CREATE EXTERNAL SCHEMA`, bo obie
te drogi czytają S3 rolą IAM workgroupu. Odczyt warstwy Gold przez katalog Glue
(`awsdatacatalog`, część W5A) idzie tożsamością zalogowanego kursanta i działa
niezależnie od tego, czy decyzja o roli zapadła.

**3. Cała sala pracuje na jednej bazie `dev`, na jednym workgroupie.**
Dlatego schemat w Redshifcie **musi** nazywać się `gold_<inicjaly>`, nigdy `gold`.
Przy wspólnej nazwie pierwsza osoba tworzy schemat, druga dostaje
`schema already exists`, a trzecia wlewa swoje dane do cudzych tabel — i nikt tego
od razu nie widzi, bo `COPY` nie zgłasza wtedy żadnego błędu.

### Kolejność i plan awaryjny

W1 → W2 → W3 → W4 to jeden ciąg i całość dzieje się w Athenie. Każde z tych ćwiczeń
korzysta z wyniku poprzedniego: model z W1–W2 jest wdrażany w W3, a W4 pyta o dane
powstałe w W3.

W5 jest **dodatkiem**, nie zwieńczeniem: pokazuje ten sam model na silniku data
warehouse. Gdyby Redshift okazał się niedostępny, kompletny model wymiarowy i tak
istnieje — w Athenie, z W3 i W4 — i nic z materiału nie przepada. W5 oglądamy wtedy
wspólnie na projektorze.

### Dwa silniki, trzy formy nazw

Dzisiaj po raz pierwszy te same dane są odpytywane z dwóch różnych silników.
Athena i Redshift patrzą na jeden Glue Data Catalog, ale **adresują tabele inaczej**
— i to jest najczęstsze źródło błędu tego dnia. Warto poznać wszystkie trzy formy
od razu, bo każda z nich pojawia się dzisiaj w plikach `sql/`:

| Forma | Gdzie działa | Co oznacza |
|---|---|---|
| `aurora_<inicjaly>.gold_fact_order_item` | **tylko Athena** | baza w Glue Data Catalog + tabela. Athena rozmawia z katalogiem wprost, więc trzeciego członu nie ma |
| `"awsdatacatalog"."aurora_<inicjaly>"."gold_fact_order_item"` | **tylko Redshift** | ta sama tabela w tym samym katalogu Glue, oglądana z Redshifta. Trzy człony: katalog · baza Glue · tabela |
| `gold_<inicjaly>.fact_order_item` | **tylko Redshift** | lokalna tabela w bazie `dev`, do której dane trafiły instrukcją `COPY`. To już nie jest data lake |

`awsdatacatalog` to nazwa zarezerwowana: Redshift Serverless montuje pod nią
katalog Glue całego konta, w trybie tylko do odczytu. W składni Atheny to słowo
nie istnieje i wpisane tam da błąd.

Zapytanie Athenowe wklejone do Query Editora v2 kończy się komunikatem:

```text
ERROR: schema "aurora_jk" does not exist
```

To nie jest literówka ani brak tabeli. Redshift szuka wtedy **lokalnego schematu**
o nazwie `aurora_<inicjaly>` w bazie `dev`, a baza Glue lokalnym schematem nie
jest. Rozwiązaniem jest dopisanie członu `"awsdatacatalog"`, nigdy utworzenie
w Redshifcie schematu o nazwie `aurora_<inicjaly>` — taki schemat przykryłby
prawdziwe źródło własną kopią, która od pierwszego przeładowania data lake
zaczyna się z nim rozjeżdżać.

Każdy plik w `sql/` ma w pierwszych liniach napisane, na którym silniku działa.

---

## W0. Stan wejściowy z dnia 1 (10 min)

**Cel:** upewnienie się, że wszystko, na czym dzisiaj budujemy, nadal istnieje.

Logowanie do AWS Management Console (np. przez IAM Identity Center lub własny sandbox AWS, rola typu **PowerUserAccess**). Region w prawym górnym rogu: **`eu-central-1` (Frankfurt)** —
ten sam co w dniu 1, bo S3, Glue, Athena i Redshift są usługami regionalnymi.

Cztery rzeczy do potwierdzenia, w tej kolejności:

| # | Co | Gdzie | Czego oczekujemy |
|---|---|---|---|
| 1 | bucket `aurora-<inicjaly>-lake` | S3 → *Buckets* | istnieje, w środku `bronze/` i `silver/` |
| 2 | lokalizacja wyników Atheny | Athena → *Query settings* | `s3://aurora-<inicjaly>-lake/athena-results/` |
| 3 | tabele Silver | Athena → lista tabel w bazie `aurora_<inicjaly>` | `silver_orders`, `silver_order_items`, `silver_customers`, `silver_products` |
| 4 | *Reuse query results* | przełącznik nad wynikami | wyłączony |

Lista baz w Athenie jest długa — na koncie zostało kilkadziesiąt baz Glue
po poprzednich grupach. Pole filtra i wpisane `aurora` skraca ją do własnych.

### Kontrola

```sql
SELECT
  (SELECT COUNT(*) FROM aurora_<inicjaly>.silver_orders)      AS orders,
  (SELECT COUNT(*) FROM aurora_<inicjaly>.silver_order_items) AS items,
  (SELECT COUNT(*) FROM aurora_<inicjaly>.silver_customers)   AS customers,
  (SELECT COUNT(*) FROM aurora_<inicjaly>.silver_products)    AS products;
```

Oczekiwane: **170 · 409 · 80 · 25**. Suma `net_amount_pln` na `silver_order_items`
to **1 427 124,00** — ta liczba wróci dzisiaj jeszcze trzy razy jako punkt
odniesienia.

Gdyby bucketu nie było (np. po wcześniejszym sprzątaniu środowiska), do odtworzenia zostaje Bronze + Silver z plików
`Dzien1_Data_Lakes/dane/bronze/` i DDL z `Dzien1_Data_Lakes/02_rozwiazania.md`.
Zajmuje to około 20 minut i wystarcza na cały dzień 2.

---

## W1. Cztery kroki Kimballa — decyzje już spisane (20 min)

**Cel:** decyzja o kształcie modelu przed napisaniem pierwszej linii SQL —
bo w tej kolejności taniej. Na zajęciach online nie ma czasu na tablicę; poniżej
jest **gotowa tabela decyzji** Aurory, którą omawiamy.

Ralph Kimball opisał projektowanie modelu wymiarowego jako cztery decyzje
podejmowane **w tej kolejności**. Zmiana kolejności jest najczęstszą przyczyną
modeli, które trzeba potem przepisać od zera.

| Krok | Decyzja Aurory |
|---|---|
| 1. Proces biznesowy | Sprzedaż w sklepie Aurora: klient kupuje pozycje produktów w kanale web / mobile / marketplace. Wiersz danych powstaje, gdy pozycja wchodzi do opłaconego lub wysłanego zamówienia. |
| 2. Grain | Jeden wiersz faktu to **jedna pozycja zamówienia** (`order_id` + produkt). Nie zamówienie (za mało szczegółu na kategorię) i nie klient-dzień (za mało na AOV i na pozycje). |
| 3. Wymiary | Data (`date_actual`, weekend, miesiąc), klient (miasto, segment), produkt (kategoria, marka), kanał. |
| 4. Miary | `qty` i `net_amount_pln` — addytywne. AOV = suma / liczba **zamówień**, nie `AVG` pozycji. |

Trzy pytania, na które ten grain odpowiada:

- Jaki przychód netto dała kategoria `Telefony` 16 stycznia 2026?
- Które miasto dostawy kupuje najwięcej AGD?
- Jaka jest średnia wartość zamówienia (AOV) w kanale `web`?

### Kontrola

Decyzje są gotowe do W2, gdy widać w tabeli powyżej: jedno zdanie o procesie,
grain pozycji, cztery wymiary z atrybutami i miary z addytywnością. W2 przekłada
to na kolumny w `sql/W2_ddl_star.sql`.

**Pytanie do przemyślenia:** który z trzech wariantów grain uniemożliwia odpowiedź
na pierwsze pytanie i dlaczego? A który uniemożliwia policzenie AOV?

---

## W2. Projekt star schema (20 min)

**Cel:** przełożenie decyzji z W1 na konkretne kolumny i typy — nadal jako projekt kolumn, zanim CTAS cokolwiek zapisze,
ale już w formie, którą da się wykonać.

Pięć tabel do zaprojektowania:

`dim_date`, `dim_customer`, `dim_product`, `dim_channel`, `fact_order_item`.

Wymagania modelu:

- każdy wymiar ma **surrogate key** (`*_sk`) typu `int` — klucz nadany przez data
  warehouse, niezależny od identyfikatorów pochodzących ze źródła
- `dim_customer` zachowuje `customer_id` ze źródła jako **natural key**
- fakt jest na grain **pozycji zamówienia**
- fakt zawiera miary: `qty`, `unit_price_pln`, `discount_pln`, `net_amount_pln`
- fakt zawiera klucze obce: `date_sk`, `customer_sk`, `product_sk`, `channel_sk`

Szkielet do wypełnienia: `sql/W2_ddl_star.sql`. To jest projekt modelu, nie skrypt
do uruchomienia — Athena nie zna „gołego” `CREATE TABLE` z samymi kolumnami,
a wersją wykonywalną tego samego modelu jest skrypt Redshiftowy z W5.

### Po co surrogate key, skoro jest customer_id

Pytanie wraca na każdym szkoleniu i ma trzy odpowiedzi:

- natural key bywa zmienny — źródło potrafi zmienić format identyfikatora,
  a firma potrafi zmigrować na inny CRM
- natural key bywa złożony z kilku kolumn, co obciąża każde złączenie
- **jutro, w dniu 3**: gdy klient zmieni miasto, w wymiarze pojawi się jego druga
  wersja. Obie mają ten sam `customer_id` i muszą różnić się kluczem, bo fakt
  ze stycznia ma dalej wskazywać na stare miasto (SCD Type 2)

### Gdzie te tabele powstaną

Athena i Redshift różnią się tu na tyle, że warto to rozpisać:

| Silnik | Pełna nazwa | Jak powstaje |
|---|---|---|
| Athena | `aurora_<inicjaly>.gold_dim_date` | CTAS. Baza w Glue nie ma schematów w środku, więc warstwę oznacza prefiks `gold_` |
| Redshift | `gold_<inicjaly>.dim_date` | prawdziwy `CREATE SCHEMA` i klasyczny `CREATE TABLE` |

Zapis `gold.dim_date` w tym ćwiczeniu oznacza **nazwę w modelu**, nie literalny SQL.

W Athenie nie ma kluczy obcych ani `PRIMARY KEY` z egzekwowaniem — silnik niczego
nie sprawdzi, a spójność zapewnia sposób ładowania i konwencja nazw. W Redshifcie
klucze da się zadeklarować, ale są **informacyjne**: optymalizator z nich korzysta,
a duplikat i tak wejdzie do tabeli.

### Kontrola

Projekt jest kompletny, gdy dla każdej z pięciu tabel są wypisane nazwy kolumn
i typy, a każda kolumna `*_sk` na faktcie ma swój odpowiednik w którymś wymiarze.
Na faktcie nie powinna zostać żadna nazwa miasta, kategorii ani nazwisko — jeśli
została, to znaczy, że wymiar i fakt się zlały.

**Pytanie do przemyślenia:** `order_id` zostaje na faktcie, choć nie jest kluczem
do żadnego wymiaru. Co takiego dałoby się bez niego policzyć, a czego już nie?

---

## W3. ELT: Silver → Gold (35 min)

**Cel:** model z W2 przestaje być projektem kolumn — powstają realne tabele Gold
z danymi, gotowe do pytań biznesowych.

Źródłem są **tabele Silver z dnia 1**, nie pliki CSV. Silver odfiltrował już
anulowane zamówienia i uporządkował typy, więc Gold zajmuje się wyłącznie układem.
To jest litera „T” z ELT: transformacja zwykłym SQL-em, wykonywana przez silnik,
który i tak już mamy.

Narzędziem jest **CTAS** (`CREATE TABLE AS SELECT`), dokładnie jak w L5 wczoraj:
Athena wykonuje `SELECT`, zapisuje wynik na S3 jako nowe pliki Parquet i rejestruje
tabelę w Glue Data Catalog. Wzorzec składni razem z opisem każdej właściwości
`WITH (...)` jest w `../00_Sciagawka_SQL.md`.

### Kolejność jest wymuszona przez model

**Najpierw wszystkie wymiary, potem fakt.** Fakt złącza się z wymiarami po ich
natural key — `customer_id`, `product_id`, `order_date`, `channel` — i podmienia je
na `*_sk`, więc w momencie budowania faktu wymiary muszą już istnieć — inaczej
złączenie nie ma do czego trafić.

Wewnątrz grupy wymiarów kolejność jest dowolna.

### Wymiary

**`gold_dim_date`** — jeden wiersz na dzień. Atrybuty: `date_sk`, `date_actual`,
`year`, `month`, `day`, `day_name`, `is_weekend`. Źródłem mogą być daty
występujące w zamówieniach.

Wymiar daty prawie zawsze buduje się osobno, nie z faktu. Powód: gdyby dzień bez
sprzedaży nie miał wiersza, raport „sprzedaż dzień po dniu” po cichu pominąłby
weekendy zamiast pokazać zero.

Dla `date_sk` przyjęła się konwencja `RRRRMMDD` jako liczba (`20260115`) — czytelna
przy debugowaniu i stabilna między przeładowaniami.

**`gold_dim_channel`** — z unikalnych wartości `channel`. Mały wymiar, trzy wiersze,
ale własna tabela pozwala dołożyć atrybuty (koszt kanału, opiekun) bez ruszania
tabeli faktów.

**`gold_dim_customer`** i **`gold_dim_product`** — z odpowiednich tabel Silver,
z dołożonym surrogate key.

Sposób nadania surrogate key w Athenie:

```sql
CAST(ROW_NUMBER() OVER (ORDER BY customer_id) AS integer) AS customer_sk
```

`ROW_NUMBER()` numeruje wiersze w kolejności zadanej przez `ORDER BY`.
`CAST(... AS integer)` nie jest ozdobnikiem: funkcja zwraca `bigint`, więc plik
Parquet zapisze `int64`, a `COPY` do kolumny `int` w Redshifcie (W5) przerwie
na niezgodności typów. Ta sama pułapka dotyczy `year()`, `month()` i `day()`
w wymiarze daty.

### Fakt

`gold_fact_order_item` powstaje z `silver_order_items` złączonego z każdym wymiarem
po natural key; z wymiaru bierzemy wyłącznie `*_sk`. Dochodzi jedno złączenie
z `silver_orders` — kanał jest atrybutem zamówienia, a nie pozycji, więc bez tego
złączenia nie ma skąd wziąć `channel`.

Fakt zawiera **klucze i liczby**. Nazw miast, kategorii i nazwisk w nim nie ma —
one siedzą w wymiarach. To odróżnia star schema od jednej szerokiej tabeli.

Fakt partycjonujemy po `dt`, tak jak Silver. Kolumna partycjonująca musi być
**ostatnia** na liście `SELECT` — Athena bierze tyle ostatnich kolumn, ile jest
w `partitioned_by`, a nie dopasowuje ich po nazwie.

### Gdzie to wyląduje na S3

Każdy CTAS dostaje jawne `external_location`, więc pliki Parquet trafiają pod:

```text
s3://aurora-<inicjaly>-lake/gold/dim_date/
s3://aurora-<inicjaly>-lake/gold/dim_channel/
s3://aurora-<inicjaly>-lake/gold/dim_customer/
s3://aurora-<inicjaly>-lake/gold/dim_product/
s3://aurora-<inicjaly>-lake/gold/fact_order_item/dt=2026-01-15/
s3://aurora-<inicjaly>-lake/gold/fact_order_item/dt=2026-01-16/
```

Te ścieżki nie są kosmetyką: **W5 czyta instrukcją `COPY` dokładnie te pliki.**
Warto je zanotować albo mieć konsolę S3 otwartą w drugiej karcie.

Athena nigdy nie kasuje danych z S3, więc drugie uruchomienie CTAS-a bez
`IF NOT EXISTS` kończy się `TABLE_ALREADY_EXISTS`. A jeśli tabela została
skasowana przez `DROP`, ale pliki pod `gold/` zostały, pojawi się
`HIVE_PATH_ALREADY_EXISTS` — wtedy do usunięcia zostaje prefiks w S3 albo do
wskazania inna lokalizacja.

### Kontrola

| Tabela | Wierszy |
|---|---|
| `gold_dim_date` | **2** (15 i 16 stycznia) |
| `gold_dim_channel` | **3** (`marketplace`, `mobile`, `web`) |
| `gold_dim_customer` | **80** |
| `gold_dim_product` | **25** |
| `gold_fact_order_item` | **409** |

Fakt musi mieć **dokładnie tyle wierszy, co `silver_order_items`** — czyli 409.
Mniejsza liczba znaczy, że któryś `INNER JOIN` do wymiaru nie trafił i po cichu
usunął wiersze. Większa znaczy, że któryś wymiar ma duplikat natural key
i fakt się zdublował.

Suma `net_amount_pln` na faktcie: **1 427 124,00** — ta sama co na Silver.

**Pytanie do przemyślenia:** wszystkie złączenia w faktcie to `INNER JOIN`.
Co by się stało z liczbą wierszy, gdyby jeden klient ze `silver_order_items`
nie miał odpowiednika w `gold_dim_customer` — i czy zauważylibyśmy to bez
zapytania kontrolnego?

---

## W4. Pytania biznesowe na star schema (25 min)

**Cel:** sprawdzenie, czy model odpowiada na pytania z W1 — i czy odpowiada
tak samo jak warstwa niżej.

**Silnik: Athena**, nazwy dwuczłonowe (`aurora_<inicjaly>.gold_…`). Te same cztery
pytania zadane z Redshifta wyglądają inaczej i są w W5.

Wyłącznie na warstwie Gold, bez wracania do Bronze i Silver.

1. Przychód netto według kategorii i dnia.
2. Pięć miast o najwyższym przychodzie w kategorii AGD.
3. AOV według kanału — suma netto podzielona przez liczbę **unikalnych** `order_id`.
4. Rekoncyliacja: ten sam przychód policzony na `gold_fact_order_item`
   i na `silver_order_items` z dnia 1. Kwoty muszą zgadzać się co do grosza.

W punkcie 3 liczba zamówień to `COUNT(DISTINCT order_id)`, nie `COUNT(*)`.
Grain faktu to **pozycja** zamówienia, więc jedno zamówienie zajmuje kilka wierszy —
`COUNT(*)` policzyłby pozycje i zaniżył AOV około dwuipółkrotnie.

Punkt 4 to **rekoncyliacja** — standardowy test każdego data warehouse,
uruchamiany po każdym przeładowaniu. Rozjazd oznacza jedną z dwóch rzeczy
i kierunek różnicy je odróżnia: Gold mniejszy od Silver to zgubione wiersze
na `INNER JOIN`, Gold większy to duplikat natural key w wymiarze.

### Kontrola

| Pytanie | Czego oczekujemy |
|---|---|
| 1 | 12 wierszy (6 kategorii × 2 dni). Suma dnia 15.01 = **653 531,00**, dnia 16.01 = **773 593,00** |
| 1 | najwyższa kategoria 15.01 to **Telefony**, 16.01 to **Komputery** — ranking zmienia się między dniami |
| 2 | pierwsze miejsce **Gdańsk**, 47 052,00 |
| 3 | kolejność malejąca: `mobile` (49 zamówień), `web` (55), `marketplace` (66) |
| 4 | **1 427 124,00** w obu wierszach |

AOV wychodzi w okolicach 8–9 tysięcy złotych. To nie jest błąd: Aurora jest
sklepem z elektroniką, a koszyk ma średnio 2,4 pozycji.

Warto przy okazji policzyć złączenia w zapytaniu z punktu 1 i zestawić tę liczbę
z tym, ile trzeba by ich napisać na znormalizowanym modelu źródłowym
(klient → zamówienie → pozycja → produkt). To jest mierzalna, a nie deklarowana,
korzyść z modelu Kimballa.

**Pytanie do przemyślenia:** AOV w kanale `mobile` to 9 235,24, a w `marketplace`
7 902,91. Czy średnia z tych trzech liczb da AOV całego sklepu? Jeśli nie, to jak
policzyć AOV całości i co ta różnica mówi o miarach będących ilorazem?

---

## W5. Redshift Serverless (45 min)

**Cel:** ten sam model wymiarowy, przeniesiony na silnik data warehouse — i te same
kwoty na wyjściu, policzone zupełnie inną drogą.

Athena czyta pliki z S3 przy każdym zapytaniu. Redshift trzyma dane u siebie,
w kolumnowym magazynie rozłożonym na węzły, i dopiero na nich liczy. Różnicy
w wydajności na 409 wierszach nie da się poczuć — realna różnica dotyczy modelu
pracy, nie prędkości.

Kolejność w tym ćwiczeniu: najpierw zrozumienie, co to jest namespace i workgroup
(bo bez tego nazwy na ekranie nic nie znaczą), potem obejrzenie ekranu tworzenia,
potem połączenie przez Query Editor v2, na końcu SQL.

### Gdzie to jest w konsoli

Usługa nazywa się **Amazon Redshift** (wyszukiwarka konsoli, pole *Search*).
W lewej nawigacji jest sekcja **Redshift Serverless**, a w niej pozycja
**Serverless dashboard** — to jedno miejsce z listą namespace'ów, listą workgroupów
i wykresem zużycia RPU.

Prowizjonowany Redshift (klastry `dc2`, `ra3`) siedzi w tej samej konsoli,
w sekcji **Clusters**. Nie korzystamy z niego: klaster trzeba wyłączyć, żeby
przestał kosztować, a Serverless usypia sam.

W środowisku warsztatowym w regionie `eu-central-1` może już istnieć domyślny workgroup:

| Workgroup | Base capacity | Rola IAM |
|---|---|---|
| `default-workgroup` | 128 RPU | domyślna rola `AmazonRedshift-CommandsAccessRole-*` |

To ma dwie konsekwencje. Pierwsza: `default-workgroup` z 128 RPU jest kosztowną
konfiguracją domyślną — nie warto go budzić bez potrzeby, jeśli tworzymy własny workgroup z 8 RPU. Druga: domyślna rola konsolowa
widzi na S3 wyłącznie buckety z „redshift” w nazwie (z zastrzeżeniem
z sekcji o ograniczeniach na początku pliku).

Przy pierwszym wejściu do Redshift Serverless na koncie bez zasobów konsola pokazuje
ekran powitalny z przyciskiem **Create workgroup** i wyborem między
*Use default settings* (AWS dobiera VPC, subnety, pojemność i nazwy) oraz
*Customize settings* (te same pola, ale do wypełnienia ręcznie). Wariant domyślny
ustawia 128 RPU — i to jest główny powód, dla którego warto wejść w
*Customize settings*.

### Namespace kontra workgroup

Redshift Serverless rozdziela dwie rzeczy, które w klasycznym klastrze były jednym
bytem:

| | **Namespace** | **Workgroup** |
|---|---|---|
| Czym jest | warstwa **danych** | warstwa **obliczeniowa** |
| Co w sobie trzyma | bazy danych, użytkownicy, uprawnienia, podpięte role IAM, klucz szyfrujący, snapshoty | pojemność (RPU), VPC i subnety, security group, dostępność publiczna, limity użycia |
| Co się dzieje po usunięciu | dane przepadają (o ile nie ma snapshotu) | dane zostają, przepada tylko moc obliczeniowa |

Powód rozdzielenia jest praktyczny: moc skaluje się niezależnie od danych. Do tego
samego namespace'u da się podpiąć osobny workgroup dla nocnego ELT (dużo RPU,
krótko) i osobny dla dashboardów (mało RPU, cały dzień) — a rachunek i limity
liczą się dla każdego z nich oddzielnie.

### Ustawiamy

| Ekran | Opcja | Wartość | Dlaczego |
|---|---|---|---|
| Workgroup | Workgroup name | `aurora-<inicjaly>-wg` | izolacja zasobów w środowisku wieloosobowym |
| Workgroup | **Base capacity (RPU)** | `8` | 128 to wartość domyślna — sensowna dla produkcji, nie dla 409 wierszy |
| Namespace | Namespace name | `aurora-<inicjaly>-ns` | jak wyżej |
| Namespace | **Associated IAM roles** | rola z dostępem do naszego bucketu | tu rozstrzyga się, czy `COPY` przejdzie |
| Namespace | **Admin user credentials** | nazwa użytkownika i hasło | jedyne dane, które trzeba zanotować przed opuszczeniem ekranu |

Nazwy do zanotowania: nazwa workgroupu, nazwa namespace'u, nazwa bazy (`dev`)
i login administratora.

**RPU** (*Redshift Processing Unit*) to jednostka mocy: pamięć i procesor w jednej
paczce. Serverless rozlicza się za **sekundy pracy** pomnożone przez liczbę RPU,
a nie za czas istnienia workgroupu — bezczynny workgroup nie kosztuje. Base capacity
to punkt startowy: niższa wartość obniża rachunek i wydłuża zapytania, wyższa
odwrotnie. Dla naszego datasetu 8 RPU jest z dużym zapasem wystarczające.

### Zostawiamy domyślne — i po co te opcje istnieją

- **Publicly accessible: wyłączone.** Włączenie nadaje workgroupowi publiczny
  endpoint, dostępny z internetu. Potrzebne, gdy łączy się do niego narzędzie
  spoza VPC — lokalny DBeaver, Tableau na laptopie, usługa w innej chmurze.
  Query Editor v2 działa z wnętrza konsoli AWS i publicznego adresu nie potrzebuje,
  a każdy publiczny endpoint bazy to powierzchnia ataku.
- **VPC, Subnets, Security group: domyślne.** Konsola podstawia domyślny VPC
  regionu i wszystkie jego subnety. W firmie wskazuje się tu subnety prywatne
  i security group wpuszczającą tylko wybrane adresy — Redshift Serverless wymaga
  subnetów w co najmniej trzech strefach dostępności, i to jest najczęstsza
  przyczyna błędu przy własnym, oszczędnie zaprojektowanym VPC.
- **Encryption: AWS owned key.** Szyfrowanie w spoczynku jest w Redshift Serverless
  obowiązkowe i nie da się go wyłączyć — pytanie dotyczy tylko tego, czyj jest
  klucz. Własny klucz KMS daje osobny audyt użycia i możliwość odcięcia dostępu
  polityką klucza, ale kosztuje za żądania i wymaga, żeby każda rola sięgająca
  po dane miała `kms:Decrypt`.
- **Audit logging / Log export: wyłączone.** Włączenie wysyła do CloudWatch Logs
  historię połączeń, historię użytkowników i log zapytań. W produkcji jest to
  standard: bez tego nie da się odpowiedzieć na pytanie „kto uruchomił to zapytanie
  o trzeciej w nocy”. Na kursie generowałoby koszt logów bez żadnego pożytku.
- **Snapshot / Retention period: domyślne.** Serverless sam trzyma automatyczne
  *recovery points* z krótkim okresem retencji i to wystarcza do cofnięcia
  przypadkowego `DROP`. Jawny snapshot zakłada się przed migracją albo przed
  usunięciem namespace'u, gdy dane mają przeżyć infrastrukturę.
- **Usage limits: puste.** Limit RPU-godzin na okres rozliczeniowy z akcją
  „ostrzeż” albo „wyłącz zapytania”. To jedyny mechanizm, który w Serverless
  fizycznie zatrzymuje narastanie rachunku — w produkcji na wspólnym koncie warto
  go mieć.

Układ i nazwy sekcji na ekranie tworzenia zmieniają się między wersjami konsoli;
nazwy pól z tabel powyżej zmieniają się znacznie rzadziej.

### Ile to trwa

Utworzenie namespace'u i workgroupu zajmuje **około 2–5 minut**. W tym czasie
status workgroupu na liście to `Creating` i nic — ani Query Editor v2, ani `COPY` —
jeszcze nie zadziała. Do pracy nadaje się dopiero status **Available**.
Odświeżanie listy w tym czasie niczego nie przyspiesza.

### Query Editor v2 — panele i połączenie

Dwie drogi wejścia: lewa nawigacja konsoli Redshift → **Query editor v2**,
albo przycisk **Query data** na stronie workgroupu. Obie prowadzą w to samo miejsce.

Query Editor v2 to osobne, przeglądarkowe narzędzie SQL wbudowane w konsolę —
odpowiednik wczorajszego Query editora Atheny, tylko dla Redshifta. Ekran ma cztery
części:

- **lewe drzewo** — hierarchia `workgroup → baza danych → schemat → tabele →
  kolumny`. Nasz schemat `gold_<inicjaly>` pojawi się w nim dopiero po
  `CREATE SCHEMA` i odświeżeniu gałęzi.
- **zakładka edytora** — miejsce na SQL. Zakładek może być kilka, każda ma **własną,
  odizolowaną sesję**: tabela tymczasowa albo `SET` z jednej zakładki nie jest
  widoczna w drugiej. Przy podzielonym na kilka zakładek skrypcie potrafi to
  zaskoczyć.
- **przycisk Run** — wykonuje zaznaczony fragment, a gdy nic nie jest zaznaczone,
  całą zawartość zakładki. Przy skryptach z wielu instrukcji różnica jest istotna.
- **siatka wyników** pod edytorem — razem z czasem wykonania i możliwością eksportu
  do CSV.

Kliknięcie workgroupu w lewym drzewie otwiera okno połączenia z wyborem
uwierzytelnienia:

- **Federated user** — tożsamość z konsoli AWS, ta sama, z którą weszliśmy przez
  portal SSO. **Ta opcja**, nic więcej nie trzeba: żadnego hasła, żadnego sekretu.
- *Database user name and password* — konto lokalne w bazie danych. Przydatne dla
  aplikacji spoza AWS, które nie mają tożsamości IAM.
- *AWS Secrets Manager* — hasło pobierane z sekretu. Standard w potokach
  produkcyjnych, bo hasło nie występuje wtedy w żadnym pliku konfiguracyjnym
  i podlega rotacji.

Baza danych: **`dev`** — powstaje razem z namespace'em i jest bazą domyślną.

### Do wykonania — dwie drogi do tych samych liczb

Skrypt gotowy do wypełnienia: `sql/W5_redshift.sql`. Jest podzielony na dwie
części, bo Redshift potrafi dojść do warstwy Gold na dwa sposoby, a różnica
między nimi jest sama w sobie tematem lekcji.

| | **W5A — przez katalog Glue** | **W5B — przez `COPY`** |
|---|---|---|
| Gdzie leżą dane w trakcie zapytania | na S3, w data lake | w magazynie Redshifta |
| Nazwa tabeli | `"awsdatacatalog"."aurora_<inicjaly>"."gold_…"` | `gold_<inicjaly>.…` |
| Co trzeba przygotować | nic poza połączeniem jako Federated user | schemat, DDL pięciu tabel, `COPY` |
| Czym S3 jest czytane | tożsamością zalogowanego kursanta | rolą IAM workgroupu |
| Po co to robimy | żeby zobaczyć te same dane innym silnikiem, natychmiast | żeby zobaczyć, czym data warehouse różni się od silnika czytającego pliki |

**Zaczynamy od W5A**, bo nie zależy od niczego poza zalogowaniem i daje wynik
w kilkanaście sekund. Właściwą treścią ćwiczenia jest jednak W5B: typy, rozkład
danych między węzły, `SORTKEY` i idempotencja `COPY` to rzeczy, których droga
przez katalog Glue nie pokazuje.

Gdyby `COPY` zwrócił `Access Denied` (znany bloker z początku pliku), W5A zostaje
jako pełnoprawna odpowiedź na pytanie „czy Redshift widzi naszą warstwę Gold”.

#### W5A

1. Połączenie w Query Editorze v2 jako **Federated user**, baza `dev`. Ten wybór
   nie jest kosmetyką — obraz katalogu Glue jest widoczny wyłącznie przy tym
   sposobie uwierzytelnienia.
2. `SHOW data_catalog_auto_mount;` — oczekiwane `on`.
3. Pytanie W4 punkt 1 i rekoncyliacja Silver = Gold, w zapisie trzyczłonowym.

Zapis trzyczłonowy jest tu obowiązkowy: `USE` i `search_path` dla bazy
`awsdatacatalog` nie działają, więc skrócić go do dwóch członów się nie da.
Baza jest tylko do odczytu — żaden `INSERT` ani `CTAS` nie zapisze niczego
z powrotem do katalogu Glue.

#### W5B

1. **Schemat `gold_<inicjaly>`.** Nie `gold` — powód jest w sekcji o blokerach
   na początku pliku.
2. **Pięć tabel** — cztery wymiary i fakt, klasycznym `CREATE TABLE` z typami.
   Typy muszą odpowiadać plikom Parquet z warstwy Gold: `*_sk` oraz `year`,
   `month`, `day` jako `int`, kwoty jako `double precision`, teksty jako
   `varchar(n)`.
3. **Ładowanie instrukcją `COPY`** z warstwy **Gold** na S3 — z tych samych
   ścieżek `s3://aurora-<inicjaly>-lake/gold/…`, które powstały w W3.
4. **Powtórzenie pytania W4 punkt 1.** Kwoty muszą zgadzać się z Ateną co
   do grosza.

Trzy rzeczy o `COPY`, które warto znać przed pierwszym uruchomieniem:

**Źródłem jest Gold, nigdy Silver.** `COPY ... FORMAT AS PARQUET` dopasowuje
kolumny **po kolejności i typie, nie po nazwie**. Silver nie ma surrogate keys,
więc przy ładowaniu z `silver/products/` kolumna `product_id` wylądowałaby
w `product_sk` — bez żadnego błędu, po prostu z cichym przekłamaniem danych.

**Tabela faktu ma w Redshifcie o jedną kolumnę mniej.** Kolumna partycjonująca `dt`
nie jest zapisana wewnątrz pliku Parquet — siedzi w ścieżce S3 (`dt=2026-01-15/`).
Plik ma więc 9 kolumn i tabela w Redshifcie też musi mieć 9, inaczej `COPY`
przerwie na liczbie kolumn.

**`COPY` nie jest idempotentny.** Drugie uruchomienie tej samej instrukcji dokłada
te same wiersze jeszcze raz i tabela ma ich dwa razy tyle. `CREATE TABLE
IF NOT EXISTS` chroni przed powtórzeniem DDL, ale nie przed powtórzeniem
ładowania — przed powtórką potrzebny jest `TRUNCATE` albo `DELETE`.

### Uprawnienia — trzy warstwy, których nie warto mylić

Prawie każdy błąd W5 to brak uprawnienia w jednej z trzech niezależnych warstw.
Każda daje inny komunikat, więc rozpoznanie warstwy po komunikacie skraca
diagnostykę do minuty.

**(a) Uprawnienia bazodanowe w Redshifcie.** To zwykłe `GRANT`-y wykonywane
w SQL i dotyczą tego, co wolno tożsamości **wewnątrz** bazy `dev`.

```sql
-- odczyt katalogu Glue dla własnej tożsamości federacyjnej
GRANT USAGE ON DATABASE awsdatacatalog TO "IAMR:<nazwa-roli-SSO>";

-- udostępnienie własnego schematu Gold komuś innemu
GRANT USAGE ON SCHEMA gold_<inicjaly> TO "IAMR:<ktos>";
GRANT SELECT ON ALL TABLES IN SCHEMA gold_<inicjaly> TO "IAMR:<ktos>";
```

Nadaje je administrator bazy, połączony jako *Database user name and password*.
Własną tożsamość do wstawienia w cudzysłów zwraca `SELECT current_user;`.
Twórca schematu jest jego właścicielem i dla siebie nie potrzebuje żadnego
`GRANT`-a. Objaw braku: `permission denied for database awsdatacatalog` albo
`permission denied for schema …` — w komunikacie nie ma ani słowa o S3.

**(b) Obraz katalogu Glue po stronie Redshifta.** Ta warstwa odpowiada
na pytanie, czy baza `awsdatacatalog` w ogóle istnieje w tym połączeniu.
Redshift Serverless montuje katalog Glue konta automatycznie; stan tego
ustawienia pokazuje `SHOW data_catalog_auto_mount;`, a włącza je administrator
przez `ALTER SYSTEM SET data_catalog_auto_mount = on;` (na Serverless zmiana
wchodzi po uśpieniu i wybudzeniu workgroupu). Drugi warunek to połączenie jako
**Federated user** — przy logowaniu hasłem baza `awsdatacatalog` się nie pojawi.

Wariant jawny, dla porównania — klasyczny Redshift Spectrum:

```sql
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_<inicjaly>
FROM DATA CATALOG DATABASE 'aurora_<inicjaly>'
IAM_ROLE 'arn:aws:iam::123456789012:role/service-role/<rola>';
```

Różnica jest istotna: external schema czyta S3 **rolą IAM**, a automatyczne
montowanie — tożsamością zalogowanego użytkownika. Objaw braku tej warstwy:
`ERROR: database "awsdatacatalog" does not exist`.

**(c) Uprawnienia IAM do S3 i Glue.** Ta warstwa jest poza bazą danych i dotyczy
samych plików oraz metadanych. Potrzebne są `s3:GetObject` i `s3:ListBucket`
na `aurora-<inicjaly>-lake` oraz `glue:GetDatabase`, `glue:GetDatabases`,
`glue:GetTable`, `glue:GetTables` i `glue:GetPartitions`.

Komu je nadać, zależy od drogi:

| Droga | Kto sięga po S3 | Typowe zachowanie z rolą `PowerUserAccess` |
|---|---|---|
| W5A, `awsdatacatalog` | tożsamość zalogowanego użytkownika | `PowerUserAccess` ma odczyt S3 i Glue — działa od razu |
| W5B, `COPY` | rola IAM podpięta do namespace'u | domyślna `AmazonRedshiftAllCommandsFullAccess` widzi tylko buckety z „redshift” w nazwie — bez dodatkowej polityki S3 zwraca `Access Denied` |
| `CREATE EXTERNAL SCHEMA` | rola IAM podpięta do namespace'u | jak wyżej |

Stąd obserwacja, która wygląda na sprzeczność, a nią nie jest: `COPY` z bucketu
`aurora-<inicjaly>-lake` zwraca `Access Denied`, a `SELECT` z tych samych plików
przez `awsdatacatalog` przechodzi bez problemu. Gdyby katalog był zarządzany
przez Lake Formation, doszłaby jeszcze akcja `lakeformation:GetDataAccess`
i grant LF na bazę — przy domyślnym trybie katalogu `IAM_ALLOWED_PRINCIPALS`
samo IAM wystarcza. Objaw braku: `S3ServiceException: Access Denied`.

### Kontrola

Po W5A: pytanie W4 punkt 1 zadane przez `awsdatacatalog` daje **12 wierszy**
i te same kwoty co w Athenie, a rekoncyliacja — **1 427 124,00** w obu wierszach.

Po W5B:

| Tabela w `gold_<inicjaly>` | Wierszy |
|---|---|
| `dim_date` | **2** |
| `dim_channel` | **3** |
| `dim_customer` | **80** |
| `dim_product` | **25** |
| `fact_order_item` | **409** |

Dokładnie te same liczby co w Athenie. Liczby podwojone (4 · 6 · 160 · 50 · 818)
znaczą, że `COPY` poszedł dwa razy.

Zapytanie z W4 punkt 1 musi dać **653 531,00** dla 15.01 i **773 593,00** dla 16.01,
w sumie **1 427 124,00** — te same kwoty, które wyszły w Athenie i na warstwie
Silver. Dwa różne silniki, dwa różne formaty przechowywania, ta sama liczba: to jest
najkrótszy dowód, że model jest poprawny.

Gdyby `COPY` zwrócił `S3ServiceException: Access Denied` — to jest znany bloker
opisany na początku pliku, nie błąd w ścieżce ani w SQL.

**Pytanie do przemyślenia:** kiedy fakt warto trzymać w Redshifcie, a kiedy
wystarczy Athena na plikach Parquet w data lake? Warto rozważyć liczbę zapytań
na godzinę, liczbę równoległych odbiorców oraz to, kto płaci za zeskanowane bajty.

---

## Bilet na dzień 3

Warstwa Gold w Athenie i schemat w Redshifcie **zostają**. Jutro dochodzi nowy
dzień sprzedaży (`dt=2026-01-20`) oraz klienci, którzy zmienili miasto — a to
znaczy SCD Type 2 na `gold_dim_customer` i ładowanie przyrostowe na faktcie.

Do zabrania z dzisiaj:

1. własne nazwy: bucket, baza w Athenie, schemat w Redshifcie
2. skrypty z `sql/` z podmienionym prefiksem — jutro startujemy od nich
3. liczby kontrolne: 409 wierszy faktu i 1 427 124,00 przychodu

Gdyby cokolwiek trzeba było skasować, warto zachować DDL — model da się wtedy
odtworzyć rano w kilka minut.
