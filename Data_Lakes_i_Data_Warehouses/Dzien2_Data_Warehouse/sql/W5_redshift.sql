-- =============================================================================
-- Dzien 2, W5 - ten sam model wymiarowy na Redshift Serverless
-- =============================================================================
-- SILNIK: AMAZON REDSHIFT SERVERLESS, Query Editor v2. NIE Athena - Athena nie
-- zna ani CREATE SCHEMA, ani COPY, ani awsdatacatalog.
--
-- Do podmiany: gold_jk -> gold_<inicjaly>, aurora_jk -> aurora_<inicjaly>,
-- aurora-jk-lake -> wlasny bucket.
--
-- -----------------------------------------------------------------------------
-- TRZY FORMY NAZW - to jest miejsce, w ktorym W5 najczesciej sie wywraca
-- -----------------------------------------------------------------------------
-- 1) gold_<inicjaly>.dim_date                    <- DZIALA, czesc W5B
--    Lokalna tabela Redshifta w bazie `dev`. Dane fizycznie leza w magazynie
--    Redshifta, bo wlozyl je tam COPY. Dwa czlony: schemat . tabela.
--
-- 2) "awsdatacatalog"."aurora_<inicjaly>"."gold_dim_date"   <- DZIALA, W5A
--    TA SAMA warstwa Gold, ale czytana wprost z data lake przez katalog Glue,
--    bez kopiowania czegokolwiek. Trzy czlony: katalog . baza Glue . tabela.
--    awsdatacatalog to zamontowany automatycznie, tylko do odczytu obraz Glue
--    Data Catalog tego konta; aurora_<inicjaly> to nasza baza z Atheny.
--
-- 3) aurora_<inicjaly>.gold_dim_date             <- NIE DZIALA w Redshifcie
--    To jest zapis ATHENOWY. Redshift konczy go bledem
--      ERROR: schema "aurora_jk" does not exist
--    bo szuka wtedy lokalnego schematu o tej nazwie w bazie `dev`. Baza Glue
--    lokalnym schematem nie jest. Lokalnego schematu aurora_<inicjaly> tu NIE
--    tworzymy: przykrylby prawdziwe zrodlo wlasna kopia, ktora od pierwszego
--    przeladowania data lake zaczyna sie z nim rozjezdzac.
--
-- Zrodlem tego bledu jest prawie zawsze przeklejenie zapytania z W4
-- (W4_pytania_gold.sql, plik Athenowy) do zakladki Redshifta.
--
-- WEJSCIE: Query Editor v2 w konsoli AWS (Redshift -> Query editor v2, albo
-- przycisk Query data na stronie workgroupu). Polaczenie jako Federated user,
-- baza `dev`. Wybor Federated user nie jest tu kosmetyka: obraz awsdatacatalog
-- jest widoczny wylacznie przy tym sposobie uwierzytelnienia (na workgroupie
-- Serverless; na klastrze provisioned odpowiednikiem jest "Temporary
-- credentials using your IAM identity").
--
-- Gdy Redshift Data API jest zablokowane polityka organizacji (SCP),
-- AWS CLI, boto3 i skrypty sterujace Redshiftem nie zadzialaja. QEv2 idzie
-- inna droga i dziala normalnie.
--
-- Uwaga o edytorze: kazda zakladka QEv2 ma WLASNA, odizolowana sesje. Przycisk
-- Run wykonuje zaznaczony fragment, a gdy nic nie jest zaznaczone - cala
-- zawartosc zakladki. Przy skrypcie z wielu instrukcji ta roznica ma znaczenie.
--
-- -----------------------------------------------------------------------------
-- UKLAD PLIKU: W5A czyta data lake, W5B kopiuje go do Redshifta
-- -----------------------------------------------------------------------------
-- W5A (ponizej) idzie przez awsdatacatalog. Nie wymaga niczego poza polaczeniem
--     jako Federated user, wiec przechodzi takze wtedy, gdy COPY jest odciete
--     przez uprawnienia. Od niego zaczynamy, zeby nikt nie utknal na starcie.
-- W5B to DDL i COPY: dane trafiaja do magazynu Redshifta i dopiero wtedy widac,
--     czym data warehouse rozni sie od silnika czytajacego pliki. To jest wlasciwa
--     tresc cwiczenia i wymaga uprawnien opisanych w sekcji UPRAWNIENIA na dole.
-- Obie drogi prowadza do tej samej liczby: 1427124.00.
--
-- Dwa warunki, ktore musza byc spelnione, zanim COPY ma szanse przejsc:
--
-- 1) SCHEMAT JEST WLASNY. Cala sala pracuje na jednym workgroupie i jednej
--    bazie `dev`. Przy `CREATE SCHEMA gold` pierwsza osoba go tworzy, druga
--    dostaje "schema already exists", a trzecia wlewa dane do cudzych tabel -
--    bez zadnego bledu, po prostu z podwojonymi liczbami. Stad gold_<inicjaly>,
--    dokladnie jak bucket i baza w Athenie.
--
-- 2) ROLA IAM WORKGROUPU CZYTA TEN BUCKET. To jest krok, na ktorym W5 pada
--    najczesciej - opis bledu i warianty naprawy na dole pliku.
--
-- TYPY musza odpowiadac plikom Parquet z warstwy Gold, bo COPY FORMAT AS PARQUET
-- dopasowuje kolumny po KOLEJNOSCI I TYPIE, nie po nazwie:
--   *_sk, year, month, day -> int              (w Athenie jawny CAST AS integer)
--   kwoty                  -> double precision (w data lake sa double)
--   teksty                 -> varchar(n)       (Parquet trzyma string bez dlugosci)
-- W produkcji kwoty trzymaloby sie jako DECIMAL(12,2), zeby uniknac bledow
-- zaokraglen przy sumowaniu milionow wierszy. Rzutowanie musialoby wtedy nastapic
-- juz w CTAS-ie warstwy Silver, nie dopiero tutaj - bo typ w pliku Parquet jest
-- wczesniejszy niz typ w tabeli docelowej.
--
-- Wszystkie CREATE maja IF NOT EXISTS, wiec DDL mozna uruchomic ponownie.
-- Sam COPY idempotentny NIE JEST - dwa uruchomienia to podwojone wiersze.
-- Przed powtorka: TRUNCATE tabeli.
-- =============================================================================


-- #############################################################################
-- W5A. Warstwa Gold czytana z data lake - bez kopiowania
-- #############################################################################
-- Redshift Serverless montuje katalog Glue tego konta jako baze tylko do
-- odczytu o nazwie awsdatacatalog. Nie trzeba jej tworzyc ani konfigurowac -
-- jest to samo zrodlo, z ktorego czyta Athena, ogladane innym silnikiem.
--
-- Sprawdzenie, czy montowanie jest wlaczone na tym workgroupie:
--   SHOW data_catalog_auto_mount;
-- Wynik `on` oznacza, ze ponizsze zapytania maja szanse dzialac. Przy `off`
-- wlacza sie je jako administrator bazy:
--   ALTER SYSTEM SET data_catalog_auto_mount = on;
-- - z tym zastrzezeniem, ze na Serverless zmiana wchodzi dopiero po uspieniu
-- i wybudzeniu workgroupu, a nie natychmiast.
--
-- Lista widocznych baz Glue i tabel w naszej bazie:
--   SHOW SCHEMAS FROM DATABASE awsdatacatalog;
--   SHOW TABLES FROM SCHEMA awsdatacatalog.aurora_jk;
--
-- Cudzyslowy wokol kazdego czlonu nie sa obowiazkowe, ale sa zalecane: nazwy
-- baz Glue bywaja pisane wielka litera albo z myslnikiem, a wtedy bez
-- cudzyslowow parser Redshifta rozjezdza sie na czlonach.
--
-- Ograniczenia, ktore warto znac od razu:
--   - awsdatacatalog jest TYLKO DO ODCZYTU - zaden INSERT, UPDATE ani CTAS
--     nie zapisze niczego do bazy Glue,
--   - zapis trzyczlonowy jest OBOWIAZKOWY; `USE` i `search_path` dla tej bazy
--     nie dzialaja, wiec skrocic go do dwoch czlonow sie nie da,
--   - wymagane jest polaczenie jako Federated user.

-- To samo pytanie co W4 pkt 1, tyle ze zadane z Redshifta. Kwoty musza byc
-- identyczne jak w Athenie:
--   15.01: Telefony 199197 | Komputery 152821 | TV 142358 | AGD 117153 |
--          Audio 36870 | Akcesoria 5132        -> suma dnia 653531.00
--   16.01: Komputery 209784 | Telefony 182780 | TV 182442 | AGD 143912 |
--          Audio 46384 | Akcesoria 8291        -> suma dnia 773593.00
-- Suma calosci: 1427124.00
SELECT
  d.date_actual,
  p.category,
  ROUND(SUM(f.net_amount_pln), 2) AS revenue_pln
FROM "awsdatacatalog"."aurora_jk"."gold_fact_order_item" f
JOIN "awsdatacatalog"."aurora_jk"."gold_dim_date" d
  ON f.date_sk = d.date_sk
JOIN "awsdatacatalog"."aurora_jk"."gold_dim_product" p
  ON f.product_sk = p.product_sk
GROUP BY d.date_actual, p.category
ORDER BY d.date_actual, revenue_pln DESC;

-- Rekoncyliacja Silver = Gold, takze z data lake. Oczekiwane: 1427124.00
-- w obu wierszach. Warstwa Silver jest w tej samej bazie Glue, wiec sposob
-- adresowania jest ten sam.
SELECT 'silver' AS layer, ROUND(SUM(net_amount_pln), 2) AS revenue_pln
FROM "awsdatacatalog"."aurora_jk"."silver_order_items"
UNION ALL
SELECT 'gold', ROUND(SUM(net_amount_pln), 2)
FROM "awsdatacatalog"."aurora_jk"."gold_fact_order_item"
ORDER BY layer;

-- Wariant posredni, ZAMIAST calego W5B, gdy COPY jest odciety uprawnieniami:
-- Redshift czyta pliki przez katalog Glue i zapisuje wynik do wlasnej tabeli.
-- Dane laduja tam, gdzie mial je polozyc COPY, ale droga do nich idzie
-- tozsamoscia zalogowanego uzytkownika, nie rola workgroupu.
--   CREATE SCHEMA IF NOT EXISTS gold_jk;
--   CREATE TABLE gold_jk.fact_order_item AS
--   SELECT * FROM "awsdatacatalog"."aurora_jk"."gold_fact_order_item";
-- To jest ALTERNATYWA dla DDL i COPY z W5B, nie dodatek: po uruchomieniu obu
-- CREATE TABLE zderzy sie z juz istniejaca tabela. Zastrzezenie merytoryczne:
-- tak zbudowana tabela dziedziczy typy z Parquetu (miedzy innymi bigint tam,
-- gdzie DDL z W5B ma int) i nie ma SORTKEY - wiec zastepuje COPY jako droga
-- do danych, ale nie jako lekcja o typach i rozkladzie danych miedzy wezly.


-- #############################################################################
-- W5B. Warstwa Gold skopiowana do magazynu Redshifta
-- #############################################################################
-- Od tego miejsca wszystkie nazwy sa DWUCZLONOWE i wskazuja LOKALNY schemat
-- w bazie `dev`: gold_<inicjaly>.tabela. To juz nie jest data lake.

CREATE SCHEMA IF NOT EXISTS gold_jk;

-- PRIMARY KEY w Redshifcie jest INFORMACYJNY: optymalizator z niego korzysta,
-- ale silnik go NIE EGZEKWUJE. Duplikat da sie wstawic mimo deklaracji, wiec
-- za spojnosc odpowiadaja zapytania kontrolne na dole pliku.
CREATE TABLE IF NOT EXISTS gold_jk.dim_date (
  date_sk int PRIMARY KEY,
  date_actual date,
  year int,
  month int,
  day int,
  day_name varchar(20),
  is_weekend boolean
);

CREATE TABLE IF NOT EXISTS gold_jk.dim_customer (
  customer_sk int PRIMARY KEY,
  customer_id varchar(16),
  first_name varchar(64),
  last_name varchar(64),
  city varchar(64),
  voivodeship varchar(64),
  segment varchar(32),
  signup_date date,
  valid_from date,
  valid_to date,
  is_current boolean
);

CREATE TABLE IF NOT EXISTS gold_jk.dim_product (
  product_sk int PRIMARY KEY,
  product_id int,
  sku varchar(32),
  product_name varchar(128),
  category varchar(64),
  brand varchar(64),
  list_price_pln double precision
);

CREATE TABLE IF NOT EXISTS gold_jk.dim_channel (
  channel_sk int PRIMARY KEY,
  channel_code varchar(32)
);

-- DISTSTYLE AUTO - Redshift sam decyduje o rozkladzie wierszy miedzy wezly.
--   KEY = rozklad po kolumnie zlaczenia (dobry dla duzych faktow: wiersze faktu
--         i wymiaru o tym samym kluczu trafiaja na ten sam wezel, wiec zlaczenie
--         nie wymaga przesylania danych miedzy wezlami),
--   ALL = kopia calej tabeli na kazdym wezle (dobry dla malych wymiarow),
--   EVEN = rownomiernie, bez ogladania sie na klucze.
-- SORTKEY (date_sk) - fizyczna kolejnosc zapisu na dysku. Filtr po dacie moze
-- wtedy pomijac cale bloki przy odczycie; data jest tu najczesciej filtrowana
-- kolumna. To odpowiednik partycji z Atheny, tylko wewnatrz silnika.
--
-- Tabela ma 9 kolumn, nie 10. Kolumna partycjonujaca dt z Atheny NIE JEST
-- zapisana w pliku Parquet - siedzi w nazwie katalogu (dt=2026-01-15/). To
-- ogolna wlasciwosc partycjonowania w stylu Hive. Dziesiata kolumna dalaby
-- blad o liczbie kolumn przy COPY.
CREATE TABLE IF NOT EXISTS gold_jk.fact_order_item (
  order_id varchar(16),
  date_sk int,
  customer_sk int,
  product_sk int,
  channel_sk int,
  qty int,
  unit_price_pln double precision,
  discount_pln double precision,
  net_amount_pln double precision
)
DISTSTYLE AUTO
SORTKEY (date_sk);

-- ---------------------------------------------------------------------------
-- COPY - ladowanie z S3
-- ---------------------------------------------------------------------------
-- Zrodlem jest warstwa GOLD, nigdy Silver. Silver nie ma surrogate keys, a COPY
-- dopasowuje kolumny po kolejnosci - przy ladowaniu z silver/products/ kolumna
-- product_id wyladowalaby w product_sk. Bez bledu, po prostu z cichym
-- przeklamaniem danych.
--
-- COPY laduje pliki ROWNOLEGLE, na wszystkich wezlach naraz - to jest powod,
-- dla ktorego uzywa sie go zamiast INSERT-ow. Przy 409 wierszach roznicy nie ma;
-- przy 400 milionach roznica to godziny.
--
-- IAM_ROLE default oznacza role oznaczona w namespace jako domyslna. Jesli
-- workgroup ma podpieta wiecej niz jedna role, ARN podaje sie wprost (patrz
-- sekcja o Access Denied).
--
-- LICZBA URUCHOMIEN MA ZNACZENIE: COPY nie jest idempotentny. Drugie wykonanie
-- doklada te same wiersze jeszcze raz.

COPY gold_jk.dim_date
FROM 's3://aurora-jk-lake/gold/dim_date/'
IAM_ROLE default
FORMAT AS PARQUET;

COPY gold_jk.dim_channel
FROM 's3://aurora-jk-lake/gold/dim_channel/'
IAM_ROLE default
FORMAT AS PARQUET;

COPY gold_jk.dim_customer
FROM 's3://aurora-jk-lake/gold/dim_customer/'
IAM_ROLE default
FORMAT AS PARQUET;

COPY gold_jk.dim_product
FROM 's3://aurora-jk-lake/gold/dim_product/'
IAM_ROLE default
FORMAT AS PARQUET;

-- Sciezka bez wskazania partycji - COPY czyta rekurencyjnie oba katalogi
-- dt=2026-01-15/ i dt=2026-01-16/. Sama wartosc dt do tabeli nie wchodzi.
COPY gold_jk.fact_order_item
FROM 's3://aurora-jk-lake/gold/fact_order_item/'
IAM_ROLE default
FORMAT AS PARQUET;

-- ---------------------------------------------------------------------------
-- KONTROLA
-- ---------------------------------------------------------------------------
-- Oczekiwane (kolejnosc z ORDER BY t): dim_channel 3 | dim_customer 80 |
-- dim_date 2 | dim_product 25 | fact_order_item 409 - dokladnie tyle samo
-- co w Athenie.
--
-- Liczby podwojone (6 | 160 | 4 | 50 | 818) oznaczaja COPY uruchomiony dwa razy.
-- Naprawa: TRUNCATE gold_jk.<tabela>; i COPY jeszcze raz. TRUNCATE jest szybszy
-- od DELETE i zwalnia miejsce od razu, ale wykonuje niejawny COMMIT - wewnatrz
-- transakcji zachowuje sie inaczej niz DELETE.
--
-- ORDER BY na koncu, bo UNION ALL - tak samo jak w Athenie - nie gwarantuje
-- kolejnosci wierszy.
SELECT 'dim_date' AS t, COUNT(*) FROM gold_jk.dim_date
UNION ALL SELECT 'dim_channel', COUNT(*) FROM gold_jk.dim_channel
UNION ALL SELECT 'dim_customer', COUNT(*) FROM gold_jk.dim_customer
UNION ALL SELECT 'dim_product', COUNT(*) FROM gold_jk.dim_product
UNION ALL SELECT 'fact_order_item', COUNT(*) FROM gold_jk.fact_order_item
ORDER BY t;

-- To samo pytanie co W4 pkt 1. Kwoty musza byc identyczne jak w Athenie:
--   15.01: Telefony 199197 | Komputery 152821 | TV 142358 | AGD 117153 |
--          Audio 36870 | Akcesoria 5132        -> suma dnia 653531.00
--   16.01: Komputery 209784 | Telefony 182780 | TV 182442 | AGD 143912 |
--          Audio 46384 | Akcesoria 8291        -> suma dnia 773593.00
-- Suma calosci: 1427124.00
--
-- Dwa rozne silniki, dwa rozne modele przechowywania, ta sama liczba. To jest
-- dowod, ze model wymiarowy jest niezalezny od technologii, a wybor silnika jest
-- decyzja o kosztach i wzorcu uzycia, nie o poprawnosci.
SELECT d.date_actual, p.category, ROUND(SUM(f.net_amount_pln), 2) AS revenue_pln
FROM gold_jk.fact_order_item f
JOIN gold_jk.dim_date d ON f.date_sk = d.date_sk
JOIN gold_jk.dim_product p ON f.product_sk = p.product_sk
GROUP BY d.date_actual, p.category
ORDER BY d.date_actual, revenue_pln DESC;

-- ---------------------------------------------------------------------------
-- Gdy COPY zwraca "S3ServiceException: Access Denied"
-- ---------------------------------------------------------------------------
-- To nie jest literowka w sciezce ani brak pliku. Sciezka i uprawnienia to dwie
-- rozne rzeczy i warto sprawdzac je osobno.
--
-- Domyslny workgroup `default-workgroup` ma zwykle podpieta role
-- AmazonRedshift-CommandsAccessRole-*, a ta rola ma polityke
-- AmazonRedshiftAllCommandsFullAccess. Ta polityka daje dostep do S3 WYLACZNIE
-- dla bucketow, ktorych nazwa zawiera "redshift" (albo obiektow z tagiem
-- Redshift=true). Nasz bucket nazywa sie aurora-<inicjaly>-lake, wiec odmowa
-- jest zgodna z polityka - mimo ze bucket lezy w tym samym koncie i regionie.
--
-- Odblokowanie wymaga uprawnien Administratora, wiec jest robione RAZ DLA CALEJ
-- SALI, jednym z trzech sposobow:
--   a) dolozenie AmazonS3ReadOnlyAccess do roli workgroupu - najprostsze,
--   b) wlasny namespace i workgroup z rola majaca dostep do S3 - czystsze,
--      ale wymaga odczekania na provisioning (2-5 minut),
--   c) nazwa bucketu zawierajaca "redshift" (aurora-<inicjaly>-lake-redshift) -
--      brzydkie, ale nie rusza IAM w ogole.
--
-- Z roli PowerUserAccess tego kroku nie da sie wykonac samodzielnie. To jedyne
-- takie miejsce w calym kursie.
--
-- Sprawdzenie uprawnien bez zgadywania (IAM -> Policy simulator albo CLI):
--   aws iam simulate-principal-policy \
--     --policy-source-arn arn:aws:iam::123456789012:role/service-role/<rola> \
--     --action-names s3:GetObject s3:ListBucket \
--     --resource-arns arn:aws:s3:::aurora-jk-lake/gold/dim_date/x.parquet
--
-- Gdyby IAM_ROLE default nie zadzialalo mimo poprawnych uprawnien, ARN roli
-- podaje sie wprost:
--   IAM_ROLE 'arn:aws:iam::123456789012:role/service-role/<rola>'
-- ARN jest widoczny w Redshift -> Namespace -> Security and encryption.
--
-- Alternatywa bez COPY: W5A na poczatku tego pliku. Warto zauwazyc, ze te dwie
-- drogi NIE ida tymi samymi uprawnieniami, mimo ze czytaja te same pliki:
--   COPY                     -> rola IAM podpieta do namespace'u / workgroupu,
--   awsdatacatalog (W5A)     -> tozsamosc zalogowanego uzytkownika federacyjnego.
-- Domyslna rola konsolowa jest ograniczona do bucketow z "redshift" w nazwie, a druga
-- to PowerUserAccess z pelnym odczytem S3 i Glue. Stad obserwacja, ktora wyglada
-- na sprzecznosc, a sprzecznoscia nie jest: COPY zwraca Access Denied, a SELECT
-- przez awsdatacatalog z tego samego bucketu przechodzi.
--
-- Trzecia droga, wymieniona dla porzadku: jawny CREATE EXTERNAL SCHEMA (klasyczny
-- Redshift Spectrum). Ten wariant znow idzie rola IAM, wiec bez dodatkowej polityki S3 podlega temu
-- samemu ograniczeniu co COPY - i dlatego nie jest sciezka domyslna tego cwiczenia.

-- ---------------------------------------------------------------------------
-- UPRAWNIENIA - trzy warstwy, ktore latwo ze soba pomylic
-- ---------------------------------------------------------------------------
-- Kazda z nich moze zawiesc osobno i kazda daje inny komunikat. Sprawdzanie ich
-- w tej kolejnosci oszczedza najwiecej czasu.
--
-- (a) UPRAWNIENIA BAZODANOWE W REDSHIFCIE (GRANT, wykonywany w SQL)
--     Dotycza tego, co wolno roli lub uzytkownikowi WEWNATRZ bazy `dev`.
--       - odczyt katalogu Glue:
--           GRANT USAGE ON DATABASE awsdatacatalog TO "IAMR:<nazwa-roli-SSO>";
--         a gdyby uzytkownik nie istnial jeszcze w bazie:
--           CREATE USER "IAMR:<nazwa-roli-SSO>" WITH PASSWORD DISABLE;
--         Wykonuje to administrator bazy, polaczony jako Database user name
--         and password. Wlasna tozsamosc do wpisania w cudzyslow daje
--           SELECT current_user;
--       - wlasny schemat Gold: jego tworca jest wlascicielem i nie potrzebuje
--         zadnego GRANT-a. Udostepnienie go innej osobie to
--           GRANT USAGE ON SCHEMA gold_<inicjaly> TO "IAMR:<ktos>";
--           GRANT SELECT ON ALL TABLES IN SCHEMA gold_<inicjaly> TO "IAMR:<ktos>";
--     Objaw braku: "permission denied for database awsdatacatalog" albo
--     "permission denied for schema ...". Nie ma tu ani slowa o S3.
--
-- (b) ISTNIENIE OBRAZU KATALOGU GLUE W REDSHIFCIE
--     Dotyczy tego, czy baza awsdatacatalog w ogole jest widoczna.
--       SHOW data_catalog_auto_mount;      -- oczekiwane: on
--       ALTER SYSTEM SET data_catalog_auto_mount = on;   -- gdy off
--     Na Serverless zmiana wchodzi po uspieniu i wybudzeniu workgroupu.
--     Warunek drugi: polaczenie jako Federated user (baza `dev`).
--     Wariant jawny, gdy montowanie jest niedostepne albo gdy zamiast trzech
--     czlonow ma byc zwykly schemat:
--       CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_<inicjaly>
--       FROM DATA CATALOG DATABASE 'aurora_<inicjaly>'
--       IAM_ROLE 'arn:aws:iam::123456789012:role/service-role/<rola>';
--     Uwaga: ten wariant czyta S3 rola IAM, nie tozsamoscia uzytkownika, wiec
--     bez uprawnien S3 na roli workgroupu pada tak samo jak COPY.
--     Objaw braku: ERROR: database "awsdatacatalog" does not exist, albo brak
--     tej pozycji w lewym drzewie QEv2.
--
-- (c) UPRAWNIENIA IAM DO S3 I GLUE
--     Dotycza dostepu do samych plikow i metadanych, poza baza danych.
--     Potrzebne akcje: s3:GetObject i s3:ListBucket na aurora-<inicjaly>-lake
--     oraz glue:GetDatabase, glue:GetDatabases, glue:GetTable, glue:GetTables
--     i glue:GetPartitions.
--     Komu je nadac, zalezy od drogi:
--       droga W5A            -> roli SSO, ktora jest zalogowany kursant
--                               (PowerUserAccess juz je ma),
--       COPY / EXTERNAL SCHEMA -> roli IAM podpietej do namespace'u.
--     Gdyby katalog byl zarzadzany przez Lake Formation, doszlaby jeszcze
--     akcja lakeformation:GetDataAccess i grant LF na baze. W domyslnym trybie
--     IAM_ALLOWED_PRINCIPALS samo IAM wystarcza.
--     Objaw braku: S3ServiceException: Access Denied.

-- ---------------------------------------------------------------------------
-- Inne bledy, ktore moga sie tu pojawic
-- ---------------------------------------------------------------------------
--   ERROR: schema "aurora_jk" does not exist  -> zapytanie Athenowe wklejone do
--                                                Redshifta; forma 3 zamiast 1
--                                                albo 2 (patrz naglowek pliku)
--   ERROR: database "awsdatacatalog" does not
--   exist                                     -> polaczenie inne niz Federated
--                                                user albo wylaczone
--                                                data_catalog_auto_mount
--   "permission denied for database
--   awsdatacatalog"                           -> brak GRANT USAGE, warstwa (a)
--   "Column type mismatch" / blad o int64          -> brak CAST(... AS integer)
--                                                     w W3_elt_athena.sql
--   blad o liczbie kolumn na fact_order_item       -> tabela ma 10 kolumn
--                                                     zamiast 9 (kolumna dt)
--   "schema ... already exists"                    -> schemat bez inicjalow
--   liczby dokladnie dwa razy wieksze              -> COPY uruchomiony dwukrotnie
--   "relation ... does not exist" w lewym drzewie  -> drzewo QEv2 wymaga
--                                                     odswiezenia galezi

-- Po zajeciach: DROP SCHEMA gold_<inicjaly> CASCADE. Workgroup i namespace
-- kasuje osoba, ktora je stawiala - RPU licza sie same, niezaleznie od tego,
-- czy ktos jeszcze pyta.
