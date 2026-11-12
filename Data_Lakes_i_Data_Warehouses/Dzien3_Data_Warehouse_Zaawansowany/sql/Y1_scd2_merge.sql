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
