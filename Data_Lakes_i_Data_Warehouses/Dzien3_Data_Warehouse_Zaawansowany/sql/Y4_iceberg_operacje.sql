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
