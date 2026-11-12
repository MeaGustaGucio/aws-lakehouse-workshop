# Data Lakes & Data Warehouses — materiały do warsztatu

Materiały warsztatowe Data Lakes i Data Warehouses (AWS) · **3 sesje × 3 godziny** · praktyka w konsoli AWS

Cała praca odbywa się w przeglądarce, w konsoli AWS (konto warsztatowe / sandbox AWS, np. z logowaniem federacyjnym IAM Identity Center). Nie ma nic do zainstalowania lokalnie: żadnego Dockera, Jupytera ani klastra.

---

## Co jest w tym pakiecie

```text
Data_Lakes_i_Data_Warehouses/
  README.md                              ten plik
  00_Sciagawka_SQL.md                    składnia Athena + Redshift na wszystkie 3 dni
                                         (sekcje 12–14: Iceberg, Redshift i SQL analityczny z wersji zaawansowanej)
  Dzien1_Data_Lakes/
    01_zadania.md                        przebieg dnia, ćwiczenia L0–L6 + zadania dodatkowe
                                         + „Dodatki po zajęciach” D1–D6 (opcjonalne, niepotrzebne w dniu 2 i 3)
    02_rozwiazania.md                    rozwiązania z liczbami kontrolnymi i wyjaśnieniami
    sala.html                            dwupanelowy podgląd offline: zadania i rozwiązania obok siebie
    dane/bronze/…                        7 plików w docelowym układzie prefiksów S3
    sql/…                                jeden plik = jedno ćwiczenie: L3_bronze_ddl.sql, L4_…, L5_…, L6_…, S1_…, S2_…, D1_…–D5_…
  Dzien2_Data_Warehouse/
    01_zadania.md                        ćwiczenia W1–W5
    02_rozwiazania.md
    sala.html                            dwupanelowy podgląd offline w przeglądarce
    sql/…                                W2_ddl_star.sql, W3_elt_athena.sql, W4_pytania_gold.sql, W5_redshift.sql
  Dzien3_Data_Warehouse/
    01_zadania.md                        ćwiczenia W6–W10
    02_rozwiazania.md
    sala.html                            dwupanelowy podgląd offline w przeglądarce
    dane/bronze/…                        dopływ danych na dzień 3
    sql/…                                W6_scd2_customer.sql, W7_incremental.sql, W8_most_lake_model.sql, W9_pytania_dyrektora.sql
  Dzien2_Data_Warehouse_Zaawansowany/    wersja zaawansowana — alternatywa dla Dzien2_Data_Warehouse
    01_zadania.md                        ćwiczenia Z0–Z7: star schema na Icebergu + Redshift z własnym DDL
    02_rozwiazania.md                    rzeczywiste wyniki z konsoli AWS i omówienie pułapek
    sala.html                            dwupanelowy podgląd offline w przeglądarce
    dane/bronze/shipments/               dodatkowe źródło (wysyłki) tylko dla tej wersji
    sql/…                                Z0_stan_wejsciowy.sql … Z6_redshift.sql, Z6g_athena_po_unload.sql
  Dzien3_Data_Warehouse_Zaawansowany/    wersja zaawansowana — alternatywa dla Dzien3_Data_Warehouse
    01_zadania.md                        ćwiczenia Y0–Y6: SCD2 przez MERGE, time travel, OPTIMIZE, capstone
    02_rozwiazania.md
    sala.html                            dwupanelowy podgląd offline w przeglądarce
    dane/bronze/…                        dopływ dnia 3 (customers_updates, orders 20.01, pozycje) + shipments_20260121/
    sql/…                                Y0_stan_wejsciowy.sql … Y6_capstone_i_sprzatanie.sql
```

### Dwie wersje na dni 2 i 3

Dzień 1 jest wspólny. Na dni 2 i 3 do wyboru jest **jedna z dwóch wersji** — obie
startują z tego samego stanu po dniu 1 i kończą tą samą kwotą kontrolną, różnią się
głębokością inżynieryjną:

| | Wersja podstawowa | Wersja zaawansowana |
|---|---|---|
| Foldery | `Dzien2_Data_Warehouse/`, `Dzien3_Data_Warehouse/` | `Dzien2_Data_Warehouse_Zaawansowany/`, `Dzien3_Data_Warehouse_Zaawansowany/` |
| Format tabel Gold | Parquet (CTAS) | **Iceberg** (MERGE, time travel, OPTIMIZE, VACUUM) |
| Redshift | demo + Spectrum | pełne ćwiczenie: DISTKEY/SORTKEY, EXPLAIN, materialized view, UNLOAD |
| SCD2 | `UNION ALL` + podmiana tabeli | dwa kroki `MERGE` + `INSERT`, wersja sprzed zmiany przez time travel |
| Schemat w Redshifcie | `gold_<inicjaly>` | `dw_<inicjaly>` |

Nie ma potrzeby czytać obu — wystarczy folder tej wersji, która została wybrana.

Dane leżą już w takim układzie katalogów, w jakim mają wylądować na S3, więc upload
sprowadza się do jednego *Upload → Add folder → bronze*. Nie ma potrzeby odtwarzania
struktury klikaniem.

---

## Rozwiązania są w pakiecie od pierwszego dnia

Każdy dzień ma obok zadań plik `02_rozwiazania.md` — to **jedna ścieżka czytania „jak”**:
zdanie, co powstało, pełny SQL w bloku i liczby kontrolne z etykietą. Katalog `sql/`
trzyma **ten sam** SQL, jeden plik na ćwiczenie (`sql/L3_bronze_ddl.sql`,
`sql/Z1_wymiar_daty.sql`, `sql/W6_scd2_customer.sql`, …), do otwarcia w edytorze.
Nic nie jest schowane — i to jest świadoma decyzja, bo najwięcej wynosi się
z porównania własnego rozwiązania z wzorcowym, a nie z samego przeczytania cudzego.

Sposób korzystania, który się sprawdza:

1. Najpierw własna próba na podstawie `01_zadania.md` i ściągawki SQL.
2. Dopiero potem `02_rozwiazania.md` — sekcja odpowiadająca temu ćwiczeniu.
3. Liczby kontrolne (`### Kontrola`) do sprawdzenia, czy wynik jest ten sam.
   Rozjazd liczb jest ciekawszy niż zgodność: w rozwiązaniach opisane jest, co
   najczęściej go powoduje.

Skrypty z `sql/` można wkleić do Query editor, ale warto najpierw przeczytać
komentarze w `02_rozwiazania.md` — one wyjaśniają, dlaczego instrukcja wygląda tak,
a nie inaczej. Same wyniki bez tego niewiele wnoszą.

---

## Kolejność dni

| Dzień | Temat | Co powstaje |
|---|---|---|
| 1 | Data lake na S3 + Athena | bucket, warstwa Bronze, tabele zewnętrzne, Silver w Parquecie |
| 2 | Data warehouse: model Kimballa | star schema w warstwie Gold, ELT ze Silver, ten sam model w Redshift Serverless |
| 3 | SCD2, ładowanie przyrostowe, lakehouse | historia zmian w wymiarze, nowy dzień sprzedaży, most między warstwami, sprzątanie |

Dni **spinają się w jeden ciąg** — dzień 2 czyta dokładnie te tabele, które powstają
w dniu 1, a dzień 3 rozbudowuje model z dnia 2. Między sesjami bucket i tabele
**zostają**; kasujemy je świadomie na końcu dnia 3 (ćwiczenie W10 w wersji podstawowej,
sekcja „Sprzątanie” w Y6 w wersji zaawansowanej).

Gdyby zasoby z poprzedniego dnia zniknęły, do odtworzenia Bronze i Silver wystarczą
pliki z `dane/` oraz skrypty z `sql/`. Zajmuje to około 20 minut.

---

## Zasada nazewnictwa zasobów

W środowisku wieloosobowym konto AWS bywa współdzielone, a nazwy bucketów S3 są globalne
w całym AWS. Dlatego każdy zasób dostaje w nazwie własne inicjały:

| Zasób | Wzorzec | Przykład |
|---|---|---|
| bucket S3 | `aurora-<inicjaly>-lake` | `aurora-jk-lake` |
| baza w Athenie | `aurora_<inicjaly>` | `aurora_jk` |
| schemat w Redshifcie | `gold_<inicjaly>` (wersja podstawowa) / `dw_<inicjaly>` (wersja zaawansowana) | `gold_jk` / `dw_jk` |

W przykładach w rozwiązaniach występuje `aurora_jk` / `aurora-jk-lake` — te nazwy
podmieniamy na własne. W Redshifcie ta zasada jest szczególnie istotna przy pracy
wieloosobowej na jednej bazie `dev`, bo schemat `gold` bez inicjałów zderzy się z czyimś.

Jedna rzecz, która zaskakuje w dniu 2: **ta sama tabela nazywa się inaczej
w zależności od silnika.** Athena i Redshift patrzą na jeden Glue Data Catalog,
ale adresują go różnie:

| Skąd pytamy | Jak nazywa się warstwa Gold |
|---|---|
| Athena | `aurora_<inicjaly>.gold_fact_order_item` |
| Redshift, dane zostają na S3 | `"awsdatacatalog"."aurora_<inicjaly>"."gold_fact_order_item"` |
| Redshift, dane skopiowane instrukcją `COPY` | `gold_<inicjaly>.fact_order_item` |

Zapis Athenowy użyty w Redshifcie kończy się komunikatem
`ERROR: schema "aurora_jk" does not exist`. Pełne wyjaśnienie jest
w `00_Sciagawka_SQL.md` (sekcja 10) i w nagłówku każdego pliku `sql/`, który
zaczyna się od linii `-- SILNIK: …`.

Region na wszystkie trzy dni: **`eu-central-1` (Frankfurt)**. S3, Glue i Athena są
regionalne, więc bucket utworzony we Frankfurcie nie pojawi się na liście, gdy konsola
pokazuje Irlandię.

---

## Zanim zaczniemy dzień 1

- Dostęp do AWS Management Console (własny sandbox AWS lub konto warsztatowe IAM Identity
  Center z rolą typu **PowerUserAccess** — wystarcza na całe zajęcia).
- Otwarta ściągawka `00_Sciagawka_SQL.md` obok zadań. Zawiera wzorce składni
  Atheny i Redshifta oraz tabelę najczęstszych komunikatów błędów z przyczynami.
- Własne inicjały ustalone i zapisane. Wracają w każdej nazwie przez trzy dni.
