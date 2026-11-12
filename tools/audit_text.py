"""Audit: wording rules for the package handed to students.

Three classes of regression are easy to reintroduce during an edit and expensive on
the day of the workshop:

1. Text claiming solutions are trainer-only. Solutions ship with the package now, so
   any such sentence contradicts what the students are holding.
2. Polonised technical terms. The trainer teaches `data lake`, `grain`, `star schema`,
   `surrogate key`, `natural key` — the Polish calques make the terms unsearchable and
   collide with how they are used at work.
3. Imperative mood. The whole package is written in a collaborative register
   (`Do zrobienia:`, `wchodzimy do…`, `warto…`), and a single `kliknij` breaks it.
4. Exercises described as compulsory. The course has no pass/fail gate, so
   "obowiązkowe", "nie jest opcją" and "zaliczenie" promise something that does not
   exist. Cleanup matters because the account is shared, not because it is ordered.

Findings in the student package are errors. Findings in the trainer folders are only
reported for the first class of rules, and only for the strings that no longer describe
reality (stale paths, "solutions stay with you").

A fifth class is about SQL rather than wording, and is checked separately below:
the course runs two engines against one Glue Data Catalog, and they address the same
table differently. Athena takes `aurora_xx.gold_fact_order_item`; Redshift needs
`"awsdatacatalog"."aurora_xx"."gold_fact_order_item"` and answers the two-part form
with `ERROR: schema "aurora_xx" does not exist`. Mixing the two is invisible on
reading and fatal in the room, so every `.sql` file declares its engine in the header
and the naming form is verified against that declaration.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "Data_Lakes_i_Data_Warehouses"
TRAINER = [ROOT / "README.md", ROOT / "tools"]

# --- 1. solutions-are-secret phrasing, plus paths from the pre-merge layout ----------
SECRECY = [
    (r"nie wysyłaj\w*", "rozwiązania są w pakiecie kursanta"),
    (r"materiał trenera", "plik jest teraz dla kursantów"),
    (r"[Tt]ylko Ty\b", "nie ma już materiału „tylko dla trenera” w pakiecie"),
    (r"zostaje u Ciebie", "rozwiązania idą do kursantów"),
    (r"trainer only", "rozwiązania idą do kursantów"),
    (r"[Rr]ozwiązania pokazuj dopiero", "rozwiązania są od początku u kursantów"),
    (r"BEZ rozwiąza", "rozwiązania idą razem z zadaniami"),
    (r"rozwiązania po zajęciach", "rozwiązania idą przed zajęciami"),
    (r"03_Rozwiazania", "nieaktualna ścieżka"),
    (r"04_Dla_Kursantow", "nieaktualna ścieżka"),
    (r"dane_do_S3", "nieaktualna ścieżka (teraz `dane/`)"),
]

# --- 2. Polonised terms -------------------------------------------------------------
POLONIZED = [
    (r"jezior\w+", "data lake"),
    (r"hurtowni\w*", "data warehouse"),
    (r"\bziarn\w+", "grain"),
    (r"(?:schema\w*|model\w*|schemat\w*)\s+gwiazd\w+", "star schema"),
    (r"\bgwiazd[ay]\b", "star schema"),
    (r"klucz\w*\s+(?:sztuczn|zastępcz|naturaln)\w+", "surrogate key / natural key"),
    (r"(?:sztuczn|zastępcz|naturaln)\w+\s+klucz\w*", "surrogate key / natural key"),
]

# --- 3. imperative mood -------------------------------------------------------------
# Only unambiguous 2nd-person-singular imperatives, so that infinitives ("trzeba
# zaznaczyć") and nouns ("Sprawdzenie", "Kontrola") do not trip the check.
IMPERATIVE = [
    "zrób", "zróbcie", "kliknij", "klikaj", "sprawdź", "wpisz", "utwórz", "wejdź",
    "otwórz", "zapisz", "uruchom", "porównaj", "policz", "skasuj", "usuń", "opróżnij",
    "zanotuj", "zaznacz", "odpal", "rozdaj", "pamiętaj", "musisz", "wybierz",
    "przejdź", "dodaj", "wklej", "pokaż", "zapytaj", "nie ruszaj",
]
IMPERATIVE_RULES = [(rf"\b{w}\b", "tryb rozkazujący") for w in IMPERATIVE]
IMPERATIVE_RULES.append((r"\bkaza[ćjłl]\w*", "tryb rozkazujący"))

# Sentences quoted from the AWS console or from an error message are descriptions of
# somebody else's UI text, not instructions addressed to the reader.
ALLOWED_LINES = re.compile(r"^\s*(?:--|#)?\s*(?:\||```)")

# --- 4. exercises framed as compulsory ----------------------------------------------
# There is no pass/fail gate on this course, so nothing in the package may present an
# exercise as a requirement or as a condition of finishing the day. Cleanup still has
# to be worth doing, but the argument is the shared account and somebody else's bill,
# not an order given to the reader.
#
# `obowiązkowy` itself has to stay available, because the engines really do demand
# things: the three-part name in Redshift, the explicit CAST that Parquet will not do
# for you, the DISTINCT that keeps a dimension from multiplying the fact. Those are
# statements about SQL, not about what the student owes anyone. So the word is only a
# finding when the same line also talks about an exercise, a task, cleanup or a prefix.
MANDATORY_WORD = re.compile(r"obowiązkow\w*|przymusow\w*", re.I)
MANDATORY_TOPIC = re.compile(
    r"ćwiczeni|zadani|sprząta|sprzątani|cleanup|prefiks|prefix|\bW\d{1,2}\b|\bL\d\b",
    re.I,
)
# Framing that is a finding wherever it appears, because it has no technical reading.
MANDATORY_ALWAYS = [
    (r"zaliczeni\w*", "kurs nie ma zaliczenia"),
    (r"nie (?:jest )?opcj\w*", "ćwiczenie opisane jako nieopcjonalne"),
    (r"musi zostać wykonan\w*", "ćwiczenie opisane jako nakaz"),
    (r"pod rygorem", "ćwiczenie opisane jako nakaz"),
    (r"wym[oó]g\w*\s+(?:kursu|dnia|zaliczenia)", "kurs nie ma wymogów"),
]
MANDATORY_LABEL = "ćwiczenie opisane jako obowiązkowe"

# --- 6. „tor” as the name of the two course versions --------------------------------
# The two Day 2–3 offerings are „wersja podstawowa” / „wersja zaawansowana”. Calling
# them tory leaked into student files and made classroom Polish sound like a metro map.
TOR_VOCAB = [
    (r"\btory\b", "wersja, nie tor"),
    (r"\btorów\b", "wersja, nie tor"),
    (r"\btorze\b", "wersja, nie tor"),
    (r"\btorem\b", "wersja, nie tor"),
    (r"\btoru\b", "wersja, nie tor"),
    (r"\bten tor\b", "wersja, nie tor"),
    (r"\btor zaawans", "wersja, nie tor"),
    (r"\btor podstaw", "wersja, nie tor"),
    (r"równoległ\w*\s+tor", "wersja, nie tor"),
    (r"w tempie", "hasło zamiast zdania"),
    (r"3,5 h", "zajęcia trwają 3 godziny"),
    (r"210 min", "zajęcia trwają 3 godziny"),
]


def scan_mandatory(path: Path) -> list[str]:
    """Compulsory framing around exercises, cleanup and prefixes."""
    hits = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        for pattern, label in MANDATORY_ALWAYS:
            for m in re.finditer(pattern, line, re.I):
                hits.append(f"{path.relative_to(ROOT)}:{lineno}  {label} — „{m.group(0)}”")
        if not MANDATORY_TOPIC.search(line):
            continue
        for m in MANDATORY_WORD.finditer(line):
            hits.append(
                f"{path.relative_to(ROOT)}:{lineno}  {MANDATORY_LABEL} — „{m.group(0)}”"
            )
    return hits


# --- 5. engine vs. table naming -----------------------------------------------------
# Only the first sentence of the banner names the engine; what follows is prose that
# routinely names the other engine too ("NIE Redshift — Redshift nie zna CTAS-a").
ENGINE_BANNER = re.compile(r"^--\s*SILNIK:\s*([^.]+)", re.M)
SQL_COMMENT = re.compile(r"^\s*--")

# `aurora_jk.gold_dim_date` and friends. The lookbehind keeps the three-part form
# `awsdatacatalog.aurora_jk.gold_dim_date` out of the match, and lines that mention
# the catalog at all are skipped, which covers the quoted spelling as well.
TWO_PART_GLUE = re.compile(r"(?<![\w.\"'])aurora_[\w<>]+\s*\.")

# Statements that only Redshift understands. Their presence marks a fenced block in a
# markdown file as Redshift-context even though markdown carries no engine banner.
REDSHIFT_ONLY = re.compile(
    r"\b(?:COPY\s+\w|CREATE\s+SCHEMA|IAM_ROLE|DISTSTYLE|SORTKEY|DISTKEY|"
    r"awsdatacatalog|data_catalog_auto_mount|ALTER\s+SYSTEM|"
    r"GRANT\s+USAGE\s+ON\s+DATABASE|CREATE\s+EXTERNAL\s+SCHEMA)\b",
    re.I,
)

BAD_TWO_PART = (
    "zapis dwuczłonowy w kontekście Redshifta — "
    'poprawnie "awsdatacatalog"."aurora_<inicjaly>"."tabela"'
)
BAD_CATALOG = "awsdatacatalog w pliku Athenowym — Athena nie zna tej nazwy"


def sql_engine(path: Path) -> str | None:
    """Engine declared in the header banner, or None when the file has no banner."""
    head = "\n".join(path.read_text(encoding="utf-8").splitlines()[:20])
    m = ENGINE_BANNER.search(head)
    return m.group(1).upper() if m else None


def scan_sql_engine(path: Path) -> list[str]:
    """Naming form must match the engine the file declares.

    Only executable lines are checked. Comments are where the file explains that the
    other engine's spelling exists and why it fails, so flagging them would make the
    explanation impossible to write.
    """
    engine = sql_engine(path)
    rel = path.relative_to(ROOT)
    if engine is None:
        return [f"{rel}:1  brak nagłówka „-- SILNIK: …” — nie wiadomo, na czym to działa"]

    hits = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if SQL_COMMENT.match(line) or not line.strip():
            continue
        if "REDSHIFT" in engine:
            if "awsdatacatalog" not in line and TWO_PART_GLUE.search(line):
                hits.append(f"{rel}:{lineno}  {BAD_TWO_PART} — „{line.strip()}”")
        elif "ATHENA" in engine:
            if "awsdatacatalog" in line:
                hits.append(f"{rel}:{lineno}  {BAD_CATALOG} — „{line.strip()}”")
    return hits


def scan_md_engine(path: Path) -> list[str]:
    """Same rule inside fenced SQL blocks, where the engine is inferred per block.

    Full exercise scripts now live in 02_rozwiazania.md. Their comments name the
    other engine on purpose, so a mention of awsdatacatalog in a comment must not
    reclassify an Athena fence as Redshift. Prefer ``-- SILNIK:``; otherwise look
    only at executable lines.
    """
    rel = path.relative_to(ROOT)
    hits = []
    block: list[tuple[int, str]] = []
    in_sql = False
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if line.startswith("```"):
            if in_sql:
                body = "\n".join(text for _n, text in block)
                banner = ENGINE_BANNER.search(body)
                exec_lines = [
                    (n, text)
                    for n, text in block
                    if not SQL_COMMENT.match(text) and text.strip()
                ]
                if banner:
                    engine = banner.group(1).upper()
                    is_redshift = "REDSHIFT" in engine
                    is_athena = "ATHENA" in engine
                else:
                    exec_body = "\n".join(text for _n, text in exec_lines)
                    is_redshift = bool(REDSHIFT_ONLY.search(exec_body))
                    is_athena = False
                if is_redshift:
                    for n, text in exec_lines:
                        if "awsdatacatalog" not in text and TWO_PART_GLUE.search(text):
                            hits.append(f"{rel}:{n}  {BAD_TWO_PART} — „{text.strip()}”")
                elif is_athena:
                    for n, text in exec_lines:
                        if "awsdatacatalog" in text:
                            hits.append(f"{rel}:{n}  {BAD_CATALOG} — „{text.strip()}”")
                block, in_sql = [], False
            else:
                in_sql = line.startswith("```sql")
            continue
        if in_sql:
            block.append((lineno, line))
    return hits


def scan(path: Path, rules: list[tuple[str, str]]) -> list[str]:
    hits = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        for pattern, label in rules:
            for m in re.finditer(pattern, line, re.I):
                hits.append(f"{path.relative_to(ROOT)}:{lineno}  {label} — „{m.group(0)}”")
    return hits


def files(base: Path, suffixes: tuple[str, ...]) -> list[Path]:
    if base.is_file():
        return [base] if base.suffix in suffixes else []
    return sorted(p for p in base.rglob("*") if p.is_file() and p.suffix in suffixes)


def report(title: str, hits: list[str]) -> int:
    print(f"\n=========== {title} ===========")
    if hits:
        for h in hits:
            print(f"  !! {h}")
    else:
        print("  brak")
    return len(hits)


def main() -> int:
    package_md = files(PACKAGE, (".md",))
    package_sql = files(PACKAGE, (".sql",))

    print(f"pakiet kursanta: {len(package_md)} plików .md, {len(package_sql)} plików .sql")

    problems = 0
    problems += report(
        "PAKIET: FRAZY O NIEUDOSTĘPNIANIU ROZWIĄZAŃ I NIEAKTUALNE ŚCIEŻKI",
        [h for f in package_md + package_sql for h in scan(f, SECRECY)],
    )
    problems += report(
        "PAKIET: SPOLSZCZONE TERMINY",
        [h for f in package_md + package_sql for h in scan(f, POLONIZED)],
    )
    problems += report(
        "PAKIET: TRYB ROZKAZUJĄCY W MARKDOWN",
        [h for f in package_md for h in scan(f, IMPERATIVE_RULES)],
    )
    problems += report(
        "PAKIET: ĆWICZENIA OPISANE JAKO OBOWIĄZKOWE",
        [h for f in package_md + package_sql for h in scan_mandatory(f)],
    )
    problems += report(
        "PAKIET: „TOR” / HASŁA / 3,5 H",
        [h for f in package_md + package_sql for h in scan(f, TOR_VOCAB)],
    )
    problems += report(
        "PAKIET: SILNIK KONTRA ZAPIS NAZWY TABELI",
        [h for f in package_sql for h in scan_sql_engine(f)]
        + [h for f in package_md for h in scan_md_engine(f)],
    )

    trainer_files = [f for base in TRAINER for f in files(base, (".md", ".py"))]
    trainer_hits = [
        h
        for f in trainer_files
        # audit_text.py contains the forbidden strings on purpose as patterns to match.
        if f.name not in {"audit_text.py"}
        for h in scan(f, SECRECY)
    ]
    problems += report("MATERIAŁY TRENERA: NIEAKTUALNE ŚCIEŻKI I FRAZY", trainer_hits)

    print(f"\nRAZEM: {problems} problemów")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
