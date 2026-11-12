"""Audit: every multi-row result the students compare against must have an ORDER BY.

The failure this guards against was seen in the room: a `UNION ALL` of five
`COUNT(*)` rows, listed in the solutions as `customers | products | order_items |
orders | events`, came back from Athena in a different order. Neither Athena nor
Redshift promises any row order for `UNION ALL` or for `GROUP BY` without an
`ORDER BY` — the engine returns branches in whatever order they finished — so a
listed result can only be reproduced when the query sorts explicitly.

Rule: in every fenced ```sql block of the student markdown and in every `.sql`
script of the package, a statement that starts with SELECT or WITH and contains
`UNION ALL` or `GROUP BY` has to contain `ORDER BY` as well.

Not checked, on purpose:
- statements that create or load something (CREATE ... AS SELECT, INSERT, MERGE,
  views) — row order inside a table being written is meaningless;
- diagnostic queries with `HAVING` — they are expected to return no rows at all
  (duplicate checks), so there is nothing to order;
- a SELECT whose `UNION ALL` branch has no FROM (a literal row appended to a
  dimension) — that is the body of a CTAS quoted in prose, not a listed result.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "Data_Lakes_i_Data_Warehouses"

UNORDERED = re.compile(r"\bUNION\s+ALL\b|\bGROUP\s+BY\b", re.I)
ORDERED = re.compile(r"\bORDER\s+BY\b", re.I)
HAVING = re.compile(r"\bHAVING\b", re.I)
RESULT_STATEMENT = re.compile(r"^\s*(?:SELECT|WITH)\b", re.I)
LINE_COMMENT = re.compile(r"--[^\n]*")
LITERAL_BRANCH = re.compile(r"\bUNION\s+ALL\s+SELECT\b(?:(?!\bFROM\b).)*$", re.I | re.S)


def statements(sql: str) -> list[str]:
    """Statements of a script or fenced block, comments stripped, empty ones dropped."""
    text = LINE_COMMENT.sub("", sql)
    return [s.strip() for s in text.split(";") if s.strip()]


def fenced_sql(md: str) -> list[tuple[int, str]]:
    """(first line number, body) of every ```sql block in a markdown file."""
    blocks: list[tuple[int, str]] = []
    start, body = None, []
    for lineno, line in enumerate(md.splitlines(), start=1):
        if line.startswith("```"):
            if start is None:
                if line.startswith("```sql"):
                    start, body = lineno + 1, []
            else:
                blocks.append((start, "\n".join(body)))
                start = None
            continue
        if start is not None:
            body.append(line)
    return blocks


def offending(statement: str) -> bool:
    if not RESULT_STATEMENT.match(statement):
        return False
    if not UNORDERED.search(statement) or ORDERED.search(statement):
        return False
    if HAVING.search(statement):
        return False
    if LITERAL_BRANCH.search(statement):
        return False
    return True


def head(statement: str) -> str:
    return " ".join(statement.split())[:80]


def scan_sql_file(path: Path) -> list[str]:
    rel = path.relative_to(ROOT)
    hits = []
    for s in statements(path.read_text(encoding="utf-8")):
        if offending(s):
            hits.append(f"{rel}  brak ORDER BY — „{head(s)}”")
    return hits


def scan_md_file(path: Path) -> list[str]:
    rel = path.relative_to(ROOT)
    hits = []
    for lineno, body in fenced_sql(path.read_text(encoding="utf-8")):
        for s in statements(body):
            if offending(s):
                hits.append(f"{rel}:{lineno}  brak ORDER BY — „{head(s)}”")
    return hits


def main() -> int:
    md_files = sorted(p for p in PACKAGE.rglob("*.md"))
    sql_files = sorted(p for p in PACKAGE.rglob("*.sql"))
    print(f"pakiet kursanta: {len(md_files)} plików .md, {len(sql_files)} plików .sql")

    hits = [h for f in sql_files for h in scan_sql_file(f)]
    hits += [h for f in md_files for h in scan_md_file(f)]

    print("\n=========== UNION ALL / GROUP BY BEZ ORDER BY ===========")
    if hits:
        for h in hits:
            print(f"  !! {h}")
    else:
        print("  brak")

    print(f"\nRAZEM: {len(hits)} problemów")
    return 1 if hits else 0


if __name__ == "__main__":
    raise SystemExit(main())
