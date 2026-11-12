"""Audit: are all referenced tables created, and do all referenced files exist?"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "Data_Lakes_i_Data_Warehouses"

CREATE = re.compile(
    r"CREATE\s+(?:EXTERNAL\s+|OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:aurora_jk\.)?([a-z0-9_]+)",
    re.I,
)
REF = re.compile(r"\baurora_jk\.([a-z0-9_]+)", re.I)
COMMENT = re.compile(r"--.*$", re.M)

# Filenames that appear in prose as cautionary examples of a data swamp, not as
# files this repo ships.
PROSE_ONLY = {
    "nowy_dump_final_v3.csv",
    "orders_final_v2_FIXED.csv",
    "orders/dt=2026-01-15/file.csv",
}


def main() -> int:
    errors = 0
    sql_files = sorted(PACKAGE.rglob("*.sql"))
    md_files = sorted(ROOT.rglob("*.md"))

    created: dict[str, str] = {}
    referenced: dict[str, set[str]] = {}

    for f in sql_files:
        text = COMMENT.sub("", f.read_text(encoding="utf-8"))
        for m in CREATE.finditer(text):
            created.setdefault(m.group(1).lower(), f.name)
        for m in REF.finditer(text):
            referenced.setdefault(m.group(1).lower(), set()).add(f.name)

    print("=== TABELE TWORZONE (w kolejności dni) ===")
    for name, src in created.items():
        print(f"  {name:34} <- {src}")

    print("\n=== REFERENCJE BEZ CREATE ===")
    missing = {n: s for n, s in referenced.items() if n not in created}
    if missing:
        errors += len(missing)
        for name, srcs in sorted(missing.items()):
            print(f"  !! {name:32} używane w {sorted(srcs)}")
    else:
        print("  brak — każda tabela jest gdzieś tworzona")

    print("\n=== SPÓJNOŚĆ SUFIKSÓW ===")
    # dw_ = warstwa Gold wersji zaawansowanej (Iceberg w Athenie, schemat dw_<inicjaly> w Redshifcie)
    bad = [n for n in created if not re.match(r"^(bronze|silver|gold|dw|v)_", n)]
    if bad:
        errors += len(bad)
    print("  nazwy poza konwencją bronze_/silver_/gold_/dw_/v_:", bad or "brak")

    print("\n=== ŚCIEŻKI S3 ===")
    buckets: set[str] = set()
    for f in sql_files:
        buckets |= set(re.findall(r"s3://([a-z0-9\-]+)/", f.read_text(encoding="utf-8")))
    print("  buckety w SQL:", sorted(buckets), "(oczekiwany jeden wzorcowy)")
    if buckets != {"aurora-jk-lake"}:
        errors += 1

    print("\n=== PLIKI DANYCH WSKAZYWANE W MARKDOWN ===")
    data_refs: set[str] = set()
    for f in md_files:
        text = f.read_text(encoding="utf-8")
        data_refs |= set(re.findall(r"`([a-zA-Z0-9_\-/\.=]+\.(?:csv|json))`", text))
        data_refs |= set(re.findall(r"\|\s*`([^`]+\.(?:csv|json))`", text))
    for ref in sorted(data_refs - PROSE_ONLY):
        name = ref.split("/")[-1]
        hits = list(PACKAGE.rglob(name))
        if not hits:
            errors += 1
        status = "OK " if hits else "!! BRAK"
        print(f"  {status} {ref}")

    print("\n=== SKRYPTY WSKAZYWANE W MARKDOWN ===")
    script_refs: set[str] = set()
    for f in md_files:
        text = f.read_text(encoding="utf-8")
        script_refs |= set(re.findall(r"`((?:tools|sql)[\\/][a-zA-Z0-9_]+\.(?:py|sql))`", text))
        script_refs |= set(re.findall(r"`(sql/[a-zA-Z0-9_]+\.sql)`", text))
    for ref in sorted(script_refs):
        name = ref.replace("\\", "/").split("/")[-1]
        hits = list(ROOT.rglob(name))
        if not hits:
            errors += 1
        print(f"  {'OK ' if hits else '!! BRAK'} {ref}")

    print(f"\nRAZEM: {errors} problemów")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
