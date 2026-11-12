"""Build offline dual-pane HTML viewers for teaching from the student markdown.

VS Code / Cursor restore the text-editor cursor across file switches, but the
Markdown preview webview jumps to the top. This script does not replace the
``.md`` files (they stay the source of truth). It writes ``sala.html`` next to
each ``01_zadania.md`` + ``02_rozwiazania.md`` pair so the trainer can run the
room from Chrome/Edge on ``file://``: two independent scroll panes, a sticky
exercise nav that syncs both, copyable SQL, sessionStorage so a refresh keeps
place.

Usage (from repo root)::

    python tools/build_sala_html.py

Needs the ``Markdown`` package (``fenced_code``). If it is missing, a venv is
created at ``tools/.venv`` and the package is installed there — no global pip.
SQL fences are pretty-printed only when the source is one long line / ragged
(``sqlparse`` if present) and then highlighted offline at generate time — no CDN.
"""
from __future__ import annotations

import html
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "Data_Lakes_i_Data_Warehouses"
VENV = Path(__file__).resolve().parent / ".venv"

MD_EXTENSIONS = ["fenced_code", "tables", "sane_lists"]

FOLDER_TITLE = {
    "Dzien1_Data_Lakes": "Dzień 1 — Data Lakes",
    "Dzien2_Data_Warehouse": "Dzień 2 — Data Warehouse (podstawowy)",
    "Dzien2_Data_Warehouse_Zaawansowany": "Dzień 2 — Data Warehouse (zaawansowany)",
    "Dzien3_Data_Warehouse": "Dzień 3 — Data Warehouse (podstawowy)",
    "Dzien3_Data_Warehouse_Zaawansowany": "Dzień 3 — Data Warehouse (zaawansowany)",
}

VERIFY_IDS = {
    "Dzien1_Data_Lakes": ("l3", "l5"),
    "Dzien2_Data_Warehouse": ("w1", "w3"),
    "Dzien2_Data_Warehouse_Zaawansowany": ("z1", "z3"),
    "Dzien3_Data_Warehouse": ("w6", "w7"),
    "Dzien3_Data_Warehouse_Zaawansowany": ("y1", "y3"),
}

# Exercise headings start with L/W/Z/Y/S/D + digits, then ".", em-dash, or more
# text. Bare "### S3" (AWS bucket cleanup) must not match.
EX_HEAD = re.compile(
    r"^("
    r"[LWSZYD]\d+[a-z]?"
    r"(?:\s*[–—−-]\s*(?:[LWSZYD]\d+[a-z]?|\d+[a-z]?))?"
    r")"
    r"(?:\.|\s+[—–−-]|\s+\S)",
    re.I,
)
HEADING_RE = re.compile(r"<h([1-6])>(.*?)</h\1>", re.I | re.S)
PRE_RE = re.compile(r"<pre><code([^>]*)>(.*?)</code></pre>", re.I | re.S)
TAG_RE = re.compile(r"<[^>]+>")
SQL_FILE_RE = re.compile(r"^([A-Za-z]\d+[a-z]?)_")
TOKEN_RE = re.compile(r"^([A-Z])(\d+)([a-z]?)$", re.I)
RANGE_SPLIT = re.compile(r"\s*[–—−-]\s*")
CDN_RE = re.compile(
    r"<(?:script|link)[^>]+(?:src|href)\s*=\s*['\"]https?://",
    re.I,
)


def ensure_markdown():
    try:
        import markdown  # noqa: WPS433

        return markdown
    except ImportError:
        pass
    py = VENV / ("Scripts" if os.name == "nt" else "bin")
    py = py / ("python.exe" if os.name == "nt" else "python")
    print("Brak pakietu Markdown — tworzę tools/.venv i instaluję go lokalnie.")
    if not py.exists():
        subprocess.check_call([sys.executable, "-m", "venv", str(VENV)])
    subprocess.check_call([str(py), "-m", "pip", "install", "Markdown"])
    if Path(sys.executable).resolve() != py.resolve():
        os.execv(str(py), [str(py), *sys.argv])
    import markdown  # noqa: WPS433

    return markdown


def strip_tags(fragment: str) -> str:
    return html.unescape(TAG_RE.sub("", fragment)).strip()


def parse_token(raw: str) -> tuple[str, int, str] | None:
    m = TOKEN_RE.fullmatch(raw.strip())
    if not m:
        return None
    return m.group(1).upper(), int(m.group(2)), m.group(3).lower()


def format_id(letter: str, num: int, suf: str) -> str:
    return f"{letter.lower()}{num}{suf}"


def expand_head(head: str) -> list[str]:
    parts = [p for p in RANGE_SPLIT.split(head.strip()) if p]
    if not parts:
        return []
    left = parse_token(parts[0])
    if left is None:
        return []
    if len(parts) == 1:
        return [format_id(*left)]
    right = parse_token(parts[1])
    if right is None:
        right = parse_token(left[0] + parts[1])
    if right is None:
        right = parse_token(f"{left[0]}{left[1]}{parts[1]}")
    if right is None:
        return [format_id(*left)]
    ids: list[str] = []
    if left[0] == right[0] and left[1] == right[1] and left[2] and right[2]:
        lo, hi = ord(left[2]), ord(right[2])
        if 0 <= hi - lo <= 12:
            for code in range(lo, hi + 1):
                ids.append(format_id(left[0], left[1], chr(code)))
            return ids
    if left[0] == right[0] and not left[2] and not right[2]:
        lo, hi = left[1], right[1]
        if 0 <= hi - lo <= 12:
            for n in range(lo, hi + 1):
                ids.append(format_id(left[0], n, ""))
            return ids
    return [format_id(*left), format_id(*right)]


def ids_from_heading(inner_html: str) -> list[str]:
    text = strip_tags(inner_html)
    m = EX_HEAD.match(text)
    if not m:
        return []
    return expand_head(m.group(1))


def label_of(eid: str) -> str:
    m = re.fullmatch(r"([a-z]+)(\d+)([a-z]?)", eid)
    if not m:
        return eid.upper()
    return f"{m.group(1).upper()}{m.group(2)}{m.group(3)}"


def primary_of(eid: str) -> str:
    m = re.fullmatch(r"([a-z]+)(\d+)([a-z])", eid)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    return eid


def suffix_of(eid: str) -> str:
    m = re.fullmatch(r"[a-z]+\d+([a-z])", eid)
    return m.group(1) if m else ""


# Longest-first Athena / Redshift phrases. Matched case-insensitively, emitted
# in the source casing so Kopiuj still pastes what the trainer sees.
_SQL_MULTI = [
    "create or replace view",
    "create external table",
    "create table if not exists",
    "create table",
    "create view",
    "create schema",
    "insert into",
    "merge into",
    "when not matched",
    "when matched",
    "then update set",
    "then update",
    "then insert",
    "partitioned by",
    "row format serde",
    "row format",
    "stored as",
    "with serdeproperties",
    "left outer join",
    "right outer join",
    "full outer join",
    "left join",
    "right join",
    "inner join",
    "cross join",
    "full join",
    "group by",
    "order by",
    "partition by",
    "union all",
    "union distinct",
    "if not exists",
    "if exists",
    "is not null",
    "is null",
    "not exists",
    "not like",
    "not in",
]
_SQL_MULTI.sort(key=lambda p: (-len(p.split()), -len(p)))
_SQL_MULTI_RE = re.compile(
    r"(?i)\b(?:"
    + "|".join(re.escape(p).replace(r"\ ", r"\s+") for p in _SQL_MULTI)
    + r")\b"
)
_SQL_KEYWORDS = {
    "select", "from", "where", "join", "using", "on", "and", "or", "not", "as",
    "in", "between", "like", "ilike", "distinct", "all", "any", "some", "exists",
    "insert", "update", "delete", "drop", "alter", "add", "rename", "replace",
    "merge", "unnest", "lateral",
    "create", "table", "view", "schema", "database", "external",
    "into", "values", "set", "with", "recursive",
    "case", "when", "then", "else", "end", "cast",
    "union", "except", "intersect",
    "having", "limit", "offset", "fetch",
    "true", "false", "null", "unknown",
    "inner", "left", "right", "full", "cross", "outer",
    "over", "rows", "range", "unbounded", "preceding", "following",
    "current", "row", "window",
    "partitioned", "stored", "location", "tblproperties", "serdeproperties",
    "msck", "repair", "show", "describe", "desc",
    "copy", "unload", "grant", "revoke",
    "distkey", "sortkey", "diststyle", "encode",
    "explain", "analyze", "vacuum", "optimize",
    "refresh", "materialized",
    "interval", "date", "timestamp", "time",
    "integer", "int", "bigint", "smallint", "tinyint",
    "double", "precision", "real", "float", "decimal", "numeric",
    "varchar", "char", "string", "boolean", "bool", "array", "map", "struct",
    "if", "exists", "by", "for", "of", "to", "is",
    "asc", "desc", "nulls", "first", "last",
    "primary", "key", "foreign", "references", "constraint",
    "ctas", "parquet", "iceberg", "textfile", "json",
    "serde", "inputformat", "outputformat",
    "begin", "commit", "rollback", "transaction",
}
_SQL_KW_RE = re.compile(
    r"(?i)\b(?:" + "|".join(re.escape(k) for k in sorted(_SQL_KEYWORDS, key=len, reverse=True)) + r")\b"
)
_SQL_IDENT_RE = re.compile(r"[A-Za-z_][\w$]*")
_SQL_NUM_RE = re.compile(r"\d+(?:\.\d+)?")
_SQL_WS_RE = re.compile(r"\s+")


def _span(kind: str, text: str) -> str:
    return f'<span class="{kind}">{html.escape(text)}</span>'


def highlight_sql(src: str) -> str:
    """Offline Athena/Redshift highlighter. Comments and strings win over keywords."""
    out: list[str] = []
    i, n = 0, len(src)
    while i < n:
        ch = src[i]
        if ch == "-" and i + 1 < n and src[i + 1] == "-":
            end = src.find("\n", i)
            if end < 0:
                end = n
            out.append(_span("c", src[i:end]))
            i = end
            continue
        if ch == "/" and i + 1 < n and src[i + 1] == "*":
            end = src.find("*/", i + 2)
            end = n if end < 0 else end + 2
            out.append(_span("c", src[i:end]))
            i = end
            continue
        if ch in {'"', "'", "`"}:
            q = ch
            j = i + 1
            while j < n:
                if src[j] == "\\" and q != "`":
                    j += 2
                    continue
                if src[j] == q:
                    if q != "`" and j + 1 < n and src[j + 1] == q:
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            kind = "i" if q == "`" else "s"
            out.append(_span(kind, src[i:j]))
            i = j
            continue
        ws = _SQL_WS_RE.match(src, i)
        if ws:
            out.append(ws.group(0))
            i = ws.end()
            continue
        multi = _SQL_MULTI_RE.match(src, i)
        if multi:
            out.append(_span("k", multi.group(0)))
            i = multi.end()
            continue
        kw = _SQL_KW_RE.match(src, i)
        if kw:
            out.append(_span("k", kw.group(0)))
            i = kw.end()
            continue
        ident = _SQL_IDENT_RE.match(src, i)
        if ident:
            word = ident.group(0)
            j = ident.end()
            k = j
            while k < n and src[k] in " \t":
                k += 1
            if k < n and src[k] == "(":
                out.append(_span("f", word))
            else:
                out.append(_span("n", word))
            i = j
            continue
        if ch.isdigit():
            num = _SQL_NUM_RE.match(src, i)
            out.append(_span("m", num.group(0) if num else ch))
            i = num.end() if num else i + 1
            continue
        out.append(html.escape(ch))
        i += 1
    return "".join(out)


def _sql_is_ragged(sql: str) -> bool:
    lines = [ln.rstrip() for ln in sql.splitlines() if ln.strip()]
    if not lines:
        return False
    if len(lines) <= 2 and max(len(ln) for ln in lines) > 100:
        return True
    longish = sum(1 for ln in lines if len(ln) > 160)
    return longish >= max(2, (len(lines) + 2) // 3)


def pretty_sql(sql: str) -> str:
    """Reindent only compact / one-line fences. Already-aligned scripts stay put."""
    if not _sql_is_ragged(sql):
        return sql
    try:
        import sqlparse  # noqa: WPS433
    except ImportError:
        return sql
    formatted = sqlparse.format(
        sql,
        reindent=True,
        keyword_case=None,
        identifier_case=None,
        indent_width=2,
        strip_comments=False,
        use_space_around_operators=True,
    )
    if formatted.count("--") < sql.count("--"):
        return sql
    if formatted.count("'") != sql.count("'"):
        return sql
    if formatted.count('"') != sql.count('"'):
        return sql
    return formatted.rstrip() + ("\n" if sql.endswith("\n") else "")


def fence_lang(attrs: str) -> str:
    cm = re.search(r'class="([^"]*)"', attrs)
    if not cm:
        return ""
    return cm.group(1).replace("language-", "").replace("lang-", "").lower().strip()


@dataclass
class Section:
    ids: list[str]
    html: str


@dataclass
class Converted:
    prologue: str
    sections: list[Section]
    ids: list[str]

    @property
    def present(self) -> set[str]:
        found: set[str] = set()
        for section in self.sections:
            found.update(section.ids)
        return found

    def render(self) -> str:
        return self.prologue + "".join(section.html for section in self.sections)


def wrap_fences(src: str) -> str:
    def repl(match: re.Match[str]) -> str:
        attrs, body = match.group(1), match.group(2)
        lang = fence_lang(attrs)
        lang_attr = html.escape(lang, quote=True)
        extra = " code-sql" if lang in {"sql", "athena", "redshift"} else ""
        if lang in {"sql", "athena", "redshift"}:
            text = pretty_sql(html.unescape(body))
            inner = highlight_sql(text)
            code_attrs = ' class="language-sql"'
        else:
            inner = body
            code_attrs = attrs
        return (
            f'<div class="code-block{extra}" data-lang="{lang_attr}">'
            f'<button type="button" class="copy-code">Kopiuj</button>'
            f"<pre><code{code_attrs}>{inner}</code></pre>"
            f"</div>"
        )

    return PRE_RE.sub(repl, src)


def sql_files_in(folder: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    sql_dir = folder / "sql"
    if not sql_dir.is_dir():
        return out
    for path in sorted(sql_dir.glob("*.sql")):
        m = SQL_FILE_RE.match(path.name)
        if m:
            out[m.group(1).lower()] = path
    return out


def sql_bar(ids: list[str], sql_map: dict[str, Path]) -> str:
    buttons = []
    seen: set[str] = set()
    for eid in ids:
        path = sql_map.get(eid)
        if path is None or eid in seen:
            continue
        seen.add(eid)
        buttons.append(
            f'<button type="button" class="copy-file" data-sql-id="{html.escape(eid)}" '
            f'title="Kopiuj {html.escape(path.name)}">Kopiuj {html.escape(path.name)}</button>'
        )
    if not buttons:
        return ""
    return '<p class="sql-bar">' + "".join(buttons) + "</p>"


def inject_heading(chunk: str, ids: list[str], prefix: str, sql_map: dict[str, Path]) -> str:
    def repl(match: re.Match[str]) -> str:
        level, inner = match.group(1), match.group(2)
        extras = "".join(
            f'<span class="ghost-anchor" id="{prefix}-{eid}" data-ex="{eid}"></span>'
            for eid in ids[1:]
        )
        return (
            f'<h{level} id="{prefix}-{ids[0]}" data-ex="{ids[0]}">'
            f"{inner}{extras}</h{level}>"
            f"{sql_bar(ids, sql_map)}"
        )

    return HEADING_RE.sub(repl, chunk, count=1)


def to_article(md_html: str, prefix: str, sql_map: dict[str, Path]) -> Converted:
    md_html = wrap_fences(md_html)
    found: list[tuple[re.Match[str], list[str]]] = []
    for match in HEADING_RE.finditer(md_html):
        ids = ids_from_heading(match.group(2))
        if ids:
            found.append((match, ids))
    if not found:
        return Converted(f'<div class="prologue">{md_html}</div>', [], [])

    ordered: list[str] = []
    seen: set[str] = set()
    prologue = f'<div class="prologue">{md_html[: found[0][0].start()]}</div>'
    sections: list[Section] = []
    for i, (match, ids) in enumerate(found):
        end = found[i + 1][0].start() if i + 1 < len(found) else len(md_html)
        chunk = inject_heading(md_html[match.start() : end], ids, prefix, sql_map)
        for eid in ids:
            if eid not in seen:
                ordered.append(eid)
                seen.add(eid)
        id_attr = " ".join(ids)
        primary = primary_of(ids[0])
        sections.append(
            Section(
                ids=list(ids),
                html=(
                    f'<section class="ex" data-ids="{id_attr}" data-primary="{primary}">'
                    f"{chunk}</section>"
                ),
            )
        )
    return Converted(prologue, sections, ordered)


def insert_ghosts(conv: Converted, needed: list[str], prefix: str) -> None:
    present = conv.present
    rank = {eid: i for i, eid in enumerate(needed)}
    for eid in needed:
        if eid in present:
            continue
        ghost = Section(
            ids=[eid],
            html=(
                f'<section class="ex ghost-section" data-ids="{eid}" '
                f'data-primary="{primary_of(eid)}">'
                f'<span class="ghost-anchor" id="{prefix}-{eid}" data-ex="{eid}"></span>'
                f'<p class="ghost-note">Brak osobnego nagłówka {html.escape(label_of(eid))} '
                f"w tym pliku — kotwica, żeby klik w nawigacji zgrał oba panele.</p>"
                f"</section>"
            ),
        )
        insert_at = 0
        for i, section in enumerate(conv.sections):
            if any(rank.get(x, 10_000) < rank[eid] for x in section.ids):
                insert_at = i + 1
        conv.sections.insert(insert_at, ghost)
        present.add(eid)


def group_nav(ids: list[str]) -> list[tuple[str, list[str]]]:
    primaries: list[str] = []
    subs: dict[str, list[str]] = {}
    seen: set[str] = set()
    for eid in ids:
        if eid in seen:
            continue
        seen.add(eid)
        parent = primary_of(eid)
        if parent not in subs:
            primaries.append(parent)
            subs[parent] = []
        if eid != parent:
            subs[parent].append(eid)
    return [(p, subs[p]) for p in primaries]


def nav_html(ids: list[str]) -> str:
    chunks = ['<nav class="nav" aria-label="Ćwiczenia">']
    for primary, subs in group_nav(ids):
        chunks.append('<div class="nav-group">')
        chunks.append(
            f'<a class="nav-id" href="#{primary}" data-ex="{primary}">'
            f"{html.escape(label_of(primary))}</a>"
        )
        if subs:
            chunks.append('<div class="nav-subs">')
            for sid in subs:
                suf = suffix_of(sid) or label_of(sid)
                chunks.append(
                    f'<a href="#{sid}" data-ex="{sid}" title="{html.escape(label_of(sid))}">'
                    f"{html.escape(suf)}</a>"
                )
            chunks.append("</div>")
        chunks.append("</div>")
    chunks.append("</nav>")
    return "\n".join(chunks)


def json_for_script(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=None).replace("<", "\\u003c")


CSS = r"""
:root {
  --navy: #12263a;
  --navy-2: #1b3348;
  --red: #d61f26;
  --paper: #f4efe6;
  --pane: #fffdf8;
  --ink: #1c1917;
  --muted: #534d46;
  --line: #d4cbbd;
  --code-bg: #15202b;
  --code-fg: #e8eef4;
  --base: 17px;
}
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; }
html { font-size: var(--base); }
body {
  font-family: "Segoe UI", system-ui, sans-serif;
  color: var(--ink);
  background: var(--navy);
  line-height: 1.55;
}
.app {
  display: grid;
  grid-template-columns: 6.6rem 1fr 1fr;
  grid-template-rows: auto 1fr;
  height: 100vh;
  height: 100dvh;
}
.top {
  grid-column: 1 / -1;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.55rem 0.9rem;
  padding: 0.45rem 0.75rem;
  background: var(--navy);
  color: #f4efe6;
  border-bottom: 3px solid var(--red);
}
.top h1 {
  font-size: 1.05rem;
  font-weight: 650;
  margin: 0;
  letter-spacing: 0.01em;
}
.brand {
  color: var(--red);
  font-weight: 700;
  margin-right: 0.2rem;
}
.hint { color: #c9c0b3; font-size: 0.82rem; }
.tools {
  margin-left: auto;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.45rem 0.7rem;
}
.tools label {
  display: flex;
  align-items: center;
  gap: 0.35rem;
  font-size: 0.88rem;
  cursor: pointer;
  user-select: none;
}
button.tool {
  font: inherit;
  font-size: 0.85rem;
  background: var(--navy-2);
  color: #f4efe6;
  border: 1px solid #3a5166;
  border-radius: 4px;
  padding: 0.2rem 0.55rem;
  cursor: pointer;
}
button.tool:hover { border-color: #f4efe6; }
.nav {
  grid-row: 2;
  overflow: auto;
  background: var(--navy-2);
  padding: 0.45rem 0.35rem 1rem;
  border-right: 1px solid #0b1824;
}
.nav-group { margin-bottom: 0.28rem; }
.nav a {
  color: #f4efe6;
  text-decoration: none;
}
.nav-id {
  display: block;
  font-weight: 700;
  font-size: 0.95rem;
  padding: 0.22rem 0.4rem;
  border-radius: 4px;
  border-left: 3px solid transparent;
}
.nav-subs {
  display: flex;
  flex-wrap: wrap;
  gap: 0.12rem;
  padding: 0.05rem 0.2rem 0.2rem 0.55rem;
}
.nav-subs a {
  font-size: 0.78rem;
  min-width: 1.15rem;
  text-align: center;
  padding: 0.05rem 0.28rem;
  border-radius: 3px;
  color: #c9c0b3;
}
.nav a:hover { background: #24425c; }
.nav a.is-active {
  background: #fffdf8;
  color: var(--navy);
  border-left-color: var(--red);
}
.nav-subs a.is-active { color: var(--navy); }
.pane {
  overflow: auto;
  background: var(--pane);
  min-width: 0;
}
.pane + .pane { border-left: 1px solid var(--line); }
.pane-head {
  position: sticky;
  top: 0;
  z-index: 3;
  background: var(--paper);
  border-bottom: 1px solid var(--line);
  padding: 0.4rem 1rem;
  font-weight: 700;
  font-size: 0.92rem;
  letter-spacing: 0.02em;
  color: var(--navy);
}
.pane-head span { color: var(--muted); font-weight: 500; font-size: 0.8rem; }
.doc {
  padding: 0.7rem 1.15rem 3rem;
  max-width: 52rem;
}
.doc h1 { font-size: 1.45rem; line-height: 1.25; margin: 0.2rem 0 0.7rem; }
.doc h2, .doc h3, .ghost-anchor { scroll-margin-top: 2.8rem; }
.doc h2 {
  font-size: 1.22rem;
  line-height: 1.3;
  margin: 1.5rem 0 0.55rem;
  padding-bottom: 0.2rem;
  border-bottom: 2px solid var(--red);
  color: var(--navy);
}
.doc h3 { font-size: 1.05rem; margin: 1.15rem 0 0.4rem; color: var(--navy); }
.doc h4 { font-size: 1rem; margin: 1rem 0 0.35rem; }
.doc p, .doc li { font-size: 1rem; }
.doc ul, .doc ol { padding-left: 1.3rem; }
.doc li { margin: 0.2rem 0; }
.doc a { color: #9b1820; }
.doc hr { border: 0; border-top: 1px solid var(--line); margin: 1.2rem 0; }
.doc table {
  border-collapse: collapse;
  margin: 0.6rem 0 1rem;
  font-size: 0.92rem;
  width: 100%;
}
.doc th, .doc td {
  border: 1px solid var(--line);
  padding: 0.28rem 0.45rem;
  vertical-align: top;
  text-align: left;
}
.doc th { background: var(--paper); color: var(--navy); }
.doc tr:nth-child(even) td { background: #fbf7f0; }
.doc code {
  font-family: Consolas, "Cascadia Mono", ui-monospace, monospace;
  font-size: 0.88em;
  background: #efe8dc;
  padding: 0.05em 0.28em;
  border-radius: 3px;
}
.code-block {
  position: relative;
  background: var(--code-bg);
  color: var(--code-fg);
  border-radius: 6px;
  margin: 0.65rem 0 1rem;
}
.code-block pre {
  margin: 0;
  padding: 1.7rem 0.85rem 0.85rem;
  overflow-x: auto;
}
.code-block code {
  background: transparent;
  color: inherit;
  padding: 0;
  font-family: Consolas, "Cascadia Mono", ui-monospace, monospace;
  font-size: 14.5px;
  line-height: 1.45;
  white-space: pre;
}
.code-sql .k { color: #7eb8ff; font-weight: 650; }
.code-sql .f { color: #9cdcfe; }
.code-sql .s { color: #e8b86d; }
.code-sql .m { color: #e09a7b; }
.code-sql .c { color: #8aaa7a; font-style: italic; }
.code-sql .n { color: #e7edf3; }
.code-sql .i { color: #d2c4a8; }
.copy-code, .copy-file {
  font: inherit;
  font-size: 0.75rem;
  cursor: pointer;
  border-radius: 4px;
}
.copy-code {
  position: absolute;
  top: 0.35rem;
  right: 0.4rem;
  background: #2a3d4e;
  color: #f4efe6;
  border: 1px solid #4a6276;
  padding: 0.12rem 0.45rem;
}
.copy-code:hover, .copy-file:hover { filter: brightness(1.08); }
.sql-bar { margin: 0.25rem 0 0.6rem; }
.copy-file {
  background: var(--navy);
  color: #f4efe6;
  border: 0;
  padding: 0.18rem 0.55rem;
  margin-right: 0.3rem;
}
.ghost-note {
  color: var(--muted);
  font-size: 0.9rem;
  font-style: italic;
  border-left: 3px solid var(--line);
  padding-left: 0.6rem;
}
.ghost-anchor {
  display: block;
  height: 0;
  overflow: hidden;
}
body.is-filtered .prologue,
body.is-filtered .ex { display: none; }
body.is-filtered .ex.is-match { display: block; }
body.is-filtered .ghost-section.is-match { display: block; }
.doc blockquote {
  margin: 0.6rem 0;
  padding: 0.15rem 0.8rem;
  border-left: 3px solid var(--red);
  color: var(--muted);
}
"""

JS = r"""
(function () {
  const paneZ = document.getElementById("pane-z");
  const paneR = document.getElementById("pane-r");
  const KEY = window.SALA.key;
  const SQL = window.SALA.sql || {};
  history.scrollRestoration = "manual";

  function load() {
    try { return JSON.parse(sessionStorage.getItem(KEY) || "{}"); }
    catch (e) { return {}; }
  }
  function save(partial) {
    const next = Object.assign(load(), partial);
    sessionStorage.setItem(KEY, JSON.stringify(next));
  }

  function copyText(text) {
    if (navigator.clipboard && window.isSecureContext) {
      return navigator.clipboard.writeText(text).catch(function () { fallback(text); });
    }
    fallback(text);
  }
  function fallback(text) {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand("copy"); } catch (e) {}
    document.body.removeChild(ta);
  }
  function flash(btn) {
    const old = btn.textContent;
    btn.textContent = "Skopiowane";
    setTimeout(function () { btn.textContent = old; }, 1200);
  }

  function paneTarget(pane, ex) {
    return pane.querySelector('[data-ex="' + ex + '"]');
  }
  function scrollPane(pane, ex) {
    const el = paneTarget(pane, ex);
    if (!el) return false;
    const head = pane.querySelector(".pane-head");
    const offset = (head ? head.offsetHeight : 0) + 10;
    const top = el.getBoundingClientRect().top - pane.getBoundingClientRect().top + pane.scrollTop - offset;
    pane.scrollTo(0, Math.max(0, top));
    return true;
  }
  function markNav(ex) {
    document.querySelectorAll(".nav a.is-active").forEach(function (a) {
      a.classList.remove("is-active");
    });
    document.querySelectorAll('.nav a[data-ex="' + ex + '"]').forEach(function (a) {
      a.classList.add("is-active");
    });
    const parent = ex.replace(/[a-z]$/, "");
    if (parent !== ex) {
      document.querySelectorAll('.nav-id[data-ex="' + parent + '"]').forEach(function (a) {
        a.classList.add("is-active");
      });
    }
  }
  function familyOf(ex) {
    return ex.replace(/[a-z]$/, "");
  }
  function applyFilter(ex) {
    const on = document.getElementById("flt").checked;
    document.body.classList.toggle("is-filtered", on);
    const fam = familyOf(ex || "");
    document.querySelectorAll(".ex").forEach(function (sec) {
      const primary = sec.getAttribute("data-primary") || "";
      const ids = (sec.getAttribute("data-ids") || "").split(/\s+/);
      const match = !on || primary === fam || ids.indexOf(ex) !== -1 || ids.indexOf(fam) !== -1;
      sec.classList.toggle("is-match", match);
    });
  }

  let current = (load().ex || "");
  if (!current) {
    const first = document.querySelector(".nav a[data-ex]");
    if (first) current = first.getAttribute("data-ex") || "";
  }
  let fullZ = 0, fullR = 0;

  function goto(ex, both) {
    current = ex;
    markNav(ex);
    applyFilter(ex);
    if (both !== false) {
      scrollPane(paneZ, ex);
      scrollPane(paneR, ex);
    }
    save({ ex: ex, z: paneZ.scrollTop, r: paneR.scrollTop });
  }

  document.querySelector(".nav").addEventListener("click", function (ev) {
    const a = ev.target.closest("a[data-ex]");
    if (!a) return;
    ev.preventDefault();
    goto(a.getAttribute("data-ex"), true);
  });

  document.getElementById("sync").addEventListener("click", function () {
    if (current) goto(current, true);
  });
  document.getElementById("flt").addEventListener("change", function () {
    const on = this.checked;
    if (on) {
      fullZ = paneZ.scrollTop;
      fullR = paneR.scrollTop;
      save({ filter: true, fullZ: fullZ, fullR: fullR });
      applyFilter(current);
      if (current) {
        scrollPane(paneZ, current);
        scrollPane(paneR, current);
      }
    } else {
      const st = load();
      applyFilter(current);
      paneZ.scrollTop = st.fullZ != null ? st.fullZ : fullZ;
      paneR.scrollTop = st.fullR != null ? st.fullR : fullR;
      save({ filter: false, z: paneZ.scrollTop, r: paneR.scrollTop });
    }
  });

  function bump(delta) {
    const cur = parseFloat(getComputedStyle(document.documentElement).fontSize) || 17;
    const next = Math.min(22, Math.max(15, cur + delta));
    document.documentElement.style.setProperty("--base", next + "px");
    save({ base: next });
  }
  document.getElementById("fs-down").addEventListener("click", function () { bump(-1); });
  document.getElementById("fs-up").addEventListener("click", function () { bump(1); });

  document.addEventListener("click", function (ev) {
    const fileBtn = ev.target.closest(".copy-file");
    if (fileBtn) {
      const id = fileBtn.getAttribute("data-sql-id");
      if (SQL[id]) { copyText(SQL[id]); flash(fileBtn); }
      return;
    }
    const codeBtn = ev.target.closest(".copy-code");
    if (!codeBtn) return;
    const block = codeBtn.closest(".code-block");
    const code = block ? block.querySelector("code") : null;
    if (code) { copyText(code.textContent.replace(/\n$/, "")); flash(codeBtn); }
  });

  let ticking = false;
  function onScroll() {
    if (ticking) return;
    ticking = true;
    requestAnimationFrame(function () {
      save({ z: paneZ.scrollTop, r: paneR.scrollTop, ex: current });
      ticking = false;
    });
  }
  paneZ.addEventListener("scroll", onScroll, { passive: true });
  paneR.addEventListener("scroll", onScroll, { passive: true });
  window.addEventListener("pagehide", function () {
    save({ z: paneZ.scrollTop, r: paneR.scrollTop, ex: current, filter: document.getElementById("flt").checked });
  });

  const st = load();
  if (st.base) document.documentElement.style.setProperty("--base", st.base + "px");
  if (st.filter) document.getElementById("flt").checked = true;
  if (st.ex) {
    current = st.ex;
    markNav(st.ex);
    applyFilter(st.ex);
  }
  requestAnimationFrame(function () {
    if (st.z != null) paneZ.scrollTop = st.z;
    if (st.r != null) paneR.scrollTop = st.r;
  });
})();
"""


def render_page(
    title: str,
    folder_name: str,
    nav: str,
    left: str,
    right: str,
    sql_payload: dict[str, str],
) -> str:
    safe_title = html.escape(title)
    sala = json_for_script({"key": f"sala:{folder_name}", "sql": sql_payload})
    return f"""<!DOCTYPE html>
<html lang="pl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{safe_title} — na salę</title>
<style>
{CSS}
</style>
</head>
<body>
<!-- Generated by tools/build_sala_html.py. Source of truth: 01_zadania.md + 02_rozwiazania.md. Offline, file://. -->
<div class="app">
  <header class="top">
    <h1>{safe_title}</h1>
    <span class="hint">Klik ID po lewej — obie kolumny skaczą do tego ćwiczenia. Przewijanie paneli jest niezależne.</span>
    <div class="tools">
      <label><input type="checkbox" id="flt"> tylko to ćwiczenie</label>
      <button type="button" class="tool" id="sync">Synchronizuj</button>
      <button type="button" class="tool" id="fs-down" title="Mniejsza czcionka">A−</button>
      <button type="button" class="tool" id="fs-up" title="Większa czcionka">A+</button>
    </div>
  </header>
  {nav}
  <div class="pane" id="pane-z">
    <div class="pane-head">Zadania <span>01_zadania.md</span></div>
    <article class="doc">{left}</article>
  </div>
  <div class="pane" id="pane-r">
    <div class="pane-head">Rozwiązania <span>02_rozwiazania.md</span></div>
    <article class="doc">{right}</article>
  </div>
</div>
<script>window.SALA = {sala};</script>
<script>
{JS}
</script>
</body>
</html>
"""


def discover() -> list[Path]:
    folders = []
    for path in sorted(PACKAGE.iterdir()):
        if path.is_dir() and (path / "01_zadania.md").exists() and (path / "02_rozwiazania.md").exists():
            folders.append(path)
    return folders


def convert_md(markdown, text: str) -> str:
    return markdown.markdown(text, extensions=MD_EXTENSIONS)


def verify(path: Path, folder_name: str, needed: list[str]) -> None:
    text = path.read_text(encoding="utf-8")
    if CDN_RE.search(text):
        raise SystemExit(f"{path}: zawiera skrypt/CSS z sieci — plik ma być offline")
    if "<pre>" not in text:
        raise SystemExit(f"{path}: brak bloku <pre> (oczekiwany SQL/tekst)")
    tokens = VERIFY_IDS.get(folder_name, ())
    for token in tokens:
        z = len(re.findall(rf'data-ex="{token}"', text))
        if z < 2:
            raise SystemExit(f"{path}: data-ex=\"{token}\" powinno być w obu kolumnach (jest {z})")
        if f'id="z-{token}"' not in text or f'id="r-{token}"' not in text:
            raise SystemExit(f"{path}: brak id z-{token} / r-{token}")
    for eid in needed:
        if f'data-ex="{eid}"' not in text:
            raise SystemExit(f"{path}: nawigacja ma {eid}, ale brak kotwicy")
    if folder_name == "Dzien2_Data_Warehouse_Zaawansowany":
        z1 = text.find('id="r-z1"')
        z2 = text.find('id="r-z2"')
        if z1 < 0 or z2 < 0:
            raise SystemExit(f"{path}: brak kotwic Z1/Z2 do weryfikacji SQL")
        chunk = text[z1:z2] if z2 > z1 else text[z1 : z1 + 20000]
        for word in ("CREATE", "TABLE", "SELECT"):
            if not re.search(rf'<span class="k">[^<]*{word}[^<]*</span>', chunk):
                raise SystemExit(f"{path}: Z1 SQL bez podświetlonego {word}")
        z2_end = text.find('id="r-z3"', z2)
        z2_chunk = text[z2 : z2_end if z2_end > z2 else z2 + 25000]
        if not re.search(r'<span class="k">[^<]*JOIN[^<]*</span>', z2_chunk):
            raise SystemExit(f"{path}: Z2 SQL bez podświetlonego JOIN")


def build_one(markdown, folder: Path) -> Path:
    title = FOLDER_TITLE.get(folder.name, folder.name.replace("_", " "))
    sql_map = sql_files_in(folder)
    zadania = convert_md(markdown, (folder / "01_zadania.md").read_text(encoding="utf-8"))
    rozw = convert_md(markdown, (folder / "02_rozwiazania.md").read_text(encoding="utf-8"))
    left = to_article(zadania, "z", sql_map)
    right = to_article(rozw, "r", sql_map)

    needed: list[str] = []
    for eid in left.ids + right.ids:
        if eid not in needed:
            needed.append(eid)

    insert_ghosts(left, needed, "z")
    insert_ghosts(right, needed, "r")

    sql_payload = {eid: path.read_text(encoding="utf-8") for eid, path in sql_map.items()}
    page = render_page(
        title, folder.name, nav_html(needed), left.render(), right.render(), sql_payload
    )
    out = folder / "sala.html"
    out.write_text(page, encoding="utf-8")
    verify(out, folder.name, needed)
    kb = out.stat().st_size // 1024
    print(f"OK  {out.relative_to(ROOT)}  ({kb} KB, nav: {' '.join(label_of(i) for i in needed)})")
    return out


def main() -> int:
    markdown = ensure_markdown()
    folders = discover()
    if not folders:
        print("Nie znaleziono par 01_zadania.md + 02_rozwiazania.md", file=sys.stderr)
        return 1
    for folder in folders:
        build_one(markdown, folder)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
