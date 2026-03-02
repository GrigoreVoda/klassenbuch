#!/usr/bin/env python3
"""
Themendokumentation PDF → PostgreSQL Tool
==========================================
Three modes of operation:

  1. PRINT mode (default) — generate SQL and print to stdout
       python pdf_to_sql.py file1.pdf file2.pdf

  2. EXECUTE mode — connect to Postgres and run the generated INSERTs
       python pdf_to_sql.py --db-host localhost --db-name mydb \\
                            --db-user postgres --db-password secret \\
                            file1.pdf file2.pdf

  3. QUERY mode — open an interactive SQL shell against the DB
       python pdf_to_sql.py --db-host localhost --db-name mydb \\
                            --db-user postgres --db-password secret \\
                            --query

  Fix missing permissions (needs a superuser connection):
       python pdf_to_sql.py --db-host localhost --db-name mydb \\
                            --db-user postgres --fix-permissions \\
                            --grant-to myappuser

  Environment variables (used if CLI flags are omitted):
       PGHOST, PGDATABASE, PGUSER, PGPASSWORD, PGPORT

Requirements:
    pip install pdfplumber psycopg2-binary
"""

import re
import sys
import os
import glob
import argparse
import textwrap
from datetime import datetime
from pathlib import Path

import pdfplumber

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_TABLES = [
    "lernfeld", "dozent", "lernfeld_dozent", "lerntag", "unterrichtseinheit",
]

# Column x-ranges in the Themendokumentation template (points)
COL_STUNDE      = (30,  74)
COL_LEHRINHALTE = (74,  359)
COL_DOZENT      = (459, 549)

# Words that bleed in from the table header row — must be filtered out
HEADER_NOISE = frozenset({
    "Stunde", "Lehrinhalte", "Lernformat/-methodik",
    "Lernformat/", "-methodik", "Dozent",
    "Unterrichtsbeginn:", "08:30",
})


# ─────────────────────────────────────────────────────────────────────────────
# Generic helpers
# ─────────────────────────────────────────────────────────────────────────────

def parse_date(raw: str) -> str:
    return datetime.strptime(raw.strip(), "%d.%m.%Y").strftime("%Y-%m-%d")

def sql_escape(text: str) -> str:
    return text.replace("'", "''")


# ─────────────────────────────────────────────────────────────────────────────
# Header parsing  (regex on raw page text)
# ─────────────────────────────────────────────────────────────────────────────

DATUM_RE = re.compile(r"Datum:\s*(\d{2}\.\d{2}\.\d{4})")
TITEL_RE = re.compile(
    r"Titel:\s*(LF[-\w]*)\s+(.+?)"
    r"(?:\s+(\d{2}\.\d{2}\.\d{4})-(\d{2}\.\d{2}\.\d{4})?)?"
    r"\s*$",
    re.MULTILINE,
)

def parse_header(text: str) -> dict:
    datum_m = DATUM_RE.search(text)
    titel_m = TITEL_RE.search(text)
    if not datum_m:
        raise ValueError("Datum not found in PDF header")
    if not titel_m:
        raise ValueError("Titel / Lernfeld not found in PDF header")
    start_raw = titel_m.group(3)
    end_raw   = titel_m.group(4)
    return {
        "datum":       parse_date(datum_m.group(1)),
        "lernfeld_id": titel_m.group(1),
        "titel":       titel_m.group(2).strip(),
        "start_datum": parse_date(start_raw) if start_raw else None,
        "end_datum":   parse_date(end_raw)   if end_raw   else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Word-based table extraction
#
# Strategy:
#   1. Find the y-positions of stunde numbers 1-9 in the Stunde column.
#   2. Compute per-row y-ranges using midpoints between consecutive numbers.
#   3. For each row, collect words from Lehrinhalte / Dozent columns.
#
# This is robust against pdfplumber splitting the table into fragments and
# against rows where the stunde number is vertically offset from the content.
# ─────────────────────────────────────────────────────────────────────────────

def _words_in_col(words: list, x0: float, x1: float,
                  y0: float, y1: float) -> list[str]:
    """Return texts of all words whose x0 is inside [x0,x1) and top is in [y0,y1)."""
    return [
        w["text"] for w in words
        if x0 <= w["x0"] < x1 and y0 <= w["top"] < y1
    ]


def _first_dozent(raw_words: list[str]) -> tuple[str, str]:
    """
    Extract the first 'Nachname, Vorname' pair from a list of words.
    Returns (nachname, vorname).
    Handles repeated names caused by multi-line cell bleed.
    """
    text = " ".join(raw_words)
    # Strip header noise
    text = re.sub(r"\bDozent\b", "", text).strip()
    if not text:
        return ("", "")
    if "," in text:
        nachname, rest = text.split(",", 1)
        nachname = nachname.strip()
        # Vorname: words before the name repeats
        vorname_words = rest.strip().split()
        for i, w in enumerate(vorname_words):
            if w == nachname:          # name starts repeating
                vorname_words = vorname_words[:i]
                break
        vorname = " ".join(vorname_words).strip()
    else:
        parts = text.split()
        nachname = parts[-1] if parts else ""
        vorname  = " ".join(parts[:-1])
    return nachname, vorname


def _detect_row_boundaries(words: list, table_top: float,
                            gap_threshold: float = 4.0) -> list[float] | None:
    """
    Detect row separator y-positions from gaps in the Lehrinhalte column.
    Returns a sorted list of y-midpoints of gaps, or None if no gaps found
    (fall back to stunde-midpoint method).
    """
    lh = sorted(
        [w for w in words
         if COL_LEHRINHALTE[0] <= w["x0"] < COL_LEHRINHALTE[1]
         and w["top"] > table_top
         and w["text"] not in HEADER_NOISE],
        key=lambda w: w["top"],
    )
    gaps = []
    for i in range(1, len(lh)):
        gap = lh[i]["top"] - lh[i - 1]["bottom"]
        if gap > gap_threshold:
            mid = (lh[i - 1]["bottom"] + lh[i]["top"]) / 2
            gaps.append(mid)
    return gaps if gaps else None


def parse_rows_from_page(page) -> list[dict]:
    """
    Extract all lesson rows from a Themendokumentation page.
    Returns list of {stunde, inhalt, dozent_vorname, dozent_nachname}.

    Row boundaries are determined by visible gaps between cell content in the
    Lehrinhalte column (reliable for dense multi-line cells).  Falls back to
    stunde-number midpoints when no gaps are found (sparse PDFs).
    """
    words = page.extract_words(x_tolerance=3, y_tolerance=3)

    # ── Locate stunde numbers 1-9 ──────────────────────────────────────────
    raw_stunden = [
        (int(w["text"]), w["top"])
        for w in words
        if w["text"].isdigit()
        and 1 <= int(w["text"]) <= 9
        and COL_STUNDE[0] <= w["x0"] < COL_STUNDE[1]
    ]
    if not raw_stunden:
        raise ValueError("No stunde numbers (1-9) found in Stunde column")

    # Deduplicate — keep first (topmost) occurrence of each number
    seen: set = set()
    stunden: list = []
    for nr, top in sorted(raw_stunden, key=lambda x: x[1]):
        if nr not in seen:
            seen.add(nr)
            stunden.append((nr, top))

    # Estimate where the table header ends (just above the first stunde number)
    table_top = min(t for _, t in stunden) - 35

    # ── Preferred: gap-based row boundaries ───────────────────────────────
    gap_separators = _detect_row_boundaries(words, table_top)

    if gap_separators:
        # Build row ranges from detected gaps
        # Clip separators that are past the table (signature line, footer)
        last_stunde_top = max(t for _, t in stunden)
        # Anything more than ~90pt below the last stunde is footer territory
        max_sep = last_stunde_top + 90
        separators = [g for g in gap_separators if g <= max_sep]

        # Row N spans from separators[N-1] to separators[N]
        row_ranges = []
        bounds = [table_top] + separators + [last_stunde_top + 90]
        for i in range(len(bounds) - 1):
            row_ranges.append((bounds[i], bounds[i + 1]))

        # Match each stunde number to the row range it falls in
        matched: list[tuple] = []
        for nr, s_top in stunden:
            for y0, y1 in row_ranges:
                if y0 - 5 <= s_top <= y1 + 5:
                    matched.append((nr, y0, y1))
                    break

        # If matching failed for some stunden, fall back to midpoints below
        if len(matched) == len(stunden):
            boundaries = matched
        else:
            gap_separators = None  # trigger fallback

    # ── Fallback: midpoint between consecutive stunde numbers ─────────────
    if not gap_separators:
        table_bottom = max(
            (w["bottom"] for w in words if w["bottom"] < 700),
            default=600,
        )
        boundaries = []
        for i, (nr, top) in enumerate(stunden):
            y_start = (stunden[i - 1][1] + top) / 2 if i > 0 else top - 30
            y_end   = (top + stunden[i + 1][1]) / 2  if i + 1 < len(stunden)                       else table_bottom
            boundaries.append((nr, y_start, y_end))

    # ── Extract content for each row ──────────────────────────────────────
    rows = []
    for nr, y0, y1 in boundaries:
        inhalt_words = _words_in_col(words, COL_LEHRINHALTE[0], COL_LEHRINHALTE[1], y0, y1)
        dozent_words = _words_in_col(words, COL_DOZENT[0],      COL_DOZENT[1],      y0, y1)

        inhalt_words = [w for w in inhalt_words if w not in HEADER_NOISE]
        nachname, vorname = _first_dozent(dozent_words)

        rows.append({
            "stunde":          nr,
            "inhalt":          " ".join(inhalt_words),
            "dozent_vorname":  vorname,
            "dozent_nachname": nachname,
        })

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# PDF entry point
# ─────────────────────────────────────────────────────────────────────────────

def extract_pdf(pdf_path: str) -> dict:
    with pdfplumber.open(pdf_path) as pdf:
        # Header is always on page 1
        text = pdf.pages[0].extract_text() or ""
        header = parse_header(text)

        # Collect rows across ALL pages (some PDFs overflow to page 2+)
        # Track which stunde numbers have already been found so continuation
        # pages (which repeat header/footer but no new stunden) are skipped cleanly.
        seen_stunden = set()
        all_rows = []
        for page in pdf.pages:
            try:
                rows = parse_rows_from_page(page)
            except ValueError:
                continue  # page has no stunde numbers at all (e.g. pure footer page)
            for row in rows:
                if row["stunde"] not in seen_stunden:
                    seen_stunden.add(row["stunde"])
                    all_rows.append(row)

        all_rows.sort(key=lambda r: r["stunde"])
        return {"header": header, "rows": all_rows}


# ─────────────────────────────────────────────────────────────────────────────
# In-memory deduplication state
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# DB-execute path  (no manual IDs — PostgreSQL SERIAL handles everything)
# ─────────────────────────────────────────────────────────────────────────────

def _get_or_create_dozent(cur, cache: dict, vorname: str, nachname: str) -> int:
    """
    Return the dozent_id for (vorname, nachname), creating the row if needed.
    Uses SELECT-then-INSERT because dozent has no UNIQUE on (vorname, nachname).
    """
    key = (vorname, nachname)
    if key in cache["dozenten"]:
        return cache["dozenten"][key]

    # Check if the row already exists in the DB
    cur.execute(
        "SELECT dozent_id FROM dozent WHERE vorname=%s AND nachname=%s LIMIT 1;",
        key,
    )
    row = cur.fetchone()
    if row:
        did = row[0]
    else:
        cur.execute(
            "INSERT INTO dozent (vorname, nachname) VALUES (%s, %s) RETURNING dozent_id;",
            key,
        )
        did = cur.fetchone()[0]

    cache["dozenten"][key] = did
    return did


def execute_pdf_into_db(pdf_data: dict, cur, cache: dict) -> int:
    """
    Insert one PDF's data directly using the DB cursor.
    Uses RETURNING to get auto-generated SERIAL IDs — never passes IDs manually.

    Schema (updated):
        lerntag now has a dozent_id column.
        dozent has no UNIQUE on (vorname, nachname) — handled via SELECT-then-INSERT.

    cache keys (shared across all PDFs in a run):
        'lernfelder' : set of lernfeld_id strings
        'dozenten'   : (vorname, nachname) -> dozent_id
        'lerntage'   : datum_str -> lerntag_id
        'lf_doz'     : set of (lernfeld_id, dozent_id)
    """
    hdr  = pdf_data["header"]
    rows = pdf_data["rows"]
    affected = 0

    lf_id = hdr["lernfeld_id"]

    # ── lernfeld (VARCHAR PK — ON CONFLICT handles re-runs) ──────────────
    if lf_id not in cache["lernfelder"]:
        cur.execute(
            "INSERT INTO lernfeld (lernfeld_id, titel, start_datum, end_datum) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (lernfeld_id) DO NOTHING;",
            (lf_id, hdr["titel"], hdr["start_datum"],
             hdr["end_datum"] if hdr["end_datum"] else None),
        )
        affected += cur.rowcount
        cache["lernfelder"].add(lf_id)

    # ── dozenten — resolve all unique names that appear in this PDF ───────
    # All stunden on one day share the same dozent; collect unique names.
    dozent_keys = {
        (r["dozent_vorname"], r["dozent_nachname"]) for r in rows
    }
    for vorname, nachname in dozent_keys:
        did = _get_or_create_dozent(cur, cache, vorname, nachname)

        # lernfeld_dozent link
        combo = (lf_id, did)
        if combo not in cache["lf_doz"]:
            cur.execute(
                "INSERT INTO lernfeld_dozent (lernfeld_id, dozent_id) "
                "VALUES (%s, %s) ON CONFLICT (lernfeld_id, dozent_id) DO NOTHING;",
                (lf_id, did),
            )
            affected += cur.rowcount
            cache["lf_doz"].add(combo)

    # ── lerntag (SERIAL PK, UNIQUE datum) ────────────────────────────────
    # Use the dozent from the first row (all stunden on a day share one dozent).
    datum = hdr["datum"]
    if datum not in cache["lerntage"]:
        first_row  = rows[0]
        day_dozent = _get_or_create_dozent(
            cur, cache,
            first_row["dozent_vorname"], first_row["dozent_nachname"],
        )
        cur.execute(
            "INSERT INTO lerntag (datum, lernfeld_id, dozent_id) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (datum) DO NOTHING "
            "RETURNING lerntag_id;",
            (datum, lf_id, day_dozent),
        )
        result = cur.fetchone()
        if result:
            lt_id = result[0]
        else:
            # Already existed — just fetch the ID
            cur.execute("SELECT lerntag_id FROM lerntag WHERE datum=%s;", (datum,))
            lt_id = cur.fetchone()[0]
        affected += 1
        cache["lerntage"][datum] = lt_id
    lt_id = cache["lerntage"][datum]

    # ── unterrichtseinheiten (SERIAL PK, UNIQUE lerntag_id+stunde) ────────
    for row in rows:
        cur.execute(
            "INSERT INTO unterrichtseinheit (lerntag_id, stunde, inhalt) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (lerntag_id, stunde) DO NOTHING;",
            (lt_id, row["stunde"], row["inhalt"]),
        )
        affected += cur.rowcount

    return affected


def make_db_cache() -> dict:
    return {"lernfelder": set(), "dozenten": {}, "lerntage": {}, "lf_doz": set()}


# ─────────────────────────────────────────────────────────────────────────────
# Print / dry-run path  (self-contained SQL file with explicit IDs from 1)
# ─────────────────────────────────────────────────────────────────────────────

class PrintState:
    """Tracks IDs only for generating a standalone SQL file (no DB needed)."""
    def __init__(self):
        self.lernfelder  = {}
        self.dozenten    = {}
        self.lerntage    = {}
        self.lf_doz      = set()
        self.dozent_seq  = 1
        self.lerntag_seq = 1
        self.einheit_seq = 1


def build_print_statements(pdf_data: dict, state: PrintState) -> list[str]:
    hdr  = pdf_data["header"]
    rows = pdf_data["rows"]
    stmts = []

    lf_id = hdr["lernfeld_id"]
    if lf_id not in state.lernfelder:
        state.lernfelder[lf_id] = True
        start_val = f"'{hdr['start_datum']}'" if hdr["start_datum"] else "NULL"
        end_val   = f"'{hdr['end_datum']}'"   if hdr["end_datum"]   else "NULL"
        stmts.append(
            f"INSERT INTO lernfeld (lernfeld_id, titel, start_datum, end_datum) "
            f"VALUES ('{lf_id}', '{sql_escape(hdr['titel'])}', "
            f"{start_val}, {end_val}) "
            f"ON CONFLICT (lernfeld_id) DO NOTHING;"
        )

    # ── dozenten first (lerntag references dozent_id) ────────────────────
    for row in rows:
        key = (row["dozent_vorname"], row["dozent_nachname"])
        if key not in state.dozenten:
            state.dozenten[key] = state.dozent_seq
            state.dozent_seq += 1
            stmts.append(
                f"INSERT INTO dozent (vorname, nachname) "
                f"VALUES ('{sql_escape(key[0])}', '{sql_escape(key[1])}') "
                f"ON CONFLICT DO NOTHING;"
            )
        did = state.dozenten[key]

        combo = (lf_id, did)
        if combo not in state.lf_doz:
            state.lf_doz.add(combo)
            stmts.append(
                f"INSERT INTO lernfeld_dozent (lernfeld_id, dozent_id) "
                f"SELECT '{lf_id}', dozent_id FROM dozent "
                f"WHERE vorname='{sql_escape(key[0])}' AND nachname='{sql_escape(key[1])}' "
                f"LIMIT 1 "
                f"ON CONFLICT (lernfeld_id, dozent_id) DO NOTHING;"
            )

    # ── lerntag — includes dozent_id via subquery ─────────────────────────
    datum = hdr["datum"]
    if datum not in state.lerntage:
        state.lerntage[datum] = state.lerntag_seq
        state.lerntag_seq += 1
        first = rows[0]
        stmts.append(
            f"INSERT INTO lerntag (datum, lernfeld_id, dozent_id) "
            f"SELECT '{datum}', '{lf_id}', dozent_id "
            f"FROM dozent "
            f"WHERE vorname='{sql_escape(first['dozent_vorname'])}' "
            f"AND nachname='{sql_escape(first['dozent_nachname'])}' "
            f"LIMIT 1 "
            f"ON CONFLICT (datum) DO NOTHING;"
        )

    # ── unterrichtseinheiten — subquery resolves lerntag_id by datum ──────
    for row in rows:
        stmts.append(
            f"INSERT INTO unterrichtseinheit (lerntag_id, stunde, inhalt) "
            f"SELECT lerntag_id, {row['stunde']}, '{sql_escape(row['inhalt'])}' "
            f"FROM lerntag WHERE datum='{datum}' "
            f"ON CONFLICT (lerntag_id, stunde) DO NOTHING;"
        )

    return stmts


# ─────────────────────────────────────────────────────────────────────────────
# Permission helpers
# ─────────────────────────────────────────────────────────────────────────────

def grant_statements(target_user: str) -> list[str]:
    tables_list = ", ".join(SCHEMA_TABLES)
    return [
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON {tables_list} TO {target_user};",
        f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {target_user};",
    ]

def check_permissions(cur, db_user: str) -> dict[str, list[str]]:
    cur.execute("""
        SELECT table_name, privilege_type
        FROM   information_schema.role_table_grants
        WHERE  table_schema = 'public'
          AND  grantee       = %s
          AND  table_name    = ANY(%s)
        ORDER  BY table_name, privilege_type;
    """, (db_user, SCHEMA_TABLES))
    result = {t: [] for t in SCHEMA_TABLES}
    for row in cur.fetchall():
        result[row["table_name"]].append(row["privilege_type"])
    return result

def print_permissions_table(perms: dict[str, list[str]]) -> bool:
    needed = {"INSERT", "SELECT"}
    print(f"\n{'Table':<25} {'Has INSERT':^12} {'Has SELECT':^12} {'Status':^10}")
    print("─" * 62)
    all_ok = True
    for table in SCHEMA_TABLES:
        privs  = set(perms.get(table, []))
        has_i  = "✓" if "INSERT" in privs else "✗"
        has_s  = "✓" if "SELECT" in privs else "✗"
        ok     = needed.issubset(privs)
        if not ok:
            all_ok = False
        print(f"  {table:<23} {has_i:^12} {has_s:^12} {'OK' if ok else 'MISSING':^10}")
    print()
    return all_ok


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL connector
# ─────────────────────────────────────────────────────────────────────────────

class PGConnector:

    def __init__(self, host, port, dbname, user, password):
        if not PSYCOPG2_AVAILABLE:
            print("ERROR: psycopg2 is not installed.\n  Run:  pip install psycopg2-binary")
            sys.exit(1)
        print(f"Connecting to PostgreSQL {host}:{port}/{dbname} as '{user}' …",
              file=sys.stderr)
        try:
            self.conn = psycopg2.connect(
                host=host, port=port, dbname=dbname,
                user=user, password=password, connect_timeout=10,
            )
        except psycopg2.OperationalError as e:
            print(f"\nERROR: Could not connect.\n  {e}")
            print("  Check --db-host / --db-port / --db-name / --db-user / --db-password")
            print("  Or set: PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD")
            sys.exit(1)
        self.conn.autocommit = False
        self.current_user = user
        print("Connected.", file=sys.stderr)

    # ── permission check / fix ────────────────────────────────────────────

    def check_permissions_for(self, target_user: str | None = None) -> bool:
        user = target_user or self.current_user
        print(f"\nChecking permissions for user '{user}':")
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            perms = check_permissions(cur, user)
        ok = print_permissions_table(perms)
        if not ok:
            print("  Fix option A — rerun with --fix-permissions (needs superuser):")
            print(f"    python pdf_to_sql.py --fix-permissions --grant-to {user} "
                  f"--db-user <superuser> ...\n")
            print("  Fix option B — run as a superuser in psql:")
            for s in grant_statements(user):
                print(f"    {s}")
            print()
        return ok

    def fix_permissions(self, target_user: str) -> bool:
        stmts = grant_statements(target_user)
        print(f"\nGranting permissions to '{target_user}':")
        with self.conn.cursor() as cur:
            for stmt in stmts:
                print(f"  {stmt}")
                try:
                    cur.execute(stmt)
                except psycopg2.Error as e:
                    self.conn.rollback()
                    print(f"\n  ERROR: {e}")
                    print("  Connected user may not have GRANT privileges.")
                    return False
        self.conn.commit()
        print("  ✓ Permissions granted.\n")
        return True

    # ── execute one PDF directly (no manual IDs) ─────────────────────────

    def execute_pdf(self, pdf_data: dict, cache: dict, source_label="") -> int:
        """
        Insert one PDF's data using parameterised queries and RETURNING.
        The DB SERIAL sequences assign all IDs — we never pass one manually.
        """
        try:
            with self.conn.cursor() as cur:
                affected = execute_pdf_into_db(pdf_data, cur, cache)
            self.conn.commit()
            return affected
        except psycopg2.errors.InsufficientPrivilege as e:
            self.conn.rollback()
            m = re.search(r'table "?(\w+)"?', str(e))
            tbl = m.group(1) if m else "?"
            raise PermissionError(
                f"User '{self.current_user}' lacks INSERT on '{tbl}'.\n\n"
                f"  Fix A: rerun with --fix-permissions --grant-to {self.current_user} "
                f"--db-user <superuser>\n"
                f"  Fix B: run as superuser:\n"
                + "\n".join(f"    {s}" for s in grant_statements(self.current_user))
            )
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"DB error in '{source_label}':\n  {e}")

    # ── single query ──────────────────────────────────────────────────────

    def run_query(self, sql: str) -> list[dict] | None:
        sql = sql.strip()
        if not sql:
            return None
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            try:
                cur.execute(sql)
                if cur.description:
                    rows = cur.fetchall()
                    self._print_table(rows)
                    return [dict(r) for r in rows]
                self.conn.commit()
                print(f"OK — {cur.rowcount} row(s) affected.")
                return None
            except psycopg2.errors.InsufficientPrivilege as e:
                self.conn.rollback()
                m = re.search(r'table "?(\w+)"?', str(e))
                tbl = m.group(1) if m else "?"
                print(f"\n  ✗ Permission denied on '{tbl}'. Run '\\perms' for details.")
                return None
            except psycopg2.Error as e:
                self.conn.rollback()
                print(f"  ERROR: {e}")
                return None

    # ── interactive shell ─────────────────────────────────────────────────

    def interactive_shell(self):
        print("\n" + "═" * 64)
        print("  PostgreSQL interactive shell  (\\q to quit, \\? for help)")
        print("═" * 64 + "\n")

        buffer = []
        while True:
            prompt = "sql> " if not buffer else "   … "
            try:
                line = input(prompt)
            except (EOFError, KeyboardInterrupt):
                print("\nBye.")
                break

            stripped = line.strip()

            if stripped == r"\q":
                print("Bye.")
                break
            elif stripped == r"\?":
                print(textwrap.dedent("""
                  \\q                quit
                  \\t                list all tables
                  \\d <table>        describe table columns
                  \\perms [user]     permission report
                  \\grant <user>     GRANT to user (needs superuser connection)
                  \\?                this help
                """))
                continue
            elif stripped == r"\t":
                self.run_query(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='public' ORDER BY table_name;"
                )
                buffer = []
                continue
            elif stripped.startswith(r"\d "):
                tbl = stripped[3:].strip()
                self.run_query(
                    f"SELECT column_name, data_type, is_nullable, column_default "
                    f"FROM information_schema.columns "
                    f"WHERE table_name='{tbl}' ORDER BY ordinal_position;"
                )
                buffer = []
                continue
            elif stripped.startswith(r"\perms"):
                user = stripped.split()[1] if len(stripped.split()) > 1 else None
                self.check_permissions_for(user)
                buffer = []
                continue
            elif stripped.startswith(r"\grant "):
                user = stripped[7:].strip()
                if user:
                    self.fix_permissions(user)
                else:
                    print("Usage: \\grant <username>")
                buffer = []
                continue

            buffer.append(line)
            joined = " ".join(buffer).strip()
            if joined.endswith(";") or (line == "" and joined):
                if joined:
                    self.run_query(joined)
                buffer = []

    # ── pretty table printer ──────────────────────────────────────────────

    @staticmethod
    def _print_table(rows):
        if not rows:
            print("(no rows returned)")
            return
        cols   = list(rows[0].keys())
        widths = {c: len(c) for c in cols}
        for r in rows:
            for c in cols:
                widths[c] = max(widths[c], len(str(r[c]) if r[c] is not None else "NULL"))
        sep  = "+" + "+".join("-" * (widths[c] + 2) for c in cols) + "+"
        head = "|" + "|".join(f" {c:<{widths[c]}} " for c in cols) + "|"
        print(sep); print(head); print(sep)
        for r in rows:
            print("|" + "|".join(
                f" {str(r[c]) if r[c] is not None else 'NULL':<{widths[c]}} "
                for c in cols
            ) + "|")
        print(sep)
        print(f"({len(rows)} row{'s' if len(rows) != 1 else ''})\n")

    def close(self):
        self.conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            Themendokumentation PDF → PostgreSQL tool

            Examples
            ────────
            # Print SQL only
            python pdf_to_sql.py *.pdf

            # Insert into DB
            python pdf_to_sql.py --db-host localhost --db-name mydb \\
                                 --db-user postgres --db-password secret *.pdf

            # Check permissions for a user
            python pdf_to_sql.py --db-host localhost --db-name mydb \\
                                 --db-user postgres --check-permissions --grant-to myuser

            # Fix permissions (connect as superuser, grant to app user)
            python pdf_to_sql.py --db-host localhost --db-name mydb \\
                                 --db-user postgres --fix-permissions --grant-to myappuser

            # Interactive query shell
            python pdf_to_sql.py --db-host localhost --db-name mydb \\
                                 --db-user postgres --query

            # Insert PDFs then drop into shell
            python pdf_to_sql.py --db-host localhost --db-name mydb \\
                                 --db-user postgres --query *.pdf
        """),
    )
    db = p.add_argument_group("Database connection (falls back to PG* env vars)")
    db.add_argument("--db-host",     default=os.getenv("PGHOST",     "localhost"))
    db.add_argument("--db-port",     default=os.getenv("PGPORT",     "5432"), type=int)
    db.add_argument("--db-name",     default=os.getenv("PGDATABASE", ""))
    db.add_argument("--db-user",     default=os.getenv("PGUSER",     ""))
    db.add_argument("--db-password", default=os.getenv("PGPASSWORD", ""))

    pm = p.add_argument_group("Permission management")
    pm.add_argument("--check-permissions", action="store_true",
                    help="Show privilege report for --grant-to user, then exit")
    pm.add_argument("--fix-permissions",   action="store_true",
                    help="GRANT INSERT/SELECT on all schema tables to --grant-to user")
    pm.add_argument("--grant-to", metavar="USERNAME",
                    help="Target DB user for --check-permissions / --fix-permissions")

    p.add_argument("--query",   "-q", action="store_true",
                   help="Open interactive SQL shell after processing PDFs")
    p.add_argument("--dry-run",       action="store_true",
                   help="Print SQL to stdout instead of executing (even if DB flags set)")
    p.add_argument("pdfs", nargs="*", help="PDF file paths (globs supported)")
    return p


def resolve_pdfs(raw_args: list[str]) -> list[Path]:
    paths = []
    for arg in raw_args:
        expanded = glob.glob(arg)
        paths.extend(Path(e) for e in (expanded if expanded else [arg]))
    return paths


def main():
    parser    = build_arg_parser()
    args      = parser.parse_args()
    pdf_paths = resolve_pdfs(args.pdfs)

    needs_db = args.db_name and not args.dry_run and (
        pdf_paths or args.query or args.check_permissions or args.fix_permissions
    )

    connector = None
    if needs_db:
        connector = PGConnector(
            host=args.db_host, port=args.db_port,
            dbname=args.db_name, user=args.db_user, password=args.db_password,
        )

    # ── Permission check / fix ────────────────────────────────────────────
    if args.check_permissions:
        if not connector:
            print("ERROR: --check-permissions requires --db-name.")
            sys.exit(1)
        connector.check_permissions_for(args.grant_to or args.db_user)
        if not args.fix_permissions and not pdf_paths and not args.query:
            connector.close(); return

    if args.fix_permissions:
        if not connector:
            print("ERROR: --fix-permissions requires --db-name.")
            sys.exit(1)
        if not args.grant_to:
            print("ERROR: --fix-permissions requires --grant-to <username>.")
            sys.exit(1)
        if not connector.fix_permissions(args.grant_to):
            connector.close(); sys.exit(1)
        if not pdf_paths and not args.query:
            connector.close(); return

    # ── Process PDFs ──────────────────────────────────────────────────────
    use_db     = connector is not None and not args.dry_run
    db_cache   = make_db_cache()   # shared across all PDFs in this run
    print_state = PrintState()
    all_statements = []

    for path in pdf_paths:
        if not path.exists():
            print(f"WARNING: {path} not found – skipping", file=sys.stderr)
            continue
        print(f"Processing: {path.name}", file=sys.stderr)
        try:
            pdf_data = extract_pdf(str(path))

            if use_db:
                # DB path: no manual IDs, SERIAL + RETURNING handles everything
                affected = connector.execute_pdf(pdf_data, db_cache,
                                                 source_label=path.name)
                print(f"  ✓ {affected} rows affected.", file=sys.stderr)
            else:
                # Print path: self-contained SQL with explicit IDs from 1
                stmts = build_print_statements(pdf_data, print_state)
                all_statements.append(f"-- Source: {path.name}")
                all_statements.extend(stmts)
                all_statements.append("")

        except PermissionError as e:
            print(f"\n  ✗ PERMISSION ERROR in {path.name}:\n{e}\n", file=sys.stderr)
        except Exception as e:
            print(f"  ERROR in {path.name}: {e}", file=sys.stderr)

    # ── Print mode output ─────────────────────────────────────────────────
    if not connector or args.dry_run:
        if all_statements:
            print("\n".join([
                "-- Auto-generated by pdf_to_sql.py",
                "-- NOTE: end_datum may be NULL when the PDF has no closing date",
                "", "BEGIN;", "",
                *all_statements,
                "COMMIT;",
            ]))
        elif not args.query and not args.check_permissions and not args.fix_permissions:
            print("Nothing to do.", file=sys.stderr)

    # ── Interactive shell ─────────────────────────────────────────────────
    if args.query:
        if not connector:
            print("ERROR: --query requires --db-name.", file=sys.stderr)
            sys.exit(1)
        connector.interactive_shell()

    if connector:
        connector.close()


if __name__ == "__main__":
    main()
