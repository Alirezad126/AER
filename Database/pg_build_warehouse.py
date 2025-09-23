#!/usr/bin/env python3
"""
pg_build_warehouse.py
Build a PostgreSQL warehouse from CSVs exported by the Tableau scraper.

Folder layout expected (example):
Data/
  01-01-099-14W4/
    Well_Summary_Report/
      01-01-099-14W4__Completion_Interval.csv
      01-01-099-14W4__Status_History.csv
      ...
    Well_Gas_Analysis/
      01-01-099-14W4__Gas_Analysis.csv
    Reservoir_Evaluation/
      01-01-099-14W4__Resource_Evaluation.csv
  01-02-049-27W4/
    ...

The loader groups files by (dashboard, sheet) and creates one raw table per group:
  raw.well_summary_report__completion_interval
  raw.well_summary_report__status_history
  raw.well_gas_analysis__<sheet> ...
Every column is TEXT in raw. Missing sheets/wells are fine -> just no rows.

We ensure there is a single canonical join key column named UWI_Formatted (TEXT).
"""

import os, sys, re, csv, glob, time, argparse, unicodedata
from pathlib import Path
from typing import List, Dict, Tuple, Iterable, Optional
from dataclasses import dataclass

import psycopg2
import psycopg2.extras as pgx

# ---------- CONFIG ----------
DATA_ROOT = Path("../WebScraping/Data")
RAW_SCHEMA = "raw"
CUR_SCHEMA = "curated"
DIM_SCHEMA = "dim"
BATCH_ROWS = 2000          # batch size for inserts
KEEP_PROVENANCE = True     # add source_dashboard, source_sheet, source_file to raw
# ----------------------------


# ---------- small utils ----------
def snake(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_").lower()
    if not s:
        s = "col"
    return s[:63]  # PG identifier limit

def sanitize_sheet_from_filename(name: str) -> str:
    # expected file name "<uwi>__<Sheet>.csv"
    base = Path(name).stem
    if "__" in base:
        sheet = base.split("__", 1)[1]
    else:
        sheet = base
    return snake(sheet)

def dashboard_from_path(p: Path) -> str:
    # Data/<uwi>/<Dashboard>/file.csv
    try:
        return snake(p.parent.name)   # dashboard folder name
    except Exception:
        return "unknown"

def short_uwi_from_folder(p: Path) -> Optional[str]:
    # Data/<UWI_Short>/<Dashboard>/file.csv
    try:
        return p.parent.parent.name
    except Exception:
        return None

def wrap_uwi(short_or_wrapped: str) -> str:
    s = short_or_wrapped.strip()
    if s.startswith("00/") and s.endswith("/0"):
        return s
    return f"00/{s}/0"

# --- encoding & delimiter detection ---
CANDIDATE_DELIMS = [",", ";", "\t", "|"]

def sniff_encoding(path: Path) -> str:
    with open(path, "rb") as fb:
        head = fb.read(4096)
    if head.startswith(b"\xff\xfe"): return "utf-16-le"
    if head.startswith(b"\xfe\xff"): return "utf-16-be"
    if head.startswith(b"\xef\xbb\xbf"): return "utf-8-sig"
    if b"\x00" in head[:100]: return "utf-16-le"
    try:
        head.decode("utf-8"); return "utf-8"
    except Exception:
        return "cp1252"

def detect_delimiter(text: str) -> str:
    lines = [ln for ln in text.splitlines() if ln.strip()][:60]
    if not lines:
        return ","
    best_delim, best_var, best_modal = ",", float("inf"), 0
    for d in CANDIDATE_DELIMS:
        cols = [ln.count(d) + 1 for ln in lines]
        modal = max(set(cols), key=cols.count)
        var = sum((c - modal) ** 2 for c in cols)
        if (var < best_var) or (var == best_var and modal > best_modal):
            best_delim, best_var, best_modal = d, var, modal
    return best_delim

def read_csv_rows(path: Path) -> Tuple[List[str], Iterable[List[str]], str]:
    """
    Returns (header, iterator over rows, delimiter) after robust sniffing.
    """
    enc = sniff_encoding(path)
    raw = path.read_text(encoding=enc, errors="replace")
    delim = detect_delimiter(raw)

    # stream parse to avoid big memory usage
    def iter_rows():
        for ln in raw.splitlines():
            yield next(csv.reader([ln], delimiter=delim))
    it = iter(iter_rows())
    try:
        header = next(it)
    except StopIteration:
        return [], [], delim
    return header, it, delim

# --- header normalization (UWI) ---
def norm_key(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", h.strip().lower())

FORMATTED_SYNS = {
    "wellidentifier", "formatteduwi", "welluwiformatted",
    "enterwellidentifieruwi", "prodstringuwiformatted",
    "uwiformatted"
}
NUMERIC_SYNS = {"welluwi", "uwi", "uwi_", "uwi-", "uwi—"}

def rename_headers_to_canonical(header: List[str]) -> Tuple[List[str], Optional[int], Optional[int]]:
    """
    Map any UWI-like headers to canonical names.
    Returns (mapped_header, idx_formatted, idx_numeric)
    """
    mapped = []
    idx_fmt = idx_num = None
    for i, h in enumerate(header):
        k = norm_key(h)
        if k in FORMATTED_SYNS or ("uwi" in k and "formatted" in k) or k == "wellidentifier":
            mapped.append("UWI_Formatted"); idx_fmt = i
        elif k in NUMERIC_SYNS and "formatted" not in k:
            mapped.append("UWI_Numeric"); idx_num = i
        else:
            mapped.append(snake(h))
    return mapped, idx_fmt, idx_num

# ---------- Postgres helpers ----------
@dataclass
class PGConn:
    host: str
    port: int
    user: str
    password: str
    dbname: str

def connect(pg: PGConn):
    return psycopg2.connect(
        host=pg.host, port=pg.port, user=pg.user,
        password=pg.password, dbname=pg.dbname
    )

def ensure_database(pg: PGConn):
    # create DB if --create-db passed
    with psycopg2.connect(
        host=pg.host, port=pg.port, user=pg.user,
        password=pg.password, dbname="postgres"
    ) as c:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (pg.dbname,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{pg.dbname}"')

def exec_sql(cn, sql: str, params=None):
    with cn.cursor() as cur:
        cur.execute(sql, params)

def ensure_schemas(cn):
    for sch in (RAW_SCHEMA, CUR_SCHEMA, DIM_SCHEMA):
        exec_sql(cn, f'CREATE SCHEMA IF NOT EXISTS "{sch}"')

def table_exists(cn, schema: str, table: str) -> bool:
    with cn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
        """, (schema, table))
        return cur.fetchone() is not None

def current_columns(cn, schema: str, table: str) -> List[str]:
    with cn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position
        """, (schema, table))
        return [r[0] for r in cur.fetchall()]

def create_empty_raw(cn, schema: str, table: str, extra_cols: List[str]):
    cols = ['"uwi_formatted" TEXT']
    for c in extra_cols:
        cols.append(f'"{c}" TEXT')
    if KEEP_PROVENANCE:
        cols += ['"source_dashboard" TEXT', '"source_sheet" TEXT', '"source_file" TEXT']
    exec_sql(cn, f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({", ".join(cols)})')
    exec_sql(cn, f'CREATE INDEX IF NOT EXISTS idx_{table}_uwi ON "{schema}"."{table}" ("uwi_formatted")')

def ensure_raw_table(cn, schema: str, table: str, union_cols: List[str]):
    """Create or alter a raw table so it contains all union_cols as TEXT + provenance."""
    if not table_exists(cn, schema, table):
        create_empty_raw(cn, schema, table, [c for c in union_cols if c != "uwi_formatted"])
        return
    existing = set(current_columns(cn, schema, table))
    to_add = [c for c in union_cols if c not in existing]
    if KEEP_PROVENANCE:
        for c in ("source_dashboard", "source_sheet", "source_file"):
            if c not in existing:
                to_add.append(c)
    for c in to_add:
        exec_sql(cn, f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{c}" TEXT')

def insert_rows(cn, schema: str, table: str, cols: List[str], rows: List[List[str]]):
    if not rows:
        return
    # build insert with execute_values for speed
    col_list = ", ".join(f'"{c}"' for c in cols)
    sql = f'INSERT INTO "{schema}"."{table}" ({col_list}) VALUES %s'
    with cn.cursor() as cur:
        pgx.execute_values(cur, sql, rows, page_size=BATCH_ROWS)


# ---------- discovery of datasets ----------
@dataclass
class FileInfo:
    path: Path
    dashboard: str
    sheet: str
    uwi_short: str

def discover_files(root: Path) -> Dict[Tuple[str, str], List[FileInfo]]:
    files = list(root.rglob("*.csv"))
    group: Dict[Tuple[str, str], List[FileInfo]] = {}
    for p in files:
        dash = dashboard_from_path(p)
        sheet = sanitize_sheet_from_filename(p.name)
        uwi_short = short_uwi_from_folder(p) or ""
        key = (dash, sheet)
        group.setdefault(key, []).append(FileInfo(p, dash, sheet, uwi_short))
    return group


# ---------- main ingestion ----------
def build(pg: PGConn, create_db: bool):
    if create_db:
        ensure_database(pg)
    with connect(pg) as cn:
        cn.autocommit = True
        ensure_schemas(cn)

        # 0) dim.dim_well from Data/<UWI_Short> directories
        uwis = sorted([p.name for p in DATA_ROOT.iterdir() if p.is_dir() and not p.name.startswith("_")])
        exec_sql(cn, f'DROP TABLE IF EXISTS "{DIM_SCHEMA}"."dim_well"')
        exec_sql(cn, f'CREATE TABLE "{DIM_SCHEMA}"."dim_well" (uwi_short TEXT PRIMARY KEY, uwi_formatted TEXT)')
        rows = [(u, wrap_uwi(u)) for u in uwis]
        insert_rows(cn, DIM_SCHEMA, "dim_well", ["uwi_short", "uwi_formatted"], rows)
        print(f"dim_well: {len(rows)} wells")

        # 1) discover datasets: (dashboard, sheet) -> list of files
        datasets = discover_files(DATA_ROOT)
        if not datasets:
            print("No CSV files found under Data/.")
            return

        # 2) for each dataset build union-of-columns and load
        for (dash, sheet), files in sorted(datasets.items()):
            raw_table = f"{dash}__{sheet}"                    # ex: well_summary_report__completion_interval
            print(f"\n[{dash} / {sheet}] files: {len(files)}  -> raw.{raw_table}")

            # 2a) union headers
            union_cols: List[str] = []
            seen = set()
            for info in files:
                hdr, _, _ = read_csv_rows(info.path)
                if not hdr:
                    continue
                mapped, idx_fmt, _ = rename_headers_to_canonical(hdr)
                # ensure uwi_formatted exists in union
                if "UWI_Formatted" not in mapped:
                    mapped.append("UWI_Formatted")
                # add unique, in order
                for m in mapped:
                    key = m.lower()
                    if key not in seen:
                        seen.add(key); union_cols.append(m)
            # downgrade to snake case in DB
            union_cols = [snake(c) for c in union_cols]
            # ensure join key present
            if "uwi_formatted" not in union_cols:
                union_cols.insert(0, "uwi_formatted")

            # 2b) ensure raw table exists with all columns
            ensure_raw_table(cn, RAW_SCHEMA, raw_table, union_cols)

            # 2c) load each file (align columns by name)
            load_cols = current_columns(cn, RAW_SCHEMA, raw_table)  # includes provenance if enabled
            for info in files:
                hdr, rows_iter, delim = read_csv_rows(info.path)
                if not hdr:
                    continue
                mapped, idx_fmt, _ = rename_headers_to_canonical(hdr)
                mapped = [snake(c) for c in mapped]

                # Build column index mapping: file_col_index -> dest_col_name
                file_to_dest: List[Tuple[int, str]] = [(i, mapped[i]) for i in range(len(mapped)) if mapped[i] in load_cols]

                batch: List[List[str]] = []
                uwi_wrapped = wrap_uwi(info.uwi_short) if info.uwi_short else None
                for r in rows_iter:
                    # pad short rows
                    if len(r) < len(mapped):
                        r = r + [""] * (len(mapped) - len(r))
                    rowdict = {mapped[i]: r[i] for i in range(len(mapped))}
                    # ensure UWI_Formatted
                    if "uwi_formatted" in load_cols:
                        if "uwi_formatted" in rowdict and (rowdict["uwi_formatted"] or "").strip():
                            pass
                        else:
                            rowdict["uwi_formatted"] = uwi_wrapped or ""
                    # assemble in table column order
                    out = []
                    for col in load_cols:
                        if KEEP_PROVENANCE and col in ("source_dashboard","source_sheet","source_file"):
                            if col == "source_dashboard": out.append(info.dashboard)
                            elif col == "source_sheet":   out.append(info.sheet)
                            else: out.append(str(info.path))
                        else:
                            out.append(rowdict.get(col, ""))
                    batch.append(out)
                    if len(batch) >= BATCH_ROWS:
                        insert_rows(cn, RAW_SCHEMA, raw_table, load_cols, batch); batch.clear()
                if batch:
                    insert_rows(cn, RAW_SCHEMA, raw_table, load_cols, batch)
            print(f"  loaded -> raw.{raw_table}")

        # 3) curated examples (SAFE: only use columns that exist)
        # 3) curated examples (schema-aware, safe when columns vary)
        # ----------------------------------------------------------

        def cols_set(schema: str, table: str) -> set[str]:
            return set(current_columns(cn, schema, table))

        def coalesce_text(alias: str, have: set[str], candidates: list[str]) -> str:
            opts = [f"NULLIF({alias}.\"{c}\",'')" for c in candidates if c in have]
            return "COALESCE(" + ", ".join(opts + ["NULL::text"]) + ")"

        def coalesce_double(alias: str, have: set[str], candidates: list[str]) -> str:
            opts = [f"NULLIF({alias}.\"{c}\",'')::double precision" for c in candidates if c in have]
            return "COALESCE(" + ", ".join(opts + ["NULL::double precision"]) + ")"

        def coalesce_date(alias: str, have: set[str], candidates: list[str]) -> str:
            opts = [f"NULLIF({alias}.\"{c}\",'')::date" for c in candidates if c in have]
            return "COALESCE(" + ", ".join(opts + ["NULL::date"]) + ")"

        def has_table(schema: str, table: str) -> bool:
            return table_exists(cn, schema, table)

        def curated_from_raw(raw_tbl: str, sql_body: str, curated_name: str):
            if not has_table(RAW_SCHEMA, raw_tbl):
                print(f"  skip curated.{curated_name} (raw.{raw_tbl} missing)")
                return
            exec_sql(cn, f'DROP TABLE IF EXISTS "{CUR_SCHEMA}"."{curated_name}"')
            exec_sql(cn, f'CREATE TABLE "{CUR_SCHEMA}"."{curated_name}" AS {sql_body}')
            exec_sql(cn, f'CREATE INDEX IF NOT EXISTS idx_{curated_name}_uwi ON "{CUR_SCHEMA}"."{curated_name}" ("uwi_formatted")')
            print(f"  created curated.{curated_name}")

        # ------- Completion Interval -----------------------------------------
        if has_table(RAW_SCHEMA, "well_summary_report__completion_interval"):
            ci_have = cols_set(RAW_SCHEMA, "well_summary_report__completion_interval")
            # candidates across variants you’ve seen
            ci_top   = coalesce_double("ci", ci_have, ["gross_completion_interval_top","gci_top","top","gross_comp_interval_top"])
            ci_base  = coalesce_double("ci", ci_have, ["gross_completion_interval_base","gci_base","base","gross_comp_interval_base"])
            ci_form  = coalesce_text  ("ci", ci_have, ["formation","prod_string_formation","gci_formation"])
            ci_qual  = coalesce_text  ("ci", ci_have, ["gci_quality","quality"])

            curated_from_raw(
                "well_summary_report__completion_interval",
                f"""
                SELECT
                  ci."uwi_formatted",
                  {ci_form}  AS formation,
                  {ci_qual}  AS quality,
                  {ci_top}   AS top_m,
                  {ci_base}  AS base_m
                FROM "{RAW_SCHEMA}"."well_summary_report__completion_interval" AS ci
                WHERE ci."uwi_formatted" IS NOT NULL
                """,
                "fact_completion_interval"
            )

        # ------- Status History ----------------------------------------------
        if has_table(RAW_SCHEMA, "well_summary_report__status_history"):
            sh_have = cols_set(RAW_SCHEMA, "well_summary_report__status_history")
            sh_date  = coalesce_date ("sh", sh_have, ["status_date","date","status_date_"])
            sh_stat  = coalesce_text ("sh", sh_have, ["status"])
            sh_fluid = coalesce_text ("sh", sh_have, ["status_fluid","fluid"])

            curated_from_raw(
                "well_summary_report__status_history",
                f"""
                SELECT
                  sh."uwi_formatted",
                  {sh_stat}  AS status,
                  {sh_fluid} AS fluid,
                  {sh_date}  AS status_date
                FROM "{RAW_SCHEMA}"."well_summary_report__status_history" AS sh
                WHERE sh."uwi_formatted" IS NOT NULL
                """,
                "fact_status_history"
            )

        # ------- Geological Tops ---------------------------------------------
        if has_table(RAW_SCHEMA, "well_summary_report__geological_tops_markers"):
            gt_have = cols_set(RAW_SCHEMA, "well_summary_report__geological_tops_markers")
            gt_form = coalesce_text  ("gt", gt_have, ["formation","formation_name"])
            gt_top  = coalesce_double("gt", gt_have, ["formation_depth_m","top_depth_m","top_md_m"])
            gt_desc = coalesce_text  ("gt", gt_have, ["description","remark","comments"])

            curated_from_raw(
                "well_summary_report__geological_tops_markers",
                f"""
                SELECT
                  gt."uwi_formatted",
                  {gt_form} AS formation,
                  {gt_top}  AS top_md_m,
                  {gt_desc} AS description
                FROM "{RAW_SCHEMA}"."well_summary_report__geological_tops_markers" AS gt
                WHERE gt."uwi_formatted" IS NOT NULL
                """,
                "fact_geological_tops"
            )

# -------------------------------


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg-host", default="localhost")
    ap.add_argument("--pg-port", default=5432, type=int)
    ap.add_argument("--pg-user", required=True)
    ap.add_argument("--pg-pass", required=True)
    ap.add_argument("--pg-db",   required=True)
    ap.add_argument("--create-db", action="store_true", help="Create database if it does not exist")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    pg = PGConn(
        host=args.pg_host, port=args.pg_port,
        user=args.pg_user, password=args.pg_pass,
        dbname=args.pg_db
    )
    build(pg, create_db=args.create_db)
