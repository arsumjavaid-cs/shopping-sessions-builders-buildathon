#!/usr/bin/env python3
import json
import os
import duckdb
from ingest import parse_ts_ms, build_small_tables
from analytics import run_full_sql_pipeline

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")

with open(os.path.join(DATA, "manifest.json"), encoding="utf-8") as f:
    manifest = json.load(f)

win_start_ms = parse_ts_ms(manifest["window_start"])
win_end_ms = parse_ts_ms(manifest["window_end"])
assert win_start_ms is not None and win_end_ms is not None

user_map_tbl, products_tbl, campaigns_tbl = build_small_tables(DATA)
con.register("user_map", user_map_tbl)
con.register("products", products_tbl)
con.register("campaigns", campaigns_tbl)

run_full_sql_pipeline(con, DATA, OUT, win_start_ms, win_end_ms)
