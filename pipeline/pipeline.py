#!/usr/bin/env python3
import json
import os
import duckdb
from ingest import ingest_all

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

events_tbl, clicks_tbl, campaigns_tbl = ingest_all(DATA, manifest)
con.register("events", events_tbl)
con.register("clicks", clicks_tbl)
con.register("campaigns", campaigns_tbl)

os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
con.execute(f"""
    COPY (
        SELECT NULL::VARCHAR AS user_id, NULL::DATE AS day, NULL::BIGINT AS sessions,
               NULL::BIGINT AS events, NULL::BIGINT AS revenue_cents, NULL::VARCHAR AS top_category
        WHERE false
    ) TO '{os.path.join(OUT, "user_daily", "part-0.parquet")}' (FORMAT parquet)
""")
con.execute(f"""
    COPY (
        SELECT NULL::VARCHAR AS campaign_id, NULL::VARCHAR AS channel, NULL::DATE AS day,
               NULL::BIGINT AS sessions, NULL::BIGINT AS purchasing_sessions, NULL::BIGINT AS revenue_cents,
               NULL::BIGINT AS assisted_sessions
        WHERE false
    ) TO '{os.path.join(OUT, "campaign_daily", "part-0.parquet")}' (FORMAT parquet)
""")
