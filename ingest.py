"""
ingest.py — Load a call export CSV into Supabase via HTTPS (no direct DB connection needed).
Usage:  python ingest.py path/to/export.csv
Safe to re-run: duplicate rows (same number + time) are silently skipped.

One-time setup: run the CREATE TABLE SQL in Supabase SQL editor before first use.
"""

import sys
import os
import math
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
BATCH_SIZE   = 500   # rows per upsert request

CREATE_TABLE_SQL = """
-- Run this once in Supabase SQL Editor before first ingest
CREATE TABLE IF NOT EXISTS calls (
    number                   TEXT,
    time                     TIMESTAMPTZ,
    use_case                 TEXT,
    call_status              TEXT,
    duration                 INTEGER,
    agent_number             TEXT,
    recording_url            TEXT,
    summary                  TEXT,
    user_sentiment           TEXT,
    task_completion          TEXT,
    issue_status             TEXT,
    call_quality             TEXT,
    status                   TEXT,
    activity_status          TEXT,
    activity_time            TEXT,
    call_summary             TEXT,
    long_hault_reason        TEXT,
    not_interested_reason    TEXT,
    notice_period            TEXT,
    is_jaipur_based          TEXT,
    can_operate_laptop       TEXT,
    preferred_device         TEXT,
    can_handle_documents     TEXT,
    is_over_qualified        TEXT,
    is_misaligned            TEXT,
    knows_excel_or_sheets    TEXT,
    preferred_date           TEXT,
    expected_salary          TEXT,
    status_for_next_round    TEXT,
    interview_preferred_date TEXT,
    current_salary           TEXT,
    number_category          TEXT,
    ingested_at              TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (number, time)
);
"""


def classify_number(num):
    s = str(num).strip() if pd.notna(num) else ""
    if not s or s in ("nan", "-"):
        return "missing"
    if "_" in s:
        return "has_underscore"
    if s.startswith("++"):
        return "double_plus"
    if not s.startswith("+"):
        return "no_plus"
    digits = s[1:]
    if not digits.isdigit():
        return "non_numeric"
    if len(digits) != 12:
        return "invalid_length"
    return "valid"


def normalise_agent(val):
    try:
        return "+" + str(int(float(val)))
    except (ValueError, TypeError):
        return None


def load_csv(path):
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df["Time"] = pd.to_datetime(df["Time"], errors="coerce")
    df["Duration"] = pd.to_numeric(df["Duration"], errors="coerce").fillna(0).astype(int)
    df["Agent Number"] = df["Agent Number"].apply(normalise_agent)

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].replace({"-": None, "": None, "nan": None})

    df["number_category"] = df["Number"].apply(classify_number)

    col_map = {
        "Number": "number", "Time": "time", "Use Case": "use_case",
        "Call Status": "call_status", "Duration": "duration",
        "Agent Number": "agent_number", "Recording URL": "recording_url",
        "Analysis.summary": "summary", "Analysis.user_sentiment": "user_sentiment",
        "Analysis.task_completion": "task_completion", "Analysis.issue_status": "issue_status",
        "Analysis.call_quality": "call_quality", "Analysis.status": "status",
        "Analysis.activity_status": "activity_status", "Analysis.activity_time": "activity_time",
        "Analysis.call_summary": "call_summary", "Analysis.long_hault_reason": "long_hault_reason",
        "Analysis.not_interested_reason": "not_interested_reason",
        "Analysis.notice_period": "notice_period", "Analysis.is_jaipur_based": "is_jaipur_based",
        "Analysis.can_operate_laptop": "can_operate_laptop",
        "Analysis.preferred_device": "preferred_device",
        "Analysis.can_handle_documents": "can_handle_documents",
        "Analysis.is_over_qualified": "is_over_qualified", "Analysis.is_misaligned": "is_misaligned",
        "Analysis.knows_excel_or_sheets": "knows_excel_or_sheets",
        "Analysis.preferred_date": "preferred_date", "Analysis.expected_salary": "expected_salary",
        "Analysis.status_for_next_round": "status_for_next_round",
        "Analysis.interview_preferred_date": "interview_preferred_date",
        "Analysis.current_salary": "current_salary",
    }
    df = df.rename(columns=col_map)
    db_cols = list(col_map.values()) + ["number_category"]
    df = df[[c for c in db_cols if c in df.columns]]
    df = df.dropna(subset=["number", "time"])
    df = df.drop_duplicates(subset=["number", "time"], keep="last")

    # Convert timestamps to ISO strings for JSON serialisation
    df["time"] = df["time"].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    return df


def to_records(df):
    """Convert DataFrame to list of dicts, dropping NaN values."""
    records = []
    for row in df.to_dict(orient="records"):
        records.append({k: (None if (isinstance(v, float) and math.isnan(v)) else v)
                        for k, v in row.items()})
    return records


def main(csv_path):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: Set SUPABASE_URL and SUPABASE_KEY in your .env file.")
        print("  SUPABASE_URL: Supabase → Project Settings → API → Project URL")
        print("  SUPABASE_KEY: Supabase → Project Settings → API → service_role key")
        sys.exit(1)

    print(f"Loading {csv_path} ...")
    df = load_csv(csv_path)
    print(f"  {len(df):,} rows after cleanup")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    records   = to_records(df)
    total     = len(records)
    inserted  = 0
    n_batches = math.ceil(total / BATCH_SIZE)

    for i in range(n_batches):
        batch = records[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
        sb.table("calls").upsert(batch, on_conflict="number,time").execute()
        inserted += len(batch)
        print(f"  Batch {i+1}/{n_batches} — {inserted:,}/{total:,} rows sent", end="\r")

    print(f"\nDone. {total:,} rows upserted (duplicates silently ignored by Supabase).")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ingest.py path/to/export.csv")
        print()
        print("First time? Run this SQL once in Supabase SQL Editor:")
        print(CREATE_TABLE_SQL)
        sys.exit(1)
    main(sys.argv[1])
