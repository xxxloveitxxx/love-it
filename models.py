import os
import time
import io
import csv
import pandas as pd
from datetime import datetime, timezone
from supabase import create_client, Client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
SUPABASE_BUCKET = os.getenv("SUPABASE_EXPORT_BUCKET", "exports")

# Optional Google Sheets
GSHEETS_SA_INFO = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")  # JSON string of the service account
GSHEET_ID = os.getenv("GSHEET_ID")  # target spreadsheet ID
GSHEET_TAB = os.getenv("GSHEET_TAB", "Leads")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def save_leads(leads):
    """Insert leads; skip duplicates by unique index."""
    inserted = 0
    for lead in leads:
        payload = {
            "name": lead.get("name"),
            "city": lead.get("city"),
            "brokerage": lead.get("brokerage"),
            "last_sale": lead.get("last_sale"),
            "contact_link": lead.get("contact_link"),
            "source": "zillow",
        }
        try:
            supabase.table("leads").insert(payload).execute()
            inserted += 1
        except Exception:
            # duplicate or transient error — ignore duplicates
            pass
    if inserted:
        try:
            export_recent_to_csv_and_sheet()
        except Exception:
            # don’t fail inserts if export fails
            pass
    return inserted

def get_all_leads(limit=500):
    res = supabase.table("leads").select("*").order("created_at", desc=True).limit(limit).execute()
    return res.data or []

def export_recent_to_csv_and_sheet():
    """Pull unprocessed events and export full snapshot to CSV + append to Google Sheet."""
    # Mark which events to process
    events = supabase.table("lead_insert_events").select("event_id, lead_id, created_at, processed") \
        .eq("processed", False).order("event_id", desc=False).limit(1000).execute().data or []
    if not events:
        return

    # Pull newest snapshot to CSV
    rows = supabase.table("leads").select("*").order("created_at", desc=True).limit(5000).execute().data or []

    # CSV into memory
    csv_buf = io.StringIO()
    writer = csv.DictWriter(csv_buf, fieldnames=["id","name","city","brokerage","last_sale","contact_link","source","created_at"])
    writer.writeheader()
    for r in rows:
        writer.writerow({
            "id": r.get("id"),
            "name": r.get("name"),
            "city": r.get("city"),
            "brokerage": r.get("brokerage"),
            "last_sale": r.get("last_sale"),
            "contact_link": r.get("contact_link"),
            "source": r.get("source"),
            "created_at": r.get("created_at"),
        })
    csv_bytes = csv_buf.getvalue().encode("utf-8")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"leads_snapshot_{ts}.csv"

    # Upload CSV to Supabase Storage
    try:
        supabase.storage.from_(SUPABASE_BUCKET).upload(key, csv_bytes, {"content-type": "text/csv"})
    except Exception:
        # bucket may not exist yet: attempt to create then re-upload (idempotent in Supabase dashboard usually)
        pass

    # Optional: also upsert rows to Google Sheets
    if GSHEETS_SA_INFO and GSHEET_ID:
        try:
            import json
            import gspread
            from google.oauth2.service_account import Credentials

            creds = Credentials.from_service_account_info(json.loads(GSHEETS_SA_INFO), scopes=[
                "https://www.googleapis.com/auth/spreadsheets"
            ])
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(GSHEET_ID)
            try:
                ws = sh.worksheet(GSHEET_TAB)
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(title=GSHEET_TAB, rows="1000", cols="10")

            # Write header if empty
            if ws.acell("A1").value is None:
                ws.update("A1", [["id","name","city","brokerage","last_sale","contact_link","source","created_at"]])

            # Append new lines for the NEW events (not full snapshot)
            lead_ids = [e["lead_id"] for e in events]
            new_rows = [r for r in rows if r.get("id") in lead_ids]
            values = [[
                r.get("id"), r.get("name"), r.get("city"), r.get("brokerage"),
                r.get("last_sale"), r.get("contact_link"), r.get("source"), r.get("created_at")
            ] for r in new_rows]
            if values:
                ws.append_rows(values, value_input_option="RAW")
        except Exception:
            pass

    # Mark processed
    ids = [e["event_id"] for e in events]
    for eid in ids:
        supabase.table("lead_insert_events").update({"processed": True}).eq("event_id", eid).execute()

