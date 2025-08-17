# models.py
from typing import List, Dict, Any, Optional
import os
from dotenv import load_dotenv

# Supabase client (supabase-py)
try:
    from supabase import create_client, Client
except Exception:
    # helpful error if the package is missing
    raise

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    # raise only at import-time in CI/local runs so errors are clear
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in environment / .env")

_sb_client: Optional[Client] = None


def get_supabase() -> Client:
    """Return a cached supabase client."""
    global _sb_client
    if _sb_client is None:
        _sb_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _sb_client


def save_leads(leads: List[Dict[str, Any]], on_conflict: str = "email") -> Dict[str, Any]:
    """
    Save a list of lead dicts to the 'leads' table in Supabase.

    - leads: list of dicts; keys must match your table columns (e.g. name, email, city, brokerage, last_sale, source)
    - on_conflict: column name used to deduplicate/upsert (default 'email').

    Returns a dict with 'data' and 'error' keys (best-effort).
    """
    if not isinstance(leads, list):
        raise ValueError("leads must be a list of dictionaries")

    if len(leads) == 0:
        return {"data": [], "error": None}

    client = get_supabase()
    try:
        # Use upsert so duplicates (by email or other unique key) update instead of erroring.
        resp = client.table("leads").upsert(leads, on_conflict=on_conflict).execute()

        # The supabase client returns a response object or dict depending on version;
        # be defensive when extracting values:
        data = getattr(resp, "data", None) or resp.get("data") if isinstance(resp, dict) else resp
        error = getattr(resp, "error", None) or (resp.get("error") if isinstance(resp, dict) else None)

        return {"data": data, "error": error}
    except Exception as exc:
        return {"data": None, "error": str(exc)}
