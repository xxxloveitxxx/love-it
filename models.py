# models.py
"""
Supabase helper for leads table.

Provides: insert_lead, save_leads, get_all_leads.
Requires SUPABASE_URL and SUPABASE_KEY as environment variables.
"""

from typing import List, Dict, Any, Optional, Tuple
import os
from dotenv import load_dotenv

load_dotenv()

# supabase client
try:
    from supabase import create_client, Client
except Exception as e:
    raise RuntimeError("Missing supabase package. Make sure `supabase` is in requirements.txt") from e

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    # fail early: makes CI errors easier to understand
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in environment / Secrets")

_sb_client: Optional[Client] = None


def get_supabase() -> Client:
    """Return a cached Supabase client."""
    global _sb_client
    if _sb_client is None:
        _sb_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _sb_client


def _extract_response(resp: Any) -> Tuple[Optional[Any], Optional[Any]]:
    """
    Robustly extract (data, error) from various supabase client response shapes.
    """
    # supabase-py v2 often returns a dict-like object with keys 'data' and 'error'
    try:
        if isinstance(resp, dict):
            return resp.get("data"), resp.get("error")
        # some wrappers return an object with attributes
        data = getattr(resp, "data", None)
        error = getattr(resp, "error", None)
        # if still None, try dict-like access
        if data is None and hasattr(resp, "get"):
            data = resp.get("data")
            error = resp.get("error")
        return data, error
    except Exception:
        return None, f"unexpected response shape: {type(resp)}"


def insert_lead(lead: Dict[str, Any], on_conflict: str = "email") -> Dict[str, Any]:
    """
    Insert/upsert a single lead into the 'leads' table.
    - lead: dict with keys matching your Supabase table columns (name, email, city, brokerage, last_sale, source, etc.)
    - on_conflict: column name to use for upsert deduplication (default: 'email')
    Returns dict: {'data': ..., 'error': ...}
    """
    if not isinstance(lead, dict):
        raise ValueError("lead must be a dict")

    client = get_supabase()
    try:
        resp = client.table("leads").upsert([lead], on_conflict=on_conflict).execute()
        data, error = _extract_response(resp)
        return {"data": data, "error": error}
    except Exception as exc:
        return {"data": None, "error": str(exc)}


def save_leads(leads: List[Dict[str, Any]], on_conflict: str = "email") -> Dict[str, Any]:
    """
    Insert/upsert multiple leads.
    - leads: list of dicts
    - on_conflict: upsert key (default 'email')
    Returns dict: {'data': ..., 'error': ...}
    """
    if not isinstance(leads, list):
        raise ValueError("leads must be a list of dicts")

    if len(leads) == 0:
        return {"data": [], "error": None}

    client = get_supabase()
    try:
        resp = client.table("leads").upsert(leads, on_conflict=on_conflict).execute()
        data, error = _extract_response(resp)
        return {"data": data, "error": error}
    except Exception as exc:
        return {"data": None, "error": str(exc)}


def get_all_leads(limit: Optional[int] = 100, offset: int = 0, order_by: Optional[str] = None, desc: bool = True) -> Dict[str, Any]:
    """
    Fetch leads from the 'leads' table.
    - limit: number of rows to return (None => no limit)
    - offset: offset for paging
    - order_by: column name to order by (e.g. 'created_at'), or None
    - desc: whether to order descending
    Returns dict: {'data': [...], 'error': ...}
    """
    client = get_supabase()
    try:
        query = client.table("leads").select("*")
        if order_by:
            # supabase-py order signature: .order(column, desc=True/False)
            query = query.order(order_by, desc=desc)
        if limit is not None:
            query = query.limit(limit).offset(offset)
        resp = query.execute()
        data, error = _extract_response(resp)
        return {"data": data, "error": error}
    except Exception as exc:
        return {"data": None, "error": str(exc)}


# convenience alias for backwards compatibility (in case other parts import this)
def fetch_leads(*args, **kwargs):
    return get_all_leads(*args, **kwargs)


# Exported names
__all__ = [
    "get_supabase",
    "insert_lead",
    "save_leads",
    "get_all_leads",
    "fetch_leads",
]
