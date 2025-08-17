# models.py
import os
from supabase import create_client, Client

# Load Supabase credentials from environment variables
SUPABASE_URL: str = os.environ.get("SUPABASE_URL")
SUPABASE_KEY: str = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("⚠️ SUPABASE_URL and SUPABASE_KEY must be set in environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def insert_lead(name: str, city: str, brokerage: str, last_sale: str, contact_link: str, source: str = "zillow"):
    """
    Insert a lead into Supabase.
    Duplicate entries are ignored because of the unique index.
    """
    data = {
        "name": name,
        "city": city,
        "brokerage": brokerage,
        "last_sale": last_sale,
        "contact_link": contact_link,
        "source": source,
    }
    try:
        result = supabase.table("leads").insert(data).execute()
        return result.data
    except Exception as e:
        print(f"⚠️ Error inserting lead: {e}")
        return None


def get_all_leads(limit: int = 50):
    """
    Fetch leads from Supabase, newest first.
    """
    try:
        result = (
            supabase.table("leads")
            .select("*")
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        return result.data
    except Exception as e:
        print(f"⚠️ Error fetching leads: {e}")
        return []
