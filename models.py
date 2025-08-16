# models.py
from supabase import create_client
import os

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)

def save_leads(leads):
    for lead in leads:
        supabase.table("leads").insert(lead).execute()

def get_all_leads():
    return supabase.table("leads").select("*").execute().data
