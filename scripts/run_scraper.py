# scripts/run_scraper.py
from scraper.realtor_scraper import run_scrape
import os, json
from models import save_leads

if __name__ == "__main__":
    debug = os.getenv("DEBUG", "").lower() in ("1","true","yes")
    leads = run_scrape(debug=debug)
    print("Leads collected:", len(leads))
    if leads:
        try:
            res = save_leads(leads)
            print("save_leads result:", res)
        except Exception as e:
            print("Error saving leads:", e)
