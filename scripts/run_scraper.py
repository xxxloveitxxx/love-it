#!/usr/bin/env python3
# scripts/run_scraper.py
import os
import sys
import asyncio
from pprint import pprint

# ensure repo root importable
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scraper.zillow_scraper import run_scrape

# try to import your DB save helper
save_leads = None
try:
    from models import save_leads as save_leads_fn
    save_leads = save_leads_fn
except Exception as e:
    print("Warning: couldn't import save_leads from models.py:", e)

async def main():
    seeds_env = os.getenv("ZILLOW_SEED_URLS", "")
    seeds = [s.strip() for s in seeds_env.split(",") if s.strip()] if seeds_env else None

    max_props = int(os.getenv("MAX_LISTINGS_TOTAL", "6"))
    headless = os.getenv("HEADLESS", "true").lower() not in ("0", "false", "no")
    debug = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

    print("Starting Zillow scrape; seeds:", seeds or "default")
    leads = await run_scrape(search_urls=seeds, max_properties=max_props, headless=headless, debug=debug)
    print("Scraped", len(leads), "leads")
    if debug:
        pprint(leads)

    if save_leads:
        print("Saving leads via models.save_leads...")
        try:
            res = save_leads(leads)
            print("Save result:", res)
        except Exception as e:
            print("Error saving leads:", e)
    else:
        print("No save_leads available; skipping DB save. Implement save_leads(leads) in models.py")

if __name__ == "__main__":
    asyncio.run(main())
