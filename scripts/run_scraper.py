#!/usr/bin/env python3
# scripts/run_scraper.py
import os
import sys
import asyncio
from pprint import pprint

# make repo root importable
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# import the async scraper
from scraper.zillow_scraper import run_scrape

# import your DB helper if present
try:
    from models import save_leads
except Exception as e:
    print("Warning: could not import save_leads from models.py:", e)
    save_leads = None

async def main():
    # seeds can be passed from env or left None to use defaults
    seeds_env = os.getenv("ZILLOW_SEED_URLS", "")
    seeds = [s.strip() for s in seeds_env.split(",") if s.strip()] if seeds_env else None

    max_props = int(os.getenv("MAX_LISTINGS_TOTAL", "6"))
    headless = os.getenv("HEADLESS", "true").lower() not in ("0", "false", "no")

    print("Starting Zillow scrape; seeds:", seeds or "default")
    leads = await run_scrape(search_urls=seeds, max_properties=max_props, headless=headless)
    print("Scraped", len(leads), "leads")
    pprint(leads[:10])

    if save_leads:
        print("Saving leads via models.save_leads...")
        try:
            res = save_leads(leads)
            print("Save result:", res)
        except Exception as e:
            print("Error saving leads:", e)
    else:
        print("models.save_leads not available; skipping DB save. Implement save_leads(leads) in models.py")

if __name__ == "__main__":
    asyncio.run(main())
