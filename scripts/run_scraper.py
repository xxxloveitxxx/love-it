# scripts/run_scraper.py

import os
import asyncio
from pprint import pprint

# make sure our repo root is importable when run by GH Actions; your CI already does PYTHONPATH=.
# from models import save_leads   <- import inside main so errors surface after scrape

async def main():
    from scraper.zillow_scraper import run_scrape
    from models import save_leads

    seeds_env = os.environ.get("SCRAPE_SEEDS")
    if seeds_env:
        seeds = [s.strip() for s in seeds_env.split(",") if s.strip()]
    else:
        seeds = None  # let run_scrape use defaults

    print("Starting Zillow scrape; seeds:", "custom" if seeds else "default")
    leads = await run_scrape(search_urls=seeds, max_per_seed=6, max_listings=12, debug=True)
    print("Scraped", len(leads), "leads")
    pprint(leads[:3])
    print("Saving leads via models.save_leads...")
    # save_leads should accept List[dict] and return supabase response
    res = save_leads(leads)
    print("Save result:", res)

if __name__ == "__main__":
    asyncio.run(main())
