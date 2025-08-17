# scripts/run_scraper.py
import os
import sys
import asyncio
import json

# ensure repo root is importable
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from scraper.zillow_scraper import run_scrape
try:
    import models
except Exception:
    models = None

async def main():
    # read seeds from env; if absent run defaults inside run_scrape
    seeds_env = os.getenv("ZILLOW_SEED_URLS", "")
    if seeds_env:
        seeds = [s.strip() for s in seeds_env.split(",") if s.strip()]
    else:
        seeds = None  # allow scraper to use its built-in defaults

    # read various names that might be present in CI
    max_per_seed = os.getenv("MAX_LISTINGS_PER_SEARCH")
    max_listings = os.getenv("MAX_LISTINGS_TOTAL") or os.getenv("MAX_LISTINGS") or os.getenv("MAX_LISTINGS_TOTAL")
    debug = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

    # build kwargs - pass both canonical and possible aliases
    kwargs = {}
    if seeds is not None:
        kwargs["search_urls"] = seeds
    if max_per_seed:
        kwargs["max_per_seed"] = int(max_per_seed)
    if max_listings:
        kwargs["max_listings"] = int(max_listings)

    # call scraper (it accepts aliases)
    print("Starting Zillow scrape; seeds:", ("default" if seeds is None else seeds))
    leads = await run_scrape(debug=debug, **kwargs)

    print(f"Scraped {len(leads)} leads")
    if len(leads) > 0:
        print("Sample lead:")
        print(json.dumps(leads[0], indent=2))

    # try to save via models if available
    if models is not None:
        try:
            print("Saving leads via models.save_leads...")
            res = models.save_leads(leads)
            print("Save result:", res)
        except Exception as e:
            print("models.save_leads error:", e)
    else:
        print("models module not available — skipping save.")

if __name__ == "__main__":
    asyncio.run(main())
