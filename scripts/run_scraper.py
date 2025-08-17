# scripts/run_scraper.py
import os
import sys
from pathlib import Path
import asyncio
from dotenv import load_dotenv

# ensure repo root on path
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

load_dotenv()

from scraper.zillow_scraper import run_scrape
from models import save_leads

# config: get seed urls from env variable or fallback to examples
SEED_URLS_RAW = os.getenv("ZILLOW_SEED_URLS", "")
if SEED_URLS_RAW:
    SEED_URLS = [u.strip() for u in SEED_URLS_RAW.split(",") if u.strip()]
else:
    # Example search seed(s) — replace with the search URLs you want to scan.
    SEED_URLS = [
        "https://www.zillow.com/homes/for_sale/San-Francisco-CA_rb/",
        "https://www.zillow.com/homes/for_sale/Los-Angeles-CA_rb/"
    ]


async def main():
    print("Starting Zillow scrape; seeds:", SEED_URLS)
    leads = await run_scrape(
        search_urls=SEED_URLS,
        max_listings_per_search=int(os.getenv("MAX_LISTINGS_PER_SEARCH", "6")),
        max_listings_total=int(os.getenv("MAX_LISTINGS_TOTAL", "12")),
        headless=(os.getenv("PLAYWRIGHT_HEADLESS", "1") == "1"),
        delay_min=float(os.getenv("DELAY_MIN", "2.0")),
        delay_max=float(os.getenv("DELAY_MAX", "5.0")),
        proxy=os.getenv("HTTP_PROXY") or os.getenv("PROXY")
    )

    # Filter out leads with no name and no email (optional)
    filtered = []
    for l in leads:
        if l.get("name") or l.get("email"):
            filtered.append(l)

    print(f"Collected {len(filtered)} leads. Saving to Supabase...")
    if filtered:
        res = save_leads(filtered)
        print("Supabase result:", res)
    else:
        print("No leads to save.")

if __name__ == "__main__":
    asyncio.run(main())
