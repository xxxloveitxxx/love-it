#!/usr/bin/env python3
# scripts/test_listing.py
import os
import asyncio
from pprint import pprint
import sys

# ensure repo root importable
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scraper.zillow_scraper import run_scrape

async def main():
    # set a single known listing URL here or via env var LISTING_URL
    listing = os.getenv("LISTING_URL") or "https://www.zillow.com/homedetails/48-Terra-Vista-Ave-A-San-Francisco-CA-94115/2060419537_zpid/"  # replace with a real listing
    print("Testing single listing:", listing)
    # run_scrape will accept search_urls forming seeds; pass our listing as seed so it visits it
    leads = await run_scrape(search_urls=[listing], max_properties=1, headless=False, debug=True)
    print("Result leads:")
    pprint(leads)

if __name__ == "__main__":
    asyncio.run(main())
