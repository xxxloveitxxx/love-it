# scripts/run_realtor.py
"""
Run the Realtor scraper and (optionally) save leads via models.save_leads().
Designed to be executed by CI / GitHub Actions. Uses PYTHONPATH=. so `import models` works.
"""

import os
from scraper.realtor_scraper import run_scrape

def main():
    debug = os.getenv("DEBUG", "").lower() in ("1", "true", "yes")
    # Optional overrides from env
    max_per = os.getenv("MAX_LISTINGS_PER_SEARCH")
    max_total = os.getenv("MAX_LISTINGS_TOTAL")
    seeds = os.getenv("REALTOR_SEED_URLS")

    kwargs = {"debug": debug}
    if max_per:
        kwargs["max_per_search"] = int(max_per)
    if max_total:
        kwargs["max_total"] = int(max_total)
    if seeds:
        kwargs["search_urls"] = [s.strip() for s in seeds.split(",") if s.strip()]

    print("Starting scraper with kwargs:", kwargs)
    leads = run_scrape(**kwargs)
    print("SCRAPED", len(leads), "leads")

    # Try to save using models.save_leads if available
    try:
        import models  # your repo's models.py
        if hasattr(models, "save_leads"):
            try:
                print("Saving via models.save_leads()")
                res = models.save_leads(leads)
                print("Save result:", res)
            except Exception as e:
                print("Error while calling models.save_leads():", e)
        else:
            print("models.save_leads not found; skipping save.")
    except Exception as e:
        print("Could not import models module; skipping save. Import error:", e)

if __name__ == "__main__":
    main()
