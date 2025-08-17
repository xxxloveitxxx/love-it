# scripts/run_scraper.py
import os
from dotenv import load_dotenv

# load .env if present (useful for local dev)
load_dotenv()

from models import save_leads

def main():
    # Minimal smoke-test payload. Replace with your real scraper invocation.
    sample_leads = [
        {
            "name": "GitHub Actions Test Agent",
            "email": "gh-action-test+1@example.com",
            "city": "Test City",
            "brokerage": "Test Brokerage",
            "last_sale": "2025-08-01",
            "source": "zillow"
        }
    ]

    print("Saving sample leads to Supabase...")
    res = save_leads(sample_leads)
    print("Result:", res)

if __name__ == "__main__":
    main()
