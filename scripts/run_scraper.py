# scripts/run_scraper.py
import os
import sys
from pathlib import Path

# ---- Ensure repo root is on sys.path ----
# If this file lives in <repo>/scripts/run_scraper.py, repo_root = parent of scripts
repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# ---- Debug info (prints to CI logs) ----
print("CWD:", os.getcwd())
print("Script path:", Path(__file__).resolve())
print("Repo root added to sys.path:", repo_root)
print("sys.path[0:6]:", sys.path[:6])

# ---- Now import your module(s) ----
try:
    from models import save_leads
except Exception as e:
    print("ERROR importing 'models' module:", e)
    # show files at repo root to help debug in CI
    try:
        print("Files at repo root:", list(repo_root.iterdir()))
    except Exception:
        pass
    raise

# ---- Main runner (sample) ----
def main():
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
