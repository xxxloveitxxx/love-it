# scraper/realtor_scraper.py
"""
Lightweight Realtor.com scraper with rate limiting handling.
"""
from typing import List, Dict, Optional
import os
import time
import random
import json
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

# --- config / helpers ----------------------------------------------------

DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

HEADERS_BASE = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

REQUEST_TIMEOUT = 15.0
MAX_RETRIES = 3
INITIAL_DELAY = 5.0

def get_headers():
    h = HEADERS_BASE.copy()
    h["User-Agent"] = random.choice(DEFAULT_USER_AGENTS)
    return h

def make_request(url: str, debug: bool = False) -> Optional[requests.Response]:
    """Make request with retries and exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                url,
                headers=get_headers(),
                timeout=REQUEST_TIMEOUT
            )
            
            if resp.status_code == 429:
                wait_time = INITIAL_DELAY * (2 ** attempt)
                if debug:
                    print(f"[realtor] rate limited, waiting {wait_time}s (attempt {attempt + 1})")
                time.sleep(wait_time)
                continue
                
            return resp
            
        except Exception as e:
            if debug:
                print(f"[realtor] request error (attempt {attempt + 1}):", e)
            time.sleep(INITIAL_DELAY * (2 ** attempt))
    
    return None

def extract_json_ld(html: str) -> List[Dict]:
    """Return JSON-LD objects found on the page (best-effort)."""
    out = []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        txt = tag.string
        if not txt:
            continue
        try:
            data = json.loads(txt)
            out.append(data)
        except Exception:
            # Sometimes JSON-LD contains multiple objects or trailing commas; try to safely load pieces
            try:
                cleaned = txt.strip()
                # if it's a list
                if cleaned.startswith("["):
                    data = json.loads(cleaned)
                    if isinstance(data, list):
                        out.extend(data)
                else:
                    # sometimes there are multiple JSON objects concatenated; split heuristically
                    for part in cleaned.split("\n"):
                        part = part.strip()
                        if not part:
                            continue
                        try:
                            parsed = json.loads(part)
                            out.append(parsed)
                        except Exception:
                            continue
            except Exception:
                continue
    return out


# --- listing discovery ---------------------------------------------------

def collect_listing_urls_from_search(
    search_url: str,
    limit: int = 12,
    debug: bool = False,
) -> List[str]:
    """
    Extract listing detail URLs from a Realtor search page with rate limit handling.
    """
    if debug:
        print("[realtor] fetching search page:", search_url)
    
    resp = make_request(search_url, debug)
    if not resp:
        if debug:
            print("[realtor] failed after retries")
        return []

    if resp.status_code != 200:
        if debug:
            print(f"[realtor] non-200 response: {resp.status_code}")
        return []

    html = resp.text

    # simple CAPTCHA/blocked detection
    lower = html.lower()
    if "captcha" in lower or "verify you are human" in lower or "bot" in lower:
        if debug:
            print("[realtor] CAPTCHA / anti-bot content detected in search page.")
        return []

    found = []
    # 1) JSON-LD
    for obj in extract_json_ld(html):
        # Many pages include ItemList or arrays describing search results; try several shapes.
        if isinstance(obj, dict):
            t = obj.get("@type")
            if t in ("ItemList",):
                items = obj.get("itemListElement") or []
                for it in items:
                    # item can be a dict with 'url' or 'item' containing 'url'
                    if isinstance(it, dict):
                        url = it.get("url") or (it.get("item") or {}).get("url")
                        if url and "/realestateandhomes-detail/" in url:
                            url = urljoin(search_url, url)
                            if url not in found:
                                found.append(url)
                                if len(found) >= limit:
                                    return found
            # Sometimes there's a direct url field for listings
            if obj.get("url") and "/realestateandhomes-detail/" in obj.get("url"):
                url = urljoin(search_url, obj.get("url"))
                if url not in found:
                    found.append(url)
                    if len(found) >= limit:
                        return found

    # 2) Anchor scanning fallback
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        # known Realtor listing segment
        if "/realestateandhomes-detail/" in href:
            full = urljoin(search_url, href)
            if full not in found:
                found.append(full)
                if len(found) >= limit:
                    break

    # 3) A last attempt: look for data attributes or links containing '/home-details/' variants
    if not found:
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "detail" in href and ("realestateandhomes-detail" in href or "/home-details/" in href):
                full = urljoin(search_url, href)
                if full not in found:
                    found.append(full)
                    if len(found) >= limit:
                        break

    if debug:
        print(f"[realtor] found {len(found)} listing urls (limit {limit})")
    return found[:limit]


# --- listing extraction --------------------------------------------------

def extract_listing_data(listing_url: str, debug: bool = False) -> Optional[Dict]:
    """
    Fetch a listing page and parse JSON-LD (preferred), falling back to a few DOM selectors.
    Returns a dict with common fields (url, address, city, price, agent_name, agent_phone, agent_email, brokerage).
    """
    if debug:
        print("[realtor] fetching listing:", listing_url)
    try:
        resp = requests.get(listing_url, headers=get_headers(), timeout=REQUEST_TIMEOUT)
    except Exception as e:
        if debug:
            print("[realtor] listing request error:", e)
        return None

    if resp.status_code != 200:
        if debug:
            print("[realtor] non-200 listing response:", resp.status_code)
        return None

    html = resp.text
    lower = html.lower()
    if "captcha" in lower or "verify you are human" in lower:
        if debug:
            print("[realtor] CAPTCHA / anti-bot content detected on listing page.")
        return None

    result = {"url": listing_url, "source": "realtor"}

    # JSON-LD parsing
    json_objs = extract_json_ld(html)
    for obj in json_objs:
        if isinstance(obj, dict):
            t = obj.get("@type")
            if t in ("SingleFamilyResidence","House","Apartment","Residence","Product","Offer"):
                # address
                addr = obj.get("address") or {}
                if isinstance(addr, dict):
                    result.setdefault("address", addr.get("streetAddress"))
                    result.setdefault("city", addr.get("addressLocality"))
                    result.setdefault("region", addr.get("addressRegion"))
                    result.setdefault("postal_code", addr.get("postalCode"))
                # price
                if "offers" in obj and isinstance(obj["offers"], dict):
                    price = obj["offers"].get("price")
                    if price:
                        result["price"] = price
                elif "price" in obj:
                    result["price"] = obj.get("price")
            if t in ("RealEstateAgent",):
                result.setdefault("agent_name", obj.get("name"))
                result.setdefault("agent_telephone", obj.get("telephone"))
                result.setdefault("agent_email", obj.get("email"))
                # affiliation might contain brokerage
                aff = obj.get("affiliation") or {}
                if isinstance(aff, dict):
                    result.setdefault("brokerage", aff.get("name"))
            # sometimes there's an Offer object with price
            if t == "Offer" and isinstance(obj.get("price"), (str, int, float)):
                result.setdefault("price", obj.get("price"))

    # DOM fallback: find common selectors (best-effort; site may change)
    if "price" not in result or "address" not in result:
        soup = BeautifulSoup(html, "html.parser")
        # address/title patterns
        if "address" not in result:
            for sel in ["h1", ".address", ".ldp-address", ".listing-street-address"]:
                el = soup.select_one(sel)
                if el and el.get_text(strip=True):
                    result["address"] = el.get_text(strip=True)
                    break
        # price patterns
        if "price" not in result:
            for sel in [".price", ".rui__k8o6b6-0", ".ldp-price"]:
                el = soup.select_one(sel)
                if el and el.get_text(strip=True):
                    result["price"] = el.get_text(strip=True)
                    break
        # agent info fallback
        if "agent_name" not in result:
            el = soup.select_one(".listing-agent-name, .agent-name")
            if el and el.get_text(strip=True):
                result["agent_name"] = el.get_text(strip=True)
        if "agent_telephone" not in result:
            el = soup.select_one('a[href^="tel:"]')
            if el and el.get("href"):
                result["agent_telephone"] = el.get("href").split("tel:")[-1].split("?")[0]

        # search for any visible email (rare)
        if "agent_email" not in result:
            text = soup.get_text(" ")
            if "@" in text:
                # simple regex
                import re
                m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
                if m:
                    result["agent_email"] = m.group(0)

    if debug:
        short = {k: result.get(k) for k in ("address", "city", "price", "agent_name", "agent_email")}
        print("[realtor] extracted:", short)

    return result


# --- public run function -------------------------------------------------

def run_scrape(
    search_urls: Optional[List[str]] = None,
    max_per_search: Optional[int] = None,
    max_total: Optional[int] = None,
    debug: bool = False,
) -> List[Dict]:
    """
    Synchronous entrypoint for collecting realtor leads.
    Returns list of leads (dictionaries).
    """
    # env defaults
    if search_urls is None:
        env = os.getenv("REALTOR_SEED_URLS", "").strip()
        if env:
            search_urls = [s.strip() for s in env.split(",") if s.strip()]
        else:
            # sensible defaults
            search_urls = [
                "https://www.realtor.com/realestateandhomes-search/San-Francisco_CA",
                "https://www.realtor.com/realestateandhomes-search/Los-Angeles_CA",
            ]

    max_per_search = int(max_per_search or os.getenv("MAX_LISTINGS_PER_SEARCH", "6"))
    max_total = int(max_total or os.getenv("MAX_LISTINGS_TOTAL", "12"))

    all_urls: List[str] = []
    for seed in search_urls:
        urls = collect_listing_urls_from_search(seed, limit=max_per_search, debug=debug)
        for u in urls:
            if u not in all_urls:
                all_urls.append(u)
            if len(all_urls) >= max_total:
                break
        if len(all_urls) >= max_total:
            break

    if debug:
        print(f"[realtor] will scrape {len(all_urls)} listings total")

    leads: List[Dict] = []
    for url in all_urls[:max_total]:
        # polite pacing
        time.sleep(1.0 + random.random() * 1.5)
        data = extract_listing_data(url, debug=debug)
        if data:
            leads.append(data)

    if debug:
        print(f"[realtor] scraped {len(leads)} leads")

    return leads


# If run as script, fetch and attempt to save via models.save_leads() if available
if __name__ == "__main__":
    import sys

    DEBUG = bool(os.getenv("DEBUG", "False").lower() in ("1", "true", "yes"))
    leads = run_scrape(debug=DEBUG)
    print("Leads:", leads)
    # Try to save via your models if present
    try:
        import models  # type: ignore
        if hasattr(models, "save_leads"):
            print("Saving leads via models.save_leads...")
            res = models.save_leads(leads)
            print("Save result:", res)
        else:
            print("models.save_leads not found; skipping save.")
    except Exception as e:
        print("Could not import models.save_leads:", e)
        sys.exit(0)
