# scraper/realtor_scraper.py
"""
Lightweight Realtor.com scraper with improved rate-limit handling and retries.

Usage:
  from scraper.realtor_scraper import run_scrape
  leads = run_scrape(debug=True, max_per_search=3, max_total=6)
"""
from typing import List, Dict, Optional
import os
import time
import random
import json
import re
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

# --- config / helpers ----------------------------------------------------

DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

HEADERS_BASE = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

DEFAULT_TIMEOUT = 15.0
DEFAULT_MAX_RETRIES = 5
BASE_BACKOFF = 3.0

# Use a single session for pooling
_session: Optional[requests.Session] = None


def _ensure_session() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update({"Accept": HEADERS_BASE["Accept"], "Accept-Language": HEADERS_BASE["Accept-Language"]})
        _session = s
    return _session


def _get_headers() -> Dict[str, str]:
    h = HEADERS_BASE.copy()
    h["User-Agent"] = random.choice(DEFAULT_USER_AGENTS)
    # sometimes adding a plausible Referer helps
    h["Referer"] = "https://www.google.com/"
    return h


def _choose_proxy(debug: bool = False) -> Optional[Dict[str, str]]:
    """
    Read REALTOR_PROXIES env var (comma-separated) and choose one at random.
    Example format:
      http://user:pass@1.2.3.4:8000,http://user2:pass2@5.6.7.8:8000
    Returns requests-compatible proxies dict or None.
    """
    raw = os.getenv("REALTOR_PROXIES", "").strip()
    if not raw:
        return None
    proxies = [p.strip() for p in raw.split(",") if p.strip()]
    if not proxies:
        return None
    sel = random.choice(proxies)
    if debug:
        print(f"[realtor] selected proxy: {sel}")
    return {"http": sel, "https": sel}


def fetch_with_retries(
    url: str,
    session: Optional[requests.Session] = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout: float = DEFAULT_TIMEOUT,
    backoff_base: float = BASE_BACKOFF,
    debug: bool = False,
) -> Optional[requests.Response]:
    """
    Fetch URL using requests with retries, backoff, jitter, and 429/Retry-After handling.
    Returns Response on success (status_code == 200) or None on persistent fail.
    """
    session = session or _ensure_session()
    proxies_template = _choose_proxy(debug=debug)
    for attempt in range(1, max_retries + 1):
        headers = _get_headers()
        proxies = proxies_template
        try:
            if debug:
                print(f"[realtor] fetch attempt {attempt} -> {url}")
            resp = session.get(url, headers=headers, timeout=timeout, proxies=proxies)
        except requests.RequestException as exc:
            wait = backoff_base * (2 ** (attempt - 1)) + random.uniform(0.5, 2.0)
            if debug:
                print(f"[realtor] network error: {exc}; sleeping {wait:.1f}s before retry")
            time.sleep(wait)
            continue

        # If successful
        if resp.status_code == 200:
            return resp

        # Rate limited handling (429)
        if resp.status_code == 429:
            # try to honor Retry-After header
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    wait = float(ra)
                except Exception:
                    # sometimes Retry-After is a date; fallback
                    wait = backoff_base * (2 ** (attempt - 1)) + random.uniform(1.0, 3.0)
            else:
                wait = backoff_base * (2 ** (attempt - 1)) + random.uniform(1.0, 4.0)

            if debug:
                print(f"[realtor] rate limited (429). sleeping {wait:.1f}s (attempt {attempt})")
            time.sleep(wait)
            continue

        # Other non-200: print snippet in debug, and optionally retry a few times for 5xx
        if 500 <= resp.status_code < 600 and attempt < max_retries:
            wait = backoff_base * (2 ** (attempt - 1)) + random.uniform(0.5, 2.5)
            if debug:
                snippet = resp.text[:800].replace("\n", " ")
                print(f"[realtor] server error {resp.status_code}; sleeping {wait:.1f}s before retry; snippet: {snippet!r}")
            time.sleep(wait)
            continue

        # For other status codes (403, 401, 404, etc.), provide debug info and bail
        if debug:
            snippet = resp.text[:800].replace("\n", " ")
            print(f"[realtor] non-200 response: {resp.status_code}; snippet: {snippet!r}")
        return resp

    if debug:
        print(f"[realtor] failed to fetch {url} after {max_retries} attempts")
    return None


def extract_json_ld(html: str) -> List[Dict]:
    """Extract & parse JSON-LD script blocks (best-effort)."""
    out: List[Dict] = []
    # quick regex for script blocks (robust to whitespace)
    pattern = re.compile(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.DOTALL | re.IGNORECASE)
    matches = pattern.findall(html)
    for raw in matches:
        txt = raw.strip()
        if not txt:
            continue
        # sometimes the JSON-LD contains HTML comments or CDATA wrappers
        txt = txt.replace("/*<![CDATA[*/", "").replace("/*]]>*/", "").strip()
        try:
            parsed = json.loads(txt)
            if isinstance(parsed, list):
                out.extend(parsed)
            else:
                out.append(parsed)
            continue
        except Exception:
            # attempt to salvage by splitting when concatenated objects present
            parts = re.split(r'\}\s*\{', txt)
            if len(parts) > 1:
                # re-add braces and try parse individually
                for i, part in enumerate(parts):
                    if i == 0:
                        s = part + "}"
                    elif i == len(parts) - 1:
                        s = "{" + part
                    else:
                        s = "{" + part + "}"
                    try:
                        parsed = json.loads(s)
                        if isinstance(parsed, list):
                            out.extend(parsed)
                        else:
                            out.append(parsed)
                    except Exception:
                        continue
            # else ignore this block
    return out


# --- listing discovery ---------------------------------------------------


def collect_listing_urls_from_search(
    search_url: str,
    limit: int = 12,
    debug: bool = False,
) -> List[str]:
    """
    Extract listing detail URLs from a Realtor search page with robust fallback strategies.
    """
    if debug:
        print("[realtor] fetching search page:", search_url)

    resp = fetch_with_retries(search_url, debug=debug)
    if not resp:
        if debug:
            print("[realtor] failed after retries")
        return []

    if resp.status_code != 200:
        if debug:
            print(f"[realtor] non-200 response: {resp.status_code}")
        return []

    html = resp.text
    lower = html.lower()
    # detect simple block/captcha
    if "captcha" in lower or "verify you are human" in lower or "access denied" in lower or "distil" in lower:
        if debug:
            print("[realtor] CAPTCHA / anti-bot content detected in search page.")
            # optionally print a short snippet to inspect
            print("[realtor] snippet:", html[:600].replace("\n", " "))
        return []

    found: List[str] = []

    # 1) JSON-LD method: ItemList or itemListElement often present
    try:
        for obj in extract_json_ld(html):
            if not isinstance(obj, dict):
                continue
            t = obj.get("@type") or obj.get("type")
            # item lists
            if t and t.lower() in ("itemlist",):
                items = obj.get("itemListElement") or []
                for it in items:
                    # item may be dict with 'url' or nested 'item'
                    if isinstance(it, dict):
                        url = it.get("url") or (it.get("item") or {}).get("url")
                        if url and "/realestateandhomes-detail/" in url:
                            full = urljoin(search_url, url)
                            if full not in found:
                                found.append(full)
                                if len(found) >= limit:
                                    return found
            # Sometimes an object is directly a listing
            if obj.get("url") and "/realestateandhomes-detail/" in obj.get("url"):
                u = urljoin(search_url, obj.get("url"))
                if u not in found:
                    found.append(u)
                    if len(found) >= limit:
                        return found
    except Exception as e:
        if debug:
            print("[realtor] json-ld extraction error:", e)

    # 2) Anchor scanning fallback
    try:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # realtor listing path pattern
            if "/realestateandhomes-detail/" in href:
                full = urljoin(search_url, href)
                if full not in found:
                    found.append(full)
                    if len(found) >= limit:
                        break
        # last attempt: sometimes links are JS encoded or use different patterns like '/home-details/'
        if len(found) < limit:
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if "detail" in href and ("realestateandhomes-detail" in href or "/home-details/" in href):
                    full = urljoin(search_url, href)
                    if full not in found:
                        found.append(full)
                        if len(found) >= limit:
                            break
    except Exception as e:
        if debug:
            print("[realtor] anchor scanning error:", e)

    if debug:
        print(f"[realtor] found {len(found)} listing urls (limit {limit})")

    return found[:limit]


# --- listing extraction --------------------------------------------------


def extract_listing_data(listing_url: str, debug: bool = False) -> Optional[Dict]:
    """
    Fetch a listing page and parse JSON-LD (preferred) and common DOM selectors as fallback.
    Returns a dict with typical fields.
    """
    if debug:
        print("[realtor] fetching listing:", listing_url)

    resp = fetch_with_retries(listing_url, debug=debug)
    if not resp:
        if debug:
            print("[realtor] failed to fetch listing after retries")
        return None

    if resp.status_code != 200:
        if debug:
            print("[realtor] non-200 listing response:", resp.status_code)
            print("[realtor] snippet:", resp.text[:600].replace("\n", " "))
        return None

    html = resp.text
    lower = html.lower()
    if "captcha" in lower or "verify you are human" in lower or "access denied" in lower:
        if debug:
            print("[realtor] CAPTCHA / anti-bot content detected on listing page.")
        return None

    result: Dict = {"url": listing_url, "source": "realtor"}

    # JSON-LD parsing (preferred)
    try:
        json_objs = extract_json_ld(html)
        for obj in json_objs:
            if not isinstance(obj, dict):
                continue
            t = obj.get("@type") or obj.get("type")
            # property-like objects
            if t and any(x in str(t).lower() for x in ("residence", "singlefamily", "house", "apartment")):
                addr = obj.get("address") or {}
                if isinstance(addr, dict):
                    if addr.get("streetAddress"):
                        result.setdefault("address", addr.get("streetAddress"))
                    if addr.get("addressLocality"):
                        result.setdefault("city", addr.get("addressLocality"))
                    if addr.get("addressRegion"):
                        result.setdefault("region", addr.get("addressRegion"))
                    if addr.get("postalCode"):
                        result.setdefault("postal_code", addr.get("postalCode"))
                # price may be inside offers
                offers = obj.get("offers") or {}
                if isinstance(offers, dict):
                    price = offers.get("price") or offers.get("priceSpecification", {}).get("price")
                    if price:
                        result.setdefault("price", price)
                elif obj.get("price"):
                    result.setdefault("price", obj.get("price"))
            # agent objects
            if t and "realestateagent" in str(t).lower():
                result.setdefault("agent_name", obj.get("name"))
                result.setdefault("agent_telephone", obj.get("telephone"))
                result.setdefault("agent_email", obj.get("email"))
                aff = obj.get("affiliation") or {}
                if isinstance(aff, dict):
                    result.setdefault("brokerage", aff.get("name"))
            # offers
            if t and "offer" in str(t).lower():
                if isinstance(obj.get("price"), (int, float, str)):
                    result.setdefault("price", obj.get("price"))
    except Exception as e:
        if debug:
            print("[realtor] JSON-LD parse error:", e)

    # DOM fallback
    try:
        soup = BeautifulSoup(html, "html.parser")
        if "address" not in result:
            for sel in ["h1", ".address", ".ldp-address", ".listing-street-address"]:
                el = soup.select_one(sel)
                if el and el.get_text(strip=True):
                    result["address"] = el.get_text(strip=True)
                    break
        if "price" not in result:
            for sel in [".price", ".rui__k8o6b6-0", ".ldp-price", ".listing-price"]:
                el = soup.select_one(sel)
                if el and el.get_text(strip=True):
                    result["price"] = el.get_text(strip=True)
                    break
        if "agent_name" not in result:
            el = soup.select_one(".listing-agent-name, .agent-name, .broker-name")
            if el and el.get_text(strip=True):
                result["agent_name"] = el.get_text(strip=True)
        if "agent_telephone" not in result:
            tel = soup.select_one('a[href^="tel:"]')
            if tel and tel.get("href"):
                result["agent_telephone"] = tel.get("href").split("tel:")[-1].split("?")[0]
        if "agent_email" not in result:
            text_blob = soup.get_text(" ")
            if "@" in text_blob:
                m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text_blob)
                if m:
                    result["agent_email"] = m.group(0)
    except Exception as e:
        if debug:
            print("[realtor] DOM parsing error:", e)

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
    Accepts:
      - search_urls: list or None (reads REALTOR_SEED_URLS or defaults)
      - max_per_search: int
      - max_total: int
      - debug: bool
    """
    # env defaults
    if search_urls is None:
        env_val = os.getenv("REALTOR_SEED_URLS", "").strip()
        if env_val:
            search_urls = [s.strip() for s in env_val.split(",") if s.strip()]
        else:
            search_urls = [
                "https://www.realtor.com/realestateandhomes-search/San-Francisco_CA",
                "https://www.realtor.com/realestateandhomes-search/Los-Angeles_CA",
                "https://www.realtor.com/realestateandhomes-search/New-York_NY",
            ]

    max_per_search = int(max_per_search or os.getenv("MAX_LISTINGS_PER_SEARCH", "6"))
    max_total = int(max_total or os.getenv("MAX_LISTINGS_TOTAL", "12"))

    if debug:
        print("[realtor] starting run_scrape; seeds:", search_urls)

    session = _ensure_session()

    all_listing_urls: List[str] = []
    for seed in search_urls:
        # small randomized delay between seeds to avoid burst behaviour
        if debug:
            print(f"[realtor] processing seed: {seed}")
        time.sleep(random.uniform(1.5, 3.5))
        urls = collect_listing_urls_from_search(seed, limit=max_per_search, debug=debug)
        for u in urls:
            if u not in all_listing_urls:
                all_listing_urls.append(u)
            if len(all_listing_urls) >= max_total:
                break
        if len(all_listing_urls) >= max_total:
            break

    if debug:
        print(f"[realtor] will scrape {len(all_listing_urls)} listings total")

    leads: List[Dict] = []
    for idx, url in enumerate(all_listing_urls[:max_total]):
        # polite pacing
        time.sleep(1.0 + random.random() * 2.0)
        try:
            data = extract_listing_data(url, debug=debug)
            if data:
                leads.append(data)
        except Exception as e:
            if debug:
                print(f"[realtor] per-listing error for {url}:", e)
            continue

    if debug:
        print(f"[realtor] scraped {len(leads)} leads")

    return leads


# If run as script, fetch and attempt to save via models.save_leads() if available
if __name__ == "__main__":
    import sys

    DEBUG = bool(os.getenv("DEBUG", "False").lower() in ("1", "true", "yes"))
    MAX_PER = int(os.getenv("MAX_LISTINGS_PER_SEARCH", "6"))
    MAX_TOTAL = int(os.getenv("MAX_LISTINGS_TOTAL", "12"))
    seeds_env = os.getenv("REALTOR_SEED_URLS", "").strip()
    seeds = [s.strip() for s in seeds_env.split(",")] if seeds_env else None

    leads = run_scrape(search_urls=seeds, max_per_search=MAX_PER, max_total=MAX_TOTAL, debug=DEBUG)
    print("SCRAPED", len(leads))
    if DEBUG:
        print("Leads sample:", leads[:3])

    # Try to save via your models if present
    try:
        import models  # type: ignore
        if hasattr(models, "save_leads"):
            print("Saving leads via models.save_leads...")
            try:
                res = models.save_leads(leads)
                print("Save result:", res)
            except Exception as e:
                print("models.save_leads call failed:", e)
        else:
            print("models.save_leads not found; skipping save.")
    except Exception as e:
        print("Could not import models; skipping save. Import error:", e)
        # don't exit with non-zero, so CI/cron can continue
        sys.exit(0)

