# scraper/zillow_scraper.py
"""
Synchronous Zillow scraper using Playwright sync API.

Exports:
    run_scrape(max_properties: int = 6) -> list[dict]

Note: Zillow's markup changes frequently. This is a conservative, best-effort
template. Update selectors to match Zillow's live DOM for better data.
"""
import os
import time
from typing import List, Dict, Optional
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

DEFAULT_SEED = "https://www.zillow.com/homes/for_sale/"

def _get_seed_urls() -> List[str]:
    env = os.getenv("ZILLOW_SEED_URLS", "")
    if env:
        return [s.strip() for s in env.split(",") if s.strip()]
    return [DEFAULT_SEED]

def _normalize_url(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    # Zillow sometimes uses relative links
    if href.startswith("/"):
        return urljoin("https://www.zillow.com", href)
    return href

def _collect_listing_links_from_search_page(page, max_links: int) -> List[str]:
    """Return a list of listing URLs found on the search page (best-effort)."""
    links = []
    # Try a few heuristics for Zillow listing links
    try:
        # Common: /homedetails/... or links with "list-card-link" or "list-card"
        anchors = page.query_selector_all("a[href*='/homedetails/'], a.list-card-link, a.zsg-photo-card-overlay-link")
        for a in anchors:
            href = a.get_attribute("href")
            href = _normalize_url(href)
            if not href:
                continue
            if href not in links:
                links.append(href)
            if len(links) >= max_links:
                break
    except Exception:
        pass

    # Fallback: search for anchors with "zillow" and "homedetails"
    if not links:
        try:
            anchors = page.query_selector_all("a")
            for a in anchors:
                href = a.get_attribute("href")
                if href and "/homedetails/" in href:
                    href = _normalize_url(href)
                    if href and href not in links:
                        links.append(href)
                    if len(links) >= max_links:
                        break
        except Exception:
            pass

    return links

def _extract_listing_data(page, url: str) -> Dict:
    """Best-effort extraction for a single property page. Keep fields lightweight."""
    # Default values
    name = None
    city = None
    brokerage = None
    email = None
    last_sale = None

    try:
        # address/title -> usually in <h1> or a top header container
        addr_el = page.query_selector("h1, .ds-address-container, .zsg-content-header, .ds-address")
        if addr_el:
            city = addr_el.inner_text().strip()

        # Agent/Listing info - Zillow layout varies. Try multiple fallbacks.
        # Try: agent name link
        agent_sel_variants = [
            "a.ds-agent-name",        # some Zillow components
            ".listing-agent-name",    # fallback
            ".zsg-agent-name",
            ".ds-listing-agent-title a"
        ]
        for sel in agent_sel_variants:
            el = page.query_selector(sel)
            if el:
                text = el.inner_text().strip()
                if text:
                    name = text
                    break

        # Brokerage
        brokerage_sel_variants = [
            ".ds-listing-agent-company", ".zsg-agent-company", ".agent-company"
        ]
        for sel in brokerage_sel_variants:
            el = page.query_selector(sel)
            if el:
                brokerage = el.inner_text().strip()
                break

        # Last sale / transaction history - often not available; try to find "Last sold" text
        sold_el = page.query_selector("text=Last sold, .zsg-listing-attribute, .ds-home-fact-list")
        if sold_el:
            try:
                last_sale = sold_el.inner_text().strip()
            except Exception:
                last_sale = None

        # Zillow usually does not show agent emails publicly; leave None
        email = None

    except Exception:
        # If anything breaks, return whatever we managed to find
        pass

    return {
        "name": name,
        "city": city,
        "email": email,
        "brokerage": brokerage,
        "last_sale": last_sale,
        "source": "zillow",
        "url": url,
    }

def run_scrape(max_properties: int = 6, headless: bool = True) -> List[Dict]:
    """
    Run a synchronous scrape and return a list of lead dicts.

    Parameters
    ----------
    max_properties : int
        Maximum number of properties to scrape across seed pages (total).
    headless : bool
        If False, will run browsers visibly (useful for local debugging).
    """
    results: List[Dict] = []
    seeds = _get_seed_urls()
    per_seed_limit = max(1, max_properties)  # we'll cap overall later

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()

        for seed in seeds:
            if len(results) >= max_properties:
                break
            try:
                page.goto(seed, timeout=30000)
            except PWTimeoutError:
                # skip if page load times out
                continue
            except Exception:
                continue

            # Allow some JS to run — site is dynamic
            time.sleep(2)

            # collect candidate links from this search result page
            links = _collect_listing_links_from_search_page(page, per_seed_limit)
            # iterate links, visit each property
            for link in links:
                if len(results) >= max_properties:
                    break
                try:
                    page.goto(link, timeout=30000)
                except PWTimeoutError:
                    continue
                except Exception:
                    continue

                # let page render
                time.sleep(2)
                lead = _extract_listing_data(page, link)
                results.append(lead)

                # polite pause between visits
                time.sleep(1.0)

        # cleanup
        try:
            context.close()
            browser.close()
        except Exception:
            pass

    return results

# If run directly, print a short sample (helps debugging if someone runs this file)
if __name__ == "__main__":
    import json
    max_props = int(os.getenv("MAX_LISTINGS_TOTAL", "4"))
    leads = run_scrape(max_properties=max_props, headless=True)
    print(json.dumps(leads, indent=2))
