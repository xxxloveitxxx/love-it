# scraper/zillow_scraper.py
"""
Async Zillow scraper using Playwright.

Usage:
    from scraper.zillow_scraper import run_scrape
    leads = await run_scrape(search_urls=[...], max_properties=6, headless=True, debug=True)

Notes:
 - This is best-effort scraping of public listing pages.
 - It scrolls the search page to load more cards, collects anchors with '/homedetails/' or '/homedetails' in href,
   then opens each listing and extracts a few fields (best-effort).
 - It intentionally uses random short delays and a conservative max_properties to reduce load.
"""
import os
import asyncio
import random
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

DEFAULT_SEEDS = [
    "https://www.zillow.com/homes/for_sale/San-Francisco-CA_rb/",
    "https://www.zillow.com/homes/for_sale/Los-Angeles-CA_rb/",
]

def _get_seed_urls(provided: Optional[List[str]] = None) -> List[str]:
    if provided:
        return provided
    env = os.getenv("ZILLOW_SEED_URLS", "")
    if env:
        return [s.strip() for s in env.split(",") if s.strip()]
    return DEFAULT_SEEDS

def _normalize_url(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("/"):
        return urljoin("https://www.zillow.com", href)
    return href

async def _scroll_and_collect_links(page, max_links_per_seed: int, debug: bool=False) -> List[str]:
    """Scroll the search page to load listings and collect listing links."""
    links: List[str] = []
    seen: Set[str] = set()

    # How many scrolls to try (conservative)
    max_scrolls = 10
    for i in range(max_scrolls):
        if debug:
            print(f"[debug] scroll pass {i+1}/{max_scrolls}")

        # collect anchors likely pointing to listing pages
        anchors = await page.query_selector_all("a")
        for a in anchors:
            try:
                href = await a.get_attribute("href")
            except Exception:
                href = None
            if not href:
                continue
            href = _normalize_url(href)
            if not href:
                continue
            # Zillow listing pages commonly contain '/homedetails/' or contain '_zpid' or '/homedetails'
            if ("/homedetails/" in href) or ("_zpid" in href and "homedetails" in href) or "/b/" in href:
                if href not in seen:
                    seen.add(href)
                    links.append(href)
                    if debug:
                        print(f"[debug] found {href}")
                    if len(links) >= max_links_per_seed:
                        return links

        # scroll a bit to load more results
        try:
            await page.evaluate(
                """() => {
                    window.scrollBy(0, Math.max(document.body.clientHeight, 800));
                }"""
            )
        except Exception:
            pass

        # wait a bit (randomized) for JS to load more cards
        await asyncio.sleep(1.0 + random.random() * 1.5)

    return links

async def _extract_listing_data(page, url: str, debug: bool=False) -> Dict:
    """Best-effort extraction of agent/name/city/brokerage/last_sale from a listing page."""
    name = None
    city = None
    brokerage = None
    email = None
    last_sale = None

    # Wait for a main address or header to appear (if blocked this may timeout)
    try:
        # try a few useful selectors
        await page.wait_for_timeout(500)  # give the page a moment
        # address / title - common patterns
        for sel in ["h1", ".ds-address-container", "h1.ds-address-container", ".zsg-content-header", ".StyledPropertyCardData"]:
            try:
                el = await page.query_selector(sel)
                if el:
                    text = (await el.inner_text()).strip()
                    if text:
                        city = text
                        if debug:
                            print(f"[debug] address/title ({sel}): {text}")
                        break
            except Exception:
                continue

        # agent / listing by
        agent_selectors = [
            "a.ds-agent-name", "text=Listing by", ".ds-listing-agent-title", '[data-testid="listing-agent-name"]',
            ".listing-agent-name", ".zsg-agent-name", ".agent-name"
        ]
        for sel in agent_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    text = (await el.inner_text()).strip()
                    if text:
                        name = text
                        if debug:
                            print(f"[debug] agent ({sel}): {text}")
                        break
            except Exception:
                continue

        # brokerage/company
        brokerage_selectors = [
            ".ds-listing-agent-company", ".agent-company", '[data-testid="brokerage-name"]', ".listing-agent-company"
        ]
        for sel in brokerage_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    text = (await el.inner_text()).strip()
                    if text:
                        brokerage = text
                        if debug:
                            print(f"[debug] brokerage ({sel}): {text}")
                        break
            except Exception:
                continue

        # last sale / sold info
        sold_keywords = ["Last sold", "Sold for", "Last sold for", "Sold on"]
        page_text = await page.content()
        # quick best-effort search for a short snippet
        for kw in sold_keywords:
            if kw in page_text:
                # find a surrounding chunk
                try:
                    # prefer a visible element containing the keyword
                    handles = await page.query_selector_all(f"text={kw}")
                    if handles:
                        h = handles[0]
                        parent = await h.evaluate_handle("node => node.parentElement || node")
                        last_sale_text = await (await parent.get_property("innerText")).json_value()
                        last_sale = last_sale_text.strip()
                    else:
                        # fallback to content search: extract substring
                        idx = page_text.find(kw)
                        snippet = page_text[max(0, idx-100): idx+200]
                        last_sale = " ".join(snippet.split())[:400]
                    if debug and last_sale:
                        print(f"[debug] last_sale found near '{kw}': {last_sale[:120]}")
                    break
                except Exception:
                    continue

        # Emails are rarely on Zillow public pages. But try to find an @ in page text (best-effort)
        if "@" in page_text:
            # very naive: find first token with @
            import re
            m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", page_text)
            if m:
                email = m.group(0)
                if debug:
                    print(f"[debug] found possible email: {email}")

    except PWTimeoutError:
        if debug:
            print("[debug] Timeout waiting on page content")
    except Exception as e:
        if debug:
            print("[debug] extraction error:", e)

    return {
        "name": name,
        "city": city,
        "email": email,
        "brokerage": brokerage,
        "last_sale": last_sale,
        "source": "zillow",
        "url": url,
    }

async def run_scrape(
    search_urls: Optional[List[str]] = None,
    max_properties: int = 6,
    headless: bool = True,
    debug: bool = False,
) -> List[Dict]:
    """
    Async run_scrape.

    Args:
      search_urls: list of seed search pages.
      max_properties: total number of listing pages to visit across seeds.
      headless: run headless or not.
      debug: print debug logs.

    Returns:
      list of lead dicts.
    """
    seeds = _get_seed_urls(search_urls)
    results = []
    per_seed_limit = max(1, max_properties)  # we'll limit globally as well

    pw_args = ["--no-sandbox", "--disable-dev-shm-usage"]
    if debug:
        print(f"[debug] seeds: {seeds}, max_properties: {max_properties}, headless: {headless}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, args=pw_args)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            timezone_id="America/Los_Angeles",
        )
        page = await context.new_page()

        for seed in seeds:
            if len(results) >= max_properties:
                break
            try:
                if debug:
                    print(f"[debug] goto seed: {seed}")
                await page.goto(seed, wait_until="networkidle", timeout=30000)
            except PWTimeoutError:
                if debug:
                    print("[debug] seed load timed out, continuing")
                continue
            except Exception as e:
                if debug:
                    print("[debug] seed load error:", e)
                continue

            # small wait to allow client-side rendering
            await asyncio.sleep(1.0 + random.random() * 1.5)

            # scroll the search results and collect links
            links = await _scroll_and_collect_links(page, max_links_per_seed=per_seed_limit, debug=debug)
            if debug:
                print(f"[debug] collected {len(links)} listing links from seed")

            for link in links:
                if len(results) >= max_properties:
                    break
                # skip duplicates
                if any(r.get("url") == link for r in results):
                    continue

                # basic anti-block detection
                url_lower = (link or "").lower()
                if "captcha" in url_lower or "access-denied" in url_lower:
                    if debug:
                        print("[debug] skipping blocked url:", link)
                    continue

                try:
                    if debug:
                        print("[debug] visiting listing:", link)
                    await page.goto(link, wait_until="networkidle", timeout=30000)
                except PWTimeoutError:
                    if debug:
                        print("[debug] listing load timed out:", link)
                    continue
                except Exception as e:
                    if debug:
                        print("[debug] error loading listing:", e)
                    continue

                # best-effort extraction
                lead = await _extract_listing_data(page, link, debug=debug)
                results.append(lead)

                # polite delay between listing visits
                await asyncio.sleep(1.2 + random.random() * 2.0)

        try:
            await context.close()
            await browser.close()
        except Exception:
            pass

    if debug:
        print(f"[debug] finished; total leads: {len(results)}")
    return results

# run quick local test
if __name__ == "__main__":
    import asyncio, json
    leads = asyncio.run(run_scrape(max_properties=4, headless=True, debug=True))
    print(json.dumps(leads, indent=2))
