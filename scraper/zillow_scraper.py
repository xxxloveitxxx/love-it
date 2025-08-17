# scraper/zillow_scraper.py
"""
Async Zillow scraper using Playwright async API.

Call:
    leads = await run_scrape(search_urls=[...], max_properties=10, headless=True)

Returns:
    list[dict] with keys: name, city, email, brokerage, last_sale, source, url
"""
import os
import asyncio
from typing import List, Dict, Optional
from urllib.parse import urljoin

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

DEFAULT_SEEDS = [
    "https://www.zillow.com/homes/for_sale/"
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

async def _collect_listing_links_from_search_page(page, max_links: int) -> List[str]:
    links: List[str] = []
    try:
        # heuristics for common listing anchors
        anchors = await page.query_selector_all("a[href*='/homedetails/'], a.list-card-link, a.zsg-photo-card-overlay-link")
        for a in anchors:
            href = await a.get_attribute("href")
            href = _normalize_url(href)
            if not href:
                continue
            if href not in links:
                links.append(href)
            if len(links) >= max_links:
                break
    except Exception:
        pass

    # fallback scan
    if not links:
        try:
            anchors = await page.query_selector_all("a")
            for a in anchors:
                href = await a.get_attribute("href")
                if href and "/homedetails/" in href:
                    href = _normalize_url(href)
                    if href and href not in links:
                        links.append(href)
                    if len(links) >= max_links:
                        break
        except Exception:
            pass

    return links

async def _extract_listing_data(page, url: str) -> Dict:
    name = None
    city = None
    brokerage = None
    email = None
    last_sale = None

    try:
        addr_el = await page.query_selector("h1, .ds-address-container, .zsg-content-header, .ds-address")
        if addr_el:
            city = (await addr_el.inner_text()).strip()

        agent_sel_variants = [
            "a.ds-agent-name",
            ".listing-agent-name",
            ".zsg-agent-name",
            ".ds-listing-agent-title a",
            "text=Listing by"
        ]
        for sel in agent_sel_variants:
            try:
                el = await page.query_selector(sel)
                if el:
                    text = (await el.inner_text()).strip()
                    if text:
                        name = text
                        break
            except Exception:
                continue

        brokerage_sel_variants = [
            ".ds-listing-agent-company", ".zsg-agent-company", ".agent-company"
        ]
        for sel in brokerage_sel_variants:
            try:
                el = await page.query_selector(sel)
                if el:
                    brokerage = (await el.inner_text()).strip()
                    break
            except Exception:
                continue

        # last sale / sold info - try searching for "Last sold" text nodes, best-effort
        try:
            sold_el = await page.query_selector("text=Last sold")
            if sold_el:
                # get parent or surrounding
                parent = await sold_el.evaluate_handle("node => node.parentElement || node")
                last_sale = (await (await parent.get_property("innerText")).json_value()).strip()
        except Exception:
            pass

        # Zillow normally doesn't reveal emails publicly
        email = None

    except Exception:
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

async def run_scrape(
    search_urls: Optional[List[str]] = None,
    max_properties: int = 6,
    headless: bool = True,
) -> List[Dict]:
    """
    Async run_scrape that your runner can await.

    Args:
        search_urls: optional list of search pages to start from. If None, uses env or defaults.
        max_properties: total number of property pages to scrape across seeds.
        headless: run headless browser if True.

    Returns:
        list of lead dicts.
    """
    seeds = _get_seed_urls(search_urls)
    results: List[Dict] = []
    per_seed_limit = max(1, max_properties)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, args=["--no-sandbox"])
        context = await browser.new_context()
        page = await context.new_page()

        for seed in seeds:
            if len(results) >= max_properties:
                break
            try:
                await page.goto(seed, timeout=30000)
            except PWTimeoutError:
                continue
            except Exception:
                continue

            # let dynamic content render
            await asyncio.sleep(2)

            links = await _collect_listing_links_from_search_page(page, per_seed_limit)
            for link in links:
                if len(results) >= max_properties:
                    break
                try:
                    await page.goto(link, timeout=30000)
                except PWTimeoutError:
                    continue
                except Exception:
                    continue

                await asyncio.sleep(2)
                lead = await _extract_listing_data(page, link)
                results.append(lead)
                await asyncio.sleep(1.0)

        try:
            await context.close()
            await browser.close()
        except Exception:
            pass

    return results

# quick debug when invoked directly
if __name__ == "__main__":
    import asyncio, json
    seeds_env = os.getenv("ZILLOW_SEED_URLS")
    seeds = [s.strip() for s in seeds_env.split(",")] if seeds_env else None
    leads = asyncio.run(run_scrape(search_urls=seeds, max_properties=4, headless=True))
    print(json.dumps(leads, indent=2))
