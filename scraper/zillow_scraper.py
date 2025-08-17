# scraper/zillow_scraper.py
import asyncio
import random
import re
import time
from typing import List, Dict, Optional
from urllib.parse import urlparse, urljoin

from playwright.async_api import async_playwright, Page, Browser

# small set of realistic user agents (rotate)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko)"
    " Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/114.0.0.0 Safari/537.36",
]

# sensible defaults to avoid hitting the site too fast
DEFAULT_MIN_DELAY = 2.0
DEFAULT_MAX_DELAY = 6.0

async def random_delay(min_s=DEFAULT_MIN_DELAY, max_s=DEFAULT_MAX_DELAY):
    await asyncio.sleep(random.uniform(min_s, max_s))


async def collect_listing_links_from_search(page: Page, search_url: str, max_links: int = 20) -> List[str]:
    """
    Open a Zillow search results page and collect listing URLs.
    The function is defensive: it tries a few common patterns for result links.
    """
    await page.goto(search_url, wait_until="domcontentloaded")
    # give some rendering time for dynamic content
    await asyncio.sleep(1.0)

    # try multiple selectors commonly used for zillow search result links
    selectors = [
        'a.list-card-link',                 # older Zillow card
        'a[data-testid="property-card-link"]',
        'a.zsg-photo-link',                 # backup
        'a[href*="/homedetails/"]',
        'a[href*="/b/"]'                    # sometimes agent links or other
    ]

    hrefs = set()
    for sel in selectors:
        elements = await page.query_selector_all(sel)
        for e in elements:
            try:
                href = await e.get_attribute("href")
                if href:
                    # normalize relative URLs
                    if href.startswith("/"):
                        base = "{scheme}://{netloc}".format(**urlparse(page.url)._asdict())
                        href = urljoin(base, href)
                    # only collect Zillow listing-like URLs (homedetails or /b/)
                    if "/homedetails/" in href or "/homes/" in href:
                        hrefs.add(href.split("?")[0])
            except Exception:
                continue
        if len(hrefs) >= max_links:
            break

    # fallback: find all anchors and filter
    if len(hrefs) < max_links:
        all_links = await page.query_selector_all("a")
        for a in all_links:
            try:
                href = await a.get_attribute("href")
                if href and ("/homedetails/" in href or "/homes/" in href):
                    if href.startswith("/"):
                        base = "{scheme}://{netloc}".format(**urlparse(page.url)._asdict())
                        href = urljoin(base, href)
                    hrefs.add(href.split("?")[0])
            except Exception:
                pass
            if len(hrefs) >= max_links:
                break

    return list(hrefs)[:max_links]


async def extract_agent_info_from_listing(page: Page, listing_url: str) -> Dict[str, Optional[str]]:
    """
    Visit a Zillow listing page and try to extract:
    - agent name
    - email (if present)
    - brokerage
    - city
    - last_sale (if the page contains sold date or last sold price)
    """
    await page.goto(listing_url, wait_until="domcontentloaded")
    # let JS hydrate
    await asyncio.sleep(1.2 + random.random() * 1.2)

    text = await page.content()

    def try_selectors(selectors):
        for sel in selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    txt = (await el.inner_text()).strip()
                    if txt:
                        return txt
            except Exception:
                continue
        return None

    # candidate selectors for agent name & brokerage
    agent_selectors = [
        'a[data-testid="listing-agent-name"]',
        'span[data-testid="listing-agent-name"]',
        'a[href*="/agent/"]',
        'a[href*="/AgentDetails/"]',
        'div.listing-agent-name',
        'div.ds-agent-name'  # sometimes appears in new markup
    ]

    brokerage_selectors = [
        'div[data-testid="listing-agent-company"]',
        'span.ds-listing-company',
        'div.listing-agent-company',
        'li.brokerage',
    ]

    city = None
    # try location breadcrumbs or address block
    city = try_selectors([
        'h1[data-testid="home-details-summary-headline"]',
        'h1.ds-address-container',
        'h1.zsg-h1',
        'h1'
    ])
    if city:
        # city extraction heuristic: last two tokens or token after comma
        m = re.search(r",\s*([A-Za-z .'-]+)", city)
        if m:
            city = m.group(1).strip()
        else:
            # fallback to full line if city can't be isolated
            city = city.strip()

    agent_name = try_selectors(agent_selectors)
    brokerage = try_selectors(brokerage_selectors)

    # email: rarely present - attempt regex on page visible text and mailto links
    email = None
    try:
        mailto = await page.query_selector("a[href^='mailto:']")
        if mailto:
            href = await mailto.get_attribute("href")
            if href and "mailto:" in href:
                email = href.split("mailto:")[1].split("?")[0]
    except Exception:
        pass

    if not email:
        # regex fallback on visible page HTML/text
        emails_found = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
        if emails_found:
            # pick the first reasonable one
            email = emails_found[0]

    # last sale / last sold date: search for common phrases
    last_sale = None
    sold_matches = re.search(r"Last sold[\s:]*([A-Za-z0-9, ]+)", text, re.IGNORECASE)
    if sold_matches:
        last_sale = sold_matches.group(1).strip()
    else:
        sold_date = re.search(r"Sold on[:\s]*([A-Za-z0-9, ]+)", text, re.IGNORECASE)
        if sold_date:
            last_sale = sold_date.group(1).strip()

    # ensure we return simple scalars
    return {
        "source": "zillow",
        "listing_url": listing_url,
        "name": agent_name,
        "email": email,
        "brokerage": brokerage,
        "city": city,
        "last_sale": last_sale,
    }


async def run_scrape(search_urls: List[str],
                     max_listings_per_search: int = 10,
                     max_listings_total: int = 30,
                     headless: bool = True,
                     delay_min: float = DEFAULT_MIN_DELAY,
                     delay_max: float = DEFAULT_MAX_DELAY,
                     proxy: Optional[str] = None) -> List[Dict]:
    """
    Master runner: takes seed search URLs (Zillow search pages), visits them,
    collects a bounded set of listing links, visits each listing, extracts agents,
    and returns a list of lead dicts.
    """
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless,
                                          args=["--no-sandbox", "--disable-dev-shm-usage"])
        context_args = {}
        if proxy:
            context_args["proxy"] = {"server": proxy}

        context = await browser.new_context(**context_args,
                                            user_agent=random.choice(USER_AGENTS))
        page = await context.new_page()

        listings_seen = set()

        for search_url in search_urls:
            # set a randomized UA per search page context if desired
            await page.set_extra_http_headers({"accept-language": "en-US,en;q=0.9"})
            # rotate UA per-navigation
            await context.set_extra_http_headers({})
            await page.evaluate("() => {}")  # noop to ensure context ready

            listing_links = await collect_listing_links_from_search(page, search_url, max_links=max_listings_per_search)
            for link in listing_links:
                if len(listings_seen) >= max_listings_total:
                    break
                if link in listings_seen:
                    continue
                listings_seen.add(link)

                # randomize UA on per-listing basis (set user agent via new context is heavy; instead set header)
                await page.set_extra_http_headers({"user-agent": random.choice(USER_AGENTS)})
                lead = await extract_agent_info_from_listing(page, link)
                results.append(lead)

                # polite random delay
                await random_delay(delay_min, delay_max)

            if len(listings_seen) >= max_listings_total:
                break

        await context.close()
        await browser.close()

    return results
