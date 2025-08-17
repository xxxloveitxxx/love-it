# scraper/zillow_scraper.py
from typing import Dict, List, Optional
import os
import re
import urllib.parse
import asyncio
import random
import time
import json

from playwright.async_api import (
    async_playwright,
    Page,
    BrowserContext,
    TimeoutError as PWTimeoutError,
)

# -------------------------
# helpers
# -------------------------
def _normalize_url(href: str) -> str:
    if not href:
        return href
    href = href.strip()
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return urllib.parse.urljoin("https://www.zillow.com", href)
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urllib.parse.urljoin("https://www.zillow.com", href)

def extract_json_ld(page_content: str) -> List[Dict]:
    """Extract structured JSON-LD data from page source"""
    pattern = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.DOTALL)
    matches = pattern.findall(page_content)
    results = []
    for match in matches:
        try:
            clean_match = match.replace('/*<![CDATA[*/', '').replace('/*]]>*/', '')
            data = json.loads(clean_match.strip())
            results.append(data)
        except json.JSONDecodeError:
            continue
    return results

async def _set_anti_bot_headers(page: Page):
    # basic anti-bot headers / navigator tweak
    await page.set_extra_http_headers({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9"
    })
    try:
        await page.evaluate(
            """() => {
                try {
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                } catch(e) {}
            }"""
        )
    except Exception:
        pass


async def collect_listing_urls_from_search(
    context: BrowserContext,
    seed_url: str,
    max_listings: int = 20,
    debug: bool = False,
) -> List[str]:
    page = await context.new_page()
    try:
        await _set_anti_bot_headers(page)
        await page.set_viewport_size({"width": 1366, "height": 768})

        if debug:
            print(f"[debug] Opening search page: {seed_url}")

        try:
            # Use more reliable navigation with multiple wait strategies
            await page.goto(seed_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_load_state("networkidle", timeout=30000)
        except Exception as e:
            if debug:
                print(f"[debug] Navigation error: {e}")
            return []

        found = set()
        start = time.time()
        timeout_seconds = 45  # Increased timeout for slow loading
        
        # Improved selector that works for both mobile and desktop layouts
        listing_selector = 'a[href*="/homedetails/"], a[data-testid*="property-card-link"]'
        
        # Wait for listings using multiple strategies
        try:
            await page.wait_for_selector(listing_selector, state="attached", timeout=20000)
        except Exception as e:
            if debug:
                print(f"[debug] Waiting for selector failed: {e}")
            # Try to find listings through alternative methods
            if "captcha" in (await page.content()).lower():
                print("WARNING: CAPTCHA detected on Zillow")
        
        # 1. Try to extract from JSON-LD in page source
        page_content = await page.content()
        try:
            json_ld_data = extract_json_ld(page_content)
            for data in json_ld_data:
                if data.get("@type") == "ListItem" and data.get("url"):
                    url = _normalize_url(data["url"])
                    if "zillow.com/homedetails/" in url:
                        found.add(url)
            if debug:
                print(f"[debug] Found {len(found)} URLs from JSON-LD")
        except Exception as e:
            if debug:
                print(f"[debug] JSON-LD extraction error: {e}")

        # 2. Try to extract from window state
        if not found:
            try:
                state = await page.evaluate("""() => {
                    return window.__initialState__ || window.appState || {};
                }""")
                
                if state and 'gdpClientCache' in state:
                    for key in state['gdpClientCache']:
                        if key.startswith('ForSaleDoubleScrollFullRenderQuery'):
                            data = state['gdpClientCache'][key].get('json', {})
                            results = data.get('cat1', {}).get('searchResults', [])
                            for result in results:
                                if 'detailUrl' in result:
                                    url = _normalize_url(result['detailUrl'])
                                    found.add(url)
                if debug:
                    print(f"[debug] Found {len(found)} URLs from state")
            except Exception as e:
                if debug:
                    print(f"[debug] State extraction error: {e}")

        # 3. Fallback to DOM scraping
        if not found:
            if debug:
                print("[debug] Using DOM fallback for URLs")
            try:
                # Scroll to trigger lazy loading
                await page.evaluate("window.scrollTo(0, 500)")
                await page.wait_for_timeout(1000)
                
                # Get all possible listing links
                links = await page.query_selector_all(listing_selector)
                for link in links:
                    href = await link.get_attribute("href")
                    if href and "zillow.com/homedetails/" in href:
                        normalized = _normalize_url(href)
                        found.add(normalized)
                        if len(found) >= max_listings:
                            break
            except Exception as e:
                if debug:
                    print(f"[debug] DOM scraping error: {e}")

        # Scroll to load more results if needed
        if len(found) < max_listings:
            if debug:
                print("[debug] Scrolling to load more results")
            try:
                for _ in range(3):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                    await page.wait_for_timeout(1500 + random.randint(0, 500))
                    
                    # Check for new listings
                    new_links = await page.query_selector_all(listing_selector)
                    for link in new_links:
                        href = await link.get_attribute("href")
                        if href and "zillow.com/homedetails/" in href:
                            normalized = _normalize_url(href)
                            found.add(normalized)
                            if len(found) >= max_listings:
                                break
                    if len(found) >= max_listings:
                        break
            except Exception:
                pass

        results = list(found)[:max_listings]
        if debug:
            print(f"[debug] Found {len(results)} listing URLs")
        return results
    finally:
        try:
            await page.close()
        except Exception:
            pass


async def _extract_listing_data(page: Page, url: str, debug: bool = False) -> Dict:
    name = None
    city = None
    brokerage = None
    email = None
    last_sale = None

    try:
        page_content = await page.content()
        json_ld_data = extract_json_ld(page_content)
        
        if debug:
            print(f"[debug] Found {len(json_ld_data)} JSON-LD objects")
        
        # Process JSON-LD data first
        for data in json_ld_data:
            # Extract from RealEstateAgent
            if data.get("@type") == "RealEstateAgent":
                name = data.get("name") or name
                email = data.get("email") or email
                if "affiliation" in data and data["affiliation"].get("@type") == "RealEstateBrokerage":
                    brokerage = data["affiliation"].get("name") or brokerage
            
            # Extract from Offer
            if data.get("@type") == "Offer" and "price" in data:
                price_str = f"${data['price']} {data.get('priceCurrency', '')}"
                last_sale = f"Last sold: {price_str}"
            
            # Extract from PostalAddress
            if "address" in data and data["address"].get("@type") == "PostalAddress":
                city = data["address"].get("addressLocality") or city
        
        # Fallback to DOM scraping if JSON data incomplete
        if not name:
            for sel in ["a.ds-agent-name", ".ds-listing-agent-name", '[data-testid="listing-agent-name"]', ".listing-agent-name"]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        text = (await el.inner_text()).strip()
                        if text:
                            name = text
                            if debug:
                                print(f"[debug] agent name ({sel}): {text}")
                            break
                except Exception:
                    continue

        if not city:
            for sel in ["h1", ".ds-address-container", "h1.ds-address-container", ".zsg-content-header"]:
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

        if not brokerage:
            for sel in [".ds-listing-agent-company", ".agent-company", '[data-testid="brokerage-name"]', ".listing-agent-company"]:
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

        if not last_sale:
            sold_keywords = ["Last sold", "Sold for", "Last sold for", "Sold on"]
            page_text = page_content.lower()
            for kw in sold_keywords:
                kw_lower = kw.lower()
                if kw_lower in page_text:
                    try:
                        idx = page_text.find(kw_lower)
                        snippet = page_content[max(0, idx - 120): idx + 200]
                        last_sale = " ".join(snippet.split())[:400]
                        if debug and last_sale:
                            print(f"[debug] last_sale near '{kw}': {last_sale[:120]}")
                        break
                    except Exception:
                        continue

        # Email extraction
        mail_el = await page.query_selector('a[href^="mailto:"]')
        if mail_el:
            try:
                href = (await mail_el.get_attribute("href")) or ""
                if href.startswith("mailto:"):
                    email = href.split("mailto:")[1].split("?")[0]
                    if debug:
                        print(f"[debug] found mailto: {email}")
            except Exception:
                pass

        if not email:
            agent_link = None
            anchors = await page.query_selector_all("a")
            for a in anchors:
                try:
                    href = await a.get_attribute("href")
                except Exception:
                    href = None
                if not href:
                    continue
                href_l = href.lower()
                if "zillow.com/profile" in href_l or "/agent/" in href_l or "/profile/" in href_l or "/agents/" in href_l:
                    agent_link = href
                    break
                txt = (await a.inner_text()) or ""
                if "agent" in txt.lower() and href_l and href_l.startswith("/"):
                    agent_link = href
                    break

            if agent_link:
                agent_link = _normalize_url(agent_link)
                if debug:
                    print("[debug] following agent link:", agent_link)
                try:
                    context = page.context
                    agent_page = await context.new_page()
                    try:
                        await agent_page.goto(agent_link, wait_until="domcontentloaded", timeout=15000)
                        await agent_page.wait_for_timeout(800)
                        content = await agent_page.content()

                        # Check JSON-LD first
                        json_ld = extract_json_ld(content)
                        for data in json_ld:
                            if data.get("@type") == "RealEstateAgent":
                                email = data.get("email") or email
                                if not brokerage:
                                    if "affiliation" in data and data["affiliation"].get("@type") == "RealEstateBrokerage":
                                        brokerage = data["affiliation"].get("name") or brokerage

                        # Fallback to DOM
                        if not email:
                            mail_el = await agent_page.query_selector('a[href^="mailto:"]')
                            if mail_el:
                                try:
                                    href = (await mail_el.get_attribute("href")) or ""
                                    if href.startswith("mailto:"):
                                        email = href.split("mailto:")[1].split("?")[0]
                                        if debug:
                                            print(f"[debug] agent profile mailto: {email}")
                                except Exception:
                                    pass

                        if not email and "@" in content:
                            m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", content)
                            if m:
                                email = m.group(0)
                                if debug:
                                    print(f"[debug] agent profile possible email: {email}")

                    finally:
                        try:
                            await agent_page.close()
                        except Exception:
                            pass
                except Exception as e:
                    if debug:
                        print("[debug] agent profile follow error:", e)

    except Exception as e:
        if debug:
            print("[debug] extraction exception:", e)

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
    max_total: Optional[int] = None,
    max_per_search: Optional[int] = None,
    debug: bool = False,
    **kwargs,
) -> List[Dict]:
    """
    Robust entrypoint. Accepts multiple alias names via kwargs:
      - max_per_search OR max_per_seed
      - max_total OR max_listings OR max_listings_total
    Returns list of leads (dict).
    """
    # accept alias names
    if max_per_search is None:
        for alt in ("max_per_seed", "max_per_search", "per_seed", "max_results_per_search"):
            if alt in kwargs and kwargs[alt] is not None:
                max_per_search = int(kwargs[alt])
                break

    if max_total is None:
        for alt in ("max_listings", "max_listings_total", "max_total", "max_results"):
            if alt in kwargs and kwargs[alt] is not None:
                max_total = int(kwargs[alt])
                break

    # env defaults / normalization
    search_urls = search_urls or os.getenv("ZILLOW_SEED_URLS", "")
    if isinstance(search_urls, str):
        if not search_urls:
            seed_defaults = [
                "https://www.zillow.com/homes/for_sale/San-Francisco-CA_rb/",
                "https://www.zillow.com/homes/for_sale/Los-Angeles-CA_rb/",
            ]
            search_urls = seed_defaults
        else:
            search_urls = [s.strip() for s in search_urls.split(",") if s.strip()]

    max_per_search = int(max_per_search or os.getenv("MAX_LISTINGS_PER_SEARCH", "6"))
    max_total = int(max_total or os.getenv("MAX_LISTINGS_TOTAL", "12"))

    leads: List[Dict] = []
    async with async_playwright() as p:
        # Use stealthier browser launch options
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-setuid-sandbox",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-site-isolation-trials",
                "--disable-infobars",
                "--disable-breakpad",
                "--disable-client-side-phishing-detection",
                "--disable-default-apps",
                "--mute-audio"
            ]
        )
        
        # Create incognito context with stealth settings
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="America/Los_Angeles",
            permissions=[],
            bypass_csp=True
        )

        try:
            collected_urls: List[str] = []
            for seed in search_urls:
                if len(collected_urls) >= max_total:
                    break
                try:
                    urls = await collect_listing_urls_from_search(context, seed, max_per_search, debug=debug)
                except Exception as e:
                    if debug:
                        print("[debug] collect_listing_urls_from_search error:", e)
                    urls = []
                for u in urls:
                    if len(collected_urls) >= max_total:
                        break
                    if u not in collected_urls:
                        collected_urls.append(u)

            if debug:
                print(f"[debug] will scrape {len(collected_urls)} listings")

            for i, url in enumerate(collected_urls):
                if len(leads) >= max_total:
                    break
                try:
                    page = await context.new_page()
                    await _set_anti_bot_headers(page)
                    await page.set_viewport_size({"width": 1366, "height": 768})
                    try:
                        await page.goto(url, wait_until="networkidle", timeout=20000)
                    except Exception:
                        try:
                            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                        except Exception:
                            if debug:
                                print(f"[debug] failed to open listing {url}")
                            await page.close()
                            continue

                    lead = await _extract_listing_data(page, url, debug=debug)
                    if lead:
                        leads.append(lead)
                        if debug:
                            print("[debug] scraped lead:", {k: v for k, v in lead.items() if k in ("name", "email", "city")})
                    await page.close()

                    await asyncio.sleep(0.8 + random.random() * 0.6)
                except Exception as e:
                    if debug:
                        print("[debug] per-listing error:", e)
                    continue
        finally:
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass

    return leads
