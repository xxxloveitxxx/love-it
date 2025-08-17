# scraper/zillow_scraper.py — add this at the very top of the file

from typing import Dict, List, Optional
import re
import urllib.parse
import asyncio
import random
import time

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError



async def _set_anti_bot_headers(page: Page):
    # realistic UA + headers to reduce bot fingerprinting
    await page.set_extra_http_headers({
        "accept-language": "en-US,en;q=0.9",
    })
    await page.evaluate(
        """() => {
            // override some navigator properties in-page (best-effort)
            try {
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            } catch (e) {}
        }"""
    )

async def _extract_listing_data(page, url: str, debug: bool=False) -> Dict:
    """Best-effort extraction of agent/name/city/brokerage/last_sale from a listing page.
       Also tries to follow an agent/profile link if present to extract email/brokerage.
    """
    name = None
    city = None
    brokerage = None
    email = None
    last_sale = None

    try:
        await page.wait_for_timeout(500)
        # try address/title selectors
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

        # try agent name on listing page
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

        # try brokerage on listing page
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

        # best-effort last sale detection
        sold_keywords = ["Last sold", "Sold for", "Last sold for", "Sold on"]
        page_text = await page.content()
        for kw in sold_keywords:
            if kw in page_text:
                try:
                    handles = await page.query_selector_all(f"text={kw}")
                    if handles:
                        h = handles[0]
                        parent = await h.evaluate_handle("node => node.parentElement || node")
                        last_sale_text = await (await parent.get_property("innerText")).json_value()
                        last_sale = last_sale_text.strip()
                    else:
                        idx = page_text.find(kw)
                        snippet = page_text[max(0, idx-120): idx+200]
                        last_sale = " ".join(snippet.split())[:400]
                    if debug and last_sale:
                        print(f"[debug] last_sale near '{kw}': {last_sale[:120]}")
                    break
                except Exception:
                    continue

        # if listing page contains a mailto link, capture it right away
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

        # If no email found, try to locate an agent/profile link on the listing
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
                # patterns where Zillow exposes an agent profile
                if "zillow.com/profile" in href_l or "/agent/" in href_l or "/profile/" in href_l or "/agents/" in href_l:
                    agent_link = href
                    break
                # sometimes the agent anchor text includes 'Agent' and the href is relative
                txt = (await a.inner_text()) or ""
                if "agent" in txt.lower() and href_l and href_l.startswith("/"):
                    agent_link = href
                    break

            if agent_link:
                agent_link = _normalize_url(agent_link)
                if debug:
                    print("[debug] following agent link:", agent_link)
                try:
                    # open a new page for agent profile to avoid changing the listing page state
                    context = page.context
                    agent_page = await context.new_page()
                    try:
                        await agent_page.goto(agent_link, wait_until="networkidle", timeout=20000)
                        await agent_page.wait_for_timeout(800)
                        content = await agent_page.content()

                        # try to find mailto on agent profile
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

                        # try plain-text email via regex
                        if not email and "@" in content:
                            import re
                            m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", content)
                            if m:
                                email = m.group(0)
                                if debug:
                                    print(f"[debug] agent profile possible email: {email}")

                        # try to extract brokerage/company on agent page
                        for sel in [".brokerage", ".company", ".agent-company", ".agent-brokerage", '.agent-company-name']:
                            try:
                                el = await agent_page.query_selector(sel)
                                if el:
                                    txt = (await el.inner_text()).strip()
                                    if txt:
                                        brokerage = brokerage or txt
                                        if debug:
                                            print(f"[debug] brokerage from agent page ({sel}): {txt}")
                                        break
                            except Exception:
                                continue

                        # try agent name on agent profile if missing
                        if not name:
                            for sel in ["h1", ".agent-name", ".profile-name", "h1.agent-title"]:
                                try:
                                    el = await agent_page.query_selector(sel)
                                    if el:
                                        txt = (await el.inner_text()).strip()
                                        if txt:
                                            name = txt
                                            if debug:
                                                print(f"[debug] agent name from profile ({sel}): {txt}")
                                            break
                                except Exception:
                                    continue

                    finally:
                        try:
                            await agent_page.close()
                        except Exception:
                            pass
                except Exception as e:
                    if debug:
                        print("[debug] agent profile follow error:", e)

        # fallback: scan listing content for obvious emails
        if not email:
            page_text = await page.content()
            if "@" in page_text:
                import re
                m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", page_text)
                if m:
                    email = m.group(0)
                    if debug:
                        print(f"[debug] found email in listing text: {email}")

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
# scraper/zillow_scraper.py  (append this below your _extract_listing_data)

from typing import Dict, List, Optional
import random
import asyncio
from playwright.async_api import async_playwright

DEFAULT_SEEDS = [
    "https://www.zillow.com/homes/for_sale/San-Francisco-CA_rb/",
    "https://www.zillow.com/homes/for_sale/Los-Angeles-CA_rb/",
]

async def _gather_listing_urls_from_search(page, search_url: str, max_links: int = 20, debug: bool=False) -> List[str]:
    """Open a search page and return a deduped list of listing URLs (homedetails etc.)."""
    try:
        await page.goto(search_url, wait_until="networkidle", timeout=30000)
    except Exception:
        try:
            await page.goto(search_url, wait_until="load", timeout=30000)
        except Exception:
            if debug: print("[debug] failed to open search url:", search_url)
            return []

    # small wait so dynamic content has time to load
    await page.wait_for_timeout(1000 + random.randint(0, 1000))

    anchors = await page.query_selector_all("a")
    found = []
    for a in anchors:
        try:
            href = await a.get_attribute("href")
        except Exception:
            href = None
        if not href:
            continue
        href = href.split("#")[0]
        lower = href.lower()
        # Zillow listings commonly use /homedetails/ in the path; sometimes /b/ or /homedetails
        if "/homedetails/" in lower or "/b/" in lower and "homedetails" not in lower:
            url = _normalize_url(href)
            if url not in found:
                found.append(url)
        # also include explicit /profile/ or /agent/ links? we only want listing pages here
        if len(found) >= max_links:
            break
    if debug: print(f"[debug] found {len(found)} listing urls on {search_url}")
    return found

async def run_scrape(search_urls: List[str]=None, max_per_seed:int=6, max_listings:int=20, debug: bool=False) -> List[dict]:
    """
    Main entrypoint expected by scripts/run_scraper.py.
    - search_urls: list of Zillow search pages to seed from. If None, uses DEFAULT_SEEDS.
    - max_per_seed: how many listing pages to scrape per seed (keeps volume low).
    - max_listings: global cap (safety).
    """
    if search_urls is None:
        search_urls = DEFAULT_SEEDS

    results = []
    visited = set()
    sem = asyncio.Semaphore(3)  # concurrency limit for scraping listing pages

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )

        # single page to collect listing urls
        base_page = await context.new_page()

        try:
            listing_candidates = []
            for seed in search_urls:
                try:
                    urls = await _gather_listing_urls_from_search(base_page, seed, max_links=max_per_seed*3, debug=debug)
                    # limit per seed
                    listing_candidates.extend(urls[:max_per_seed])
                except Exception as e:
                    if debug: print("[debug] error gathering from seed", seed, ":", e)
                # random pause between seeds to reduce fingerprint
                await base_page.wait_for_timeout(800 + random.randint(0,800))

            # dedupe & limit globally
            deduped = []
            for u in listing_candidates:
                if u not in visited:
                    visited.add(u)
                    deduped.append(u)
                if len(deduped) >= max_listings:
                    break

            if debug: print(f"[debug] will scrape {len(deduped)} listings")

            async def _scrape_one(url):
                async with sem:
                    page = await context.new_page()
                    try:
                        await page.goto(url, wait_until="networkidle", timeout=25000)
                    except Exception:
                        try:
                            await page.goto(url, wait_until="load", timeout=25000)
                        except Exception as e:
                            if debug: print("[debug] failed to open listing", url, e)
                            await page.close()
                            return None

                    # small random delay to mimic human browsing
                    await page.wait_for_timeout(600 + random.randint(0, 800))
                    try:
                        data = await _extract_listing_data(page, url, debug=debug)
                    except Exception as e:
                        if debug: print("[debug] extract failed for", url, e)
                        data = None
                    try:
                        await page.close()
                    except Exception:
                        pass
                    return data

            tasks = [asyncio.create_task(_scrape_one(u)) for u in deduped]
            completed = await asyncio.gather(*tasks)
            for item in completed:
                if item:
                    results.append(item)

        finally:
            try:
                await base_page.close()
            except Exception:
                pass
            await context.close()
            await browser.close()

    return results
