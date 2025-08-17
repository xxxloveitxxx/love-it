# scraper/zillow_scraper.py
from typing import Dict, List, Optional
import os
import re
import urllib.parse
import asyncio
import random
import time

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


async def _set_anti_bot_headers(page: Page):
    await page.set_extra_http_headers({"accept-language": "en-US,en;q=0.9"})
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
        await page.set_user_agent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        try:
            await page.goto(seed_url, wait_until="domcontentloaded", timeout=30000)
        except PWTimeoutError:
            if debug:
                print("[debug] initial goto timed out; retrying networkidle")
            try:
                await page.goto(seed_url, wait_until="networkidle", timeout=30000)
            except Exception:
                if debug:
                    print("[debug] goto retry failed; continue and try to parse current DOM")

        found = set()
        start = time.time()
        timeout_seconds = 20
        last_count = 0

        while len(found) < max_listings and (time.time() - start) < timeout_seconds:
            try:
                hrefs = await page.eval_on_selector_all("a", "els => els.map(e => e.href)")
            except Exception:
                hrefs = []

            for href in (hrefs or []):
                if not href:
                    continue
                low = href.lower()
                if "zillow.com" in low and ("/homedetails/" in low or "zpid" in low):
                    href_n = href.split("?")[0].rstrip("/")
                    found.add(href_n)

            try:
                await page.evaluate("window.scrollBy(0, window.innerHeight);")
            except Exception:
                pass

            await page.wait_for_timeout(600 + random.randint(0, 800))

            if len(found) == last_count:
                await page.wait_for_timeout(500)
                break
            last_count = len(found)

        results = list(found)
        if debug:
            print(f"[debug] collect_listing_urls_from_search found {len(results)} urls (limit {max_listings})")
        return results[:max_listings]
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
        await page.wait_for_timeout(500)
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
                        snippet = page_text[max(0, idx - 120) : idx + 200]
                        last_sale = " ".join(snippet.split())[:400]
                    if debug and last_sale:
                        print(f"[debug] last_sale near '{kw}': {last_sale[:120]}")
                    break
                except Exception:
                    continue

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
                        await agent_page.goto(agent_link, wait_until="networkidle", timeout=20000)
                        await agent_page.wait_for_timeout(800)
                        content = await agent_page.content()

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

        if not email:
            page_text = await page.content()
            if "@" in page_text:
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
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context()

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
                    await page.set_user_agent(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    )
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
