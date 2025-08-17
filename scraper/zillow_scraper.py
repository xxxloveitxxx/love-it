# scraper/zillow_scraper.py
from typing import Dict, List, Optional
import os
import re
import urllib.parse
import asyncio
import json
import random
import time

from playwright.async_api import async_playwright, Page, BrowserContext, TimeoutError as PWTimeoutError

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
    try:
        await page.set_extra_http_headers({"accept-language": "en-US,en;q=0.9"})
    except Exception:
        pass
    # override navigator.webdriver if possible
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

# parse JSON-LD strings into Python objects safely
def _parse_jsonld_text(text: str):
    try:
        return json.loads(text)
    except Exception:
        # sometimes Zillow dumps multiple JSON objects or invalid JS comments; try to extract first JSON-like object
        m = re.search(r'(\{.+\})', text, flags=re.S)
        if m:
            try:
                return json.loads(m.group(1))
            except Exception:
                pass
    return None

async def _collect_urls_from_jsonld(page: Page, debug: bool=False) -> List[str]:
    """Look through <script type="application/ld+json"> and return listing urls found."""
    results = []
    try:
        scripts = await page.query_selector_all('script[type="application/ld+json"]')
        for s in scripts:
            txt = (await s.inner_text()) or ""
            obj = _parse_jsonld_text(txt)
            if not obj:
                continue
            # obj may be dict or list
            candidates = []
            if isinstance(obj, dict):
                candidates = [obj]
            elif isinstance(obj, list):
                candidates = obj
            else:
                continue

            for item in candidates:
                # many objects will have "url" or "offers" or nested properties
                if not isinstance(item, dict):
                    continue
                u = item.get("url")
                if u:
                    u = _normalize_url(u)
                    results.append(u)
                # sometimes an "offers" dict contains url
                offers = item.get("offers")
                if isinstance(offers, dict):
                    of_u = offers.get("url")
                    if of_u:
                        results.append(_normalize_url(of_u))
                # event / place might contain a url in nested structure
                if "location" in item and isinstance(item["location"], dict):
                    loc = item["location"].get("url")
                    if loc:
                        results.append(_normalize_url(loc))
    except Exception as e:
        if debug:
            print("[debug] jsonld parse error:", e)
    return list(dict.fromkeys(results))  # preserve order, dedupe

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

        try:
            await page.goto(seed_url, wait_until="networkidle", timeout=30000)
        except PWTimeoutError:
            if debug:
                print("[debug] search page goto timeout; retrying domcontentloaded")
            try:
                await page.goto(seed_url, wait_until="domcontentloaded", timeout=30000)
            except Exception:
                if debug:
                    print("[debug] search goto failed entirely, will try to parse whatever loaded")

        found = []
        # method A: anchors with /homedetails or zpid
        try:
            hrefs = await page.eval_on_selector_all("a", "els => els.map(e => e.href).filter(Boolean)")
        except Exception:
            hrefs = []
        for href in hrefs or []:
            if not href:
                continue
            low = href.lower()
            if "zillow.com" in low and ("/homedetails/" in low or "zpid" in low):
                u = href.split("?")[0].rstrip("/")
                if u not in found:
                    found.append(u)

        # method B: JSON-LD structured data
        jsonld_urls = await _collect_urls_from_jsonld(page, debug=debug)
        for u in jsonld_urls:
            if "zillow.com" in u and ("/homedetails/" in u or "zpid" in u):
                u_clean = u.split("?")[0].rstrip("/")
                if u_clean not in found:
                    found.append(u_clean)

        # method C: regex search in the HTML for homedetails URLs (last resort)
        if len(found) < max_listings:
            try:
                content = await page.content()
                matches = re.findall(r"https?://www\.zillow\.com/homedetails/[^\"'\s]+", content)
                for u in matches:
                    u_clean = u.split("?")[0].rstrip("/")
                    if u_clean not in found:
                        found.append(u_clean)
                        if len(found) >= max_listings:
                            break
            except Exception:
                pass

        # sometimes listings are rendered after scroll - do some gentle scrolling and re-check anchors
        if len(found) < max_listings:
            for _ in range(3):
                try:
                    await page.evaluate("window.scrollBy(0, window.innerHeight)")
                    await page.wait_for_timeout(500 + random.randint(0, 800))
                except Exception:
                    pass
                try:
                    hrefs = await page.eval_on_selector_all("a", "els => els.map(e => e.href).filter(Boolean)")
                except Exception:
                    hrefs = []
                for href in hrefs or []:
                    low = href.lower()
                    if "zillow.com" in low and ("/homedetails/" in low or "zpid" in low):
                        u = href.split("?")[0].rstrip("/")
                        if u not in found:
                            found.append(u)
                if len(found) >= max_listings:
                    break

        if debug:
            print(f"[debug] found {len(found)} listing urls on {seed_url} (limit {max_listings})")
        return found[:max_listings]
    finally:
        try:
            await page.close()
        except Exception:
            pass

async def _extract_listing_data(page: Page, url: str, debug: bool=False) -> Dict:
    # try JSON-LD first (most reliable)
    name = None
    city = None
    brokerage = None
    email = None
    last_sale = None
    price = None
    image = None
    lat = None
    lon = None
    try:
        await page.wait_for_timeout(400)
        # goto already done by caller
        # check JSON-LD
        try:
            scripts = await page.query_selector_all('script[type="application/ld+json"]')
            for s in scripts:
                txt = (await s.inner_text()) or ""
                obj = _parse_jsonld_text(txt)
                if not obj:
                    continue
                # if dict, inspect keys
                def process_obj(o):
                    nonlocal name, city, brokerage, email, last_sale, price, image, lat, lon
                    if not isinstance(o, dict):
                        return
                    # address block
                    addr = o.get("address")
                    if isinstance(addr, dict):
                        # addressLocality is city
                        city = city or addr.get("addressLocality") or addr.get("addressRegion")
                        # street may be in address
                        if not name:
                            name = addr.get("streetAddress") or o.get("name")
                    # offers.price or offers.priceCurrency
                    offers = o.get("offers")
                    if isinstance(offers, dict):
                        price = price or offers.get("price") or offers.get("priceCurrency")
                    # performer could be company/agent
                    performer = o.get("performer")
                    if isinstance(performer, (dict, str)):
                        if isinstance(performer, dict):
                            brokerage = brokerage or performer.get("name")
                        elif isinstance(performer, str):
                            brokerage = brokerage or performer
                    # images
                    img = o.get("image")
                    if img and not image:
                        if isinstance(img, list):
                            image = img[0]
                        elif isinstance(img, str):
                            image = img
                    # geo coordinates
                    loc = o.get("location") or o.get("geo")
                    if isinstance(loc, dict):
                        geo = loc.get("geo") or loc
                        if isinstance(geo, dict):
                            lat = lat or geo.get("latitude")
                            lon = lon or geo.get("longitude")
                    # url
                    # try to get email in text blob
                if isinstance(obj, dict):
                    process_obj(obj)
                elif isinstance(obj, list):
                    for sub in obj:
                        process_obj(sub)
        except Exception as e:
            if debug:
                print("[debug] jsonld extraction error:", e)

        # fallback: look for mailto
        try:
            mail_el = await page.query_selector('a[href^="mailto:"]')
            if mail_el:
                try:
                    href = (await mail_el.get_attribute("href")) or ""
                    if href.startswith("mailto:"):
                        email = href.split("mailto:")[1].split("?")[0]
                        if debug:
                            print("[debug] found mailto:", email)
                except Exception:
                    pass
        except Exception:
            pass

        # fallback: regex on page html
        if not email:
            content = await page.content()
            if "@" in content:
                m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", content)
                if m:
                    email = m.group(0)
                    if debug:
                        print("[debug] found email via regex:", email)

        # try address / title / agent name by selectors if missing
        if not name:
            for sel in ["h1", ".ds-address-container", ".zsg-content-header", ".zpid-title", ".ds-home-details-chip"]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        txt = (await el.inner_text()).strip()
                        if txt:
                            name = txt
                            break
                except Exception:
                    continue

        if debug:
            small = {k: v for k, v in (("name", name), ("city", city), ("email", email), ("brokerage", brokerage), ("price", price))}
            print("[debug] extracted (partial):", small)

    except Exception as e:
        if debug:
            print("[debug] extraction exception:", e)

    return {
        "name": name,
        "city": city,
        "email": email,
        "brokerage": brokerage,
        "last_sale": last_sale,
        "price": price,
        "image": image,
        "lat": lat,
        "lon": lon,
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
    # support alternate kw names
    if max_per_search is None:
        for alt in ("max_per_seed", "max_per_search", "max_per_page"):
            if alt in kwargs and kwargs[alt] is not None:
                max_per_search = int(kwargs[alt])
                break
    if max_total is None:
        for alt in ("max_listings", "max_listings_total", "max_total"):
            if alt in kwargs and kwargs[alt] is not None:
                max_total = int(kwargs[alt])
                break

    search_urls = search_urls or os.getenv("ZILLOW_SEED_URLS", "")
    if isinstance(search_urls, str):
        if not search_urls:
            search_urls = [
                "https://www.zillow.com/homes/for_sale/San-Francisco-CA_rb/",
                "https://www.zillow.com/homes/for_sale/Los-Angeles-CA_rb/",
            ]
        else:
            search_urls = [s.strip() for s in search_urls.split(",") if s.strip()]

    max_per_search = int(max_per_search or os.getenv("MAX_LISTINGS_PER_SEARCH", "6"))
    max_total = int(max_total or os.getenv("MAX_LISTINGS_TOTAL", "12"))

    leads: List[Dict] = []
    async with async_playwright() as p:
        UA = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(user_agent=UA, locale="en-US")
        try:
            collected_urls: List[str] = []
            for seed in search_urls:
                if len(collected_urls) >= max_total:
                    break
                try:
                    urls = await collect_listing_urls_from_search(context, seed, max_per_search, debug=debug)
                except Exception as e:
                    if debug:
                        print("[debug] collect error:", e)
                    urls = []
                for u in urls:
                    if len(collected_urls) >= max_total:
                        break
                    if u not in collected_urls:
                        collected_urls.append(u)

            if debug:
                print("[debug] will scrape", len(collected_urls), "listings:", collected_urls[:10])

            for url in collected_urls:
                if len(leads) >= max_total:
                    break
                try:
                    page = await context.new_page()
                    await _set_anti_bot_headers(page)
                    await page.set_viewport_size({"width": 1366, "height": 768})
                    try:
                        await page.goto(url, wait_until="networkidle", timeout=25000)
                    except Exception:
                        try:
                            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                        except Exception:
                            if debug:
                                print("[debug] failed to open listing", url)
                            await page.close()
                            continue

                    lead = await _extract_listing_data(page, url, debug=debug)
                    if lead:
                        leads.append(lead)
                    try:
                        await page.close()
                    except Exception:
                        pass

                    await asyncio.sleep(0.8 + random.random()*0.8)
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
