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


def get_random_user_agent():
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    ]
    return random.choice(agents)


def get_random_viewport():
    sizes = [
        {"width": 1366, "height": 768},
        {"width": 1920, "height": 1080},
        {"width": 1536, "height": 864},
        {"width": 1280, "height": 720},
        {"width": 1440, "height": 900},
    ]
    return random.choice(sizes)


async def _set_stealth_measures(page: Page):
    """Set headers, viewport, and small JS tweaks to reduce automation fingerprinting."""
    user_agent = get_random_user_agent()
    viewport = get_random_viewport()

    # Set headers and viewport before navigation
    try:
        await page.set_extra_http_headers(
            {
                "User-Agent": user_agent,
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            }
        )
    except Exception:
        pass

    try:
        await page.set_viewport_size(viewport)
    except Exception:
        pass

    # inject lightweight anti-detection script
    try:
        await page.add_init_script(
            """() => {
                try {
                    // navigator.webdriver standard tweak
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                } catch(e) {}
                try {
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                } catch(e) {}
                try {
                    Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
                } catch(e) {}
            }"""
        )
    except Exception:
        pass

    # small random pause and mouse movement to appear human
    try:
        await page.mouse.move(
            random.randint(100, max(150, viewport["width"] - 100)),
            random.randint(100, max(150, viewport["height"] - 100)),
        )
        await page.wait_for_timeout(random.randint(150, 500))
    except Exception:
        pass


async def bypass_captcha(page: Page, debug: bool = False) -> bool:
    """
    Best-effort check for captcha / bot interstitial. Returns True if OK to continue.
    This cannot solve advanced captchas — if present, caller should skip or use a proxy/manual solve.
    """
    try:
        content = (await page.content()).lower()
    except Exception:
        content = ""

    if "captcha" in content or "verify" in content or "unusual traffic" in content:
        if debug:
            print("[debug] CAPTCHA / anti-bot content detected in page content.")
        # try mild remedies: wait, reload, small interaction
        try:
            await page.wait_for_timeout(2000)
            await page.reload()
            await page.wait_for_timeout(2000)
            # re-check
            content2 = (await page.content()).lower()
            if "captcha" in content2 or "verify" in content2:
                # still blocked
                return False
            return True
        except Exception:
            return False
    return True


def extract_json_ld(page_content: str) -> List[Dict]:
    """Extract structured JSON-LD objects from page source."""
    pattern = re.compile(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.DOTALL | re.IGNORECASE)
    matches = pattern.findall(page_content or "")
    results: List[Dict] = []
    for match in matches:
        try:
            # strip common wrappers and comments that sometimes wrap the JSON-LD
            clean = match.strip()
            clean = clean.replace("/*<![CDATA[*/", "").replace("/*]]>*/", "")
            # JSON-LD can be an object or an array
            parsed = json.loads(clean)
            if isinstance(parsed, list):
                results.extend(parsed)
            elif isinstance(parsed, dict):
                results.append(parsed)
        except Exception:
            # be permissive - some scripts contain non-json; skip those
            continue
    return results


# -------------------------
# listing URL collector
# -------------------------
async def collect_listing_urls_from_search(
    context: BrowserContext,
    seed_url: str,
    max_listings: int = 20,
    debug: bool = False,
) -> List[str]:
    """
    Robust collector for Zillow search pages.
    Tries JSON-LD, window state, DOM anchors, and scrolling.
    """
    page = await context.new_page()
    try:
        await _set_stealth_measures(page)
        if debug:
            print(f"[debug] Opening search page: {seed_url}")

        # Attempt to use a natural referrer flow
        try:
            # small friendly referrer visit
            try:
                await page.goto("https://www.google.com", wait_until="domcontentloaded", timeout=10000)
                await page.wait_for_timeout(random.randint(400, 1200))
            except Exception:
                pass

            await page.goto(seed_url, wait_until="domcontentloaded", timeout=30000)
            # extra wait for dynamic content
            try:
                await page.wait_for_load_state("networkidle", timeout=20000)
            except Exception:
                # if networkidle fails, continue - sometimes dynamic content loads later
                pass
            await page.wait_for_timeout(800 + random.randint(0, 1200))
        except Exception as e:
            if debug:
                print(f"[debug] Navigation error: {e}")
            return []

        # Check for captcha / interstitial
        ok = await bypass_captcha(page, debug=debug)
        if not ok:
            if debug:
                print("[debug] Aborting collect: captcha detected.")
            return []

        found = set()

        # 1) Try JSON-LD first
        try:
            content = await page.content()
            json_ld = extract_json_ld(content)
            if debug:
                print(f"[debug] JSON-LD objects found: {len(json_ld)}")
            for obj in json_ld:
                # Many JSON-LD snippets won't be the listing list, but some may point to homedetails
                if isinstance(obj, dict):
                    # If the JSON-LD contains offers or urls, prefer those
                    if obj.get("url") and ("zillow.com" in obj.get("url")):
                        u = _normalize_url(obj.get("url"))
                        if "homedetails" in u or "_zpid" in u:
                            found.add(u.split("?")[0].rstrip("/"))
                    # "itemListElement" might contain entries
                    if "itemListElement" in obj and isinstance(obj["itemListElement"], list):
                        for el in obj["itemListElement"]:
                            try:
                                if isinstance(el, dict) and el.get("url"):
                                    u = _normalize_url(el["url"])
                                    if "homedetails" in u or "_zpid" in u:
                                        found.add(u.split("?")[0].rstrip("/"))
                            except Exception:
                                continue
        except Exception:
            pass

        # 2) Try reading from known window state keys (Zillow stores various state objects)
        if len(found) < max_listings:
            try:
                state = await page.evaluate("""() => {
                    try {
                        return window.__INITIAL_STATE__ || window.__initialState__ || window.appState || window.__state || {};
                    } catch(e) { return {}; }
                }""")
                if state and isinstance(state, dict):
                    # try multiple possible keys used by Zillow
                    possible_keys = []
                    for k, v in state.items():
                        possible_keys.append(k)
                    # attempt to navigate common structures
                    # look for urls in nested dicts
                    def collect_from_obj(o):
                        results = set()
                        if isinstance(o, dict):
                            for kk, vv in o.items():
                                if isinstance(vv, str) and ("zillow.com" in vv and ("homedetails" in vv or "_zpid" in vv)):
                                    results.add(_normalize_url(vv))
                                else:
                                    results.update(collect_from_obj(vv))
                        elif isinstance(o, list):
                            for item in o:
                                results.update(collect_from_obj(item))
                        return results

                    state_urls = collect_from_obj(state)
                    for u in state_urls:
                        if "homedetails" in u or "_zpid" in u:
                            found.add(u.split("?")[0].rstrip("/"))
                    if debug:
                        print(f"[debug] Found {len(found)} URLs from window state")
            except Exception:
                if debug:
                    print("[debug] window state extraction failed")

        # 3) DOM anchors fallback
        if len(found) < max_listings:
            listing_selector = 'a[href*="/homedetails/"], a[href*="_zpid"], [data-testid="property-card-link"], a.list-card-link'
            try:
                # Wait a reasonable time for at least one anchor to attach
                try:
                    await page.wait_for_selector(listing_selector, state="attached", timeout=12000)
                except Exception:
                    # proceed anyway; maybe dynamic
                    if debug:
                        print("[debug] wait_for_selector for listing anchor timed out (continuing fallback)")
                # initial scan
                anchors = await page.query_selector_all(listing_selector)
                if debug:
                    print(f"[debug] anchors found by selector: {len(anchors)}")
                for a in anchors:
                    try:
                        href = await a.get_attribute("href")
                        if not href:
                            continue
                        if "zillow.com" not in href and href.startswith("/"):
                            href = _normalize_url(href)
                        if "homedetails" in href or "_zpid" in href:
                            found.add(href.split("?")[0].rstrip("/"))
                            if len(found) >= max_listings:
                                break
                    except Exception:
                        continue
            except Exception as e:
                if debug:
                    print(f"[debug] DOM fallback error: {e}")

        # 4) Scroll to load more and repeat DOM scan if needed
        if len(found) < max_listings:
            try:
                for _ in range(3):
                    await page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
                    await page.wait_for_timeout(1200 + random.randint(0, 800))
                    anchors = await page.query_selector_all('a[href*="/homedetails/"], a[href*="_zpid"]')
                    for a in anchors:
                        try:
                            href = await a.get_attribute("href")
                            if not href:
                                continue
                            if "zillow.com" not in href and href.startswith("/"):
                                href = _normalize_url(href)
                            if "homedetails" in href or "_zpid" in href:
                                found.add(href.split("?")[0].rstrip("/"))
                                if len(found) >= max_listings:
                                    break
                        except Exception:
                            continue
                    if len(found) >= max_listings:
                        break
            except Exception:
                pass

        # 5) final HTML regex scan (last resort)
        if len(found) < max_listings:
            try:
                html = await page.content()
                for m in re.finditer(r"https?://[^\s'\"<>]*zillow\.com[^\s'\"<>]*", html):
                    u = m.group(0)
                    if ("homedetails" in u) or ("_zpid" in u):
                        found.add(u.split("?")[0].rstrip("/"))
                        if len(found) >= max_listings:
                            break
            except Exception:
                pass

        results = list(found)[:max_listings]
        if debug:
            print(f"[debug] collect_listing_urls_from_search found {len(results)} urls (limit {max_listings})")
        return results
    finally:
        try:
            await page.close()
        except Exception:
            pass


# -------------------------
# listing page extractor
# -------------------------
async def _extract_listing_data(page: Page, url: str, debug: bool = False) -> Dict:
    """
    Best-effort extraction of name/city/brokerage/last_sale/email from a listing page.
    Tries structured JSON-LD first, then DOM.
    """
    name = None
    city = None
    brokerage = None
    email = None
    last_sale = None

    try:
        # small wait to allow the page to settle
        try:
            await page.wait_for_timeout(600)
        except Exception:
            pass

        page_content = await page.content()
        json_ld_data = extract_json_ld(page_content)
        if debug:
            print(f"[debug] Found {len(json_ld_data)} JSON-LD objects on listing page")

        # Process JSON-LD objects
        for data in json_ld_data:
            if not isinstance(data, dict):
                continue
            t = data.get("@type", "").lower()
            if t and "realestateagent" in t:
                name = name or data.get("name")
                email = email or data.get("email")
                # affiliation
                aff = data.get("affiliation")
                if isinstance(aff, dict) and aff.get("@type", "").lower().find("broker") >= 0:
                    brokerage = brokerage or aff.get("name")
            # Offer / price info
            if t and ("offer" in t or "listing" in t):
                price = data.get("price") or data.get("offers", {}).get("price")
                if price:
                    last_sale = last_sale or f"Price: {price} {data.get('priceCurrency','')}"
            # Postal address
            addr = data.get("address")
            if isinstance(addr, dict):
                city = city or addr.get("addressLocality") or addr.get("addressRegion")

        # DOM fallbacks
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

        # last sale / sold detection
        if not last_sale:
            sold_keywords = ["Last sold", "Sold for", "Last sold for", "Sold on", "Price:"]
            lower_text = (page_content or "").lower()
            for kw in sold_keywords:
                if kw.lower() in lower_text:
                    try:
                        idx = lower_text.find(kw.lower())
                        snippet = page_content[max(0, idx - 120) : idx + 200]
                        last_sale = " ".join(snippet.split())[:400]
                        if debug:
                            print(f"[debug] last_sale snippet ({kw}): {last_sale[:120]}")
                        break
                    except Exception:
                        continue

        # email capture: mailto first, regex fallback
        try:
            mail_el = await page.query_selector('a[href^="mailto:"]')
            if mail_el:
                href = (await mail_el.get_attribute("href")) or ""
                if href.startswith("mailto:"):
                    email = href.split("mailto:")[1].split("?")[0]
                    if debug:
                        print(f"[debug] found mailto: {email}")
        except Exception:
            pass

        if not email:
            # follow agent profile link if present
            try:
                anchors = await page.query_selector_all("a")
                agent_link = None
                for a in anchors:
                    try:
                        href = (await a.get_attribute("href")) or ""
                    except Exception:
                        href = ""
                    if not href:
                        continue
                    href_l = href.lower()
                    if "zillow.com/profile" in href_l or "/agent/" in href_l or "/profile/" in href_l or "/agents/" in href_l:
                        agent_link = href
                        break
                    txt = ""
                    try:
                        txt = (await a.inner_text()) or ""
                    except Exception:
                        txt = ""
                    if "agent" in txt.lower() and href_l.startswith("/"):
                        agent_link = href
                        break

                if agent_link:
                    agent_link = _normalize_url(agent_link)
                    if debug:
                        print("[debug] following agent link:", agent_link)
                    agent_page = await page.context.new_page()
                    try:
                        await agent_page.goto(agent_link, wait_until="domcontentloaded", timeout=15000)
                        await agent_page.wait_for_timeout(800)
                        content2 = await agent_page.content()

                        # JSON-LD on agent page
                        for j in extract_json_ld(content2):
                            if isinstance(j, dict) and j.get("@type", "").lower().find("realestateagent") >= 0:
                                email = email or j.get("email")
                                if not brokerage and "affiliation" in j and isinstance(j["affiliation"], dict):
                                    brokerage = brokerage or j["affiliation"].get("name")

                        # mailto fallback on agent page
                        try:
                            m_el = await agent_page.query_selector('a[href^="mailto:"]')
                            if m_el:
                                href2 = (await m_el.get_attribute("href")) or ""
                                if href2.startswith("mailto:"):
                                    email = email or href2.split("mailto:")[1].split("?")[0]
                                    if debug:
                                        print(f"[debug] agent page mailto: {email}")
                        except Exception:
                            pass

                        # regex fallback in agent content
                        if not email and "@" in content2:
                            m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", content2)
                            if m:
                                email = m.group(0)
                                if debug:
                                    print(f"[debug] agent page regex email: {email}")
                    finally:
                        try:
                            await agent_page.close()
                        except Exception:
                            pass
            except Exception:
                pass

        # last attempt: find any email in listing page text
        if not email:
            try:
                if "@" in page_content:
                    m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", page_content)
                    if m:
                        email = m.group(0)
                        if debug:
                            print(f"[debug] found email by regex on listing: {email}")
            except Exception:
                pass

    except Exception as e:
        if debug:
            print(f"[debug] extraction exception: {e}")

    return {
        "name": name,
        "city": city,
        "email": email,
        "brokerage": brokerage,
        "last_sale": last_sale,
        "source": "zillow",
        "url": url,
    }


# -------------------------
# main entrypoint
# -------------------------
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
                "--mute-audio",
            ],
        )

        context = await browser.new_context(
            user_agent=get_random_user_agent(),
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="America/Los_Angeles",
            bypass_csp=True,
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
                print(f"[debug] will scrape {len(collected_urls)} listings: {collected_urls}")

            for i, url in enumerate(collected_urls):
                if len(leads) >= max_total:
                    break
                try:
                    page = await context.new_page()
                    await _set_stealth_measures(page)
                    try:
                        await page.goto(url, wait_until="networkidle", timeout=20000)
                    except Exception:
                        try:
                            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                        except Exception:
                            if debug:
                                print(f"[debug] failed to open listing {url}")
                            try:
                                await page.close()
                            except Exception:
                                pass
                            continue

                    lead = await _extract_listing_data(page, url, debug=debug)
                    if lead:
                        leads.append(lead)
                        if debug:
                            print("[debug] scraped lead:", {k: lead.get(k) for k in ("name", "email", "city")})
                    try:
                        await page.close()
                    except Exception:
                        pass

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

