# scraper/zillow_scraper.py
from typing import Dict, List, Optional
import os
import re
import urllib.parse
import asyncio
import random
import time
import json
import sys

from playwright.async_api import (
    async_playwright,
    Page,
    BrowserContext,
    TimeoutError as PWTimeoutError,
)

# -------------------------
# Helpers
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

def get_random_user_agent() -> str:
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    ]
    return random.choice(agents)

def get_random_viewport() -> Dict:
    sizes = [
        {"width": 1366, "height": 768},
        {"width": 1920, "height": 1080},
        {"width": 1536, "height": 864},
        {"width": 1280, "height": 720},
        {"width": 1440, "height": 900},
    ]
    return random.choice(sizes)

def extract_json_ld(page_content: str) -> List[Dict]:
    """Extract structured JSON-LD data from page source"""
    pattern = re.compile(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.DOTALL | re.IGNORECASE)
    matches = pattern.findall(page_content)
    results = []
    for match in matches:
        try:
            clean_match = match.replace('/*<![CDATA[*/', '').replace('/*]]>*/', '')
            data = json.loads(clean_match.strip())
            results.append(data)
        except json.JSONDecodeError:
            # sometimes page includes multiple objects or trailing commas; try best-effort
            try:
                # if it is an array-like text
                data = json.loads("[" + clean_match.strip().rstrip(",") + "]")
                results.extend(data)
            except Exception:
                continue
    return results

# -------------------------
# Playwright helpers
# -------------------------

async def _apply_stealth_on_new_doc(page: Page):
    # small anti-automation tweaks to navigator props
    await page.evaluate_on_new_document(
        """() => {
            try {
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            } catch(e) {}
        }"""
    )

async def _set_page_headers_and_viewport(page: Page, debug: bool = False):
    # Use random-ish UA and viewport to reduce automation fingerprint
    ua = get_random_user_agent()
    viewport = get_random_viewport()
    try:
        await page.set_extra_http_headers({
            "User-Agent": ua,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        })
    except Exception:
        if debug:
            print("[debug] set_extra_http_headers failed")

    try:
        await page.set_viewport_size(viewport)
    except Exception:
        if debug:
            print("[debug] set_viewport_size failed")

    # small random mouse movement
    try:
        await page.mouse.move(random.randint(50, viewport["width"] - 50), random.randint(50, viewport["height"] - 50))
    except Exception:
        pass

async def bypass_captcha(page: Page, debug: bool = False) -> bool:
    """Detect simple captcha/interstitial content and try light handling. Return True if page is OK."""
    content = (await page.content()).lower()
    if "captcha" in content or "verify" in content or "are you a human" in content:
        if debug:
            print("[debug] CAPTCHA / anti-bot content detected in page content.")
        # light attempts to get past ephemeral checks
        try:
            await page.reload(timeout=8000)
            await page.wait_for_timeout(1500)
        except Exception:
            pass
        # if still captcha-like, give up for this seed
        content2 = (await page.content()).lower()
        if "captcha" in content2 or "verify" in content2:
            if debug:
                print("[debug] Aborting collect: captcha detected.")
            return False
    return True

# -------------------------
# Collect listing URLs from a Zillow search page
# -------------------------

async def collect_listing_urls_from_search(
    context: BrowserContext,
    seed_url: str,
    max_listings: int = 20,
    debug: bool = False,
) -> List[str]:
    page = await context.new_page()
    try:
        await _apply_stealth_on_new_doc(page)
        await _set_page_headers_and_viewport(page, debug=debug)

        if debug:
            print(f"[debug] Opening search page: {seed_url}")

        try:
            # go to seed
            await page.goto(seed_url, wait_until="domcontentloaded", timeout=30000)
            # allow resources to settle
            try:
                await page.wait_for_load_state("networkidle", timeout=20000)
            except Exception:
                # some pages don't hit networkidle; ignore
                pass
            await page.wait_for_timeout(random.randint(800, 1800))
        except Exception as e:
            if debug:
                print(f"[debug] Navigation error: {e}")
            return []

        # quick captcha check
        ok = await bypass_captcha(page, debug=debug)
        if not ok:
            return []

        found = set()

        # attempt 1: JSON-LD
        page_content = await page.content()
        json_ld_data = extract_json_ld(page_content)
        if debug:
            print(f"[debug] Found {len(json_ld_data)} JSON-LD objects")
        for data in json_ld_data:
            # some JSON-LD objects are nested; handle common shapes
            try:
                if isinstance(data, dict) and data.get("@type") in ("ListItem", "Offer", "ItemList"):
                    # ItemList may include itemListElement
                    if data.get("url"):
                        url = _normalize_url(data["url"])
                        if "zillow.com/homedetails/" in url or "homedetails" in url:
                            found.add(url)
                    for k in ("itemListElement", "itemlistelement"):
                        if k in data and isinstance(data[k], list):
                            for it in data[k]:
                                u = None
                                if isinstance(it, dict) and it.get("url"):
                                    u = it.get("url")
                                elif isinstance(it, dict) and it.get("item") and isinstance(it.get("item"), dict):
                                    u = it["item"].get("url")
                                if u:
                                    u = _normalize_url(u)
                                    if "zillow.com/homedetails/" in u:
                                        found.add(u)
            except Exception:
                continue

        # attempt 2: window state (site uses a client state object sometimes)
        if not found:
            try:
                state = await page.evaluate("() => (window.__initialState__ || window.appState || window.__REDUX_STATE__ || {})")
                if state:
                    # navigate common nested structures (best-effort)
                    if isinstance(state, dict):
                        # attempt to find URLs inside gdpClientCache or search results
                        def scan_obj(o):
                            urls = []
                            if isinstance(o, dict):
                                for k, v in o.items():
                                    if isinstance(v, str) and ("zillow.com/homedetails" in v or "/homedetails/" in v):
                                        urls.append(_normalize_url(v))
                                    else:
                                        urls.extend(scan_obj(v))
                            elif isinstance(o, list):
                                for item in o:
                                    urls.extend(scan_obj(item))
                            return urls
                        found_urls = scan_obj(state)
                        for u in found_urls:
                            if "zillow.com/homedetails/" in u:
                                found.add(u)
                if debug:
                    print(f"[debug] Found {len(found)} URLs from state")
            except Exception:
                if debug:
                    print("[debug] State extraction error (ignored)")

        # attempt 3: DOM scraping fallback
        if not found:
            if debug:
                print("[debug] Using DOM fallback for URLs")
            try:
                listing_selector = 'a[href*="/homedetails/"], a[data-testid*="property-card-link"], a[class*="property-card"]'
                # initial small scrolls to trigger lazy load
                await page.evaluate("window.scrollTo(0, 600)")
                await page.wait_for_timeout(700)
                links = await page.query_selector_all(listing_selector)
                for link in links:
                    try:
                        href = await link.get_attribute("href")
                    except Exception:
                        href = None
                    if href:
                        normalized = _normalize_url(href)
                        if "zillow.com/homedetails/" in normalized:
                            found.add(normalized)
                            if len(found) >= max_listings:
                                break
            except Exception as e:
                if debug:
                    print(f"[debug] DOM scraping error: {e}")

        # attempt 4: incremental scroll to find more
        if len(found) < max_listings:
            try:
                for _ in range(4):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(1200 + random.randint(0, 1000))
                    listing_selector = 'a[href*="/homedetails/"], a[data-testid*="property-card-link"], a[class*="property-card"]'
                    links = await page.query_selector_all(listing_selector)
                    for link in links:
                        try:
                            href = await link.get_attribute("href")
                        except Exception:
                            href = None
                        if href:
                            normalized = _normalize_url(href)
                            if "zillow.com/homedetails/" in normalized:
                                found.add(normalized)
                                if len(found) >= max_listings:
                                    break
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
# Extract data from individual listing page
# -------------------------

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
            print(f"[debug] Found {len(json_ld_data)} JSON-LD objects on listing")

        for data in json_ld_data:
            try:
                t = data.get("@type") if isinstance(data, dict) else None
                if t == "RealEstateAgent":
                    name = name or data.get("name")
                    email = email or data.get("email")
                    if "affiliation" in data and isinstance(data["affiliation"], dict):
                        brokerage = brokerage or data["affiliation"].get("name")
                if t == "Offer" and "price" in data:
                    last_sale = f"Price: {data.get('price')} {data.get('priceCurrency','')}"
                if isinstance(data, dict) and "address" in data and isinstance(data["address"], dict):
                    city = city or data["address"].get("addressLocality")
            except Exception:
                continue

        # fallback DOM selectors
        if not name:
            for sel in ["a.ds-agent-name", ".ds-listing-agent-name", '[data-testid="listing-agent-name"]', ".listing-agent-name"]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        text = (await el.inner_text()).strip()
                        if text:
                            name = text
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
                            break
                except Exception:
                    continue

        # last sale detection via in-page text
        if not last_sale:
            sold_keywords = ["Last sold", "Sold for", "Last sold for", "Sold on"]
            lower_content = page_content.lower()
            for kw in sold_keywords:
                if kw.lower() in lower_content:
                    idx = lower_content.find(kw.lower())
                    snippet = page_content[max(0, idx - 120): idx + 200]
                    last_sale = " ".join(snippet.split())[:400]
                    break

        # email extraction: mailto first
        try:
            mail_el = await page.query_selector('a[href^="mailto:"]')
            if mail_el:
                href = (await mail_el.get_attribute("href")) or ""
                if href.startswith("mailto:"):
                    email = href.split("mailto:")[1].split("?")[0]
        except Exception:
            pass

        # follow agent profile if no email
        if not email:
            try:
                anchors = await page.query_selector_all("a")
                agent_link = None
                for a in anchors:
                    try:
                        href = await a.get_attribute("href")
                        txt = (await a.inner_text()) or ""
                    except Exception:
                        href = None
                        txt = ""
                    if not href:
                        continue
                    hl = href.lower()
                    if ("zillow.com/profile" in hl) or ("/agent/" in hl) or ("/profile/" in hl) or ("/agents/" in hl):
                        agent_link = href
                        break
                    if "agent" in txt.lower() and href.startswith("/"):
                        agent_link = href
                        break

                if agent_link:
                    agent_link = _normalize_url(agent_link)
                    if debug:
                        print("[debug] following agent link:", agent_link)
                    context = page.context
                    agent_page = await context.new_page()
                    try:
                        await agent_page.goto(agent_link, wait_until="domcontentloaded", timeout=15000)
                        await agent_page.wait_for_timeout(700)
                        content = await agent_page.content()
                        # json-ld
                        jld = extract_json_ld(content)
                        for d in jld:
                            if isinstance(d, dict) and d.get("@type") == "RealEstateAgent":
                                email = email or d.get("email")
                                if not brokerage and "affiliation" in d and isinstance(d["affiliation"], dict):
                                    brokerage = brokerage or d["affiliation"].get("name")
                        if not email:
                            mail_el = await agent_page.query_selector('a[href^="mailto:"]')
                            if mail_el:
                                href = (await mail_el.get_attribute("href")) or ""
                                if href.startswith("mailto:"):
                                    email = href.split("mailto:")[1].split("?")[0]
                            elif "@" in content:
                                m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", content)
                                if m:
                                    email = m.group(0)
                    except Exception:
                        if debug:
                            print("[debug] agent profile follow error (ignored)")
                    finally:
                        try:
                            await agent_page.close()
                        except Exception:
                            pass
            except Exception:
                pass

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

# -------------------------
# Main runner
# -------------------------

async def run_scrape(
    search_urls: Optional[List[str]] = None,
    max_total: Optional[int] = None,
    max_per_search: Optional[int] = None,
    debug: bool = False,
    **kwargs,
) -> List[Dict]:
    """
    Entrypoint that returns a list of leads.
    Accepts many alias names via kwargs for backwards compatibility.
    """
    # alias handling
    if max_per_search is None:
        for alt in ("max_per_seed", "per_seed", "max_results_per_search"):
            if alt in kwargs and kwargs[alt] is not None:
                max_per_search = int(kwargs[alt])
                break
    if max_total is None:
        for alt in ("max_listings", "max_listings_total", "max_results"):
            if alt in kwargs and kwargs[alt] is not None:
                max_total = int(kwargs[alt])
                break

    # env defaults
    search_urls = search_urls or os.getenv("ZILLOW_SEED_URLS", "")
    if isinstance(search_urls, str):
        if not search_urls:
            seed_defaults = [
                "https://www.zillow.com/homes/for_sale/San-Francisco-CA_rb/",
                "https://www.zillow.com/homes/for_sale/Los-Angeles-CA_rb/",
                "https://www.zillow.com/homes/for_sale/New-York-NY_rb/",
            ]
            search_urls = seed_defaults
        else:
            search_urls = [s.strip() for s in search_urls.split(",") if s.strip()]

    max_per_search = int(max_per_search or os.getenv("MAX_LISTINGS_PER_SEARCH", "6"))
    max_total = int(max_total or os.getenv("MAX_LISTINGS_TOTAL", "12"))

    # runtime options via env
    proxy_server = os.getenv("PROXY")  # e.g. http://user:pass@host:port
    slow_mo_env = int(os.getenv("SLOW_MO", "0"))
    storage_state_path = os.getenv("STORAGE_STATE", "")  # path to storage_state file to reuse cookies
    headless_env = os.getenv("HEADLESS", "true").lower() not in ("0", "false", "no")

    # Force headless in CI or if no DISPLAY available (prevents the X server error)
    if os.getenv("CI") or os.getenv("GITHUB_ACTIONS") or ("DISPLAY" not in os.environ and not headless_env):
        if not headless_env:
            # user asked for headed but an X server is not available (CI). We'll override.
            if debug:
                print("[debug] No DISPLAY or running in CI; forcing headless=True to avoid X server errors.")
        headless_env = True

    leads: List[Dict] = []
    async with async_playwright() as p:
        # construct launch args
        launch_args = [
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--disable-setuid-sandbox",
            "--disable-web-security",
            "--disable-features=IsolateOrigins,site-per-process",
            "--disable-site-isolation-trials",
            "--disable-infobars",
            "--mute-audio",
        ]
        if proxy_server:
            # Playwright accepts proxy via launch args for Chromium: --proxy-server
            launch_args.append(f"--proxy-server={proxy_server}")

        try:
            browser = await p.chromium.launch(
                headless=headless_env,
                slow_mo=slow_mo_env if slow_mo_env > 0 else None,
                args=launch_args,
            )
        except Exception as e:
            # if launch still fails, surface a helpful message
            print("[error] Browser launch failed:", e)
            raise

        # context kwargs; include storage_state if provided
        context_kwargs = dict(
            user_agent=get_random_user_agent(),
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="America/Los_Angeles",
            bypass_csp=True,
        )
        if storage_state_path:
            # only attach if file exists
            if os.path.exists(storage_state_path):
                context_kwargs["storage_state"] = storage_state_path
            else:
                if debug:
                    print(f"[debug] STORAGE_STATE path provided but file not found: {storage_state_path}")

        context = await browser.new_context(**context_kwargs)

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

            # scrape each listing
            for url in collected_urls:
                if len(leads) >= max_total:
                    break
                try:
                    page = await context.new_page()
                    await _apply_stealth_on_new_doc(page)
                    await _set_page_headers_and_viewport(page, debug=debug)
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

                    ok = await bypass_captcha(page, debug=debug)
                    if not ok:
                        if debug:
                            print("[debug] Skipping listing due to captcha/interstitial.")
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
