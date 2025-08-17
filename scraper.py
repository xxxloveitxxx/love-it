from playwright.sync_api import sync_playwright
import re
import time

def clean_text(t):
    if not t:
        return None
    return re.sub(r"\s+", " ", t).strip()

def parse_last_sale(text):
    # Examples: "Sold 7 homes in the last 12 months" or "10 recent sales"
    return clean_text(text)

def scrape_zillow_agents(city="new-york-ny", limit=10, delay=1.5):
    """
    Scrape Zillow agent directory for a city slug like 'new-york-ny' or 'los-angeles-ca'.
    Returns list of dicts: name, city, brokerage, last_sale, contact_link
    """
    base_url = f"https://www.zillow.com/{city}/real-estate-agent-reviews/"
    results = []

    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(30000)
        page.goto(base_url, wait_until="domcontentloaded")
        # Let client-side fetch/render finish
        page.wait_for_timeout(2500)

        # Try to accept cookies if banner shows up
        try:
            page.get_by_role("button", name=re.compile("Accept|Agree", re.I)).click(timeout=3000)
        except Exception:
            pass

        # Cards: try robust locators (role + has selectors)
        # Zillow often uses <article> cards; we’ll find links containing "profile" or “agent” in href
        cards = page.locator("a[href*='profile'], a[href*='agent/']").element_handles()
        seen = set()

        for a in cards:
            if len(results) >= limit:
                break
            try:
                href = a.get_attribute("href") or ""
                if not href or href in seen or "/profile/" not in href:
                    continue
                seen.add(href)

                # Open in new tab to read details
                agent_page = browser.new_page()
                if href.startswith("/"):
                    url = "https://www.zillow.com" + href
                elif href.startswith("http"):
                    url = href
                else:
                    url = "https://www.zillow.com/" + href.lstrip("/")
                agent_page.goto(url, wait_until="domcontentloaded")
                agent_page.wait_for_timeout(1800)

                # Extract fields with multiple strategies
                # Name
                name = None
                for sel in [
                    "h1", "[data-testid='profile-name']", "[itemprop='name']"
                ]:
                    try:
                        el = agent_page.locator(sel).first
                        if el and el.count() > 0:
                            t = el.inner_text()
                            if t and len(t.strip()) > 1:
                                name = clean_text(t)
                                break
                    except Exception:
                        pass

                # City / location
                city = None
                for sel in [
                    "[data-testid='profile-location']",
                    "text=/^Serves /i",
                    "text=/^Based in /i"
                ]:
                    try:
                        el = agent_page.locator(sel).first
                        if el and el.count() > 0:
                            city = clean_text(el.inner_text())
                            break
                    except Exception:
                        pass

                # Brokerage
                brokerage = None
                for sel in [
                    "[data-testid='profile-brokerage']",
                    "text=/Brokerage/i",
                    "xpath=//*[contains(text(),'Brokerage')]/following::*[1]"
                ]:
                    try:
                        el = agent_page.locator(sel).first
                        if el and el.count() > 0:
                            brokerage = clean_text(el.inner_text())
                            break
                    except Exception:
                        pass

                # Last sale / recent sales
                last_sale = None
                for sel in [
                    "text=/Sold .* (last|past) .* months/i",
                    "text=/recent sales/i",
                    "[data-testid='transaction-count']",
                ]:
                    try:
                        el = agent_page.locator(sel).first
                        if el and el.count() > 0:
                            last_sale = parse_last_sale(el.inner_text())
                            break
                    except Exception:
                        pass

                # Contact link (keep Zillow contact URL)
                contact_link = None
                try:
                    btn = agent_page.get_by_role("link", name=re.compile("Contact|Message", re.I)).first
                    if btn and btn.count() > 0:
                        href2 = btn.get_attribute("href")
                        if href2:
                            contact_link = href2 if href2.startswith("http") else "https://www.zillow.com" + href2
                except Exception:
                    # fallback to the profile URL itself
                    contact_link = url

                if name:
                    results.append({
                        "name": name,
                        "city": city,
                        "brokerage": brokerage,
                        "last_sale": last_sale,
                        "contact_link": contact_link or url
                    })

                agent_page.close()
                page.wait_for_timeout(int(delay * 1000))
            except Exception:
                try:
                    a.page.close()
                except Exception:
                    pass
                continue

        browser.close()
    return results

if __name__ == "__main__":
    print(scrape_zillow_agents(city="los-angeles-ca", limit=5))
