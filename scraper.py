# scraper.py
from playwright.sync_api import sync_playwright
import time

def scrape_real_estate_agents(limit=10):
    results = []
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()

        # Example: Replace with real estate directory URL
        page.goto("https://www.example-realestate.com/agents")

        # Wait for JS to load
        time.sleep(2)

        agents = page.query_selector_all(".agent-card")[:limit]
        for agent in agents:
            name = agent.query_selector(".name").inner_text()
            email = agent.query_selector(".email").inner_text()
            city = agent.query_selector(".city").inner_text()
            brokerage = agent.query_selector(".brokerage").inner_text()
            last_sale = agent.query_selector(".last-sale").inner_text()

            results.append({
                "name": name,
                "email": email,
                "city": city,
                "brokerage": brokerage,
                "last_sale": last_sale
            })

        browser.close()
    return results
