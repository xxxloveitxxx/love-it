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
