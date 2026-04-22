"""
Company Website Grand Openings / Coming Soon Scraper
=====================================================
Scrapers:
  • ALDI           — https://www.aldi.us/grand-openings (Selenium)
  • Dogtopia       — REST API JSON endpoint (requests)
  • Burlington     — https://www.burlington.com/grand-openings (Selenium)
  • Five Below     — https://locations.fivebelow.com/coming-soon/index.html (requests)
  • HomeGoods      — https://www.homegoods.com/grand-openings (Selenium)
  • Homesense      — https://us.homesense.com/grand-openings (Selenium)
  • Jersey Mike's  — https://www.jerseymikes.com/locations/coming-soon (requests)
  • Chick-fil-A    — https://www.chick-fil-a.com/press-room/openings/list-view (requests)
  • LongHorn       — https://www.longhornsteakhouse.com/locations/new-locations (requests)

Output:
  docs/company_website_latest.json
  {
    "last_updated": "April 17, 2026 08:33 UTC",
    "data": [
      {
        "company": "ALDI",
        "address": "14600 Palm Beach Blvd., Fort Myers, FL 33905",
        "opening_date": "April 19, 2026",
        "link": "https://www.aldi.us/stores/..."
      }, ...
    ]
  }
"""

import re
import time
import json
import os
import requests
import pandas as pd
from datetime import date, datetime, timezone
from bs4 import BeautifulSoup, NavigableString, Tag
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ── Shared helpers ────────────────────────────────────────────────────────────

DATE_RE = re.compile(
    r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?"
    r"|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    r"\s+\d{1,2},?\s+\d{4}\b"
    r"|\b\d{1,2}[/\-]\d{1,2}[/\-]\d{4}\b",
    re.IGNORECASE,
)


def extract_date(text: str) -> str:
    m = DATE_RE.search(text or "")
    return m.group(0).strip() if m else ""



def make_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


# ── ALDI scraper ──────────────────────────────────────────────────────────────

ALDI_OPENINGS_URL = "https://www.aldi.us/grand-openings"
ALDI_BASE_URL = "https://www.aldi.us"
ALDI_STORE_LINK_RE = re.compile(r"/stores/l/[^/]+/[^/]+/[^/]+/[a-z0-9]+", re.IGNORECASE)


def _aldi_opening_date(driver: webdriver.Chrome, url: str) -> str:
    try:
        driver.get(url)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")

        for sel in ["[class*='grand-opening']", "[class*='grandOpening']",
                    "[class*='opening-date']", "[class*='openingDate']",
                    "[class*='store-opening']"]:
            tag = soup.select_one(sel)
            if tag:
                d = extract_date(tag.get_text(" ", strip=True))
                if d:
                    return d

        for tag in soup.find_all(string=re.compile(r"grand\s+opening", re.I)):
            d = extract_date(tag.parent.get_text(" ", strip=True))
            if d:
                return d

        full_text = soup.get_text(" ", strip=True)
        m = re.search(
            r"opening[^.]{0,80}?"
            r"(\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May"
            r"|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?"
            r"|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},?\s+\d{4}\b"
            r"|\b\d{1,2}[/\-]\d{1,2}[/\-]\d{4}\b)",
            full_text, re.IGNORECASE,
        )
        if m:
            return m.group(1).strip()
        return extract_date(full_text)
    except Exception as e:
        print(f"  ALDI date fetch error {url}: {e}")
        return ""


def scrape_aldi(driver: webdriver.Chrome) -> list[dict]:
    print(f"[ALDI] Loading {ALDI_OPENINGS_URL}")
    driver.get(ALDI_OPENINGS_URL)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    seen, stores = set(), []
    for a in soup.find_all("a", href=ALDI_STORE_LINK_RE):
        href = a["href"].strip()
        full_link = href if href.startswith("http") else ALDI_BASE_URL + href
        if full_link in seen:
            continue
        seen.add(full_link)

        address_text = a.get_text(separator=" ", strip=True)
        m = re.match(r"^(.*?),\s*(.*?),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)$", address_text)
        if m:
            street, city, state, zip_code = m.groups()
            full_address = f"{street.strip(', ')}, {city.strip()}, {state.strip()} {zip_code.strip()}"
        else:
            full_address = address_text

        stores.append({"full_link": full_link, "address": full_address})

    print(f"[ALDI] Found {len(stores)} store(s). Fetching opening dates…")
    results = []
    for i, store in enumerate(stores, 1):
        print(f"  [{i}/{len(stores)}] {store['full_link']}")
        opening_date = _aldi_opening_date(driver, store["full_link"])
        print(f"         → {opening_date or '(not found)'}")
        results.append({
            "company":      "ALDI",
            "address":      store["address"],
            "opening_date": opening_date.strip(),
            "link":         store["full_link"],
        })
        time.sleep(1.0)
    return results


# ── Dogtopia scraper ──────────────────────────────────────────────────────────

DOGTOPIA_API = "https://www.dogtopia.com/wp-json/store-locator/v1/locations.json"


def scrape_dogtopia() -> list[dict]:
    print(f"[Dogtopia] Fetching {DOGTOPIA_API}")
    try:
        response = requests.get(DOGTOPIA_API, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"[Dogtopia] Error: {e}")
        return []

    results = []
    for loc in data:
        if not loc.get("opening_soon"):
            continue
        try:
            addr_info  = loc["store_info"]["location_address_info"][0]
            hours_info = loc["store_info"]["location_hours_info"][0]

            street   = addr_info.get("location_street_address", "") or ""
            city     = addr_info.get("location_city", "") or ""
            state    = addr_info.get("location_state_prov", "") or ""
            zipcode  = addr_info.get("location_zip_postal", "") or ""
            parts    = [p for p in [street, city, state, zipcode] if p]
            address  = ", ".join(parts)

            opening_date = hours_info.get("coming_soon_header_text", "") or ""
            link         = loc.get("link", "") or ""

            results.append({
                "company":      "Dogtopia",
                "address":      address,
                "opening_date": opening_date.strip(),
                "link":         link,
            })
        except (KeyError, IndexError, TypeError) as e:
            print(f"  [Dogtopia] Skipping malformed record: {e}")

    print(f"[Dogtopia] Found {len(results)} coming-soon location(s).")
    return results


# ── Burlington scraper ────────────────────────────────────────────────────────

BURLINGTON_URL = "https://www.burlington.com/grand-openings"

US_STATES_RE = re.compile(
    r"^(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut"
    r"|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas"
    r"|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota"
    r"|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey"
    r"|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon"
    r"|Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas"
    r"|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming)$"
)

# Matches Burlington entry format: "05/01 - Fayetteville (#1829) 3835 North Mall Ave Ste 2"
# Group 1: opening date  (05/01)
# Group 2: store name    (Fayetteville (#1829))
# Group 3: address       (3835 North Mall Ave Ste 2)
BURLINGTON_ENTRY_RE = re.compile(
    r'^(\d{2}/\d{2})\s*[-–]\s*(.+?\(#\d+\))\s+(.+)$'
)


def _burlington_expand_accordions(driver: webdriver.Chrome) -> int:
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.TAG_NAME, "button"))
    )
    buttons = driver.find_elements(By.CSS_SELECTOR, "button[aria-expanded='false']")
    if not buttons:
        buttons = [b for b in driver.find_elements(By.TAG_NAME, "button")
                   if b.text.strip() in ("+", "expand", "Show")]
    if not buttons:
        buttons = driver.find_elements(By.XPATH, "//*[normalize-space(text())='+']")

    count = 0
    for btn in buttons:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.2)
            driver.execute_script("arguments[0].click();", btn)
            count += 1
            time.sleep(0.6)
        except Exception:
            continue
    time.sleep(3)
    return count


def _burlington_find_container(state_tag):
    node = state_tag
    for _ in range(6):
        parent = node.parent
        if parent is None or parent.name in ("body", "html", "[document]"):
            break
        if parent.find(["a", "li"]):
            return parent
        for sibling in node.next_siblings:
            if not hasattr(sibling, "find"):
                continue
            if sibling.find(["a", "li"]):
                return sibling
        node = parent
    return state_tag.parent


def _burlington_entries(container):
    entries = []
    candidates = container.find_all("a") or container.find_all("li")
    for el in candidates:
        text = el.get_text(" ", strip=True)
        if len(text) < 3:
            continue
        row = el.find_parent(["li", "div", "article", "p"]) or el.parent
        row_text = row.get_text(" ", strip=True) if row else text
        entries.append((text, row_text))
    return entries


def _burlington_parse(soup: BeautifulSoup) -> list[dict]:
    section = (
        soup.find(id="UpcomingGrandOpenings")
        or soup.find(id=re.compile(r"grand.?opening", re.I))
        or soup.find(attrs={"class": re.compile(r"grand.?opening", re.I)})
        or soup.body
    )
    state_tags = section.find_all(
        lambda t: t.name in ("h2", "h3", "h4", "strong", "b", "p", "span", "div")
        and US_STATES_RE.match(t.get_text(strip=True))
    )
    print(f"[Burlington] Found {len(state_tags)} state heading(s).")

    stores, seen = [], set()
    for state_tag in state_tags:
        state_name = state_tag.get_text(strip=True)
        container  = _burlington_find_container(state_tag)
        entries    = _burlington_entries(container)
        print(f"  [{state_name}] {len(entries)} entries.")

        for text, row_text in entries:
            key = (state_name, text.lower())
            if key in seen:
                continue
            seen.add(key)

            m = BURLINGTON_ENTRY_RE.match(text.strip())
            if m:
                opening_date = m.group(1).strip()
                address      = f"{m.group(3).strip()}, {state_name}"
            else:
                if len(text) <= 4:
                    continue
                opening_date = extract_date(text) or extract_date(row_text)
                address      = state_name

            stores.append({
                "address":      address,
                "opening_date": opening_date,
            })
    return stores


def scrape_burlington(driver: webdriver.Chrome) -> list[dict]:
    print(f"[Burlington] Loading {BURLINGTON_URL}")
    driver.get(BURLINGTON_URL)
    time.sleep(8)
    n = _burlington_expand_accordions(driver)
    print(f"[Burlington] Expanded {n} accordion(s).")
    soup = BeautifulSoup(driver.page_source, "html.parser")
    stores = _burlington_parse(soup)

    results = []
    for s in stores:
        results.append({
            "company":      "Burlington",
            "address":      s["address"],
            "opening_date": s["opening_date"].strip(),
            "link":         "",
        })
    print(f"[Burlington] {len(results)} store(s) parsed.")
    return results


# ── Five Below scraper ───────────────────────────────────────────────────────

FIVE_BELOW_INDEX_URL = "https://locations.fivebelow.com/coming-soon/index.html"
FIVE_BELOW_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    )
}


def _five_below_get_json_ld(url: str) -> dict:
    resp = requests.get(url, headers=FIVE_BELOW_HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    script = soup.find("script", type="application/ld+json")
    return json.loads(script.string)


def _five_below_format_address(address: dict) -> str:
    if not address:
        return ""
    parts = [
        address.get("streetAddress", "").strip(),
        address.get("addressLocality", "").strip(),
        address.get("addressRegion", "").strip(),
        address.get("postalCode", "").strip(),
    ]
    return ", ".join(p for p in parts if p)


def scrape_five_below() -> list[dict]:
    print(f"[Five Below] Fetching {FIVE_BELOW_INDEX_URL}")
    try:
        index_data = _five_below_get_json_ld(FIVE_BELOW_INDEX_URL)
        state_urls = [
            item["item"]["url"]
            for item in index_data["@graph"][0]["mainEntity"]["itemListElement"]
        ]
    except Exception as e:
        print(f"[Five Below] Error fetching index: {e}")
        return []

    results = []
    for state_url in state_urls:
        try:
            state_data = _five_below_get_json_ld(state_url)
            city_urls = [
                item["item"]["url"]
                for item in state_data["@graph"][0]["mainEntity"]["itemListElement"]
            ]
        except Exception as e:
            print(f"[Five Below] Error fetching state {state_url}: {e}")
            continue

        for city_url in city_urls:
            try:
                city_data = _five_below_get_json_ld(city_url)
                for entry in city_data["@graph"][0]["mainEntity"]["itemListElement"]:
                    store = entry["item"]
                    results.append({
                        "company":      "Five Below",
                        "address":      _five_below_format_address(store.get("address", {})),
                        "opening_date": "",
                        "link":         store.get("url", "") or "",
                    })
            except Exception as e:
                print(f"[Five Below] Error fetching city {city_url}: {e}")
                continue

    print(f"[Five Below] Found {len(results)} coming-soon location(s).")
    return results


# ── HomeGoods scraper ────────────────────────────────────────────────────────

HOMEGOODS_URL = "https://www.homegoods.com/grand-openings"
HOMEGOODS_BASE_URL = "https://www.homegoods.com"


def scrape_homegoods(driver: webdriver.Chrome) -> list[dict]:
    print(f"[HomeGoods] Loading {HOMEGOODS_URL}")
    driver.get(HOMEGOODS_URL)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    results = []
    for state_li in soup.find_all("li", class_="state-dropdown"):
        state = state_li.find("h4").get_text(strip=True) if state_li.find("h4") else ""

        for store_li in state_li.find_all("li"):
            address_tag  = store_li.find("address")
            opening_tag  = store_li.find("h5")
            link_tag     = store_li.find("a", class_="arrow-link")

            address      = address_tag.get_text(separator=", ", strip=True) if address_tag else ""
            opening_date = opening_tag.get_text(strip=True) if opening_tag else ""
            href         = link_tag["href"] if link_tag and link_tag.get("href") else ""
            link         = (HOMEGOODS_BASE_URL + href) if href.startswith("/") else href

            if not address:
                continue

            results.append({
                "company":      "HomeGoods",
                "address":      f"{address}, {state}" if state else address,
                "opening_date": opening_date.strip(),
                "link":         link,
            })

    print(f"[HomeGoods] {len(results)} store(s) parsed.")
    return results


# ── Homesense scraper ────────────────────────────────────────────────────────

HOMESENSE_URL = "https://us.homesense.com/grand-openings"


def scrape_homesense(driver: webdriver.Chrome) -> list[dict]:
    print(f"[Homesense] Loading {HOMESENSE_URL}")
    driver.get(HOMESENSE_URL)
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "locator-txt"))
        )
    except Exception:
        print("[Homesense] Timed out waiting for cards; trying anyway…")
        time.sleep(5)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    cards = soup.find_all("div", class_="locator-txt")
    print(f"[Homesense] Found {len(cards)} store card(s).")

    results = []
    for card in cards:
        city_tag     = card.find("h2")
        date_tag     = card.find(class_="lime-txt")
        city         = city_tag.get_text(strip=True) if city_tag else ""
        opening_date = date_tag.get_text(strip=True) if date_tag else ""

        address_parts = []
        directions_url = ""
        for p in card.find_all("p"):
            if "lime-txt" in (p.get("class") or []):
                continue
            hit_hours = False
            for elem in p.children:
                if isinstance(elem, Tag):
                    if elem.name == "img":
                        hit_hours = True
                    elif elem.name == "a":
                        href = elem.get("href", "")
                        text = elem.get_text(strip=True)
                        if href.startswith("tel:"):
                            continue
                        elif "maps" in href or "Get Directions" in text:
                            directions_url = href
                elif isinstance(elem, NavigableString) and not hit_hours:
                    text = str(elem).strip().strip(",").strip()
                    if text:
                        address_parts.append(text)

        street  = ", ".join(p for p in address_parts if p)
        address = f"{street}, {city}" if street and city else street or city

        results.append({
            "company":      "Homesense",
            "address":      address,
            "opening_date": extract_date(opening_date),
            "link":         directions_url,
        })

    print(f"[Homesense] {len(results)} store(s) parsed.")
    return results


# ── Jersey Mike's scraper ────────────────────────────────────────────────────

JERSEY_MIKES_BASE_URL = "https://www.jerseymikes.com/locations/coming-soon"

JM_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}

JM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    )
}


def _jm_resolve(data: list, index):
    """Resolve a Vue.js serialised index back to its actual value."""
    if isinstance(index, int) and 0 <= index < len(data):
        return data[index]
    return index


def _jm_parse_payload(raw_data: list) -> list[dict]:
    """Parse store records from the Vue.js JSON payload embedded in the page."""
    stores = []
    try:
        if len(raw_data) <= 6:
            return stores
        for store_idx in raw_data[6]:
            if store_idx >= len(raw_data):
                continue
            store_dict = _jm_resolve(raw_data, store_idx)
            if not isinstance(store_dict, dict):
                continue
            store = {}
            for key in ("id", "number", "name", "openDate"):
                if key in store_dict:
                    store[key] = _jm_resolve(raw_data, store_dict[key])
            if "address" in store_dict:
                addr_raw = _jm_resolve(raw_data, store_dict["address"])
                if isinstance(addr_raw, dict):
                    store["address"] = {
                        k: _jm_resolve(raw_data, addr_raw[k])
                        for k in ("street1", "city", "subdivisionCode", "postalCode")
                        if k in addr_raw
                    }
                else:
                    store["address"] = {}
            stores.append(store)
    except Exception as e:
        print(f"  [Jersey Mike's] Payload parse error: {e}")
    return stores


def scrape_jersey_mikes() -> list[dict]:
    print(f"[Jersey Mike's] Scraping {JERSEY_MIKES_BASE_URL} …")
    results = []

    for state_code, state_name in JM_STATES.items():
        url = f"{JERSEY_MIKES_BASE_URL}/US-{state_code}"
        try:
            resp = requests.get(url, headers=JM_HEADERS, timeout=15)
            resp.raise_for_status()
        except Exception:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        script = soup.find("script", type="application/json")
        if not (script and script.string):
            continue

        try:
            raw_data = json.loads(script.string)
        except Exception:
            continue

        stores = _jm_parse_payload(raw_data)
        for s in stores:
            addr     = s.get("address") or {}
            street   = addr.get("street1", "") or ""
            city     = addr.get("city", "") or ""
            zip_code = addr.get("postalCode", "") or ""
            address_parts = [p for p in [street, city, f"{state_code} {zip_code}".strip()] if p]
            address  = ", ".join(address_parts) if address_parts else state_name

            open_date = s.get("openDate", "") or ""

            results.append({
                "company":      "Jersey Mike's",
                "address":      address,
                "opening_date": extract_date(open_date) or "In Development",
                "link":         url,
            })

        time.sleep(0.5)

    print(f"[Jersey Mike's] Found {len(results)} coming-soon location(s).")
    return results


# ── Chick-fil-A scraper ──────────────────────────────────────────────────────

CHICK_FIL_A_BASE_URL = "https://www.chick-fil-a.com/press-room/openings/list-view"
CHICK_FIL_A_PAGES    = 5
CHICK_FIL_A_HEADERS  = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def _cfa_fetch_page(page: int) -> str:
    url = CHICK_FIL_A_BASE_URL if page == 1 else f"{CHICK_FIL_A_BASE_URL}?query-0-page={page}"
    resp = requests.get(url, headers=CHICK_FIL_A_HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


def _cfa_parse_list(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for h3 in soup.find_all("h3"):
        a = h3.find("a", href=True)
        if not a:
            continue
        href = a["href"]
        if "/press-room/" not in href or "openings" in href:
            continue
        rows.append({"url": href})
    return rows


def _cfa_fetch_article(url: str) -> tuple[str, str]:
    try:
        resp = requests.get(url, headers=CHICK_FIL_A_HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [Chick-fil-A] Could not fetch {url}: {e}")
        return "", ""

    soup = BeautifulSoup(resp.text, "html.parser")
    body_text = " ".join(p.get_text(" ", strip=True) for p in soup.find_all("p"))

    address = ""
    addr_m = re.search(
        r"[Ll]ocated at\s+([\d]+[^,\.]+(?:Ave|Blvd|Dr|Rd|St|Way|Ln|Pkwy|Hwy|"
        r"Pike|Plaza|Circle|Cir|Court|Ct|Trail|Trl|Route|Rt|Square|Sq|"
        r"Drive|Street|Road|Lane|Parkway|Highway)[^,\.]*)",
        body_text,
    )
    if addr_m:
        address = addr_m.group(1).strip()

    opening_date = ""
    date_m = re.search(
        r"(?:begin serving|opening|opens?|open(?:ing)? its doors)[^.]{0,80}"
        r"on\s+(?:\w+day,\s*)?"
        r"((?:January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+\d{1,2},?\s+\d{4})",
        body_text,
        re.IGNORECASE,
    )
    if date_m:
        opening_date = date_m.group(1).strip()
    else:
        h1 = soup.find("h1")
        if h1:
            container = h1.find_parent()
            if container:
                pub_m = re.search(
                    r"((?:January|February|March|April|May|June|July|August|"
                    r"September|October|November|December)\s+\d{1,2},?\s+\d{4})",
                    container.get_text(" ", strip=True),
                )
                if pub_m:
                    opening_date = pub_m.group(1).strip()

    return address, opening_date


def scrape_chick_fil_a() -> list[dict]:
    print(f"[Chick-fil-A] Scraping {CHICK_FIL_A_BASE_URL} ({CHICK_FIL_A_PAGES} pages)…")
    listings = []
    for page in range(1, CHICK_FIL_A_PAGES + 1):
        try:
            html = _cfa_fetch_page(page)
            rows = _cfa_parse_list(html)
            listings.extend(rows)
            print(f"  Page {page}: {len(rows)} record(s) (total: {len(listings)})")
        except Exception as e:
            print(f"  [Chick-fil-A] Page {page} error: {e}")
        time.sleep(0.5)

    results = []
    for i, row in enumerate(listings, 1):
        print(f"  [{i}/{len(listings)}] {row['url']}")
        address, opening_date = _cfa_fetch_article(row["url"])
        results.append({
            "company":      "Chick-fil-A",
            "address":      address,
            "opening_date": opening_date,
            "link":         row["url"],
        })
        time.sleep(0.5)

    print(f"[Chick-fil-A] Found {len(results)} opening(s).")
    return results


# ── LongHorn Steakhouse scraper ──────────────────────────────────────────────

LONGHORN_URL = "https://www.longhornsteakhouse.com/locations/new-locations"
LONGHORN_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    )
}


def scrape_longhorn() -> list[dict]:
    print(f"[LongHorn] Fetching {LONGHORN_URL}")
    try:
        resp = requests.get(LONGHORN_URL, headers=LONGHORN_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"[LongHorn] Error: {e}")
        return []

    soup = BeautifulSoup(resp.content, "html.parser")
    content = soup.find_all("div", class_="newloc row")

    results = []
    for data in content:
        address_div = data.find("div", class_="span7 margtop3")
        if not address_div:
            continue

        parts = address_div.get_text(separator="\n", strip=True).split("\n")
        street     = parts[1] if len(parts) > 1 else ""
        city_state = parts[2] if len(parts) > 2 else ""
        field3     = parts[3] if len(parts) > 3 else ""
        field4     = parts[4] if len(parts) > 4 else ""

        # field3 is a phone number when it starts with '(' or a digit;
        # for "OPENING SPRING 2026" rows there is no phone — field3 is the date.
        is_phone     = field3.startswith("(") or (bool(field3) and field3[0].isdigit())
        opening_date = field4 if is_phone else field3

        address = f"{street}, {city_state}".strip(", ")
        if not address:
            continue

        results.append({
            "company":      "LongHorn Steakhouse",
            "address":      address,
            "opening_date": extract_date(opening_date) or opening_date,
            "link":         LONGHORN_URL,
        })

    print(f"[LongHorn] {len(results)} location(s) parsed.")
    return results


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    all_stores = []

    # ── ALDI ──
    driver = None
    try:
        driver = make_driver()
        all_stores.extend(scrape_aldi(driver))
    except Exception as e:
        print(f"[ALDI] Scraping failed: {e}")
    finally:
        if driver:
            driver.quit()

    # ── Dogtopia ──
    try:
        all_stores.extend(scrape_dogtopia())
    except Exception as e:
        print(f"[Dogtopia] Scraping failed: {e}")

    # ── Burlington ──
    driver = None
    try:
        driver = make_driver()
        all_stores.extend(scrape_burlington(driver))
    except Exception as e:
        print(f"[Burlington] Scraping failed: {e}")
    finally:
        if driver:
            driver.quit()

    # ── Five Below ──
    try:
        all_stores.extend(scrape_five_below())
    except Exception as e:
        print(f"[Five Below] Scraping failed: {e}")

    # ── HomeGoods ──
    driver = None
    try:
        driver = make_driver()
        all_stores.extend(scrape_homegoods(driver))
    except Exception as e:
        print(f"[HomeGoods] Scraping failed: {e}")
    finally:
        if driver:
            driver.quit()

    # ── Homesense ──
    driver = None
    try:
        driver = make_driver()
        all_stores.extend(scrape_homesense(driver))
    except Exception as e:
        print(f"[Homesense] Scraping failed: {e}")
    finally:
        if driver:
            driver.quit()

    # ── Jersey Mike's ──
    try:
        all_stores.extend(scrape_jersey_mikes())
    except Exception as e:
        print(f"[Jersey Mike's] Scraping failed: {e}")

    # ── Chick-fil-A ──
    try:
        all_stores.extend(scrape_chick_fil_a())
    except Exception as e:
        print(f"[Chick-fil-A] Scraping failed: {e}")

    # ── LongHorn Steakhouse ──
    try:
        all_stores.extend(scrape_longhorn())
    except Exception as e:
        print(f"[LongHorn] Scraping failed: {e}")

    print(f"\nTotal records collected: {len(all_stores)}")

    # ── Save JSON ──
    os.makedirs("docs", exist_ok=True)
    output = {
        "last_updated": datetime.now(timezone.utc).strftime("%B %d, %Y %H:%M UTC"),
        "data": all_stores,
    }
    out_path = os.path.join("docs", "company_website_latest.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"Saved → {out_path}")

    # ── Also save Excel for convenience ──
    if all_stores:
        df = pd.DataFrame(all_stores, columns=["company", "address", "opening_date", "link"])
        today_str = date.today().strftime("%B %d, %Y")
        excel_path = f"company_website_openings_{today_str}.xlsx"
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Openings")
            ws = writer.sheets["Openings"]
            for col in ws.columns:
                max_len = max(len(str(cell.value or "")) for cell in col)
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)
        print(f"Saved → {excel_path}")


if __name__ == "__main__":
    main()
