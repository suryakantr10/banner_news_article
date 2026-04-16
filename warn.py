"""
WARN Act Unified Scraper
========================
Add new states by implementing a scrape_<state>() function and
registering it in SCRAPERS at the bottom of the file.

Each scraper must return a pd.DataFrame with at least these columns
(add state-specific columns freely — they'll be preserved):
    state, company, city, notice_date, layoff_date,
    employees_affected, closure_type, notes
"""

import asyncio
import sys
import time
import logging
import requests
import pandas as pd
from datetime import date
from pathlib import Path
from io import StringIO

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("warn_scraper")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

def _run_async(coro):
    """
    Run an async coroutine safely from both plain Python scripts and
    Jupyter notebooks (which already have a running event loop).
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import nest_asyncio
        nest_asyncio.apply()
        return loop.run_until_complete(coro)
    else:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        return asyncio.run(coro)


OUTPUT_COLS = [
    "state", "company", "city", "notice_date", "layoff_date",
    "employees_affected", "closure_type", "notes",
]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _filter_from_2025(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows whose notice_date is before 2025. Rows with unparseable dates are kept."""
    if df.empty or "notice_date" not in df.columns:
        return df
    parsed = pd.to_datetime(df["notice_date"], errors="coerce")
    keep = parsed.isna() | (parsed.dt.year >= 2025)
    return df[keep].reset_index(drop=True)


def _normalise(df: pd.DataFrame, state: str) -> pd.DataFrame:
    """Ensure every required column exists, state is set, and rows are 2025+."""
    df = df.copy()
    df["state"] = state
    for col in OUTPUT_COLS:
        if col not in df.columns:
            df[col] = ""
    extras = [c for c in df.columns if c not in OUTPUT_COLS]
    return _filter_from_2025(df[OUTPUT_COLS + extras])


# ── State scrapers ────────────────────────────────────────────────────────────

async def _scrape_alabama_async() -> list[dict]:
    data = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://workforce.alabama.gov/warn-list/")
        await page.wait_for_selector('tr.fw-warn-list__items[data-year="2026"]', timeout=15000)

        rows = await page.query_selector_all(
            'tr.fw-warn-list__items[data-year="2025"], tr.fw-warn-list__items[data-year="2026"]'
        )
        for row in rows:
            status = await row.query_selector('td[data-label="Closing or Layoff"]')
            if not status:
                continue
            status_text = (await status.inner_text()).strip().lower()
            if status_text != "closure":
                continue

            async def _txt(label):
                el = await row.query_selector(f'td[data-label="{label}"]')
                return (await el.inner_text()).strip() if el else ""

            data.append({
                "company":            await _txt("Company"),
                "city":               await _txt("City"),
                "notice_date":        await _txt("Initial Report Date"),
                "layoff_date":        await _txt("Planned Starting Date"),
                "employees_affected": await _txt("Planned # of Affected Employees"),
                "closure_type":       "closure",
                "notes":              "",
            })
        await browser.close()
    return data


def scrape_alabama() -> pd.DataFrame:
    """
    Scrapes Alabama WARN list (2026, closure events only) via async Playwright.
    URL: https://workforce.alabama.gov/warn-list/
    """
    log.info("Scraping Alabama...")
    data = _run_async(_scrape_alabama_async())
    df = pd.DataFrame(data)
    log.info(f"  Alabama: {len(df)} rows")
    return _normalise(df, "Alabama")


def scrape_alaska() -> pd.DataFrame:
    """
    Scrapes Alaska WARN notices via requests + BeautifulSoup.
    URL: https://jobs.alaska.gov/rr/WARN_notices.htm
    """
    log.info("Scraping Alaska...")
    resp = requests.get("https://jobs.alaska.gov/rr/WARN_notices.htm", headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "html.parser")

    data = []
    for row in soup.find_all("tr")[1:]:   # [1:] skips the single header row
        cols = row.find_all("td")
        if len(cols) < 6:
            continue
        data.append({
            "company":            cols[0].get_text(strip=True),
            "city":               cols[1].get_text(strip=True),
            "notice_date":        cols[2].get_text(strip=True),
            "layoff_date":        cols[3].get_text(strip=True),
            "employees_affected": cols[4].get_text(strip=True),
            "closure_type":       "",
            "notes":              cols[5].get_text(strip=True),
        })

    df = pd.DataFrame(data)
    log.info(f"  Alaska: {len(df)} rows")
    return _normalise(df, "Alaska")


async def _scrape_dc_async() -> list[dict]:
    """
    Parse DC WARN table by scraping <tr>/<td> directly via Playwright,
    bypassing pd.read_html entirely.
    Fetches both current year and previous year pages to get full data.
    """
    current_year = date.today().year
    URLS = [
        f"https://does.dc.gov/page/industry-closings-and-layoffs-warn-notifications-{current_year}",
        f"https://does.dc.gov/page/industry-closings-and-layoffs-warn-notifications-{current_year - 1}",
    ]
    SELECTOR = ".field-name-body table, .field-items table, article table"

    col_map = {
        "organization name": "company",
        "company name":      "company",
        "company":           "company",
        "notice date":       "notice_date",
        "date":              "notice_date",
        "effective layoff date": "layoff_date",
        "layoff date":       "layoff_date",
        "number toemployees affected": "employees_affected",
        "number of employees affected": "employees_affected",
        "employees affected": "employees_affected",
        "# employees":       "employees_affected",
        "affected":          "employees_affected",
        "code type":         "closure_type",
        "type":              "closure_type",
        "location":          "city",
        "city":              "city",
    }

    all_records = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        for url in URLS:
            try:
                page = await browser.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_selector(SELECTOR, timeout=20000)

                rows_data = await page.eval_on_selector(
                    SELECTOR,
                    """tbl => {
                        const rows = Array.from(tbl.querySelectorAll('tr'));
                        return rows.map(r =>
                            Array.from(r.querySelectorAll('th, td'))
                                .map(c => c.innerText.trim())
                        );
                    }"""
                )
                await page.close()

                if not rows_data:
                    continue

                header_idx = next(
                    (i for i, r in enumerate(rows_data) if sum(bool(c) for c in r) >= 3),
                    0
                )
                headers = [h.strip() for h in rows_data[header_idx]]
                data_rows = rows_data[header_idx + 1:]
                norm_headers = [col_map.get(h.lower(), h.lower()) for h in headers]

                for row in data_rows:
                    if not any(row):
                        continue
                    padded = row + [""] * (len(norm_headers) - len(row))
                    all_records.append(dict(zip(norm_headers, padded)))

            except Exception as exc:
                log.warning(f"  DC: failed to scrape {url} — {exc}")

        await browser.close()

    return all_records


def scrape_dc() -> pd.DataFrame:
    """
    Scrapes Washington DC WARN notifications via async Playwright.
    URL: https://does.dc.gov/page/industry-closings-and-layoffs-warn-notifications-2025
    """
    log.info("Scraping DC...")
    records = _run_async(_scrape_dc_async())
    raw = pd.DataFrame(records)
    raw.dropna(how="all", inplace=True)
    raw = raw[raw.apply(lambda r: r.astype(str).str.strip().ne("").any(), axis=1)]

    log.info(f"  DC: {len(raw)} rows")
    return _normalise(raw, "DC")


def scrape_washington() -> pd.DataFrame:
    """
    Scrapes Washington State WARN notices via ASP.NET postback pagination.
    URL: https://fortress.wa.gov/esd/file/WARN/Public/SearchWARN.aspx
    """
    log.info("Scraping Washington...")
    URL = "https://fortress.wa.gov/esd/file/WARN/Public/SearchWARN.aspx"
    session = requests.Session()

    def _get_payload(soup):
        return {i.get("name"): i.get("value", "") for i in soup.select("input") if i.get("name")}

    def _extract_rows(soup):
        rows = []
        for r in soup.select("#ucPSW_gvMain tr"):
            cols = [c.get_text(strip=True) for c in r.select("td")]
            if len(cols) >= 7 and not cols[0].isdigit() and cols[0] not in ("Company", ""):
                rows.append(cols[:7])
        return rows

    res = session.get(URL, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(res.text, "html.parser")
    payload = _get_payload(soup)
    payload["ucPSW$btnSearchCompany"] = "Search"

    res = session.post(URL, data=payload, timeout=15)
    soup = BeautifulSoup(res.text, "html.parser")

    data = []
    page = 1

    while True:
        log.info(f"  Washington page {page}")
        rows = _extract_rows(soup)
        if not rows:
            log.info("  No rows found, stopping.")
            break
        data.extend(rows)

        pager_links = soup.select("#ucPSW_gvMain a")
        page_numbers = [a.get_text(strip=True) for a in pager_links if a.get_text(strip=True).isdigit()]
        next_page = page + 1

        if str(next_page) not in page_numbers:
            log.info(f"  No more pages after page {page}.")
            break

        payload = _get_payload(soup)
        payload["__EVENTTARGET"] = "ucPSW$gvMain"
        payload["__EVENTARGUMENT"] = f"Page${next_page}"
        payload.pop("ucPSW$btnSearchCompany", None)

        res = session.post(URL, data=payload, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
        page += 1

    raw_cols = ["company", "city", "layoff_date", "employees_affected", "closure_type", "type_detail", "notice_date"]
    df = pd.DataFrame(data, columns=raw_cols)
    log.info(f"  Washington: {len(df)} rows")
    return _normalise(df, "Washington")


# ── Washington standalone runner (mirrors original script behaviour) ──────────

def run_washington_standalone(
    output_dir: str = ".",
    max_pages: int | None = None,
) -> pd.DataFrame:
    """
    Reproduces the original standalone Washington script exactly:
    scrapes all pages (or up to max_pages), prints progress, saves a
    dated CSV, and returns the raw DataFrame with the original column names.
    """
    URL = "https://fortress.wa.gov/esd/file/WARN/Public/SearchWARN.aspx"
    session = requests.Session()

    def _get_payload(soup):
        return {i.get("name"): i.get("value", "") for i in soup.select("input") if i.get("name")}

    def _extract_rows(soup):
        rows = []
        for r in soup.select("#ucPSW_gvMain tr"):
            cols = [c.get_text(strip=True) for c in r.select("td")]
            if len(cols) >= 7 and not cols[0].isdigit() and cols[0] not in ("Company", ""):
                rows.append(cols[:7])
        return rows

    res = session.get(URL, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(res.text, "html.parser")
    payload = _get_payload(soup)
    payload["ucPSW$btnSearchCompany"] = "Search"

    res = session.post(URL, data=payload, timeout=15)
    soup = BeautifulSoup(res.text, "html.parser")

    data = []
    page = 1

    while True:
        print(f"Scraping page: {page}")

        if max_pages and page > max_pages:
            print(f"Reached maximum pages ({max_pages}), stopping.")
            break

        rows = _extract_rows(soup)
        if not rows:
            print("No rows found, stopping.")
            break
        data.extend(rows)

        pager_links = soup.select("#ucPSW_gvMain a")
        page_numbers = [a.get_text(strip=True) for a in pager_links if a.get_text(strip=True).isdigit()]
        next_page = page + 1

        if str(next_page) not in page_numbers:
            print(f"No more pages after page {page}.")
            break

        payload = _get_payload(soup)
        payload["__EVENTTARGET"] = "ucPSW$gvMain"
        payload["__EVENTARGUMENT"] = f"Page${next_page}"
        payload.pop("ucPSW$btnSearchCompany", None)

        res = session.post(URL, data=payload, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
        page += 1

    columns = ["Company", "Location", "Layoff Start Date", "# Workers", "Closure/Layoff", "Type", "Received Date"]
    df = pd.DataFrame(data, columns=columns)

    print(df.head())
    print("Total rows:", len(df))

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    csv_path = out / f"warn_layoffs_{date.today().strftime('%Y-%m-%d')}.csv"
    df.to_csv(csv_path, index=False)
    print(f"Saved to {csv_path}")

    return df


def scrape_maryland() -> pd.DataFrame:
    """
    Scrapes Maryland WARN/ESA table via requests + pd.read_html.
    URL: https://labor.maryland.gov/employment/warn.shtml
    """
    log.info("Scraping Maryland...")
    resp = requests.get("https://labor.maryland.gov/employment/warn.shtml", headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.find("table")
    if not table:
        raise RuntimeError("Maryland: no table found on page.")

    raw = pd.read_html(StringIO(str(table)), header=0)[0]
    raw.columns = [c.strip().replace("\n", " ") for c in raw.columns]
    raw.dropna(how="all", inplace=True)
    raw = raw.loc[:, raw.columns.notna()]

    col_map = {
        "Company Name":           "company",
        "Company":                "company",
        "City":                   "city",
        "Location":               "city",
        "Notice Date":            "notice_date",
        "Effective Date":         "layoff_date",
        "Layoff Date":            "layoff_date",
        "Number of Employees":    "employees_affected",
        "Employees Affected":     "employees_affected",
        "Type":                   "closure_type",
        "Notes":                  "notes",
    }
    raw.rename(columns={k: v for k, v in col_map.items() if k in raw.columns}, inplace=True)

    log.info(f"  Maryland: {len(raw)} rows")
    return _normalise(raw, "Maryland")


# ── Vermont ───────────────────────────────────────────────────────────────────

_VT_RESULTS_URL = (
    "https://www.vermontjoblink.com/search/warn_lookups"
    "?commit=Search"
    "&q%5Bemployer_name_cont%5D="
    "&q%5Bmain_contact_contact_info_addresses_full_location_city_matches%5D="
    "&q%5Bnotice_eq%5D=true"
    "&q%5Bnotice_on_gteq%5D="
    "&q%5Bnotice_on_lteq%5D="
    "&q%5Bservice_delivery_area_id_eq%5D="
    "&q%5Bzipcode_code_start%5D="
)

_VT_COLUMNS = ["Employer", "City", "ZIP", "LWIB Area", "Notice Date", "WARN Type"]


def _vt_parse_rows(soup: BeautifulSoup) -> list[dict]:
    import re
    table = soup.find("table", {"id": re.compile(r"^a11y_table_")})
    if not table:
        return []
    tbody = table.find("tbody")
    if not tbody:
        return []

    rows = []
    for tr in tbody.find_all("tr", recursive=False):
        tds = tr.find_all("td", recursive=False)
        if not tds:
            continue
        cells = [re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip() for td in tds]
        while len(cells) < len(_VT_COLUMNS):
            cells.append("")
        row = dict(zip(_VT_COLUMNS, cells[: len(_VT_COLUMNS)]))
        if row["Employer"]:
            rows.append(row)
    return rows


def _vt_find_next_url(soup: BeautifulSoup, current_url: str) -> str | None:
    import re
    from urllib.parse import urljoin

    nav = soup.find("div", class_=re.compile(r"pagination", re.I)) or \
          soup.find("nav", attrs={"aria-label": re.compile(r"Page controls", re.I)})
    if not nav:
        return None

    a = nav.find("a", attrs={"rel": "next"}) or \
        nav.find("a", class_=re.compile(r"next_page", re.I))
    if a and a.get("href"):
        return urljoin(current_url, a["href"])

    for link in nav.find_all("a", href=True):
        if "next" in link.get_text(" ", strip=True).lower():
            return urljoin(current_url, link["href"])
    return None


def scrape_vermont(max_pages: int | None = None) -> pd.DataFrame:
    """
    Scrapes Vermont WARN notices via requests + BeautifulSoup with pagination.
    URL: https://www.vermontjoblink.com/search/warn_lookups/new
    """
    log.info("Scraping Vermont...")
    session = requests.Session()
    session.headers.update(HEADERS)

    resp = session.get(_VT_RESULTS_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    current_url = resp.url

    all_rows: list[dict] = []
    seen: set[tuple] = set()
    page_num = 1

    while True:
        rows = _vt_parse_rows(soup)
        for r in rows:
            key = tuple(r.get(col, "") for col in _VT_COLUMNS)
            if key not in seen:
                seen.add(key)
                all_rows.append(r)

        log.info(f"  Vermont page {page_num}: {len(rows)} rows | total: {len(all_rows)}")

        if max_pages and page_num >= max_pages:
            break

        next_url = _vt_find_next_url(soup, current_url)
        if not next_url:
            break

        time.sleep(1.0)
        resp = session.get(next_url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        current_url = resp.url
        page_num += 1

    raw = pd.DataFrame(all_rows)
    if raw.empty:
        raw = pd.DataFrame(columns=_VT_COLUMNS)

    # Map Vermont columns → standard schema
    raw.rename(columns={
        "Employer":    "company",
        "City":        "city",
        "Notice Date": "notice_date",
        "WARN Type":   "closure_type",
        "ZIP":         "zip",
        "LWIB Area":   "lwib_area",
    }, inplace=True)

    raw["layoff_date"] = ""
    raw["employees_affected"] = ""
    raw["notes"] = ""

    log.info(f"  Vermont: {len(raw)} rows total")
    return _normalise(raw, "Vermont")


# ── Texas ─────────────────────────────────────────────────────────────────────

_TX_CSV_URL = (
    "https://data.texas.gov/api/views/8w53-c4f6/rows.csv?accessType=DOWNLOAD"
)


def scrape_texas() -> pd.DataFrame:
    """
    Downloads the full Texas WARN dataset directly from the Socrata open-data API.
    URL: https://data.texas.gov/dataset/Worker-Adjustment-and-Retraining-Notification-WARN/8w53-c4f6
    """
    log.info("Scraping Texas (direct CSV download)...")
    resp = requests.get(_TX_CSV_URL, headers=HEADERS, timeout=120)
    resp.raise_for_status()

    raw = pd.read_csv(StringIO(resp.text), low_memory=False)
    raw.columns = [c.strip() for c in raw.columns]
    log.info(f"  Texas raw columns: {raw.columns.tolist()}")

    # Actual Texas Socrata CSV uses ALL_CAPS_UNDERSCORE column names.
    # Map both the real names and common title-case variants as fallback.
    col_map = {
        # company — actual Socrata name
        "JOB_SITE_NAME":            "company",
        # city
        "CITY_NAME":                "city",
        "City":                     "city",
        # notice date
        "NOTICE_DATE":              "notice_date",
        "Notice Date":              "notice_date",
        # layoff / effective date
        "LayOff_Date":              "layoff_date",
        "LAYOFF_DATE":              "layoff_date",
        "Layoff Date":              "layoff_date",
        "Effective Date":           "layoff_date",
        # employees
        "TOTAL_LAYOFF_NUMBER":      "employees_affected",
        "# Employees Affected":     "employees_affected",
        "Employees Affected":       "employees_affected",
        "Number of Affected Workers": "employees_affected",
        # closure type (Texas dataset has no dedicated type column; leave blank)
        "Type of Layoff/Closure":   "closure_type",
        "Type of Layoff":           "closure_type",
        "Closure/Layoff":           "closure_type",
        # notes
        "Notes":                    "notes",
        "Comments":                 "notes",
    }
    raw.rename(columns={k: v for k, v in col_map.items() if k in raw.columns}, inplace=True)

    log.info(f"  Texas: {len(raw)} rows")
    return _normalise(raw, "Texas")


# ── Virginia ──────────────────────────────────────────────────────────────────

async def _scrape_virginia_async(max_pages: int | None = None) -> list[dict]:
    import re

    URL = "https://virginiaworks.gov/im-an-employer/retain-and-grow/warn-notices/"
    REQUEST_DELAY_MS = 800

    def _split_company_address(first_td_html: str) -> tuple[str, str]:
        """Split the first cell into company name and address."""
        from bs4 import BeautifulSoup as BS
        td = BS(first_td_html, "html.parser")
        text = td.get_text("\n", strip=True)
        parts = [re.sub(r"\s+", " ", p).strip() for p in text.split("\n") if p.strip()]
        if not parts:
            return "", ""
        return parts[0], ", ".join(parts[1:]) if len(parts) > 1 else ""

    def _parse_table(html: str) -> list[dict]:
        from bs4 import BeautifulSoup as BS
        soup = BS(html, "html.parser")
        table = soup.find("table", {"id": "warn-notice-table"})
        if not table:
            return []
        tbody = table.find("tbody")
        if not tbody:
            return []

        rows = []
        for tr in tbody.find_all("tr", recursive=False):
            tds = tr.find_all("td", recursive=False)
            if not tds:
                continue

            company, address = _split_company_address(str(tds[0]))
            other = [re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip() for td in tds[1:]]
            while len(other) < 7:
                other.append("")

            row = {
                "company":            company,
                "address":            address,
                "notice_date":        other[0],
                "layoff_date":        other[1],
                "employees_affected": other[2],
                "city":               other[3],
                "contact_person":     other[4],
                "closure_type":       other[5],
                "collective_bargaining_unit": other[6],
            }
            if row["company"]:
                rows.append(row)
        return rows

    all_rows: list[dict] = []
    seen: set[tuple] = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1400, "height": 1200})

        log.info(f"  Virginia: loading {URL}")
        await page.goto(URL, wait_until="networkidle", timeout=60000)
        await page.wait_for_selector("#warn-notice-table tbody tr", timeout=60000)

        page_num = 1
        while True:
            html = await page.content()
            rows = _parse_table(html)

            added = 0
            for r in rows:
                key = (r["company"], r["notice_date"], r["layoff_date"])
                if key not in seen:
                    seen.add(key)
                    all_rows.append(r)
                    added += 1

            log.info(f"  Virginia page {page_num}: {len(rows)} rows | new: {added} | total: {len(all_rows)}")

            if max_pages and page_num >= max_pages:
                break

            next_btn = page.locator('button.dt-paging-button.next[aria-label="Next"]')
            if await next_btn.count() == 0:
                break
            aria_disabled = await next_btn.first.get_attribute("aria-disabled")
            if aria_disabled == "true":
                break

            prev_first = rows[0]["company"] if rows else ""
            await next_btn.first.click()
            await page.wait_for_timeout(REQUEST_DELAY_MS)
            await page.wait_for_load_state("networkidle")

            try:
                await page.wait_for_function(
                    """(prev) => {
                        const cell = document.querySelector('#warn-notice-table tbody tr td');
                        return cell && cell.textContent.trim() !== prev;
                    }""",
                    arg=prev_first,
                    timeout=15000,
                )
            except Exception:
                pass

            page_num += 1

        await browser.close()

    return all_rows


def scrape_virginia(max_pages: int | None = None) -> pd.DataFrame:
    """
    Scrapes Virginia WARN notices via async Playwright (DataTables pagination).
    URL: https://virginiaworks.gov/im-an-employer/retain-and-grow/warn-notices/
    """
    log.info("Scraping Virginia...")
    data = _run_async(_scrape_virginia_async(max_pages=max_pages))
    df = pd.DataFrame(data) if data else pd.DataFrame()
    log.info(f"  Virginia: {len(df)} rows")
    return _normalise(df, "Virginia")


# ── Registry ──────────────────────────────────────────────────────────────────
# To add a new state: implement scrape_<state>() above and add it here.

SCRAPERS: dict[str, callable] = {
    "Alabama":   scrape_alabama,
    "Alaska":    scrape_alaska,
    "DC":        scrape_dc,
    "Maryland":  scrape_maryland,
    "Texas":     scrape_texas,
    "Vermont":   scrape_vermont,
    "Virginia":  scrape_virginia,
    "Washington": scrape_washington,
    # "Arizona": scrape_arizona,   ← add new states here
}


# ── Runner ────────────────────────────────────────────────────────────────────

def run_all(
    states: list[str] | None = None,
    output_dir: str = ".",
    combined_filename: str | None = None,
) -> pd.DataFrame:
    """
    Run scrapers for all (or selected) states, save individual CSVs,
    and write a combined CSV ready for website import.

    Args:
        states:            List of state keys to run (default: all).
        output_dir:        Folder to write CSVs into.
        combined_filename: Override the combined CSV filename.

    Returns:
        Combined DataFrame.
    """
    today = date.today().strftime("%Y-%m-%d")
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    targets = states or list(SCRAPERS.keys())
    frames: list[pd.DataFrame] = []

    for name in targets:
        if name not in SCRAPERS:
            log.warning(f"No scraper registered for '{name}' — skipping.")
            continue
        try:
            df = SCRAPERS[name]()
            frames.append(df)

            state_slug = name.lower().replace(" ", "_")
            path = out / f"warn_{state_slug}_{today}.csv"
            df.to_csv(path, index=False)
            log.info(f"  Saved {path}")

        except Exception as exc:
            log.error(f"  {name} failed: {exc}")

    if not frames:
        log.warning("No data collected.")
        return pd.DataFrame(columns=OUTPUT_COLS)

    combined = pd.concat(frames, ignore_index=True)
    combined_path = out / (combined_filename or f"warn_combined_{today}.csv")
    combined.to_csv(combined_path, index=False)
    log.info(f"\nCombined file saved → {combined_path}  ({len(combined)} total rows)")

    return combined


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    df = run_all(output_dir="warn_output")

    print(f"\n{'='*60}")
    print(f"TOTAL ROWS: {len(df)}")
    print(f"STATES:     {df['state'].unique().tolist()}")
    print(f"{'='*60}\n")
    print(df.to_string(index=False, max_rows=30))