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
import pandas as pd
from datetime import date
from pathlib import Path

import requests
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
        # Jupyter / IPython: schedule on the existing loop
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

def _normalise(df: pd.DataFrame, state: str) -> pd.DataFrame:
    """Ensure every required column exists and state is set."""
    df = df.copy()
    df["state"] = state
    for col in OUTPUT_COLS:
        if col not in df.columns:
            df[col] = ""
    # reorder: required cols first, then any extras
    extras = [c for c in df.columns if c not in OUTPUT_COLS]
    return df[OUTPUT_COLS + extras]


# ── State scrapers ────────────────────────────────────────────────────────────

async def _scrape_alabama_async() -> list[dict]:
    data = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://workforce.alabama.gov/warn-list/")
        await page.wait_for_selector('tr.fw-warn-list__items[data-year="2026"]', timeout=15000)

        rows = await page.query_selector_all('tr.fw-warn-list__items[data-year="2026"]')
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
    for row in soup.find_all("tr")[2:]:
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
    bypassing pd.read_html entirely (the DC table has deeply broken
    multi-level headers that confuse pandas into creating hundreds of
    junk columns).
    """
    URL = "https://does.dc.gov/page/industry-closings-and-layoffs-warn-notifications-2025"
    SELECTOR = ".field-name-body table, .field-items table, article table"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_selector(SELECTOR, timeout=20000)

        # Pull every row's cell text directly — avoids pandas header confusion
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
        await browser.close()

    if not rows_data:
        return []

    # Identify the real header row — the first row with 3+ non-empty cells
    header_idx = next(
        (i for i, r in enumerate(rows_data) if sum(bool(c) for c in r) >= 3),
        0
    )
    headers = [h.strip() for h in rows_data[header_idx]]
    data_rows = rows_data[header_idx + 1:]

    # Map whatever DC calls its columns → standard names
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

    norm_headers = [col_map.get(h.lower(), h.lower()) for h in headers]

    records = []
    for row in data_rows:
        if not any(row):          # skip fully blank rows
            continue
        # pad short rows so zip works
        padded = row + [""] * (len(norm_headers) - len(row))
        records.append(dict(zip(norm_headers, padded)))

    return records


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

    # Initial page load + trigger search
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

    Args:
        output_dir: Folder to write the CSV into.
        max_pages:  Cap on pages to scrape (None = all pages).
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

    from io import StringIO
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


# ── Registry ──────────────────────────────────────────────────────────────────
# To add a new state: implement scrape_<state>() above and add it here.

SCRAPERS: dict[str, callable] = {
    "Alabama":  scrape_alabama,
    "Alaska":   scrape_alaska,
    "DC":       scrape_dc,
    "Maryland": scrape_maryland,
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

            # Per-state CSV
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
    # Run all registered scrapers and save to current directory
    df = run_all(output_dir="warn_output")

    print(f"\n{'='*60}")
    print(f"TOTAL ROWS: {len(df)}")
    print(f"STATES:     {df['state'].unique().tolist()}")
    print(f"{'='*60}\n")
    print(df.to_string(index=False, max_rows=30))