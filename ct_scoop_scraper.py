#!/usr/bin/env python
"""Scrape Connecticut Scoop articles and save results to Excel/JSON.

This script is derived from `cs.ipynb`.
"""

from selenium import webdriver
from selenium.common.exceptions import SessionNotCreatedException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import datetime, date, timedelta
import json
from selenium.webdriver.chrome.options import Options
import os
import shutil


class DateEncoder(json.JSONEncoder):
    """JSON encoder that converts date/datetime objects to ISO strings."""

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def _find_chrome_binary() -> str | None:
    """Return a usable Chrome/Chromium binary path for this environment."""
    # Respect env vars used by some CI systems.
    for env_var in ("CHROME_BIN", "GOOGLE_CHROME_BIN"):
        path = os.environ.get(env_var)
        if path and os.path.exists(path):
            return path

    # Try common executable names first, then absolute paths.
    candidates = [
        "google-chrome",
        "google-chrome-stable",
        "chromium-browser",
        "chromium",
        "/snap/bin/chromium",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
    ]

    for candidate in candidates:
        if os.path.isabs(candidate):
            if os.path.exists(candidate):
                return candidate
        else:
            found = shutil.which(candidate)
            if found:
                return found

    return None


def _make_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-translate")
    options.add_argument("--disable-features=VizDisplayCompositor")

    chrome_path = _find_chrome_binary()
    if chrome_path:
        print(f"Using Chrome/Chromium binary: {chrome_path}")
        options.binary_location = chrome_path
    else:
        print("Warning: No Chrome/Chromium binary found; relying on system defaults.")

    # Selenium Manager will download a matching ChromeDriver if one is not available.
    try:
        return webdriver.Chrome(options=options)
    except SessionNotCreatedException as exc:
        print("First attempt to start Chrome failed; retrying with legacy headless mode...")
        print(str(exc))

        # Retry with the legacy headless flag (some environments/distro builds require it)
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-sync")
        options.add_argument("--disable-translate")
        options.add_argument("--disable-features=VizDisplayCompositor")
        if chrome_path:
            options.binary_location = chrome_path

        return webdriver.Chrome(options=options)


def main():
    # All links in a list
    links = [
        "https://www.theconnecticutscoop.com/statewide-news.html",
        "https://www.theconnecticutscoop.com/tolland-county.html",
        "https://www.theconnecticutscoop.com/windham-county.html",
        "https://www.theconnecticutscoop.com/hartford-county.html",
        "https://www.theconnecticutscoop.com/new-haven-county.html",
        "https://www.theconnecticutscoop.com/litchfield-county.html",
        "https://www.theconnecticutscoop.com/fairfield-county.html",
        "https://www.theconnecticutscoop.com/new-london-county.html",
        "https://www.theconnecticutscoop.com/middlesex-county.html",
        "https://www.theconnecticutscoop.com/massachusetts.html",
    ]

    all_links = []

    # ---- Date filter (configurable via env var CT_SCOOP_DAYS) ----
    today = datetime.today().date()
    lookback_days = int(os.environ.get("CT_SCOOP_DAYS", "2"))
    two_days_ago = today - timedelta(days=lookback_days)
    print(f"Filtering articles from {two_days_ago.isoformat()} onwards (last {lookback_days} days)")

    # Create driver ONCE
    driver = _make_driver()
    wait = WebDriverWait(driver, 15)

    try:
        for url in links:
            print(f"Scraping: {url}")
            driver.get(url)

            wait.until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "div.waddons-blog-card")
                )
            )

            cards = driver.find_elements(By.CSS_SELECTOR, "div.waddons-blog-card")
            print(f"Found {len(cards)} card(s) on page: {url}")

            for card in cards:

                # ---- Heading ----
                heading_elements = card.find_elements(By.CSS_SELECTOR, "div.waddons-blog-header")
                heading = heading_elements[0].text.strip() if heading_elements else ""

                # ---- Date ----
                meta_elements = card.find_elements(By.CSS_SELECTOR, "div.waddons-blog-meta")
                article_date = None

                if meta_elements:
                    meta_text = meta_elements[0].text.strip()
                    # Extract MM/DD/YYYY from the meta text
                    import re

                    match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", meta_text)
                    if match:
                        date_text = match.group(1)
                        try:
                            article_date = datetime.strptime(date_text, "%m/%d/%Y").date()
                        except Exception:
                            article_date = None
                    else:
                        print("Warning: could not find a date in meta text:", repr(meta_text))

                # ---- Link ----
                link_elements = card.find_elements(By.CSS_SELECTOR, "a.waddons-blog-card-link-full")
                link = link_elements[0].get_attribute("href") if link_elements else ""

                # DEBUG: show what we parsed for the card (helps in CI logs)
                print("Card debug:", {
                    "heading": heading,
                    "meta_text": meta_text if meta_elements else None,
                    "date_text": locals().get("date_text"),
                    "article_date": article_date,
                    "link": link,
                })

                # ---- Filter last 2 days ----
                if link and article_date and article_date >= two_days_ago:
                    print("Added:", heading, article_date)

                    all_links.append({
                        "heading": heading,
                        "date": article_date,
                        "link": link,
                    })

    finally:
        driver.quit()

    # Build DataFrame (handle case where no articles were found)
    df = pd.DataFrame(all_links)
    if df.empty:
        print("Warning: no articles were found; producing empty output files.")
        df = pd.DataFrame(columns=["heading", "date", "link"])
    else:
        print("DataFrame columns before normalization:", df.columns.tolist())
        if "link" not in df.columns:
            df["link"] = ""
        if "date" not in df.columns:
            df["date"] = pd.NaT

        # Remove duplicates
        df = df.drop_duplicates(subset=["link"])

        # Sort newest first
        try:
            df = df.sort_values(by="date", ascending=False)
        except KeyError as exc:
            print("Warning: 'date' column missing when sorting; adding placeholder and retrying.")
            df["date"] = pd.NaT
            df = df.sort_values(by="date", ascending=False)
        except Exception as exc:
            print("Warning: could not sort by date (missing or invalid values).", exc)

    print("Total articles found:", len(df))

    # Save to Excel (fallback to CSV if openpyxl is missing)
    export_path = f"ct_scoop_links_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    try:
        df.to_excel(export_path, index=False)
        print(f"Saved {len(df)} records to {export_path}")
    except ModuleNotFoundError as exc:
        fallback_path = export_path.replace(".xlsx", ".csv")
        df.to_csv(fallback_path, index=False)
        print(
            "openpyxl not installed (", exc, "); saved to CSV instead:", fallback_path
        )

    # Save JSON for dashboard tab (convert date objects to strings)
    json_path = "ct_scoop_latest.json"
    json_data = df.copy()
    if "date" in json_data.columns:
        json_data["date"] = json_data["date"].astype(str)

    json_payload = {
        "last_updated": today.strftime("%Y-%m-%d"),
        "data": json_data.to_dict(orient="records"),
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_payload, f, ensure_ascii=False, indent=2, cls=DateEncoder)
    print(f"Saved JSON to {json_path}")


if __name__ == "__main__":
    main()
