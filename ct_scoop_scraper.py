#!/usr/bin/env python
"""Scrape Connecticut Scoop articles and save results to Excel/JSON.

This script is derived from `cs.ipynb`.
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import datetime, timedelta
import json
from selenium.webdriver.chrome.options import Options
options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(options=options)

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

    # ---- Date filter ----
    today = datetime.today().date()
    two_days_ago = today - timedelta(days=2)

    # Create driver ONCE
    driver = webdriver.Chrome()
    wait = WebDriverWait(driver, 15)

    for url in links:
        print(f"Scraping: {url}")
        driver.get(url)

        wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "div.waddons-blog-card")
            )
        )

        cards = driver.find_elements(By.CSS_SELECTOR, "div.waddons-blog-card")

        for card in cards:

            # ---- Heading ----
            heading_elements = card.find_elements(By.CSS_SELECTOR, "div.waddons-blog-header")
            heading = heading_elements[0].text.strip() if heading_elements else ""

            # ---- Date ----
            meta_elements = card.find_elements(By.CSS_SELECTOR, "div.waddons-blog-meta")
            article_date = None

            if meta_elements:
                meta_text = meta_elements[0].text.strip()
                date_text = meta_text.split("-")[0].strip()

                try:
                    article_date = datetime.strptime(date_text, "%m/%d/%Y").date()
                except Exception:
                    article_date = None

            # ---- Link ----
            link_elements = card.find_elements(By.CSS_SELECTOR, "a.waddons-blog-card-link-full")
            link = link_elements[0].get_attribute("href") if link_elements else ""

            # ---- Filter last 2 days ----
            if link and article_date and article_date >= two_days_ago:
                print("Added:", heading, article_date)

                all_links.append({
                    "heading": heading,
                    "date": article_date,
                    "link": link,
                })

    driver.quit()

    # Remove duplicates
    df = pd.DataFrame(all_links).drop_duplicates(subset=["link"])

    # Sort newest first
    df = df.sort_values(by="date", ascending=False)

    print("Total articles found:", len(df))

    # Save to Excel
    export_path = f"ct_scoop_links_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    df.to_excel(export_path, index=False)
    print(f"Saved {len(df)} records to {export_path}")

    # Save JSON for dashboard tab
    json_path = "ct_scoop_latest.json"
    json_payload = {
        "last_updated": today.strftime("%Y-%m-%d"),
        "data": df.to_dict(orient="records"),
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_payload, f, ensure_ascii=False, indent=2)
    print(f"Saved JSON to {json_path}")


if __name__ == "__main__":
    main()
