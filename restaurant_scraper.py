from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd
import json

# ── Chrome setup (GitHub Actions compatible) ──────────────────────────────────
def get_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    )

    # Explicitly use the chromedriver from PATH (installed by setup-chrome action)
    # This avoids Selenium picking up the mismatched system chromedriver
    from selenium.webdriver.chrome.service import Service
    import shutil
    chromedriver_path = shutil.which("chromedriver")
    print(f"Using chromedriver: {chromedriver_path}")
    service = Service(executable_path=chromedriver_path)
    return webdriver.Chrome(service=service, options=options)

# ── Config ────────────────────────────────────────────────────────────────────
URL       = "https://whatnow.com/category/restaurants/"
MAX_PAGES = 10
now_utc   = datetime.now(timezone.utc)
CUTOFF    = now_utc - timedelta(days=2)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    )
}

# ── Scrape post listing ───────────────────────────────────────────────────────
driver = get_driver()

try:
    driver.get(URL)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "p-wrap"))
        )
    except Exception:
        print("⚠️  No posts found on initial load — check if selector changed")

    def count_posts():
        return len(driver.find_elements(By.CLASS_NAME, "p-wrap"))

    pages_viewed = 1
    while pages_viewed < MAX_PAGES:
        prev_count = count_posts()
        try:
            view_more = driver.find_element(
                By.XPATH, "//a[contains(@class, 'loadmore-trigger')]"
            )
        except Exception:
            print(f"No more 'View More' button — stopping at page {pages_viewed}")
            break

        driver.execute_script("arguments[0].click();", view_more)
        try:
            WebDriverWait(driver, 10).until(lambda d: count_posts() > prev_count)
            pages_viewed += 1
            print(f"  Loaded page {pages_viewed}/{MAX_PAGES}")
            time.sleep(0.5)
        except Exception:
            print("Click did not load new posts or timed out")
            break

    page_source = driver.page_source

finally:
    driver.quit()   # always quit — even if scraping throws an error

# ── Parse posts ───────────────────────────────────────────────────────────────
soup  = BeautifulSoup(page_source, "html.parser")
posts = soup.select("div.p-wrap")

rows      = []
seen_urls = set()

for p in posts:
    a    = p.select_one("h4.entry-title a.p-url") or p.select_one("h4.entry-title a")
    href = a.get("href") if a else None
    if not href or href in seen_urls:
        continue

    time_el  = p.select_one("time[datetime]") or p.select_one("time")
    post_dt  = None
    date_str = None

    if time_el:
        if time_el.has_attr("datetime"):
            dt_str = time_el["datetime"].replace("Z", "+00:00")
            try:
                post_dt = datetime.fromisoformat(dt_str)
                if post_dt.tzinfo is None:
                    post_dt = post_dt.replace(tzinfo=timezone.utc)
                date_str = post_dt.strftime("%B %d, %Y")
            except Exception:
                date_str = time_el.get_text(strip=True)
        else:
            date_str = time_el.get_text(strip=True)

    # Skip posts outside the 48-hour window
    if post_dt is None or post_dt < CUTOFF:
        continue

    seen_urls.add(href)
    rows.append({"date": date_str, "url": href})

print(f"\n📋 Posts within last 48 hours: {len(rows)}")

if not rows:
    print("No posts found in window — exiting without writing files.")
    exit(0)

# ── Fetch individual article pages ────────────────────────────────────────────
for i, row in enumerate(rows, 1):
    try:
        resp = requests.get(row["url"], headers=HEADERS, timeout=15)
        resp.raise_for_status()
        article_soup = BeautifulSoup(resp.text, "html.parser")

        row["title"] = (
            article_soup.title.string.strip() if article_soup.title else ""
        )

        address  = None
        addr_div = article_soup.find(
            "div", class_="bottom_infowindow bottom_infowindow0 only_one"
        )
        if addr_div:
            h3 = addr_div.find("h3")
            if h3:
                address = h3.get_text(strip=True)
        row["address"] = address or ""

        print(f"  [{i}/{len(rows)}] ✓ {row['title'][:60]}")

    except Exception as e:
        row["title"]   = ""
        row["address"] = ""
        print(f"  [{i}/{len(rows)}] ✗ Failed to fetch {row['url']} — {e}")

    time.sleep(0.3)   # polite delay between article requests

# ── Save CSV ──────────────────────────────────────────────────────────────────
today    = datetime.now().strftime("%Y-%m-%d")
csv_file = f"Daily_restaurants_{today}.csv"
df       = pd.DataFrame(rows, columns=["date", "title", "address", "url"])
df.to_csv(csv_file, index=False, encoding="utf-8")
print(f"\n✅ CSV  saved → {csv_file} ({len(df)} rows)")

# ── Save JSON ─────────────────────────────────────────────────────────────────
json_payload = {
    "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    "total":        len(rows),
    "data":         rows,
}
with open("restaurant_latest.json", "w", encoding="utf-8") as f:
    json.dump(json_payload, f, ensure_ascii=False, indent=2)

print(f"✅ JSON saved → restaurant_latest.json ({len(rows)} records)")