from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd
import json
from selenium.webdriver.chrome.options import Options

url = "https://whatnow.com/category/restaurants/"
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"

options = Options()
options.add_argument("user-agent=" + user_agent)
driver = webdriver.Chrome(options=options)
driver.get(url)
time.sleep(3)

max_pages = 10
pages_viewed = 1

# ── Date filter ────────────────────────────────────────────────────────────────
now_utc = datetime.now(timezone.utc)
cutoff  = now_utc - timedelta(days=2)
# ──────────────────────────────────────────────────────────────────────────────

def count_posts():
    return len(driver.find_elements(By.CLASS_NAME, "p-wrap"))

try:
    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, "p-wrap"))
    )
except:
    print("No posts found on initial load")

while pages_viewed < max_pages:
    prev_count = count_posts()
    try:
        view_more = driver.find_element(By.XPATH, "//a[contains(@class, 'loadmore-trigger')]")
    except:
        print("No more VIEW MORE button found")
        break

    driver.execute_script("arguments[0].click();", view_more)
    try:
        WebDriverWait(driver, 10).until(lambda d: count_posts() > prev_count)
        pages_viewed += 1
        print(f"Viewed pages: {pages_viewed}/{max_pages}")
        time.sleep(0.5)
    except:
        print("Click did not load new posts or timed out")
        break

page_source = driver.page_source
driver.quit()

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
            except:
                date_str = time_el.get_text(strip=True)
        else:
            date_str = time_el.get_text(strip=True)

    if post_dt is None or post_dt < cutoff:
        continue

    seen_urls.add(href)
    rows.append({"date": date_str, "url": href})

print(f"Posts within last 2 days: {len(rows)}")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

for row in rows:
    response     = requests.get(row["url"], headers=headers)
    soup         = BeautifulSoup(response.text, "html.parser")
    row["title"] = soup.title.string.strip() if soup.title else ""

    address  = None
    addr_div = soup.find("div", class_="bottom_infowindow bottom_infowindow0 only_one")
    if addr_div:
        h3 = addr_div.find("h3")
        if h3:
            address = h3.get_text(strip=True)
    row["address"] = address or ""   # empty string instead of None for clean JSON

# ── Save CSV (daily archive) ───────────────────────────────────────────────────
today    = datetime.now().strftime("%Y-%m-%d")
csv_file = f"Daily_restaurants_{today}.csv"
df = pd.DataFrame(rows)
df.to_csv(csv_file, index=False, encoding="utf-8")
print(f"CSV saved  → {csv_file} ({len(df)} rows)")

# ── Save JSON (this is what the website reads) ────────────────────────────────
json_payload = {
    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
    "data": rows   # list of {date, url, title, address}
}

with open("restaurant_latest.json", "w", encoding="utf-8") as f:
    json.dump(json_payload, f, ensure_ascii=False, indent=2)

print(f"JSON saved → restaurant_latest.json ({len(rows)} records)")