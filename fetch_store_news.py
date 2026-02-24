import feedparser
from urllib.parse import quote_plus
import time
from datetime import date, timedelta
from pathlib import Path
from dateutil import parser as date_parser
import pandas as pd
from collections import defaultdict
import json
import re
from html import unescape

# ────────────────────────────────────────────────
# Default fallback (only used if analysts.csv is missing)
# ────────────────────────────────────────────────
default_stores = [
    "Doggie Style", "Dogtopia", "Earthwise Pet", "Feeders Supply",
    "Friendly Pets", "Hollywood Feed", "Kahoots Pet Products", "Kriser's",
    "Mud Bay", "Pet Club Food and Supplies", "Pet Depot", "Pet Evolution"
]

# ────────────────────────────────────────────────
# Load stores + analyst mapping from analysts.csv (single source of truth)
# ────────────────────────────────────────────────
stores = []
analyst_map = {}

csv_file = Path('analyst.csv')

if csv_file.exists():
    try:
        df = pd.read_csv(csv_file)
        # Clean up whitespace issues
        df['Store']  = df['Store'].astype(str).str.strip()
        df['Analyst'] = df['Analyst'].astype(str).str.strip()
        
        # Drop rows that are completely empty or missing critical columns
        df = df.dropna(subset=['Store'])
        df = df[df['Store'].str.len() > 0]
        
        stores = df['Store'].tolist()
        analyst_map = dict(zip(df['Store'], df['Analyst']))
        
        print(f"→ Loaded {len(stores)} stores + {len(analyst_map)} mappings from analysts.csv")
        
        # Optional: show how many analysts are actually used
        unique_analysts = len(set(analyst_map.values()))
        print(f"   → {unique_analysts} unique analysts detected")
        
    except Exception as e:
        print(f"Error reading analysts.csv: {e}")
        print("→ Falling back to default pet stores list")
        stores = default_stores
        analyst_map = {s: "Unassigned" for s in default_stores}
else:
    print("→ analyst.csv not found")
    print("→ Using default pet stores fallback")
    stores = default_stores
    analyst_map = {s: "Unassigned" for s in default_stores}

# ────────────────────────────────────────────────
# Helper functions
# ────────────────────────────────────────────────
_TAG_RE = re.compile(r"<[^>]+>")


def clean_summary(summary: str) -> str:
    """
    Convert HTML summaries (e.g. <a href=\"...\">Title</a>) into plain text.
    This keeps the text readable in CSV/JSON and on the dashboard.
    """
    if not summary:
        return "No summary"
    # Decode HTML entities first
    text = unescape(str(summary))
    # Strip HTML tags
    text = _TAG_RE.sub("", text)
    # Collapse extra whitespace
    text = " ".join(text.split())
    return text or "No summary"


def is_recent(published_str, cutoff_date):
    if not published_str or published_str == 'Date not available':
        return False
    try:
        pub_date = date_parser.parse(published_str).date()
        return pub_date >= cutoff_date
    except:
        return False

def build_query(store, is_closure=False):
    base_keywords_open = (
        '"new store" OR "new location" OR "opening soon" OR "coming soon" OR '
        '"grand opening" OR "now open" OR "opens new" OR "opening in" OR '
        '"to open" OR "set to open" OR "plans to open" OR "breaks ground" OR '
        '"now hiring" OR "store opening" OR "location opening"'
    )
    
    base_keywords_close = (
        '"store closing" OR "closing soon" OR "closing" OR "closures" OR '
        '"shutting down" OR "shutters" OR "permanent closure" OR "permanent closing" OR '
        '"going out of business" OR "going-out-of-business" OR "liquidation" OR '
        '"everything must go" OR "store closing sale" OR "last day" OR "final day" OR '
        '"final closing" OR "ceases operations" OR "store to close" OR "stores to close" OR '
        '"closing all locations" OR "closing locations" OR "shutter stores"'
    )
    
    keywords = base_keywords_close if is_closure else base_keywords_open
    
    retail_context = '(store OR location OR retail OR shop OR outlet OR station OR pharmacy OR supermarket OR grocery OR "auto parts")'
    locations = '(USA OR Canada OR "United States" OR America OR state OR city OR county)'
    
    recent_date = (date.today() - timedelta(days=4)).strftime('%Y-%m-%d')
    
    query = f'"{store}" {keywords} {retail_context} {locations} after:{recent_date}'
    return query

def fetch_news_for_store(store, is_closure=False):
    query = build_query(store, is_closure)
    encoded_query = quote_plus(query)
    rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en&scoring=d"
    
    feed = feedparser.parse(rss_url)
    
    cutoff_date = date.today() - timedelta(days=2)
    
    results = []
    for entry in feed.entries:
        published_str = entry.get('published', 'Date not available')
        if is_recent(published_str, cutoff_date):
            raw_summary = entry.get('summary', 'No summary')
            results.append({
                'title': entry.title,
                'link': entry.link,
                'published': published_str,
                # Store a clean, plain-text summary (no HTML tags)
                'summary': clean_summary(raw_summary),
                'type': 'Closing' if is_closure else 'Opening'
            })
    
    results.sort(key=lambda x: x['published'], reverse=True)
    return results

# ────────────────────────────────────────────────
# Main execution
# ────────────────────────────────────────────────
print(f"\nRun date: {date.today()}")
print("Fetching recent (last 2 days) store OPENING + CLOSING news in USA/Canada...\n")

analyst_results = defaultdict(list)
found_any = False

for store in stores:
    # Look up analyst using the exact string from the CSV
    analyst = analyst_map.get(store, "Unassigned")
    
    # Check for both types of news
    results_open  = fetch_news_for_store(store, is_closure=False)
    results_close = fetch_news_for_store(store, is_closure=True)
    
    all_results = results_open + results_close
    
    if all_results:
        found_any = True
        for res in all_results:
            analyst_results[analyst].append({
                'Store': store,
                'Analyst': analyst,
                'Type': res['type'],
                'Title': res['title'],
                'Link': res['link'],
                'Published': res['published'],
                'Summary': res['summary']
            })
    
    time.sleep(1.3)  # polite delay

# ────────────────────────────────────────────────
# Output – grouped by analyst
# ────────────────────────────────────────────────
if not found_any:
    print("No recent store opening or closing news found in the last 2 days.")
else:
    print("=" * 85)
    print("     STORE OPENING & CLOSING NEWS – GROUPED BY ANALYST (last 2 days only)")
    print("=" * 85 + "\n")
    
    for analyst, items in sorted(analyst_results.items()):
        print(f"Analyst: {analyst}   ({len(items)} article(s))")
        print("-" * 70)
        
        for i, item in enumerate(items, 1):
            print(f"{i}. [{item['Type']}] {item['Store']}")
            print(f"   {item['Title']}")
            print(f"   Published: {item['Published']}")
            print(f"   Link:      {item['Link']}")
            summary_short = (item['Summary'][:220] + "...") if len(item['Summary']) > 220 else item['Summary']
            print(f"   {summary_short}\n")
        
        print()

    # Save results
    all_rows = []
    for items in analyst_results.values():
        all_rows.extend(items)
    
    if all_rows:
        df = pd.DataFrame(all_rows)
        today_str = date.today().strftime("%Y-%m-%d")
        filename = f"store_open_close_news_{today_str}.csv"
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\nResults saved to: {filename}")
        print(f"Total articles: {len(all_rows)}")

# ────────────────────────────────────────────────
# Always create latest_news.json (even if no news found)
# Also create a date-stamped archive JSON (latest_news_YYYY-MM-DD.json)
# ────────────────────────────────────────────────
today_str = date.today().strftime("%Y-%m-%d")

# Convert defaultdict to regular dict for JSON serialization
json_data = {
    "last_updated": today_str,
    "data": {}
}

# Convert analyst_results to the required format
for analyst, items in sorted(analyst_results.items()):
    json_data["data"][analyst] = items

# Write latest snapshot JSON file used by the dashboard
with open('latest_news.json', 'w', encoding='utf-8') as f:
    json.dump(json_data, f, ensure_ascii=False, indent=2)

# Write date-stamped archive JSON so historical days are preserved
archive_filename = f"latest_news_{today_str}.json"
with open(archive_filename, 'w', encoding='utf-8') as f:
    json.dump(json_data, f, ensure_ascii=False, indent=2)

print(f"\n✓ latest_news.json created/updated (last_updated: {today_str})")
print(f"✓ {archive_filename} created as historical snapshot")
