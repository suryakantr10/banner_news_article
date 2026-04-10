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
import base64

# ────────────────────────────────────────────────
# Google News URL Decoder
# ────────────────────────────────────────────────

def _decode_gnews_blob(encoded: str) -> str | None:
    """
    Decode a base64url-encoded Google News article blob to the real article URL.

    Google News encodes article links as protobuf-style blobs. The structure is:
        0x08  varint          – field 1 (article type tag)
        0x12  varint length   – field 2 (length-delimited)
        <length bytes>        – the inner blob (another layer)

    Inside the inner blob, field 1 (0x0A + varint length) typically holds the URL.
    We walk the bytes looking for any UTF-8 sequence starting with 'http'.
    """
    # Fix base64 padding
    rem = len(encoded) % 4
    if rem:
        encoded += '=' * (4 - rem)

    try:
        data = base64.urlsafe_b64decode(encoded)
    except Exception:
        return None

    # Walk every byte position and look for 0x0A (protobuf field 1, wire type 2)
    # followed by a varint length, followed by URL-like bytes.
    i = 0
    while i < len(data) - 4:
        if data[i] == 0x0A:
            # Read varint length (single-byte varints cover up to 127 bytes)
            length_byte = data[i + 1]
            if length_byte & 0x80:
                # Multi-byte varint – handle 2-byte case (covers up to 16 383)
                if i + 2 < len(data):
                    length = (length_byte & 0x7F) | ((data[i + 2] & 0x7F) << 7)
                    start = i + 3
                else:
                    i += 1
                    continue
            else:
                length = length_byte
                start = i + 2

            end = start + length
            if end <= len(data):
                candidate = data[start:end]
                try:
                    s = candidate.decode('utf-8')
                    if s.startswith('http'):
                        return s
                except UnicodeDecodeError:
                    pass
        i += 1

    # Fallback: find the first 'http' sequence anywhere in the blob
    idx = data.find(b'http')
    if idx != -1:
        chunk = data[idx:]
        end = next((j for j, b in enumerate(chunk) if b < 32), len(chunk))
        try:
            return chunk[:end].decode('utf-8', errors='ignore') or None
        except Exception:
            pass

    return None


def decode_google_news_url(entry) -> str:
    """
    Return the real article URL from a feedparser RSS entry.

    Strategy (in priority order):
    1. entry.source['href']  – feedparser exposes the publisher's domain here;
       sometimes this IS the canonical URL.
    2. Decode the base64 blob in entry.link.
    3. Return entry.link unchanged (Google redirect) as last resort.
    """
    raw_link: str = entry.get('link', '')

    # ── Strategy 1: check entry.source ──────────────────────────────────────
    source = entry.get('source', {})
    source_href: str = source.get('href', '') if isinstance(source, dict) else ''
    # entry.source.href is usually just the publisher homepage (e.g. CNN.com),
    # not the article URL, so we only use it if it looks like a full article path.
    if source_href.startswith('http') and '/' in source_href.lstrip('https://'):
        path = source_href.split('/', 3)
        if len(path) > 3 and len(path[3]) > 5:          # has a non-trivial path
            return source_href

    # ── Strategy 2: decode the blob ─────────────────────────────────────────
    match = re.search(r'/articles/([A-Za-z0-9_=-]+)', raw_link)
    if match:
        decoded = _decode_gnews_blob(match.group(1))
        if decoded:
            return decoded

    # ── Strategy 3: return as-is (still clickable, just goes via Google) ────
    return raw_link


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
        df['Store']   = df['Store'].astype(str).str.strip()
        df['Analyst'] = df['Analyst'].astype(str).str.strip()
        df = df.dropna(subset=['Store'])
        df = df[df['Store'].str.len() > 0]

        stores = df['Store'].tolist()
        analyst_map = dict(zip(df['Store'], df['Analyst']))

        print(f"→ Loaded {len(stores)} stores + {len(analyst_map)} mappings from analysts.csv")
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
    if not summary:
        return "No summary"
    text = unescape(str(summary))
    text = _TAG_RE.sub("", text)
    text = " ".join(text.split())
    return text or "No summary"


def is_recent(published_str, cutoff_date):
    if not published_str or published_str == 'Date not available':
        return False
    try:
        pub_date = date_parser.parse(published_str).date()
        return pub_date >= cutoff_date
    except Exception:
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

    return f'"{store}" {keywords} {retail_context} {locations} after:{recent_date}'


def fetch_news_for_store(store, is_closure=False):
    query = build_query(store, is_closure)
    encoded_query = quote_plus(query)
    rss_url = (
        f"https://news.google.com/rss/search?q={encoded_query}"
        f"&hl=en-US&gl=US&ceid=US:en&scoring=d"
    )

    feed = feedparser.parse(rss_url)
    cutoff_date = date.today() - timedelta(days=2)

    results = []
    for entry in feed.entries:
        published_str = entry.get('published', 'Date not available')
        if not is_recent(published_str, cutoff_date):
            continue

        # ── FIXED: decode the Google News redirect URL ──────────────────────
        real_link = decode_google_news_url(entry)
        # ────────────────────────────────────────────────────────────────────

        raw_summary = entry.get('summary', 'No summary')
        results.append({
            'title':     entry.title,
            'link':      real_link,          # ← now the real article URL
            'published': published_str,
            'summary':   clean_summary(raw_summary),
            'type':      'Closing' if is_closure else 'Opening',
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
    analyst = analyst_map.get(store, "Unassigned")

    results_open  = fetch_news_for_store(store, is_closure=False)
    results_close = fetch_news_for_store(store, is_closure=True)

    all_results = results_open + results_close

    if all_results:
        found_any = True
        for res in all_results:
            analyst_results[analyst].append({
                'Store':     store,
                'Analyst':   analyst,
                'Type':      res['type'],
                'Title':     res['title'],
                'Link':      res['link'],
                'Published': res['published'],
                'Summary':   res['summary'],
            })

    time.sleep(1.3)

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

    all_rows = []
    for items in analyst_results.values():
        all_rows.extend(items)

    if all_rows:
        df_out = pd.DataFrame(all_rows)
        today_str = date.today().strftime("%Y-%m-%d")
        filename = f"banner_news_{today_str}.csv"
        df_out.to_csv(filename, index=False, encoding='utf-8')
        print(f"\nResults saved to: {filename}")
        print(f"Total articles: {len(all_rows)}")

# ────────────────────────────────────────────────
# Always create latest_news.json (even if no news found)
# ────────────────────────────────────────────────
today_str = date.today().strftime("%Y-%m-%d")

json_data = {
    "last_updated": today_str,
    "data": {analyst: items for analyst, items in sorted(analyst_results.items())},
}

with open('latest_news.json', 'w', encoding='utf-8') as f:
    json.dump(json_data, f, ensure_ascii=False, indent=2)

archive_filename = f"latest_news_{today_str}.json"
with open(archive_filename, 'w', encoding='utf-8') as f:
    json.dump(json_data, f, ensure_ascii=False, indent=2)

print(f"\n✓ latest_news.json created/updated (last_updated: {today_str})")
print(f"✓ {archive_filename} created as historical snapshot")