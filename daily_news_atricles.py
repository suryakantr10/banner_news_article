"""
daily_news_atricles.py
──────────────────────
Fetches general daily retail news (store openings, closings, expansions)
from Google News RSS and saves results to:
  - daily_news_latest.json          ← dashboard reads this
  - daily_news_YYYY-MM-DD.json      ← dated archive

Run manually:
    python daily_news_atricles.py

Or automatically via GitHub Actions (.github/workflows/daily-news-articls.yml).
"""

import json
import time
from datetime import date, timedelta
from urllib.parse import quote_plus
from html import unescape
import re

import feedparser
from dateutil import parser as date_parser

# ────────────────────────────────────────────────
# Search topics – each is a standalone Google News query
# ────────────────────────────────────────────────
SEARCH_QUERIES = [
    '"store opening" OR "grand opening" OR "now open" retail USA',
    '"store closing" OR "closing soon" OR "permanent closure" retail USA',
    '"new location" OR "opens new store" OR "opening soon" retail',
    '"restaurant opening" OR "new restaurant" USA 2026',
    '"restaurant closing" OR "closed permanently" USA 2026',
    '"retail expansion" OR "new outlet" OR "coming soon" store',
]

CUTOFF_DAYS = 3   # only keep articles published within the last N days

_TAG_RE = re.compile(r"<[^>]+>")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = unescape(str(text))
    text = _TAG_RE.sub("", text)
    return " ".join(text.split())


def is_recent(published_str: str, cutoff: date) -> bool:
    if not published_str or published_str == "Date not available":
        return False
    try:
        return date_parser.parse(published_str).date() >= cutoff
    except Exception:
        return False


def fetch_query(query: str, cutoff: date) -> list[dict]:
    encoded = quote_plus(query)
    url = (
        f"https://news.google.com/rss/search?q={encoded}"
        "&hl=en-US&gl=US&ceid=US:en&scoring=d"
    )
    feed = feedparser.parse(url)
    results = []
    for entry in feed.entries:
        pub = entry.get("published", "")
        if not is_recent(pub, cutoff):
            continue
        results.append({
            "title": clean_text(entry.get("title", "No title")),
            "date":  pub,
            "link":  entry.get("link", ""),
        })
    return results


def deduplicate(articles: list[dict]) -> list[dict]:
    seen_links = set()
    seen_titles = set()
    unique = []
    for a in articles:
        link  = a["link"].strip()
        title = a["title"].lower().strip()
        if link in seen_links or title in seen_titles:
            continue
        seen_links.add(link)
        seen_titles.add(title)
        unique.append(a)
    return unique


# ────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────
def main():
    today     = date.today()
    cutoff    = today - timedelta(days=CUTOFF_DAYS)
    today_str = today.strftime("%Y-%m-%d")

    print(f"\nRun date : {today_str}")
    print(f"Fetching retail news from last {CUTOFF_DAYS} days …\n")

    all_articles = []
    for i, query in enumerate(SEARCH_QUERIES, 1):
        print(f"  [{i}/{len(SEARCH_QUERIES)}] {query[:60]}…")
        results = fetch_query(query, cutoff)
        print(f"          → {len(results)} article(s) found")
        all_articles.extend(results)
        time.sleep(1.5)  # polite delay between requests

    all_articles = deduplicate(all_articles)
    # Most recent first
    all_articles.sort(key=lambda a: a["date"], reverse=True)

    print(f"\n✅  {len(all_articles)} unique articles after deduplication")

    payload = {"last_updated": today_str, "data": all_articles}

    # Write latest snapshot (dashboard reads this)
    with open("daily_news_latest.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    # Write dated archive
    archive = f"daily_news_{today_str}.json"
    with open(archive, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"💾  Saved daily_news_latest.json  ({len(all_articles)} articles)")
    print(f"📁  Saved {archive}")


if __name__ == "__main__":
    main()
