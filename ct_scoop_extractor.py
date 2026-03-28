#!/usr/bin/env python3
"""
CT Scoop Article Fetcher
Reads ct_scoop_latest.json, fetches each article body, and saves the
combined text to ct_scoop_articles_ready.txt — ready to paste into Claude
for manual extraction.

Usage:
    python ct_scoop_extractor.py
"""

import json
import time
from datetime import date
from pathlib import Path

import requests
from bs4 import BeautifulSoup


def fetch_article_text(url: str, max_chars: int = 2500) -> str:
    """Fetch an article page and return its plain-text body."""
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for tag in soup(["script", "style", "nav", "footer", "header", "aside", "form"]):
            tag.decompose()

        body = (
            soup.find("article")
            or soup.find("main")
            or soup.find("div", class_=lambda c: c and "content" in c.lower())
            or soup
        )
        return body.get_text(separator=" ", strip=True)[:max_chars]

    except Exception as exc:
        return f"[Error fetching article: {exc}]"


def save_extraction_json(data: list[dict], non_working: list[dict] | None = None):
    """Save extraction results to ct_scoop_extraction_latest.json."""
    payload = {
        "last_updated": date.today().isoformat(),
        "data": data,
        "non_working": non_working or [],
    }
    out = Path("ct_scoop_extraction_latest.json")
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✓ Saved {len(data)} rows to {out}")


def main():
    scoop_path = Path("ct_scoop_latest.json")
    if not scoop_path.exists():
        print("ct_scoop_latest.json not found.")
        return

    with open(scoop_path, encoding="utf-8") as f:
        scoop_data = json.load(f)

    articles = scoop_data.get("data", [])
    if not articles:
        print("No articles found.")
        return

    print(f"Fetching {len(articles)} article(s)...\n")
    blocks = []
    for i, art in enumerate(articles, 1):
        print(f"  [{i}/{len(articles)}] {art['link']}")
        text = fetch_article_text(art["link"])
        blocks.append(
            f"--- Article {i} ---\n"
            f"URL: {art['link']}\n"
            f"Headline: {art['heading']}\n\n"
            f"{text}"
        )
        time.sleep(1)

    output = "\n\n".join(blocks)
    out_path = Path("ct_scoop_articles_ready.txt")
    out_path.write_text(output, encoding="utf-8")
    print(f"\n✓ Article text saved to {out_path}")
    print("  → Paste the contents into Claude with your extraction prompt.")


if __name__ == "__main__":
    main()
