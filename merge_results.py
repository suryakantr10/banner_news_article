"""
Merges docs/news_data_opening.json and docs/news_data_closing.json
into docs/news_data.json. Run by the GitHub Actions merge job after
both scraper jobs complete.
"""

import json
import os
from datetime import datetime, timezone

DOCS        = "docs"
INPUTS      = ["news_data_opening.json", "news_data_closing.json"]
OUTPUT      = "news_data.json"

articles = []
for fname in INPUTS:
    path = os.path.join(DOCS, fname)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            articles.extend(json.load(f).get("articles", []))
        print(f"  Loaded {path}")
    else:
        print(f"  Warning: {path} not found — skipping")

# Dedup by title (case-insensitive), keep highest relevance_score
seen: dict = {}
for art in articles:
    key = art.get("title", "").lower().strip()
    if key not in seen or art.get("relevance_score", 0) > seen[key].get("relevance_score", 0):
        seen[key] = art

merged = sorted(
    seen.values(),
    key=lambda x: (x.get("relevance_score", 0), x.get("published_date") or ""),
    reverse=True,
)

os.makedirs(DOCS, exist_ok=True)
output_path = os.path.join(DOCS, OUTPUT)
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(
        {"generated": datetime.now(timezone.utc).isoformat(), "articles": merged},
        f, indent=2, ensure_ascii=False,
    )

print(f"Merged {len(merged)} unique articles → {output_path}")
