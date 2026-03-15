"""
Feed Configuration
==================

Single source of truth for all ingestion buckets and their RSS feeds.

Each bucket has:
  - domain     : top-level ingestion domain ("finance", "technology", "football")
  - label      : human-readable name
  - query      : NewsAPI search query string
  - feeds      : list of RSS feed dicts with {name, url}

Field semantics in the pipeline:
  - bucket  : always the domain (e.g. "finance", "football")
  - league  : football sub-classification only (e.g. "premier_league") — null for non-football

To add a new domain, add a new top-level key here. No other files need to change.
"""

from __future__ import annotations

from urllib.parse import quote_plus

from src.utils.football import FOOTBALL_BUCKETS

FINANCE_BUCKETS: dict = {
    "finance": {
        "domain": "finance",
        "label": "Finance",
        "query": (
            '"stock market" OR "interest rates" OR "Federal Reserve" OR inflation '
            'OR "earnings report" OR "IPO" OR "hedge fund" OR "private equity"'
        ),
        "feeds": [
            {
                "name": "Reuters Business",
                "url": "https://feeds.reuters.com/reuters/businessNews",
            },
            {
                "name": "Reuters Finance",
                "url": "https://feeds.reuters.com/reuters/financialsNews",
            },
            {
                "name": "Google News Finance",
                "url": "https://news.google.com/rss/search?q="
                + quote_plus("stock market OR inflation OR interest rates OR earnings")
                + "&hl=en-US&gl=US&ceid=US:en",
            },
        ],
    },
}

TECHNOLOGY_BUCKETS: dict = {
    "technology": {
        "domain": "technology",
        "label": "Technology",
        "query": (
            '"artificial intelligence" OR "machine learning" OR "large language model" '
            'OR startup OR "venture capital" OR "open source" OR "cloud computing"'
        ),
        "feeds": [
            {
                "name": "TechCrunch",
                "url": "https://techcrunch.com/feed/",
            },
            {
                "name": "Hacker News",
                "url": "https://hnrss.org/frontpage",
            },
            {
                "name": "Wired",
                "url": "https://www.wired.com/feed/rss",
            },
            {
                "name": "Google News Technology",
                "url": "https://news.google.com/rss/search?q="
                + quote_plus(
                    "artificial intelligence OR machine learning OR startup OR cloud"
                )
                + "&hl=en-US&gl=US&ceid=US:en",
            },
        ],
    },
}

# Master config — all active ingestion buckets
# Add new domain dicts here to expand coverage
BUCKET_CONFIG: dict = {
    **FINANCE_BUCKETS,
    **TECHNOLOGY_BUCKETS,
    **FOOTBALL_BUCKETS,
}
