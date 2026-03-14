from __future__ import annotations

from urllib.parse import quote_plus


LEAGUE_CONFIG = {
    "premier_league": {
        "label": "Premier League",
        "query": (
            '"Premier League" OR EPL OR "English Premier League" OR Arsenal '
            'OR Chelsea OR Liverpool OR "Manchester City" OR "Manchester United" '
            'OR Tottenham OR Newcastle'
        ),
        "feeds": [
            {
                "name": "The Guardian Premier League",
                "url": "https://www.theguardian.com/football/premierleague/rss",
            },
            {
                "name": "Google News Premier League",
                "url": "https://news.google.com/rss/search?q="
                + quote_plus('"Premier League" OR EPL')
                + "&hl=en-US&gl=US&ceid=US:en",
            },
        ],
    },
    "serie_a": {
        "label": "Serie A",
        "query": (
            '"Serie A" OR "Italian football" OR Juventus OR Inter OR Milan '
            'OR Napoli OR Roma OR Lazio'
        ),
        "feeds": [
            {
                "name": "The Guardian Serie A",
                "url": "https://www.theguardian.com/football/serieafootball/rss",
            },
            {
                "name": "Google News Serie A",
                "url": "https://news.google.com/rss/search?q="
                + quote_plus('"Serie A" OR Juventus OR Inter OR Milan')
                + "&hl=en-US&gl=US&ceid=US:en",
            },
        ],
    },
    "la_liga": {
        "label": "La Liga",
        "query": (
            '"La Liga" OR "Spanish football" OR "Real Madrid" OR Barcelona '
            'OR "Atletico Madrid" OR Sevilla OR Valencia'
        ),
        "feeds": [
            {
                "name": "The Guardian La Liga",
                "url": "https://www.theguardian.com/football/laligafootball/rss",
            },
            {
                "name": "Google News La Liga",
                "url": "https://news.google.com/rss/search?q="
                + quote_plus('"La Liga" OR "Real Madrid" OR Barcelona')
                + "&hl=en-US&gl=US&ceid=US:en",
            },
        ],
    },
}

TRANSFER_TERMS = {
    "transfer",
    "transfers",
    "signing",
    "signings",
    "loan",
    "loans",
    "bid",
    "bids",
    "deal",
    "deals",
    "medical",
    "window",
    "contract",
    "release clause",
}

TREND_TERMS = {
    "trend",
    "trends",
    "trending",
    "form",
    "streak",
    "standings",
    "table",
    "title race",
    "relegation",
    "momentum",
    "surge",
    "slump",
    "run",
    "race",
}

FOOTBALL_TERMS = {
    "football",
    "soccer",
    "premier league",
    "serie a",
    "la liga",
    "arsenal",
    "chelsea",
    "liverpool",
    "manchester city",
    "manchester united",
    "tottenham",
    "newcastle",
    "juventus",
    "inter",
    "milan",
    "napoli",
    "roma",
    "lazio",
    "real madrid",
    "barcelona",
    "atletico madrid",
    "sevilla",
    "valencia",
}


def _as_text_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if item is not None]
    if hasattr(value, "tolist"):
        converted = value.tolist()
        if isinstance(converted, list):
            return [str(item) for item in converted if item is not None]
    return [str(value)]


def league_label(league: str | None) -> str | None:
    if not league:
        return None
    return LEAGUE_CONFIG.get(league, {}).get("label")


def classify_signal(*parts: object) -> str:
    haystack = " ".join(str(part or "").lower() for part in parts)
    if any(term in haystack for term in TRANSFER_TERMS):
        return "transfers"
    if any(term in haystack for term in TREND_TERMS):
        return "trends"
    return "general_news"


def is_football_article(article: dict) -> bool:
    league = article.get("league") or article.get("bucket")
    if league in LEAGUE_CONFIG:
        return True
    haystack = " ".join(
        str(article.get(field, "")).lower()
        for field in ("title", "excerpt", "content", "publisher_name")
    )
    keywords = _as_text_list(article.get("keywords"))
    haystack = f"{haystack} {' '.join(str(keyword).lower() for keyword in keywords)}"
    return any(term in haystack for term in FOOTBALL_TERMS)
