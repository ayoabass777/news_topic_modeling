"""
Football-focused GraphRAG-style service.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import pandas as pd

from src.utils.football import LEAGUE_CONFIG, classify_signal, is_football_article, league_label
from src.utils.logger import get_logger


logger = get_logger("football.graphrag")
DEFAULT_DATA_DIRS = (
    Path(__file__).parent.parent.parent.parent / "src" / "data",
    Path(__file__).parent.parent.parent.parent / "data" / "silver_parquet",
)

QUERY_HINTS = {
    "premier_league": ("premier league", "epl", "arsenal", "chelsea", "liverpool", "manchester city"),
    "serie_a": ("serie a", "juventus", "inter", "milan", "napoli", "roma"),
    "la_liga": ("la liga", "real madrid", "barcelona", "atletico madrid", "sevilla"),
}


class GraphRAGService:
    def __init__(self, data_dirs: Optional[Sequence[Path]] = None):
        self.data_dirs = tuple(data_dirs) if data_dirs is not None else DEFAULT_DATA_DIRS
        self.articles_cache: list[dict] = []
        self._load_articles()

    def _load_articles(self, limit: int = 2500) -> None:
        self.articles_cache = []
        for data_dir in self.data_dirs:
            if not Path(data_dir).exists():
                continue
            for parquet_file in sorted(Path(data_dir).rglob("*.parquet")):
                try:
                    df = pd.read_parquet(parquet_file)
                except Exception as e:
                    logger.warning("graphrag: failed to load %s (%s)", parquet_file, e)
                    continue
                for record in df.to_dict("records"):
                    if is_football_article(record):
                        normalized = dict(record)
                        normalized["league"] = normalized.get("league") or normalized.get("bucket")
                        normalized["league_label"] = league_label(normalized.get("league"))
                        normalized["signal_type"] = normalized.get("signal_type") or classify_signal(
                            normalized.get("title"),
                            normalized.get("excerpt"),
                            normalized.get("content"),
                        )
                        self.articles_cache.append(normalized)
                        if len(self.articles_cache) >= limit:
                            return

    async def query(
        self,
        query: str,
        max_results: int = 10,
        league: str | None = None,
        signal_type: str | None = None,
    ) -> Dict:
        query_lower = query.lower()
        inferred_league = league or self._infer_league(query_lower)
        inferred_signal = signal_type or self._infer_signal(query_lower)

        matched_articles = []
        for article in self._filtered_articles(inferred_league, inferred_signal):
            title = str(article.get("title", "")).lower()
            content = str(article.get("content", "")).lower()
            excerpt = str(article.get("excerpt", "")).lower()
            raw_keywords = article.get("keywords", [])
            if hasattr(raw_keywords, "tolist"):
                raw_keywords = raw_keywords.tolist()
            keywords = " ".join(raw_keywords if isinstance(raw_keywords, list) else []).lower()
            match_score = sum(
                1 for term in query_lower.split()
                if term and (term in title or term in content or term in excerpt or term in keywords)
            )
            if inferred_league and article.get("league") == inferred_league:
                match_score += 2
            if inferred_signal and article.get("signal_type") == inferred_signal:
                match_score += 1
            if match_score > 0:
                ranked = dict(article)
                ranked["match_score"] = match_score
                matched_articles.append(ranked)

        matched_articles.sort(key=lambda article: article.get("match_score", 0), reverse=True)
        matched_articles = matched_articles[:max_results]

        return {
            "answer": self._generate_answer(query, matched_articles, inferred_league, inferred_signal),
            "sources": [self._serialize_article(article) for article in matched_articles],
            "entities": self._extract_entities(query, matched_articles),
            "relationships": self._extract_relationships(matched_articles),
        }

    def _filtered_articles(self, league: str | None, signal_type: str | None) -> Iterable[dict]:
        for article in self.articles_cache:
            if league and article.get("league") != league:
                continue
            if signal_type and article.get("signal_type") != signal_type:
                continue
            yield article

    def _infer_league(self, query_lower: str) -> str | None:
        for league, hints in QUERY_HINTS.items():
            if any(hint in query_lower for hint in hints):
                return league
        return None

    def _infer_signal(self, query_lower: str) -> str | None:
        signal = classify_signal(query_lower)
        return None if signal == "general_news" else signal

    def _extract_entities(self, query: str, articles: List[Dict]) -> List[str]:
        entities = []
        stop_words = {"the", "a", "an", "and", "or", "in", "on", "at", "to", "for", "of", "with", "latest"}
        entities.extend(
            word.capitalize()
            for word in query.split()
            if word.lower() not in stop_words and len(word) > 3
        )
        for article in articles[:5]:
            if article.get("league_label"):
                entities.append(article["league_label"])
            publisher = article.get("publisher_name")
            if publisher:
                entities.append(publisher)
        deduped = []
        for entity in entities:
            if entity not in deduped:
                deduped.append(entity)
        return deduped[:20]

    def _extract_relationships(self, articles: List[Dict]) -> List[Dict]:
        relationships = []
        for article in articles[:5]:
            league = article.get("league_label")
            signal = article.get("signal_type")
            publisher = article.get("publisher_name")
            if league and signal:
                relationships.append(
                    {
                        "source": league,
                        "relationship": "coverage_type",
                        "target": signal,
                        "confidence": 0.8,
                    }
                )
            if league and publisher:
                relationships.append(
                    {
                        "source": publisher,
                        "relationship": "covers",
                        "target": league,
                        "confidence": 0.7,
                    }
                )
        return relationships[:8]

    def _generate_answer(
        self,
        query: str,
        articles: List[Dict],
        league: str | None,
        signal_type: str | None,
    ) -> str:
        if not articles:
            return f"No football articles matched '{query}'. Try a league name, club, or transfer keyword."

        top_article = articles[0]
        league_text = league_label(league) if league else "all tracked leagues"
        signal_text = signal_type or "all football story types"
        publishers = sorted(
            {article.get("publisher_name") for article in articles if article.get("publisher_name")}
        )
        publisher_text = ", ".join(publishers[:3]) if publishers else "multiple sources"
        return (
            f"Found {len(articles)} football articles for '{query}' across {league_text}. "
            f"The strongest cluster is {signal_text}, led by '{top_article.get('title', 'Untitled')}'. "
            f"Coverage is coming from {publisher_text}."
        )

    def _serialize_article(self, article: dict) -> dict:
        payload = dict(article)
        if payload.get("published_date") is not None:
            payload["published_date"] = str(payload["published_date"])
        if payload.get("crawl_ts") is not None:
            payload["crawl_ts"] = str(payload["crawl_ts"])
        content = str(payload.get("content", ""))
        if len(content) > 500:
            payload["content"] = f"{content[:500]}..."
        return payload
