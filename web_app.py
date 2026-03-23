"""
FastAPI web application for Football News Intelligence.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.services.semantic_search_service import SemanticSearchService
from src.utils.football import LEAGUE_CONFIG, league_label
from src.utils.logger import get_logger


logger = get_logger("football.web")
app = FastAPI(title="Football News Intelligence API", version="1.0.0")

static_dir = Path(__file__).parent / "web" / "static"
static_dir.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

DATA_DIRS = (
    Path(__file__).parent / "src" / "data",
    Path(__file__).parent / "data" / "silver_parquet",
)
LEAGUES = tuple(LEAGUE_CONFIG.keys())


class ArticleResponse(BaseModel):
    url: str
    title: Optional[str]
    content: Optional[str]
    publisher_name: Optional[str]
    publisher_domain: Optional[str]
    published_date: Optional[str]
    crawl_ts: Optional[str]
    content_id: Optional[str]
    excerpt: Optional[str]
    authors: List[str] = []
    keywords: List[str] = []
    bucket: Optional[str] = None
    league: Optional[str] = None
    league_label: Optional[str] = None
    signal_type: Optional[str] = None
    source_type: Optional[str] = None
    source_feed: Optional[str] = None



class SemanticSearchResult(BaseModel):
    url: str
    title: Optional[str] = None
    content: Optional[str] = None
    publisher_name: Optional[str] = None
    published_date: Optional[str] = None
    content_id: Optional[str] = None
    similarity: float
    league: Optional[str] = None
    league_label: Optional[str] = None
    signal_type: Optional[str] = None
    source_type: Optional[str] = None


# Lazy-initialised: model loads on first search request, not at startup
_semantic_service: Optional[SemanticSearchService] = None


def _get_semantic_service() -> SemanticSearchService:
    global _semantic_service
    if _semantic_service is None:
        db_path = Path(__file__).parent / "src" / "data" / "articles.db"
        _semantic_service = SemanticSearchService(db_path=db_path, lazy_model=True)
    return _semantic_service


def _candidate_parquet_files() -> list[Path]:
    files: list[Path] = []
    for directory in DATA_DIRS:
        if not directory.exists():
            continue
        files.extend(sorted(directory.rglob("*.parquet")))
    return files


def _to_datetime(value) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except ValueError:
        return None


def _article_timestamp(article: dict) -> datetime | None:
    return _to_datetime(article.get("published_date")) or _to_datetime(article.get("crawl_ts"))


def _normalize_article(article: dict) -> dict:
    normalized = dict(article)
    normalized["league"] = normalized.get("league") or normalized.get("bucket")
    normalized["league_label"] = league_label(normalized.get("league"))
    authors = normalized.get("authors")
    keywords = normalized.get("keywords")
    if hasattr(authors, "tolist"):
        authors = authors.tolist()
    if hasattr(keywords, "tolist"):
        keywords = keywords.tolist()
    normalized["authors"] = authors if isinstance(authors, list) else []
    normalized["keywords"] = keywords if isinstance(keywords, list) else []
    if normalized.get("published_date") is not None:
        normalized["published_date"] = str(normalized["published_date"])
    if normalized.get("crawl_ts") is not None:
        normalized["crawl_ts"] = str(normalized["crawl_ts"])
    return normalized


def load_articles_from_parquet(limit: int | None = None) -> List[dict]:
    articles: list[dict] = []
    for parquet_file in _candidate_parquet_files():
        try:
            df = pd.read_parquet(parquet_file)
        except Exception as e:
            logger.warning("web: failed to load %s (%s)", parquet_file, e)
            continue
        for record in df.to_dict("records"):
            normalized = _normalize_article(record)
            if normalized.get("league") in LEAGUES:
                articles.append(normalized)
                if limit is not None and len(articles) >= limit:
                    return articles
    articles.sort(key=lambda article: _article_timestamp(article) or datetime.min, reverse=True)
    return articles


def filter_articles(
    articles: Iterable[dict],
    *,
    league: str | None = None,
    signal_type: str | None = None,
    publisher_name: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    from_dt = _to_datetime(date_from)
    to_dt = _to_datetime(date_to)
    if to_dt is not None:
        to_dt = to_dt + timedelta(days=1)

    filtered = []
    for article in articles:
        if league and article.get("league") != league:
            continue
        if signal_type and article.get("signal_type") != signal_type:
            continue
        if publisher_name and publisher_name.lower() not in str(article.get("publisher_name", "")).lower():
            continue
        article_dt = _article_timestamp(article)
        if from_dt is not None and (article_dt is None or article_dt < from_dt):
            continue
        if to_dt is not None and (article_dt is None or article_dt >= to_dt):
            continue
        filtered.append(article)
    filtered.sort(key=lambda article: _article_timestamp(article) or datetime.min, reverse=True)
    return filtered


def _article_response(article: dict, *, truncate_content: bool = False) -> ArticleResponse:
    content = article.get("content")
    if truncate_content and content:
        content = f"{str(content)[:500]}..." if len(str(content)) > 500 else content
    return ArticleResponse(
        url=article.get("url", ""),
        title=article.get("title"),
        content=content,
        publisher_name=article.get("publisher_name"),
        publisher_domain=article.get("publisher_domain"),
        published_date=article.get("published_date"),
        crawl_ts=article.get("crawl_ts"),
        content_id=article.get("content_id"),
        excerpt=article.get("excerpt"),
        authors=article.get("authors", []),
        keywords=article.get("keywords", []),
        bucket=article.get("bucket"),
        league=article.get("league"),
        league_label=article.get("league_label"),
        signal_type=article.get("signal_type"),
        source_type=article.get("source_type"),
        source_feed=article.get("source_feed"),
    )


@app.get("/", response_class=HTMLResponse)
async def root():
    html_path = Path(__file__).parent / "web" / "index.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text(), media_type="text/html")
    return HTMLResponse(
        content="<h1>Football News Intelligence API</h1><p>Frontend not found. Please check web/index.html</p>",
        media_type="text/html",
    )


@app.get("/api/articles", response_model=List[ArticleResponse])
async def get_articles(
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    league: Optional[str] = Query(None),
    signal_type: Optional[str] = Query(None),
    publisher_name: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    try:
        all_articles = load_articles_from_parquet()
        filtered = filter_articles(
            all_articles,
            league=league,
            signal_type=signal_type,
            publisher_name=publisher_name,
            date_from=date_from,
            date_to=date_to,
        )
        return [_article_response(article) for article in filtered[offset:offset + limit]]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading articles: {str(e)}")


@app.get("/api/articles/search", response_model=List[ArticleResponse])
async def search_articles(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
    league: Optional[str] = Query(None),
    signal_type: Optional[str] = Query(None),
    publisher_name: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    try:
        all_articles = load_articles_from_parquet(limit=3000)
        filtered = filter_articles(
            all_articles,
            league=league,
            signal_type=signal_type,
            publisher_name=publisher_name,
            date_from=date_from,
            date_to=date_to,
        )
        query_lower = q.lower()
        matched = []
        for article in filtered:
            text = " ".join(
                [
                    str(article.get("title", "")),
                    str(article.get("content", "")),
                    str(article.get("excerpt", "")),
                    " ".join(article.get("keywords", [])),
                ]
            ).lower()
            if query_lower in text:
                matched.append(article)
                if len(matched) >= limit:
                    break
        return [_article_response(article, truncate_content=True) for article in matched]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching articles: {str(e)}")


@app.get("/api/articles/semantic-search", response_model=List[SemanticSearchResult])
async def semantic_search_articles(
    q: str = Query(..., min_length=1, description="Natural-language search query"),
    limit: int = Query(10, ge=1, le=50),
    min_similarity: float = Query(0.25, ge=0.0, le=1.0),
    league: Optional[str] = Query(None),
    signal_type: Optional[str] = Query(None),
):
    """
    Semantic search: finds articles by *meaning*, not just keywords.

    Example: searching "transfer rumours" will also surface articles about
    "player swap deals" or "signing negotiations" — because the embeddings
    capture that these concepts are semantically close.
    """
    try:
        service = _get_semantic_service()
        results = service.search(
            query=q,
            limit=limit,
            min_similarity=min_similarity,
            league=league,
            signal_type=signal_type,
        )
        return [
            SemanticSearchResult(
                url=r.get("url", ""),
                title=r.get("title"),
                content=(str(r.get("content", ""))[:500] + "...") if r.get("content") and len(str(r.get("content", ""))) > 500 else r.get("content"),
                publisher_name=r.get("publisher_name"),
                published_date=str(r.get("date") or r.get("published_date") or ""),
                content_id=r.get("content_id"),
                similarity=r["similarity"],
                league=r.get("league") or r.get("bucket"),
                league_label=league_label(r.get("league") or r.get("bucket")),
                signal_type=r.get("signal_type"),
                source_type=r.get("source_type"),
            )
            for r in results
        ]
    except Exception as e:
        logger.exception("semantic_search endpoint error")
        raise HTTPException(status_code=500, detail=f"Semantic search error: {str(e)}")


@app.get("/api/semantic-search/stats")
async def semantic_search_stats():
    """Return stats about the semantic search index (embedding count, model, etc)."""
    try:
        service = _get_semantic_service()
        return service.stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stats error: {str(e)}")



@app.get("/api/stats")
async def get_stats():
    try:
        all_articles = load_articles_from_parquet(limit=5000)
        total_articles = len(all_articles)
        unique_publishers = sorted(
            {article.get("publisher_name") for article in all_articles if article.get("publisher_name")}
        )
        date_values = [timestamp for article in all_articles if (timestamp := _article_timestamp(article))]
        league_counts = {
            league: len([article for article in all_articles if article.get("league") == league])
            for league in LEAGUES
        }
        signal_counts = {}
        for article in all_articles:
            signal = article.get("signal_type") or "general_news"
            signal_counts[signal] = signal_counts.get(signal, 0) + 1

        publisher_counts = {}
        for article in all_articles:
            publisher = article.get("publisher_name")
            if publisher:
                publisher_counts[publisher] = publisher_counts.get(publisher, 0) + 1
        top_publishers = sorted(
            publisher_counts.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:5]

        timeline_counts = {}
        for article in all_articles:
            timestamp = _article_timestamp(article)
            if timestamp is None:
                continue
            day = timestamp.date().isoformat()
            timeline_counts[day] = timeline_counts.get(day, 0) + 1
        recent_timeline = [
            {"date": day, "count": timeline_counts[day]}
            for day in sorted(timeline_counts.keys())[-14:]
        ]

        return {
            "total_articles": total_articles,
            "unique_publishers": len(unique_publishers),
            "date_range": {
                "earliest": min(date_values).isoformat() if date_values else None,
                "latest": max(date_values).isoformat() if date_values else None,
            },
            "by_league": {
                league: {"count": count, "label": league_label(league)}
                for league, count in league_counts.items()
            },
            "by_signal_type": signal_counts,
            "top_publishers": [
                {"publisher_name": publisher, "count": count}
                for publisher, count in top_publishers
            ],
            "recent_timeline": recent_timeline,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting stats: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
