"""
FastAPI web application for News Intelligence.
"""
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.services.semantic_search_service import SemanticSearchService
from src.utils.logger import get_logger


logger = get_logger("news.web")
app = FastAPI(title="News Intelligence API", version="1.0.0")

static_dir = Path(__file__).parent / "web" / "static"
static_dir.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

DB_PATH = Path(__file__).parent / "src" / "data" / "articles.db"


# ── Pydantic models ────────────────────────────────────────────────────────────

class ArticleResponse(BaseModel):
    url: str
    title: Optional[str] = None
    content: Optional[str] = None
    publisher_name: Optional[str] = None
    published_date: Optional[str] = None
    content_id: Optional[str] = None
    bucket: Optional[str] = None
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
    bucket: Optional[str] = None
    signal_type: Optional[str] = None
    source_type: Optional[str] = None


# ── Semantic search service (lazy-loaded) ──────────────────────────────────────

_semantic_service: Optional[SemanticSearchService] = None


def _get_semantic_service() -> SemanticSearchService:
    global _semantic_service
    if _semantic_service is None:
        _semantic_service = SemanticSearchService(db_path=DB_PATH, lazy_model=True)
    return _semantic_service


# ── SQLite helpers ─────────────────────────────────────────────────────────────

@contextmanager
def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _to_datetime(value) -> datetime | None:
    if value in (None, ""):
        return None
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


def _row_to_article(row: sqlite3.Row, *, truncate: bool = False) -> ArticleResponse:
    content = row["content"]
    if truncate and content and len(str(content)) > 500:
        content = str(content)[:500] + "..."
    return ArticleResponse(
        url=row["url"],
        title=row["title"],
        content=content,
        publisher_name=row["publisher_name"],
        published_date=str(row["date"]) if row["date"] else None,
        content_id=row["content_id"],
        bucket=row["bucket"],
        signal_type=row["signal_type"],
        source_type=row["source_type"],
        source_feed=row["source_feed"],
    )


def _build_where(
    bucket: str | None,
    signal_type: str | None,
    publisher_name: str | None,
    date_from: str | None,
    date_to: str | None,
) -> tuple[str, list]:
    clauses, params = [], []
    if bucket:
        clauses.append("bucket = ?")
        params.append(bucket)
    if signal_type:
        clauses.append("signal_type = ?")
        params.append(signal_type)
    if publisher_name:
        clauses.append("LOWER(publisher_name) LIKE ?")
        params.append(f"%{publisher_name.lower()}%")
    from_dt = _to_datetime(date_from)
    to_dt = _to_datetime(date_to)
    if from_dt:
        clauses.append("date >= ?")
        params.append(from_dt.isoformat())
    if to_dt:
        to_dt = to_dt + timedelta(days=1)
        clauses.append("date < ?")
        params.append(to_dt.isoformat())
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    html_path = Path(__file__).parent / "web" / "index.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text(), media_type="text/html")
    return HTMLResponse(content="<h1>News Intelligence API</h1><p>Frontend not found.</p>")


@app.get("/api/filters")
async def get_filters():
    """Return distinct buckets and signal types available in the database."""
    try:
        with _get_conn() as conn:
            buckets = [r[0] for r in conn.execute(
                "SELECT DISTINCT bucket FROM meta WHERE bucket IS NOT NULL ORDER BY bucket"
            ).fetchall()]
            signals = [r[0] for r in conn.execute(
                "SELECT DISTINCT signal_type FROM meta WHERE signal_type IS NOT NULL ORDER BY signal_type"
            ).fetchall()]
        return {"buckets": buckets, "signal_types": signals}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/articles", response_model=List[ArticleResponse])
async def get_articles(
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    bucket: Optional[str] = Query(None),
    signal_type: Optional[str] = Query(None),
    publisher_name: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    try:
        where, params = _build_where(bucket, signal_type, publisher_name, date_from, date_to)
        sql = f"SELECT * FROM meta {where} ORDER BY date DESC LIMIT ? OFFSET ?"
        params += [limit, offset]
        with _get_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_article(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading articles: {str(e)}")


@app.get("/api/articles/search", response_model=List[ArticleResponse])
async def search_articles(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
    bucket: Optional[str] = Query(None),
    signal_type: Optional[str] = Query(None),
    publisher_name: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    try:
        kw = f"%{q.lower()}%"
        where, params = _build_where(bucket, signal_type, publisher_name, date_from, date_to)
        keyword_clause = "(LOWER(title) LIKE ? OR LOWER(content) LIKE ?)"
        if where:
            full_where = f"{where} AND {keyword_clause}"
        else:
            full_where = f"WHERE {keyword_clause}"
        all_params = params + [kw, kw, limit]
        sql = f"SELECT * FROM meta {full_where} ORDER BY date DESC LIMIT ?"
        with _get_conn() as conn:
            rows = conn.execute(sql, all_params).fetchall()
        return [_row_to_article(row, truncate=True) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")


@app.get("/api/articles/semantic-search", response_model=List[SemanticSearchResult])
async def semantic_search_articles(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
    min_similarity: float = Query(0.25, ge=0.0, le=1.0),
    bucket: Optional[str] = Query(None),
    signal_type: Optional[str] = Query(None),
):
    try:
        service = _get_semantic_service()
        results = service.search(
            query=q,
            limit=limit,
            min_similarity=min_similarity,
            league=bucket,
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
                bucket=r.get("bucket") or r.get("league"),
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
    try:
        return _get_semantic_service().stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats")
async def get_stats():
    try:
        with _get_conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM meta").fetchone()[0]

            publishers = conn.execute(
                "SELECT publisher_name, COUNT(*) as c FROM meta WHERE publisher_name IS NOT NULL "
                "GROUP BY publisher_name ORDER BY c DESC LIMIT 5"
            ).fetchall()

            by_bucket = conn.execute(
                "SELECT bucket, COUNT(*) as c FROM meta WHERE bucket IS NOT NULL GROUP BY bucket"
            ).fetchall()

            by_signal = conn.execute(
                "SELECT signal_type, COUNT(*) as c FROM meta WHERE signal_type IS NOT NULL GROUP BY signal_type"
            ).fetchall()

            date_range = conn.execute(
                "SELECT MIN(date), MAX(date) FROM meta WHERE date IS NOT NULL"
            ).fetchone()

            timeline = conn.execute(
                "SELECT DATE(date) as day, COUNT(*) as c FROM meta WHERE date IS NOT NULL "
                "GROUP BY day ORDER BY day DESC LIMIT 14"
            ).fetchall()

        return {
            "total_articles": total,
            "unique_publishers": len(publishers),
            "date_range": {
                "earliest": date_range[0],
                "latest": date_range[1],
            },
            "by_bucket": {r["bucket"]: r["c"] for r in by_bucket},
            "by_signal_type": {r["signal_type"]: r["c"] for r in by_signal},
            "top_publishers": [
                {"publisher_name": r["publisher_name"], "count": r["c"]} for r in publishers
            ],
            "recent_timeline": [
                {"date": r["day"], "count": r["c"]} for r in reversed(timeline)
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stats error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
