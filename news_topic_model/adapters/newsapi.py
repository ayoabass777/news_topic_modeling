"""NewsAPI-style URL discovery adapter.

Maps provider fields into the internal DiscoveredItem contract so the rest of
the pipeline can remain unchanged.

Available fields (per user): title, url, excerpt, thumbnail, language, paywall,
content length, date (ISO), authors, keywords, publisher {name,url,favicon}.
Also paging/meta: success, size, totalHits, hitsPerPage, page, totalPages, timeMs.

Two entry points:
- discover_newsapi_page(...)  -> returns one page of DiscoveredItem + meta
- discover_newsapi_paged(...) -> async generator yielding pages
"""
from __future__ import annotations

from typing import Any, AsyncGenerator, Dict, Iterable, List, Optional, Tuple
import logging
import datetime as dt
from urllib.parse import urlparse
from aiolimiter import AsyncLimiter
NEWSAPI_LIMITER = AsyncLimiter(10, 1)  # 10 requests per second

import httpx

from news_topic_model.contract import DiscoveredItem
from news_topic_model.utils.urlnorm import normalize_url

logger = logging.getLogger("news_topic_model.adapters.newsapi")


def _domain_from_url(u: str) -> str:
    try:
        return urlparse(u).hostname or ""
    except Exception:
        return ""



def _as_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v if x is not None]
    return [str(v)]


async def discover_newsapi_page(
    client: httpx.AsyncClient,
    *,
    api_url: str,
    api_headers: Optional[Dict[str, str]] = None,
    query: str,
    page: int = 1,
    page_size: int = 50,
    language: Optional[str] = "en",
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> Tuple[List[DiscoveredItem], Dict[str, Any]]:
    """Fetch a single page of results and map to DiscoveredItem list.

    Parameters
    ----------
    client : httpx.AsyncClient
        Pre-configured async HTTP client (with timeouts/headers as desired).
    api_url : str
        Full endpoint URL (e.g., RapidAPI proxy URL for your News API provider).
    api_headers : dict | None
        Headers required by the provider (e.g., API key, Host).
    query : str
        Search query string.
    page : int
        1-based page index.
    page_size : int
        Number of items per page (provider caps often at 20).
    language : str | None
        Language filter (e.g., "en").
    since / until : str | None
        Optional time bounds (ISO 8601). Forwarded as `from`/`to` when provided.

    Returns
    -------
    (items, meta)
        items: list[DiscoveredItem]
        meta: {"page", "page_size", "total_pages", "total_hits"}
    """
    params: Dict[str, Any] = {
        "q": query,
        "page": page,
        # Many providers use either `pageSize` or `hitsPerPage`; set both defensively.
        "pageSize": page_size,
        "hitsPerPage": page_size,
    }
    if language:
        params["language"] = language
    if since:
        params["from"] = since
    if until:
        params["to"] = until

   
    try:
        async with NEWSAPI_LIMITER:
            r = await client.get(api_url, params=params, headers=api_headers)
    except Exception as e:
        logger.error("newsapi: request failed on page %s – %s", page, e)
        return [], {"page": page, "page_size": page_size, "total_pages": 0, "total_hits": 0}

    if r.status_code != 200:
        logger.warning("newsapi: non-200 status=%s page=%s", r.status_code, page)
        return [], {"page": page, "page_size": page_size, "total_pages": 0, "total_hits": 0}

    try:
        data = r.json()
    except Exception as e:
        logger.error("newsapi: JSON parse error on page %s – %s", page, e)
        return [], {"page": page, "page_size": page_size, "total_pages": 0, "total_hits": 0}

   
    total_hits = data.get("totalHits", 0)
    total_pages = data.get("totalPages", 0)
    size = data.get("hitsPerPage", 0) 
   
    discovery = data.get("data", None)

    items: List[DiscoveredItem] = []
    if not discovery:
        return items, {"page": page, "page_size": int(size), "total_pages": int(total_pages or 0), "total_hits": int(total_hits or 0)}

    for item in discovery:
        try:
            raw_url: str = item.get("url")
            if not raw_url:
                continue
            norm = normalize_url(raw_url)
            #do i need to check if norm is None?
            if not norm:
                continue

            title = item.get("title")
            excerpt = item.get("excerpt")
            thumb = item.get("thumbnail") 
            lang = item.get("language")
            paywall = item.get("paywall") #bool
            content_len = item.get("contentLength")
            date_iso = item.get("date")
            keywords = item.get("keywords", [])

            pub = item.get("publisher", {})
            publisher_name = pub.get("name",None) 
            publisher_url = pub.get("url", None) 
            publisher_favicon = pub.get("favicon", None) 
            publisher_domain = _domain_from_url(publisher_url or raw_url)

            authors = _as_list(item.get("authors"))
            keywords = _as_list(item.get("keywords"))

            items.append(
                DiscoveredItem(
                    title=title,
                    url=norm,
                    excerpt=excerpt,
                    thumbnail=thumb,
                    language=lang,
                    paywall=bool(paywall) if isinstance(paywall, bool) else None,
                    content_length=int(content_len) if isinstance(content_len, int) else None,
                    published_at=date_iso,
                    authors=authors,
                    keywords=keywords,
                    source_name=publisher_name,
                    source_domain=publisher_domain,
                    source_url=publisher_url,
                    source_favicon=publisher_favicon,
                    raw=item,
                )
            )
        except Exception as e:
            logger.debug("newsapi: skipping malformed article on page %s – %s", page, e)
            continue

    if not total_pages and total_hits and size:
        total_pages = (int(total_hits) + int(size) - 1) // int(size)

    meta = {
        "page": page,
        "page_size": int(size),
        "total_pages": int(total_pages or 0),
        "total_hits": int(total_hits or 0),
    }
    return items, meta


async def discover_newsapi_paged(
    *,
    api_url: str,
    api_headers: Optional[Dict[str, str]] = None,
    query: str,
    page_size: int = 50,
    language: Optional[str] = "en",
    since: Optional[str] = None,
    until: Optional[str] = None,
    max_pages: int = 5,
) -> AsyncGenerator[List[DiscoveredItem], None]:
    """Yield pages of `DiscoveredItem` by iterating provider pagination.

    `max_pages` acts as a safety guard for broad queries.
    """

    
    async with httpx.AsyncClient(follow_redirects=True) as client:
        page = 1
        total_pages: Optional[int] = None
        while page <= max_pages and (total_pages is None or page <= total_pages):
            try:
                items, meta = await discover_newsapi_page(
                    client,
                    api_url=api_url,
                    api_headers=api_headers,
                    query=query,
                    page=page,
                    page_size=page_size,
                    language=language,
                    since=since,
                    until=until,
                )
            except Exception as e:
                logger.error("newsapi: request failed on page %s – %s", page, e)
                break
            if not items:
                if page == 1:
                    logger.info("newsapi: no results for query=%r", query)
                break

            yield items
            total_pages = meta.get("total_pages") or total_pages
            page += 1
