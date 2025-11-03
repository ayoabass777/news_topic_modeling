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

from dotenv import load_dotenv
import os
import logging
from aiolimiter import AsyncLimiter
from typing import List, Optional
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.DEBUG,               # show debug, info, warning, error
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)


load_dotenv()


def _load_newsapi_config():
    url = os.getenv("NEWSAPI_URL")
    key = os.getenv("NEWSAPI_KEY")
    host = os.getenv("NEWSAPI_HOST")
    missing = [name for name, value in {
        "NEWSAPI_URL": url,
        "NEWSAPI_KEY": key,
        "NEWSAPI_HOST": host,
    }.items() if not value]
    if missing:
        missing_values = ", ".join(sorted(missing))
        raise RuntimeError(
            f"NewsAPI configuration error: missing {missing_values}. "
            "Set the variables in your environment or .env file."
        )
    return url, {"x-rapidapi-key": key, "x-rapidapi-host": host}


API_URL, API_HEADERS = _load_newsapi_config()
LANGUAGE_DEFAULT = "en"
LIMIT_PER_CALL = 50  #newsapi max page size
NEWSAPI_LIMITER = AsyncLimiter(10, 1)  # 10 requests per second


class NewsDataContract(BaseModel):
    """A single news article data item."""
    url: str
    title: Optional[str]
    excerpt: Optional[str]
    paywall: Optional[bool] = False
    authors: Optional[List[str]]
    keywords: List[str] = Field(default_factory=list)
    publisher_name: Optional[str]
    published_date: Optional[str]
    thumbnail: Optional[str]
    content_length: Optional[int]

    def get(self, key: str, default=None):
        return getattr(self, key, default)
    
    
class PaginationMetaContract(BaseModel):
    """metadata about pagination"""
    page: int = 0
    totalHits: Optional[int] = 0
    totalPages: Optional[int] = 0
    size: Optional[int] = LIMIT_PER_CALL

    def get(self, key: str, default=None):
        return getattr(self, key, default)
    
class PageOfNewsDataContract(BaseModel):
    """ A page of news data results with metadata."""
    data: List[NewsDataContract] = Field(default_factory=list)
    metadata: PaginationMetaContract = Field(default_factory=PaginationMetaContract)
    


def _as_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v if x is not None]
    return [str(v)]


async def discover_newsapi_page(
    client,
    *,
    query: str,
    page: int
):
    """Fetch a single page of results and map to DiscoveredItem list.

    Parameters
    ----------
    client : httpx.AsyncClient
        Pre-configured async HTTP client (with timeouts/headers as desired).
    query : str
        Search query string.
    page : int
        1-based page index.

    Returns
    -------
    PageOfNewsDataContract - is a pydantic model with:
        List[NewsDataContract]
        PaginationMetaContract
    """
    params = {
        "query": query,
        "page": page,
        "language": LANGUAGE_DEFAULT,
    }


    try:
        async with NEWSAPI_LIMITER:
            r = await client.get(API_URL, params=params, headers=API_HEADERS)
    except Exception as e:
        logging.error("newsapi: request failed on page %s – %s", page, e)
        return PageOfNewsDataContract()

    if r.status_code != 200:
        logging.warning("newsapi: non-200 status=%s page=%s", r.status_code, page)
        return PageOfNewsDataContract()

    try:
        data = r.json()
    except Exception as e:
        logging.error("newsapi: JSON parse error on page %s – %s", page, e)
        return PageOfNewsDataContract()


   
    total_hits = data.get("totalHits", 0)
    total_pages = data.get("totalPages", 0)
    size = data.get("size", LIMIT_PER_CALL)
    if not total_pages and total_hits and size:
        total_pages = (int(total_hits) + int(size) - 1) // int(size)

    metadata =  PaginationMetaContract(
        page=page,
        totalHits=total_hits,
        totalPages=total_pages,
        size=size
    )
   
    discovery = data.get("data", None)

    items =[]
    if not discovery:
        return PageOfNewsDataContract(data=items, metadata=metadata)

    for item in discovery:
        try:
            url = item.get("url")
            title = item.get("title", "No title")
            excerpt = item.get("excerpt", "")
            paywall = item.get("paywall", False)  # bool
            date_iso = item.get("date", None)
            pub = item.get("publisher", {})
            publisher_name = pub.get("name", None)
            authors = _as_list(item.get("authors"))
            keywords = _as_list(item.get("keywords"))
            thumbnail = item.get("thumbnail")
            content_length_raw = item.get("contentLength") or item.get("content_length")
            try:
                content_length = int(content_length_raw) if content_length_raw is not None else None
            except (TypeError, ValueError):
                content_length = None

            news_data = NewsDataContract(
                url=url,
                title=title,
                excerpt=excerpt,
                paywall=paywall,
                authors=authors,
                keywords=keywords,
                publisher_name=publisher_name,
                published_date=date_iso,
                thumbnail=thumbnail,
                content_length=content_length,
            )

            items.append(news_data)
        except Exception as e:
            logging.debug("newsapi: skipping malformed article on page %s – %s", page, e)
            continue

    
    #logging.info("newsapi: page=%s page_size=%s total_pages=%s total_hits=%s items=%s", meta.get("page"), meta.get("page_size"), meta.get("total_pages"), meta.get("total_hits"), len(items))
    return PageOfNewsDataContract(data=items, metadata=metadata)
