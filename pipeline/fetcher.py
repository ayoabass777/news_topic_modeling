"""
producer_msg -> UrlDiscovered
UrlDiscovered is one of discovery
Discovery is an object that contains (at minimum):
#     - url: str
#     - paywall: bool
#     - title: str
#     - excerpt: str
# Optional additions from NewsDataContract:
#     - authors: List[str]
#     - thumbnail: Optional[str]
#     - content_length: Optional[int]
#     - keywords: List[str]
#     - published_date: Optional[str]
#     - publisher_name: Optional[str]
#     - extracted_content_length: int (added by fetcher)
#     - bucket: Optional[str]

if paywall is True, then we use excerpt as content
else we fetch html from url using trailfutura to extract text
then we create ArticleFetched object and send to content_queue

ArticleFetched is a message that contains:
#     - url: str (used to identify the discovered URL in database)
#     - html_sha1: Optional[str] (SHA1 of the fetched HTML body)
#     - title: Optional[str] (extracted title)
#     - content: Optional[str] (main extracted text content)
#     Not sure - published_at: Optional[datetime] (when the article was published)
#     Not sure- source_domain: Optional[str] (effective source domain/host)
#     - crawl_ts: datetime (when the article was fetched)
"""
from pydantic import BaseModel, Field, HttpUrl, field_validator
import httpx
import time
import asyncio
import trafilatura
import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import urlparse
from contextlib import asynccontextmanager

from src.utils.logger import get_logger

_DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

logger = get_logger("pipeline.fetcher")

class ArticleFetched(BaseModel):
    url: HttpUrl
    content_id: str = Field(min_length=40, max_length=40, description="SHA1 hex digest")
    title: str | None = Field(default=None, description="Article title")
    excerpt: str | None = Field(default=None, description="Provider-supplied excerpt")
    paywall: bool | None = Field(default=None, description="True if full article sits behind a paywall")
    authors: list[str] = Field(default_factory=list, description="Byline authors")
    keywords: list[str] = Field(default_factory=list, description="Provider-supplied keywords/tags")
    thumbnail: str | None = Field(default=None, description="Thumbnail image URL")
    content_length: int | None = Field(default=None, description="Provider-estimated content length")
    extracted_content_length: int = Field(ge=0, description="Length of normalized extracted content")
    content: str = Field(description="Extracted main text content", min_length=10)
    published_date: datetime | None = Field(default=None, description="Published date of the article")
    publisher_name: str | None = Field(default=None, description="Publisher display name")
    publisher_domain: str = Field(description="Publisher domain/host", min_length=3)
    bucket: str | None = Field(default=None, description="Source bucket category")
    crawl_ts: datetime

    @field_validator("publisher_domain")
    @classmethod
    def normalize_domain(cls, value):
        value = (value or "").strip().lower()
        if value.startswith("www."):
            value = value[4:]
        return value
    
    def get(self, key: str, default=None):
        return getattr(self, key, default)

class DomainSemaphore:
    """A dict of a semaphores keyed by domain to limit concurrent fetches per domain.
    Also using LRU and TTL to limit memory usage.
    
    Parameters
    ----------
    max : int - maximum concurrent fetches per domain
    ttl_sec : int - time to live for each semaphore in seconds
    max_size : int - maximum number of semaphores to keep"""
    def __init__(self, max: int = 3, ttl_sec: int = 1800, max_size: int = 500 ):
        #state for memory and concurrency management
        self._max = max
        self._ttl = ttl_sec
        self._max_size = max_size
        self._lock = asyncio.Lock()
        self._create_count = 0

        # store with key as domain and and entry as value
        # entry = {'sem': Semaphore, 'last_used': timestamp, 'inflight': int}
        self._sems = {}  # domain -> entry

    async def get_semaphore(self, domain):
        """Get or create an entry for the given domain."""
        entry = self._sems.get(domain)
        # Fast path: update last_used and return existing entry
        if entry is not None:
            entry['last_used'] = time.monotonic()
            return entry
        #Slow path: create new entry for the domain using critical section
        async with self._lock:

            existing = self._sems.get(domain)
            if existing is not None:
                existing['last_used'] = time.monotonic()
                return existing
            
            entry = {
                'sem': asyncio.Semaphore(self._max),
                "last_used": time.monotonic(),
                "inflight": 0,
            }
            self._sems[domain] = entry
            self._create_count += 1
            # Periodic cleanup
            if self._create_count % 100 == 0:
                self._cleanup()
            return entry
        
    @asynccontextmanager
    async def guard(self, domain: str):
        """
        Acquire the per-domain semaphore and handle in-flight and last-used bookkeeping.
        Usage:
            async with domain_semaphore.guard(domain):
                ... do the fetch ...
        """
        entry = await self.get_semaphore(domain)
        sem = entry['sem']
        async with sem:
            entry['inflight'] += 1
            try:
                yield
            finally:
                entry['inflight'] -= 1
                entry['last_used'] = time.monotonic()
        
    def _cleanup(self):
        """Cleanup old semaphores to limit memory usage."""
        # Remove semaphores that are older than TTL
        now = time.monotonic()
        to_delete = []
        for domain, entry in self._sems.items():
            age = now - entry['last_used']
            if age > self._ttl:
                to_delete.append(domain)
        for domain in to_delete:
            del self._sems[domain]

        # If still over max size, remove oldest
        if len(self._sems) > self._max_size:
            sorted_sems = sorted(self._sems.items(), key=lambda item: item[1]['last_used'])
            for domain, _ in sorted_sems[:len(self._sems) - self._max_size]:
                del self._sems[domain]



_ws_re = re.compile(r"\s+")


def _ensure_str_list(value) -> list[str]:
    """Normalize optional iterables/strings into a list of strings."""
    if not value:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value if v is not None]
    return [str(value)]


def normalize_text(s: str) -> str:
    """Normalize text for hashing/comparison.

    - Collapse all whitespace to single spaces
    - Strip leading/trailing whitespace
    """
    if s is None:
        return ""
    return _ws_re.sub(" ", s).strip()


async def fetch_content(url: str, excerpt: str, client: httpx.AsyncClient) -> str:
    """Fetch the content of a URL and extract the main text body.

    Returns the extracted text (fallbacks to excerpt if extraction fails).
    """
    for attempt in (0, 1, 2):  # up to 3 attempts
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            html = resp.text
            content = trafilatura.extract(html) or ""
            if not content:
                content = excerpt or ""
            return content
        except (httpx.TimeoutException, httpx.TransportError) as e:
            if attempt < 2:
                await asyncio.sleep(0.5 * (2 ** attempt))  # 0.5s, 1s backoff
                continue
            logger.warning("fetch_content: network error for %s: %s", url, e)
            return excerpt or ""
        except httpx.HTTPStatusError as e:
            logger.info("fetch_content: HTTP %s for %s", e.response.status_code, url)
            return excerpt or ""
        except Exception as e:
            logger.warning("fetch_content: unexpected error for %s: %s", url, e)
            return excerpt or ""
    
def hash_content(content: str) -> str:
    """Generate a SHA1 hash of the content."""
    normalized = _ws_re.sub(" ", (content or "")).strip()
    h = hashlib.sha1()
    h.update(normalized.encode("utf-8", errors="ignore"))
    return h.hexdigest()

domain_semaphore = DomainSemaphore(max=3)

async def fetcher(url_queue: asyncio.Queue,
                  content_queue: asyncio.Queue) -> None:
    """
    Consume url_queue -> fetch_content -> ArticleFetched -> send to 
    content_queue"""

    limits = httpx.Limits(max_keepalive_connections=50, max_connections=200)
    timeout = httpx.Timeout(connect=10.0, read=20.0, write=20.0, pool=10.0)
    logger.info(
        "fetcher: starting; url_queue=%s content_queue=%s",
        getattr(url_queue, "name", "url_queue"),
        getattr(content_queue, "name", "content_queue"),
    )
    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=timeout,
        limits=limits,
        headers=_DEFAULT_HEADERS,
    ) as client:
        logger.debug("fetcher: HTTP client initialised")
        while True:
            try:
                producer_msg = await url_queue.get()
                if producer_msg is None:
                    logger.info("fetcher: received sentinel; exiting loop")
                    break
                url = producer_msg.get("url")
                excerpt = producer_msg.get("excerpt")
                paywall = producer_msg.get("paywall")
                bucket = producer_msg.get("bucket")
                authors = _ensure_str_list(producer_msg.get("authors"))
                keywords = _ensure_str_list(producer_msg.get("keywords"))
                thumbnail = producer_msg.get("thumbnail")
                if thumbnail is not None:
                    thumbnail = str(thumbnail)
                content_length = producer_msg.get("content_length")
                try:
                    content_length = int(content_length) if content_length is not None else None
                except (TypeError, ValueError):
                    content_length = None
                
                if not url:
                    continue
                
                publisher_domain = producer_msg.get("publisher_domain")
                if not publisher_domain:
                    try:
                        publisher_domain = urlparse(url).netloc
                    except Exception:
                        raise ValueError(
                            "Fetcher could not parse domain from URL, and no publisher_domain provided; "
                            "publisher_domain is needed for semaphore management"
                        )
                        
                
                if paywall:
                    content = excerpt or ""
                else:
                    # Use per-domain semaphore to limit concurrent fetches
                    async with domain_semaphore.guard(publisher_domain):
                        content = await fetch_content(url, excerpt, client)
                
                # Normalize whitespace only for storage; keep case
                content = normalize_text(content)

                if len(content) < 10:
                    logger.debug(
                        "fetcher: content too short for url=%s bucket=%s",
                        url,
                        bucket,
                    )
                    continue
                
                content_id = hash_content(content)
                extracted_content_length = len(content)

                # Create the ArticleFetched message
                item = {
                    "url": url,
                    "content_id": content_id,
                    "title": producer_msg.get("title"),
                    "excerpt": excerpt,
                    "paywall": paywall,
                    "authors": authors,
                    "keywords": keywords,
                    "thumbnail": thumbnail,
                    "content_length": content_length,
                    "extracted_content_length": extracted_content_length,
                    "content": content,
                    "published_date": producer_msg.get("published_date"),
                    "publisher_name": producer_msg.get("publisher_name"),
                    "publisher_domain": publisher_domain,
                    "bucket": bucket,
                    "crawl_ts": datetime.now(timezone.utc),
                }
                # Validate ArticleFetched message
                try:
                    article_fetched = ArticleFetched(**item)
                except Exception as e:
                    logger.warning("fetcher: invalid ArticleFetched for url=%s: %s", url, e)
                    continue
                # Send the ArticleFetched message to the content queue
                # Persistor expects JSON-serialisable payloads; `mode='json'` ensures safe types.
                payload = article_fetched.model_dump(mode="json")
                logger.debug(
                    "fetcher: putting content len=%d url=%s domain=%s bucket=%s",
                    len(content),
                    payload.get("url"),
                    payload.get("publisher_domain"),
                    payload.get("bucket"),
                )
                
                await content_queue.put(payload)
                logger.debug(
                    "fetcher: queued article bucket=%s queue_len=%d",
                    payload.get("bucket"),
                    content_queue.qsize(),
                )
            except asyncio.CancelledError:
                logger.info("fetcher: cancelled")
                raise
            except Exception as e:
                logger.warning("fetcher: error processing %s: %s", producer_msg, e)
                continue

            finally:
                url_queue.task_done()
    logger.info("fetcher: shutdown complete")
