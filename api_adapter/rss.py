from __future__ import annotations

from email.utils import parsedate_to_datetime
from typing import Optional
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

from src.api_adapter.newsapi import (
    LIMIT_PER_CALL,
    NewsDataContract,
    PageOfNewsDataContract,
    PaginationMetaContract,
)
from src.utils.logger import get_logger


logger = get_logger("pipeline.rss")

ATOM_NS = "{http://www.w3.org/2005/Atom}"
MEDIA_NS = "{http://search.yahoo.com/mrss/}"


def _text(node: Optional[ET.Element], default: str | None = None) -> str | None:
    if node is None or node.text is None:
        return default
    value = node.text.strip()
    return value or default


def _find_text(item: ET.Element, path: str, default: str | None = None) -> str | None:
    return _text(item.find(path), default)


def _published_iso(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).isoformat()
    except Exception:
        return value


def _publisher_from_link(link: str | None) -> str | None:
    if not link:
        return None
    try:
        domain = urlparse(link).netloc
    except Exception:
        return None
    return domain or None


def _entry_link(item: ET.Element) -> str | None:
    link = _find_text(item, "link")
    if link:
        return link
    atom_link = item.find(f"{ATOM_NS}link")
    if atom_link is not None:
        return atom_link.attrib.get("href")
    return None


def _entry_title(item: ET.Element) -> str | None:
    return _find_text(item, "title") or _find_text(item, f"{ATOM_NS}title")


def _entry_summary(item: ET.Element) -> str | None:
    return (
        _find_text(item, "description")
        or _find_text(item, "summary")
        or _find_text(item, f"{ATOM_NS}summary")
        or _find_text(item, f"{ATOM_NS}content")
    )


def _entry_keywords(item: ET.Element) -> list[str]:
    keywords = []
    for category in item.findall("category"):
        value = category.attrib.get("term") or _text(category)
        if value:
            keywords.append(value)
    for category in item.findall(f"{ATOM_NS}category"):
        value = category.attrib.get("term") or _text(category)
        if value:
            keywords.append(value)
    return keywords


def _entry_thumbnail(item: ET.Element) -> str | None:
    media_content = item.find(f"{MEDIA_NS}thumbnail")
    if media_content is not None:
        return media_content.attrib.get("url")
    enclosure = item.find("enclosure")
    if enclosure is not None:
        media_type = enclosure.attrib.get("type", "")
        if media_type.startswith("image/"):
            return enclosure.attrib.get("url")
    return None


def _entry_published(item: ET.Element) -> str | None:
    raw_value = (
        _find_text(item, "pubDate")
        or _find_text(item, "published")
        or _find_text(item, f"{ATOM_NS}published")
        or _find_text(item, f"{ATOM_NS}updated")
    )
    return _published_iso(raw_value)


def _feed_title(root: ET.Element) -> str | None:
    return _find_text(root, "./channel/title") or _find_text(root, f"./{ATOM_NS}title")


def _map_entry(item: ET.Element, *, league: str, feed_name: str, feed_url: str) -> NewsDataContract | None:
    link = _entry_link(item)
    if not link:
        return None

    title = _entry_title(item)
    excerpt = _entry_summary(item)
    published_date = _entry_published(item)
    keywords = _entry_keywords(item)
    thumbnail = _entry_thumbnail(item)

    return NewsDataContract(
        url=link,
        title=title,
        excerpt=excerpt,
        paywall=False,
        authors=[],
        keywords=keywords,
        publisher_name=feed_name or _publisher_from_link(link),
        published_date=published_date,
        thumbnail=thumbnail,
        content_length=None,
        league=league,
        source_type="rss",
        source_feed=feed_url,
    )


async def discover_rss_feed(
    client,
    *,
    feed_url: str,
    league: str,
    feed_name: str | None = None,
) -> PageOfNewsDataContract:
    try:
        response = await client.get(feed_url, follow_redirects=True)
        response.raise_for_status()
    except Exception as e:
        logger.warning("rss: request failed for %s (%s)", feed_url, e)
        return PageOfNewsDataContract()

    try:
        root = ET.fromstring(response.text)
    except Exception as e:
        logger.warning("rss: parse failed for %s (%s)", feed_url, e)
        return PageOfNewsDataContract()

    items = []
    rss_items = root.findall("./channel/item")
    atom_items = root.findall(f"./{ATOM_NS}entry")
    resolved_feed_name = feed_name or _feed_title(root) or "RSS Feed"
    for entry in rss_items + atom_items:
        mapped = _map_entry(
            entry,
            league=league,
            feed_name=resolved_feed_name,
            feed_url=feed_url,
        )
        if mapped is not None:
            items.append(mapped)
        if len(items) >= LIMIT_PER_CALL:
            break

    metadata = PaginationMetaContract(page=1, totalHits=len(items), totalPages=1, size=len(items))
    return PageOfNewsDataContract(data=items, metadata=metadata)
