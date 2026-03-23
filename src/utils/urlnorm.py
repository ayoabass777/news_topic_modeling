

"""
URL normalization helper for deduplication and comparison.
"""

import re
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, urlsplit

# Common tracking/query parameters to drop
_TRACKING_PARAMS = {
    # UTM params (covered by startswith below, but included for completeness)
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    # ad/analytics click ids
    "fbclid", "gclid", "dclid", "msclkid", "twclid", "yclid",
    # social/app shares & marketing platforms
    "igshid", "_hsmi", "_hsenc", "mc_cid", "mc_eid", "vero_conv", "vero_id",
    # generic campaign refs frequently used by publishers
    "ref", "ref_src", "campaign", "cmp", "cmpid", "mbid",
}

def get_domain(url: str) -> str:
    try:
        return urlsplit(url).netloc.lower()
    except Exception:
        return ""

def normalize_url(url: str) -> str:
    """
    Normalize a URL string for deduplication:
      - ensures scheme is present (default https)
      - lowercases scheme and hostname
      - removes fragments (#...)
      - strips common tracking query parameters
      - sorts query parameters for stable comparison
    """
    if not url:
        return ""

    # Default scheme if missing
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", url):
        url = "https://" + url

    parsed = urlparse(url)

    # Lowercase scheme and hostname
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()

    # Drop default ports
    if scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    elif scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]

    # Remove fragment
    fragment = ""

    # Clean query parameters
    query_params = []
    for k, v in parse_qsl(parsed.query, keep_blank_values=True):
        if k not in _TRACKING_PARAMS:
            query_params.append((k, v))
    query_params.sort()
    query = urlencode(query_params)

    normalized = urlunparse((
        scheme,
        netloc,
        parsed.path or "/",
        parsed.params or "",  # rarely used; keep empty for normalization
        query,
        fragment,
    ))

    return normalized