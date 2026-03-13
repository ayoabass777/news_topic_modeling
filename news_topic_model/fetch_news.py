from dotenv import load_dotenv
import os, sys, json, time, hashlib, argparse
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import requests

load_dotenv()
NEWSAPI_URL = "https://newsapi.org/v2/everything"

DEFAULT_SUBTOPICS = {
    "ai_ml": '(AI OR "artificial intelligence" OR "machine learning")',
    "chips": '(semiconductor OR chip OR Nvidia OR AMD OR Intel OR TSMC)',
    "cybersec": '(breach OR ransomware OR "zero-day" OR cybersecurity)',
    "cloud_devops": '(AWS OR Azure OR GCP OR Kubernetes OR DevOps)',
    "mobile": '(iPhone OR Android OR Samsung OR Pixel OR smartphone)',
    "enterprise": '(startup OR funding OR SaaS OR enterprise)'
}

# Some high-signal tech outlets (optional filter)
DEFAULT_DOMAINS = ",".join([
    "arstechnica.com",
    "theverge.com",
    "techcrunch.com",
    "wired.com",
    "theregister.com",
    "engadget.com",
    "zdnet.com",
    "thestack.technology",
    "computerworld.com",
    "venturebeat.com"
])

def iso_days_ago(days: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def to_iso_z(dt: datetime) -> str:
    """Format a datetime as ISO 8601 Zulu (no micros)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def hash_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]

def fetch_page(api_key: str, query: str, from_iso: str, to_iso: Optional[str], page: int, page_size: int,
               language: str, search_in: str, domains: Optional[str], timeout: int = 20) -> Dict[str, Any]:
    params = {
        "q": query,
        "from": from_iso,
        "language": language,
        "sortBy": "publishedAt",
        "pageSize": page_size,
        "page": page,
        "searchIn": search_in,
    }
    if domains:
        params["domains"] = domains
    if to_iso:
        params["to"] = to_iso

    headers = {"X-Api-Key": api_key}
    resp = requests.get(NEWSAPI_URL, params=params, headers=headers, timeout=timeout)
    return {"status_code": resp.status_code, "headers": resp.headers, "json": (resp.json() if resp.content else {})}

def normalize(article: Dict[str, Any], topic_key: str, page: int) -> Dict[str, Any]:
    src = article.get("source") or {}
    title = (article.get("title") or "").strip()
    desc = (article.get("description") or "").strip()
    content = (article.get("content") or "").strip()

    return {
        "id": hash_id(article.get("url", "")),
        "title": title,
        "description": desc,
        "content": content,
        "text": " ".join([t for t in [title, desc, content] if t]).strip(),
        "source_id": src.get("id"),
        "source_name": src.get("name"),
        "author": article.get("author"),
        "url": article.get("url"),
        "urlToImage": article.get("urlToImage"),
        "published_at": article.get("publishedAt"),
        "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "topic": topic_key,
        "page": page,
    }

def backoff_sleep(try_idx: int, retry_after: Optional[str] = None, base: float = 1.5, max_wait: float = 60.0):
    if retry_after and retry_after.isdigit():
        wait = min(max_wait, float(retry_after))
    else:
        wait = min(max_wait, base ** try_idx)
    time.sleep(wait)

def main():
    ap = argparse.ArgumentParser(description="Fetch tech news articles from NewsAPI and save to JSONL.")
    ap.add_argument("--outfile", default=f"articles_tech_{datetime.now().strftime('%Y%m%d')}.jsonl",
                    help="Output JSONL file")
    ap.add_argument("--from-days", type=int, default=5, help="Look back this many days")
    ap.add_argument("--pages", type=int, default=2, help="Pages per subtopic (in 'pages' mode) OR slices per subtopic (in 'timeslice' mode)")
    ap.add_argument("--page-size", type=int, default=100, choices=range(1, 101), metavar="[1-100]",
                    help="Items per page (NewsAPI max is 100)")
    ap.add_argument("--language", default="en")
    ap.add_argument("--search-in", default="title,description", help="title,description,content")
    ap.add_argument("--domains", default=DEFAULT_DOMAINS, help="Comma-separated allowlist of domains (optional)")
    ap.add_argument("--no-domains", action="store_true", help="Ignore the default domains filter")
    ap.add_argument("--budget", type=int, default=95, help="Max requests to use (stay below your daily cap)")
    ap.add_argument("--sleep", type=float, default=0.25, help="Seconds to sleep between requests")
    ap.add_argument("--topics", nargs="*", default=list(DEFAULT_SUBTOPICS.keys()),
                    help=f"Subset of topics to fetch: {list(DEFAULT_SUBTOPICS.keys())}")
    ap.add_argument("--paging-mode", choices=["pages", "timeslice"], default="timeslice",
                    help="Use 'pages' to paginate (page>1 hits 100-result cap on free tier). Use 'timeslice' to move a time window backward, always requesting page=1.")
    ap.add_argument("--window-hours", type=int, default=24,
                    help="In timeslice mode, size of each slice in hours (e.g., 24 = one day per slice).")
    args = ap.parse_args()

    api_key = os.getenv("NEWSAPI_KEY")
    if not api_key:
        print("ERROR: Set NEWSAPI_KEY environment variable.", file=sys.stderr)
        sys.exit(1)

    subtopics = {k: v for k, v in DEFAULT_SUBTOPICS.items() if k in set(args.topics)}
    if not subtopics:
        print("ERROR: No valid topics selected.", file=sys.stderr)
        sys.exit(1)

    total_planned = len(subtopics) * args.pages
    if total_planned > args.budget:
        # cap pages/slices to budget
        max_units = max(1, args.budget // len(subtopics))
        print(f"[warn] Planned requests ({total_planned}) exceed budget ({args.budget}). "
              f"Capping from {args.pages} -> {max_units}")
        args.pages = max_units

    start_dt = datetime.now(timezone.utc) - timedelta(days=args.from_days)
    from_iso_info = to_iso_z(start_dt)
    domains = None if args.no_domains else (args.domains or None)

    print(f"[info] Topics: {list(subtopics.keys())}")
    print(f"[info] Mode: {args.paging_mode}")
    print(f"[info] From: {from_iso_info} | language={args.language} | searchIn={args.search_in}")
    if domains:
        print(f"[info] Domains filter: {domains}")
    label = "Pages per topic" if args.paging_mode == "pages" else "Slices per topic"
    print(f"[info] {label}: {args.pages} | pageSize: {args.page_size} | request budget: {args.budget}")

    seen_urls = set()
    written = 0
    total_requests = 0

    with open(args.outfile, "w", encoding="utf-8") as fout:
        for topic_key, query in subtopics.items():

            if args.paging_mode == "pages":
                # Traditional paging (beware 100-result cap on free tier)
                from_iso = from_iso_info
                for page in range(1, args.pages + 1):
                    # pacing
                    if args.sleep > 0:
                        time.sleep(args.sleep)

                    tries = 0
                    while True:
                        total_requests += 1
                        resp = fetch_page(
                            api_key=api_key,
                            query=query,
                            from_iso=from_iso,
                            to_iso=None,
                            page=page,
                            page_size=args.page_size,
                            language=args.language,
                            search_in=args.search_in,
                            domains=domains,
                        )
                        code = resp["status_code"]
                        data = resp["json"]

                        if code == 200:
                            articles = data.get("articles", [])
                            if not articles:
                                print(f"[info] {topic_key} page {page}: 0 articles")
                            for a in articles:
                                url = a.get("url", "")
                                if not url or url in seen_urls:
                                    continue
                                seen_urls.add(url)
                                rec = normalize(a, topic_key=topic_key, page=page)
                                # Skip very short items (low signal)
                                if len((rec.get("text") or "")) < 120:
                                    continue
                                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                                written += 1
                            print(f"[ok] {topic_key} page {page}: wrote {written} total (unique urls: {len(seen_urls)})")
                            break
                        elif code in (429, 502, 503, 504):
                            tries += 1
                            ra = resp["headers"].get("Retry-After")
                            print(f"[retry] HTTP {code} on {topic_key} page {page}. try={tries} retry_after={ra}")
                            backoff_sleep(tries, retry_after=ra)
                            continue
                        elif code == 426:
                            # Free/dev plan: cannot request results beyond 100.
                            print(f"[warn] HTTP 426 on {topic_key} page {page}: 100-result cap hit. "
                                  f"Consider --paging-mode timeslice or reduce --from-days / add domain filters.")
                            break
                            # (We do not auto-switch modes to avoid surprises.)
                        else:
                            err = data.get("message") or str(data)[:200]
                            print(f"[error] HTTP {code} on {topic_key} page {page}: {err}")
                            break

            else:
                # Time-slice mode: move a time window backward, always requesting page=1
                slice_end = datetime.now(timezone.utc)
                for slice_idx in range(1, args.pages + 1):
                    slice_start = max(start_dt, slice_end - timedelta(hours=args.window_hours))
                    from_iso = to_iso_z(slice_start)
                    to_iso = to_iso_z(slice_end)

                    if args.sleep > 0:
                        time.sleep(args.sleep)

                    tries = 0
                    while True:
                        total_requests += 1
                        resp = fetch_page(
                            api_key=api_key,
                            query=query,
                            from_iso=from_iso,
                            to_iso=to_iso,
                            page=1,  # always page 1 to avoid 100-result cap
                            page_size=args.page_size,
                            language=args.language,
                            search_in=args.search_in,
                            domains=domains,
                        )
                        code = resp["status_code"]
                        data = resp["json"]

                        if code == 200:
                            articles = data.get("articles", [])
                            if not articles:
                                print(f"[info] {topic_key} slice {slice_idx} [{from_iso}..{to_iso}]: 0 articles")
                            for a in articles:
                                url = a.get("url", "")
                                if not url or url in seen_urls:
                                    continue
                                seen_urls.add(url)
                                rec = normalize(a, topic_key=topic_key, page=slice_idx)
                                if len((rec.get("text") or "")) < 120:
                                    continue
                                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                                written += 1
                            print(f"[ok] {topic_key} slice {slice_idx} [{from_iso}..{to_iso}]: wrote {written} total (unique urls: {len(seen_urls)})")
                            break
                        elif code in (429, 502, 503, 504):
                            tries += 1
                            ra = resp["headers"].get("Retry-After")
                            print(f"[retry] HTTP {code} on {topic_key} slice {slice_idx}. try={tries} retry_after={ra}")
                            backoff_sleep(tries, retry_after=ra)
                            continue
                        elif code == 426:
                            # Shouldn't happen in timeslice mode (page=1), but log just in case.
                            print(f"[warn] HTTP 426 on {topic_key} slice {slice_idx}: try reducing window size (--window-hours) or --from-days.")
                            break
                        else:
                            err = data.get("message") or str(data)[:200]
                            print(f"[error] HTTP {code} on {topic_key} slice {slice_idx}: {err}")
                            break

                    slice_end = slice_start
                    if slice_end <= start_dt:
                        break  # reached the lower bound of the time range

    print(f"\n[done] Requests used: {total_requests} | Articles written: {written} | File: {args.outfile}")

if __name__ == "__main__":
    main()