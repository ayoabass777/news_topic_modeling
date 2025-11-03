
from asyncio import Queue
from src.utils.logger import get_logger
from collections import defaultdict, deque
from itertools import cycle
from urllib.parse import urlparse
import httpx


from src.api_adapter.newsapi import discover_newsapi_page


#--------- Configuration ---------

logger = get_logger("pipeline.producer")


buckets = {
    "business":      "business OR economy OR finance OR cryptocurrency OR forex OR markets OR property OR startup",
    "entertainment": "entertainment OR arts OR books OR celebrities OR gaming OR movies OR music OR tv",
    "general_health":"general OR health OR disease OR fitness OR medication OR publichealth",
    "lifestyle":     "lifestyle OR autos OR beauty OR cooking OR fashion OR religion OR tourism OR transportation OR travel",
    "politics":      "politics OR government OR humanrights OR infrastructure OR policy",
    "science":       "science OR climate OR education OR energy OR environment OR genetics OR geology OR physics OR space OR wildlife",
    "sports":        "sports OR baseball OR basketball OR boxing OR cricket OR esports OR f1 OR football OR golf OR hockey OR nascar OR rugby OR soccer OR tennis OR volleyball",
    "technology":    "technology OR ai OR computing OR cybersec OR gadgets OR internet OR mobile OR robot OR vr",
    "world":         "world OR culture OR history",
}

TARGET_TOTAL = 100     #overall target of 4000 urls

INIT_QUOTA = TARGET_TOTAL//len(buckets)  #evenly distribute quota across buckets
MIN_PER_BUCKET = int(0.2 * INIT_QUOTA)  #minimum quota per bucket
MAX_PER_BUCKET = INIT_QUOTA * 2    # allow growth if yielding well
PER_DOMAIN_CAP_RATIO = 0.10     #maximum of 10% of a bucket can come from a single domain
LOW_YIELD_MIN_ADDS = 10      #fewer than this in each page signifyies low yield
DEDUP_WINDOW_PAGES = 4        # recent pages window to consider for deduplication
DEDUP_RATE_BAD = 0.60         # if > 60%  duplication rate, reallocate quota

def domain_from_url(url: str) -> str:
    """Extract the domain from a URL."""
    """Parameters
    url : str
        The URL from which to extract the domain.
    Returns
    str
        The domain extracted from the URL.
    """
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def ok_to_add(
    item: dict,
    bucket: str,
    *,
    seen_urls,
    quota,
    per_domain_bucket,
) -> bool:
    """
    Check if an item can be added to the specified bucket based on various criteria.

    Parameters
    ----------
    item : dict
        The item to be checked, expected to contain 'url'.
    bucket : str
        The bucket to which the item is being considered for addition.
    seen_urls : set, keyword-only
        Tracks URLs already accepted to avoid duplicates.
    quota : dict, keyword-only
        Current quota allocation per bucket.
    per_domain_bucket : dict, keyword-only
        Tracks per-domain contribution within a bucket.

    Returns
    -------
    bool
        True if the item can be added, False otherwise.
    """
    url = item.get('url')
    if not url or url in seen_urls:
        return False
    domain = domain_from_url(url)
    if not domain:
        return False
    
    #per-domain cap relative to the bucket's current quota
    if per_domain_bucket[(bucket, domain)] >= int(PER_DOMAIN_CAP_RATIO * max(1, quota[bucket])):
        return False
    
    return True
    
def record_add(
    item: dict,
    bucket: str,
    *,
    counts,
    seen_urls,
    per_domain_bucket,
) -> None:
    """
    Record the addition of an item to the specified bucket.

    This function updates the counts, seen URLs, and per-domain bucket counts
    when an item is successfully added to a bucket.

    Parameters
    ----------
    item : dict
        The item that was added, expected to contain 'url'.
    bucket : str
        The bucket to which the item was added.
    counts : dict, keyword-only
        Accumulates accepted URL counts per bucket.
    seen_urls : set, keyword-only
        Tracks URLs already accepted to avoid duplicates.
    per_domain_bucket : dict, keyword-only
        Tracks per-domain contribution within a bucket.
    """
    url = item.get('url')
    seen_urls.add(url)
    per_domain_bucket[(bucket, domain_from_url(url))] += 1
    counts[bucket] += 1
    

def should_reallocate(bucket: str, *, recent_stats) -> bool:   
    """
    Determine if the quota for a bucket should be reallocated based on recent statistics.

    This function checks the recent deduplication rate for a bucket and decides
    whether to reallocate its quota based on the defined threshold.

    Parameters
    ----------
    bucket : str
        The bucket for which to check the reallocation condition.
    recent_stats : dict, keyword-only
        Rolling deduplication history per bucket.

    Returns
    -------
    bool
        True if the quota should be reallocated, False otherwise.
    """
    if len(recent_stats[bucket]) < DEDUP_WINDOW_PAGES:
        # Not enough recent stats to evaluate
        return False
    
    avg_dup_rate = sum(recent_stats[bucket]) / len(recent_stats[bucket])
    return avg_dup_rate > DEDUP_RATE_BAD

    
async def run_round_robin()-> list:   
    """Run a round-robin discovery process across multiple buckets.
    This function iterates over the defined buckets, checking if there are
    items to discover in each bucket. If a bucket has items available,
    it will call the discovery function for that bucket and process the results.
    """
    logger.info("producer: starting round robin target=%d across %d buckets", TARGET_TOTAL, len(buckets))
    quota = {bucket: INIT_QUOTA for bucket in buckets}  # current quota per bucket
    counts = {bucket: 0 for bucket in buckets}          # current counts of accepted url per bucket
    pages = {bucket: 0 for bucket in buckets}           # next page to request per bucket
    active = set(buckets.keys())                        # currently active buckets
    seen_urls = set()                                   # set of seen URLs to avoid duplicates
    per_domain_bucket = defaultdict(int)                # (bucket, domain) -> count of domain per bucket
    recent_stats = {bucket: deque(maxlen=DEDUP_WINDOW_PAGES) for bucket in buckets}
    urls_discovered = []

    async with httpx.AsyncClient(follow_redirects= True) as client:
        for bucket in cycle(buckets.keys()):
            if not active:
                break
            if bucket not in active:
                continue
            if counts[bucket] >= quota[bucket]:
                active.discard(bucket)
                continue

            pages[bucket] += 1

            # Seperate the newsdatacontract from pagination metadata!!!!!!!!!!!!!
            news_page = await discover_newsapi_page(
                client,
                query=buckets[bucket],
                page=pages[bucket],
            )
            items = news_page.data
            if not items:
                active.discard(bucket)
                logger.info(
                    "producer: bucket=%s page=%d returned no items; marking inactive",
                    bucket,
                    pages[bucket],
                )
                continue
            
            
            #logger.info(f"Discovered {len(items)} items in bucket {bucket} on page {pages[bucket]}")
            accepted = 0
            dups = 0
            
            for item in items:
                if counts[bucket] >= quota[bucket]:
                    active.discard(bucket)
                    break
                if ok_to_add(
                    item,
                    bucket,
                    seen_urls=seen_urls,
                    quota=quota,
                    per_domain_bucket=per_domain_bucket,
                ):
                    record_add(
                        item,
                        bucket,
                        counts=counts,
                        seen_urls=seen_urls,
                        per_domain_bucket=per_domain_bucket,
                    )
                    accepted += 1
                    urls_discovered.append(
                        {
                            **item.model_dump(),
                            "bucket": bucket,
                        }
                    )
                else:
                    dups += 1 

            total = accepted + dups
            dedupe_rate = dups / total if total > 0 else 0
            recent_stats[bucket].append(dedupe_rate)
            logger.debug(
                "producer: bucket=%s page=%d accepted=%d duplicates=%d dedupe_rate=%.2f quota=%d count=%d",
                bucket,
                pages[bucket],
                accepted,
                dups,
                dedupe_rate,
                quota[bucket],
                counts[bucket],
            )
            #logger.info(f"Bucket {bucket}: accepted={accepted}, duplicates={dups}, dedupe_rate={dedupe_rate:.2%}")
            
            #if low yield, try to reallocate quota
            if accepted < LOW_YIELD_MIN_ADDS:
                # struggling bucket, try to reallocate quota
                #logger.info(f"Bucket {bucket} has low yield, considering reallocation")
                if len(active) <= 1:
                    # Only one active bucket, no point in reallocating
                    continue
                # Check if the bucket should be reallocated
                if should_reallocate(bucket, recent_stats=recent_stats):
                    spare = max(0, quota[bucket]- counts[bucket]-MIN_PER_BUCKET)
                    donation = min(100, spare)
                    if donation > 0:
                        # Reallocate quota to other buckets
                        #logger.info(f"Reallocating {donate} from {bucket} to other buckets")
                        quota[bucket] -= donation
                        possible_recipients = [b for b in active if b != bucket]
                        chosen_recipient = min(possible_recipients, key= lambda b: quota[b])
                        quota[chosen_recipient] = min(MAX_PER_BUCKET, quota[chosen_recipient] + donation)
                        logger.info(
                            "producer: reallocating %d quota from %s to %s",
                            donation,
                            bucket,
                            chosen_recipient,
                        )
                else:
                    continue  # No reallocation needed, continue to next bucket
            
            # stop condition
            if sum(counts.values()) >= TARGET_TOTAL:
                active.clear()
                break

            #logger.info(f"Bucket {bucket}: counts={counts[bucket]}, quota={quota[bucket]}")
        #logger.info(f"{"counts": counts, "quota": quota, "seen_urls": len(seen_urls), "active_buckets": len(active), "pages": pages}) 
        total_discovered = len(urls_discovered)
        logger.info(
            "producer: discovery complete total=%d active_remaining=%d",
            total_discovered,
            len(active),
        )
        return urls_discovered   


async def producer(url_queue: Queue) -> None:
    """
    Producer function to discover news articles and enqueue their URLs.
    creates the discoverd_url object and adds it to the url_queue.

    This function runs a round-robin discovery process across multiple buckets,
    fetching articles based on predefined queries and managing quotas.
    
    Parameters
    ----------
    url_queue : Queue
        The queue to which discovered URLs will be added.
    total_limit : int, optional
        The total number of URLs to discover, by default TARGET_TOTAL.
    """
    logger.info("producer: starting producer coroutine")
    urls_discovered = await run_round_robin()
    logger.info("producer: enqueuing %d discovered urls", len(urls_discovered))
    for discovery in urls_discovered:
        msg = discovery
        await url_queue.put(msg)
        logger.debug(
            "producer: queued url=%s bucket=%s",
            msg.get("url"),
            msg.get("bucket"),
        )
