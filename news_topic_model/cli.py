from __future__ import annotations

import argparse
import asyncio
import logging

from news_topic_model.pipeline.run import run_queue
from news_topic_model.config import (
    Settings,
    pipeline_logger,
    producer_logger,
    fetcher_logger,
    persistor_logger,
    rejections_logger,
)


DESCRIPTION = """
News Topic Model – ingestion CLI

Examples:
  # Simple: last 1 day (default window), English-only, write JSONL+Parquet
  news-topic-model --query "artificial intelligence" --out data

  # Explicit bounds (local time parsed strictly), fill the missing side from --window
  news-topic-model --query "renewable energy" --start "2025-08-15 06:00" --window 6h
  news-topic-model --query "renewable energy" --end   "2025-08-15 18:00" --window 6h

  # Explicit start+end (window ignored when both are provided)
  news-topic-model --query "inflation" --start "2025-08-15 00:00" --end "2025-08-16 00:00"
"""


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="news-topic-model",
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    p.add_argument("--query", required=True, help="Search query for the provider(s)")

    # Time window / bounds
    p.add_argument(
        "--window",
        default="1d",
        help=(
            "Time window like '30min', '1h', '1d', '2w'. "
            "Used when neither --start nor --end are provided, or to fill the missing bound when only one of them is provided."
        ),
    )
    p.add_argument(
        "--start",
        help=(
            "Start datetime in strict local format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM[:SS]'. "
            "If provided without --end, the end bound is computed as start + --window."
        ),
    )
    p.add_argument(
        "--end",
        help=(
            "End datetime in strict local format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM[:SS]'. "
            "If provided without --start, the start bound is computed as end - --window."
        ),
    )

    # Provider selection (extensible)
    p.add_argument(
        "--providers",
        nargs="+",
        default=["gdelt"],
        choices=["gdelt"],
        help="Content discovery providers to use (default: gdelt)",
    )

    # Concurrency and limits
    p.add_argument("--limit", type=int, default=100, help="Max items to discover (API caps at 100 per request)")
    p.add_argument("--number_of_fetchers", type=int, default=2, help="Number of concurrent fetcher workers")
    p.add_argument("--domain_concurrency", type=int, default=2, help="Max concurrent requests per domain")

    # Output
    p.add_argument("--out", default="data", help="Output base directory (JSONL + Parquet under this path)")

    # Flags for future extensibility
    p.add_argument("--parquet", action="store_true", help="(Reserved) Also write Parquet output – enabled by default in persistor")

    # Verbosity
    p.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity (repeatable)")

    return p


def _apply_verbosity(v: int) -> None:
    """Map `-v` occurrences to logger levels across our centralized loggers.

    v=0 → WARNING (default)
    v=1 → INFO
    v>=2 → DEBUG
    """
    level = logging.WARNING if v <= 0 else (logging.INFO if v == 1 else logging.DEBUG)

    # Adjust centralized loggers
    for lg in (pipeline_logger, producer_logger, fetcher_logger, persistor_logger, rejections_logger):
        lg.setLevel(level)

    # Root logger too (in case any third-party modules emit)
    logging.getLogger().setLevel(level)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Initialize settings (ensures data/logs dir exists via config)
    _ = Settings()

    # Apply verbosity mapping to centralized file loggers
    _apply_verbosity(args.verbose)

    # Run orchestrator
    try:
        asyncio.run(run_queue(args))
        return 0
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())