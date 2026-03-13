"""
Configuration for news_topic_model using Pydantic BaseSettings (pydantic v2).

Usage:
    from .config import settings
    print(settings.number_of_fetchers)

Environment overrides:
  - Place a .env file in the project root (same dir as this file or cwd) or set OS env vars.
  - Example .env:
        NUMBER_OF_FETCHERS=4
        DOMAIN_CONCURRENCY=3
        UA="news-topic-model/1.0 (+https://github.com/ayoabass777)"
        LOG_LEVEL=DEBUG
"""

from __future__ import annotations

from pathlib import Path
from typing import Set

import logging
from logging.handlers import RotatingFileHandler

from pydantic_settings import BaseSettings, SettingsConfigDict  # type: ignore

class Settings(BaseSettings):
    # ---------------- Paths ----------------
    base_dir: Path = Path(__file__).resolve().parent
    # Default outputs/logs under the current working directory (overridable via .env)
    data_dir: Path = Path.cwd() / "data"

    # ---------------- Defaults ----------------
    default_provider: str = "gdelt"
    default_window: str = "1d"
    default_out: Path = data_dir

    # ---------------- Concurrency ----------------
    number_of_fetchers: int = 2
    domain_concurrency: int = 2
    max_queue_size: int = 500

    # ---------------- HTTP / Retry policy ----------------
    ua: str = "news-topic-model/1.0 (+https://github.com/ayoabass777)"
    # Browser-like headers but exclude 'Accept-Encoding' to avoid double-compression bugs
    default_headers: dict = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    # Retry on these HTTP status codes only; exclude 429 as it is handled separately with domain throttling
    retry_status: Set[int] = {408, 500, 502, 503, 504}
    max_retries: int = 3
    backoff_initial: float = 0.5   # seconds
    backoff_factor: float = 2.0
    jitter_max: float = 0.25       # seconds

    # ---------------- GDELT ----------------
    gdelt_api: str = "https://api.gdeltproject.org/api/v2/doc/doc"

    # ---------------- Language filtering ----------------
    lang_min_prob: float = 0.80  # threshold for hybrid english detector

    # ---------------- Logging ----------------
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=(".env",),
        env_prefix="",
        case_sensitive=False,
        extra="ignore",
    )

settings = Settings()


# ---------------- Logging Setup (centralized) ----------------
# Create data/logs directory
log_dir = settings.data_dir / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

# Common formatter
_LOG_FMT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_formatter = logging.Formatter(_LOG_FMT)

# Helper to build a rotating file logger without duplicate handlers
_DEF_MAX_BYTES = 5_000_000  # ~5 MB
_DEF_BACKUPS = 5

def _build_file_logger(name: str, filename: str, level: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level.upper())
    logger.propagate = False  # do not bubble to root
    # Avoid duplicate handlers if re-imported
    file_path = log_dir / filename
    need_handler = True
    for h in logger.handlers:
        if isinstance(h, RotatingFileHandler) and getattr(h, 'baseFilename', None) == str(file_path):
            need_handler = False
            break
    if need_handler:
        fh = RotatingFileHandler(
            file_path,
            maxBytes=_DEF_MAX_BYTES,
            backupCount=_DEF_BACKUPS,
            encoding="utf-8",
            delay=True,
        )
        fh.setLevel(level.upper())
        fh.setFormatter(_formatter)
        logger.addHandler(fh)
    return logger

# Individual module loggers
pipeline_logger  = _build_file_logger("news_topic_model.pipeline",           "pipeline.log",  settings.log_level)
producer_logger  = _build_file_logger("news_topic_model.pipeline.producer",  "producer.log",  settings.log_level)
fetcher_logger   = _build_file_logger("news_topic_model.pipeline.fetcher",   "fetcher.log",   settings.log_level)
persistor_logger = _build_file_logger("news_topic_model.pipeline.persistor", "persistor.log", settings.log_level)
gdelt_adapter_logger  = _build_file_logger("news_topic_model.adapters.gdelt", "gdelt_adapter.log",   settings.log_level)
# Dedicated rejections stream (for dropped/filtered URLs)
rejections_logger = _build_file_logger("news_topic_model.rejections",        "rejections.log", settings.log_level)

# Back-compat: some modules import `logger` from config expecting a pipeline logger
logger = pipeline_logger

# Back-compat simple constants (imported elsewhere)
UA = settings.ua
RETRY_STATUS = settings.retry_status
MAX_RETRIES = settings.max_retries
BACKOFF_INITIAL = settings.backoff_initial
BACKOFF_FACTOR = settings.backoff_factor
JITTER_MAX = settings.jitter_max
MAX_QUEUE_SIZE = settings.max_queue_size