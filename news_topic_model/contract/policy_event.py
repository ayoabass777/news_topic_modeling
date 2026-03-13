 # src/news_topic_model/contracts/policy_event.py
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
import asyncio
import logging

class PolicyEventType(str, Enum):
    RECORD_OUTCOME = "record_outcome"
    SET_MODE       = "set_mode"
    SET_RETRY_AFTER= "set_retry_after"
    UPDATE_RSS     = "update_rss"

@dataclass(frozen=True)
class PolicyEvent:
    type: PolicyEventType
    domain: str

    @staticmethod
    async def enqueue(queue, event: "PolicyEvent"):
        """
        Enqueue a PolicyEvent instance into the provided asyncio queue and log the action.
        """
        await queue.put(event)
        logging.info(f"Enqueued PolicyEvent: {event}")


# --- Specific event payloads ---

@dataclass(frozen=True)
class RecordOutcome(PolicyEvent):
    status: int
    extract_ok: bool

    def __init__(self, domain: str, status: int, extract_ok: bool):
        super().__init__(PolicyEventType.RECORD_OUTCOME, domain)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "extract_ok", extract_ok)


@dataclass(frozen=True)
class SetMode(PolicyEvent):
    mode: str
    reason: str

    def __init__(self, domain: str, mode: str, reason: str):
        super().__init__(PolicyEventType.SET_MODE, domain)
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "reason", reason)


@dataclass(frozen=True)
class SetRetryAfter(PolicyEvent):
    until_iso: str

    def __init__(self, domain: str, until_iso: str):
        super().__init__(PolicyEventType.SET_RETRY_AFTER, domain)
        object.__setattr__(self, "until_iso", until_iso)


@dataclass(frozen=True)
class UpdateRss(PolicyEvent):
    rss_url: str

    def __init__(self, domain: str, rss_url: str):
        super().__init__(PolicyEventType.UPDATE_RSS, domain)
        object.__setattr__(self, "rss_url", rss_url)