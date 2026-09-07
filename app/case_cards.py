"""Durable Discord case-card identities shared by bot and webhook delivery.

Only message IDs and content hashes are stored, never webhook credentials or case
text. A process lock serializes concurrent updates across workers and restarts.
"""
from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
from enum import Enum
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

from app import log
from app.safe_errors import classify_exception


LOCK_TIMEOUT_S = 30
DELIVERY_TIMEOUT_S = 60
CARD_REFRESH_S = 6 * 3600


class CardDeliveryOutcome(Enum):
    SUPERSEDED = "superseded"


class CardNotFound(Exception):
    """The transport confirmed that the previous card was deleted."""


async def deliver_case_card(
    *,
    destination: str,
    case_id: str,
    payload: dict[str, Any],
    revision: float | None = None,
    force_refresh: bool = False,
    create: Callable[[], Awaitable[int | None]],
    edit: Callable[[int], Awaitable[bool]],
) -> bool | CardDeliveryOutcome:
    revision = time.time() if revision is None else revision
    directory = os.getenv("DISCORD_CASE_STATE_DIR") or str(
        Path(os.getenv("MAIL_DRAFT_DIR", "data/mail-drafts")) / ".notifications" / "case-cards"
    )
    key = hashlib.sha256(f"{destination}\0{case_id}".encode()).hexdigest()
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    root = Path(directory)
    try:
        root.mkdir(parents=True, exist_ok=True)
        with (root / f"{key}.lock").open("a") as lock:
            lock_deadline = asyncio.get_running_loop().time() + LOCK_TIMEOUT_S
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if asyncio.get_running_loop().time() >= lock_deadline:
                        raise TimeoutError("case card lock timed out")
                    await asyncio.sleep(0.05)
            try:
                path = root / f"{key}.json"
                try:
                    state = json.loads(path.read_text())
                    message_id = state["message_id"]
                    previous = state["digest"]
                    previous_revision = state.get("revision", 0)
                    if not isinstance(previous_revision, (int, float)):
                        previous_revision = 0
                    verified_at = state.get("verified_at", 0)
                    if not isinstance(verified_at, (int, float)):
                        verified_at = 0
                    if type(message_id) is not int or message_id <= 0 or not isinstance(previous, str):
                        raise ValueError("invalid card state")
                except FileNotFoundError:
                    message_id, previous, verified_at, previous_revision = None, None, 0, 0
                except (ValueError, KeyError, TypeError):
                    log.warn("discord_case_state_invalid", case_id=case_id)
                    message_id, previous, verified_at, previous_revision = None, None, 0, 0
                verified_now = time.time()
                if message_id is not None:
                    if revision < previous_revision:
                        return CardDeliveryOutcome.SUPERSEDED
                    if not force_refresh and previous == digest and 0 <= verified_now - verified_at < CARD_REFRESH_S:
                        if revision == previous_revision:
                            return True
                        # Advance ordering without a network request or falsely
                        # refreshing the last time Discord confirmed the card.
                        verified_now = verified_at
                    else:
                        try:
                            if not await asyncio.wait_for(edit(message_id), timeout=DELIVERY_TIMEOUT_S):
                                return False
                        except CardNotFound:
                            message_id = None
                if message_id is None:
                    message_id = await asyncio.wait_for(create(), timeout=DELIVERY_TIMEOUT_S)
                    if type(message_id) is not int or message_id <= 0:
                        return False
                temporary = path.with_suffix(".tmp")
                with temporary.open("w") as output:
                    json.dump({"message_id": message_id, "digest": digest, "verified_at": verified_now, "revision": revision}, output)
                    output.write("\n")
                    output.flush()
                    os.fsync(output.fileno())
                temporary.replace(path)
                directory_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
                return True
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)
    except Exception as exc:
        safe = classify_exception(exc)
        # HTTP exceptions can contain a credential-bearing webhook URL.
        log.warn("discord_case_delivery_failed", error_type=type(exc).__name__,
                 category=safe.category, case_id=case_id)
        return False
