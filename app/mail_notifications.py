"""Persist mailbox outage transitions independently of model availability.

The poller already has a process lock; a separate notification lock also covers
manual polls. State contains no mail content, credentials, or exception text.
"""

import asyncio
import fcntl
import json
from pathlib import Path

from app.discord import Verbosity, send_discord_notification
from app.model_metrics import record_sanitized_discord_failure


async def report_mailbox_state(
    directory: str, *, failed: bool, description: str, safe_category: str = "unknown_infrastructure",
) -> None:
    root = Path(directory) / ".notifications"
    if not failed and not root.exists():
        return
    root.mkdir(parents=True, exist_ok=True)
    path = root / "mailbox-notification.json"
    with (root / "mailbox-notification.lock").open("a") as lock:
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                # Serialize different outcomes without blocking the event loop
                # or dropping a transient failure between healthy polls.
                await asyncio.sleep(0.05)
        try:
            try:
                state = json.loads(path.read_text())
                previous = state["failed"]
                was_delivered = state.get("delivered", True)
                if not isinstance(previous, bool) or not isinstance(was_delivered, bool):
                    previous, was_delivered = None, False
            except FileNotFoundError:
                previous, was_delivered = False, True
            except (ValueError, KeyError, TypeError, OSError):
                # Dedup state must not become an alert-delivery dependency.
                # Unknown state re-announces the current condition.
                previous, was_delivered = None, False
            if previous == failed and was_delivered:
                return
            delivered = False
            try:
                delivered = await send_discord_notification(
                    title="❌ Mailbox polling unavailable" if failed else "✅ Mailbox polling recovered",
                    description=description,
                    color=0xE74C3C if failed else 0x2ECC71,
                    level=Verbosity.ERROR if failed else Verbosity.INFO,
                )
                if delivered and failed:
                    record_sanitized_discord_failure(safe_category)
            finally:
                # Observed health and delivery are separate: an undelivered or
                # filtered recovery must not suppress the next distinct outage.
                # Failed delivery remains retryable on subsequent identical polls.
                temporary = path.with_suffix(".tmp")
                temporary.write_text(json.dumps({"failed": failed, "delivered": bool(delivered)}) + "\n")
                temporary.replace(path)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
