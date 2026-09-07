"""Persist mailbox outage transitions independently of model availability.

The poller already has a process lock; a separate notification lock also covers
manual polls. State contains no mail content, credentials, or exception text.
"""

import fcntl
import json
from pathlib import Path

from app.discord import Verbosity, send_discord_notification


async def report_mailbox_state(directory: str, *, failed: bool, description: str) -> None:
    root = Path(directory) / ".notifications"
    if not failed and not root.exists():
        return
    root.mkdir(parents=True, exist_ok=True)
    path = root / "mailbox-notification.json"
    with (root / "mailbox-notification.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return  # The next poll retries if the other sender fails.
        try:
            previous = json.loads(path.read_text())["failed"] if path.exists() else False
            if previous == failed:
                return
            delivered = await send_discord_notification(
                title="❌ Mailbox polling unavailable" if failed else "✅ Mailbox polling recovered",
                description=description,
                color=0xE74C3C if failed else 0x2ECC71,
                level=Verbosity.ERROR if failed else Verbosity.INFO,
            )
            if delivered:
                temporary = path.with_suffix(".tmp")
                temporary.write_text(json.dumps({"failed": failed}) + "\n")
                temporary.replace(path)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
