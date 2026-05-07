import os
import httpx
from typing import Any
import enum

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

class Verbosity(enum.IntEnum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40

def get_verbosity() -> Verbosity:
    level_str = os.environ.get("LOG_LEVEL_DISCORD", "INFO").upper()
    return getattr(Verbosity, level_str, Verbosity.INFO)

async def send_discord_notification(title: str, description: str, color: int = 0x3498db, fields: list[dict[str, Any]] = None, level: Verbosity = Verbosity.INFO):
    """
    Sends an embed message to a Discord webhook.
    """
    if level < get_verbosity():
        return

    if not DISCORD_WEBHOOK_URL:
        print(f"[{level.name}] {title}: {description}")
        return

    payload = {
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": color,
                "fields": fields or []
            }
        ]
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(DISCORD_WEBHOOK_URL, json=payload)
            response.raise_for_status()
        except httpx.HTTPError as e:
            print(f"Error sending Discord notification: {e}")

async def notify_start(task_name: str, description: str):
    await send_discord_notification(
        title=f"⏳ Starting: {task_name}",
        description=description,
        color=0xf39c12, # Orange
        level=Verbosity.DEBUG
    )

async def notify_finish(task_name: str, description: str, is_error: bool = False):
    await send_discord_notification(
        title=f"{'❌ Failed' if is_error else '✅ Finished'}: {task_name}",
        description=description,
        color=0xe74c3c if is_error else 0x2ecc71,
        level=Verbosity.INFO
    )
