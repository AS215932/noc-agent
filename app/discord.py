import os
import httpx
from typing import Any
import enum

from app.model_metrics import record_sanitized_discord_failure
from app.safe_errors import classify_exception, log_exception

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
BOT_NOTIFIER = None
CASE_BOT_NOTIFIER = None

class Verbosity(enum.IntEnum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40

def get_verbosity() -> Verbosity:
    level_str = os.environ.get("LOG_LEVEL_DISCORD", "INFO").upper()
    return getattr(Verbosity, level_str, Verbosity.INFO)

async def send_discord_notification(
    title: str,
    description: str,
    color: int = 0x3498db,
    fields: list[dict[str, Any]] | None = None,
    level: Verbosity = Verbosity.INFO,
):
    """
    Sends an embed message to a Discord webhook.
    """
    if level < get_verbosity():
        return

    if BOT_NOTIFIER is not None:
        await BOT_NOTIFIER(title=title, description=description, color=color, fields=fields or [])
        return

    if not DISCORD_WEBHOOK_URL:
        from app import log
        log.warn(
            "discord_webhook_unset",
            level=level.name,
            title=title,
            description=description,
        )
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
            safe = classify_exception(e)
            log_exception("discord_notification_failed", e, category=safe.category)


async def send_case_notification(
    case_id: str,
    title: str,
    description: str,
    color: int = 0x3498db,
    fields: list[dict[str, Any]] | None = None,
    level: Verbosity = Verbosity.INFO,
):
    if level < get_verbosity():
        return
    if CASE_BOT_NOTIFIER is not None:
        await CASE_BOT_NOTIFIER(
            case_id=case_id,
            title=title,
            description=description,
            color=color,
            fields=fields or [],
        )
        return
    await send_discord_notification(title=title, description=description, color=color, fields=fields or [], level=level)


def install_bot_notifier(notifier):
    global BOT_NOTIFIER
    BOT_NOTIFIER = notifier


def install_case_notifier(notifier):
    global CASE_BOT_NOTIFIER
    CASE_BOT_NOTIFIER = notifier

async def notify_start(task_name: str, description: str, level: Verbosity = Verbosity.DEBUG):
    await send_discord_notification(
        title=f"⏳ Starting: {task_name}",
        description=description,
        color=0xf39c12, # Orange
        level=level
    )

async def notify_finish(
    task_name: str,
    description: str,
    is_error: bool = False,
    level: Verbosity | None = None,
    safe_category: str | None = None,
):
    if is_error:
        record_sanitized_discord_failure(safe_category or "unknown_infrastructure")
    level = level or (Verbosity.ERROR if is_error else Verbosity.INFO)
    await send_discord_notification(
        title=f"{'❌ Failed' if is_error else '✅ Finished'}: {task_name}",
        description=description,
        color=0xe74c3c if is_error else 0x2ecc71,
        level=level
    )
