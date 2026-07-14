import asyncio
import enum
import os
from dataclasses import dataclass
from typing import Any, Literal

import httpx

from app.model_metrics import record_discord_delivery, record_sanitized_discord_failure
from app.safe_errors import classify_exception, log_exception


DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
BOT_NOTIFIER = None
CASE_BOT_NOTIFIER = None
NotificationRoute = Literal["network", "ai", "ci"]


@dataclass(frozen=True, slots=True)
class DiscordDeliveryResult:
    message_id: str
    channel_id: str = ""
    action: Literal["created", "updated", "replaced"] = "created"


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
    color: int = 0x3498DB,
    fields: list[dict[str, Any]] | None = None,
    level: Verbosity = Verbosity.INFO,
    route: NotificationRoute = "ai",
) -> DiscordDeliveryResult | None:
    """Send a non-case embed to its explicitly routed Discord webhook."""

    if level < get_verbosity():
        return None
    if BOT_NOTIFIER is not None and route == "ai":
        await BOT_NOTIFIER(title=title, description=description, color=color, fields=fields or [])
        return None

    webhook_url = _webhook_url(route)
    if not webhook_url:
        from app import log

        log.warn(
            "discord_webhook_unset",
            route=route,
            level=level.name,
            title=title,
            description=description,
        )
        return None

    async with httpx.AsyncClient() as client:
        try:
            response = await _request_with_retry(
                client,
                "POST",
                _wait_url(webhook_url),
                _embed_payload(title, description, color, fields),
            )
            result = _delivery_result(response, action="created")
            record_discord_delivery(action="create", route=route, outcome="succeeded")
            return result
        except Exception as exc:
            record_discord_delivery(action="create", route=route, outcome="failed")
            safe = classify_exception(exc)
            log_exception("discord_notification_failed", exc, category=safe.category, route=route)
            return None


async def send_case_notification(
    case_id: str,
    title: str,
    description: str,
    color: int = 0x3498DB,
    fields: list[dict[str, Any]] | None = None,
    level: Verbosity = Verbosity.INFO,
    route: NotificationRoute = "network",
    message_id: str = "",
) -> DiscordDeliveryResult | None:
    """Create or edit the persistent Discord card for one case.

    Case callers persist the returned message/channel ids on their authoritative
    case projection. A missing card is replaced exactly once after Discord
    returns 404. Other failures propagate to the CaseService outbox for retry.
    """

    if level < get_verbosity():
        return None
    if CASE_BOT_NOTIFIER is not None and route == "ai":
        try:
            delivery = await CASE_BOT_NOTIFIER(
                case_id=case_id,
                title=title,
                description=description,
                color=color,
                fields=fields or [],
                message_id=message_id,
            )
        except Exception:
            record_discord_delivery(
                action="update" if message_id else "create",
                route=route,
                outcome="failed",
            )
            raise
        if delivery is None or not getattr(delivery, "message_id", ""):
            raise RuntimeError("Discord bot case delivery returned no persistent message id")
        record_discord_delivery(
            action="update" if message_id else "create",
            route=route,
            outcome="succeeded",
        )
        return delivery
    webhook_url = _webhook_url(route)
    if not webhook_url:
        raise RuntimeError(f"Discord webhook is not configured for route {route}")
    payload = _embed_payload(title, description, color, fields)

    async with httpx.AsyncClient() as client:
        if message_id:
            response = await _request_with_retry(
                client,
                "PATCH",
                f"{webhook_url.rstrip('/')}/messages/{message_id}",
                payload,
                allow_not_found=True,
            )
            if response.status_code != 404:
                record_discord_delivery(action="update", route=route, outcome="succeeded")
                return _delivery_result(response, action="updated", fallback_message_id=message_id)
            record_discord_delivery(action="update", route=route, outcome="not_found")

        response = await _request_with_retry(client, "POST", _wait_url(webhook_url), payload)
        action: Literal["created", "replaced"] = "replaced" if message_id else "created"
        record_discord_delivery(action="replace" if message_id else "create", route=route, outcome="succeeded")
        return _delivery_result(response, action=action)


def _webhook_url(route: NotificationRoute) -> str:
    if route == "network":
        return (os.getenv("DISCORD_NOC_WEBHOOK_URL") or os.getenv("DISCORD_WEBHOOK_URL") or "").strip()
    if route == "ai":
        return (os.getenv("DISCORD_AI_WEBHOOK_URL") or "").strip()
    return (os.getenv("DISCORD_CI_WEBHOOK_URL") or "").strip()


def _embed_payload(
    title: str,
    description: str,
    color: int,
    fields: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    return {
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": color,
                "fields": fields or [],
            }
        ]
    }


def _wait_url(webhook_url: str) -> str:
    return f"{webhook_url}{'&' if '?' in webhook_url else '?'}wait=true"


async def _request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    payload: dict[str, Any],
    *,
    allow_not_found: bool = False,
) -> httpx.Response:
    response: httpx.Response | None = None
    for attempt in range(3):
        response = await client.request(method, url, json=payload)
        if allow_not_found and response.status_code == 404:
            return response
        if response.status_code != 429 and response.status_code < 500:
            response.raise_for_status()
            return response
        if attempt < 2:
            await asyncio.sleep(_retry_delay(response, attempt))
            continue
        response.raise_for_status()
    if response is None:  # pragma: no cover - defensive guard
        raise RuntimeError("Discord request did not run")
    return response


def _retry_delay(response: httpx.Response, attempt: int) -> float:
    value: object = response.headers.get("Retry-After")
    if response.status_code == 429:
        try:
            body = response.json()
            value = body.get("retry_after", value) if isinstance(body, dict) else value
        except ValueError:
            pass
    try:
        return min(5.0, max(0.1, float(value)))
    except (TypeError, ValueError):
        return min(5.0, float(2**attempt))


def _delivery_result(
    response: httpx.Response,
    *,
    action: Literal["created", "updated", "replaced"],
    fallback_message_id: str = "",
) -> DiscordDeliveryResult:
    try:
        body = response.json()
    except ValueError:
        body = {}
    message_id = str(body.get("id") or fallback_message_id or "") if isinstance(body, dict) else fallback_message_id
    channel_id = str(body.get("channel_id") or "") if isinstance(body, dict) else ""
    if not message_id:
        raise RuntimeError("Discord webhook did not return a message id; wait=true is required")
    return DiscordDeliveryResult(message_id=message_id, channel_id=channel_id, action=action)


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
        color=0xF39C12,
        level=level,
        route="ai",
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
        color=0xE74C3C if is_error else 0x2ECC71,
        level=level,
        route="ai",
    )
