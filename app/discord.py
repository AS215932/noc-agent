import os
import httpx
from typing import Any

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

async def send_discord_notification(title: str, description: str, color: int = 0x3498db, fields: list[dict[str, Any]] = None):
    """
    Sends an embed message to a Discord webhook.
    """
    if not DISCORD_WEBHOOK_URL:
        print("Warning: DISCORD_WEBHOOK_URL not set. Skipping Discord notification.")
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
