from types import SimpleNamespace

import httpx
import pytest

from app.discord import send_case_notification


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def request(self, method, url, json):
        self.calls.append(SimpleNamespace(method=method, url=url, json=json))
        status_code, payload = self.responses.pop(0)
        return httpx.Response(status_code, json=payload, request=httpx.Request(method, url))


@pytest.mark.asyncio
async def test_case_delivery_creates_wait_message_on_routed_webhook(monkeypatch):
    client = FakeClient([(200, {"id": "m1", "channel_id": "noc"})])
    monkeypatch.setenv("DISCORD_NOC_WEBHOOK_URL", "https://discord.invalid/api/webhooks/1/token")
    monkeypatch.setattr("app.discord.httpx.AsyncClient", lambda: client)

    result = await send_case_notification(case_id="case-1", title="Router down", description="BGP failed")

    assert result is not None
    assert result.message_id == "m1"
    assert result.channel_id == "noc"
    assert result.action == "created"
    assert client.calls[0].method == "POST"
    assert client.calls[0].url.endswith("?wait=true")


@pytest.mark.asyncio
async def test_case_delivery_edits_existing_message(monkeypatch):
    client = FakeClient([(200, {"id": "m1", "channel_id": "ai"})])
    monkeypatch.setenv("DISCORD_AI_WEBHOOK_URL", "https://discord.invalid/api/webhooks/2/token")
    monkeypatch.setattr("app.discord.httpx.AsyncClient", lambda: client)

    result = await send_case_notification(
        case_id="case-2",
        title="Model degraded",
        description="Fallback active",
        route="ai",
        message_id="m1",
    )

    assert result is not None and result.action == "updated"
    assert client.calls[0].method == "PATCH"
    assert client.calls[0].url.endswith("/messages/m1")


@pytest.mark.asyncio
async def test_case_delivery_replaces_deleted_message_once(monkeypatch):
    client = FakeClient(
        [
            (404, {"message": "Unknown Message"}),
            (200, {"id": "m2", "channel_id": "ci"}),
        ]
    )
    monkeypatch.setenv("DISCORD_CI_WEBHOOK_URL", "https://discord.invalid/api/webhooks/3/token")
    monkeypatch.setattr("app.discord.httpx.AsyncClient", lambda: client)

    result = await send_case_notification(
        case_id="case-3",
        title="Deploy failed",
        description="Apply failed",
        route="ci",
        message_id="deleted",
    )

    assert result is not None
    assert result.message_id == "m2"
    assert result.action == "replaced"
    assert [call.method for call in client.calls] == ["PATCH", "POST"]


@pytest.mark.asyncio
async def test_ai_case_delivery_uses_bot_and_returns_persistent_id(monkeypatch):
    calls = []

    async def notifier(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(message_id="bot-message", channel_id="ai", action="updated")

    monkeypatch.setattr("app.discord.CASE_BOT_NOTIFIER", notifier)
    result = await send_case_notification(
        case_id="case-ai",
        title="Model degraded",
        description="Fallback active",
        route="ai",
        message_id="existing-message",
    )

    assert result is not None
    assert result.message_id == "bot-message"
    assert calls[0]["message_id"] == "existing-message"
