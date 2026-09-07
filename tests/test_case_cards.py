import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from app.case_cards import CardNotFound, deliver_case_card


@pytest.fixture(autouse=True)
def state_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("DISCORD_CASE_STATE_DIR", str(tmp_path))


async def deliver(create, edit, *, description="first", case_id="case-1", destination="bot:42"):
    return await deliver_case_card(
        destination=destination, case_id=case_id,
        payload={"description": description}, create=create, edit=edit,
    )


@pytest.mark.asyncio
async def test_card_survives_restart_and_identical_updates_are_quiet():
    create, edit = AsyncMock(return_value=123), AsyncMock(return_value=True)
    assert await deliver(create, edit)
    # New callbacks represent a new bot process with no message cache.
    restarted_create, restarted_edit = AsyncMock(return_value=456), AsyncMock(return_value=True)
    assert await deliver(restarted_create, restarted_edit)
    assert await deliver(restarted_create, restarted_edit, description="investigation failed")
    restarted_create.assert_not_called()
    restarted_edit.assert_awaited_once_with(123)


@pytest.mark.asyncio
async def test_concurrent_first_notifications_create_one_card():
    create, edit = AsyncMock(return_value=123), AsyncMock(return_value=True)
    await asyncio.gather(*(deliver(create, edit) for _ in range(20)))
    create.assert_awaited_once()
    edit.assert_not_called()


@pytest.mark.asyncio
async def test_transient_edit_failure_does_not_create_duplicate():
    create, edit = AsyncMock(return_value=123), AsyncMock(return_value=True)
    await deliver(create, edit)
    edit.side_effect = TimeoutError("temporary")
    assert not await deliver(create, edit, description="failed")
    create.assert_awaited_once()
    edit.side_effect = None
    assert await deliver(create, edit, description="failed")
    assert edit.await_count == 2


@pytest.mark.asyncio
async def test_deleted_card_is_replaced_and_new_identity_is_persisted():
    create, edit = AsyncMock(side_effect=[123, 456]), AsyncMock(return_value=True)
    await deliver(create, edit)
    edit.side_effect = CardNotFound
    assert await deliver(create, edit, description="changed")
    edit.side_effect = None
    assert await deliver(create, edit, description="recovered")
    edit.assert_awaited_with(456)
    assert create.await_count == 2


@pytest.mark.asyncio
async def test_failed_create_remains_retryable():
    create, edit = AsyncMock(side_effect=[None, 123]), AsyncMock(return_value=True)
    assert not await deliver(create, edit)
    assert await deliver(create, edit)
    assert create.await_count == 2


@pytest.mark.asyncio
async def test_destinations_and_cases_have_independent_identities(tmp_path):
    create, edit = AsyncMock(return_value=123), AsyncMock(return_value=True)
    await deliver(create, edit)
    await deliver(create, edit, destination="webhook:private-token")
    await deliver(create, edit, case_id="case-2")
    assert create.await_count == 3
    files = list(tmp_path.glob("*.json"))
    assert len(files) == 3
    for path in files:
        assert set(json.loads(path.read_text())) == {"message_id", "digest"}
        assert "private-token" not in path.name + path.read_text()


@pytest.mark.asyncio
async def test_corrupt_state_does_not_hide_current_incident(tmp_path):
    create, edit = AsyncMock(return_value=123), AsyncMock(return_value=True)
    await deliver(create, edit)
    next(tmp_path.glob("*.json")).write_text("broken")
    assert await deliver(create, edit)
    assert create.await_count == 2

@pytest.mark.asyncio
async def test_webhook_creates_once_then_patches_across_calls(monkeypatch):
    import httpx
    import app.discord as notifications

    requests = []
    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"id": "123"})

    constructor = httpx.AsyncClient
    monkeypatch.setattr(notifications.httpx, "AsyncClient", lambda: constructor(transport=httpx.MockTransport(handler)))
    monkeypatch.setattr(notifications, "CASE_BOT_NOTIFIER", None)
    monkeypatch.setattr(notifications, "DISCORD_WEBHOOK_URL", "https://discord.invalid/api/webhooks/42/secret?thread_id=7")
    assert await notifications.send_case_notification("case-1", "New incident", "first")
    assert await notifications.send_case_notification("case-1", "New incident", "first")
    assert await notifications.send_case_notification("case-1", "Investigation failed", "dependency unavailable")
    assert [r.method for r in requests] == ["POST", "PATCH"]
    assert requests[0].url.params["wait"] == "true"
    assert requests[1].url.path.endswith("/messages/123")
    assert requests[1].url.params["thread_id"] == "7"


@pytest.mark.asyncio
async def test_webhook_transient_failure_preserves_existing_card(monkeypatch):
    import httpx
    import app.discord as notifications

    requests = []
    def handler(request):
        requests.append(request)
        if request.method == "PATCH":
            return httpx.Response(503)
        return httpx.Response(200, json={"id": "123"})

    constructor = httpx.AsyncClient
    monkeypatch.setattr(notifications.httpx, "AsyncClient", lambda: constructor(transport=httpx.MockTransport(handler)))
    monkeypatch.setattr(notifications, "CASE_BOT_NOTIFIER", None)
    monkeypatch.setattr(notifications, "DISCORD_WEBHOOK_URL", "https://discord.invalid/api/webhooks/42/secret")
    assert await notifications.send_case_notification("case-1", "New incident", "first")
    assert not await notifications.send_case_notification("case-1", "Failed", "dependency unavailable")
    assert [r.method for r in requests] == ["POST", "PATCH"]


@pytest.mark.asyncio
async def test_bot_restart_edits_persisted_card(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock
    from app.discord_bot import NOCDiscordBot

    monkeypatch.setenv("DISCORD_BOT_CHANNEL_ID", "42")
    message = SimpleNamespace(id=123, edit=AsyncMock())
    channel = SimpleNamespace(send=AsyncMock(return_value=message), get_partial_message=Mock(return_value=message))
    first = NOCDiscordBot()
    first.client._connection.user = SimpleNamespace(id=999)
    monkeypatch.setattr(first.client, "get_channel", lambda _: channel)
    assert await first.send_case_embed("case-1", "Incident", "first", 0)
    restarted = NOCDiscordBot()
    restarted.client._connection.user = SimpleNamespace(id=999)
    monkeypatch.setattr(restarted.client, "get_channel", lambda _: channel)
    assert await restarted.send_case_embed("case-1", "Failed", "dependency unavailable", 0)
    channel.send.assert_awaited_once()
    channel.get_partial_message.assert_called_once_with(123)
    message.edit.assert_awaited_once()


@pytest.mark.asyncio
async def test_triage_failure_updates_case_without_standalone_failure_message(monkeypatch):
    import app.main as main

    card = AsyncMock(return_value=True)
    finish = AsyncMock()
    monkeypatch.setattr(main, "send_case_notification", card)
    monkeypatch.setattr(main, "notify_finish", finish)
    monkeypatch.setattr(main, "_take_ownership_ack", AsyncMock())
    monkeypatch.setattr(main, "run_investigation_graph", AsyncMock(side_effect=TimeoutError("backend")))
    case = {"incident_id": "case-stable", "title": "Disk on rtr"}
    result = await main.investigate_alert({"status": "firing", "alerts": []}, case=case)
    assert result is None
    assert card.await_count == 2
    assert {call.kwargs["case_id"] for call in card.await_args_list} == {"case-stable"}
    assert card.await_args_list[-1].kwargs["title"].startswith("❌ Investigation unavailable:")
    finish.assert_not_called()

@pytest.mark.asyncio
async def test_transport_exception_does_not_log_webhook_credentials(capsys):
    secret = "https://discord.invalid/api/webhooks/42/private-webhook-token"
    create = AsyncMock(side_effect=RuntimeError(secret))
    assert not await deliver(create, AsyncMock())
    output = capsys.readouterr()
    assert "private-webhook-token" not in output.out + output.err


@pytest.mark.asyncio
@pytest.mark.parametrize("code,methods", [(10008, ["POST", "PATCH", "POST"]), (10015, ["POST", "PATCH"])])
async def test_webhook_only_replaces_confirmed_missing_message(monkeypatch, code, methods):
    import httpx
    import app.discord as notifications

    requests = []
    def handler(request):
        requests.append(request.method)
        if request.method == "PATCH":
            return httpx.Response(404, json={"code": code})
        return httpx.Response(200, json={"id": "123"})

    constructor = httpx.AsyncClient
    monkeypatch.setattr(notifications.httpx, "AsyncClient", lambda: constructor(transport=httpx.MockTransport(handler)))
    monkeypatch.setattr(notifications, "CASE_BOT_NOTIFIER", None)
    monkeypatch.setattr(notifications, "DISCORD_WEBHOOK_URL", "https://discord.invalid/api/webhooks/42/secret")
    assert await notifications.send_case_notification("case-1", "New", "first")
    result = await notifications.send_case_notification("case-1", "Failed", "dependency unavailable")
    assert result is (code == 10008)
    assert requests == methods

@pytest.mark.asyncio
async def test_bot_account_replacement_creates_new_namespace(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock
    from app.discord_bot import NOCDiscordBot

    monkeypatch.setenv("DISCORD_BOT_CHANNEL_ID", "42")
    message = SimpleNamespace(id=123, edit=AsyncMock())
    channel = SimpleNamespace(send=AsyncMock(return_value=message), get_partial_message=Mock(return_value=message))
    for bot_id in [999, 1000]:
        bot = NOCDiscordBot()
        bot.client._connection.user = SimpleNamespace(id=bot_id)
        monkeypatch.setattr(bot.client, "get_channel", lambda _: channel)
        assert await bot.send_case_embed("case-1", "Incident", "first", 0)
    assert channel.send.await_count == 2
    channel.get_partial_message.assert_not_called()


@pytest.mark.asyncio
async def test_persistence_syncs_file_then_directory(monkeypatch):
    import os
    import stat

    kinds = []
    original = os.fsync
    def sync(fd):
        kinds.append("directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file")
        original(fd)
    monkeypatch.setattr(os, "fsync", sync)
    assert await deliver(AsyncMock(return_value=123), AsyncMock())
    assert kinds == ["file", "directory"]
