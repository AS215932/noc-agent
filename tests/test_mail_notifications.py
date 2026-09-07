import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from app.mail_notifications import report_mailbox_state
from app.mail import MailSettings, process_mailbox_once


@pytest.mark.asyncio
async def test_repeated_outage_recovery_and_recurrence(tmp_path, monkeypatch):
    send = AsyncMock(return_value=True)
    monkeypatch.setattr("app.mail_notifications.send_discord_notification", send)
    for _ in range(24):
        # Every call reopens persisted state, as a restarted poller would.
        await report_mailbox_state(str(tmp_path), failed=True, description="Unavailable")
    assert send.await_count == 1
    for _ in range(3):
        await report_mailbox_state(str(tmp_path), failed=False, description="Recovered")
    assert send.await_count == 2
    assert "recovered" in send.call_args.kwargs["title"]
    await report_mailbox_state(str(tmp_path), failed=True, description="Unavailable again")
    assert send.await_count == 3


@pytest.mark.asyncio
async def test_failed_delivery_retries_without_losing_recovery(tmp_path, monkeypatch):
    send = AsyncMock(side_effect=[False, True, False, True])
    monkeypatch.setattr("app.mail_notifications.send_discord_notification", send)
    for failed in [True, True, False, False, False]:
        await report_mailbox_state(str(tmp_path), failed=failed, description="Safe summary")
    assert send.await_count == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("second_failed", [True, False])
async def test_competing_polls_serialize_transitions(tmp_path, monkeypatch, second_failed):
    sending = asyncio.Event()
    release = asyncio.Event()

    async def sender(**kwargs):
        sending.set()
        await release.wait()
        return True

    send = AsyncMock(side_effect=sender)
    monkeypatch.setattr("app.mail_notifications.send_discord_notification", send)
    first = asyncio.create_task(report_mailbox_state(str(tmp_path), failed=True, description="First"))
    await sending.wait()
    second = asyncio.create_task(report_mailbox_state(str(tmp_path), failed=second_failed, description="Second"))
    await asyncio.sleep(0.01)
    assert not second.done()
    release.set()
    await asyncio.wait_for(asyncio.gather(first, second), timeout=2)
    assert send.await_count == (1 if second_failed else 2)


@pytest.mark.asyncio
async def test_failure_waiting_on_recovery_is_not_dropped(tmp_path, monkeypatch):
    root = tmp_path / ".notifications"
    root.mkdir()
    (root / "mailbox-notification.json").write_text('{"failed":true,"delivered":true}')
    sending = asyncio.Event()
    release = asyncio.Event()

    async def sender(**kwargs):
        sending.set()
        await release.wait()
        return True

    send = AsyncMock(side_effect=sender)
    monkeypatch.setattr("app.mail_notifications.send_discord_notification", send)
    recovery = asyncio.create_task(report_mailbox_state(str(tmp_path), failed=False, description="Recovered"))
    await sending.wait()
    failure = asyncio.create_task(report_mailbox_state(str(tmp_path), failed=True, description="Transient failure"))
    await asyncio.sleep(0.01)
    release.set()
    await asyncio.wait_for(asyncio.gather(recovery, failure), timeout=2)
    assert send.await_count == 2
    assert "unavailable" in send.call_args.kwargs["title"]


@pytest.mark.asyncio
async def test_healthy_start_is_silent(tmp_path, monkeypatch):
    send = AsyncMock(return_value=True)
    monkeypatch.setattr("app.mail_notifications.send_discord_notification", send)
    await report_mailbox_state(str(tmp_path), failed=False, description="Healthy")
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_poll_failure_stays_visible_without_repeated_posts(tmp_path, mocker):
    fetch = mocker.patch("app.mail.fetch_unseen_messages", side_effect=RuntimeError("unavailable"))
    mocker.patch("app.discord.send_discord_notification", new_callable=AsyncMock)
    send = mocker.patch("app.mail_notifications.send_discord_notification", new_callable=AsyncMock,
                        return_value=True)
    settings = MailSettings(draft_dir=str(tmp_path))
    for _ in range(24):
        with pytest.raises(RuntimeError, match="unavailable"):
            await process_mailbox_once(settings=settings)
    assert send.await_count == 1
    fetch.side_effect = None
    fetch.return_value = []
    await process_mailbox_once(settings=settings)
    assert send.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("contents", ['{', '{}', 'null', '{"failed":"true"}'])
@pytest.mark.parametrize("failed", [True, False])
async def test_damaged_state_does_not_disable_notifications(tmp_path, monkeypatch, contents, failed):
    root = tmp_path / ".notifications"
    root.mkdir()
    (root / "mailbox-notification.json").write_text(contents)
    send = AsyncMock(return_value=True)
    monkeypatch.setattr("app.mail_notifications.send_discord_notification", send)
    for _ in range(3):
        await report_mailbox_state(str(tmp_path), failed=failed, description="Current state")
    assert send.await_count == 1


@pytest.mark.asyncio
async def test_bot_without_channel_does_not_commit_notification_state(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from app.discord_bot import NOCDiscordBot

    channel = SimpleNamespace(send=AsyncMock())
    bot = SimpleNamespace(channel_id=123, client=SimpleNamespace(get_channel=lambda _: None))

    async def notifier(**kwargs):
        return await NOCDiscordBot.send_embed(bot, **kwargs)

    monkeypatch.setattr("app.discord.BOT_NOTIFIER", notifier)
    await report_mailbox_state(str(tmp_path), failed=True, description="Unavailable")
    state = json.loads((tmp_path / ".notifications/mailbox-notification.json").read_text())
    assert state == {"failed": True, "delivered": False}
    bot.client.get_channel = lambda _: channel
    await report_mailbox_state(str(tmp_path), failed=True, description="Unavailable")
    await report_mailbox_state(str(tmp_path), failed=True, description="Unavailable")
    channel.send.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("level", ["WARNING", "ERROR"])
async def test_filtered_recovery_does_not_hide_next_outage(tmp_path, monkeypatch, level):
    send = AsyncMock(return_value=True)
    categories = []
    monkeypatch.setenv("LOG_LEVEL_DISCORD", level)
    monkeypatch.setattr("app.discord.BOT_NOTIFIER", send)
    monkeypatch.setattr("app.mail_notifications.record_sanitized_discord_failure", categories.append)
    for failed in [True, True, False, False, True, True]:
        await report_mailbox_state(str(tmp_path), failed=failed, description="Current state",
                                   safe_category="unknown_infrastructure")
    assert send.await_count == 2
    assert categories == ["unknown_infrastructure", "unknown_infrastructure"]
