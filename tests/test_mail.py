import pytest
from pydantic_ai.models.test import TestModel
from unittest.mock import AsyncMock

from app.agent import MailDraftPlan
from app.discord import Verbosity
from app.mail import (
    InboundMail,
    MailSettings,
    StoredDraft,
    draft_reply_for_message,
    mail_needs_no_response,
    parse_message,
    process_mailbox_once,
    store_draft,
)


def test_parse_message_preserves_original_recipient():
    raw = b"""From: peer@example.net
To: noc@as215932.net
X-Original-To: peering@as215932.net
Subject: Peering request
Message-ID: <peer-1@example.net>

Hello, we would like to peer at LocIX.
"""

    message = parse_message("42", raw)

    assert message.uid == "42"
    assert message.original_recipient == "peering@as215932.net"
    assert message.from_address == "peer@example.net"
    assert "LocIX" in message.text_body


@pytest.mark.asyncio
async def test_draft_reply_requires_human_approval(tmp_path):
    message = InboundMail(
        uid="42",
        message_id="<peer-1@example.net>",
        from_address="peer@example.net",
        to_addresses=["peering@as215932.net"],
        original_recipient="peering@as215932.net",
        subject="Peering request",
        text_body="Hello, we would like to peer at LocIX.",
    )

    draft = await draft_reply_for_message(message, model=TestModel())
    settings = MailSettings(draft_dir=str(tmp_path), imap_password="")
    path = store_draft(draft, settings)

    assert draft.approval_required is True
    assert draft.sent is False
    assert draft.original_recipient == "peering@as215932.net"
    assert path.exists()


@pytest.mark.asyncio
async def test_empty_mailbox_poll_finishes_at_debug_level(mocker):
    mocker.patch("app.mail.fetch_unseen_messages", return_value=[])
    send_discord = mocker.patch("app.discord.send_discord_notification", new_callable=AsyncMock)

    drafts = await process_mailbox_once(settings=MailSettings(imap_password="secret"))

    assert drafts == []
    assert send_discord.call_count == 2
    assert send_discord.call_args_list[0].kwargs["level"] == Verbosity.DEBUG
    assert send_discord.call_args_list[1].kwargs["level"] == Verbosity.DEBUG
    assert send_discord.call_args_list[1].kwargs["description"] == "No new messages."


def test_system_report_mail_is_marked_no_response():
    message = InboundMail(
        uid="43",
        from_address="root@mail.as215932.net",
        to_addresses=["noc@as215932.net"],
        subject="mail daily security output",
        text_body="Automated daily security output from mail.as215932.net",
    )

    assert mail_needs_no_response(message) is True


@pytest.mark.asyncio
async def test_cron_mail_is_summarized_without_draft(mocker):
    message = InboundMail(
        uid="43",
        from_address="root@mail.as215932.net",
        to_addresses=["noc@as215932.net"],
        subject="mail daily output",
        text_body="Automated daily system report from mail.as215932.net",
    )
    mocker.patch("app.mail.fetch_unseen_messages", return_value=[message])
    draft_reply = mocker.patch("app.mail.draft_reply_for_message", new_callable=AsyncMock)
    store = mocker.patch("app.mail.store_draft")
    send_discord = mocker.patch("app.discord.send_discord_notification", new_callable=AsyncMock)

    drafts = await process_mailbox_once(settings=MailSettings(imap_password="secret"))

    assert drafts == []
    draft_reply.assert_not_called()
    store.assert_not_called()
    description = send_discord.call_args_list[1].kwargs["description"]
    assert "created 0 drafts" in description
    assert "marked 1 no-response" in description
    assert "mail daily output" in description
    assert "no response needed" in description


@pytest.mark.asyncio
async def test_mailbox_poll_keeps_human_draft_and_system_summary(mocker):
    human_message = InboundMail(
        uid="44",
        from_address="peer@example.net",
        to_addresses=["peering@as215932.net"],
        original_recipient="peering@as215932.net",
        subject="Peering request",
        text_body="Hello, we would like to peer at LocIX.",
    )
    system_message = InboundMail(
        uid="45",
        from_address="root@mail.as215932.net",
        to_addresses=["noc@as215932.net"],
        subject="mail daily security output",
        text_body="Checking setuid files and pf permissions",
    )
    draft = StoredDraft(
        draft_id="draft-44",
        created_at="2026-05-13T00:00:00+00:00",
        source_uid="44",
        source_message_id=None,
        original_recipient="peering@as215932.net",
        sender="peer@example.net",
        subject="Peering request",
        plan=MailDraftPlan(
            classification="peering",
            urgency="LOW",
            summary="A peer requests LocIX peering.",
            suggested_reply_subject="Re: Peering request",
            suggested_reply_body="Thanks, we will review this peering request.",
            reply_summary="Acknowledges the request for human review.",
            requires_human=True,
        ),
    )
    mocker.patch("app.mail.fetch_unseen_messages", return_value=[human_message, system_message])
    mocker.patch("app.mail.draft_reply_for_message", new=AsyncMock(return_value=draft))
    store = mocker.patch("app.mail.store_draft")
    send_discord = mocker.patch("app.discord.send_discord_notification", new_callable=AsyncMock)

    drafts = await process_mailbox_once(settings=MailSettings(imap_password="secret"))

    assert drafts == [draft]
    store.assert_called_once_with(draft, MailSettings(imap_password="secret"))
    description = send_discord.call_args_list[1].kwargs["description"]
    assert "Processed 2 messages, created 1 drafts, marked 1 no-response." in description
    assert "Peering request" in description
    assert "mail daily security output" in description
    assert "no response needed" in description
