import pytest
from pydantic_ai.models.test import TestModel

from app.mail import InboundMail, draft_reply_for_message, parse_message, store_draft


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
    from app.mail import MailSettings
    settings = MailSettings(draft_dir=str(tmp_path), imap_password="")
    path = store_draft(draft, settings)

    assert draft.approval_required is True
    assert draft.sent is False
    assert draft.original_recipient == "peering@as215932.net"
    assert path.exists()
