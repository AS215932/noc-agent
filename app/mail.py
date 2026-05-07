import hashlib
import imaplib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from email import policy
from email.message import Message
from email.parser import BytesParser
from email.utils import getaddresses
from pathlib import Path

from pydantic import BaseModel, Field

from app.agent import MailDraftPlan, noc_mail_agent
from app.model_metrics import record_failure, record_success, start_run
from app.safe_errors import classify_exception, log_exception


ROLE_ADDRESSES = {
    "noc@as215932.net",
    "abuse@as215932.net",
    "peering@as215932.net",
    "dh@as215932.net",
}


class InboundMail(BaseModel):
    uid: str
    message_id: str | None = None
    from_address: str
    to_addresses: list[str]
    original_recipient: str | None = None
    subject: str
    text_body: str
    received_at: str | None = None


class StoredDraft(BaseModel):
    draft_id: str
    created_at: str
    source_uid: str
    source_message_id: str | None
    original_recipient: str | None
    sender: str
    subject: str
    plan: MailDraftPlan
    approval_required: bool = True
    sent: bool = False


@dataclass(frozen=True)
class MailSettings:
    imap_host: str = "mail.as215932.net"
    imap_port: int = 993
    imap_user: str = "noc"
    imap_password: str = ""
    imap_timeout: float = 10.0
    smtp_host: str = "mail.as215932.net"
    smtp_port: int = 587
    smtp_user: str = "noc"
    mailbox: str = "INBOX"
    draft_dir: str = "/tmp/noc-agent-mail-drafts"

    @classmethod
    def from_env(cls) -> "MailSettings":
        return cls(
            imap_host=os.getenv("MAIL_IMAP_HOST", cls.imap_host),
            imap_port=int(os.getenv("MAIL_IMAP_PORT", str(cls.imap_port))),
            imap_user=os.getenv("MAIL_IMAP_USER", cls.imap_user),
            imap_password=os.getenv("MAIL_IMAP_PASSWORD", ""),
            imap_timeout=float(os.getenv("MAIL_IMAP_TIMEOUT", str(cls.imap_timeout))),
            smtp_host=os.getenv("MAIL_SMTP_HOST", cls.smtp_host),
            smtp_port=int(os.getenv("MAIL_SMTP_PORT", str(cls.smtp_port))),
            smtp_user=os.getenv("MAIL_SMTP_USER", os.getenv("MAIL_IMAP_USER", cls.smtp_user)),
            mailbox=os.getenv("MAIL_IMAP_MAILBOX", cls.mailbox),
            draft_dir=os.getenv("MAIL_DRAFT_DIR", cls.draft_dir),
        )


def parse_message(uid: str, raw_message: bytes) -> InboundMail:
    message = BytesParser(policy=policy.default).parsebytes(raw_message)
    text_body = _extract_text_body(message)
    to_addresses = _addresses_from_headers(message, ["to", "cc", "delivered-to", "x-original-to"])
    original_recipient = _find_original_recipient(message, to_addresses)

    return InboundMail(
        uid=uid,
        message_id=message.get("message-id"),
        from_address=_first_address(message.get("from", "")),
        to_addresses=to_addresses,
        original_recipient=original_recipient,
        subject=message.get("subject", "(no subject)"),
        text_body=text_body,
        received_at=message.get("date"),
    )


def fetch_unseen_messages(settings: MailSettings) -> list[InboundMail]:
    if not settings.imap_password:
        raise RuntimeError("MAIL_IMAP_PASSWORD is required to poll the NOC mailbox")

    with imaplib.IMAP4_SSL(settings.imap_host, settings.imap_port, timeout=settings.imap_timeout) as client:
        client.login(settings.imap_user, settings.imap_password)
        client.select(settings.mailbox)
        status, data = client.uid("search", None, "UNSEEN")
        if status != "OK":
            raise RuntimeError(f"IMAP search failed: {status}")

        messages: list[InboundMail] = []
        for uid_bytes in data[0].split():
            uid = uid_bytes.decode()
            fetch_status, fetch_data = client.uid("fetch", uid, "(RFC822)")
            if fetch_status != "OK" or not fetch_data:
                continue
            raw = _raw_rfc822(fetch_data)
            if raw is None:
                continue
            messages.append(parse_message(uid, raw))
        return messages


async def draft_reply_for_message(message: InboundMail, model=None) -> StoredDraft:
    prompt = (
        "Draft a response for this AS215932 operational email. "
        "The response must be stored for human approval and must not be sent.\n\n"
        f"Original recipient: {message.original_recipient}\n"
        f"From: {message.from_address}\n"
        f"To: {', '.join(message.to_addresses)}\n"
        f"Subject: {message.subject}\n"
        f"Body:\n{message.text_body}"
    )
    run_started = start_run("mail")
    try:
        result = await noc_mail_agent.run(prompt, model=model)
        plan = result.data if hasattr(result, "data") else result.output
        record_success("mail", run_started, result)
    except Exception as exc:
        safe = classify_exception(exc)
        record_failure("mail", run_started, safe)
        log_exception("mail_draft_model_failed", exc, category=safe.category, provider=safe.provider, model=safe.model_name)
        raise
    plan.requires_human = True

    draft_id = _draft_id(message)
    return StoredDraft(
        draft_id=draft_id,
        created_at=datetime.now(timezone.utc).isoformat(),
        source_uid=message.uid,
        source_message_id=message.message_id,
        original_recipient=message.original_recipient,
        sender=message.from_address,
        subject=message.subject,
        plan=plan,
    )


def _save_draft_to_imap(draft: StoredDraft, settings: MailSettings):
    from email.message import EmailMessage
    import time

    msg = EmailMessage()
    # If drafting reply, From should be us, To should be them. But here it's meant to be a Draft in our box.
    msg["From"] = "noc@as215932.net"
    msg["To"] = draft.sender # the original sender we are replying to
    msg["Subject"] = draft.plan.suggested_reply_subject
    msg.set_content(draft.plan.suggested_reply_body)

    try:
        with imaplib.IMAP4_SSL(settings.imap_host, settings.imap_port, timeout=settings.imap_timeout) as client:
            client.login(settings.imap_user, settings.imap_password)
            client.append("Drafts", r"(\Draft \Seen)", imaplib.Time2Internaldate(time.time()), msg.as_bytes())
    except Exception as e:
        safe = classify_exception(e)
        log_exception("mail_imap_draft_save_failed", e, category=safe.category)

def store_draft(draft: StoredDraft, settings: MailSettings) -> Path:
    path = Path(settings.draft_dir)
    path.mkdir(parents=True, exist_ok=True)
    draft_path = path / f"{draft.draft_id}.json"
    draft_path.write_text(draft.model_dump_json(indent=2) + "\n")
    if settings.imap_password:
        _save_draft_to_imap(draft, settings)
    return draft_path

async def process_mailbox_once(settings: MailSettings | None = None, model=None) -> list[StoredDraft]:
    from app.discord import Verbosity, notify_start, notify_finish

    settings = settings or MailSettings.from_env()
    await notify_start("Mailbox Poll", "Checking for new NOC email...")

    try:
        messages = fetch_unseen_messages(settings)
        drafts: list[StoredDraft] = []
        for message in messages:
            draft = await draft_reply_for_message(message, model=model)
            store_draft(draft, settings)
            drafts.append(draft)

        if messages:
            msg_details = "\n\n".join(
                f"**From:** {d.sender}\n"
                f"**Subject:** {d.subject}\n"
                f"**Message Summary:** {d.plan.summary}\n"
                f"**Draft Summary:** {d.plan.reply_summary}"
                for d in drafts
            )
            await notify_finish("Mailbox Poll", f"Processed {len(messages)} messages, created {len(drafts)} drafts.\n\n{msg_details}")
        else:
            await notify_finish("Mailbox Poll", "No new messages.", level=Verbosity.DEBUG)
        return drafts
    except Exception as e:
        safe = classify_exception(e)
        log_exception("mailbox_poll_failed", e, category=safe.category)
        await notify_finish(
            "Mailbox Poll",
            safe.discord_description("Mailbox polling"),
            is_error=True,
            safe_category=safe.category,
        )
        raise


def check_mailbox_connection(settings: MailSettings | None = None) -> dict:
    settings = settings or MailSettings.from_env()
    if not settings.imap_password:
        raise RuntimeError("MAIL_IMAP_PASSWORD is required to poll the NOC mailbox")

    with imaplib.IMAP4_SSL(settings.imap_host, settings.imap_port, timeout=settings.imap_timeout) as client:
        client.login(settings.imap_user, settings.imap_password)
        status, data = client.select(settings.mailbox, readonly=True)
        if status != "OK":
            raise RuntimeError(f"IMAP select failed: {status}")
        message_count = 0
        if data and data[0]:
            try:
                message_count = int(data[0])
            except (TypeError, ValueError):
                message_count = 0
        client.logout()

    return {
        "status": "ok",
        "host": settings.imap_host,
        "user": settings.imap_user,
        "mailbox": settings.mailbox,
        "message_count": message_count,
    }


def _extract_text_body(message: Message) -> str:
    if message.is_multipart():
        for part in message.walk():
            if part.get_content_type() == "text/plain" and not part.get_content_disposition():
                return _part_text(part)
        for part in message.walk():
            if part.get_content_type() == "text/html" and not part.get_content_disposition():
                return _part_text(part)
        return ""
    return _part_text(message)


def _part_text(part: Message) -> str:
    payload = part.get_payload(decode=True)
    if payload is None:
        return str(part.get_payload() or "")
    charset = part.get_content_charset() or "utf-8"
    return payload.decode(charset, errors="replace")


def _addresses_from_headers(message: Message, headers: list[str]) -> list[str]:
    values = []
    for header in headers:
        values.extend(message.get_all(header, []))
    return [addr.lower() for _, addr in getaddresses(values) if addr]


def _first_address(value: str) -> str:
    addresses = getaddresses([value])
    if not addresses:
        return ""
    return addresses[0][1].lower()


def _find_original_recipient(message: Message, to_addresses: list[str]) -> str | None:
    for header in ("x-original-to", "delivered-to"):
        for _, addr in getaddresses(message.get_all(header, [])):
            if addr.lower() in ROLE_ADDRESSES:
                return addr.lower()
    for addr in to_addresses:
        if addr in ROLE_ADDRESSES:
            return addr
    return None


def _raw_rfc822(fetch_data: list[object]) -> bytes | None:
    for item in fetch_data:
        if isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], bytes):
            return item[1]
    return None


def _draft_id(message: InboundMail) -> str:
    basis = message.message_id or f"{message.uid}:{message.subject}:{message.from_address}"
    return hashlib.sha256(basis.encode()).hexdigest()[:16]
