"""Retain terminal report intents locally while the case database is unavailable."""

import asyncio
import hashlib
import heapq
import os
from pathlib import Path
import tempfile

from app import log
from app.cases.models import OutboxIntent
from app.cases.store import CaseStore


MAX_RECORD_BYTES = 262144


def spool_directory() -> Path:
    return Path(os.getenv("NOC_REPORT_SPOOL_DIR") or (
        Path(os.getenv("MAIL_DRAFT_DIR", "data/mail-drafts")) / ".notifications" / "report-spool"
    ))


def _sync_directory(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _retain(intent: OutboxIntent) -> None:
    if intent.intent_type != "report" or not isinstance(intent.payload.get("card_update"), dict):
        raise ValueError("only terminal report intents may be retained")
    data = intent.model_dump_json().encode()
    if len(data) > MAX_RECORD_BYTES:
        raise ValueError("report spool record too large")
    directory = spool_directory()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    key = hashlib.sha256(intent.idempotency_key.encode()).hexdigest()
    fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=directory)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, directory / (key + ".json"))
        _sync_directory(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


async def retain_report(intent: OutboxIntent) -> None:
    await asyncio.to_thread(_retain, intent)


def _stats() -> dict:
    directory = spool_directory()
    count = invalid = 0
    oldest = None
    if directory.exists():
        for path in directory.iterdir():
            if path.suffix == ".invalid":
                invalid += 1
            if path.suffix != ".json":
                continue
            try:
                stamp = path.stat(follow_symlinks=False).st_mtime
            except FileNotFoundError:
                continue
            count += 1
            oldest = stamp if oldest is None else min(oldest, stamp)
    return {"pending": count, "invalid": invalid, "oldest_retained_at": oldest}


async def spool_stats() -> dict:
    return await asyncio.to_thread(_stats)


def _read(path: Path) -> OutboxIntent:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as source:
        data = source.read(MAX_RECORD_BYTES + 1)
    if len(data) > MAX_RECORD_BYTES:
        raise ValueError("report spool record too large")
    intent = OutboxIntent.model_validate_json(data)
    if intent.intent_type != "report" or not isinstance(intent.payload.get("card_update"), dict):
        raise ValueError("invalid retained report kind")
    return intent


def _pending(limit: int) -> list[Path]:
    directory = spool_directory()
    if not directory.exists():
        return []
    return heapq.nsmallest(limit, (path for path in directory.iterdir() if path.suffix == ".json"))


async def replay_reports(store: CaseStore, *, limit: int = 100) -> int:
    accepted = 0
    for path in await asyncio.to_thread(_pending, max(0, min(limit, 100))):
        try:
            intent = await asyncio.to_thread(_read, path)
        except FileNotFoundError:
            continue
        except Exception as exc:
            log.warn("report_spool_invalid", error_type=type(exc).__name__)
            # Retain invalid records for inspection, outside the replay set.
            try:
                await asyncio.to_thread(path.rename, path.with_suffix(".invalid"))
                await asyncio.to_thread(_sync_directory, path.parent)
            except FileNotFoundError:
                pass  # Another replay already quarantined this record.
            continue
        # The durable unique key also handles a database commit whose reply was
        # lost, or two workers replaying the same file concurrently.
        try:
            await store.enqueue_outbox(intent)
        except Exception as exc:
            # A rejected case reference must not block unrelated reports in
            # this bounded batch. Keep the original record for another retry.
            log.warn("report_spool_enqueue_failed", error_type=type(exc).__name__)
            continue
        await asyncio.to_thread(path.unlink, missing_ok=True)
        await asyncio.to_thread(_sync_directory, path.parent)
        accepted += 1
    return accepted
