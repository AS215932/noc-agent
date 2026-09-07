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
MAX_HEALTH_SCAN_ENTRIES = 1000


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
    limited = False
    try:
        entries = os.scandir(directory)
    except FileNotFoundError:
        return {"pending": 0, "invalid": 0, "oldest_retained_at": None, "scan_limited": False}
    with entries:
        for index, entry in enumerate(entries):
            if index >= MAX_HEALTH_SCAN_ENTRIES:
                limited = True
                break
            if entry.name.endswith(".invalid"):
                invalid += 1
            if not entry.name.endswith(".json"):
                continue
            try:
                stamp = entry.stat(follow_symlinks=False).st_mtime
            except FileNotFoundError:
                continue
            count += 1
            oldest = stamp if oldest is None else min(oldest, stamp)
    return {"pending": count, "invalid": invalid, "oldest_retained_at": oldest, "scan_limited": limited}


async def spool_stats() -> dict:
    return await asyncio.to_thread(_stats)


def _quarantine(path: Path) -> None:
    try:
        path.rename(path.with_suffix(".invalid"))
        _sync_directory(path.parent)
    except FileNotFoundError:
        pass


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
            await asyncio.to_thread(_quarantine, path)
            continue
        # The durable unique key also handles a database commit whose reply was
        # lost, or two workers replaying the same file concurrently.
        try:
            await store.enqueue_outbox(intent)
        except Exception as exc:
            log.warn("report_spool_enqueue_failed", error_type=type(exc).__name__)
            if str(getattr(exc, "sqlstate", "")).startswith("23"):
                # PostgreSQL integrity violations are specific to this record.
                # Preserve them outside the bounded prefix so later records
                # are reachable even after a restore loses many case rows.
                await asyncio.to_thread(_quarantine, path)
                continue
            # Connection/pool/timeouts (and unclassified failures) stop this
            # tick instead of repeating a store-wide failure up to 100 times.
            raise
        await asyncio.to_thread(path.unlink, missing_ok=True)
        await asyncio.to_thread(_sync_directory, path.parent)
        accepted += 1
    return accepted
