"""Optional Postgres CaseStore backend.

This module is dormant until a deployment provides `asyncpg` and a Postgres DSN.
It keeps the same CaseStore contract as the in-memory reference backend and uses
the schema in :mod:`app.db.schema`.
"""

from __future__ import annotations

import json
from typing import Any, cast

from app.cases.models import (
    AtomicCaseProjection,
    CaseEvent,
    CaseIdentityAlias,
    MetaCaseProjection,
    ObservationRecord,
    OperatorFeedback,
    OutboxIntent,
    TraceRecord,
)
from app.cases.store import CaseLinkResult, CaseProjection
from app.db.config import DatabaseSettings, load_database_settings
from app.db.schema import SCHEMA_STATEMENTS


class PostgresCaseStore:
    def __init__(self, pool: Any) -> None:
        self.pool = pool

    @classmethod
    async def connect(cls, settings: DatabaseSettings | None = None) -> "PostgresCaseStore":
        settings = settings or load_database_settings()
        settings.assert_ready_for_production()
        if not settings.url:
            raise RuntimeError("Postgres DSN is required for PostgresCaseStore")
        asyncpg = _load_asyncpg()
        pool = await asyncpg.create_pool(
            dsn=settings.url,
            min_size=settings.pool_min_size,
            max_size=settings.pool_max_size,
            command_timeout=settings.command_timeout_s,
            server_settings={
                "statement_timeout": str(settings.statement_timeout_ms),
                "lock_timeout": str(settings.lock_timeout_ms),
            },
        )
        return cls(pool)

    async def close(self) -> None:
        await self.pool.close()

    async def setup(self) -> None:
        async with self.pool.acquire() as conn:
            for statement in SCHEMA_STATEMENTS:
                await conn.execute(statement)

    async def put_observation(self, observation: ObservationRecord) -> ObservationRecord:
        payload = observation.model_dump(mode="json")
        conflict_clause = (
            """
                ON CONFLICT (dedup_key) WHERE dedup_key <> '' DO UPDATE SET
                    source = EXCLUDED.source,
                    detector = EXCLUDED.detector,
                    status = EXCLUDED.status,
                    severity = EXCLUDED.severity,
                    source_event_id = EXCLUDED.source_event_id,
                    source_fingerprint = EXCLUDED.source_fingerprint,
                    observed_at = EXCLUDED.observed_at,
                    received_at = EXCLUDED.received_at,
                    scan_cycle_id = EXCLUDED.scan_cycle_id,
                    signal_signature = EXCLUDED.signal_signature,
                    source_health = EXCLUDED.source_health,
                    payload = jsonb_set(EXCLUDED.payload, '{observation_id}', to_jsonb(observations.observation_id)),
                    schema_version = EXCLUDED.schema_version
            """
            if observation.dedup_key
            else "ON CONFLICT (observation_id) DO UPDATE SET observation_id = observations.observation_id"
        )
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO observations (
                    observation_id, source, detector, status, severity, dedup_key,
                    source_event_id, source_fingerprint, observed_at, received_at,
                    scan_cycle_id, signal_signature, source_health, payload, schema_version
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,$15)
                {conflict_clause}
                RETURNING payload
                """,
                observation.observation_id,
                observation.source,
                observation.detector,
                observation.status,
                observation.severity,
                observation.dedup_key,
                observation.source_event_id,
                observation.source_fingerprint,
                observation.observed_at,
                observation.received_at,
                observation.scan_cycle_id,
                observation.signal_signature,
                observation.source_health,
                json.dumps(payload),
                observation.schema_version,
            )
        return ObservationRecord.model_validate(_row_payload(row))

    async def get_observation(self, observation_id: str) -> ObservationRecord | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT payload FROM observations WHERE observation_id = $1", observation_id)
        return ObservationRecord.model_validate(_row_payload(row)) if row else None

    async def upsert_case(self, case: CaseProjection) -> CaseProjection:
        payload = case.model_dump(mode="json")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO cases (
                    case_id, kind, status, fingerprint, case_number, event_fingerprint,
                    updated_at, opened_at, payload, schema_version
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)
                ON CONFLICT (case_id) DO UPDATE SET
                    kind = EXCLUDED.kind,
                    status = EXCLUDED.status,
                    fingerprint = EXCLUDED.fingerprint,
                    case_number = EXCLUDED.case_number,
                    event_fingerprint = EXCLUDED.event_fingerprint,
                    updated_at = EXCLUDED.updated_at,
                    payload = EXCLUDED.payload,
                    row_version = cases.row_version + 1,
                    schema_version = EXCLUDED.schema_version
                RETURNING payload
                """,
                case.case_id,
                case.kind,
                case.status,
                getattr(case, "fingerprint", ""),
                getattr(case, "case_number", ""),
                getattr(case, "event_fingerprint", ""),
                getattr(case, "updated_at", ""),
                getattr(case, "opened_at", ""),
                json.dumps(payload),
                case.schema_version,
            )
        return _case_from_payload(_row_payload(row))

    async def create_atomic_case(
        self,
        case: AtomicCaseProjection,
        *,
        aliases: list[CaseIdentityAlias],
        events: list[CaseEvent],
    ) -> tuple[AtomicCaseProjection, list[CaseEvent], bool]:
        if not aliases:
            raise ValueError("at least one case identity alias is required")
        payload = case.model_dump(mode="json")
        lock_key = f"{aliases[0].alias_type}:{aliases[0].alias_value}"
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)", lock_key)
                for alias in aliases:
                    existing = await conn.fetchrow(
                        """
                        SELECT c.payload FROM case_identity_aliases a
                        JOIN cases c ON c.case_id = a.case_id
                        WHERE a.alias_type = $1 AND a.alias_value = $2 AND a.retired_at IS NULL
                        FOR UPDATE OF a, c
                        """,
                        alias.alias_type,
                        alias.alias_value,
                    )
                    if existing:
                        existing_case = _case_from_payload(_row_payload(existing))
                        if isinstance(existing_case, AtomicCaseProjection):
                            return existing_case, [], False
                        raise ValueError(f"active alias {alias.alias_type}:{alias.alias_value} resolved to non-atomic case")
                if case.fingerprint:
                    existing = await conn.fetchrow(
                        """
                        SELECT payload FROM cases
                        WHERE kind = 'atomic'
                          AND fingerprint = $1
                          AND status NOT IN ('resolved', 'expired', 'closed', 'linked')
                        FOR UPDATE
                        """,
                        case.fingerprint,
                    )
                    if existing:
                        existing_case = _case_from_payload(_row_payload(existing))
                        if isinstance(existing_case, AtomicCaseProjection):
                            return existing_case, [], False
                await conn.execute(
                    """
                    INSERT INTO cases (
                        case_id, kind, status, fingerprint, case_number, event_fingerprint,
                        updated_at, opened_at, payload, schema_version
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)
                    """,
                    case.case_id,
                    case.kind,
                    case.status,
                    case.fingerprint,
                    case.case_number,
                    "",
                    case.updated_at,
                    case.opened_at,
                    json.dumps(payload),
                    case.schema_version,
                )
                for alias in aliases:
                    await conn.execute(
                        """
                        INSERT INTO case_identity_aliases (
                            alias_id, case_id, alias_type, alias_value, source, confidence, created_at, retired_at
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        """,
                        alias.alias_id,
                        alias.case_id,
                        alias.alias_type,
                        alias.alias_value,
                        alias.source,
                        alias.confidence,
                        alias.created_at,
                        alias.retired_at,
                    )
                stored_events = []
                for event in events:
                    stored_events.append(await _insert_case_event(conn, event))
        return case.model_copy(deep=True), stored_events, True

    async def claim_investigation(
        self,
        case_id: str,
        *,
        expected_signal_signature: str,
        expected_diagnosis_signature: str,
        expected_last_investigated_at: str,
        status: str,
        policy_version: str,
        now: str,
        event: CaseEvent,
    ) -> AtomicCaseProjection | None:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("SELECT payload FROM cases WHERE case_id = $1 FOR UPDATE", case_id)
                if not row:
                    return None
                case = _case_from_payload(_row_payload(row))
                if not isinstance(case, AtomicCaseProjection):
                    return None
                if str(case.status or "") in {"resolved", "closed", "expired", "linked"}:
                    return None
                if (
                    case.signal_signature != expected_signal_signature
                    or case.diagnosis_signature != expected_diagnosis_signature
                    or case.last_investigated_at != expected_last_investigated_at
                ):
                    return None
                case.last_investigated_at = now
                case.diagnosis_signature = case.signal_signature or case.fingerprint or case.case_id
                case.investigation_status = status
                case.investigation_error = ""
                case.updated_at = now
                case.policy_version = policy_version
                payload = case.model_dump(mode="json")
                updated = await conn.fetchrow(
                    """
                    UPDATE cases SET
                        status = $2,
                        updated_at = $3,
                        payload = $4::jsonb,
                        row_version = cases.row_version + 1,
                        schema_version = $5
                    WHERE case_id = $1
                    RETURNING payload
                    """,
                    case.case_id,
                    case.status,
                    case.updated_at,
                    json.dumps(payload),
                    case.schema_version,
                )
                await _insert_case_event(conn, event)
        return cast(AtomicCaseProjection, _case_from_payload(_row_payload(updated)))

    async def get_case(self, case_id: str) -> CaseProjection | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT payload FROM cases WHERE case_id = $1", case_id)
        return _case_from_payload(_row_payload(row)) if row else None

    async def list_cases(self, *, kind: str | None = None, limit: int = 100) -> list[CaseProjection]:
        async with self.pool.acquire() as conn:
            if kind:
                rows = await conn.fetch("SELECT payload FROM cases WHERE kind = $1 ORDER BY updated_at DESC LIMIT $2", kind, limit)
            else:
                rows = await conn.fetch("SELECT payload FROM cases ORDER BY updated_at DESC LIMIT $1", limit)
        return [_case_from_payload(_row_payload(row)) for row in rows]

    async def append_event(self, event: CaseEvent) -> CaseEvent:
        async with self.pool.acquire() as conn:
            return await _insert_case_event(conn, event)

    async def case_events(self, case_id: str) -> list[CaseEvent]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT payload FROM case_events
                WHERE case_id = $1 OR meta_case_id = $1
                ORDER BY occurred_at ASC, event_id ASC
                """,
                case_id,
            )
        return [CaseEvent.model_validate(_row_payload(row)) for row in rows]

    async def record_alias(self, alias: CaseIdentityAlias) -> CaseIdentityAlias:
        async with self.pool.acquire() as conn:
            existing = await conn.fetchrow(
                """
                SELECT alias_id, case_id, alias_type, alias_value, source, confidence, created_at, retired_at
                FROM case_identity_aliases
                WHERE alias_type = $1 AND alias_value = $2 AND retired_at IS NULL
                """,
                alias.alias_type,
                alias.alias_value,
            )
            if existing:
                existing_alias = CaseIdentityAlias.model_validate(dict(existing))
                if existing_alias.case_id != alias.case_id:
                    raise ValueError(
                        f"active alias {alias.alias_type}:{alias.alias_value} already points to {existing_alias.case_id}"
                    )
                return existing_alias
            await conn.execute(
                """
                INSERT INTO case_identity_aliases (
                    alias_id, case_id, alias_type, alias_value, source, confidence, created_at, retired_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                """,
                alias.alias_id,
                alias.case_id,
                alias.alias_type,
                alias.alias_value,
                alias.source,
                alias.confidence,
                alias.created_at,
                alias.retired_at,
            )
        return alias.model_copy(deep=True)

    async def resolve_alias(self, alias_type: str, alias_value: str) -> str | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT case_id FROM case_identity_aliases
                WHERE alias_type = $1 AND alias_value = $2 AND retired_at IS NULL
                """,
                alias_type,
                alias_value,
            )
        return str(row["case_id"]) if row else None

    async def link_child_case(
        self,
        child_case_id: str,
        parent_case_id: str,
        *,
        reason: str,
        evidence_refs: list[str],
        now: str,
    ) -> CaseLinkResult | None:
        if child_case_id == parent_case_id:
            return None
        async with self.pool.acquire() as conn, conn.transaction():
            child_row = await conn.fetchrow("SELECT payload FROM cases WHERE case_id = $1 FOR UPDATE", child_case_id)
            parent_row = await conn.fetchrow("SELECT payload FROM cases WHERE case_id = $1 FOR UPDATE", parent_case_id)
            if not child_row or not parent_row:
                return None
            child = _case_from_payload(_row_payload(child_row))
            parent = _case_from_payload(_row_payload(parent_row))
            if not isinstance(child, AtomicCaseProjection) or not isinstance(parent, AtomicCaseProjection):
                return None
            child_diagnosis = dict(child.last_diagnosis or {})
            if child.status == "linked":
                if child_diagnosis.get("linked_parent_case") == parent.case_id:
                    return CaseLinkResult(child_case=child, parent_case=parent, moved_aliases=[], events=[])
                return None
            child_diagnosis["linked_parent_case"] = parent.case_id
            child_diagnosis["link_reason"] = reason
            child_diagnosis["link_evidence_refs"] = list(evidence_refs)
            child.status = "linked"
            child.resolution_reason = "linked_parent"
            child.resolved_at = now
            child.last_diagnosis = child_diagnosis
            child.updated_at = now
            parent.updated_at = now
            child_payload = child.model_dump(mode="json")
            parent_payload = parent.model_dump(mode="json")
            child_updated = await conn.fetchrow(
                """
                UPDATE cases SET
                    status = $2,
                    updated_at = $3,
                    payload = $4::jsonb,
                    row_version = cases.row_version + 1,
                    schema_version = $5
                WHERE case_id = $1
                RETURNING payload
                """,
                child.case_id,
                child.status,
                child.updated_at,
                json.dumps(child_payload),
                child.schema_version,
            )
            parent_updated = await conn.fetchrow(
                """
                UPDATE cases SET
                    updated_at = $2,
                    payload = $3::jsonb,
                    row_version = cases.row_version + 1,
                    schema_version = $4
                WHERE case_id = $1
                RETURNING payload
                """,
                parent.case_id,
                parent.updated_at,
                json.dumps(parent_payload),
                parent.schema_version,
            )
            child = cast(AtomicCaseProjection, _case_from_payload(_row_payload(child_updated)))
            parent = cast(AtomicCaseProjection, _case_from_payload(_row_payload(parent_updated)))
            moved_aliases: list[CaseIdentityAlias] = []
            rows = await conn.fetch(
                """
                SELECT alias_id, case_id, alias_type, alias_value, source, confidence, created_at, retired_at
                FROM case_identity_aliases
                WHERE case_id = $1 AND retired_at IS NULL
                FOR UPDATE
                """,
                child.case_id,
            )
            for row in rows:
                alias = CaseIdentityAlias.model_validate(dict(row))
                await conn.execute(
                    """
                    UPDATE case_identity_aliases
                    SET retired_at = $2
                    WHERE alias_id = $1
                    """,
                    alias.alias_id,
                    now,
                )
                replacement = CaseIdentityAlias(
                    case_id=parent.case_id,
                    alias_type=alias.alias_type,
                    alias_value=alias.alias_value,
                    source=alias.source or "case_link",
                    confidence=alias.confidence,
                    created_at=now,
                )
                inserted = await conn.fetchrow(
                    """
                    INSERT INTO case_identity_aliases (
                        alias_id, case_id, alias_type, alias_value, source, confidence, created_at, retired_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (alias_type, alias_value) WHERE retired_at IS NULL DO NOTHING
                    RETURNING alias_id
                    """,
                    replacement.alias_id,
                    replacement.case_id,
                    replacement.alias_type,
                    replacement.alias_value,
                    replacement.source,
                    replacement.confidence,
                    replacement.created_at,
                    replacement.retired_at,
                )
                if inserted:
                    moved_aliases.append(replacement)
            child_event = CaseEvent(
                case_id=child.case_id,
                event_type="case_linked_to_parent",
                actor_type="graph",
                occurred_at=now,
                payload={
                    "parent_case_id": parent.case_id,
                    "reason": reason,
                    "evidence_refs": list(evidence_refs),
                    "moved_alias_count": len(moved_aliases),
                },
            )
            parent_event = CaseEvent(
                case_id=parent.case_id,
                event_type="linked_child_case",
                actor_type="graph",
                occurred_at=now,
                payload={
                    "child_case_id": child.case_id,
                    "resource_key": child.resource_id,
                    "summary": reason,
                    "evidence_refs": list(evidence_refs),
                    "moved_alias_count": len(moved_aliases),
                },
            )
            events = [await _insert_case_event(conn, child_event), await _insert_case_event(conn, parent_event)]
            return CaseLinkResult(child_case=child, parent_case=parent, moved_aliases=moved_aliases, events=events)

    async def enqueue_outbox(self, intent: OutboxIntent) -> OutboxIntent:
        payload = intent.model_dump(mode="json")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO side_effect_outbox (
                    outbox_id, intent_type, case_id, meta_case_id, idempotency_key,
                    state_signature, status, attempts, next_attempt_at, created_at,
                    completed_at, external_id, external_url, error, payload, schema_version
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16)
                ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = side_effect_outbox.idempotency_key
                RETURNING payload
                """,
                intent.outbox_id,
                intent.intent_type,
                intent.case_id,
                intent.meta_case_id,
                intent.idempotency_key,
                intent.state_signature,
                intent.status,
                intent.attempts,
                intent.next_attempt_at,
                intent.created_at,
                intent.completed_at,
                intent.external_id,
                intent.external_url,
                intent.error,
                json.dumps(payload),
                intent.schema_version,
            )
        return OutboxIntent.model_validate(_row_payload(row))

    async def update_outbox(self, intent: OutboxIntent) -> OutboxIntent:
        payload = intent.model_dump(mode="json")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE side_effect_outbox SET
                    status = $2,
                    attempts = $3,
                    next_attempt_at = $4,
                    completed_at = $5,
                    external_id = $6,
                    external_url = $7,
                    error = $8,
                    payload = $9::jsonb,
                    schema_version = $10
                WHERE outbox_id = $1
                RETURNING payload
                """,
                intent.outbox_id,
                intent.status,
                intent.attempts,
                intent.next_attempt_at,
                intent.completed_at,
                intent.external_id,
                intent.external_url,
                intent.error,
                json.dumps(payload),
                intent.schema_version,
            )
        if not row:
            raise KeyError(f"outbox intent not found: {intent.outbox_id}")
        return OutboxIntent.model_validate(_row_payload(row))

    async def list_outbox(self, *, status: str | None = None) -> list[OutboxIntent]:
        async with self.pool.acquire() as conn:
            if status:
                rows = await conn.fetch(
                    "SELECT payload FROM side_effect_outbox WHERE status = $1 ORDER BY created_at ASC", status
                )
            else:
                rows = await conn.fetch("SELECT payload FROM side_effect_outbox ORDER BY created_at ASC")
        return [OutboxIntent.model_validate(_row_payload(row)) for row in rows]

    async def record_trace(self, trace: TraceRecord) -> TraceRecord:
        payload = trace.model_dump(mode="json")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO traces (
                    trace_id, cycle_id, case_id, meta_case_id, trace_type, policy_version,
                    model_chain, prompt_version, knowledge_export_version, payload, created_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10::jsonb,$11)
                ON CONFLICT (trace_id) DO UPDATE SET trace_id = traces.trace_id
                RETURNING payload
                """,
                trace.trace_id,
                trace.cycle_id,
                trace.case_id,
                trace.meta_case_id,
                trace.trace_type,
                trace.policy_version,
                json.dumps(trace.model_chain),
                trace.prompt_version,
                trace.knowledge_export_version,
                json.dumps(payload),
                trace.created_at,
            )
        return TraceRecord.model_validate(_row_payload(row))

    async def list_traces(self, *, case_id: str | None = None, meta_case_id: str | None = None) -> list[TraceRecord]:
        async with self.pool.acquire() as conn:
            if case_id is not None:
                rows = await conn.fetch("SELECT payload FROM traces WHERE case_id = $1 ORDER BY created_at", case_id)
            elif meta_case_id is not None:
                rows = await conn.fetch("SELECT payload FROM traces WHERE meta_case_id = $1 ORDER BY created_at", meta_case_id)
            else:
                rows = await conn.fetch("SELECT payload FROM traces ORDER BY created_at")
        return [TraceRecord.model_validate(_row_payload(row)) for row in rows]

    async def record_feedback(self, feedback: OperatorFeedback) -> OperatorFeedback:
        payload = feedback.model_dump(mode="json")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO operator_feedback (
                    feedback_id, case_id, meta_case_id, trace_id, actor_id, actor_role,
                    feedback_type, payload, created_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
                ON CONFLICT (feedback_id) DO UPDATE SET feedback_id = operator_feedback.feedback_id
                RETURNING payload
                """,
                feedback.feedback_id,
                feedback.case_id,
                feedback.meta_case_id,
                feedback.trace_id,
                feedback.actor_id,
                feedback.actor_role,
                feedback.feedback_type,
                json.dumps(payload),
                feedback.created_at,
            )
        return OperatorFeedback.model_validate(_row_payload(row))

    async def list_feedback(self, *, case_id: str | None = None, meta_case_id: str | None = None) -> list[OperatorFeedback]:
        async with self.pool.acquire() as conn:
            if case_id is not None:
                rows = await conn.fetch("SELECT payload FROM operator_feedback WHERE case_id = $1 ORDER BY created_at", case_id)
            elif meta_case_id is not None:
                rows = await conn.fetch(
                    "SELECT payload FROM operator_feedback WHERE meta_case_id = $1 ORDER BY created_at", meta_case_id
                )
            else:
                rows = await conn.fetch("SELECT payload FROM operator_feedback ORDER BY created_at")
        return [OperatorFeedback.model_validate(_row_payload(row)) for row in rows]


async def _insert_case_event(conn: Any, event: CaseEvent) -> CaseEvent:
    payload = event.model_dump(mode="json")
    row = await conn.fetchrow(
        """
        INSERT INTO case_events (
            event_id, case_id, meta_case_id, event_type, actor_type, actor_id,
            source, occurred_at, observed_at, correlation_id, causation_id,
            policy_version, payload, schema_version
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14)
        ON CONFLICT (event_id) DO UPDATE SET event_id = case_events.event_id
        RETURNING payload
        """,
        event.event_id,
        event.case_id,
        event.meta_case_id,
        event.event_type,
        event.actor_type,
        event.actor_id,
        event.source,
        event.occurred_at,
        event.observed_at,
        event.correlation_id,
        event.causation_id,
        event.policy_version,
        json.dumps(payload),
        event.schema_version,
    )
    return CaseEvent.model_validate(_row_payload(row))



def _load_asyncpg() -> Any:
    try:
        import asyncpg  # type: ignore[import-not-found]
    except Exception as exc:  # pragma: no cover - depends on deployment extras
        raise RuntimeError("PostgresCaseStore requires the optional asyncpg package") from exc
    return asyncpg


def _row_payload(row: Any) -> dict[str, Any]:
    raw = row["payload"]
    if isinstance(raw, str):
        loaded = json.loads(raw)
        return cast(dict[str, Any], loaded)
    if isinstance(raw, dict):
        return raw
    loaded = json.loads(str(raw))
    return cast(dict[str, Any], loaded)


def _case_from_payload(payload: dict[str, Any]) -> CaseProjection:
    kind = str(payload.get("kind") or "atomic")
    if kind == "meta":
        return MetaCaseProjection.model_validate(payload)
    return AtomicCaseProjection.model_validate(payload)
