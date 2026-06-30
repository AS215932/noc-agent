"""Optional Postgres CaseStore backend.

This module is dormant until a deployment provides `asyncpg` and a Postgres DSN.
It keeps the same CaseStore contract as the in-memory reference backend and uses
the schema in :mod:`app.db.schema`.
"""

from __future__ import annotations

import json
from typing import Any, cast

from app.cases.lhp import (
    CallbackInboxRecord,
    CaseHandoff,
    HandoffStatus,
    HandoffTransportDelivery,
    HandoffUpdate,
    KnowledgeArtifact,
    OutcomeRecord,
    VERIFIER_ONLY_HANDOFF_STATUSES,
    VerificationObjective,
    lhp_payload_hash,
    require_handoff_transition,
)
from app.cases.models import (
    AtomicCaseProjection,
    CaseEvent,
    CaseIdentityAlias,
    CaseStatus,
    MetaCaseProjection,
    ObservationRecord,
    OperatorFeedback,
    OutboxIntent,
    TraceRecord,
)
from app.cases.store import (
    CallbackClaimResult,
    CaseLinkResult,
    CaseProjection,
    HandoffCreateResult,
    HandoffUpdateResult,
)
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
                        raise ValueError(
                            f"active alias {alias.alias_type}:{alias.alias_value} resolved to non-atomic case"
                        )
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

    async def list_cases(
        self, *, kind: str | None = None, status: str | None = None, limit: int = 100
    ) -> list[CaseProjection]:
        async with self.pool.acquire() as conn:
            if kind and status:
                rows = await conn.fetch(
                    "SELECT payload FROM cases WHERE kind = $1 AND status = $2 ORDER BY updated_at DESC LIMIT $3",
                    kind,
                    status,
                    limit,
                )
            elif kind:
                rows = await conn.fetch(
                    "SELECT payload FROM cases WHERE kind = $1 ORDER BY updated_at DESC LIMIT $2", kind, limit
                )
            elif status:
                rows = await conn.fetch(
                    "SELECT payload FROM cases WHERE status = $1 ORDER BY updated_at DESC LIMIT $2", status, limit
                )
            else:
                rows = await conn.fetch("SELECT payload FROM cases ORDER BY updated_at DESC LIMIT $1", limit)
        return [_case_from_payload(_row_payload(row)) for row in rows]

    async def append_event(self, event: CaseEvent) -> CaseEvent:
        async with self.pool.acquire() as conn:
            return await _insert_case_event(conn, event)

    async def case_events(
        self, case_id: str, *, limit: int | None = None, newest_first: bool = False
    ) -> list[CaseEvent]:
        direction = "DESC" if newest_first else "ASC"
        async with self.pool.acquire() as conn:
            if limit is not None:
                rows = await conn.fetch(
                    f"""
                    SELECT payload FROM case_events
                    WHERE case_id = $1 OR meta_case_id = $1
                    ORDER BY occurred_at {direction}, event_id {direction}
                    LIMIT $2
                    """,
                    case_id,
                    _bounded_limit(limit),
                )
            else:
                rows = await conn.fetch(
                    f"""
                    SELECT payload FROM case_events
                    WHERE case_id = $1 OR meta_case_id = $1
                    ORDER BY occurred_at {direction}, event_id {direction}
                    """,
                    case_id,
                )
        return [CaseEvent.model_validate(_row_payload(row)) for row in rows]

    async def count_case_events(self, case_id: str) -> int:
        async with self.pool.acquire() as conn:
            value = await conn.fetchval(
                "SELECT count(*) FROM case_events WHERE case_id = $1 OR meta_case_id = $1",
                case_id,
            )
        return int(value or 0)

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

    async def create_handoff_with_objectives(
        self,
        handoff: CaseHandoff,
        *,
        objectives: list[VerificationObjective],
        case_status: CaseStatus | None = None,
        event: CaseEvent | None = None,
        outbox_intent: OutboxIntent | None = None,
    ) -> HandoffCreateResult:
        lock_key = _advisory_lock_key(
            "handoff",
            {
                "case_id": handoff.case_id,
                "target_loop": handoff.target_loop,
                "objective_key": handoff.objective_key,
            },
        )
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)", lock_key)
            case_row = await conn.fetchrow("SELECT payload FROM cases WHERE case_id = $1 FOR UPDATE", handoff.case_id)
            if not case_row:
                raise KeyError(f"atomic case not found: {handoff.case_id}")
            case = _case_from_payload(_row_payload(case_row))
            if not isinstance(case, AtomicCaseProjection):
                raise KeyError(f"atomic case not found: {handoff.case_id}")
            existing_row = await conn.fetchrow(
                """
                SELECT payload FROM case_handoffs
                WHERE idempotency_key = $1
                   OR (case_id = $2 AND target_loop = $3 AND objective_key = $4
                       AND status NOT IN ('resolved', 'cancelled', 'expired'))
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE
                """,
                handoff.idempotency_key,
                handoff.case_id,
                handoff.target_loop,
                handoff.objective_key,
            )
            if existing_row:
                existing = CaseHandoff.model_validate(_row_payload(existing_row))
                objective_rows = await conn.fetch(
                    "SELECT payload FROM verification_objectives WHERE handoff_id = $1 ORDER BY created_at, objective_id",
                    existing.handoff_id,
                )
                return HandoffCreateResult(
                    handoff=existing,
                    objectives=[VerificationObjective.model_validate(_row_payload(row)) for row in objective_rows],
                    case=case,
                    event=None,
                    outbox_intent=None,
                    created=False,
                )
            stored_handoff = await _insert_case_handoff(conn, handoff)
            stored_objectives = []
            for objective in objectives:
                if objective.case_id != stored_handoff.case_id:
                    raise ValueError("verification objective case_id must match handoff case_id")
                if objective.handoff_id and objective.handoff_id != stored_handoff.handoff_id:
                    raise ValueError("verification objective handoff_id must match handoff_id")
                if not objective.handoff_id:
                    objective = objective.model_copy(update={"handoff_id": stored_handoff.handoff_id})
                stored_objectives.append(await _upsert_verification_objective(conn, objective))
            updated_case = case.model_copy(deep=True)
            if case_status is not None:
                updated_case.status = case_status
            updated_case.handoff_status = stored_handoff.status
            updated_case.last_handoff_at = stored_handoff.updated_at or stored_handoff.created_at
            updated_case.updated_at = stored_handoff.updated_at or stored_handoff.created_at
            await _update_case_projection(conn, updated_case)
            stored_event = await _insert_case_event(conn, event) if event is not None else None
            stored_outbox = await _insert_outbox_intent(conn, outbox_intent) if outbox_intent is not None else None
            return HandoffCreateResult(
                handoff=stored_handoff,
                objectives=stored_objectives,
                case=updated_case,
                event=stored_event,
                outbox_intent=stored_outbox,
                created=True,
            )

    async def get_handoff(self, handoff_id: str) -> CaseHandoff | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT payload FROM case_handoffs WHERE handoff_id = $1", handoff_id)
        return CaseHandoff.model_validate(_row_payload(row)) if row else None

    async def list_handoffs(
        self, *, case_id: str | None = None, status: str | None = None, limit: int | None = None
    ) -> list[CaseHandoff]:
        async with self.pool.acquire() as conn:
            if case_id is not None and status is not None:
                if limit is not None:
                    rows = await conn.fetch(
                        "SELECT payload FROM case_handoffs WHERE case_id = $1 AND status = $2 ORDER BY updated_at DESC LIMIT $3",
                        case_id,
                        status,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT payload FROM case_handoffs WHERE case_id = $1 AND status = $2 ORDER BY updated_at DESC",
                        case_id,
                        status,
                    )
            elif case_id is not None:
                if limit is not None:
                    rows = await conn.fetch(
                        "SELECT payload FROM case_handoffs WHERE case_id = $1 ORDER BY updated_at DESC LIMIT $2",
                        case_id,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT payload FROM case_handoffs WHERE case_id = $1 ORDER BY updated_at DESC", case_id
                    )
            elif status is not None:
                if limit is not None:
                    rows = await conn.fetch(
                        "SELECT payload FROM case_handoffs WHERE status = $1 ORDER BY updated_at DESC LIMIT $2",
                        status,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT payload FROM case_handoffs WHERE status = $1 ORDER BY updated_at DESC", status
                    )
            else:
                if limit is not None:
                    rows = await conn.fetch(
                        "SELECT payload FROM case_handoffs ORDER BY updated_at DESC LIMIT $1",
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch("SELECT payload FROM case_handoffs ORDER BY updated_at DESC")
        return [CaseHandoff.model_validate(_row_payload(row)) for row in rows]

    async def count_handoffs(self, *, case_id: str | None = None, status: str | None = None) -> int:
        async with self.pool.acquire() as conn:
            if case_id is not None and status is not None:
                value = await conn.fetchval(
                    "SELECT count(*) FROM case_handoffs WHERE case_id = $1 AND status = $2",
                    case_id,
                    status,
                )
            elif case_id is not None:
                value = await conn.fetchval("SELECT count(*) FROM case_handoffs WHERE case_id = $1", case_id)
            elif status is not None:
                value = await conn.fetchval("SELECT count(*) FROM case_handoffs WHERE status = $1", status)
            else:
                value = await conn.fetchval("SELECT count(*) FROM case_handoffs")
        return int(value or 0)

    async def record_handoff_delivery(self, delivery: HandoffTransportDelivery) -> HandoffTransportDelivery:
        return await _record_handoff_delivery(self.pool, delivery, upsert=True)

    async def update_handoff_delivery(self, delivery: HandoffTransportDelivery) -> HandoffTransportDelivery:
        return await _record_handoff_delivery(self.pool, delivery, upsert=False)

    async def append_handoff_update(
        self,
        update: HandoffUpdate,
        *,
        handoff_status: str | None = None,
        case_status: CaseStatus | None = None,
        event: CaseEvent | None = None,
    ) -> HandoffUpdateResult:
        lock_key = _advisory_lock_key(
            "handoff-update", {"source_loop": update.source_loop, "external_event_id": update.external_event_id}
        )
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)", lock_key)
            existing_row = await conn.fetchrow(
                """
                SELECT payload FROM handoff_updates
                WHERE source_loop = $1 AND external_event_id = $2
                FOR UPDATE
                """,
                update.source_loop,
                update.external_event_id,
            )
            if existing_row:
                existing_update = HandoffUpdate.model_validate(_row_payload(existing_row))
                handoff_row = await conn.fetchrow(
                    "SELECT payload FROM case_handoffs WHERE handoff_id = $1", existing_update.handoff_id
                )
                if not handoff_row:
                    raise KeyError(f"handoff not found: {existing_update.handoff_id}")
                handoff = CaseHandoff.model_validate(_row_payload(handoff_row))
                case_row = await conn.fetchrow("SELECT payload FROM cases WHERE case_id = $1", handoff.case_id)
                case = _case_from_payload(_row_payload(case_row)) if case_row else None
                if not isinstance(case, AtomicCaseProjection):
                    raise KeyError(f"atomic case not found: {handoff.case_id}")
                return HandoffUpdateResult(existing_update, handoff, case, None, False)
            handoff_row = await conn.fetchrow(
                "SELECT payload FROM case_handoffs WHERE handoff_id = $1 FOR UPDATE", update.handoff_id
            )
            if not handoff_row:
                raise KeyError(f"handoff not found: {update.handoff_id}")
            handoff = CaseHandoff.model_validate(_row_payload(handoff_row))
            case_row = await conn.fetchrow("SELECT payload FROM cases WHERE case_id = $1 FOR UPDATE", handoff.case_id)
            case = _case_from_payload(_row_payload(case_row)) if case_row else None
            if not isinstance(case, AtomicCaseProjection):
                raise KeyError(f"atomic case not found: {handoff.case_id}")
            target_status = cast(HandoffStatus, handoff_status or update.status)
            if target_status in VERIFIER_ONLY_HANDOFF_STATUSES:
                raise ValueError("verified/resolved require the dedicated NOC verifier path")
            require_handoff_transition(handoff.status, target_status, actor_loop=update.source_loop)
            stored_update = await _insert_handoff_update(conn, update)
            updated_handoff = handoff.model_copy(deep=True)
            updated_handoff.status = target_status
            updated_handoff.updated_at = stored_update.created_at
            updated_handoff = await _update_case_handoff(conn, updated_handoff)
            updated_case = case.model_copy(deep=True)
            if case_status is not None:
                updated_case.status = case_status
            updated_case.handoff_status = updated_handoff.status
            updated_case.last_handoff_at = stored_update.created_at
            updated_case.updated_at = stored_update.created_at
            await _update_case_projection(conn, updated_case)
            stored_event = await _insert_case_event(conn, event) if event is not None else None
            return HandoffUpdateResult(stored_update, updated_handoff, updated_case, stored_event, True)

    async def claim_callback_event(self, callback: CallbackInboxRecord) -> CallbackClaimResult:
        lock_key = _advisory_lock_key(
            "callback", {"source_loop": callback.source_loop, "external_event_id": callback.external_event_id}
        )
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)", lock_key)
            row = await conn.fetchrow(
                """
                INSERT INTO callback_inbox (
                    callback_id, source_loop, external_event_id, payload_hash, case_id,
                    handoff_id, status, received_at, result_payload, schema_version
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)
                ON CONFLICT (source_loop, external_event_id) DO NOTHING
                RETURNING result_payload AS payload
                """,
                callback.callback_id,
                callback.source_loop,
                callback.external_event_id,
                callback.payload_hash,
                callback.case_id or None,
                callback.handoff_id or None,
                callback.status,
                callback.received_at,
                json.dumps(callback.model_dump(mode="json")),
                callback.schema_version,
            )
            if row:
                return CallbackClaimResult(CallbackInboxRecord.model_validate(_row_payload(row)), True)
            existing = await conn.fetchrow(
                "SELECT result_payload AS payload FROM callback_inbox WHERE source_loop = $1 AND external_event_id = $2",
                callback.source_loop,
                callback.external_event_id,
            )
            if not existing:
                raise RuntimeError("callback conflict row missing after insert conflict")
            return CallbackClaimResult(CallbackInboxRecord.model_validate(_row_payload(existing)), False)

    async def upsert_verification_objective(
        self, objective: VerificationObjective, *, event: CaseEvent | None = None
    ) -> VerificationObjective:
        async with self.pool.acquire() as conn, conn.transaction():
            stored = await _upsert_verification_objective(conn, objective)
            if event is not None:
                await _insert_case_event(conn, event)
            return stored

    async def update_verification_objective_result(
        self, objective: VerificationObjective, *, event: CaseEvent | None = None
    ) -> VerificationObjective:
        payload = objective.model_dump(mode="json")
        async with self.pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                """
                UPDATE verification_objectives SET
                    status = $2,
                    required_status = $3,
                    required = $4,
                    required_consecutive_passes = $5,
                    consecutive_pass_count = $6,
                    last_checked_at = $7,
                    next_check_at = $8,
                    evidence_ref = $9,
                    failure_reason = $10,
                    updated_at = $11,
                    payload = $12::jsonb,
                    schema_version = $13
                WHERE objective_id = $1
                RETURNING payload
                """,
                objective.objective_id,
                objective.status,
                objective.required_status,
                objective.required,
                objective.required_consecutive_passes,
                objective.consecutive_pass_count,
                objective.last_checked_at,
                objective.next_check_at,
                objective.evidence_ref,
                objective.failure_reason,
                objective.updated_at,
                json.dumps(payload),
                objective.schema_version,
            )
            if not row:
                raise KeyError(f"verification objective not found: {objective.objective_id}")
            if event is not None:
                await _insert_case_event(conn, event)
        return VerificationObjective.model_validate(_row_payload(row))

    async def list_due_verification_objectives(self, *, now: str, limit: int = 100) -> list[VerificationObjective]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT payload FROM verification_objectives
                WHERE status NOT IN ('pass', 'skipped')
                  AND (next_check_at = '' OR next_check_at <= $1)
                ORDER BY next_check_at ASC, created_at ASC, objective_id ASC
                LIMIT $2
                """,
                now,
                _bounded_limit(limit),
            )
        return [VerificationObjective.model_validate(_row_payload(row)) for row in rows]

    async def list_verification_objectives(
        self, *, case_id: str | None = None, limit: int | None = None, newest_first: bool = False
    ) -> list[VerificationObjective]:
        direction = "DESC" if newest_first else "ASC"
        async with self.pool.acquire() as conn:
            if case_id is not None:
                if limit is not None:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM verification_objectives
                        WHERE case_id = $1
                        ORDER BY updated_at {direction}, created_at {direction}, objective_id {direction}
                        LIMIT $2
                        """,
                        case_id,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM verification_objectives
                        WHERE case_id = $1
                        ORDER BY updated_at {direction}, created_at {direction}, objective_id {direction}
                        """,
                        case_id,
                    )
            else:
                if limit is not None:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM verification_objectives
                        ORDER BY updated_at {direction}, created_at {direction}, objective_id {direction}
                        LIMIT $1
                        """,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM verification_objectives
                        ORDER BY updated_at {direction}, created_at {direction}, objective_id {direction}
                        """
                    )
        return [VerificationObjective.model_validate(_row_payload(row)) for row in rows]

    async def count_verification_objectives(self, *, case_id: str | None = None) -> int:
        async with self.pool.acquire() as conn:
            if case_id is not None:
                value = await conn.fetchval("SELECT count(*) FROM verification_objectives WHERE case_id = $1", case_id)
            else:
                value = await conn.fetchval("SELECT count(*) FROM verification_objectives")
        return int(value or 0)

    async def mark_handoff_verified(self, handoff_id: str, *, now: str, event: CaseEvent | None = None) -> CaseHandoff:
        async with self.pool.acquire() as conn, conn.transaction():
            handoff_row = await conn.fetchrow(
                "SELECT payload FROM case_handoffs WHERE handoff_id = $1 FOR UPDATE", handoff_id
            )
            if not handoff_row:
                raise KeyError(f"handoff not found: {handoff_id}")
            handoff = CaseHandoff.model_validate(_row_payload(handoff_row))
            require_handoff_transition(handoff.status, "verified", actor_loop="noc")
            updated_handoff = handoff.model_copy(deep=True)
            updated_handoff.status = "verified"
            updated_handoff.updated_at = now
            updated_handoff = await _update_case_handoff(conn, updated_handoff)
            case_row = await conn.fetchrow(
                "SELECT payload FROM cases WHERE case_id = $1 FOR UPDATE", updated_handoff.case_id
            )
            case = _case_from_payload(_row_payload(case_row)) if case_row else None
            if not isinstance(case, AtomicCaseProjection):
                raise KeyError(f"atomic case not found: {updated_handoff.case_id}")
            case.status = "verification_pending"
            case.handoff_status = updated_handoff.status
            case.last_handoff_at = now
            case.updated_at = now
            await _update_case_projection(conn, case)
            if event is not None:
                await _insert_case_event(conn, event)
            return updated_handoff

    async def resolve_case_with_outcome(
        self,
        case_id: str,
        *,
        handoff_id: str = "",
        outcome: OutcomeRecord,
        now: str,
        event: CaseEvent | None = None,
    ) -> AtomicCaseProjection:
        async with self.pool.acquire() as conn, conn.transaction():
            case_row = await conn.fetchrow("SELECT payload FROM cases WHERE case_id = $1 FOR UPDATE", case_id)
            case = _case_from_payload(_row_payload(case_row)) if case_row else None
            if not isinstance(case, AtomicCaseProjection):
                raise KeyError(f"atomic case not found: {case_id}")
            if outcome.work_item_id != case.case_id:
                raise ValueError("outcome work_item_id must match case_id")
            if handoff_id:
                handoff_row = await conn.fetchrow(
                    "SELECT payload FROM case_handoffs WHERE handoff_id = $1 FOR UPDATE", handoff_id
                )
                if not handoff_row:
                    raise KeyError(f"handoff not found: {handoff_id}")
                handoff = CaseHandoff.model_validate(_row_payload(handoff_row))
                require_handoff_transition(handoff.status, "resolved", actor_loop="noc")
                handoff.status = "resolved"
                handoff.updated_at = now
                await _update_case_handoff(conn, handoff)
            await _insert_outcome(conn, outcome)
            case.status = "resolved"
            case.resolved_at = now
            case.resolution_reason = "lhp_outcome_verified"
            case.updated_at = now
            await _update_case_projection(conn, case)
            if event is not None:
                await _insert_case_event(conn, event)
            return case

    async def record_knowledge_artifact(
        self, artifact: KnowledgeArtifact, *, event: CaseEvent | None = None
    ) -> KnowledgeArtifact:
        payload = artifact.model_dump(mode="json")
        async with self.pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO knowledge_artifacts (
                    artifact_id, case_id, handoff_id, artifact_type, scope, status,
                    review_status, version, content_hash, created_by, created_at,
                    payload, schema_version
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13)
                ON CONFLICT (case_id, artifact_type, version) DO UPDATE SET
                    status = EXCLUDED.status,
                    review_status = EXCLUDED.review_status,
                    content_hash = EXCLUDED.content_hash,
                    payload = EXCLUDED.payload || jsonb_build_object(
                        'artifact_id', knowledge_artifacts.artifact_id,
                        'created_at', knowledge_artifacts.created_at
                    ),
                    schema_version = EXCLUDED.schema_version
                RETURNING payload
                """,
                artifact.artifact_id,
                artifact.case_id,
                artifact.handoff_id or None,
                artifact.artifact_type,
                artifact.scope,
                artifact.status,
                artifact.review_status,
                artifact.version,
                artifact.content_hash,
                artifact.created_by,
                artifact.created_at,
                json.dumps(payload),
                artifact.schema_version,
            )
            if event is not None:
                await _insert_case_event(conn, event)
        return KnowledgeArtifact.model_validate(_row_payload(row))

    async def update_knowledge_artifact(
        self, artifact: KnowledgeArtifact, *, event: CaseEvent | None = None
    ) -> KnowledgeArtifact:
        payload = artifact.model_dump(mode="json")
        async with self.pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                """
                UPDATE knowledge_artifacts SET
                    status = $2,
                    review_status = $3,
                    content_hash = $4,
                    payload = $5::jsonb,
                    schema_version = $6
                WHERE artifact_id = $1
                RETURNING payload
                """,
                artifact.artifact_id,
                artifact.status,
                artifact.review_status,
                artifact.content_hash,
                json.dumps(payload),
                artifact.schema_version,
            )
            if row is None:
                raise KeyError(f"knowledge artifact not found: {artifact.artifact_id}")
            if event is not None:
                await _insert_case_event(conn, event)
        return KnowledgeArtifact.model_validate(_row_payload(row))

    async def list_knowledge_artifacts(
        self, *, case_id: str | None = None, limit: int | None = None, newest_first: bool = False
    ) -> list[KnowledgeArtifact]:
        direction = "DESC" if newest_first else "ASC"
        async with self.pool.acquire() as conn:
            if case_id is not None:
                if limit is not None:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM knowledge_artifacts
                        WHERE case_id = $1
                        ORDER BY created_at {direction}, artifact_id {direction}
                        LIMIT $2
                        """,
                        case_id,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM knowledge_artifacts
                        WHERE case_id = $1
                        ORDER BY created_at {direction}, artifact_id {direction}
                        """,
                        case_id,
                    )
            else:
                if limit is not None:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM knowledge_artifacts
                        ORDER BY created_at {direction}, artifact_id {direction}
                        LIMIT $1
                        """,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        f"SELECT payload FROM knowledge_artifacts ORDER BY created_at {direction}, artifact_id {direction}"
                    )
        return [KnowledgeArtifact.model_validate(_row_payload(row)) for row in rows]

    async def count_knowledge_artifacts(self, *, case_id: str | None = None) -> int:
        async with self.pool.acquire() as conn:
            if case_id is not None:
                value = await conn.fetchval("SELECT count(*) FROM knowledge_artifacts WHERE case_id = $1", case_id)
            else:
                value = await conn.fetchval("SELECT count(*) FROM knowledge_artifacts")
        return int(value or 0)

    async def record_outcome(self, outcome: OutcomeRecord, *, event: CaseEvent | None = None) -> OutcomeRecord:
        async with self.pool.acquire() as conn, conn.transaction():
            stored = await _insert_outcome(conn, outcome)
            if event is not None:
                await _insert_case_event(conn, event)
            return stored

    async def list_outcomes(
        self, *, case_id: str | None = None, limit: int | None = None, newest_first: bool = False
    ) -> list[OutcomeRecord]:
        direction = "DESC" if newest_first else "ASC"
        async with self.pool.acquire() as conn:
            if case_id is not None:
                if limit is not None:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM outcome_records
                        WHERE work_item_id = $1
                        ORDER BY created_at {direction}, outcome_id {direction}
                        LIMIT $2
                        """,
                        case_id,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM outcome_records
                        WHERE work_item_id = $1
                        ORDER BY created_at {direction}, outcome_id {direction}
                        """,
                        case_id,
                    )
            else:
                if limit is not None:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM outcome_records
                        ORDER BY created_at {direction}, outcome_id {direction}
                        LIMIT $1
                        """,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        f"SELECT payload FROM outcome_records ORDER BY created_at {direction}, outcome_id {direction}"
                    )
        return [OutcomeRecord.model_validate(_row_payload(row)) for row in rows]

    async def count_outcomes(self, *, case_id: str | None = None) -> int:
        async with self.pool.acquire() as conn:
            if case_id is not None:
                value = await conn.fetchval("SELECT count(*) FROM outcome_records WHERE work_item_id = $1", case_id)
            else:
                value = await conn.fetchval("SELECT count(*) FROM outcome_records")
        return int(value or 0)

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

    async def list_traces(
        self,
        *,
        case_id: str | None = None,
        meta_case_id: str | None = None,
        limit: int | None = None,
        newest_first: bool = False,
    ) -> list[TraceRecord]:
        direction = "DESC" if newest_first else "ASC"
        async with self.pool.acquire() as conn:
            if case_id is not None:
                if limit is not None:
                    rows = await conn.fetch(
                        f"SELECT payload FROM traces WHERE case_id = $1 ORDER BY created_at {direction}, trace_id {direction} LIMIT $2",
                        case_id,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        f"SELECT payload FROM traces WHERE case_id = $1 ORDER BY created_at {direction}, trace_id {direction}",
                        case_id,
                    )
            elif meta_case_id is not None:
                if limit is not None:
                    rows = await conn.fetch(
                        f"SELECT payload FROM traces WHERE meta_case_id = $1 ORDER BY created_at {direction}, trace_id {direction} LIMIT $2",
                        meta_case_id,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        f"SELECT payload FROM traces WHERE meta_case_id = $1 ORDER BY created_at {direction}, trace_id {direction}",
                        meta_case_id,
                    )
            else:
                if limit is not None:
                    rows = await conn.fetch(
                        f"SELECT payload FROM traces ORDER BY created_at {direction}, trace_id {direction} LIMIT $1",
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        f"SELECT payload FROM traces ORDER BY created_at {direction}, trace_id {direction}"
                    )
        return [TraceRecord.model_validate(_row_payload(row)) for row in rows]

    async def count_traces(self, *, case_id: str | None = None, meta_case_id: str | None = None) -> int:
        async with self.pool.acquire() as conn:
            if case_id is not None:
                value = await conn.fetchval("SELECT count(*) FROM traces WHERE case_id = $1", case_id)
            elif meta_case_id is not None:
                value = await conn.fetchval("SELECT count(*) FROM traces WHERE meta_case_id = $1", meta_case_id)
            else:
                value = await conn.fetchval("SELECT count(*) FROM traces")
        return int(value or 0)

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

    async def list_feedback(
        self,
        *,
        case_id: str | None = None,
        meta_case_id: str | None = None,
        limit: int | None = None,
        newest_first: bool = False,
    ) -> list[OperatorFeedback]:
        direction = "DESC" if newest_first else "ASC"
        async with self.pool.acquire() as conn:
            if case_id is not None:
                if limit is not None:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM operator_feedback
                        WHERE case_id = $1
                        ORDER BY created_at {direction}, feedback_id {direction}
                        LIMIT $2
                        """,
                        case_id,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM operator_feedback
                        WHERE case_id = $1
                        ORDER BY created_at {direction}, feedback_id {direction}
                        """,
                        case_id,
                    )
            elif meta_case_id is not None:
                if limit is not None:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM operator_feedback
                        WHERE meta_case_id = $1
                        ORDER BY created_at {direction}, feedback_id {direction}
                        LIMIT $2
                        """,
                        meta_case_id,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM operator_feedback
                        WHERE meta_case_id = $1
                        ORDER BY created_at {direction}, feedback_id {direction}
                        """,
                        meta_case_id,
                    )
            else:
                if limit is not None:
                    rows = await conn.fetch(
                        f"""
                        SELECT payload FROM operator_feedback
                        ORDER BY created_at {direction}, feedback_id {direction}
                        LIMIT $1
                        """,
                        _bounded_limit(limit),
                    )
                else:
                    rows = await conn.fetch(
                        f"SELECT payload FROM operator_feedback ORDER BY created_at {direction}, feedback_id {direction}"
                    )
        return [OperatorFeedback.model_validate(_row_payload(row)) for row in rows]

    async def count_feedback(self, *, case_id: str | None = None, meta_case_id: str | None = None) -> int:
        async with self.pool.acquire() as conn:
            if case_id is not None:
                value = await conn.fetchval("SELECT count(*) FROM operator_feedback WHERE case_id = $1", case_id)
            elif meta_case_id is not None:
                value = await conn.fetchval(
                    "SELECT count(*) FROM operator_feedback WHERE meta_case_id = $1", meta_case_id
                )
            else:
                value = await conn.fetchval("SELECT count(*) FROM operator_feedback")
        return int(value or 0)


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


async def _insert_outbox_intent(conn: Any, intent: OutboxIntent) -> OutboxIntent:
    payload = intent.model_dump(mode="json")
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


async def _update_case_projection(conn: Any, case: AtomicCaseProjection) -> AtomicCaseProjection:
    payload = case.model_dump(mode="json")
    row = await conn.fetchrow(
        """
        UPDATE cases SET
            status = $2,
            fingerprint = $3,
            updated_at = $4,
            payload = $5::jsonb,
            row_version = cases.row_version + 1,
            schema_version = $6
        WHERE case_id = $1
        RETURNING payload
        """,
        case.case_id,
        case.status,
        case.fingerprint,
        case.updated_at,
        json.dumps(payload),
        case.schema_version,
    )
    if not row:
        raise KeyError(f"case not found: {case.case_id}")
    return cast(AtomicCaseProjection, _case_from_payload(_row_payload(row)))


async def _insert_case_handoff(conn: Any, handoff: CaseHandoff) -> CaseHandoff:
    payload = handoff.model_dump(mode="json")
    row = await conn.fetchrow(
        """
        INSERT INTO case_handoffs (
            handoff_id, case_id, source_loop, target_loop, objective_key, objective,
            knowledge_scope, status, owner, verifier, idempotency_key, fingerprint,
            correlation_id, trace_id, created_at, updated_at, expires_at, payload, schema_version
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb,$19)
        RETURNING payload
        """,
        handoff.handoff_id,
        handoff.case_id,
        handoff.source_loop,
        handoff.target_loop,
        handoff.objective_key,
        handoff.objective,
        handoff.knowledge_scope,
        handoff.status,
        handoff.owner,
        handoff.verifier,
        handoff.idempotency_key,
        handoff.fingerprint,
        handoff.correlation_id,
        handoff.trace_id,
        handoff.created_at,
        handoff.updated_at,
        handoff.expires_at,
        json.dumps(payload),
        handoff.schema_version,
    )
    return CaseHandoff.model_validate(_row_payload(row))


async def _update_case_handoff(conn: Any, handoff: CaseHandoff) -> CaseHandoff:
    payload = handoff.model_dump(mode="json")
    row = await conn.fetchrow(
        """
        UPDATE case_handoffs SET
            status = $2,
            owner = $3,
            verifier = $4,
            correlation_id = $5,
            trace_id = $6,
            updated_at = $7,
            expires_at = $8,
            payload = $9::jsonb,
            schema_version = $10
        WHERE handoff_id = $1
        RETURNING payload
        """,
        handoff.handoff_id,
        handoff.status,
        handoff.owner,
        handoff.verifier,
        handoff.correlation_id,
        handoff.trace_id,
        handoff.updated_at,
        handoff.expires_at,
        json.dumps(payload),
        handoff.schema_version,
    )
    if not row:
        raise KeyError(f"handoff not found: {handoff.handoff_id}")
    return CaseHandoff.model_validate(_row_payload(row))


async def _insert_handoff_update(conn: Any, update: HandoffUpdate) -> HandoffUpdate:
    payload = update.model_dump(mode="json")
    row = await conn.fetchrow(
        """
        INSERT INTO handoff_updates (
            update_id, handoff_id, case_id, source_loop, update_type, status,
            external_event_id, correlation_id, trace_id, payload_hash, created_at,
            payload, schema_version
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13)
        RETURNING payload
        """,
        update.update_id,
        update.handoff_id,
        update.case_id,
        update.source_loop,
        update.update_type,
        update.status,
        update.external_event_id,
        update.correlation_id,
        update.trace_id,
        update.payload_hash,
        update.created_at,
        json.dumps(payload),
        update.schema_version,
    )
    return HandoffUpdate.model_validate(_row_payload(row))


async def _upsert_verification_objective(conn: Any, objective: VerificationObjective) -> VerificationObjective:
    payload = objective.model_dump(mode="json")
    row = await conn.fetchrow(
        """
        INSERT INTO verification_objectives (
            objective_id, case_id, handoff_id, objective_key, objective_type, name,
            status, required_status, required, required_consecutive_passes,
            consecutive_pass_count, last_checked_at, next_check_at, evidence_ref,
            failure_reason, created_at, updated_at, payload, schema_version
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb,$19)
        ON CONFLICT (case_id, objective_key) DO UPDATE SET
            handoff_id = EXCLUDED.handoff_id,
            objective_type = EXCLUDED.objective_type,
            name = EXCLUDED.name,
            status = EXCLUDED.status,
            required_status = EXCLUDED.required_status,
            required = EXCLUDED.required,
            required_consecutive_passes = EXCLUDED.required_consecutive_passes,
            consecutive_pass_count = EXCLUDED.consecutive_pass_count,
            last_checked_at = EXCLUDED.last_checked_at,
            next_check_at = EXCLUDED.next_check_at,
            evidence_ref = EXCLUDED.evidence_ref,
            failure_reason = EXCLUDED.failure_reason,
            updated_at = EXCLUDED.updated_at,
            payload = EXCLUDED.payload || jsonb_build_object(
                'objective_id', verification_objectives.objective_id,
                'created_at', verification_objectives.created_at
            ),
            schema_version = EXCLUDED.schema_version
        RETURNING payload
        """,
        objective.objective_id,
        objective.case_id,
        objective.handoff_id or None,
        objective.objective_key,
        objective.objective_type,
        objective.name,
        objective.status,
        objective.required_status,
        objective.required,
        objective.required_consecutive_passes,
        objective.consecutive_pass_count,
        objective.last_checked_at,
        objective.next_check_at,
        objective.evidence_ref,
        objective.failure_reason,
        objective.created_at,
        objective.updated_at,
        json.dumps(payload),
        objective.schema_version,
    )
    return VerificationObjective.model_validate(_row_payload(row))


async def _record_handoff_delivery(
    pool: Any, delivery: HandoffTransportDelivery, *, upsert: bool
) -> HandoffTransportDelivery:
    payload = delivery.model_dump(mode="json")
    async with pool.acquire() as conn:
        if upsert:
            row = await conn.fetchrow(
                """
                INSERT INTO handoff_transport_deliveries (
                    delivery_id, handoff_id, case_id, transport, status, idempotency_key,
                    external_id, external_url, attempts, max_attempts, next_attempt_at,
                    last_error, payload_hash, created_at, updated_at, payload, schema_version
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb,$17)
                ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = handoff_transport_deliveries.idempotency_key
                RETURNING payload
                """,
                delivery.delivery_id,
                delivery.handoff_id,
                delivery.case_id,
                delivery.transport,
                delivery.status,
                delivery.idempotency_key,
                delivery.external_id,
                delivery.external_url,
                delivery.attempts,
                delivery.max_attempts,
                delivery.next_attempt_at,
                delivery.last_error,
                delivery.payload_hash,
                delivery.created_at,
                delivery.updated_at,
                json.dumps(payload),
                delivery.schema_version,
            )
        else:
            row = await conn.fetchrow(
                """
                UPDATE handoff_transport_deliveries SET
                    status = $2,
                    external_id = $3,
                    external_url = $4,
                    attempts = $5,
                    max_attempts = $6,
                    next_attempt_at = $7,
                    last_error = $8,
                    payload_hash = $9,
                    updated_at = $10,
                    payload = $11::jsonb,
                    schema_version = $12
                WHERE delivery_id = $1
                RETURNING payload
                """,
                delivery.delivery_id,
                delivery.status,
                delivery.external_id,
                delivery.external_url,
                delivery.attempts,
                delivery.max_attempts,
                delivery.next_attempt_at,
                delivery.last_error,
                delivery.payload_hash,
                delivery.updated_at,
                json.dumps(payload),
                delivery.schema_version,
            )
    if not row:
        raise KeyError(f"handoff delivery not found: {delivery.delivery_id}")
    return HandoffTransportDelivery.model_validate(_row_payload(row))


async def _insert_outcome(conn: Any, outcome: OutcomeRecord) -> OutcomeRecord:
    payload = outcome.model_dump(mode="json")
    row = await conn.fetchrow(
        """
        INSERT INTO outcome_records (
            outcome_id, work_item_type, work_item_id, case_type, fingerprint,
            created_at, payload, schema_version
        ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)
        ON CONFLICT (outcome_id) DO UPDATE SET outcome_id = outcome_records.outcome_id
        RETURNING payload
        """,
        outcome.outcome_id,
        outcome.work_item_type,
        outcome.work_item_id,
        outcome.case_type,
        outcome.fingerprint,
        outcome.created_at,
        json.dumps(payload),
        outcome.schema_version,
    )
    return OutcomeRecord.model_validate(_row_payload(row))


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


def _advisory_lock_key(scope: str, payload: dict[str, Any]) -> str:
    return f"{scope}:{lhp_payload_hash(payload)[:32]}"


def _bounded_limit(limit: int, *, default: int = 100, maximum: int = 500) -> int:
    if limit <= 0:
        return default
    return min(limit, maximum)
