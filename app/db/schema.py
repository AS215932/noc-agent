"""Postgres schema for case-grounded operational state.

The first version stores strongly-versioned JSONB projections plus relational
indexes/uniqueness constraints for the invariants the state machine needs. These
tables now back the primary reactive, control-plane, and proactive case paths.
"""

from __future__ import annotations

CASE_SCHEMA_VERSION = 2

SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS observation_inbox (
        inbox_id TEXT PRIMARY KEY,
        source TEXT NOT NULL DEFAULT '',
        source_event_id TEXT NOT NULL DEFAULT '',
        source_fingerprint TEXT NOT NULL DEFAULT '',
        dedup_key TEXT NOT NULL UNIQUE,
        received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        normalization_status TEXT NOT NULL DEFAULT 'pending',
        normalized_observation_id TEXT NOT NULL DEFAULT '',
        error TEXT NOT NULL DEFAULT '',
        attempts INTEGER NOT NULL DEFAULT 0,
        schema_version INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS observations (
        observation_id TEXT PRIMARY KEY,
        source TEXT NOT NULL DEFAULT '',
        detector TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'unknown',
        severity TEXT NOT NULL DEFAULT 'UNKNOWN',
        dedup_key TEXT NOT NULL DEFAULT '',
        source_event_id TEXT NOT NULL DEFAULT '',
        source_fingerprint TEXT NOT NULL DEFAULT '',
        observed_at TEXT NOT NULL DEFAULT '',
        received_at TEXT NOT NULL DEFAULT '',
        scan_cycle_id TEXT NOT NULL DEFAULT '',
        signal_signature TEXT NOT NULL DEFAULT '',
        source_health TEXT NOT NULL DEFAULT 'unknown',
        payload JSONB NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS observations_dedup_key_idx
        ON observations (dedup_key)
        WHERE dedup_key <> ''
    """,
    """
    CREATE TABLE IF NOT EXISTS cases (
        case_id TEXT PRIMARY KEY,
        kind TEXT NOT NULL CHECK (kind IN ('atomic', 'meta')),
        status TEXT NOT NULL,
        fingerprint TEXT NOT NULL DEFAULT '',
        case_number TEXT NOT NULL DEFAULT '',
        event_fingerprint TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT '',
        opened_at TEXT NOT NULL DEFAULT '',
        payload JSONB NOT NULL,
        row_version BIGINT NOT NULL DEFAULT 1,
        schema_version INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS cases_active_atomic_fingerprint_idx
        ON cases (fingerprint)
        WHERE kind = 'atomic'
          AND fingerprint <> ''
          AND status NOT IN ('resolved', 'expired', 'closed', 'linked')
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS cases_active_meta_event_fingerprint_idx
        ON cases (event_fingerprint)
        WHERE kind = 'meta'
          AND event_fingerprint <> ''
          AND status NOT IN ('resolved', 'closed', 'split', 'merged')
    """,
    """
    CREATE TABLE IF NOT EXISTS case_events (
        event_id TEXT PRIMARY KEY,
        case_id TEXT REFERENCES cases(case_id) ON DELETE SET NULL,
        meta_case_id TEXT REFERENCES cases(case_id) ON DELETE SET NULL,
        event_type TEXT NOT NULL,
        actor_type TEXT NOT NULL DEFAULT 'system',
        actor_id TEXT NOT NULL DEFAULT '',
        source TEXT NOT NULL DEFAULT '',
        occurred_at TEXT NOT NULL DEFAULT '',
        observed_at TEXT NOT NULL DEFAULT '',
        correlation_id TEXT NOT NULL DEFAULT '',
        causation_id TEXT NOT NULL DEFAULT '',
        policy_version TEXT NOT NULL DEFAULT '',
        payload JSONB NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS case_events_case_id_idx ON case_events (case_id, occurred_at, event_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS case_events_meta_case_id_idx ON case_events (meta_case_id, occurred_at, event_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS case_identity_aliases (
        alias_id TEXT PRIMARY KEY,
        case_id TEXT NOT NULL REFERENCES cases(case_id) ON DELETE CASCADE,
        alias_type TEXT NOT NULL,
        alias_value TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT '',
        confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
        created_at TEXT NOT NULL DEFAULT '',
        retired_at TEXT
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS case_identity_aliases_active_unique_idx
        ON case_identity_aliases (alias_type, alias_value)
        WHERE retired_at IS NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS side_effect_outbox (
        outbox_id TEXT PRIMARY KEY,
        intent_type TEXT NOT NULL,
        case_id TEXT REFERENCES cases(case_id) ON DELETE SET NULL,
        meta_case_id TEXT REFERENCES cases(case_id) ON DELETE SET NULL,
        idempotency_key TEXT NOT NULL UNIQUE,
        state_signature TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT '',
        completed_at TEXT,
        external_id TEXT NOT NULL DEFAULT '',
        external_url TEXT NOT NULL DEFAULT '',
        error TEXT NOT NULL DEFAULT '',
        payload JSONB NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS side_effect_outbox_pending_idx
        ON side_effect_outbox (status, next_attempt_at, created_at)
        WHERE status IN ('pending', 'failed')
    """,
    """
    CREATE TABLE IF NOT EXISTS meta_case_correlation_evidence (
        evidence_id TEXT PRIMARY KEY,
        meta_case_id TEXT NOT NULL REFERENCES cases(case_id) ON DELETE CASCADE,
        observation_id TEXT REFERENCES observations(observation_id) ON DELETE SET NULL,
        case_id TEXT REFERENCES cases(case_id) ON DELETE SET NULL,
        evidence_type TEXT NOT NULL DEFAULT '',
        feature TEXT NOT NULL DEFAULT '',
        value JSONB NOT NULL DEFAULT '{}'::jsonb,
        confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        source TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS traces (
        trace_id TEXT PRIMARY KEY,
        cycle_id TEXT NOT NULL DEFAULT '',
        case_id TEXT REFERENCES cases(case_id) ON DELETE SET NULL,
        meta_case_id TEXT REFERENCES cases(case_id) ON DELETE SET NULL,
        trace_type TEXT NOT NULL,
        policy_version TEXT NOT NULL DEFAULT '',
        model_chain JSONB NOT NULL DEFAULT '[]'::jsonb,
        prompt_version TEXT NOT NULL DEFAULT '',
        knowledge_export_version TEXT NOT NULL DEFAULT '',
        payload JSONB NOT NULL,
        created_at TEXT NOT NULL DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS operator_feedback (
        feedback_id TEXT PRIMARY KEY,
        case_id TEXT REFERENCES cases(case_id) ON DELETE SET NULL,
        meta_case_id TEXT REFERENCES cases(case_id) ON DELETE SET NULL,
        trace_id TEXT REFERENCES traces(trace_id) ON DELETE SET NULL,
        actor_id TEXT NOT NULL DEFAULT '',
        actor_role TEXT NOT NULL DEFAULT '',
        feedback_type TEXT NOT NULL,
        payload JSONB NOT NULL,
        created_at TEXT NOT NULL DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS case_handoffs (
        handoff_id TEXT PRIMARY KEY,
        case_id TEXT NOT NULL REFERENCES cases(case_id) ON DELETE CASCADE,
        source_loop TEXT NOT NULL,
        target_loop TEXT NOT NULL,
        objective_key TEXT NOT NULL,
        objective TEXT NOT NULL DEFAULT '',
        knowledge_scope TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL,
        owner TEXT NOT NULL DEFAULT '',
        verifier TEXT NOT NULL DEFAULT 'noc',
        idempotency_key TEXT NOT NULL UNIQUE,
        fingerprint TEXT NOT NULL DEFAULT '',
        correlation_id TEXT NOT NULL DEFAULT '',
        trace_id TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT '',
        expires_at TEXT NOT NULL DEFAULT '',
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        schema_version TEXT NOT NULL DEFAULT 'lhp.v1'
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS case_handoffs_active_unique_idx
        ON case_handoffs (case_id, target_loop, objective_key)
        WHERE status NOT IN ('resolved', 'cancelled', 'expired')
    """,
    """
    CREATE INDEX IF NOT EXISTS case_handoffs_case_status_idx
        ON case_handoffs (case_id, status, updated_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS case_handoffs_target_status_idx
        ON case_handoffs (target_loop, status, updated_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS case_handoffs_fingerprint_idx
        ON case_handoffs (fingerprint)
    """,
    """
    CREATE INDEX IF NOT EXISTS case_handoffs_correlation_idx
        ON case_handoffs (correlation_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS handoff_updates (
        update_id TEXT PRIMARY KEY,
        handoff_id TEXT NOT NULL REFERENCES case_handoffs(handoff_id) ON DELETE CASCADE,
        case_id TEXT NOT NULL REFERENCES cases(case_id) ON DELETE CASCADE,
        source_loop TEXT NOT NULL,
        update_type TEXT NOT NULL,
        status TEXT NOT NULL,
        external_event_id TEXT NOT NULL DEFAULT '',
        correlation_id TEXT NOT NULL DEFAULT '',
        trace_id TEXT NOT NULL DEFAULT '',
        payload_hash TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT '',
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        schema_version TEXT NOT NULL DEFAULT 'lhp.v1'
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS handoff_updates_external_event_unique_idx
        ON handoff_updates (source_loop, external_event_id)
        WHERE external_event_id <> ''
    """,
    """
    CREATE INDEX IF NOT EXISTS handoff_updates_case_idx
        ON handoff_updates (case_id, created_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS handoff_updates_handoff_idx
        ON handoff_updates (handoff_id, created_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS handoff_updates_correlation_idx
        ON handoff_updates (correlation_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS verification_objectives (
        objective_id TEXT PRIMARY KEY,
        case_id TEXT NOT NULL REFERENCES cases(case_id) ON DELETE CASCADE,
        handoff_id TEXT REFERENCES case_handoffs(handoff_id) ON DELETE SET NULL,
        objective_key TEXT NOT NULL,
        objective_type TEXT NOT NULL DEFAULT '',
        name TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'pending',
        required_status TEXT NOT NULL DEFAULT 'pass',
        required BOOLEAN NOT NULL DEFAULT true,
        required_consecutive_passes INTEGER NOT NULL DEFAULT 3,
        consecutive_pass_count INTEGER NOT NULL DEFAULT 0,
        last_checked_at TEXT NOT NULL DEFAULT '',
        next_check_at TEXT NOT NULL DEFAULT '',
        evidence_ref TEXT NOT NULL DEFAULT '',
        failure_reason TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT '',
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        schema_version TEXT NOT NULL DEFAULT 'lhp.v1',
        UNIQUE(case_id, objective_key)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS verification_objectives_case_status_idx
        ON verification_objectives (case_id, status, updated_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS verification_objectives_due_idx
        ON verification_objectives (status, next_check_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS verification_objectives_handoff_idx
        ON verification_objectives (handoff_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS knowledge_artifacts (
        artifact_id TEXT PRIMARY KEY,
        case_id TEXT NOT NULL REFERENCES cases(case_id) ON DELETE CASCADE,
        handoff_id TEXT REFERENCES case_handoffs(handoff_id) ON DELETE SET NULL,
        artifact_type TEXT NOT NULL,
        scope TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'proposed',
        review_status TEXT NOT NULL DEFAULT 'pending',
        version INTEGER NOT NULL DEFAULT 1,
        content_hash TEXT NOT NULL DEFAULT '',
        created_by TEXT NOT NULL DEFAULT 'knowledge',
        created_at TEXT NOT NULL DEFAULT '',
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        schema_version TEXT NOT NULL DEFAULT 'lhp.v1',
        UNIQUE(case_id, artifact_type, version)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS knowledge_artifacts_case_idx
        ON knowledge_artifacts (case_id, created_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS knowledge_artifacts_review_idx
        ON knowledge_artifacts (review_status, created_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS knowledge_artifacts_handoff_idx
        ON knowledge_artifacts (handoff_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS outcome_records (
        outcome_id TEXT PRIMARY KEY,
        work_item_type TEXT NOT NULL DEFAULT 'case',
        work_item_id TEXT NOT NULL REFERENCES cases(case_id) ON DELETE CASCADE,
        case_type TEXT NOT NULL DEFAULT '',
        fingerprint TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT '',
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        schema_version TEXT NOT NULL DEFAULT 'lhp.v1'
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS outcome_records_work_item_idx
        ON outcome_records (work_item_type, work_item_id, created_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS outcome_records_fingerprint_idx
        ON outcome_records (fingerprint)
    """,
    """
    CREATE TABLE IF NOT EXISTS callback_inbox (
        callback_id TEXT PRIMARY KEY,
        source_loop TEXT NOT NULL,
        external_event_id TEXT NOT NULL,
        payload_hash TEXT NOT NULL,
        case_id TEXT REFERENCES cases(case_id) ON DELETE SET NULL,
        handoff_id TEXT REFERENCES case_handoffs(handoff_id) ON DELETE SET NULL,
        status TEXT NOT NULL DEFAULT 'accepted',
        received_at TEXT NOT NULL DEFAULT '',
        result_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        schema_version TEXT NOT NULL DEFAULT 'lhp.v1',
        UNIQUE(source_loop, external_event_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS callback_inbox_case_idx
        ON callback_inbox (case_id, received_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS callback_inbox_handoff_idx
        ON callback_inbox (handoff_id, received_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS handoff_transport_deliveries (
        delivery_id TEXT PRIMARY KEY,
        handoff_id TEXT NOT NULL REFERENCES case_handoffs(handoff_id) ON DELETE CASCADE,
        case_id TEXT NOT NULL REFERENCES cases(case_id) ON DELETE CASCADE,
        transport TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        idempotency_key TEXT NOT NULL UNIQUE,
        external_id TEXT NOT NULL DEFAULT '',
        external_url TEXT NOT NULL DEFAULT '',
        attempts INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 10,
        next_attempt_at TEXT NOT NULL DEFAULT '',
        last_error TEXT NOT NULL DEFAULT '',
        payload_hash TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT '',
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        schema_version TEXT NOT NULL DEFAULT 'lhp.v1'
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS handoff_transport_deliveries_handoff_idx
        ON handoff_transport_deliveries (handoff_id, transport, status)
    """,
    """
    CREATE INDEX IF NOT EXISTS handoff_transport_deliveries_status_idx
        ON handoff_transport_deliveries (status, next_attempt_at)
    """,
)


def schema_sql() -> str:
    return ";\n".join(statement.strip() for statement in SCHEMA_STATEMENTS) + ";\n"
