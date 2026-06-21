"""Postgres schema for case-grounded operational state.

The first version stores strongly-versioned JSONB projections plus relational
indexes/uniqueness constraints for the invariants the state machine needs. These
tables now back the primary reactive, control-plane, and proactive case paths.
"""

from __future__ import annotations

CASE_SCHEMA_VERSION = 1

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
)


def schema_sql() -> str:
    return ";\n".join(statement.strip() for statement in SCHEMA_STATEMENTS) + ";\n"
