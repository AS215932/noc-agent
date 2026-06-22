"""Versioned policy knobs for case-grounded decisions.

The existing proactive settings keep their current runtime behavior. This policy
object is the durable/replayable shape the CaseService will use when the
strangler flips land; every future case event can cite `policy_version` instead
of relying on scattered constants or process-local defaults.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class CasePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    policy_version: str = "case_policy_v1"
    report_reassert_s: int = Field(default=3600, ge=1)
    investigation_cooldown_s: int = Field(default=21600, ge=0)
    investigation_failure_retry_s: int = Field(default=21600, ge=0)
    reinvestigate_stale_s: int = Field(default=21600, ge=0)
    recovery_cooldown_s: int = Field(default=600, ge=0)
    suppression_default_ttl_s: int = Field(default=86400, ge=0)
    auto_snooze_ttl_s: int = Field(default=86400, ge=0)
    storm_correlation_window_s: int = Field(default=300, ge=1)
    storm_confidence_threshold: float = Field(default=0.75, ge=0.0, le=1.0)
    storm_stabilization_window_s: int = Field(default=900, ge=1)
    child_suppression_enabled: bool = True
    require_positive_clean_for_resolve: bool = True
    source_degraded_blocks_clean: bool = True
    knowledge_export_required_for_prod: bool = True
    approved_knowledge_statuses: tuple[str, ...] = ("approved", "reviewed")
    authoritative_knowledge_tiers: tuple[str, ...] = ("canonical", "advisory", "A0", "A1", "A2")

    @classmethod
    def from_proactive_settings(cls, settings: Any, *, policy_version: str = "case_policy_v1") -> "CasePolicy":
        """Seed policy values from today's `ProactiveLoopSettings` shape.

        Kept intentionally duck-typed so importing this module does not create a
        config dependency cycle and tests can pass a small stub.
        """

        return cls(
            policy_version=policy_version,
            report_reassert_s=int(getattr(settings, "report_reassert_s", cls.model_fields["report_reassert_s"].default)),
            investigation_cooldown_s=int(
                getattr(settings, "investigation_cooldown_s", cls.model_fields["investigation_cooldown_s"].default)
            ),
            investigation_failure_retry_s=int(
                getattr(
                    settings,
                    "investigation_failure_retry_s",
                    getattr(settings, "investigation_cooldown_s", cls.model_fields["investigation_failure_retry_s"].default),
                )
            ),
            reinvestigate_stale_s=int(
                getattr(settings, "investigation_cooldown_s", cls.model_fields["reinvestigate_stale_s"].default)
            ),
            auto_snooze_ttl_s=int(getattr(settings, "auto_snooze_ttl_s", cls.model_fields["auto_snooze_ttl_s"].default)),
        )
