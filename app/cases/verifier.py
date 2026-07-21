"""Dedicated LHP-v1 verification scheduler.

The verifier is the only component allowed to move handoffs into verified and
case outcomes into resolved. It is disabled by default and can run in dry-run
mode for rollout.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from app import log
from app.cases.lhp import OutcomeRecord, VerificationObjective, sanitize_lhp_payload, sanitize_lhp_token
from app.cases.models import AtomicCaseProjection, utc_now
from app.cases.service import CaseService
from app.config import LoopHandoffSettings
from app.safe_errors import classify_exception, log_exception

VerificationCheckStatus = Literal["pass", "fail", "unknown", "skipped"]
VerifierCheck = Callable[[VerificationObjective, AtomicCaseProjection | None], Awaitable["VerificationCheckResult"]]


@dataclass(frozen=True, slots=True)
class VerificationCheckResult:
    status: VerificationCheckStatus
    evidence_ref: str = ""
    failure_reason: str = ""
    payload: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class VerificationRunReport:
    checked: int = 0
    updated: int = 0
    passed: int = 0
    failed: int = 0
    unknown: int = 0
    verified_handoffs: int = 0
    resolved_cases: int = 0
    dry_run: bool = True


class CaseVerifier:
    def __init__(
        self,
        case_service: CaseService,
        *,
        settings: LoopHandoffSettings,
        checker: VerifierCheck | None = None,
    ) -> None:
        self.case_service = case_service
        self.settings = settings
        self.checker = checker or default_verification_check

    async def run_once(self, *, now: str | None = None, limit: int = 100) -> VerificationRunReport:
        if not self.settings.case_verification_enabled:
            return VerificationRunReport(dry_run=self.settings.case_verification_dry_run)
        now = now or utc_now()
        due = await self.case_service.list_due_lhp_verification_objectives(now=now, limit=limit)
        checked = updated = passed = failed = unknown = verified = resolved = 0
        touched_handoffs: set[str] = set()
        for objective in due:
            checked += 1
            case = await self.case_service.store.get_case(objective.case_id)
            atomic_case = case if isinstance(case, AtomicCaseProjection) else None
            try:
                result = await self.checker(objective, atomic_case)
            except Exception as exc:
                safe = classify_exception(exc)
                log_exception("lhp_verification_check_failed", exc, category=safe.category, objective_id=objective.objective_id)
                result = VerificationCheckResult(status="unknown", failure_reason=safe.category)
            if result.status == "pass":
                passed += 1
            elif result.status == "fail":
                failed += 1
            else:
                unknown += 1
            updated_objective = _apply_check_result(
                objective,
                result,
                now=now,
                interval_s=self.settings.case_verification_interval_s,
            )
            if not self.settings.case_verification_dry_run:
                await self.case_service.record_lhp_verification_result(updated_objective)
                updated += 1
                if updated_objective.handoff_id:
                    touched_handoffs.add(updated_objective.handoff_id)
        if not self.settings.case_verification_dry_run:
            for handoff_id in sorted(touched_handoffs):
                did_verify, did_resolve = await self._maybe_verify_handoff(handoff_id)
                verified += int(did_verify)
                resolved += int(did_resolve)
        return VerificationRunReport(
            checked=checked,
            updated=updated,
            passed=passed,
            failed=failed,
            unknown=unknown,
            verified_handoffs=verified,
            resolved_cases=resolved,
            dry_run=self.settings.case_verification_dry_run,
        )

    async def _maybe_verify_handoff(self, handoff_id: str) -> tuple[bool, bool]:
        handoff = await self.case_service.get_lhp_handoff(handoff_id)
        if handoff is None or handoff.status in {"verified", "resolved", "cancelled", "expired"}:
            return False, False
        objectives = await self.case_service.list_lhp_verification_objectives(case_id=handoff.case_id)
        scoped = [objective for objective in objectives if objective.handoff_id == handoff_id and objective.required]
        if not scoped:
            return False, False
        if not all(_objective_satisfied(objective) for objective in scoped):
            return False, False
        verified = await self.case_service.mark_lhp_handoff_verified(handoff_id)
        if not self.settings.case_auto_resolve_enabled:
            return True, False
        outcome = OutcomeRecord(
            work_item_id=verified.case_id,
            case_type=verified.case_type,
            fingerprint=verified.fingerprint,
            validation={
                "verified_handoff_id": verified.handoff_id,
                "objective_ids": [objective.objective_id for objective in scoped],
                "required_consecutive_passes": self.settings.case_verification_required_consecutive_passes,
            },
            safety={"resolved_by": "noc_case_verifier", "auto_resolve_enabled": True},
        )
        await self.case_service.resolve_lhp_case_with_outcome(verified.case_id, handoff_id=verified.handoff_id, outcome=outcome)
        if self.settings.knowledge_context_enabled:
            await self.case_service.request_lhp_knowledge_artifact_proposal(
                verified.case_id,
                handoff_id=verified.handoff_id,
                outcome_id=outcome.outcome_id,
                payload={"source": "noc_case_verifier", "case_type": verified.case_type},
            )
        return True, True


async def default_verification_check(
    objective: VerificationObjective, case: AtomicCaseProjection | None
) -> VerificationCheckResult:
    if objective.objective_type == "monitoring_alert_clear":
        if case is None:
            return VerificationCheckResult(status="unknown", failure_reason="case_not_found")
        if _observed_clean_after_unhealthy(case.last_observed_clean, case.last_observed_unhealthy):
            safe_case_id = sanitize_lhp_token(case.case_id, limit=180)
            return VerificationCheckResult(status="pass", evidence_ref=f"case:{safe_case_id}:last_observed_clean")
        return VerificationCheckResult(status="unknown", failure_reason="no_positive_clean_observation")
    if objective.objective_type == "health_endpoint":
        return VerificationCheckResult(status="pass", evidence_ref="noc_process:verifier_alive")
    return VerificationCheckResult(status="unknown", failure_reason=f"unsupported_objective_type:{objective.objective_type}")


async def verifier_loop(verifier: CaseVerifier, *, interval_s: int, max_backoff_s: int = 300) -> None:
    log.info("lhp_verifier_loop_started", interval_s=interval_s, dry_run=verifier.settings.case_verification_dry_run)
    consecutive_failures = 0
    while True:
        sleep_s = max(1, interval_s)
        try:
            report = await verifier.run_once()
            consecutive_failures = 0
            if report.checked or report.verified_handoffs or report.resolved_cases:
                log.info(
                    "lhp_verifier_run",
                    checked=report.checked,
                    updated=report.updated,
                    passed=report.passed,
                    failed=report.failed,
                    unknown=report.unknown,
                    verified_handoffs=report.verified_handoffs,
                    resolved_cases=report.resolved_cases,
                    dry_run=report.dry_run,
                )
        except Exception as exc:
            consecutive_failures += 1
            sleep_s = min(max_backoff_s, max(1, interval_s) * min(2 ** (consecutive_failures - 1), 8))
            safe = classify_exception(exc)
            log_exception(
                "lhp_verifier_loop_failed",
                exc,
                category=safe.category,
                consecutive_failures=consecutive_failures,
                next_retry_s=sleep_s,
            )
        await asyncio.sleep(sleep_s)


def _apply_check_result(
    objective: VerificationObjective,
    result: VerificationCheckResult,
    *,
    now: str,
    interval_s: int,
) -> VerificationObjective:
    updated = objective.model_copy(deep=True)
    updated.last_checked_at = now
    updated.next_check_at = _next_check_at(now, interval_s)
    updated.evidence_ref = sanitize_lhp_token(result.evidence_ref, limit=180)
    updated.failure_reason = sanitize_lhp_token(result.failure_reason, limit=180)
    if result.payload:
        safe_payload = sanitize_lhp_payload(result.payload)
        if isinstance(safe_payload, dict):
            updated.result_payload.update(safe_payload)
        else:
            updated.result_payload["checker_payload"] = safe_payload
    if result.status == "pass":
        updated.consecutive_pass_count += 1
        updated.status = "pass" if updated.consecutive_pass_count >= updated.required_consecutive_passes else "pending"
    else:
        updated.consecutive_pass_count = 0
        updated.status = result.status
    updated.updated_at = now
    return updated


def _objective_satisfied(objective: VerificationObjective) -> bool:
    return objective.status == objective.required_status and objective.consecutive_pass_count >= objective.required_consecutive_passes


def _observed_clean_after_unhealthy(clean: str, unhealthy: str) -> bool:
    clean_dt = _parse_iso_time(clean)
    if clean_dt is None:
        return False
    unhealthy_dt = _parse_iso_time(unhealthy)
    return unhealthy_dt is None or clean_dt >= unhealthy_dt


def _next_check_at(now: str, interval_s: int) -> str:
    parsed = _parse_iso_time(now) or datetime.now(timezone.utc)
    return (parsed.astimezone(timezone.utc) + timedelta(seconds=max(1, interval_s))).isoformat()


def _parse_iso_time(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
