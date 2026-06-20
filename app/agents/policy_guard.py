"""Deterministic validation for duty-officer action proposals."""

from __future__ import annotations

from app.agents.noc_duty import ActionIntent, ValidatedIntent
from app.cases.models import AtomicCaseProjection, MetaCaseProjection
from app.cases.store import CaseStore


class PolicyGuard:
    def __init__(self, store: CaseStore) -> None:
        self.store = store

    async def validate(self, intent: ActionIntent) -> ValidatedIntent:
        if intent.intent_type == "handoff":
            return await self._validate_handoff(intent)
        if intent.intent_type == "suppress" and not intent.requires_operator_approval:
            return _reject(intent, "suppression requires deterministic policy or operator approval")
        if intent.intent_type in {"merge_meta_cases", "split_meta_case", "detach_child_from_meta_case"}:
            if not intent.requires_operator_approval:
                return _reject(intent, f"{intent.intent_type} requires operator approval")
        if intent.intent_type == "attach_child_to_meta_case":
            return await self._validate_attach_child(intent)
        return ValidatedIntent(intent_id=intent.intent_id, validation_status="accepted", validator="PolicyGuard")

    async def _validate_handoff(self, intent: ActionIntent) -> ValidatedIntent:
        if not intent.target_case_id:
            return _reject(intent, "handoff requires target_case_id")
        case = await self.store.get_case(intent.target_case_id)
        if not isinstance(case, AtomicCaseProjection):
            return _reject(intent, "handoff target is not an atomic case")
        if case.issue_url or case.issue_id:
            return _reject(intent, "case already has issue_url/issue_id")
        return ValidatedIntent(intent_id=intent.intent_id, validation_status="accepted", validator="PolicyGuard")

    async def _validate_attach_child(self, intent: ActionIntent) -> ValidatedIntent:
        if not intent.target_case_id or not intent.target_meta_case_id:
            return _reject(intent, "attach_child_to_meta_case requires target_case_id and target_meta_case_id")
        child = await self.store.get_case(intent.target_case_id)
        meta = await self.store.get_case(intent.target_meta_case_id)
        if not isinstance(child, AtomicCaseProjection):
            return _reject(intent, "child target is not an atomic case")
        if not isinstance(meta, MetaCaseProjection):
            return _reject(intent, "meta target is not a meta-case")
        if child.independent_action_required:
            return _reject(intent, "child is marked independent_action_required")
        if child.meta_case_id and child.meta_case_id != meta.case_id:
            return _reject(intent, "child already belongs to another meta-case")
        return ValidatedIntent(intent_id=intent.intent_id, validation_status="accepted", validator="PolicyGuard")


def _reject(intent: ActionIntent, reason: str) -> ValidatedIntent:
    return ValidatedIntent(
        intent_id=intent.intent_id,
        validation_status="rejected",
        rejection_reason=reason,
        validator="PolicyGuard",
    )
