import hashlib
import hmac
import json
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

from app.cases import (
    CaseEvent,
    CaseHandoff,
    CaseService,
    HandoffUpdate,
    InMemoryCaseStore,
    KnowledgeArtifact,
    ObservationRecord,
    OutcomeRecord,
    VerificationObjective,
)
from app.cases.graph_memory import CaseServiceGraphMemory
import app.main as main_module
from app.main import (
    CommentRequest,
    LocalDecisionRequest,
    LoopConsoleAckRequest,
    LoopConsoleFeedbackRequest,
    LoopConsoleHandoffCancelRequest,
    LoopConsoleKnowledgeArtifactProposalRequest,
    LoopConsoleKnowledgeArtifactReviewRequest,
    LoopConsoleKnowledgeContextRequest,
    LoopConsoleSuppressRequest,
    LoopConsoleVerificationResultRequest,
    ManualInvestigationRequest,
    SignedApprovalRequest,
    control_case_comment,
    control_case_decision,
    control_case_detail,
    control_case_events,
    control_case_service_case_detail,
    control_case_service_cases,
    control_case_service_outbox,
    control_cases,
    control_manual_investigation,
    decide_incident,
    engineering_lhp_handoff_fetch,
    engineering_lhp_handoff_update,
    incident_status,
    loop_console_case_ack,
    loop_console_case_detail,
    loop_console_case_feedback,
    loop_console_case_handoffs,
    loop_console_case_knowledge_artifacts,
    loop_console_case_outcomes,
    loop_console_case_suppress,
    loop_console_case_timeline,
    loop_console_case_verification_objectives,
    loop_console_cases,
    loop_console_health,
    loop_console_handoff_update,
    loop_console_handoff_cancel,
    loop_console_knowledge_artifact_proposal,
    loop_console_knowledge_artifact_review,
    loop_console_knowledge_context_request,
    loop_console_outbox,
    loop_console_verification_result,
    pending_incidents,
    signed_resume,
)
from app.cases.lhp import build_loop_signature


def _request(token: str = "secret"):
    return type("Request", (), {"headers": {"x-noc-control-token": token}})()


class _Url:
    def __init__(self, path: str):
        self.path = path


class _LoopRequest:
    def __init__(
        self,
        *,
        method: str,
        path: str,
        body: dict | None = None,
        secret: str = "shared",
        identity: str = "engineering",
    ):
        self.method = method
        self.url = _Url(path)
        self._body = json.dumps(body or {}, sort_keys=True, separators=(",", ":")).encode()
        timestamp = datetime.now(timezone.utc).isoformat()
        self.headers = {
            "x-noc-loop-identity": identity,
            "x-noc-loop-timestamp": timestamp,
            "x-noc-loop-signature": build_loop_signature(
                secret=secret,
                method=method,
                path=path,
                timestamp=timestamp,
                body=body or {},
            ),
        }

    async def body(self) -> bytes:
        return self._body


def _console_request(method: str, path: str, body: dict | None = None) -> _LoopRequest:
    return _LoopRequest(
        method=method,
        path=path,
        body=body,
        secret="console-shared",
        identity="observatory",
    )


@pytest.mark.asyncio
async def test_control_incidents_require_case_service_routes(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")

    with pytest.raises(HTTPException) as pending_error:
        await pending_incidents("secret")
    with pytest.raises(HTTPException) as status_error:
        await incident_status("incident-1", "secret")
    with pytest.raises(HTTPException) as decision_error:
        await decide_incident(
            "incident-1",
            LocalDecisionRequest(decision="approved", operator="svag", comment="looks right"),
            "secret",
        )

    assert pending_error.value.status_code == 410
    assert status_error.value.status_code == 410
    assert decision_error.value.status_code == 410


@pytest.mark.asyncio
async def test_signed_resume_uses_hmac(monkeypatch):
    monkeypatch.setenv("NOC_APPROVAL_SIGNING_SECRET", "sign-me")
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_PRIMARY", "1")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="BGP", resource="edge1", status="firing")
    )
    assert created.case is not None
    await CaseServiceGraphMemory(store).put_summary(
        created.case.case_id,
        {"incident_id": created.case.case_id, "status": "waiting_approval", "title": "bgp"},
    )

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)
    request = SignedApprovalRequest(
        incident_id=created.case.case_id,
        decision="rejected",
        operator="discord:42",
        comment="needs human eyes",
    )
    body = json.dumps(request.model_dump(), sort_keys=True, separators=(",", ":")).encode()
    signature = hmac.new(b"sign-me", body, hashlib.sha256).hexdigest()

    response = await signed_resume(request, signature)

    assert response["incident"]["status"] == "rejected"


@pytest.mark.asyncio
async def test_engineering_lhp_fetch_and_callback_use_hmac(monkeypatch):
    monkeypatch.setenv("NOC_LHP_ENABLED", "1")
    monkeypatch.setenv("NOC_LHP_ENGINEERING_SECRET", "shared")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="proactive", rule_id="disk_fill", resource="rtr:/", status="firing")
    )
    assert created.case is not None
    handoff = CaseHandoff(
        handoff_id="handoff_disk_1",
        case_id=created.case.case_id,
        target_loop="engineering",
        objective="resolve low root filesystem condition",
        objective_key="resolve-low-root-filesystem-condition-v1",
        idempotency_key="case_1:engineering:resolve-low-root-filesystem-condition-v1:v1",
    )
    await service.request_lhp_handoff(
        handoff,
        objectives=[
            VerificationObjective(
                case_id=created.case.case_id,
                handoff_id=handoff.handoff_id,
                objective_key=f"objective_{index}",
                objective_type="health_endpoint",
                name=f"objective {index}",
            )
            for index in range(25)
        ],
    )
    for index in range(12):
        await service.record_lhp_knowledge_artifact(
            KnowledgeArtifact(
                case_id=created.case.case_id,
                handoff_id=handoff.handoff_id,
                artifact_type="context_pack",
                version=index + 1,
                summary=f"artifact {index}",
            )
        )

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)

    fetched = await engineering_lhp_handoff_fetch(
        handoff.handoff_id,
        _LoopRequest(method="GET", path=f"/loop-handoff/v1/engineering/handoffs/{handoff.handoff_id}"),
    )
    assert fetched["handoff"]["handoff_id"] == handoff.handoff_id
    assert fetched["schema_version"] == "lhp.v1"
    assert fetched["payload_hash"]
    assert fetched["approval_scope_hash"]
    assert fetched["approval_scope"]["handoff"]["handoff_id"] == handoff.handoff_id
    assert "status" not in fetched["approval_scope"]["handoff"]
    assert len(fetched["verification_objectives"]) == 20
    assert len(fetched["approval_scope"]["verification_objectives"]) == 25
    assert len(fetched["knowledge_artifacts"]) == 10

    mutable_objective = (await service.list_lhp_verification_objectives(case_id=created.case.case_id))[0]
    mutable_objective.status = "unknown"
    mutable_objective.last_checked_at = "2026-07-21T19:30:00+00:00"
    mutable_objective.next_check_at = "2026-07-21T19:32:00+00:00"
    await service.record_lhp_verification_result(mutable_objective)
    refreshed = await engineering_lhp_handoff_fetch(
        handoff.handoff_id,
        _LoopRequest(method="GET", path=f"/loop-handoff/v1/engineering/handoffs/{handoff.handoff_id}"),
    )
    assert refreshed["payload_hash"] != fetched["payload_hash"]
    assert refreshed["approval_scope_hash"] == fetched["approval_scope_hash"]

    contract_objective = next(
        item
        for item in await service.list_lhp_verification_objectives(case_id=created.case.case_id)
        if item.objective_key == "objective_24"
    )
    contract_objective.payload = {"endpoint": "/health/changed"}
    await service.record_lhp_verification_result(contract_objective)
    contract_changed = await engineering_lhp_handoff_fetch(
        handoff.handoff_id,
        _LoopRequest(method="GET", path=f"/loop-handoff/v1/engineering/handoffs/{handoff.handoff_id}"),
    )
    assert contract_changed["approval_scope_hash"] != fetched["approval_scope_hash"]

    current_case = await store.get_case(created.case.case_id)
    assert current_case is not None
    current_case.resolved_at = "2026-07-21T19:35:00+00:00"
    current_case.status = "resolved"
    await store.upsert_case(current_case)
    resolved = await engineering_lhp_handoff_fetch(
        handoff.handoff_id,
        _LoopRequest(method="GET", path=f"/loop-handoff/v1/engineering/handoffs/{handoff.handoff_id}"),
    )
    assert resolved["approval_scope_hash"] == contract_changed["approval_scope_hash"]

    callback_body = HandoffUpdate(
        case_id=created.case.case_id,
        handoff_id=handoff.handoff_id,
        source_loop="engineering",
        update_type="accepted",
        status="accepted",
        external_event_id="eng_evt_1",
        correlation_id=handoff.correlation_id,
    ).model_dump(mode="json")
    response = await engineering_lhp_handoff_update(
        _LoopRequest(method="POST", path="/webhook/engineering-loop/handoff-update", body=callback_body)
    )
    duplicate = await engineering_lhp_handoff_update(
        _LoopRequest(method="POST", path="/webhook/engineering-loop/handoff-update", body=callback_body)
    )

    assert response["status"] == "accepted"
    assert response["created"] is True
    assert duplicate["status"] == "accepted"
    stored = await service.get_lhp_handoff(handoff.handoff_id)
    assert stored is not None and stored.status == "accepted"

    bad_body = HandoffUpdate(
        case_id=created.case.case_id,
        handoff_id=handoff.handoff_id,
        source_loop="engineering",
        update_type="implemented",
        status="blocked",
        external_event_id="eng_evt_bad_1",
        correlation_id=handoff.correlation_id,
    ).model_dump(mode="json")
    bad_body["status"] = "verified"
    with pytest.raises(HTTPException) as bad_exc:
        await engineering_lhp_handoff_update(
            _LoopRequest(method="POST", path="/webhook/engineering-loop/handoff-update", body=bad_body)
        )
    assert bad_exc.value.status_code == 422


@pytest.mark.asyncio
async def test_loop_console_can_cancel_handoff_with_idempotent_signed_action(monkeypatch):
    monkeypatch.setenv("NOC_LOOP_CONSOLE_SECRET", "console-shared")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="proactive", rule_id="disk_fill", resource="rtr:/", status="firing")
    )
    assert created.case is not None
    handoff = CaseHandoff(
        handoff_id="handoff_cancel_console",
        case_id=created.case.case_id,
        target_loop="engineering",
        objective="resolve disk condition",
        objective_key="resolve-disk-v1",
        idempotency_key="console:cancel:handoff",
    )
    await service.request_lhp_handoff(handoff)

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)
    request_body = LoopConsoleHandoffCancelRequest(
        actor_id="operator",
        idempotency_key="cancel-1",
        reason="monitor-only handoff",
    )
    body = request_body.model_dump(mode="json")
    path = f"/loop-console/v1/handoffs/{handoff.handoff_id}/cancel"

    first = await loop_console_handoff_cancel(
        handoff.handoff_id,
        request_body,
        _console_request("POST", path, body),
    )
    duplicate = await loop_console_handoff_cancel(
        handoff.handoff_id,
        request_body,
        _console_request("POST", path, body),
    )

    assert first["created"] is True
    assert duplicate["created"] is False
    assert first["handoff"]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_loop_console_v1_requires_hmac(monkeypatch):
    monkeypatch.delenv("NOC_LOOP_CONSOLE_SECRET", raising=False)
    with pytest.raises(HTTPException) as missing:
        await loop_console_health(_console_request("GET", "/loop-console/v1/health"))
    assert missing.value.status_code == 503

    monkeypatch.setenv("NOC_LOOP_CONSOLE_SECRET", "console-shared")
    with pytest.raises(HTTPException) as wrong_identity:
        await loop_console_health(
            _LoopRequest(
                method="GET",
                path="/loop-console/v1/health",
                secret="console-shared",
                identity="engineering",
            )
        )
    assert wrong_identity.value.status_code == 401


@pytest.mark.asyncio
async def test_loop_console_v1_reads_and_writes_case_service_state(monkeypatch):
    monkeypatch.setenv("NOC_LOOP_CONSOLE_SECRET", "console-shared")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="proactive", rule_id="disk_fill", resource="rtr:/", status="firing")
    )
    assert created.case is not None
    case = created.case
    handoff = CaseHandoff(
        handoff_id="handoff_console_1",
        case_id=case.case_id,
        target_loop="engineering",
        objective="resolve disk condition",
        objective_key="resolve-disk-v1",
        idempotency_key="console:handoff:1",
        resource={"host": "rtr", "mount": "/"},
        constraints=["preserve customer traffic"],
        acceptance_criteria=["disk free space stays above threshold"],
        payload={"raw_evidence": {"disk": {"free_percent": 19.9}}},
    )
    objective = VerificationObjective(
        objective_id="objective_console_1",
        case_id=case.case_id,
        handoff_id=handoff.handoff_id,
        objective_key="disk-clear",
        objective_type="monitoring_alert_clear",
        name="disk alert clears",
        payload={"raw_probe": "icinga payload"},
    )
    await service.request_lhp_handoff(handoff, objectives=[objective])
    artifact = await service.record_lhp_knowledge_artifact(
        KnowledgeArtifact(
            artifact_id="artifact_console_1",
            case_id=case.case_id,
            handoff_id=handoff.handoff_id,
            artifact_type="runbook_delta",
            summary="candidate runbook update",
            payload={"document": "raw candidate body"},
        )
    )
    await service.record_lhp_outcome(
        OutcomeRecord(
            outcome_id="outcome_console_1",
            work_item_id=case.case_id,
            proposed_action="review",
            payload={"transcript": "raw final evidence"},
        )
    )

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)

    assert (await loop_console_health(_console_request("GET", "/loop-console/v1/health")))["status"] == "ok"
    cases = await loop_console_cases(
        _console_request("GET", "/loop-console/v1/cases"), kind=None, status_filter=None, limit=100
    )
    assert cases["cases"][0]["case_id"] == case.case_id
    assert "+" in cases["cases"][0]["opened_at"]
    case.summary = f"{'large monitoring annotation ' * 80}tail-marker"
    case.status = "resolved"
    case.updated_at = "2026-01-01T00:00:00+00:00"
    await store.upsert_case(case)
    await service.observe(
        ObservationRecord(source="proactive", rule_id="memory_fill", resource="rtr:/var", status="firing")
    )
    for index in range(220):
        await store.append_event(
            CaseEvent(
                case_id=case.case_id,
                event_type=f"noise_{index}",
                occurred_at=f"2026-12-31T00:{index // 60:02d}:{index % 60:02d}+00:00",
                payload={"summary": f"noise {index}", "raw_evidence": {"index": index}},
            )
        )
    resolved_cases = await loop_console_cases(
        _console_request("GET", "/loop-console/v1/cases?status=resolved&limit=1"),
        kind=None,
        status_filter="resolved",
        limit=1,
    )
    assert [item["case_id"] for item in resolved_cases["cases"]] == [case.case_id]
    detail = await loop_console_case_detail(
        case.case_id, _console_request("GET", f"/loop-console/v1/cases/{case.case_id}")
    )
    assert detail["case"]["case_id"] == case.case_id
    assert detail["case"]["opened_at"]
    assert detail["case"]["resolved_at"] == ""
    assert detail["summary"]["updated_at"] == "2026-01-01T00:00:00+00:00"
    assert "+" in detail["case"]["opened_at"]
    assert "+" in detail["timeline"][0]["received_at"]
    assert len(detail["case"]["summary"]) <= 1200
    assert len(detail["summary"]["summary"]) <= 1200
    assert "tail-marker" not in detail["summary"]["summary"]
    assert "signal_snapshot" not in detail["case"]
    assert "last_diagnosis" not in detail["case"]
    assert detail["counts"]["timeline"] > detail["timeline_limit"]
    assert len(detail["timeline"]) == detail["timeline_limit"]
    assert "noise_219" in [item["event_type"] for item in detail["timeline"]]
    assert "noise_0" not in [item["event_type"] for item in detail["timeline"]]
    assert all("payload" not in item for item in detail["timeline"])
    assert detail["handoffs"][0]["handoff_id"] == handoff.handoff_id
    assert detail["handoffs"][0]["acceptance_criteria"] == ["disk free space stays above threshold"]
    assert "+" in detail["handoffs"][0]["updated_at"]
    assert "payload" not in detail["handoffs"][0]
    assert "payload" not in detail["verification_objectives"][0]
    assert "payload" not in detail["knowledge_artifacts"][0]
    assert "payload" not in detail["outcomes"][0]
    timeline_response = await loop_console_case_timeline(
        case.case_id,
        _console_request("GET", f"/loop-console/v1/cases/{case.case_id}/timeline"),
    )
    assert timeline_response["timeline"]
    assert all("payload" not in item for item in timeline_response["timeline"])
    handoffs_response = await loop_console_case_handoffs(
        case.case_id,
        _console_request("GET", f"/loop-console/v1/cases/{case.case_id}/handoffs"),
    )
    assert handoffs_response["handoffs"]
    assert "payload" not in handoffs_response["handoffs"][0]
    objectives_response = await loop_console_case_verification_objectives(
        case.case_id,
        _console_request("GET", f"/loop-console/v1/cases/{case.case_id}/verification-objectives"),
    )
    assert objectives_response["verification_objectives"]
    assert "payload" not in objectives_response["verification_objectives"][0]
    artifacts_response = await loop_console_case_knowledge_artifacts(
        case.case_id,
        _console_request("GET", f"/loop-console/v1/cases/{case.case_id}/knowledge-artifacts"),
    )
    assert artifacts_response["knowledge_artifacts"]
    assert "payload" not in artifacts_response["knowledge_artifacts"][0]
    outcomes_response = await loop_console_case_outcomes(
        case.case_id,
        _console_request("GET", f"/loop-console/v1/cases/{case.case_id}/outcomes"),
    )
    assert outcomes_response["outcomes"]
    assert "payload" not in outcomes_response["outcomes"][0]

    feedback_request = LoopConsoleFeedbackRequest(
        actor_id="operator",
        idempotency_key="feedback-1",
        feedback_type="operator_note",
        comment="looks useful",
    )
    feedback_body = feedback_request.model_dump(mode="json")
    await loop_console_case_feedback(
        case.case_id,
        feedback_request,
        _console_request("POST", f"/loop-console/v1/cases/{case.case_id}/feedback", feedback_body),
    )
    await loop_console_case_feedback(
        case.case_id,
        feedback_request,
        _console_request("POST", f"/loop-console/v1/cases/{case.case_id}/feedback", feedback_body),
    )
    event_types = [event.event_type for event in await store.case_events(case.case_id)]
    assert event_types.count("operator_feedback_recorded") == 1

    ack_request = LoopConsoleAckRequest(actor_id="operator", idempotency_key="ack-1")
    ack_body = ack_request.model_dump(mode="json")
    await loop_console_case_ack(
        case.case_id,
        ack_request,
        _console_request("POST", f"/loop-console/v1/cases/{case.case_id}/ack", ack_body),
    )
    await loop_console_case_ack(
        case.case_id,
        ack_request,
        _console_request("POST", f"/loop-console/v1/cases/{case.case_id}/ack", ack_body),
    )
    event_types = [event.event_type for event in await store.case_events(case.case_id)]
    assert event_types.count("case_acknowledged") == 1

    suppress_request = LoopConsoleSuppressRequest(
        actor_id="operator",
        idempotency_key="suppress-1",
        reason="maintenance window",
        ttl_seconds=60,
    )
    suppress_body = suppress_request.model_dump(mode="json")
    await loop_console_case_suppress(
        case.case_id,
        suppress_request,
        _console_request("POST", f"/loop-console/v1/cases/{case.case_id}/suppress", suppress_body),
    )
    assert (await store.get_case(case.case_id)).suppression_reason == "maintenance window"

    context_request = LoopConsoleKnowledgeContextRequest(
        actor_id="operator", idempotency_key="ctx-1", handoff_id=handoff.handoff_id
    )
    context_body = context_request.model_dump(mode="json")
    context = await loop_console_knowledge_context_request(
        case.case_id,
        context_request,
        _console_request("POST", f"/loop-console/v1/cases/{case.case_id}/knowledge-context-requests", context_body),
    )
    assert context["outbox"]["intent_type"] == "knowledge_context_requested"
    proposal_request = LoopConsoleKnowledgeArtifactProposalRequest(
        actor_id="operator", idempotency_key="ka-1", handoff_id=handoff.handoff_id
    )
    proposal_body = proposal_request.model_dump(mode="json")
    proposal = await loop_console_knowledge_artifact_proposal(
        case.case_id,
        proposal_request,
        _console_request("POST", f"/loop-console/v1/cases/{case.case_id}/knowledge-artifact-proposals", proposal_body),
    )
    assert proposal["outbox"]["intent_type"] == "knowledge_artifact_proposed"
    outbox = await loop_console_outbox(_console_request("GET", "/loop-console/v1/outbox"), outbox_status=None)
    assert len(outbox["outbox"]) == 2

    handoff_update_body = {
        "case_id": case.case_id,
        "handoff_id": handoff.handoff_id,
        "source_loop": "engineering",
        "update_type": "accepted",
        "status": "accepted",
        "external_event_id": "console_eng_evt_1",
        "correlation_id": handoff.correlation_id,
    }
    handoff_update = await loop_console_handoff_update(
        handoff.handoff_id,
        _console_request(
            "POST",
            f"/loop-console/v1/handoffs/{handoff.handoff_id}/updates",
            handoff_update_body,
        ),
    )
    assert handoff_update["handoff"]["status"] == "accepted"

    review_request = LoopConsoleKnowledgeArtifactReviewRequest(
        actor_id="operator",
        idempotency_key="review-1",
        review_status="approved",
        comment="ship it",
    )
    review_body = review_request.model_dump(mode="json")
    reviewed = await loop_console_knowledge_artifact_review(
        artifact.artifact_id,
        review_request,
        _console_request("POST", f"/loop-console/v1/knowledge-artifacts/{artifact.artifact_id}/review", review_body),
    )
    assert reviewed["artifact"]["review_status"] == "approved"
    assert reviewed["artifact"]["payload"]["review"]["untrusted_operator_text"] is True
    assert reviewed["artifact"]["payload"]["review"]["model_consumption_allowed"] is False

    verification_request = LoopConsoleVerificationResultRequest(
        actor_id="operator",
        idempotency_key="verify-1",
        status="pass",
        evidence_ref="icinga:disk-clear",
    )
    verification_body = verification_request.model_dump(mode="json")
    verified = await loop_console_verification_result(
        objective.objective_id,
        verification_request,
        _console_request(
            "POST", f"/loop-console/v1/verification-objectives/{objective.objective_id}/result", verification_body
        ),
    )
    assert verified["verification_objective"]["status"] == "pending"
    assert verified["verification_objective"]["consecutive_pass_count"] == 1

    event_types = [event.event_type for event in await store.case_events(case.case_id)]
    assert "lhp_knowledge_artifact_reviewed" in event_types
    assert "lhp_verification_objective_updated" in event_types


@pytest.mark.asyncio
async def test_engineering_lhp_fetch_rejects_bad_signature(monkeypatch):
    monkeypatch.setenv("NOC_LHP_ENABLED", "1")
    monkeypatch.setenv("NOC_LHP_ENGINEERING_SECRET", "shared")

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = CaseService(InMemoryCaseStore())
    runtime.store = runtime.service.store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)
    request = _LoopRequest(method="GET", path="/loop-handoff/v1/engineering/handoffs/handoff_1", secret="wrong")

    with pytest.raises(HTTPException) as exc:
        await engineering_lhp_handoff_fetch("handoff_1", request)

    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_control_cases_require_case_service_routes(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    request = type("Request", (), {"headers": {"x-noc-control-token": "secret"}})()

    with pytest.raises(HTTPException) as cases_error:
        await control_cases(request)
    with pytest.raises(HTTPException) as detail_error:
        await control_case_detail("NOC-legacy", request)
    with pytest.raises(HTTPException) as comment_error:
        await control_case_comment("NOC-legacy", CommentRequest(operator="svag", comment="checking"), request)

    assert cases_error.value.status_code == 410
    assert detail_error.value.status_code == 410
    assert comment_error.value.status_code == 410


@pytest.mark.asyncio
async def test_control_cases_require_case_service_primary_routes(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.delenv("NOC_CASESERVICE_CONTROL_PRIMARY", raising=False)
    monkeypatch.delenv("NOC_CASESERVICE_REACTIVE_PRIMARY", raising=False)
    monkeypatch.delenv("NOC_PROACTIVE_ENABLED", raising=False)

    with pytest.raises(HTTPException) as exc:
        await control_cases(_request())

    assert exc.value.status_code == 410
    assert "legacy control paths have been removed" in exc.value.detail


@pytest.mark.asyncio
async def test_case_service_control_lists_detail_and_outbox(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(
            source="alertmanager",
            detector="InstanceDown",
            resource="rtr1.as215932.net:9100",
            status="firing",
            severity="HIGH",
            signal_snapshot={"summary": "rtr1 exporter down"},
        )
    )
    assert created.case is not None
    await service.request_report(created.case)

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)
    request = _request()

    listing = await control_case_service_cases(request, kind=None, limit=100)
    detail = await control_case_service_case_detail(created.case.case_id, request)
    outbox = await control_case_service_outbox(request, outbox_status="pending")

    assert listing["status"] == "ok"
    assert listing["cases"][0]["case_id"] == created.case.case_id
    assert listing["cases"][0]["severity"] == "HIGH"
    assert detail["case"]["case_id"] == created.case.case_id
    assert [event["event_type"] for event in detail["events"]] == ["case_created", "case_observed_unhealthy"]
    assert outbox["outbox"][0]["intent_type"] == "report"
    assert outbox["outbox"][0]["case_id"] == created.case.case_id


@pytest.mark.asyncio
async def test_proactive_enabled_exposes_case_service_control_routes(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_PROACTIVE_ENABLED", "1")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="proactive", detector="disk_fill", resource="log:/var", status="firing")
    )
    assert created.case is not None

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)

    listing = await control_cases(_request())

    assert listing["source"] == "case_service"
    assert listing["cases"][0]["incident_id"] == created.case.case_id


@pytest.mark.asyncio
async def test_case_service_control_disabled_and_missing(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setattr(main_module, "case_service_runtime", None)
    request = _request()

    listing = await control_case_service_cases(request, kind=None, limit=100)
    assert listing == {"status": "disabled", "enabled": False, "cases": []}
    with pytest.raises(HTTPException) as exc:
        await control_case_service_case_detail("case_missing", request)
    assert exc.value.status_code == 409


@pytest.mark.asyncio
async def test_case_service_control_validates_filters_and_ids(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    store = InMemoryCaseStore()
    service = CaseService(store)

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)
    request = _request()

    with pytest.raises(HTTPException) as bad_kind:
        await control_case_service_cases(request, kind="atomic;drop", limit=100)
    with pytest.raises(HTTPException) as bad_case_id:
        await control_case_service_case_detail("case_1/../../secret", request)
    with pytest.raises(HTTPException) as bad_status:
        await control_case_service_outbox(request, outbox_status="pending;drop")

    assert bad_kind.value.status_code == 422
    assert bad_case_id.value.status_code == 422
    assert bad_status.value.status_code == 422


@pytest.mark.asyncio
async def test_case_service_primary_control_reads_only_case_service(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="InstanceDown", resource="rtr1:9100", status="firing")
    )
    assert created.case is not None
    await service.record_investigation_result(
        created.case.case_id,
        diagnosis={"summary": "node exporter down", "confidence_score": 0.8, "requires_human": True},
        recommendations=["check node_exporter"],
    )
    await CaseServiceGraphMemory(store).put_summary(
        created.case.case_id,
        {"incident_id": created.case.case_id, "status": "waiting_approval", "title": "node exporter down"},
    )

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)
    request = _request()

    listing = await control_cases(request)
    detail = await control_case_detail(created.case.case_id, request)
    events = await control_case_events(created.case.case_id, request)

    assert listing["source"] == "case_service"
    assert [case["incident_id"] for case in listing["cases"]] == [created.case.case_id]
    assert listing["cases"][0]["pending_approval"] is True
    assert listing["cases"][0]["status"] == "waiting_approval"
    assert detail["source"] == "case_service"
    assert detail["case"]["incident_id"] == created.case.case_id
    assert detail["summary"]["title"] == "node exporter down"
    assert events["events"][0]["event_type"] == "case_created"


@pytest.mark.asyncio
async def test_case_service_primary_control_routes_listed_case_numbers(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="InterfaceDown", resource="xe-0/0/0", status="firing")
    )
    assert created.case is not None
    created.case.case_number = "NOC-20260620-999"
    await store.upsert_case(created.case)

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)
    request = _request()

    listing = await control_cases(request)
    detail = await control_case_detail(listing["cases"][0]["case_number"], request)

    assert listing["cases"][0]["case_number"] == "NOC-20260620-999"
    assert detail["case"]["case_id"] == created.case.case_id


@pytest.mark.asyncio
async def test_case_service_primary_control_comments_and_acknowledges(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="Latency", resource="edge1", status="firing")
    )
    assert created.case is not None

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)
    request = _request()

    commented = await control_case_comment(
        created.case.case_id,
        CommentRequest(operator="svag", comment="checking"),
        request,
    )
    decided = await control_case_decision(
        created.case.case_id,
        LocalDecisionRequest(decision="acknowledged", operator="svag", comment="seen"),
        request,
    )

    feedback = await store.list_feedback(case_id=created.case.case_id)
    event_types = [event.event_type for event in await store.case_events(created.case.case_id)]
    detail = await control_case_detail(created.case.case_id, request)
    event_response = await control_case_events(created.case.case_id, request)
    assert commented["case"]["incident_id"] == created.case.case_id
    assert decided["incident"]["incident_id"] == created.case.case_id
    assert [item.feedback_type for item in feedback] == ["operator_note", "operator_note"]
    assert feedback[0].payload["untrusted_operator_text"] is True
    assert feedback[0].payload["model_consumption_allowed"] is False
    assert "case_acknowledged" in event_types
    assert "checking" in [event["summary"] for event in detail["events"]]
    assert "checking" in [event["summary"] for event in event_response["events"]]


@pytest.mark.asyncio
async def test_case_service_primary_detail_preserves_graph_approval_summary(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="PacketLoss", resource="edge1", status="firing")
    )
    assert created.case is not None
    await CaseServiceGraphMemory(store).put_summary(
        created.case.case_id,
        {
            "incident_id": created.case.case_id,
            "status": "waiting_approval",
            "title": "packet loss needs approval",
            "proposals": [{"type": "restart_service", "inputs": {"service": "bird"}}],
            "executed_actions": [{"ok": True}],
            "verification_results": [{"check": "bgp", "ok": True}],
        },
    )

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)

    detail = await control_case_detail(created.case.case_id, _request())

    assert detail["summary"]["title"] == "packet loss needs approval"
    assert detail["summary"]["proposals"] == [{"type": "restart_service", "inputs": {"service": "bird"}}]
    assert detail["summary"]["executed_actions"] == [{"ok": True}]
    assert detail["summary"]["verification_results"] == [{"check": "bgp", "ok": True}]


@pytest.mark.asyncio
async def test_case_service_primary_decision_updates_waiting_graph_summary(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="PacketLoss", resource="edge1", status="firing")
    )
    assert created.case is not None
    graph_memory = CaseServiceGraphMemory(store)
    await graph_memory.put_summary(
        created.case.case_id,
        {"incident_id": created.case.case_id, "status": "waiting_approval", "title": "packet loss"},
    )

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)

    response = await control_case_decision(
        created.case.case_id,
        LocalDecisionRequest(decision="approved", operator="svag", comment="ship it"),
        _request(),
    )

    stored = await store.get_case(created.case.case_id)
    events = await store.case_events(created.case.case_id)
    assert response["incident"]["status"] == "approved"
    assert (await graph_memory.get_summary(created.case.case_id))["status"] == "approved"
    assert stored.status == "resolved"
    assert stored.resolution_reason == "operator_approved"
    assert "operator_decision_recorded" in [event.event_type for event in events]


@pytest.mark.asyncio
async def test_case_service_primary_rejected_decision_updates_case_projection(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="PacketLoss", resource="edge1", status="firing")
    )
    assert created.case is not None
    await CaseServiceGraphMemory(store).put_summary(
        created.case.case_id,
        {"incident_id": created.case.case_id, "status": "waiting_approval", "title": "packet loss"},
    )

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)

    response = await control_case_decision(
        created.case.case_id,
        LocalDecisionRequest(decision="rejected", operator="svag", comment="nope"),
        _request(),
    )

    stored = await store.get_case(created.case.case_id)
    listing = await control_cases(_request())
    assert response["incident"]["status"] == "rejected"
    assert stored.status == "resolved"
    assert stored.resolution_reason == "operator_rejected"
    assert listing["cases"][0]["status"] == "rejected"
    assert listing["cases"][0]["pending_approval"] is False


@pytest.mark.asyncio
async def test_reactive_primary_approval_reads_route_to_case_service(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_PRIMARY", "1")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="PacketLoss", resource="edge1", status="firing")
    )
    assert created.case is not None
    await CaseServiceGraphMemory(store).put_summary(
        created.case.case_id,
        {"incident_id": created.case.case_id, "status": "waiting_approval", "title": "case service approval"},
    )

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)

    pending = await pending_incidents("secret")
    shown = await incident_status(created.case.case_id, "secret")
    listing = await control_cases(_request())
    detail = await control_case_detail(created.case.case_id, _request())
    events = await control_case_events(created.case.case_id, _request())
    commented = await control_case_comment(
        created.case.case_id,
        CommentRequest(operator="svag", comment="checking"),
        _request(),
    )
    decided = await control_case_decision(
        created.case.case_id,
        LocalDecisionRequest(decision="rejected", operator="svag", comment="nope"),
        _request(),
    )

    stored = await store.get_case(created.case.case_id)
    assert pending["incidents"][0]["title"] == "case service approval"
    assert shown["title"] == "case service approval"
    assert listing["source"] == "case_service"
    assert [case["incident_id"] for case in listing["cases"]] == [created.case.case_id]
    assert detail["source"] == "case_service"
    assert events["source"] == "case_service"
    assert commented["case"]["incident_id"] == created.case.case_id
    assert decided["incident"]["status"] == "rejected"
    assert stored.status == "resolved"


@pytest.mark.asyncio
async def test_case_service_primary_comment_surfaces_feedback_write_failure(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")

    class FailingFeedbackStore(InMemoryCaseStore):
        async def record_feedback(self, feedback):
            raise RuntimeError("feedback store unavailable")

    store = FailingFeedbackStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="Latency", resource="edge1", status="firing")
    )
    assert created.case is not None

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)

    with pytest.raises(RuntimeError, match="feedback store unavailable"):
        await control_case_comment(
            created.case.case_id,
            CommentRequest(operator="svag", comment="checking"),
            _request(),
        )


@pytest.mark.asyncio
async def test_case_service_feedback_records_operator_comment(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_PRIMARY", "1")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="InstanceDown", resource="rtr1:9100", status="firing")
    )
    assert created.case is not None

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)

    await control_case_comment(
        created.case.case_id,
        CommentRequest(operator="svag", comment="checking"),
        _request(),
    )

    feedback = await store.list_feedback(case_id=created.case.case_id)
    assert len(feedback) == 1
    assert feedback[0].feedback_type == "operator_note"
    assert feedback[0].actor_id == "svag"
    assert feedback[0].payload["comment"] == "checking"


@pytest.mark.asyncio
async def test_case_service_feedback_records_acknowledgement_as_note(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_PRIMARY", "1")
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="PacketLoss", resource="edge1", status="firing")
    )
    assert created.case is not None
    await CaseServiceGraphMemory(store).put_summary(
        created.case.case_id,
        {"incident_id": created.case.case_id, "status": "waiting_approval", "title": "packet loss"},
    )

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)

    await decide_incident(
        created.case.case_id,
        LocalDecisionRequest(decision="acknowledged", operator="svag", comment="seen"),
        "secret",
    )

    feedback = await store.list_feedback(case_id=created.case.case_id)
    assert len(feedback) == 1
    assert feedback[0].feedback_type == "operator_note"
    assert feedback[0].payload["decision"] == "acknowledged"


@pytest.mark.asyncio
async def test_manual_control_investigation_creates_case(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    store = InMemoryCaseStore()
    service = CaseService(store)

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)

    class Background:
        def __init__(self):
            self.tasks = []

        def add_task(self, fn, *args, **kwargs):
            self.tasks.append((fn, args, kwargs))

    request = type("Request", (), {"headers": {"x-noc-control-token": "secret"}})()
    background = Background()
    response = await control_manual_investigation(
        ManualInvestigationRequest(prompt="check noc", operator="svag"),
        background,
        request,
    )

    assert response["source"] == "case_service"
    assert response["incident_id"]
    assert background.tasks
