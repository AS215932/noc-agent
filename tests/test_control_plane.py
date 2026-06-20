import hashlib
import hmac
import json

import pytest
from fastapi import HTTPException

from app.cases import CaseIdentityAlias, CaseService, InMemoryCaseStore, ObservationRecord
import app.graph_runtime as graph_runtime
from app.incident_memory import IncidentMemory
import app.main as main_module
from app.main import (
    CommentRequest,
    LocalDecisionRequest,
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
    incident_status,
    pending_incidents,
    signed_resume,
)


def _request(token: str = "secret"):
    return type("Request", (), {"headers": {"x-noc-control-token": token}})()


@pytest.fixture
def isolated_incident_memory():
    """Install a fresh IncidentMemory and restore the process global after the test."""

    original = graph_runtime.INCIDENT_MEMORY
    memory = IncidentMemory(redis_url="")
    graph_runtime.INCIDENT_MEMORY = memory
    try:
        yield memory
    finally:
        graph_runtime.INCIDENT_MEMORY = original


@pytest.mark.asyncio
async def test_local_control_plane_lists_and_decides(monkeypatch, isolated_incident_memory):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    memory = isolated_incident_memory
    await memory.put_summary("incident-1", {"incident_id": "incident-1", "status": "waiting_approval", "title": "packet loss"})

    pending = await pending_incidents("secret")
    shown = await incident_status("incident-1", "secret")
    decided = await decide_incident(
        "incident-1",
        LocalDecisionRequest(decision="approved", operator="svag", comment="looks right"),
        "secret",
    )

    assert pending["incidents"][0]["incident_id"] == "incident-1"
    assert shown["title"] == "packet loss"
    assert decided["incident"]["status"] == "approved"


@pytest.mark.asyncio
async def test_signed_resume_uses_hmac(monkeypatch, isolated_incident_memory):
    monkeypatch.setenv("NOC_APPROVAL_SIGNING_SECRET", "sign-me")
    memory = isolated_incident_memory
    await memory.put_summary("incident-2", {"incident_id": "incident-2", "status": "waiting_approval", "title": "bgp"})
    request = SignedApprovalRequest(
        incident_id="incident-2",
        decision="rejected",
        operator="discord:42",
        comment="needs human eyes",
    )
    body = json.dumps(request.model_dump(), sort_keys=True, separators=(",", ":")).encode()
    signature = hmac.new(b"sign-me", body, hashlib.sha256).hexdigest()

    response = await signed_resume(request, signature)

    assert response["incident"]["status"] == "rejected"


@pytest.mark.asyncio
async def test_control_cases_use_case_number_and_comments(monkeypatch, isolated_incident_memory):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    memory = isolated_incident_memory
    result = await memory.intake_alert(
        {
            "source": "icinga2",
            "status": "firing",
            "groupLabels": {"alertname": "disk", "host": "noc"},
            "alerts": [{"labels": {"alertname": "disk", "host": "noc", "state": "CRITICAL"}}],
        }
    )
    case_number = result.case["case_number"]

    request = type("Request", (), {"headers": {"x-noc-control-token": "secret"}})()
    cases = await control_cases(request)
    detail = await control_case_detail(case_number, request)
    commented = await control_case_comment(case_number, CommentRequest(operator="svag", comment="checking"), request)

    assert cases["cases"][0]["case_number"] == case_number
    assert detail["case"]["incident_id"] == result.case["incident_id"]
    assert commented["case"]["case_number"] == case_number
    assert (await memory.case_events(case_number))[-1]["event_type"] == "operator_comment"


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
async def test_case_service_primary_control_reads_cases_without_legacy_fallback(monkeypatch, isolated_incident_memory):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    memory = isolated_incident_memory
    legacy = await memory.intake_alert(
        {
            "source": "alertmanager",
            "status": "firing",
            "groupLabels": {"alertname": "LegacyOnly"},
            "alerts": [{"labels": {"alertname": "LegacyOnly", "instance": "old"}}],
        }
    )
    assert legacy.case is not None
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
    await memory.put_summary(
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
    assert legacy.case["incident_id"] not in [case["incident_id"] for case in listing["cases"]]
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
async def test_case_service_primary_detail_preserves_graph_approval_summary(monkeypatch, isolated_incident_memory):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    memory = isolated_incident_memory
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="PacketLoss", resource="edge1", status="firing")
    )
    assert created.case is not None
    await memory.put_summary(
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
async def test_case_service_primary_decision_updates_waiting_graph_summary(monkeypatch, isolated_incident_memory):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    memory = isolated_incident_memory
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="PacketLoss", resource="edge1", status="firing")
    )
    assert created.case is not None
    await memory.put_summary(
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
    assert (await memory.get_summary(created.case.case_id))["status"] == "approved"
    assert stored.status == "resolved"
    assert stored.resolution_reason == "operator_approved"
    assert "operator_decision_recorded" in [event.event_type for event in events]


@pytest.mark.asyncio
async def test_case_service_primary_rejected_decision_updates_case_projection(monkeypatch, isolated_incident_memory):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_CONTROL_PRIMARY", "1")
    memory = isolated_incident_memory
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="PacketLoss", resource="edge1", status="firing")
    )
    assert created.case is not None
    await memory.put_summary(
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
async def test_case_service_feedback_mirrors_legacy_operator_comment(monkeypatch, isolated_incident_memory):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    memory = isolated_incident_memory
    legacy = await memory.intake_alert(
        {
            "source": "alertmanager",
            "status": "firing",
            "groupLabels": {"alertname": "InstanceDown"},
            "alerts": [{"labels": {"alertname": "InstanceDown", "instance": "rtr1:9100"}}],
        }
    )
    assert legacy.case is not None
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="InstanceDown", resource="rtr1:9100", status="firing")
    )
    assert created.case is not None
    await store.record_alias(
        CaseIdentityAlias(
            case_id=created.case.case_id,
            alias_type="legacy_case_number",
            alias_value=legacy.case["case_number"],
        )
    )

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)

    await control_case_comment(
        legacy.case["case_number"],
        CommentRequest(operator="svag", comment="checking"),
        _request(),
    )

    feedback = await store.list_feedback(case_id=created.case.case_id)
    assert len(feedback) == 1
    assert feedback[0].feedback_type == "operator_note"
    assert feedback[0].actor_id == "svag"
    assert feedback[0].payload["comment"] == "checking"


@pytest.mark.asyncio
async def test_case_service_feedback_records_acknowledgement_as_note(monkeypatch, isolated_incident_memory):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    memory = isolated_incident_memory
    legacy = await memory.intake_alert(
        {
            "source": "alertmanager",
            "status": "firing",
            "groupLabels": {"alertname": "PacketLoss"},
            "alerts": [{"labels": {"alertname": "PacketLoss", "instance": "edge1"}}],
        }
    )
    assert legacy.case is not None
    incident_id = legacy.case["incident_id"]
    await memory.put_summary(incident_id, {"incident_id": incident_id, "status": "waiting_approval", "title": "packet loss"})
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(
        ObservationRecord(source="alertmanager", detector="PacketLoss", resource="edge1", status="firing")
    )
    assert created.case is not None
    await store.record_alias(
        CaseIdentityAlias(case_id=created.case.case_id, alias_type="legacy_incident_id", alias_value=incident_id)
    )

    class _Runtime:
        pass

    runtime = _Runtime()
    runtime.service = service
    runtime.store = store
    monkeypatch.setattr(main_module, "case_service_runtime", runtime)

    await decide_incident(
        incident_id,
        LocalDecisionRequest(decision="acknowledged", operator="svag", comment="seen"),
        "secret",
    )

    feedback = await store.list_feedback(case_id=created.case.case_id)
    assert len(feedback) == 1
    assert feedback[0].feedback_type == "operator_note"
    assert feedback[0].payload["decision"] == "acknowledged"


@pytest.mark.asyncio
async def test_manual_control_investigation_creates_case(monkeypatch, isolated_incident_memory):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")

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

    assert response["case_number"].startswith("NOC-")
    assert response["incident_id"]
    assert background.tasks
