import hashlib
import hmac
import json

import pytest
from fastapi import HTTPException

from app.cases import CaseService, InMemoryCaseStore, ObservationRecord
from app.cases.graph_memory import CaseServiceGraphMemory
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
    """Provide a fresh legacy IncidentMemory for no-fallback assertions."""

    yield IncidentMemory(redis_url="")


@pytest.mark.asyncio
async def test_control_incidents_require_case_service_routes(monkeypatch, isolated_incident_memory):
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
async def test_signed_resume_uses_hmac(monkeypatch, isolated_incident_memory):
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
async def test_control_cases_require_case_service_routes(monkeypatch, isolated_incident_memory):
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
    assert "legacy IncidentMemory control paths have been removed" in exc.value.detail


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
        {"incident_id": created.case.case_id, "status": "waiting_approval", "title": "legacy should not appear"},
    )
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
async def test_case_service_primary_decision_updates_waiting_graph_summary(monkeypatch, isolated_incident_memory):
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
async def test_case_service_primary_rejected_decision_updates_case_projection(monkeypatch, isolated_incident_memory):
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
async def test_reactive_primary_routes_legacy_approval_reads_to_case_service(monkeypatch, isolated_incident_memory):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_CASESERVICE_REACTIVE_PRIMARY", "1")
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
        ObservationRecord(source="alertmanager", detector="PacketLoss", resource="edge1", status="firing")
    )
    assert created.case is not None
    await memory.put_summary(
        created.case.case_id,
        {"incident_id": created.case.case_id, "status": "waiting_approval", "title": "legacy should not appear"},
    )
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
    assert legacy.case["incident_id"] not in [case["incident_id"] for case in listing["cases"]]
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
async def test_case_service_feedback_records_operator_comment(monkeypatch, isolated_incident_memory):
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
async def test_case_service_feedback_records_acknowledgement_as_note(monkeypatch, isolated_incident_memory):
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
async def test_manual_control_investigation_creates_case(monkeypatch, isolated_incident_memory):
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
