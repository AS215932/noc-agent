import hashlib
import hmac
import json

import pytest

import app.graph_runtime as graph_runtime
from app.incident_memory import IncidentMemory
from app.main import (
    CommentRequest,
    LocalDecisionRequest,
    ManualInvestigationRequest,
    SignedApprovalRequest,
    control_case_comment,
    control_case_detail,
    control_cases,
    control_manual_investigation,
    decide_incident,
    incident_status,
    pending_incidents,
    signed_resume,
)


@pytest.mark.asyncio
async def test_local_control_plane_lists_and_decides(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    memory = IncidentMemory(redis_url="")
    monkeypatch.setattr(graph_runtime, "INCIDENT_MEMORY", memory)
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
async def test_signed_resume_uses_hmac(monkeypatch):
    monkeypatch.setenv("NOC_APPROVAL_SIGNING_SECRET", "sign-me")
    memory = IncidentMemory(redis_url="")
    monkeypatch.setattr(graph_runtime, "INCIDENT_MEMORY", memory)
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
async def test_control_cases_use_case_number_and_comments(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    memory = IncidentMemory(redis_url="")
    monkeypatch.setattr(graph_runtime, "INCIDENT_MEMORY", memory)
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
async def test_manual_control_investigation_creates_case(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    memory = IncidentMemory(redis_url="")
    monkeypatch.setattr(graph_runtime, "INCIDENT_MEMORY", memory)

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
