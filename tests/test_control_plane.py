import hashlib
import hmac
import json

import pytest

import app.graph_runtime as graph_runtime
from app.incident_memory import IncidentMemory
from app.main import (
    LocalDecisionRequest,
    SignedApprovalRequest,
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
    memory.put_summary("incident-1", {"incident_id": "incident-1", "status": "waiting_approval", "title": "packet loss"})

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
    memory.put_summary("incident-2", {"incident_id": "incident-2", "status": "waiting_approval", "title": "bgp"})
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
