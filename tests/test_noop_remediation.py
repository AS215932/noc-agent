from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

import pytest

from app.graph.nodes import NodeRunner, _authorization_fingerprint
from app.nocctl import _sign_authorization


class FakeMCP:
    def __init__(self, *, prepare_ok: bool = True, confirm_status: str = "confirmed"):
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.prepare_ok = prepare_ok
        self.confirm_status = confirm_status

    async def call_tool(self, source: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((name, arguments))
        if name == "prepare_commit_confirm":
            if not self.prepare_ok:
                return {"tool": name, "error_type": "policy_blocked", "sanitized_error": "denied"}
            return {"schema_version": 1, "tool": name, "guard": {"guard_id": "rg_test", "status": "pending"}}
        if name == "confirm_change":
            return {"tool": name, "guard": {"guard_id": "rg_test", "status": self.confirm_status}}
        if name == "rollback_change":
            return {"tool": name, "guard": {"guard_id": "rg_test", "status": "rolled_back"}}
        return {"ok": True, "tool": name}


def _runner(mcp: FakeMCP) -> NodeRunner:
    runtime = type("Runtime", (), {"mcp_runtime": mcp})()
    return NodeRunner(runtime)


def _approved_state() -> dict[str, Any]:
    return {
        "incident_id": "incident-1",
        "operator_decision": {"decision": "approved", "operator": "pytest"},
        "proposals": [
            {
                "proposed_remediation": ["Restart node_exporter on cr1.nl1"],
                "structured_actions": [
                    {"action_id": "act-1", "type": "restart_service", "inputs": {"host": "cr1-nl1", "service": "node_exporter"}}
                ],
            }
        ],
        "executed_actions": [],
    }


@pytest.mark.asyncio
async def test_execution_disabled_by_default(monkeypatch):
    monkeypatch.delenv("NOC_ENABLE_APPROVED_EXECUTION", raising=False)
    mcp = FakeMCP()
    update = await _runner(mcp).execute_approved_remediation(_approved_state())
    assert update["approval_state"] == "execution_disabled"
    assert mcp.calls == []


@pytest.mark.asyncio
async def test_noop_guard_prepared_no_real_mutation(monkeypatch):
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    monkeypatch.setenv("NOC_ENABLE_NOOP_ROLLBACK_GUARDS", "1")
    monkeypatch.setenv("NOC_APPROVAL_SIGNING_SECRET", "secret")
    mcp = FakeMCP()
    update = await _runner(mcp).execute_approved_remediation(_approved_state())

    called = [name for name, _ in mcp.calls]
    assert called == ["prepare_commit_confirm"]
    assert "os_service_restart" not in called
    auth = mcp.calls[0][1]["action_authorization"]
    assert auth["action_class"] == "noop_rollback_guard"

    assert update["approval_state"] == "noop_guards_prepared"
    item = update["executed_actions"][0]
    assert item["execution_mode"] == "noop_guard"
    assert item["guard_id"] == "rg_test"
    assert item["authorization_fingerprint"].startswith("sha256:")
    assert item["execution_audit"][0]["event"] == "guard_prepared"
    # the raw HMAC signature is never persisted in state
    assert "signature" not in json.dumps(item)


@pytest.mark.asyncio
async def test_failed_prepare_marks_execution_failed(monkeypatch):
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    monkeypatch.setenv("NOC_ENABLE_NOOP_ROLLBACK_GUARDS", "1")
    monkeypatch.setenv("NOC_APPROVAL_SIGNING_SECRET", "secret")
    mcp = FakeMCP(prepare_ok=False)
    update = await _runner(mcp).execute_approved_remediation(_approved_state())
    assert update["approval_state"] == "execution_failed"
    assert update["executed_actions"][0]["ok"] is False


@pytest.mark.asyncio
async def test_verify_confirms_noop_guard(monkeypatch):
    monkeypatch.setenv("NOC_APPROVAL_SIGNING_SECRET", "secret")
    mcp = FakeMCP(confirm_status="confirmed")
    state = {
        "operator_decision": {"decision": "approved", "operator": "pytest"},
        "incident_id": "incident-1",
        "executed_actions": [
            {"ok": True, "execution_mode": "noop_guard", "guard_id": "rg_test", "action": {"action_id": "act-1", "type": "restart_service", "inputs": {"host": "cr1-nl1"}}}
        ],
        "verification_results": [],
    }
    update = await _runner(mcp).verify_remediation(state)
    assert ("confirm_change", {"guard_id": "rg_test", "action_authorization": mcp.calls[0][1]["action_authorization"]}) in mcp.calls
    assert update["approval_state"] == "verified"
    assert update["verification_results"][0]["event"] == "guard_confirmed"


@pytest.mark.asyncio
async def test_verify_rolls_back_on_confirm_failure(monkeypatch):
    monkeypatch.setenv("NOC_APPROVAL_SIGNING_SECRET", "secret")
    mcp = FakeMCP(confirm_status="pending")  # confirm does not reach 'confirmed'
    state = {
        "operator_decision": {"decision": "approved", "operator": "pytest"},
        "incident_id": "incident-1",
        "executed_actions": [
            {"ok": True, "execution_mode": "noop_guard", "guard_id": "rg_test", "action": {"action_id": "act-1", "type": "restart_service", "inputs": {"host": "cr1-nl1"}}}
        ],
        "verification_results": [],
    }
    update = await _runner(mcp).verify_remediation(state)
    called = [name for name, _ in mcp.calls]
    assert "confirm_change" in called and "rollback_change" in called
    assert update["approval_state"] == "verification_failed"
    assert update["verification_results"][0]["event"] == "guard_rolled_back"


def test_authorization_fingerprint_hides_signature():
    auth = {"signature": "abc123", "operator": "x"}
    fp = _authorization_fingerprint(auth)
    assert fp.startswith("sha256:")
    assert "abc123" not in fp
    assert _authorization_fingerprint({}) == ""


def test_nocctl_sign_matches_hmac_scheme(monkeypatch):
    monkeypatch.setenv("NOC_APPROVAL_SIGNING_SECRET", "secret")
    auth = _sign_authorization(action_id="act-1", case_id="case-9", operator="alice", action_class="noop_rollback_guard", ttl=300)
    signed = {k: auth[k] for k in sorted(auth) if k != "signature"}
    body = json.dumps(signed, sort_keys=True, separators=(",", ":")).encode()
    expected = hmac.new(b"secret", body, hashlib.sha256).hexdigest()
    assert auth["signature"] == expected
    assert auth["action_class"] == "noop_rollback_guard"


def test_nocctl_sign_requires_secret(monkeypatch):
    monkeypatch.delenv("NOC_APPROVAL_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("HYRULE_MCP_ACTION_SIGNING_SECRET", raising=False)
    with pytest.raises(SystemExit):
        _sign_authorization(action_id="a", case_id="c", operator="o", action_class="noop_rollback_guard", ttl=300)
