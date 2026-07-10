"""Tier-0 standing grants: scoped auto-approval with envelope enforcement."""

from __future__ import annotations

from typing import Any

import pytest

from app.graph.nodes import NodeRunner, _standing_grant_decision


class FakeMCP:
    def __init__(self, problems: list[dict[str, Any]] | None = None):
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.problems = problems if problems is not None else []

    async def call_tool(self, source: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((name, arguments))
        if name == "icinga_list_problems":
            return {"problems": list(self.problems)}
        return {"ok": True, "tool": name}


def _runner(mcp: FakeMCP) -> NodeRunner:
    runtime = type("Runtime", (), {"mcp_runtime": mcp})()
    return NodeRunner(runtime)


def _ack_state() -> dict[str, Any]:
    return {
        "incident_id": "incident-ack",
        "proposals": [
            {
                "proposed_remediation": ["Acknowledge disk warning on mon"],
                "structured_actions": [
                    {
                        "action_id": "act-1",
                        "type": "acknowledge_icinga",
                        "inputs": {"host": "mon", "service": "disk", "comment": "known filling disk"},
                    }
                ],
            }
        ],
        "executed_actions": [],
    }


def _warning(host: str = "mon", service: str = "disk", state: int = 1) -> dict[str, Any]:
    return {"host": host, "name": f"{host}!{service}", "state": state}


def test_no_grant_without_env(monkeypatch):
    monkeypatch.delenv("NOC_STANDING_GRANT_ACTION_CLASSES", raising=False)
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    assert _standing_grant_decision(_ack_state()) is None


def test_no_grant_when_execution_disabled(monkeypatch):
    monkeypatch.setenv("NOC_STANDING_GRANT_ACTION_CLASSES", "acknowledge_icinga")
    monkeypatch.delenv("NOC_ENABLE_APPROVED_EXECUTION", raising=False)
    assert _standing_grant_decision(_ack_state()) is None


def test_no_grant_for_mixed_or_unsupported_actions(monkeypatch):
    monkeypatch.setenv("NOC_STANDING_GRANT_ACTION_CLASSES", "acknowledge_icinga,restart_service")
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    state = _ack_state()
    state["proposals"][0]["structured_actions"].append(
        {"action_id": "act-2", "type": "restart_service", "inputs": {"host": "mon", "service": "x"}}
    )
    # restart_service is not standing-grant-supported, so the mixed set falls
    # back to human approval even though the operator listed it.
    assert _standing_grant_decision(state) is None


def test_grant_synthesizes_approved_decision(monkeypatch):
    monkeypatch.setenv("NOC_STANDING_GRANT_ACTION_CLASSES", "acknowledge_icinga")
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    decision = _standing_grant_decision(_ack_state())
    assert decision is not None
    assert decision["decision"] == "approved"
    assert decision["operator"] == "standing-grant"


@pytest.mark.asyncio
async def test_approval_interrupt_skips_interrupt_under_grant(monkeypatch):
    monkeypatch.setenv("NOC_STANDING_GRANT_ACTION_CLASSES", "acknowledge_icinga")
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    state = {**_ack_state(), "case_number": "NOC-9", "resource_id": "mon", "thread_id": "t1"}
    # interrupt() raises outside a langgraph run; reaching a returned update at
    # all proves the grant path bypassed it.
    update = await _runner(FakeMCP()).approval_interrupt(state)
    assert update["approval_state"] == "approved"
    assert update["operator_decision"]["operator"] == "standing-grant"


@pytest.mark.asyncio
async def test_grant_execution_enforces_warning_envelope(monkeypatch):
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    monkeypatch.delenv("NOC_ENABLE_NOOP_ROLLBACK_GUARDS", raising=False)
    monkeypatch.setenv("NOC_APPROVAL_SIGNING_SECRET", "secret")
    monkeypatch.setenv("NOC_STANDING_GRANT_ACK_TTL_S", "3600")
    mcp = FakeMCP(problems=[_warning()])
    state = _ack_state()
    state["operator_decision"] = {"decision": "approved", "operator": "standing-grant"}
    update = await _runner(mcp).execute_approved_remediation(state)
    assert update["approval_state"] == "executed"
    ack_calls = [args for name, args in mcp.calls if name == "icinga_acknowledge_alert"]
    assert len(ack_calls) == 1
    assert ack_calls[0]["ack_ttl_seconds"] == 3600
    assert ack_calls[0]["notify"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("problems", "service"),
    [
        ([], "disk"),  # nothing firing
        ([_warning(state=2)], "disk"),  # CRITICAL — never auto-ack
        ([_warning(), _warning(service="disk2")], ""),  # no service given + two candidates: ambiguous
    ],
)
async def test_grant_execution_blocks_outside_envelope(monkeypatch, problems, service):
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    monkeypatch.delenv("NOC_ENABLE_NOOP_ROLLBACK_GUARDS", raising=False)
    monkeypatch.setenv("NOC_APPROVAL_SIGNING_SECRET", "secret")
    mcp = FakeMCP(problems=problems)
    state = _ack_state()
    state["proposals"][0]["structured_actions"][0]["inputs"]["service"] = service
    state["operator_decision"] = {"decision": "approved", "operator": "standing-grant"}
    update = await _runner(mcp).execute_approved_remediation(state)
    assert update["approval_state"] == "execution_failed"
    assert all(name != "icinga_acknowledge_alert" for name, _ in mcp.calls)


@pytest.mark.asyncio
async def test_grant_execution_blocks_unsupported_class(monkeypatch):
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    monkeypatch.delenv("NOC_ENABLE_NOOP_ROLLBACK_GUARDS", raising=False)
    monkeypatch.setenv("NOC_APPROVAL_SIGNING_SECRET", "secret")
    mcp = FakeMCP()
    state = _ack_state()
    state["proposals"][0]["structured_actions"] = [
        {"action_id": "act-1", "type": "restart_service", "inputs": {"host": "mon", "service": "x"}}
    ]
    state["operator_decision"] = {"decision": "approved", "operator": "standing-grant"}
    update = await _runner(mcp).execute_approved_remediation(state)
    assert update["approval_state"] == "execution_failed"
    assert all(name != "os_service_restart" for name, _ in mcp.calls)
