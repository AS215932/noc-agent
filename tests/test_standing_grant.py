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
async def test_approval_interrupt_skips_interrupt_when_envelope_holds(monkeypatch):
    monkeypatch.setenv("NOC_STANDING_GRANT_ACTION_CLASSES", "acknowledge_icinga")
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    state = {**_ack_state(), "case_number": "NOC-9", "resource_id": "mon", "thread_id": "t1"}
    # interrupt() raises outside a langgraph run; reaching a returned update at
    # all proves the grant path bypassed it.
    update = await _runner(FakeMCP(problems=[_warning()])).approval_interrupt(state)
    assert update["approval_state"] == "approved"
    assert update["operator_decision"]["operator"] == "standing-grant"


@pytest.mark.asyncio
async def test_approval_interrupt_falls_through_when_envelope_blocked(monkeypatch):
    monkeypatch.setenv("NOC_STANDING_GRANT_ACTION_CLASSES", "acknowledge_icinga")
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    state = {**_ack_state(), "case_number": "NOC-9", "resource_id": "mon", "thread_id": "t1"}
    # CRITICAL problem -> envelope does not hold -> the node must reach the real
    # interrupt() (which raises outside a langgraph run), keeping the case at a
    # resumable approval point instead of running the thread to END.
    with pytest.raises(Exception):
        await _runner(FakeMCP(problems=[_warning(state=2)])).approval_interrupt(state)


@pytest.mark.asyncio
async def test_grant_execution_enforces_warning_envelope(monkeypatch):
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    monkeypatch.delenv("NOC_ENABLE_NOOP_ROLLBACK_GUARDS", raising=False)
    monkeypatch.setenv("NOC_APPROVAL_SIGNING_SECRET", "secret")
    monkeypatch.setenv("NOC_STANDING_GRANT_ACK_TTL_S", "3600")
    mcp = FakeMCP(problems=[_warning()])
    state = _ack_state()
    state["operator_decision"] = {"decision": "approved", "operator": "standing-grant"}
    import time as time_module

    before = int(time_module.time())
    update = await _runner(mcp).execute_approved_remediation(state)
    assert update["approval_state"] == "executed"
    ack_calls = [args for name, args in mcp.calls if name == "icinga_acknowledge_alert"]
    assert len(ack_calls) == 1
    # the MCP tool contract takes an absolute expiry epoch, not a TTL
    assert before + 3600 <= ack_calls[0]["expiry"] <= int(time_module.time()) + 3600
    assert "ack_ttl_seconds" not in ack_calls[0]
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


@pytest.mark.asyncio
async def test_grant_announce_fires_after_execution_with_outcome(monkeypatch):
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    monkeypatch.delenv("NOC_ENABLE_NOOP_ROLLBACK_GUARDS", raising=False)
    monkeypatch.setenv("NOC_APPROVAL_SIGNING_SECRET", "secret")
    announcements: list[str] = []

    async def fake_notify(*, title, description, color, level):
        announcements.append(title)

    import app.discord as discord_mod

    monkeypatch.setattr(discord_mod, "send_discord_notification", fake_notify)

    # blocked envelope (CRITICAL problem): the audit line must say blocked
    mcp = FakeMCP(problems=[_warning(state=2)])
    state = _ack_state()
    state["operator_decision"] = {"decision": "approved", "operator": "standing-grant"}
    update = await _runner(mcp).execute_approved_remediation(state)
    assert update["approval_state"] == "execution_failed"
    assert announcements and "blocked" in announcements[-1]

    # executed: the audit line says executed
    mcp_ok = FakeMCP(problems=[_warning()])
    state_ok = _ack_state()
    state_ok["operator_decision"] = {"decision": "approved", "operator": "standing-grant"}
    update_ok = await _runner(mcp_ok).execute_approved_remediation(state_ok)
    assert update_ok["approval_state"] == "executed"
    assert "executed" in announcements[-1]


@pytest.mark.asyncio
async def test_standing_grant_terminal_state_is_persisted(monkeypatch):
    from app.graph_runtime import _persist_final_decision

    class FakeMemory:
        def __init__(self):
            self.summary = {"incident_id": "incident-ack", "status": "waiting_approval"}
            self.case_updates: list[dict] = []

        async def get_summary(self, incident_id):
            return dict(self.summary)

        async def put_summary(self, incident_id, summary):
            self.summary = summary

        async def update_case(self, incident_id, payload):
            self.case_updates.append(payload)

    memory = FakeMemory()
    decision = {"decision": "approved", "operator": "standing-grant"}
    final_state = {
        "approval_state": "verified",
        "executed_actions": [{"ok": True}],
        "verification_results": [{"ok": True}],
    }
    summary = await _persist_final_decision(memory, "incident-ack", decision, final_state)
    assert summary is not None
    assert summary["status"] == "resolved"
    assert memory.case_updates[-1]["status"] == "resolved"
    assert memory.case_updates[-1]["decision_status"] == "resolved"

    # failed execution keeps the case actionable, never silently resolved
    memory2 = FakeMemory()
    failed = await _persist_final_decision(
        memory2, "incident-ack", decision, {"approval_state": "execution_failed"}
    )
    assert failed is not None
    assert failed["status"] == "waiting_approval"
    assert memory2.case_updates[-1]["status"] == "waiting_approval"


@pytest.mark.asyncio
async def test_verify_remediation_accepts_ack_by_execution_result(monkeypatch):
    monkeypatch.setenv("NOC_ENABLE_APPROVED_EXECUTION", "1")
    monkeypatch.delenv("NOC_ENABLE_NOOP_ROLLBACK_GUARDS", raising=False)
    runner = _runner(FakeMCP())
    state = {
        **_ack_state(),
        "approval_state": "executed",
        "executed_actions": [
            {
                "action": {
                    "action_id": "act-1",
                    "type": "acknowledge_icinga",
                    "inputs": {"host": "mon", "service": "disk"},
                },
                "execution_mode": "real_action",
                "ok": True,
                "result": {"ok": True},
            }
        ],
    }
    update = await runner.verify_remediation(state)
    # mon has no Prometheus instance mapping; an ack must not be host-up probed
    assert update["approval_state"] == "verified"
    assert update["verification_results"][0]["method"] == "ack_execution_result"


def test_ack_intent_requires_whole_word_in_proposal():
    from app.graph.nodes import _approved_icinga_ack_actions

    alert = {"commonLabels": {"host": "mon", "service": "blackbox-icmp"}}
    # incidental substrings ("blackbox", "packet") and alert-only text must not
    # mint an ack action
    assert (
        _approved_icinga_ack_actions(
            {"normalized_alert": alert}, ["Investigate blackbox-icmp packet loss on mon"]
        )
        == []
    )
    assert _approved_icinga_ack_actions({"normalized_alert": alert}, []) == []
    # explicit proposal intent does
    actions = _approved_icinga_ack_actions(
        {"normalized_alert": alert}, ["Acknowledge the disk warning on mon"]
    )
    assert len(actions) == 1
    assert actions[0]["type"] == "acknowledge_icinga"
    assert actions[0]["inputs"]["host"] == "mon"
