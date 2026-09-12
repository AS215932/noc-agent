from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from agent_core.contracts import HandoffEnvelope, HandoffRecord
from agent_core.coordination import CoordinatorError

from app.cases.models import AtomicCaseProjection
from app.coordination import NocCoordinatorWorker


class FakeClient:
    def __init__(self, records: list[HandoffRecord]) -> None:
        self.records = records
        self.projected: list[Any] = []
        self.results: list[Any] = []

    async def heartbeat(self, heartbeat):  # type: ignore[no-untyped-def]
        return heartbeat.model_dump(mode="json")

    async def inbox(self, *, status: str):
        return [record for record in self.records if record.status == status]

    async def claim(self, handoff_id: str):
        return next(record for record in self.records if record.envelope.handoff_id == handoff_id)

    async def progress(self, handoff_id: str, summary: str):
        return None

    async def submit_result(self, result):  # type: ignore[no-untyped-def]
        self.results.append(result)
        return None

    async def put_case(self, projection):  # type: ignore[no-untyped-def]
        self.projected.append(projection)
        return projection

    async def create_handoff(self, envelope):  # type: ignore[no-untyped-def]
        return HandoffRecord(envelope=envelope, status="queued")


class FakeMCP:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def call_tool(self, source: str, tool: str, arguments: dict[str, Any]):
        self.calls.append((source, tool, arguments))
        return {"ok": True, "stdout": "reachable\n", "data": {"safe": True}}


class FakeStore:
    def __init__(self) -> None:
        self.case = AtomicCaseProjection(
            case_id="case_noc_1",
            title="NOC case",
            summary="Sanitized state",
            severity="HIGH",
            resource_id="rtr",
        )

    async def list_cases(self, *, limit: int):
        return [self.case]

    async def list_handoffs(self, *, limit: int):
        return []

    async def get_case(self, case_id: str):
        return self.case if case_id == self.case.case_id else None


@pytest.mark.asyncio
async def test_noc_worker_serves_bounded_read_only_snapshot_and_projects_cases() -> None:
    envelope = HandoffEnvelope(
        source_loop="soc",
        target_loop="noc",
        capability="noc.network_snapshot.read",
        payload={"tool": "frr_vtysh_cmd", "arguments": {"host": "rtr", "command": "show bgp"}},
        idempotency_key="snapshot:1",
    )
    client = FakeClient([HandoffRecord(envelope=envelope, status="queued")])
    mcp = FakeMCP()
    worker = NocCoordinatorWorker(
        client,  # type: ignore[arg-type]
        mcp,  # type: ignore[arg-type]
        SimpleNamespace(store=FakeStore()),  # type: ignore[arg-type]
    )
    report = await worker.run_once()
    assert report["completed"] == [envelope.handoff_id]
    assert report["projected_cases"] == 1
    assert client.projected[0].owner_loop == "noc"
    assert mcp.calls == [("hyrule", "frr_vtysh_cmd", {"host": "rtr", "command": "show bgp"})]
    assert client.results[0].payload["tool_result"]["stdout"] == "reachable\n"


@pytest.mark.asyncio
async def test_noc_worker_refuses_mutating_or_heavy_snapshot_tools() -> None:
    client = FakeClient([])
    worker = NocCoordinatorWorker(
        client,  # type: ignore[arg-type]
        FakeMCP(),  # type: ignore[arg-type]
        SimpleNamespace(store=FakeStore()),  # type: ignore[arg-type]
    )
    with pytest.raises(ValueError, match="not allowed"):
        await worker._dispatch(
            "noc.network_snapshot.read",
            {"tool": "prepare_commit_confirm", "arguments": {"host": "rtr"}},
        )
    with pytest.raises(ValueError, match="not allowed"):
        await worker._dispatch(
            "noc.network_snapshot.read",
            {"tool": "tcpdump_capture", "arguments": {"host": "rtr"}},
        )
    with pytest.raises(ValueError, match="one bounded show command"):
        await worker._dispatch(
            "noc.network_snapshot.read",
            {
                "tool": "frr_vtysh_cmd",
                "arguments": {
                    "host": "rtr",
                    "command": "show bgp; configure terminal",
                },
            },
        )
    with pytest.raises(ValueError, match="unsupported keys"):
        await worker._dispatch(
            "noc.network_snapshot.read",
            {
                "tool": "firewall_state",
                "arguments": {"host": "rtr", "apply": True},
            },
        )


@pytest.mark.asyncio
async def test_noc_worker_skips_a_handoff_claimed_by_another_worker() -> None:
    envelope = HandoffEnvelope(
        source_loop="soc",
        target_loop="noc",
        capability="noc.network_snapshot.read",
        payload={"tool": "firewall_state", "arguments": {"host": "rtr"}},
        idempotency_key="snapshot:claim-race",
    )

    class ClaimConflict(FakeClient):
        async def claim(self, handoff_id: str):
            raise CoordinatorError("coordinator POST /claim returned 409: already claimed")

    client = ClaimConflict([HandoffRecord(envelope=envelope, status="queued")])
    worker = NocCoordinatorWorker(
        client,  # type: ignore[arg-type]
        FakeMCP(),  # type: ignore[arg-type]
        SimpleNamespace(store=FakeStore()),  # type: ignore[arg-type]
    )
    report = await worker.run_once()
    assert report["completed"] == []
    assert report["failed"] == []
    assert client.results == []


@pytest.mark.asyncio
async def test_network_change_capability_only_prepares() -> None:
    worker = NocCoordinatorWorker(
        FakeClient([]),  # type: ignore[arg-type]
        FakeMCP(),  # type: ignore[arg-type]
        SimpleNamespace(store=FakeStore()),  # type: ignore[arg-type]
    )
    result = await worker._dispatch(
        "noc.network_change.prepare", {"requested_change": {"service": "frr"}}
    )
    assert result["plan"]["production_executed"] is False
    assert result["plan"]["requires_target_policy_gate"] is True
