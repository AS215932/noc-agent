"""NOC adapter for the shared agent-core LHP-v2 coordinator."""

from __future__ import annotations

import asyncio
import os
import re
from datetime import datetime
from typing import Any, Literal

from agent_core.contracts import (
    CaseProjection as SharedCaseProjection,
    HandoffEnvelope,
    HandoffResult,
    LoopHeartbeat,
    SourceRef,
)
from agent_core.coordination import CoordinatorClient, CoordinatorError

from app import log
from app.cases.runtime import CaseServiceRuntime, build_case_service_runtime_from_env
from app.mcp_runtime import MCPRuntime, PROACTIVE_HEAVY_TOOLS

SNAPSHOT_TOOLS = frozenset(
    {
        "icinga_list_problems",
        "icinga_get_host_state",
        "prometheus_list_targets",
        "prometheus_query",
        "frr_vtysh_cmd",
        "path_explain",
        "ecmp_path_select",
        "socket_listeners",
        "firewall_state",
        "pf_log_tail",
        "nft_log_tail",
        "ndp_state",
        "arp_state",
        "wg_show",
        "vault_agent_status",
        "os_service_status",
        "os_systemd_status",
        "os_rcctl_check",
        "dns_dig",
        "knot_zone_status",
    }
)

SNAPSHOT_ARGUMENT_KEYS: dict[str, frozenset[str]] = {
    "icinga_list_problems": frozenset({"object_type", "limit"}),
    "icinga_get_host_state": frozenset({"host"}),
    "prometheus_list_targets": frozenset({"filter"}),
    "prometheus_query": frozenset({"query"}),
    "frr_vtysh_cmd": frozenset({"host", "command"}),
    "path_explain": frozenset({"from_host", "to_addr", "protocol", "src_port"}),
    "ecmp_path_select": frozenset(
        {"from_host", "to_addr", "n_flows", "protocol", "vary"}
    ),
    "socket_listeners": frozenset({"host"}),
    "firewall_state": frozenset({"host"}),
    "pf_log_tail": frozenset({"host", "count", "filter", "since"}),
    "nft_log_tail": frozenset({"host", "count", "filter", "since"}),
    "ndp_state": frozenset({"host", "addr", "iface"}),
    "arp_state": frozenset({"host", "addr", "iface"}),
    "wg_show": frozenset({"host"}),
    "vault_agent_status": frozenset({"host"}),
    "os_service_status": frozenset({"host", "service"}),
    "os_systemd_status": frozenset({"host", "unit"}),
    "os_rcctl_check": frozenset({"host", "service"}),
    "dns_dig": frozenset({"host", "target", "query_type", "nameserver"}),
    "knot_zone_status": frozenset({"host"}),
}


def _validate_snapshot_arguments(tool: str, arguments: dict[str, Any]) -> None:
    allowed = SNAPSHOT_ARGUMENT_KEYS.get(tool)
    if allowed is None:
        raise ValueError(f"snapshot tool {tool!r} has no argument contract")
    unexpected = sorted(set(arguments) - allowed)
    if unexpected:
        raise ValueError(f"snapshot arguments contain unsupported keys: {unexpected}")
    for key, value in arguments.items():
        if isinstance(value, (dict, list, tuple, set)):
            raise ValueError(f"snapshot argument {key!r} must be a scalar")
        if isinstance(value, str) and len(value) > 4000:
            raise ValueError(f"snapshot argument {key!r} exceeds 4000 characters")
    if tool == "frr_vtysh_cmd":
        command = str(arguments.get("command") or "").strip()
        if not re.fullmatch(
            r"show(?: [A-Za-z0-9_.:/,()\[\]-]+)+", command, re.IGNORECASE
        ):
            raise ValueError("frr_vtysh_cmd accepts one bounded show command only")


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _bounded(value: Any, *, depth: int = 5) -> Any:
    if depth <= 0:
        return str(value)[:1000]
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in list(value.items())[:100]:
            safe_key = str(key)[:100]
            limit = 32_768 if safe_key == "stdout" else 4_000
            result[safe_key] = str(item)[:limit] if isinstance(item, str) else _bounded(item, depth=depth - 1)
        return result
    if isinstance(value, (list, tuple)):
        return [_bounded(item, depth=depth - 1) for item in list(value)[:100]]
    if isinstance(value, str):
        return " ".join(value.split())[:4000]
    if isinstance(value, (bool, int, float)) or value is None:
        return value
    return str(value)[:1000]


class NocCoordinatorWorker:
    def __init__(
        self,
        client: CoordinatorClient,
        mcp_runtime: MCPRuntime,
        case_runtime: CaseServiceRuntime,
    ) -> None:
        self.client = client
        self.mcp_runtime = mcp_runtime
        self.case_runtime = case_runtime

    async def run_once(self) -> dict[str, Any]:
        report: dict[str, Any] = {
            "projected_cases": 0,
            "mirrored_handoffs": 0,
            "checked": 0,
            "completed": [],
            "failed": [],
        }
        await self.client.heartbeat(
            LoopHeartbeat(loop_id="noc", status="active", summary="NOC coordinator worker active")
        )
        report["projected_cases"] = await self._project_cases()
        report["mirrored_handoffs"] = await self._mirror_legacy_handoffs()
        for record in await self.client.inbox(status="queued"):
            capability = record.envelope.capability
            if capability not in {
                "noc.network_snapshot.read",
                "noc.network_change.prepare",
                "noc.verify",
            }:
                continue
            report["checked"] += 1
            handoff_id = record.envelope.handoff_id
            try:
                await self.client.claim(handoff_id)
            except CoordinatorError as exc:
                if "returned 409" in str(exc):
                    continue
                report["failed"].append(
                    {"handoff_id": handoff_id, "error": "coordinator claim failed"}
                )
                continue
            try:
                await self.client.progress(handoff_id, f"NOC processing {capability}")
                payload = await self._dispatch(capability, record.envelope.payload)
                await self.client.submit_result(
                    HandoffResult(
                        handoff_id=handoff_id,
                        outcome="succeeded",
                        summary=str(payload.pop("summary", f"NOC completed {capability}")),
                        payload=payload,
                    )
                )
                report["completed"].append(handoff_id)
            except Exception as exc:
                log.warning("noc_coordinator_handoff_failed", handoff_id=handoff_id, error=type(exc).__name__)
                try:
                    await self.client.submit_result(
                        HandoffResult(
                            handoff_id=handoff_id,
                            outcome="failed",
                            summary=f"NOC failed {capability}: {type(exc).__name__}",
                        )
                    )
                except Exception:
                    pass
                report["failed"].append({"handoff_id": handoff_id, "error": str(exc)[:300]})
        return report

    async def _dispatch(self, capability: str, payload: dict[str, Any]) -> dict[str, Any]:
        if capability == "noc.network_snapshot.read":
            tool = str(payload.get("tool") or "")
            arguments = payload.get("arguments")
            if tool not in SNAPSHOT_TOOLS or tool in PROACTIVE_HEAVY_TOOLS:
                raise ValueError(f"tool {tool!r} is not allowed for coordinator snapshots")
            if not isinstance(arguments, dict):
                raise ValueError("snapshot arguments must be an object")
            _validate_snapshot_arguments(tool, arguments)
            result = await self.mcp_runtime.call_tool("hyrule", tool, arguments)
            return {
                "summary": f"NOC returned bounded read-only snapshot {tool}",
                "tool_result": _bounded(result),
                "source": "hyrule-mcp",
            }
        if capability == "noc.network_change.prepare":
            # Approval authorizes planning only. Existing NOC action-authorization and
            # commit-confirm gates remain separately required for production execution.
            return {
                "summary": "NOC prepared a non-executing network change plan",
                "plan": {
                    "requested_change": _bounded(payload.get("requested_change", {})),
                    "requires_target_policy_gate": True,
                    "production_executed": False,
                },
            }
        case_id = str(payload.get("case_id") or "")
        case = await self.case_runtime.store.get_case(case_id)
        if case is None:
            raise ValueError("noc.verify references an unknown NOC case")
        return {
            "summary": "NOC returned its authoritative case state",
            "case_id": case.case_id,
            "status": case.status,
            "updated_at": case.updated_at,
            "resolved_at": getattr(case, "resolved_at", None),
        }

    async def _project_cases(self) -> int:
        cases = await self.case_runtime.store.list_cases(limit=500)
        count = 0
        for case in cases:
            citations = []
            for raw in getattr(case, "knowledge_citations", [])[:40]:
                if not isinstance(raw, dict):
                    continue
                ref = str(raw.get("claim_id") or raw.get("concept_id") or raw.get("source_uri") or "")
                if ref:
                    citations.append(SourceRef(ref=ref, kind="knowledge", authority="A1"))
            await self.client.put_case(
                SharedCaseProjection(
                    case_id=case.case_id,
                    owner_loop="noc",
                    status=case.status,
                    severity=getattr(case, "severity", "UNKNOWN"),
                    title=case.title,
                    summary=case.summary,
                    resource_id=getattr(case, "resource_id", ""),
                    evidence_refs=citations,
                    opened_at=_parse_time(case.opened_at) or datetime.now().astimezone(),
                    updated_at=_parse_time(case.updated_at) or datetime.now().astimezone(),
                    resolved_at=_parse_time(getattr(case, "resolved_at", None)),
                    metadata={
                        "kind": case.kind,
                        "origin": getattr(case, "origin", "unknown"),
                        "fingerprint": getattr(case, "fingerprint", ""),
                    },
                )
            )
            count += 1
        return count

    async def _mirror_legacy_handoffs(self) -> int:
        legacy = await self.case_runtime.store.list_handoffs(limit=500)
        count = 0
        for handoff in legacy:
            if handoff.target_loop not in {"engineering", "knowledge"}:
                continue
            case = await self.case_runtime.store.get_case(handoff.case_id)
            severity = getattr(case, "severity", "MEDIUM") if case else "MEDIUM"
            approval_tier: Literal["none", "operator", "senior"]
            if handoff.target_loop == "engineering":
                capability = "engineering.draft_pr"
                approval_tier = "senior" if severity in {"HIGH", "CRITICAL"} else "operator"
            elif handoff.target_loop == "knowledge":
                capability = "knowledge.context.resolve"
                approval_tier = "none"
            envelope = HandoffEnvelope(
                handoff_id=handoff.handoff_id,
                source_loop="noc",
                target_loop=handoff.target_loop,
                capability=capability,
                case_id=handoff.case_id,
                intent=handoff.objective,
                summary=handoff.objective,
                risk_level="high" if severity in {"HIGH", "CRITICAL"} else "medium",
                approval_tier=approval_tier,
                payload=_bounded(handoff.payload),
                constraints={"legacy_lhp_v1_mirror": True, "shadow_only": True},
                idempotency_key=f"{handoff.idempotency_key}:lhp-v2",
            )
            try:
                await self.client.create_handoff(envelope)
                count += 1
            except CoordinatorError as exc:
                log.warning("noc_handoff_mirror_failed", handoff_id=handoff.handoff_id, error=str(exc))
        return count


async def run_worker() -> None:
    interval = max(1, int(os.getenv("NOC_COORDINATOR_POLL_SECONDS", "5")))
    client = CoordinatorClient.from_env("noc")
    mcp = MCPRuntime(owner="coordinator")
    runtime: CaseServiceRuntime | None = None
    await mcp.connect_tools()
    try:
        runtime = await build_case_service_runtime_from_env(force=True)
        if runtime is None:
            raise RuntimeError("NOC CaseService runtime is required")
        worker = NocCoordinatorWorker(client, mcp, runtime)
        failures = 0
        while True:
            try:
                await worker.run_once()
                failures = 0
            except Exception as exc:
                failures += 1
                log.warning(
                    "noc_coordinator_cycle_failed",
                    error=type(exc).__name__,
                    consecutive_failures=failures,
                )
            delay = interval if failures == 0 else min(60, interval * (2 ** min(failures, 4)))
            await asyncio.sleep(delay)
    finally:
        if runtime is not None:
            await runtime.close()
        await mcp.disconnect()


def main() -> None:
    asyncio.run(run_worker())
