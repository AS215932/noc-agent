"""The hotspot engine: read-only scan rules that turn live telemetry into ranked
:class:`~app.proactive.models.Hotspot` early-warning signals.

Rules are grounded in the metrics the infra actually exports and alerts on
(``configs/mon/prometheus-rules/noc-tripwire.yml``): ``up``, ``probe_success``,
``frr_bgp_peer_state``, ``node_filesystem_*``, ``node_systemd_*``. Where a
reactive tripwire fires on the hard condition (e.g. ``BGPSessionDown`` after a
2m ``for``), the proactive rule fires *earlier* on the precursor (the peer just
left Established, the session is flapping, the disk will fill within 24h).

Every rule is cheap and read-only (``prometheus_query`` / ``icinga_list_problems``
via :meth:`MCPRuntime.call_tool`), wrapped so a single rule failure degrades to
"no hotspots from this rule" rather than crashing the cycle.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from app import log
from app.config import ProactiveLoopSettings
from app.graph.routing import instance_host
from app.proactive.models import Hotspot, HotspotEvidence, Severity
from app.safe_errors import classify_exception, log_exception


# Filesystem fstypes that are not real persistent storage.
_FS_EXCLUDE = r'fstype!~"tmpfs|ramfs|overlay|squashfs|devtmpfs|fuse.*|nsfs|autofs"'


@dataclass(slots=True)
class Sample:
    labels: dict[str, str]
    value: float


def _to_float(value: Any) -> float | None:
    # Prometheus instant vectors expose value as a scalar string; some MCP
    # shims pass the raw [timestamp, "value"] pair. Tolerate both.
    if isinstance(value, (list, tuple)) and len(value) == 2:
        value = value[1]
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_prom_vector(resp: Any) -> list[Sample]:
    """Extract ``[(labels, value)]`` from a ``prometheus_query`` MCP result.

    Handles the shapes seen in the codebase: a dict with ``ok``/``result`` (see
    ``app/graph/nodes.py:_prometheus_up``) and the nested ``data.result`` form.
    """
    if not isinstance(resp, dict):
        return []
    if resp.get("ok") is False:
        return []
    rows = resp.get("result")
    if rows is None:
        rows = (resp.get("data") or {}).get("result") if isinstance(resp.get("data"), dict) else None
    if not isinstance(rows, list):
        return []
    samples: list[Sample] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = _to_float(row.get("value"))
        if value is None:
            continue
        metric = row.get("metric")
        labels = {str(k): str(v) for k, v in metric.items()} if isinstance(metric, dict) else {}
        samples.append(Sample(labels=labels, value=value))
    return samples


@dataclass
class ScanContext:
    mcp_runtime: Any
    settings: ProactiveLoopSettings
    # Active lessons injected into scanner reasoning (Phase 3). Hook kept here
    # so rules can consult tuned thresholds without a signature change.
    lessons: list[str] = field(default_factory=list)

    async def prom(self, query: str) -> list[Sample]:
        try:
            resp = await self.mcp_runtime.call_tool("hyrule", "prometheus_query", {"query": query})
        except Exception as exc:  # one rule failure must not kill the cycle
            safe = classify_exception(exc)
            log_exception("proactive_prom_query_failed", exc, category=safe.category, query=query[:120])
            return []
        return parse_prom_vector(resp)

    async def icinga_problems(self, object_type: str = "service", limit: int = 50) -> Any:
        try:
            return await self.mcp_runtime.call_tool(
                "hyrule", "icinga_list_problems", {"object_type": object_type, "limit": limit}
            )
        except Exception as exc:
            safe = classify_exception(exc)
            log_exception("proactive_icinga_problems_failed", exc, category=safe.category)
            return None


ScanRule = Callable[[ScanContext], Awaitable[list[Hotspot]]]


# --------------------------------------------------------------------------
# Rules
# --------------------------------------------------------------------------


async def rule_bgp_risk(ctx: ScanContext) -> list[Hotspot]:
    """Peer left Established (precursor to ``BGPSessionDown``) or is flapping."""
    hotspots: dict[str, Hotspot] = {}

    for sample in await ctx.prom('frr_bgp_peer_state{state!="Established"} == 1'):
        peer = sample.labels.get("peer") or sample.labels.get("neighbor") or "unknown"
        host = instance_host(sample.labels.get("instance", "")) or "router"
        state = sample.labels.get("state", "unknown")
        key = f"{host}:{peer}"
        hotspots[key] = Hotspot(
            rule_id="bgp_risk",
            key=key,
            category="bgp",
            severity="MEDIUM",
            score=240.0,
            title=f"BGP peer {peer} not Established",
            resource=host,
            summary=f"Peer {peer} on {host} is in state {state} (pre-alert; BGPSessionDown fires after 2m).",
            evidence=[HotspotEvidence(label=f"bgp peer state {host}/{peer}", value=state, threshold="Established")],
            recommended_checks=[f"frr_vtysh_cmd on {host}: show bgp summary", f"check peer {peer} session logs"],
            suggested_specialist="bgp",
            warrants_change=False,
        )

    for sample in await ctx.prom('changes(frr_bgp_peer_state[1h]) >= 4'):
        peer = sample.labels.get("peer") or sample.labels.get("neighbor") or "unknown"
        host = instance_host(sample.labels.get("instance", "")) or "router"
        key = f"{host}:{peer}"
        flaps = int(sample.value)
        existing = hotspots.get(key)
        sev: Severity = "HIGH" if existing is not None or flaps >= 8 else "MEDIUM"
        ev = HotspotEvidence(
            label=f"bgp state changes/1h {host}/{peer}",
            query="changes(frr_bgp_peer_state[1h])",
            value=str(flaps),
            threshold=">=4",
            detail="session flapping",
        )
        if existing is not None:
            existing.severity = sev
            existing.score = 320.0
            existing.summary += f" Also flapping: {flaps} state changes in the last hour."
            existing.evidence.append(ev)
            existing.warrants_change = True
            existing.change_rationale = "Recurrent BGP flap may need timer/policy or peer-side coordination."
        else:
            hotspots[key] = Hotspot(
                rule_id="bgp_risk",
                key=key,
                category="bgp",
                severity=sev,
                score=300.0 if sev == "HIGH" else 230.0,
                title=f"BGP peer {peer} flapping",
                resource=host,
                summary=f"Peer {peer} on {host} flapped {flaps} times in the last hour.",
                evidence=[ev],
                recommended_checks=[f"frr_vtysh_cmd on {host}: show bgp neighbor {peer}"],
                suggested_specialist="bgp",
                warrants_change=True,
                change_rationale="Recurrent BGP flap may need timer/policy or peer-side coordination.",
            )
    return list(hotspots.values())


async def rule_disk_fill(ctx: ScanContext) -> list[Hotspot]:
    """Filesystem low on space or projected to fill soon (precursor to a disk
    alert / service outage)."""
    free = {
        _fs_key(s.labels): (s, s.value)
        for s in await ctx.prom(
            f"node_filesystem_avail_bytes{{{_FS_EXCLUDE}}} / node_filesystem_size_bytes{{{_FS_EXCLUDE}}}"
        )
    }
    fills_24h = {
        _fs_key(s.labels)
        for s in await ctx.prom(
            f"predict_linear(node_filesystem_avail_bytes{{{_FS_EXCLUDE}}}[6h], 86400) <= 0"
        )
    }
    hotspots: list[Hotspot] = []
    for key, (sample, ratio) in free.items():
        soon = key in fills_24h
        if ratio >= 0.20 and not soon:
            continue
        host = instance_host(sample.labels.get("instance", "")) or "host"
        mount = sample.labels.get("mountpoint", "?")
        pct = round(ratio * 100, 1)
        sev: Severity = "HIGH" if (soon or ratio < 0.10) else "MEDIUM"
        score = 360.0 if sev == "HIGH" else 250.0
        detail = "projected to fill within 24h" if soon else "low free space"
        hotspots.append(
            Hotspot(
                rule_id="disk_fill",
                key=f"{host}:{mount}",
                category="disk",
                severity=sev,
                score=score,
                title=f"Disk {mount} low on {host}",
                resource=host,
                summary=f"{host}:{mount} has {pct}% free ({detail}).",
                evidence=[
                    HotspotEvidence(
                        label=f"free ratio {host}:{mount}",
                        query="node_filesystem_avail_bytes / node_filesystem_size_bytes",
                        value=f"{pct}%",
                        threshold="<20% (HIGH <10% or fills <24h)",
                        detail=detail,
                    )
                ],
                recommended_checks=[
                    f"os_journalctl / du on {host}:{mount}",
                    "identify largest growing path; check log rotation / retention",
                ],
                suggested_specialist="infrastructure",
                warrants_change=soon,
                change_rationale=("Retention/rotation or volume sizing may need a config change." if soon else ""),
            )
        )
    return hotspots


async def rule_scrape_flap(ctx: ScanContext) -> list[Hotspot]:
    """A scrape target flapping up/down (instability before a hard outage)."""
    hotspots: list[Hotspot] = []
    for sample in await ctx.prom("changes(up[2h]) >= 4"):
        host = instance_host(sample.labels.get("instance", "")) or sample.labels.get("instance", "target")
        job = sample.labels.get("job", "")
        flaps = int(sample.value)
        sev: Severity = "HIGH" if flaps >= 8 else "MEDIUM"
        hotspots.append(
            Hotspot(
                rule_id="scrape_flap",
                key=f"{host}:{job}",
                category="scrape",
                severity=sev,
                score=(300.0 if sev == "HIGH" else 220.0),
                title=f"Scrape target {host} flapping",
                resource=host,
                summary=f"Target {host} (job {job}) changed up/down {flaps} times in 2h.",
                evidence=[
                    HotspotEvidence(
                        label=f"up changes/2h {host}",
                        query="changes(up[2h])",
                        value=str(flaps),
                        threshold=">=4",
                    )
                ],
                recommended_checks=[f"check {host} resource pressure / exporter logs", "look for reboot/OOM/network blips"],
                suggested_specialist="infrastructure",
            )
        )
    return hotspots


async def rule_service_churn(ctx: ScanContext) -> list[Hotspot]:
    """Systemd unit restart churn or a failed unit."""
    hotspots: dict[str, Hotspot] = {}
    for sample in await ctx.prom("increase(node_systemd_service_restart_total[2h]) >= 3"):
        host = instance_host(sample.labels.get("instance", "")) or "host"
        unit = sample.labels.get("name") or sample.labels.get("unit") or "unit"
        restarts = int(sample.value)
        sev: Severity = "HIGH" if restarts >= 6 else "MEDIUM"
        key = f"{host}:{unit}"
        hotspots[key] = Hotspot(
            rule_id="service_churn",
            key=key,
            category="service",
            severity=sev,
            score=(300.0 if sev == "HIGH" else 230.0),
            title=f"{unit} restart churn on {host}",
            resource=host,
            summary=f"{unit} on {host} restarted ~{restarts} times in 2h.",
            evidence=[
                HotspotEvidence(
                    label=f"restarts/2h {host}/{unit}",
                    query="increase(node_systemd_service_restart_total[2h])",
                    value=str(restarts),
                    threshold=">=3",
                )
            ],
            recommended_checks=[f"os_service_logs {unit} on {host}", "check for crash loop / config error"],
            suggested_specialist="infrastructure",
        )
    for sample in await ctx.prom('node_systemd_unit_state{state="failed"} == 1'):
        host = instance_host(sample.labels.get("instance", "")) or "host"
        unit = sample.labels.get("name") or sample.labels.get("unit") or "unit"
        key = f"{host}:{unit}"
        hotspots[key] = Hotspot(
            rule_id="service_churn",
            key=key,
            category="service",
            severity="HIGH",
            score=340.0,
            title=f"{unit} failed on {host}",
            resource=host,
            summary=f"systemd unit {unit} on {host} is in failed state.",
            evidence=[
                HotspotEvidence(
                    label=f"unit state {host}/{unit}",
                    query='node_systemd_unit_state{state="failed"}',
                    value="failed",
                    threshold="active",
                )
            ],
            recommended_checks=[f"os_systemd_status {unit} on {host}", f"os_service_logs {unit}"],
            suggested_specialist="infrastructure",
        )
    return list(hotspots.values())


async def rule_reachability_flap(ctx: ScanContext) -> list[Hotspot]:
    """Intermittent ICMP reachability (e.g. WireGuard mesh link degrading before
    the 3m ``BlackboxICMPDown`` tripwire fires)."""
    hotspots: list[Hotspot] = []
    for sample in await ctx.prom('avg_over_time(probe_success{job="blackbox-icmp"}[30m])'):
        ratio = sample.value
        if not (0.0 <= ratio < 0.95):
            continue
        instance = sample.labels.get("instance", "")
        host = instance_host(instance) or instance or "target"
        wg = "ff" in instance.lower() and "::" in instance  # overlay /127 mesh endpoints
        sev: Severity = "HIGH" if ratio < 0.5 else "MEDIUM"
        hotspots.append(
            Hotspot(
                rule_id="reachability_flap",
                key=instance or host,
                category="wireguard" if wg else "service",
                severity=sev,
                score=(310.0 if sev == "HIGH" else 230.0),
                title=f"Intermittent ICMP to {host}",
                resource=host,
                summary=f"ICMP success to {instance} averaged {round(ratio * 100)}% over 30m (1.0 = healthy).",
                evidence=[
                    HotspotEvidence(
                        label=f"icmp success 30m {instance}",
                        query='avg_over_time(probe_success{job="blackbox-icmp"}[30m])',
                        value=str(round(ratio, 3)),
                        threshold="1.0",
                        detail="packet loss / link degrading",
                    )
                ],
                recommended_checks=[
                    f"wg_show on the endpoints for {host}",
                    "check latency/handshake age; underlay path",
                ],
                suggested_specialist="infrastructure",
            )
        )
    return hotspots


async def rule_tls_expiry(ctx: ScanContext) -> list[Hotspot]:
    """TLS certs approaching expiry (blackbox ``probe_ssl_earliest_cert_expiry``)."""
    hotspots: list[Hotspot] = []
    for sample in await ctx.prom("probe_ssl_earliest_cert_expiry - time()"):
        seconds = sample.value
        if seconds > 14 * 86400 or seconds < -86400:
            continue
        instance = sample.labels.get("instance", "cert")
        days = round(seconds / 86400, 1)
        sev: Severity = "HIGH" if seconds < 3 * 86400 else "MEDIUM"
        hotspots.append(
            Hotspot(
                rule_id="tls_expiry",
                key=instance,
                category="tls",
                severity=sev,
                score=(330.0 if sev == "HIGH" else 240.0),
                title=f"TLS cert near expiry for {instance}",
                resource=instance,
                summary=f"Certificate for {instance} expires in {days} day(s).",
                evidence=[
                    HotspotEvidence(
                        label=f"cert expiry {instance}",
                        query="probe_ssl_earliest_cert_expiry - time()",
                        value=f"{days}d",
                        threshold="<14d (HIGH <3d)",
                    )
                ],
                recommended_checks=["check Caddy/ACME renewal logs on proxy", "verify DNS-01/HTTP-01 path"],
                suggested_specialist="infrastructure",
                warrants_change=False,
            )
        )
    return hotspots


def _fs_key(labels: dict[str, str]) -> str:
    return f"{labels.get('instance', '')}|{labels.get('mountpoint', '')}"


# Cheap rules run every cycle; deep rules (heavier range vectors / predictions)
# run on the slower deep-scan cadence.
CHEAP_RULES: tuple[ScanRule, ...] = (
    rule_bgp_risk,
    rule_scrape_flap,
    rule_service_churn,
    rule_reachability_flap,
)
DEEP_RULES: tuple[ScanRule, ...] = (
    rule_disk_fill,
    rule_tls_expiry,
)


async def scan(ctx: ScanContext, *, deep: bool = False) -> list[Hotspot]:
    """Run the scan rules and return de-duplicated hotspots (highest score wins
    on fingerprint collision)."""
    rules = list(CHEAP_RULES) + (list(DEEP_RULES) if deep else [])
    results = await asyncio.gather(*(_run_rule(rule, ctx) for rule in rules))
    merged: dict[str, Hotspot] = {}
    for batch in results:
        for hotspot in batch:
            fp = hotspot.fingerprint()
            current = merged.get(fp)
            if current is None or hotspot.score > current.score:
                merged[fp] = hotspot
    ordered = sorted(merged.values(), key=lambda h: h.score, reverse=True)
    log.info("proactive_scan_complete", deep=deep, hotspots=len(ordered))
    return ordered


async def _run_rule(rule: ScanRule, ctx: ScanContext) -> list[Hotspot]:
    try:
        return await rule(ctx)
    except Exception as exc:  # isolate rule failures
        safe = classify_exception(exc)
        log_exception("proactive_scan_rule_failed", exc, category=safe.category, rule=getattr(rule, "__name__", "?"))
        return []
