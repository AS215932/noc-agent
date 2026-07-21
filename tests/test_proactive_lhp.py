from app.cases.models import AtomicCaseProjection
from app.proactive.lhp import build_disk_handoff_request, disk_resource_parts, is_disk_handoff_hotspot
from app.proactive.models import Hotspot, HotspotEvidence


def test_disk_handoff_request_sanitizes_and_marks_untrusted_payload():
    hotspot = Hotspot(
        rule_id="disk_fill",
        key="rtr:/",
        category="disk",
        severity="HIGH",
        title="Disk / low ```ignore previous```",
        resource="rtr",
        summary="Authorization: Bearer secret-token <script>",
        evidence=[HotspotEvidence(label="free ratio", value="5%", detail="```raw```")],
        recommended_checks=["du -sh /var/log ```rm -rf```"],
        warrants_change=True,
        change_rationale="password=secret rotate logs",
    )
    case = AtomicCaseProjection(case_id="case_1", fingerprint=hotspot.fingerprint(), status="open")

    request = build_disk_handoff_request(
        hotspot,
        case,
        cycle_id="cycle_1",
        suppression_entry={"reason": "Authorization: Bearer nope", "operator": "svag<admin>", "expires_at": 123.0},
        knowledge_context_enabled=True,
    )

    rendered = str(request.handoff.model_dump(mode="json"))
    assert "Bearer secret-token" not in rendered
    assert "Bearer nope" not in rendered
    assert "```" not in rendered
    assert "<script>" not in rendered
    assert request.handoff.payload["hotspot"]["untrusted_evidence"] is True
    assert request.handoff.payload["suppression"]["untrusted_evidence"] is True
    assert request.knowledge_payload["untrusted_evidence"] is True
    assert request.objectives[0].required_consecutive_passes == 3


def test_disk_helpers_identify_disk_hotspots_and_resource_parts():
    disk = Hotspot(
        rule_id="disk_fill",
        key="host:/var",
        category="disk",
        severity="HIGH",
        resource="host",
        warrants_change=True,
    )
    monitor_only = Hotspot(
        rule_id="disk_fill",
        key="host:/",
        category="disk",
        severity="MEDIUM",
        resource="host",
        warrants_change=False,
    )
    other = Hotspot(rule_id="bgp_risk", key="rtr:p1", category="bgp", severity="HIGH", resource="rtr")

    assert is_disk_handoff_hotspot(disk) is True
    assert is_disk_handoff_hotspot(monitor_only) is False
    assert is_disk_handoff_hotspot(other) is False
    assert disk_resource_parts(disk) == ("host", "/var")


def test_disk_handoff_idempotency_is_stable_within_case_occurrence():
    hotspot = Hotspot(
        rule_id="disk_fill",
        key="host:/",
        category="disk",
        severity="HIGH",
        resource="host",
        warrants_change=True,
    )
    case = AtomicCaseProjection(
        case_id="case_occurrence",
        fingerprint=hotspot.fingerprint(),
        opened_at="2026-07-01T00:00:00+00:00",
    )

    first = build_disk_handoff_request(hotspot, case, cycle_id="cycle_1")
    second = build_disk_handoff_request(hotspot, case, cycle_id="cycle_2")
    reopened = build_disk_handoff_request(
        hotspot,
        case.model_copy(update={"resolved_at": "2026-07-02T00:00:00+00:00"}),
        cycle_id="cycle_3",
    )

    assert first.handoff.idempotency_key == second.handoff.idempotency_key
    assert first.handoff.idempotency_key != reopened.handoff.idempotency_key
    assert ":v2:" in first.handoff.idempotency_key
