from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from app.cases import (
    AtomicCaseProjection,
    CaseEvent,
    CaseIdentityAlias,
    CasePolicy,
    CaseService,
    CorrelationService,
    InMemoryCaseStore,
    MetaCaseProjection,
    ObservationRecord,
    OutboxIntent,
    observation_from_hotspot,
    observation_identity_fingerprint,
    event_fingerprint_from_parts,
    stable_signal_signature,
)
from app.config import ProactiveLoopSettings
from app.proactive.models import Hotspot, HotspotEvidence


def test_observation_signature_is_stable_and_clean_is_explicit():
    left = {"b": [2, 1], "a": {"router": "r1", "down": True}}
    right = {"a": {"down": True, "router": "r1"}, "b": [2, 1]}

    assert stable_signal_signature(left) == stable_signal_signature(right)

    unhealthy = ObservationRecord(status="firing", signal_snapshot=left, source_health="healthy")
    clean = ObservationRecord(status="clean", signal_snapshot=right, source_health="healthy")
    degraded_clean = ObservationRecord(status="clean", signal_snapshot=right, source_health="degraded")

    assert unhealthy.signal_signature == clean.signal_signature
    assert not unhealthy.is_positive_clean
    assert clean.is_positive_clean
    assert not degraded_clean.is_positive_clean


def test_observation_from_hotspot_normalizes_proactive_evidence():
    hotspot = Hotspot(
        rule_id="disk_fill",
        key="log:/var",
        category="disk",
        severity="HIGH",
        score=99.0,
        title="Disk almost full",
        resource="log",
        summary="/var has little free space",
        evidence=[HotspotEvidence(label="free", value="4%", threshold="<10%")],
        recommended_checks=["check filesystem growth"],
        suggested_specialist="infrastructure",
        warrants_change=True,
    )

    observation = observation_from_hotspot(hotspot, cycle_id="cyc_test", source_health="healthy")

    assert observation.source == "proactive"
    assert observation.source_fingerprint == hotspot.fingerprint()
    assert observation.dedup_key == f"proactive:cyc_test:{hotspot.fingerprint()}"
    assert observation.detector == "disk_fill"
    assert observation.status == "firing"
    assert observation.signal_snapshot["evidence"][0]["value"] == "4%"
    assert observation.signal_signature


def test_meta_case_projection_counts_children_and_notifications():
    meta = MetaCaseProjection(
        child_case_ids=["case_a", "case_b"],
        notification_ids=["notif_1"],
        correlation_confidence=0.8,
    )

    assert meta.kind == "meta"
    assert meta.child_case_count == 2
    assert meta.notification_count == 1


@pytest.mark.asyncio
async def test_in_memory_store_round_trips_copies_and_events():
    store = InMemoryCaseStore()
    case = AtomicCaseProjection(case_id="case_1", fingerprint="fp", signal_snapshot={"x": 1})

    stored = await store.upsert_case(case)
    stored.title = "mutated copy"

    again = await store.get_case("case_1")
    assert again is not None
    assert again.title == ""
    assert again.signal_signature == stable_signal_signature({"x": 1})

    event = await store.append_event(CaseEvent(case_id="case_1", event_type="case_observed_unhealthy"))
    events = await store.case_events("case_1")
    assert [item.event_id for item in events] == [event.event_id]
    events[0].payload["mutated"] = True
    assert (await store.case_events("case_1"))[0].payload == {}


@pytest.mark.asyncio
async def test_identity_aliases_are_unique_for_active_case_mapping():
    store = InMemoryCaseStore()
    await store.record_alias(CaseIdentityAlias(case_id="case_1", alias_type="hotspot_fp", alias_value="abc123"))

    assert await store.resolve_alias("hotspot_fp", "abc123") == "case_1"
    same = await store.record_alias(CaseIdentityAlias(case_id="case_1", alias_type="hotspot_fp", alias_value="abc123"))
    assert same.case_id == "case_1"

    with pytest.raises(ValueError):
        await store.record_alias(CaseIdentityAlias(case_id="case_2", alias_type="hotspot_fp", alias_value="abc123"))


@pytest.mark.asyncio
async def test_outbox_idempotency_returns_original_intent():
    store = InMemoryCaseStore()
    first = await store.enqueue_outbox(
        OutboxIntent(case_id="case_1", intent_type="report", idempotency_key="report:case_1:sig")
    )
    second = await store.enqueue_outbox(
        OutboxIntent(
            case_id="case_1",
            intent_type="report",
            idempotency_key="report:case_1:sig",
            payload={"new": "payload must not replace the original"},
        )
    )

    assert second.outbox_id == first.outbox_id
    assert second.payload == {}
    assert len(await store.list_outbox(status="pending")) == 1


def test_outbox_intent_requires_target_and_idempotency_key():
    with pytest.raises(ValidationError):
        OutboxIntent(intent_type="report", idempotency_key="report:no-target")
    with pytest.raises(ValidationError):
        OutboxIntent(case_id="case_1", intent_type="report", idempotency_key="")


def test_case_policy_can_seed_from_existing_proactive_settings():
    settings = ProactiveLoopSettings(report_reassert_s=123, investigation_cooldown_s=456, auto_snooze_ttl_s=789)

    policy = CasePolicy.from_proactive_settings(settings, policy_version="test_policy")

    assert policy.policy_version == "test_policy"
    assert policy.report_reassert_s == 123
    assert policy.investigation_cooldown_s == 456
    assert policy.reinvestigate_stale_s == 456
    assert policy.auto_snooze_ttl_s == 789


@pytest.mark.asyncio
async def test_case_service_observe_unhealthy_creates_case_aliases_and_events():
    store = InMemoryCaseStore()
    service = CaseService(store, policy=CasePolicy(policy_version="test_policy"))
    observation = ObservationRecord(
        source="proactive",
        rule_id="disk_fill",
        detector="disk_fill",
        resource="noc",
        service="disk",
        severity="HIGH",
        status="firing",
        scan_cycle_id="cyc_1",
        signal_snapshot={"free_pct": 4.2},
    )

    result = await service.observe(observation)

    assert result.action == "created"
    assert result.case is not None
    assert result.case.origin == "proactive"
    assert result.case.status == "investigating"
    assert result.case.last_observed_unhealthy == observation.observed_at
    assert result.case.signal_signature == stable_signal_signature({"free_pct": 4.2})
    assert await store.resolve_alias("rule_entity", observation_identity_fingerprint(observation)) == result.case.case_id
    assert [event.event_type for event in await store.case_events(result.case.case_id)] == [
        "case_created",
        "case_observed_unhealthy",
    ]


@pytest.mark.asyncio
async def test_case_service_resolves_only_on_source_healthy_positive_clean():
    store = InMemoryCaseStore()
    service = CaseService(store)
    firing = ObservationRecord(
        source="icinga2",
        detector="noc-agent-uptime",
        resource="noc",
        severity="MEDIUM",
        status="firing",
        signal_snapshot={"state": "WARNING"},
        source_health="healthy",
    )
    created = await service.observe(firing)
    assert created.case is not None

    degraded_clean = ObservationRecord(
        source="icinga2",
        detector="noc-agent-uptime",
        resource="noc",
        severity="LOW",
        status="clean",
        signal_snapshot={"state": "OK"},
        source_health="degraded",
    )
    degraded_result = await service.observe(degraded_clean)
    assert degraded_result.action == "updated"
    assert degraded_result.case is not None
    assert degraded_result.case.status == "investigating"
    assert degraded_result.events[0].event_type == "case_observed_clean"

    healthy_clean = ObservationRecord(
        source="icinga2",
        detector="noc-agent-uptime",
        resource="noc",
        severity="LOW",
        status="clean",
        signal_snapshot={"state": "OK"},
        source_health="healthy",
    )
    resolved = await service.observe(healthy_clean)
    assert resolved.action == "resolved_positive_clean"
    assert resolved.case is not None
    assert resolved.case.status == "resolved"
    assert resolved.case.resolution_reason == "positive_clean_observation"
    assert resolved.events[0].event_type == "case_resolved_positive_clean"


@pytest.mark.asyncio
async def test_case_service_reopens_resolved_case_on_new_unhealthy_observation():
    store = InMemoryCaseStore()
    service = CaseService(store)
    firing = ObservationRecord(source="proactive", rule_id="tls_expiry", resource="web", status="firing")
    created = await service.observe(firing)
    assert created.case is not None
    await service.observe(
        ObservationRecord(source="proactive", rule_id="tls_expiry", resource="web", status="clean", source_health="healthy")
    )

    reopened = await service.observe(
        ObservationRecord(
            source="proactive",
            rule_id="tls_expiry",
            resource="web",
            status="firing",
            signal_snapshot={"days": 3},
        )
    )

    assert reopened.action == "updated"
    assert reopened.case is not None
    assert reopened.case.case_id == created.case.case_id
    assert reopened.case.status == "investigating"
    assert reopened.events[0].event_type in {"case_observed_unhealthy", "case_signal_changed"}


@pytest.mark.asyncio
async def test_case_service_report_intent_is_outbox_idempotent():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(source="proactive", rule_id="bgp", resource="r1", status="firing"))
    assert created.case is not None

    first = await service.request_report(created.case, state_signature="sig_1", payload={"body": "first"})
    second = await service.request_report(created.case, state_signature="sig_1", payload={"body": "second"})

    assert second.outbox_id == first.outbox_id
    assert second.payload == {"body": "first"}
    assert len(await store.list_outbox()) == 1


@pytest.mark.asyncio
async def test_correlation_service_creates_meta_case_and_attaches_children():
    store = InMemoryCaseStore()
    case_service = CaseService(store)
    correlation = CorrelationService(store, policy=CasePolicy(storm_confidence_threshold=0.7))
    first = await case_service.observe(
        ObservationRecord(source="icinga2", detector="router-down", resource="r1", status="firing", severity="HIGH")
    )
    second = await case_service.observe(
        ObservationRecord(source="icinga2", detector="customer-reachability", resource="cust-a", status="firing")
    )
    assert first.case is not None and second.case is not None

    result = await correlation.create_meta_case(
        title="Router R1 cascade",
        event_type="router_down",
        correlation_reason="same site burst after router-down",
        correlation_confidence=0.9,
        child_case_ids=[first.case.case_id, second.case.case_id],
    )

    assert result.meta_case.status == "active_event"
    assert set(result.meta_case.child_case_ids) == {first.case.case_id, second.case.case_id}
    stored_first = await store.get_case(first.case.case_id)
    stored_second = await store.get_case(second.case.case_id)
    assert isinstance(stored_first, AtomicCaseProjection)
    assert isinstance(stored_second, AtomicCaseProjection)
    assert stored_first.meta_case_id == result.meta_case.case_id
    assert stored_first.covered_by_meta_case is True
    assert stored_second.meta_case_id == result.meta_case.case_id
    event_types = [event.event_type for event in await store.case_events(result.meta_case.case_id)]
    assert event_types.count("meta_case_created") == 1
    assert event_types.count("child_case_attached_to_meta_case") == 2


@pytest.mark.asyncio
async def test_correlation_service_blocks_two_active_meta_parents_for_one_child():
    store = InMemoryCaseStore()
    case_service = CaseService(store)
    correlation = CorrelationService(store)
    child = await case_service.observe(ObservationRecord(source="proactive", rule_id="bgp", resource="r1", status="firing"))
    assert child.case is not None
    first = await correlation.create_meta_case(
        title="First event",
        correlation_reason="test",
        correlation_confidence=1.0,
        child_case_ids=[child.case.case_id],
    )
    second = await correlation.create_meta_case(
        title="Second event",
        correlation_reason="test",
        correlation_confidence=1.0,
    )

    with pytest.raises(ValueError):
        await correlation.attach_child(second.meta_case.case_id, child.case.case_id, reason="bad grouping", confidence=1.0)

    stored_child = await store.get_case(child.case.case_id)
    assert isinstance(stored_child, AtomicCaseProjection)
    assert stored_child.meta_case_id == first.meta_case.case_id


@pytest.mark.asyncio
async def test_correlation_service_detach_and_independent_action_resurface_child():
    store = InMemoryCaseStore()
    case_service = CaseService(store)
    correlation = CorrelationService(store)
    child = await case_service.observe(ObservationRecord(source="proactive", rule_id="disk_fill", resource="log", status="firing"))
    assert child.case is not None
    meta = await correlation.create_meta_case(
        title="Disk storm",
        correlation_reason="same host symptoms",
        correlation_confidence=1.0,
        child_case_ids=[child.case.case_id],
    )

    independent = await correlation.mark_independent_action_required(
        child.case.case_id,
        required=True,
        reason="disk remains full after event stabilized",
        actor_id="operator-1",
    )
    assert independent.independent_action_required is True
    assert independent.covered_by_meta_case is False

    detached = await correlation.detach_child(
        meta.meta_case.case_id,
        child.case.case_id,
        reason="operator split",
        actor_id="operator-1",
    )
    assert detached.meta_case.child_case_ids == []
    stored_child = await store.get_case(child.case.case_id)
    assert isinstance(stored_child, AtomicCaseProjection)
    assert stored_child.meta_case_id == ""
    assert stored_child.covered_by_meta_case is False
    assert [event.event_type for event in await store.case_events(child.case.case_id)][-2:] == [
        "child_case_independent_action_required_set",
        "child_case_detached_from_meta_case",
    ]


def test_event_fingerprint_from_parts_is_deterministic():
    assert event_fingerprint_from_parts("event", ["b", "a"]) == event_fingerprint_from_parts("event", ["a", "b"])


@pytest.mark.asyncio
async def test_case_service_investigation_reuse_is_case_grounded():
    store = InMemoryCaseStore()
    service = CaseService(store, policy=CasePolicy(investigation_cooldown_s=3600))
    created = await service.observe(
        ObservationRecord(source="proactive", rule_id="disk_fill", resource="log", status="firing", signal_snapshot={"free": 4})
    )
    assert created.case is not None
    assert service.should_investigate(created.case)

    investigated = await service.record_investigation_result(
        created.case.case_id,
        diagnosis={"root_cause": "disk growth"},
        recommendations=["clean old logs"],
    )
    assert not service.should_investigate(investigated)
    assert investigated.last_diagnosis["root_cause"] == "disk growth"
    assert investigated.diagnosis_signature == investigated.signal_signature

    changed = await service.observe(
        ObservationRecord(source="proactive", rule_id="disk_fill", resource="log", status="firing", signal_snapshot={"free": 2})
    )
    assert changed.case is not None
    assert service.should_investigate(changed.case)
    assert [event.event_type for event in await store.case_events(changed.case.case_id)][-1] == "case_signal_changed"


@pytest.mark.asyncio
async def test_case_service_suppression_ttl_lives_on_case():
    store = InMemoryCaseStore()
    service = CaseService(store, policy=CasePolicy(suppression_default_ttl_s=60))
    created = await service.observe(ObservationRecord(source="proactive", rule_id="tls_expiry", resource="web", status="firing"))
    assert created.case is not None

    suppressed = await service.suppress(
        created.case.case_id,
        reason="known maintenance",
        source="operator",
        operator="alice",
        ttl_seconds=1,
    )
    assert suppressed.suppression_source == "operator"
    assert suppressed.suppression_reason == "known maintenance"
    assert suppressed.suppressed_until

    not_expired = await service.expire_suppression(suppressed.case_id)
    assert not_expired.suppressed_until
    expired = await service.expire_suppression(
        suppressed.case_id,
        now=datetime.now(timezone.utc) + timedelta(seconds=2),
    )
    assert expired.suppressed_until == ""
    assert expired.suppression_reason == ""
    assert [event.event_type for event in await store.case_events(suppressed.case_id)][-2:] == [
        "case_suppressed",
        "case_suppression_expired",
    ]


@pytest.mark.asyncio
async def test_case_service_ack_records_operator_on_case():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(source="icinga2", detector="uptime", resource="noc", status="firing"))
    assert created.case is not None

    acked = await service.ack(created.case.case_id, operator="alice")

    assert acked.acknowledged_by == "alice"
    assert acked.acknowledged_at
    assert [event.event_type for event in await store.case_events(acked.case_id)][-1] == "case_acknowledged"


@pytest.mark.asyncio
async def test_case_service_handoff_idempotency_uses_case_issue_url():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(source="proactive", rule_id="bgp", resource="r1", status="firing"))
    assert created.case is not None

    first = await service.handoff_intent(created.case.case_id, payload={"title": "BGP issue"})
    second = await service.handoff_intent(created.case.case_id, payload={"title": "BGP issue duplicate"})
    assert first is not None
    assert second is not None
    assert second.outbox_id == first.outbox_id
    assert len(await store.list_outbox(status="pending")) == 1

    handed = await service.record_handoff_result(created.case.case_id, issue_url="https://github.invalid/issues/1", issue_id="1")
    assert handed.issue_url == "https://github.invalid/issues/1"
    skipped = await service.handoff_intent(created.case.case_id, payload={"title": "must not create"})
    assert skipped is None
    assert len(await store.list_outbox(status="pending")) == 1
    assert [event.event_type for event in await store.case_events(created.case.case_id)][-2:] == [
        "handoff_created_issue",
        "handoff_skipped_existing_issue",
    ]


@pytest.mark.asyncio
async def test_correlation_service_detects_same_site_router_down_cascade():
    store = InMemoryCaseStore()
    case_service = CaseService(store)
    correlation = CorrelationService(store, policy=CasePolicy(storm_confidence_threshold=0.75))
    observations = [
        ObservationRecord(
            source="alertmanager",
            detector="RouterDown",
            resource="r1",
            site="ams",
            service="network",
            status="firing",
            severity="HIGH",
            annotations={"summary": "router down"},
        ),
        ObservationRecord(
            source="alertmanager",
            detector="CustomerReachability",
            resource="cust-a",
            site="ams",
            service="network",
            status="firing",
            severity="MEDIUM",
            annotations={"summary": "downstream reachability failed"},
        ),
    ]
    for observation in observations:
        await case_service.observe(observation)

    result = await correlation.correlate_observations(observations)

    assert result is not None
    assert result.action == "created"
    assert result.meta_case.status == "active_event"
    assert result.meta_case.event_type == "router_down"
    assert result.meta_case.correlation_reason == "shared_site"
    assert len(result.meta_case.child_case_ids) == 2


@pytest.mark.asyncio
async def test_correlation_service_does_not_group_unrelated_simultaneous_alerts():
    store = InMemoryCaseStore()
    case_service = CaseService(store)
    correlation = CorrelationService(store)
    observations = [
        ObservationRecord(source="alertmanager", detector="RouterDown", resource="r1", site="ams", status="firing"),
        ObservationRecord(source="alertmanager", detector="DiskFull", resource="log", site="fra", status="firing"),
    ]
    for observation in observations:
        await case_service.observe(observation)

    assert await correlation.correlate_observations(observations) is None


@pytest.mark.asyncio
async def test_case_service_operator_feedback_attaches_to_case_and_event_log():
    from app.cases import OperatorFeedback

    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(source="proactive", rule_id="bgp", resource="r1", status="firing"))
    assert created.case is not None
    feedback = OperatorFeedback(
        case_id=created.case.case_id,
        actor_id="alice",
        actor_role="incident_commander",
        feedback_type="operator_confirmed_diagnosis",
        payload={"label": "correct"},
    )

    stored = await service.record_operator_feedback(feedback)
    updated = await store.get_case(created.case.case_id)

    assert stored.feedback_id == feedback.feedback_id
    assert len(await store.list_feedback(case_id=created.case.case_id)) == 1
    assert isinstance(updated, AtomicCaseProjection)
    assert updated.feedback_ids == [feedback.feedback_id]
    assert [event.event_type for event in await store.case_events(created.case.case_id)][-1] == "operator_feedback_recorded"


@pytest.mark.asyncio
async def test_case_service_trace_records_attach_to_case_for_replay():
    from app.cases import TraceRecord

    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(source="proactive", rule_id="bgp", resource="r1", status="firing"))
    assert created.case is not None
    trace = TraceRecord(
        case_id=created.case.case_id,
        cycle_id="cyc_1",
        trace_type="knowledge_retrieval",
        policy_version="policy_v1",
        knowledge_export_version="export_v1",
        payload={"citations": []},
    )

    stored = await service.record_trace(trace)
    traces = await store.list_traces(case_id=created.case.case_id)
    updated = await store.get_case(created.case.case_id)

    assert stored.trace_id == trace.trace_id
    assert [item.trace_id for item in traces] == [trace.trace_id]
    assert isinstance(updated, AtomicCaseProjection)
    assert updated.trace_ids == [trace.trace_id]


@pytest.mark.asyncio
async def test_case_service_report_signature_and_reassertion_are_case_owned():
    store = InMemoryCaseStore()
    service = CaseService(store, policy=CasePolicy(report_reassert_s=3600))
    created = await service.observe(
        ObservationRecord(source="proactive", rule_id="disk_fill", resource="log", status="firing", signal_snapshot={"free": 4})
    )
    assert created.case is not None
    signature = service.report_state_signature(created.case)
    assert service.should_report(created.case)

    reported = await service.mark_reported(created.case.case_id, state_signature=signature)
    assert not service.should_report(reported, now=datetime.now(timezone.utc) + timedelta(seconds=10))
    assert service.should_report(reported, now=datetime.now(timezone.utc) + timedelta(seconds=3700))

    changed = await service.observe(
        ObservationRecord(source="proactive", rule_id="disk_fill", resource="log", status="firing", signal_snapshot={"free": 2})
    )
    assert changed.case is not None
    assert service.report_state_signature(changed.case) != signature
    assert service.should_report(changed.case)

    reasserted = await service.mark_reported(
        changed.case.case_id,
        state_signature=service.report_state_signature(changed.case),
        reasserted=True,
    )
    assert reasserted.last_reasserted_at
    assert [event.event_type for event in await store.case_events(changed.case.case_id)][-1] == "case_reasserted"


@pytest.mark.asyncio
async def test_case_service_request_report_uses_case_signature_idempotency():
    store = InMemoryCaseStore()
    service = CaseService(store)
    created = await service.observe(ObservationRecord(source="proactive", rule_id="bgp", resource="r1", status="firing"))
    assert created.case is not None

    first = await service.request_report(created.case, payload={"body": "first"})
    second = await service.request_report(created.case, payload={"body": "second"})

    assert first.state_signature == service.report_state_signature(created.case)
    assert second.outbox_id == first.outbox_id
    assert second.payload == {"body": "first"}
