import asyncio

import pytest

from app.alert_utils import case_display_title, case_event_from_alert, fingerprint_alert
from app.cases import CaseService, InMemoryCaseStore
from app.cases.graph_memory import CaseServiceGraphMemory
from app.cases.notifications import observation_from_icinga_alert_payload


def _icinga_alert(state="WARNING", output="WARNING: uptime_seconds=556 (<1800)"):
    return {
        "source": "icinga2",
        "status": "firing" if state != "OK" else "resolved",
        "groupLabels": {"alertname": "noc-agent-uptime", "host": "noc"},
        "commonLabels": {"host": "noc", "service": "noc-agent-uptime", "check_command": "noc-agent-uptime"},
        "commonAnnotations": {"summary": output},
        "alerts": [
            {
                "status": state,
                "labels": {
                    "alertname": "noc-agent-uptime",
                    "host": "noc",
                    "service": "noc-agent-uptime",
                    "state": state,
                },
                "annotations": {"summary": output},
            }
        ],
    }


async def _observe(service: CaseService, alert: dict):
    return await service.observe(observation_from_icinga_alert_payload(alert))


def test_fingerprint_ignores_state_and_output_changes():
    warning = _icinga_alert("WARNING", "WARNING: uptime_seconds=556 (<1800)")
    critical = _icinga_alert("CRITICAL", "CRITICAL: uptime_seconds=258 (<300)")

    assert fingerprint_alert(warning) == fingerprint_alert(critical)


def test_case_title_uses_case_number_without_raw_dict():
    event = case_event_from_alert(_icinga_alert("WARNING"))
    title = case_display_title({"case_number": "NOC-20260621-001", "identity": event["identity"]}, event)

    assert title == "NOC-20260621-001: noc-agent-uptime on noc"
    assert "{" not in title


@pytest.mark.asyncio
async def test_concurrent_identical_alerts_allocate_one_case():
    store = InMemoryCaseStore()
    service = CaseService(store)
    alerts = [_icinga_alert("WARNING", f"WARNING: uptime_seconds={500 + idx} (<1800)") for idx in range(8)]

    results = await asyncio.gather(*(_observe(service, alert) for alert in alerts))

    case_ids = {result.case.case_id for result in results if result.case is not None}
    assert len(case_ids) == 1
    events = await store.case_events(next(iter(case_ids)))
    assert [event.event_type for event in events].count("case_created") == 1
    assert [event.event_type for event in events].count("case_observed_unhealthy") >= 1


@pytest.mark.asyncio
async def test_warning_to_critical_attaches_and_records_signal_change():
    store = InMemoryCaseStore()
    service = CaseService(store)

    first = await _observe(service, _icinga_alert("WARNING", "WARNING: uptime_seconds=556 (<1800)"))
    second = await _observe(service, _icinga_alert("CRITICAL", "CRITICAL: uptime_seconds=258 (<300)"))

    assert first.action == "created"
    assert second.action == "updated"
    assert second.case.case_id == first.case.case_id
    event_types = [event.event_type for event in await store.case_events(first.case.case_id)]
    assert "case_signal_changed" in event_types


@pytest.mark.asyncio
async def test_positive_recovery_resolves_case_and_recurrence_reopens_it():
    store = InMemoryCaseStore()
    service = CaseService(store)

    created = await _observe(service, _icinga_alert("CRITICAL"))
    recovered = await _observe(service, _icinga_alert("OK", "OK: uptime recovered"))
    reopened = await _observe(service, _icinga_alert("WARNING", "WARNING: uptime_seconds=700 (<1800)"))

    assert created.case is not None
    assert recovered.action == "resolved_positive_clean"
    assert recovered.case.case_id == created.case.case_id
    assert recovered.case.status == "resolved"
    assert reopened.case.case_id == created.case.case_id
    assert reopened.case.status == "investigating"


@pytest.mark.asyncio
async def test_case_service_graph_memory_links_child_to_parent_and_routes_alias():
    store = InMemoryCaseStore()
    service = CaseService(store)
    parent = await service.observe(
        observation_from_icinga_alert_payload(
            {
                "source": "icinga2",
                "status": "firing",
                "groupLabels": {"alertname": "xcpng-host-memory", "host": "xcpng:host:xoa"},
                "alerts": [{"labels": {"alertname": "xcpng-host-memory", "host": "xcpng:host:xoa", "state": "CRITICAL"}}],
            }
        )
    )
    child = await _observe(service, _icinga_alert("CRITICAL"))
    assert parent.case is not None
    assert child.case is not None
    graph_memory = CaseServiceGraphMemory(store)

    linked = await graph_memory.link_to_parent_case(child.case.case_id, parent.case.case_id, "same host pressure", ["ev1"])

    assert linked["ok"] is True
    assert linked["event_count"] == 2
    stored_child = await store.get_case(child.case.case_id)
    assert stored_child.status == "linked"
    assert stored_child.last_diagnosis["linked_parent_case"] == parent.case.case_id
    assert await store.resolve_alias("source_fp", child.observation.source_fingerprint) == parent.case.case_id
