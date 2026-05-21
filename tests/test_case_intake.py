import asyncio

import pytest

from app.incident_memory import IncidentMemory, case_display_title, fingerprint_alert


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


def test_fingerprint_ignores_state_and_output_changes():
    warning = _icinga_alert("WARNING", "WARNING: uptime_seconds=556 (<1800)")
    critical = _icinga_alert("CRITICAL", "CRITICAL: uptime_seconds=258 (<300)")

    assert fingerprint_alert(warning) == fingerprint_alert(critical)


@pytest.mark.asyncio
async def test_case_title_uses_case_number_without_raw_dict():
    memory = IncidentMemory(redis_url="")

    result = await memory.intake_alert(_icinga_alert("WARNING"))
    title = case_display_title(result.case, result.event)

    assert title.startswith("NOC-")
    assert title.endswith(": noc-agent-uptime on noc")
    assert "{" not in title


@pytest.mark.asyncio
async def test_concurrent_identical_alerts_allocate_one_case():
    memory = IncidentMemory(redis_url="")
    alerts = [
        _icinga_alert("WARNING", f"WARNING: uptime_seconds={500 + idx} (<1800)")
        for idx in range(8)
    ]

    results = await asyncio.gather(*(memory.intake_alert(alert) for alert in alerts))

    assert [result.action for result in results].count("created") == 1
    assert len({result.case["case_number"] for result in results if result.case}) == 1
    case = results[0].case
    if results[0].action != "created":
        case = next(result.case for result in results if result.action == "created")
    stored = await memory.get_case(case["incident_id"])
    events = await memory.case_events(case["incident_id"])
    assert stored["event_count"] == 8
    assert len(events) == 8


@pytest.mark.asyncio
async def test_warning_to_critical_attaches_and_records_transition():
    memory = IncidentMemory(redis_url="")

    first = await memory.intake_alert(_icinga_alert("WARNING", "WARNING: uptime_seconds=556 (<1800)"))
    second = await memory.intake_alert(_icinga_alert("CRITICAL", "CRITICAL: uptime_seconds=258 (<300)"))

    assert first.action == "created"
    assert second.action == "escalated"
    assert second.case["incident_id"] == first.case["incident_id"]
    assert second.case["latest_transition"] == {
        "from": "WARNING",
        "to": "CRITICAL",
        "meaning": "severity escalation for same issue",
    }
    assert second.should_investigate is False


@pytest.mark.asyncio
async def test_recovered_pending_reopens_same_case_during_cooldown():
    memory = IncidentMemory(redis_url="")

    created = await memory.intake_alert(_icinga_alert("CRITICAL"))
    recovered = await memory.intake_alert(_icinga_alert("OK", "OK: uptime recovered"))
    reopened = await memory.intake_alert(_icinga_alert("WARNING", "WARNING: uptime_seconds=700 (<1800)"))

    assert recovered.action == "recovered"
    assert recovered.case["status"] == "recovered_pending"
    assert reopened.action == "reopened"
    assert reopened.case["incident_id"] == created.case["incident_id"]
    assert reopened.case["case_number"] == created.case["case_number"]
    assert reopened.case["status"] == "investigating"


@pytest.mark.asyncio
async def test_resolved_case_releases_fingerprint_for_future_case():
    memory = IncidentMemory(redis_url="")

    created = await memory.intake_alert(_icinga_alert("CRITICAL"))
    await memory.update_case(created.case["incident_id"], {"status": "resolved"})
    future = await memory.intake_alert(_icinga_alert("WARNING", "WARNING: uptime_seconds=700 (<1800)"))

    assert future.action == "created"
    assert future.case["incident_id"] != created.case["incident_id"]
    assert future.case["case_number"] != created.case["case_number"]


@pytest.mark.asyncio
async def test_topology_parent_case_receives_downstream_victim():
    memory = IncidentMemory(redis_url="")
    await memory.set_topology_parents("noc", ["vm:noc"])
    await memory.set_topology_parents("vm:noc", ["xcpng:host:xoa"])
    parent = await memory.intake_alert(
        {
            "source": "icinga2",
            "status": "firing",
            "groupLabels": {"alertname": "xcpng-host-memory", "host": "xcpng:host:xoa"},
            "alerts": [{"labels": {"alertname": "xcpng-host-memory", "host": "xcpng:host:xoa", "state": "CRITICAL"}}],
        }
    )

    child = await memory.intake_alert(_icinga_alert("CRITICAL"))

    assert parent.action == "created"
    assert child.action == "linked_parent"
    assert child.should_investigate is False
    assert child.case["event_count"] == 1
    assert child.parent_case["incident_id"] == parent.case["incident_id"]
    assert "noc" in child.parent_case["downstream_victims"]


@pytest.mark.asyncio
async def test_reactive_link_routes_future_child_events_to_parent():
    memory = IncidentMemory(redis_url="")
    child = await memory.intake_alert(_icinga_alert("CRITICAL"))
    parent = await memory.intake_alert(
        {
            "source": "icinga2",
            "status": "firing",
            "groupLabels": {"alertname": "xcpng-host-memory", "host": "xcpng:host:xoa"},
            "alerts": [{"labels": {"alertname": "xcpng-host-memory", "host": "xcpng:host:xoa", "state": "CRITICAL"}}],
        }
    )

    linked = await memory.link_to_parent_case(child.case["incident_id"], parent.case["incident_id"], "same host pressure", ["ev1"])
    future = await memory.intake_alert(_icinga_alert("WARNING", "WARNING: uptime_seconds=800 (<1800)"))

    assert linked["ok"] is True
    assert future.case["incident_id"] == parent.case["incident_id"]
    assert "noc" in future.case["downstream_victims"]
