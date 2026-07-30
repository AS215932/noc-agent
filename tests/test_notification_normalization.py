import pytest

from app.cases import (
    CaseService,
    InMemoryCaseStore,
    observation_from_icinga_alert_payload,
    observations_from_alertmanager,
)


def test_alertmanager_payload_normalizes_each_alert_with_citations_ready_fields():
    payload = {
        "source": "alertmanager",
        "status": "firing",
        "receiver": "noc",
        "groupKey": "{}:{alertname=RouterDown}",
        "externalURL": "https://am.example.invalid",
        "commonLabels": {"severity": "critical", "site": "ams", "notification_route": "network"},
        "commonAnnotations": {"summary": "router down cascade"},
        "alerts": [
            {
                "status": "firing",
                "fingerprint": "am-fp-1",
                "labels": {"alertname": "RouterDown", "host": "r1", "service": "bgp"},
                "annotations": {"description": "BGP sessions dropping"},
                "startsAt": "2026-06-18T12:00:00Z",
                "generatorURL": "https://prom.example.invalid/graph",
            },
            {
                "status": "resolved",
                "labels": {"alertname": "CustomerReachability", "instance": "cust-a"},
                "annotations": {},
                "startsAt": "2026-06-18T12:01:00Z",
            },
        ],
    }

    observations = observations_from_alertmanager(payload)

    assert len(observations) == 2
    first, second = observations
    assert first.source == "alertmanager"
    assert first.source_event_id == "am-fp-1"
    assert first.source_fingerprint == "am-fp-1"
    assert first.detector == "RouterDown"
    assert first.resource == "r1"
    assert first.service == "bgp"
    assert first.site == "ams"
    assert first.severity == "HIGH"
    assert first.notification_route == "network"
    assert first.status == "firing"
    assert first.payload_ref == "https://prom.example.invalid/graph"
    assert first.signal_snapshot["groupKey"] == "{}:{alertname=RouterDown}"
    assert second.status == "resolved"
    assert second.is_positive_clean


def test_icinga_alert_payload_normalizes_problem_and_recovery():
    problem = {
        "source": "icinga2",
        "status": "firing",
        "commonLabels": {"host": "noc", "service": "noc-agent-uptime", "check_command": "uptime"},
        "commonAnnotations": {"summary": "WARNING: uptime low"},
        "alerts": [
            {
                "labels": {
                    "alertname": "noc-agent-uptime",
                    "host": "noc",
                    "service": "noc-agent-uptime",
                    "state": "WARNING",
                },
                "annotations": {"summary": "WARNING: uptime low"},
                "status": "WARNING",
            }
        ],
    }
    recovery = {
        **problem,
        "status": "resolved",
        "alerts": [{**problem["alerts"][0], "status": "OK", "labels": {**problem["alerts"][0]["labels"], "state": "OK"}}],
    }

    problem_obs = observation_from_icinga_alert_payload(problem)
    recovery_obs = observation_from_icinga_alert_payload(recovery)

    assert problem_obs.status == "firing"
    assert problem_obs.severity == "MEDIUM"
    assert problem_obs.detector == "noc-agent-uptime"
    assert problem_obs.resource == "noc"
    assert recovery_obs.status == "resolved"
    assert recovery_obs.severity == "LOW"
    assert recovery_obs.source_fingerprint == problem_obs.source_fingerprint
    assert recovery_obs.is_positive_clean


def test_notification_routes_ai_and_ci_without_model_classification():
    ai = observation_from_icinga_alert_payload(
        {
            "source": "icinga2",
            "status": "firing",
            "commonLabels": {"host": "noc", "service": "noc-agent-model-health"},
            "alerts": [{"labels": {"host": "noc", "service": "noc-agent-model-health", "state": "WARNING"}}],
            "tags": {"notification_route": "ai"},
        }
    )
    ci = observations_from_alertmanager(
        {
            "source": "alertmanager",
            "status": "firing",
            "receiver": "noc",
            "groupKey": "ci",
            "commonLabels": {"severity": "warning", "notification_route": "ci"},
            "commonAnnotations": {},
            "alerts": [{"status": "firing", "labels": {"alertname": "PipelineFailed"}}],
        }
    )[0]

    assert ai.notification_route == "ai"
    assert ci.notification_route == "ci"


@pytest.mark.asyncio
async def test_case_service_resolves_from_normalized_resolved_notification():
    store = InMemoryCaseStore()
    service = CaseService(store)
    firing = observation_from_icinga_alert_payload(
        {
            "source": "icinga2",
            "status": "firing",
            "commonLabels": {"host": "noc", "service": "noc-agent-uptime"},
            "commonAnnotations": {"summary": "WARNING"},
            "alerts": [{"labels": {"host": "noc", "service": "noc-agent-uptime", "state": "WARNING"}}],
        }
    )
    resolved = observation_from_icinga_alert_payload(
        {
            "source": "icinga2",
            "status": "resolved",
            "commonLabels": {"host": "noc", "service": "noc-agent-uptime"},
            "commonAnnotations": {"summary": "OK"},
            "alerts": [{"labels": {"host": "noc", "service": "noc-agent-uptime", "state": "OK"}}],
        }
    )

    created = await service.observe(firing)
    result = await service.observe(resolved)

    assert created.case is not None
    assert result.action == "resolved_positive_clean"
    assert result.case is not None
    assert result.case.case_id == created.case.case_id
    assert result.case.status == "resolved"
