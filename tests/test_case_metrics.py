from types import SimpleNamespace

from app.model_metrics import (
    metrics_response,
    model_result_metadata,
    record_case_service_outbox_processed,
    record_case_service_shadow_failure,
    record_case_service_shadow_observation,
    record_lhp_case_resolved,
    record_lhp_handoff_request,
    record_lhp_handoff_update,
    record_lhp_handoff_verified,
    record_lhp_knowledge_event,
    record_lhp_verification_result,
    record_fallback_attempt,
    record_success,
    set_case_service_runtime_enabled,
    start_run,
)


class _ModelResult:
    def new_messages(self):
        return [SimpleNamespace(model_name="openrouter:unit-secondary")]

    def usage(self):
        return None


def test_case_service_metrics_are_exported():
    set_case_service_runtime_enabled(True, backend="UnitBackend")
    record_case_service_shadow_observation(path="unit", source="test_source", status="firing", action="created")
    record_case_service_shadow_failure(path="unit", category="unit_failure")
    record_case_service_outbox_processed(intent_type="report_case", outcome="succeeded")

    body, content_type = metrics_response()
    text = body.decode()

    assert "text/plain" in content_type
    assert 'noc_agent_case_service_runtime_enabled{backend="UnitBackend"} 1.0' in text
    assert (
        'noc_agent_case_service_shadow_observations_total{action="created",path="unit",source="test_source",status="firing"}'
        in text
    )
    assert 'noc_agent_case_service_shadow_failures_total{category="unit_failure",path="unit"}' in text
    assert 'noc_agent_case_service_outbox_processed_total{intent_type="report_case",outcome="succeeded"}' in text

    set_case_service_runtime_enabled(False, backend="UnitBackend")


def test_lhp_metrics_are_exported():
    record_lhp_handoff_request(target_loop="engineering", case_type="proactive_disk_condition", outcome="created")
    record_lhp_handoff_update(
        source_loop="engineering",
        update_type="implemented",
        status="implemented",
        outcome="created",
    )
    record_lhp_verification_result(objective_type="prometheus_query", status="pass")
    record_lhp_handoff_verified(target_loop="engineering")
    record_lhp_case_resolved(case_type="proactive_disk_condition")
    record_lhp_knowledge_event(kind="artifact_proposed", outcome="enqueued")

    body, _ = metrics_response()
    text = body.decode()

    assert (
        'noc_agent_lhp_handoff_requests_total{case_type="proactive_disk_condition",outcome="created",'
        'target_loop="engineering"}' in text
    )
    assert (
        'noc_agent_lhp_handoff_updates_total{outcome="created",source_loop="engineering",status="implemented",'
        'update_type="implemented"}' in text
    )
    assert 'noc_agent_lhp_verification_results_total{objective_type="prometheus_query",status="pass"}' in text
    assert 'noc_agent_lhp_handoffs_verified_total{target_loop="engineering"}' in text
    assert 'noc_agent_lhp_cases_resolved_total{case_type="proactive_disk_condition"}' in text
    assert 'noc_agent_lhp_knowledge_events_total{kind="artifact_proposed",outcome="enqueued"}' in text


def test_graph_model_metadata_records_selected_fallback_success():
    started = start_run("unit-graph-triage")
    record_fallback_attempt("openrouter:unit-primary", "invalid_model_output")
    model_name, fallback_from = model_result_metadata(_ModelResult())

    record_success(
        "unit-graph-triage",
        started,
        None,
        model_name=model_name,
        fallback_from=fallback_from,
    )

    body, _ = metrics_response()
    text = body.decode()
    assert (
        'noc_agent_model_run_successes_total{agent="unit-graph-triage",model="openrouter:unit-secondary"} 1.0'
        in text
    )
    assert (
        'noc_agent_model_fallback_successes_total{from_model="openrouter:unit-primary",'
        'to_model="openrouter:unit-secondary"} 1.0' in text
    )
