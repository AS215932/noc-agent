from app.model_metrics import (
    metrics_response,
    record_case_service_outbox_processed,
    record_case_service_shadow_failure,
    record_case_service_shadow_observation,
    set_case_service_runtime_enabled,
)


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
