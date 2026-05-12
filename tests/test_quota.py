from app.quota import DEFAULT_GEMINI_QUOTA_USAGE_METRIC_TYPE, _build_usage_filter


def test_build_usage_filter_uses_consumer_quota_metric_label():
    quota_filter = _build_usage_filter(
        usage_metric_type=DEFAULT_GEMINI_QUOTA_USAGE_METRIC_TYPE,
        service="generativelanguage.googleapis.com",
        quota_metric="generativelanguage.googleapis.com/generate_requests_per_model_per_day",
    )

    assert 'metric.type = "serviceruntime.googleapis.com/quota/rate/net_usage"' in quota_filter
    assert 'resource.type = "consumer_quota"' in quota_filter
    assert 'resource.label.service = "generativelanguage.googleapis.com"' in quota_filter
    assert (
        'metric.label.quota_metric = "generativelanguage.googleapis.com/generate_requests_per_model_per_day"'
        in quota_filter
    )


def test_build_usage_filter_escapes_monitoring_filter_values():
    quota_filter = _build_usage_filter(
        usage_metric_type='serviceruntime.googleapis.com/quota/rate/net_usage"',
        service='generativelanguage.googleapis.com"',
        quota_metric='generativelanguage.googleapis.com/generate_requests_per_model_per_day"',
    )

    assert '\\"' in quota_filter
