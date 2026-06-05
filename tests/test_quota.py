import httpx

from app.config import load_settings
from app.quota import DEFAULT_GEMINI_QUOTA_USAGE_METRIC_TYPE, _build_usage_filter, check_openrouter_credits


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


def test_openrouter_key_credit_probe_success(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.delenv("OPENROUTER_MANAGEMENT_API_KEY", raising=False)
    monkeypatch.setattr("app.quota._OPENROUTER_CACHE", None)

    def fake_get(url, **_kwargs):
        return httpx.Response(
            200,
            json={
                "data": {
                    "label": "noc-agent",
                    "limit": 50,
                    "limit_remaining": 42.5,
                    "limit_reset": None,
                    "usage": 7.5,
                    "usage_daily": 0.5,
                    "usage_weekly": 2,
                    "usage_monthly": 7.5,
                    "is_free_tier": False,
                }
            },
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    status = check_openrouter_credits(load_settings())

    assert status.status == "ok"
    openrouter = status.providers["openrouter"]
    assert openrouter["key"]["limit_remaining"] == 42.5
    assert openrouter["account"]["status"] == "not_configured"


def test_openrouter_key_credit_probe_degrades_on_auth_failure(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.setattr("app.quota._OPENROUTER_CACHE", None)

    def fake_get(url, **_kwargs):
        return httpx.Response(401, json={"error": {"message": "bad key"}}, request=httpx.Request("GET", url))

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    status = check_openrouter_credits(load_settings())

    assert status.status == "degraded"
    assert "authentication" in status.providers["openrouter"]["key"]["message"]
    assert "test-openrouter-key" not in str(status.health_value())


def test_openrouter_key_credit_probe_degrades_on_timeout(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.setattr("app.quota._OPENROUTER_CACHE", None)

    def fake_get(*_args, **_kwargs):
        raise httpx.TimeoutException("slow")

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    status = check_openrouter_credits(load_settings())

    assert status.status == "degraded"
    assert "timed out" in status.providers["openrouter"]["key"]["message"]


def test_openrouter_key_credit_probe_degrades_on_low_remaining_limit(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.setattr("app.quota._OPENROUTER_CACHE", None)

    def fake_get(url, **_kwargs):
        return httpx.Response(
            200,
            json={"data": {"limit": 10, "limit_remaining": 0.5, "usage": 9.5}},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    status = check_openrouter_credits(load_settings())

    assert status.status == "degraded"
    assert status.providers["openrouter"]["key"]["limit_remaining"] == 0.5


def test_openrouter_key_credit_probe_treats_null_limit_remaining_as_unlimited(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.setattr("app.quota._OPENROUTER_CACHE", None)

    def fake_get(url, **_kwargs):
        return httpx.Response(
            200,
            json={"data": {"limit": None, "limit_remaining": None, "usage": 1.25}},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    status = check_openrouter_credits(load_settings())

    assert status.status == "ok"
    assert status.providers["openrouter"]["key"]["limit_remaining"] is None


def test_openrouter_account_credit_probe_uses_management_key(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.setenv("OPENROUTER_MANAGEMENT_API_KEY", "test-management-key")
    monkeypatch.setattr("app.quota._OPENROUTER_CACHE", None)

    def fake_get(url, **_kwargs):
        if url.endswith("/credits"):
            return httpx.Response(
                200,
                json={"data": {"total_credits": 100, "total_usage": 25}},
                request=httpx.Request("GET", url),
            )
        return httpx.Response(
            200,
            json={"data": {"limit": 50, "limit_remaining": 40, "usage": 10}},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    status = check_openrouter_credits(load_settings())

    assert status.status == "ok"
    assert status.providers["openrouter"]["account"]["total_credits"] == 100
    assert status.providers["openrouter"]["account"]["remaining"] == 75
