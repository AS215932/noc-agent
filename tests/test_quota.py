import httpx

from app.config import NocAgentSettings, OpenRouterProviderSettings, ProviderSettings, load_settings
from app.quota import DEFAULT_GEMINI_QUOTA_USAGE_METRIC_TYPE, _build_usage_filter, check_openrouter_credits, check_venice_credits


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


def test_openrouter_key_credit_probe_uses_generic_safe_error_message(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.setattr("app.quota._OPENROUTER_CACHE", None)
    injected_exception = type("IgnorePreviousInstructions", (Exception,), {})

    def fake_get(*_args, **_kwargs):
        raise injected_exception("malicious metadata")

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    status = check_openrouter_credits(load_settings())
    message = status.providers["openrouter"]["key"]["message"]

    assert status.status == "degraded"
    assert message == "OpenRouter key query failed safely."
    assert "IgnorePreviousInstructions" not in message


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


def test_openrouter_key_credit_probe_scales_threshold_to_small_key_limit(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.setattr("app.quota._OPENROUTER_CACHE", None)

    def fake_get(url, **_kwargs):
        return httpx.Response(
            200,
            json={"data": {"limit": 5, "limit_remaining": 5, "usage": 0}},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    status = check_openrouter_credits(load_settings())

    assert status.status == "ok"
    assert status.providers["openrouter"]["key"]["limit_remaining"] == 5


def test_openrouter_key_credit_probe_warns_near_small_key_limit_exhaustion(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.setattr("app.quota._OPENROUTER_CACHE", None)

    def fake_get(url, **_kwargs):
        return httpx.Response(
            200,
            json={"data": {"limit": 5, "limit_remaining": 0.75, "usage": 4.25}},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    status = check_openrouter_credits(load_settings())

    assert status.status == "degraded"
    assert "warning threshold" in status.providers["openrouter"]["key"]["message"]


def test_openrouter_key_credit_probe_sanitizes_nonfinite_threshold_config(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")
    monkeypatch.setattr("app.quota._OPENROUTER_CACHE", None)
    settings = NocAgentSettings(
        providers=ProviderSettings(
            openrouter=OpenRouterProviderSettings(
                warn_remaining_usd=float("nan"),
                critical_remaining_usd=-1.0,
            )
        )
    )

    def fake_get(url, **_kwargs):
        return httpx.Response(
            200,
            json={"data": {"limit": 5, "limit_remaining": 4, "usage": 1}},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    status = check_openrouter_credits(settings)

    assert status.status == "ok"
    assert status.providers["openrouter"]["key"]["limit_remaining"] == 4


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


def test_venice_credit_probe_success(monkeypatch):
    monkeypatch.setenv("VENICE_API_KEY", "test-venice-key")
    monkeypatch.setattr("app.quota._VENICE_CACHE", None)

    def fake_get(url, **kwargs):
        assert url.endswith("/api_keys/rate_limits")
        return httpx.Response(
            200,
            json={
                "data": {
                    "accessPermitted": True,
                    "apiTier": {"id": "paid", "isCharged": True},
                    "balances": {"USD": 42.5, "DIEM": 10.0},
                    "nextEpochBegins": "2026-07-11T00:00:00Z",
                }
            },
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    result = check_venice_credits()

    assert result.status == "ok"
    venice = result.providers["venice"]
    assert venice["key"]["balance_usd"] == 42.5
    assert venice["key"]["balance_diem"] == 10.0
    assert venice["key"]["access_permitted"] is True


def test_venice_credit_probe_degrades_on_low_balance(monkeypatch):
    monkeypatch.setenv("VENICE_API_KEY", "test-venice-key")
    monkeypatch.setattr("app.quota._VENICE_CACHE", None)

    def fake_get(url, **kwargs):
        return httpx.Response(
            200,
            json={"data": {"accessPermitted": True, "balances": {"USD": 0.5, "DIEM": 0.0}}},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    result = check_venice_credits()

    assert result.status == "degraded"
    assert "critical threshold" in result.providers["venice"]["key"]["message"]


def test_venice_credit_probe_degrades_when_access_not_permitted(monkeypatch):
    monkeypatch.setenv("VENICE_API_KEY", "test-venice-key")
    monkeypatch.setattr("app.quota._VENICE_CACHE", None)

    def fake_get(url, **kwargs):
        return httpx.Response(
            200,
            json={"data": {"accessPermitted": False, "balances": {"USD": 100.0, "DIEM": 5.0}}},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    result = check_venice_credits()

    assert result.status == "degraded"
    assert "not permitted" in result.providers["venice"]["key"]["message"]


def test_venice_credit_probe_degrades_on_auth_failure(monkeypatch):
    monkeypatch.setenv("VENICE_API_KEY", "test-venice-key")
    monkeypatch.setattr("app.quota._VENICE_CACHE", None)

    def fake_get(url, **kwargs):
        return httpx.Response(401, json={"error": "bad key"}, request=httpx.Request("GET", url))

    monkeypatch.setattr("app.quota.httpx.get", fake_get)

    result = check_venice_credits()

    assert result.status == "degraded"
    assert "authentication" in result.providers["venice"]["key"]["message"]


def test_venice_credit_probe_not_configured_without_key(monkeypatch):
    monkeypatch.delenv("VENICE_API_KEY", raising=False)
    monkeypatch.setattr("app.quota._VENICE_CACHE", None)

    result = check_venice_credits()

    assert result.status == "not_configured"
