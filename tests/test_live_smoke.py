import os
from urllib.error import URLError
from urllib.request import urlopen

import pytest


pytestmark = pytest.mark.skipif(
    os.getenv("NOC_AGENT_LIVE_SMOKE") != "1",
    reason="Set NOC_AGENT_LIVE_SMOKE=1 to run read-only live NOC smoke tests.",
)


def _get_json(path: str) -> dict:
    base_url = os.getenv("NOC_AGENT_LIVE_BASE_URL", "http://[2a0c:b641:b50:2::a0]:8000")
    try:
        with urlopen(f"{base_url.rstrip('/')}{path}", timeout=10) as response:
            assert response.status == 200
            import json

            return json.loads(response.read().decode())
    except URLError as exc:
        pytest.fail(f"live NOC agent smoke endpoint unavailable: {exc}")


def test_live_noc_agent_health():
    payload = _get_json("/health")
    assert payload["status"] == "ok"


def test_live_noc_agent_mcp_health():
    payload = _get_json("/health/mcp")
    assert payload["status"] == "ok"


def test_live_noc_agent_config_health():
    payload = _get_json("/health/config")
    assert payload["status"] == "ok"
