from types import SimpleNamespace

import pytest

import app.main as main


def _req():
    return SimpleNamespace(headers={})


@pytest.mark.asyncio
async def test_status_ack_unack_flow(monkeypatch, tmp_path):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    monkeypatch.setenv("NOC_PROACTIVE_STATE_DIR", str(tmp_path))

    # status exposes the dashboard data (hotspots + suppressions), loop not running
    status = await main.control_proactive_status(_req(), "secret", None)
    assert status["hotspots"] == []  # no loop/last_report in this test
    assert status["suppressions"] == {}

    # ack a hotspot by id
    acked = await main.control_proactive_ack(
        main.ProactiveAckRequest(fingerprint="abc123def456", reason="tracked in #268", operator="svag"),
        _req(),
        "secret",
        None,
    )
    assert acked["suppressed"]["fingerprint"] == "abc123def456"

    status2 = await main.control_proactive_status(_req(), "secret", None)
    assert "abc123def456" in status2["suppressions"]

    # unack clears it
    unacked = await main.control_proactive_unack(
        main.ProactiveUnackRequest(fingerprint="abc123def456"), _req(), "secret", None
    )
    assert unacked["removed"] is True
    status3 = await main.control_proactive_status(_req(), "secret", None)
    assert status3["suppressions"] == {}


@pytest.mark.asyncio
async def test_control_endpoints_require_token(monkeypatch):
    monkeypatch.setenv("NOC_CONTROL_TOKEN", "secret")
    from fastapi import HTTPException

    with pytest.raises(HTTPException):
        await main.control_proactive_status(_req(), "wrong-token", None)
