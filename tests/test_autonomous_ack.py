from dataclasses import replace

import pytest

import app.main as main
from app import icinga_ack
from app.config import ProactiveLoopSettings
from app.proactive import loop as loop_module
from app.proactive.loop import ProactiveLoop
from app.proactive.models import Hotspot
from app.proactive.suppressions import SuppressionStore


# --- the self-signing ack helper -----------------------------------------


def test_sign_ack_authorization_is_signed(monkeypatch):
    monkeypatch.setenv("HYRULE_MCP_ACTION_SIGNING_SECRET", "sign-me")
    auth = icinga_ack.sign_ack_authorization(case_id="NOC-1", operator="noc-agent")
    assert auth["action_class"] == "acknowledge_icinga"
    assert auth["case_id"] == "NOC-1"
    assert auth["expiry"] > 0 and auth["signature"]


class _FakeRuntime:
    def __init__(self, problems=None):
        self.calls = []
        self._problems = problems or {"problems": []}

    async def call_tool(self, source, name, args):
        self.calls.append((source, name, args))
        if name == "icinga_list_problems":
            return self._problems
        return {"ok": True}


@pytest.mark.asyncio
async def test_acknowledge_icinga_sends_expiry_and_notify():
    rt = _FakeRuntime()
    res = await icinga_ack.acknowledge_icinga(
        rt, host_name="rtr", service_name="disk", comment="x", ack_ttl_seconds=3600, notify=False
    )
    assert res == {"ok": True}
    _, name, args = rt.calls[0]
    assert name == "icinga_acknowledge_alert"
    assert args["host_name"] == "rtr" and args["service_name"] == "disk"
    assert args["notify"] is False and args["expiry"] > 0
    assert args["action_authorization"]["action_class"] == "acknowledge_icinga"


@pytest.mark.asyncio
async def test_acknowledge_icinga_noop_without_host():
    rt = _FakeRuntime()
    assert await icinga_ack.acknowledge_icinga(rt, host_name="", service_name=None, comment="x") is None
    assert rt.calls == []


# --- take-ownership ack on investigation start ----------------------------


def _icinga_payload(host="noc", service="noc-agent-uptime"):
    return {
        "source": "icinga2",
        "commonLabels": {"host": host, "service": service},
        "groupLabels": {"host": host, "alertname": service},
    }


@pytest.mark.asyncio
async def test_take_ownership_ack_on_icinga_alert(monkeypatch):
    monkeypatch.setenv("NOC_AUTO_ACK_ON_INVESTIGATION", "1")
    captured = {}

    async def fake_ack(runtime, **kw):
        captured.update(kw)
        return {"ok": True}

    monkeypatch.setattr(main, "acknowledge_icinga", fake_ack)
    await main._take_ownership_ack(_icinga_payload(), {"incident_id": "i1", "case_number": "NOC-7"}, _FakeRuntime())
    assert captured["host_name"] == "noc" and captured["service_name"] == "noc-agent-uptime"
    assert captured["ack_ttl_seconds"] >= 60 and captured["notify"] is False
    assert "NOC-7" in captured["comment"]


@pytest.mark.asyncio
async def test_take_ownership_ack_skips_alertmanager_and_when_disabled(monkeypatch):
    calls = []

    async def fake_ack(runtime, **kw):
        calls.append(kw)

    monkeypatch.setattr(main, "acknowledge_icinga", fake_ack)

    # flag off → no ack even for an icinga alert
    monkeypatch.setenv("NOC_AUTO_ACK_ON_INVESTIGATION", "0")
    await main._take_ownership_ack(_icinga_payload(), {}, _FakeRuntime())
    # flag on but alertmanager-sourced → no Icinga object to ack
    monkeypatch.setenv("NOC_AUTO_ACK_ON_INVESTIGATION", "1")
    await main._take_ownership_ack({"source": "alertmanager", "commonLabels": {"host": "x"}}, {}, _FakeRuntime())
    assert calls == []


# --- proactive auto-snooze ------------------------------------------------


def _settings(tmp_path, **kw):
    base = ProactiveLoopSettings(enabled=True, state_dir=str(tmp_path), memory_dir=str(tmp_path / "m"))
    return replace(base, **kw)


def _low():
    return Hotspot(rule_id="tls_expiry", key="proxy:cert", category="tls", severity="LOW", title="Cert expiring soon", resource="proxy")


def _high():
    return Hotspot(rule_id="disk_fill", key="rtr:/", category="disk", severity="HIGH", title="Disk low", resource="rtr")


@pytest.mark.asyncio
async def test_auto_snooze_mutes_low_not_high(tmp_path, monkeypatch):
    monkeypatch.setattr(loop_module, "send_discord_notification", _anoop)
    store = SuppressionStore(tmp_path / "s.json")
    lp = ProactiveLoop(
        _FakeRuntime(),
        settings=_settings(tmp_path, auto_snooze_enabled=True, auto_snooze_icinga_ack=False),
        suppressions=store,
        model_chain=lambda: ["m"],
    )
    low, high = _low(), _high()
    snoozed = await lp._auto_snooze([high, low])
    assert snoozed == ["proxy:cert"]  # LOW snoozed, HIGH left alone
    active = store.active()
    assert low.fingerprint() in active and high.fingerprint() not in active
    assert active[low.fingerprint()]["operator"] == "agent"
    assert active[low.fingerprint()]["expires_at"] is not None  # TTL'd
    # second pass: already snoozed → not re-snoozed
    assert await lp._auto_snooze([high, low]) == []


@pytest.mark.asyncio
async def test_auto_snooze_disabled_is_noop(tmp_path):
    store = SuppressionStore(tmp_path / "s.json")
    lp = ProactiveLoop(_FakeRuntime(), settings=_settings(tmp_path, auto_snooze_enabled=False), suppressions=store, model_chain=lambda: ["m"])
    assert await lp._auto_snooze([_low()]) == []
    assert store.active() == {}


@pytest.mark.asyncio
async def test_auto_snooze_respects_per_cycle_cap(tmp_path, monkeypatch):
    monkeypatch.setattr(loop_module, "send_discord_notification", _anoop)
    store = SuppressionStore(tmp_path / "s.json")
    lp = ProactiveLoop(
        _FakeRuntime(),
        settings=_settings(tmp_path, auto_snooze_enabled=True, auto_snooze_icinga_ack=False, auto_snooze_max_per_cycle=1),
        suppressions=store,
        model_chain=lambda: ["m"],
    )
    lows = [Hotspot(rule_id="tls_expiry", key=f"h{i}:c", category="tls", severity="LOW", title=f"c{i}", resource=f"h{i}") for i in range(3)]
    assert len(await lp._auto_snooze(lows)) == 1  # capped


@pytest.mark.asyncio
async def test_auto_snooze_acks_matching_icinga_warning(tmp_path, monkeypatch):
    monkeypatch.setattr(loop_module, "send_discord_notification", _anoop)
    acked = {}

    async def fake_ack(runtime, **kw):
        acked.update(kw)
        return {"ok": True}

    monkeypatch.setattr(loop_module, "acknowledge_icinga", fake_ack)
    # one WARNING (state 1) tls problem on proxy → confident match; a CRITICAL is ignored
    problems = {"problems": [
        {"name": "proxy!tls-cert", "host": "proxy", "state": 1.0},
        {"name": "rtr!disk", "host": "rtr", "state": 2.0},
    ]}
    store = SuppressionStore(tmp_path / "s.json")
    lp = ProactiveLoop(
        _FakeRuntime(problems=problems),
        settings=_settings(tmp_path, auto_snooze_enabled=True, auto_snooze_icinga_ack=True),
        suppressions=store,
        model_chain=lambda: ["m"],
    )
    await lp._auto_snooze([_low()])
    assert acked["host_name"] == "proxy" and acked["service_name"] == "tls-cert"
    assert acked["notify"] is False and acked["ack_ttl_seconds"] > 0


async def _anoop(*a, **k):
    return None
