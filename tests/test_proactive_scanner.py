import pytest

from app.config import ProactiveLoopSettings
from app.proactive import scanner
from app.proactive.scanner import ScanContext, parse_prom_vector


def _vector(*rows: tuple[dict, str]):
    return {"ok": True, "result": [{"metric": metric, "value": value} for metric, value in rows]}


class FakeMCPRuntime:
    """Routes prometheus_query calls to canned vectors by substring match."""

    def __init__(self, by_query: dict[str, dict] | None = None, *, raise_on: str | None = None):
        self.by_query = by_query or {}
        self.raise_on = raise_on
        self.calls: list[tuple[str, str, dict]] = []

    async def call_tool(self, source, name, arguments):
        self.calls.append((source, name, arguments))
        query = str(arguments.get("query", ""))
        if self.raise_on and self.raise_on in query:
            raise RuntimeError("boom")
        for needle, response in self.by_query.items():
            if needle in query:
                return response
        return {"ok": True, "result": []}


def _ctx(runtime) -> ScanContext:
    return ScanContext(mcp_runtime=runtime, settings=ProactiveLoopSettings())


# --- parser ---------------------------------------------------------------


def test_parse_prom_vector_scalar_and_pair_and_failure():
    scalar = _vector(({"instance": "x"}, "0.5"))
    assert parse_prom_vector(scalar)[0].value == 0.5

    paired = {"ok": True, "result": [{"metric": {"instance": "x"}, "value": [1718000000, "0.25"]}]}
    assert parse_prom_vector(paired)[0].value == 0.25

    nested = {"data": {"result": [{"metric": {}, "value": "3"}]}}
    assert parse_prom_vector(nested)[0].value == 3.0

    assert parse_prom_vector({"ok": False, "result": [{"metric": {}, "value": "1"}]}) == []
    assert parse_prom_vector("not a dict") == []


# --- rules ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_disk_fill_flags_low_and_soon():
    runtime = FakeMCPRuntime(
        {
            "node_filesystem_size_bytes": _vector(
                ({"instance": "[2a0c:b641:b50:2::b0]:9100", "mountpoint": "/var"}, "0.05"),
            ),
            "predict_linear": _vector(
                ({"instance": "[2a0c:b641:b50:2::b0]:9100", "mountpoint": "/var"}, "-1000"),
            ),
        }
    )
    hotspots = await scanner.rule_disk_fill(_ctx(runtime))
    assert len(hotspots) == 1
    hs = hotspots[0]
    assert hs.category == "disk"
    assert hs.severity == "HIGH"
    assert hs.resource == "2a0c:b641:b50:2::b0"
    assert hs.warrants_change is True


@pytest.mark.asyncio
async def test_disk_fill_ignores_healthy():
    runtime = FakeMCPRuntime(
        {
            "node_filesystem_size_bytes": _vector(
                ({"instance": "h:9100", "mountpoint": "/"}, "0.80"),
            ),
            "predict_linear": _vector(),
        }
    )
    assert await scanner.rule_disk_fill(_ctx(runtime)) == []


@pytest.mark.asyncio
async def test_bgp_risk_non_established_then_flap_escalates():
    runtime = FakeMCPRuntime(
        {
            'state!="Established"': _vector(
                ({"instance": "[2a0c:b641:b50::a]:9342", "peer": "2a0c:b640:8:69::ffff", "state": "Active"}, "1"),
            ),
            "changes(frr_bgp_peer_state[1h])": _vector(
                ({"instance": "[2a0c:b641:b50::a]:9342", "peer": "2a0c:b640:8:69::ffff"}, "6"),
            ),
        }
    )
    hotspots = await scanner.rule_bgp_risk(_ctx(runtime))
    assert len(hotspots) == 1
    hs = hotspots[0]
    assert hs.category == "bgp"
    assert hs.suggested_specialist == "bgp"
    assert hs.severity == "HIGH"  # not-established + flapping
    assert hs.warrants_change is True


@pytest.mark.asyncio
async def test_scrape_flap_and_service_churn_and_failed_unit():
    runtime = FakeMCPRuntime(
        {
            "changes(up[2h])": _vector(({"instance": "api:9100", "job": "node-infra"}, "9")),
            "increase(node_systemd_service_restart_total[2h])": _vector(
                ({"instance": "api:9100", "name": "hyrule-cloud"}, "6"),
                ({"instance": "api:9100", "name": "cloud-init-main.service"}, "5"),  # benign → ignored
            ),
            # The failed-unit query now requires a recent transition; the mock
            # only returns units that already satisfy it.
            "node_systemd_unit_state": _vector(
                ({"instance": "mon:9100", "name": "vector"}, "1"),
                ({"instance": "rtr:9100", "name": "unbound-resolvconf.service"}, "1"),  # benign → ignored
            ),
        }
    )
    scrape = await scanner.rule_scrape_flap(_ctx(runtime))
    assert scrape and scrape[0].severity == "HIGH" and scrape[0].category == "scrape"

    churn = await scanner.rule_service_churn(_ctx(runtime))
    units = {hs.key for hs in churn}
    # benign cloud-init / *-resolvconf units are filtered out
    assert units == {"api:hyrule-cloud", "mon:vector"}
    churn_hs = next(hs for hs in churn if hs.key == "api:hyrule-cloud")
    failed_hs = next(hs for hs in churn if hs.key == "mon:vector")
    assert churn_hs.severity == "HIGH"  # 6 restarts
    assert failed_hs.severity == "MEDIUM" and "failed state within the last 2h" in failed_hs.summary


def test_benign_unit_matcher_filters_known_noise():
    m = scanner._benign_unit_matcher()
    for unit in ("cloud-init-main.service", "cloud-init-network", "unbound-resolvconf.service", "openipmi", "cloud-final.service"):
        assert scanner._is_benign_unit(unit, m), unit
    for unit in ("hyrule-cloud", "vector.service", "apache2.service", "knot.service"):
        assert not scanner._is_benign_unit(unit, m), unit


def test_benign_unit_matcher_env_extension(monkeypatch):
    monkeypatch.setenv("NOC_PROACTIVE_IGNORE_UNITS", "apache2, foo.*")
    m = scanner._benign_unit_matcher()
    assert scanner._is_benign_unit("apache2.service", m)
    assert scanner._is_benign_unit("foobar", m)
    assert not scanner._is_benign_unit("hyrule-cloud", m)


@pytest.mark.asyncio
async def test_reachability_flap_partial_loss():
    runtime = FakeMCPRuntime(
        {
            'avg_over_time(probe_success{job="blackbox-icmp"}[30m])': _vector(
                ({"instance": "[2a0c:b641:b50:ff00::1]:0"}, "0.4"),
                ({"instance": "[2a0c:b641:b50:2::20]:0"}, "1.0"),  # healthy, ignored
            ),
        }
    )
    hotspots = await scanner.rule_reachability_flap(_ctx(runtime))
    assert len(hotspots) == 1
    assert hotspots[0].severity == "HIGH"
    assert hotspots[0].category == "wireguard"


@pytest.mark.asyncio
async def test_scan_dedup_and_orders_by_score_and_isolates_failures():
    # bgp non-established (240) plus a HIGH disk (360) on deep scan.
    runtime = FakeMCPRuntime(
        {
            'state!="Established"': _vector(
                ({"instance": "[2a0c:b641:b50::a]:9342", "peer": "p1", "state": "Idle"}, "1"),
            ),
            "node_filesystem_size_bytes": _vector(({"instance": "h:9100", "mountpoint": "/"}, "0.05")),
            "predict_linear": _vector(({"instance": "h:9100", "mountpoint": "/"}, "-1")),
        },
        raise_on="changes(up[2h])",  # scrape rule blows up; must be isolated
    )
    hotspots = await scanner.scan(_ctx(runtime), deep=True)
    assert [h.rule_id for h in hotspots[:2]] == ["disk_fill", "bgp_risk"]  # ordered by score
    assert all(isinstance(h.score, float) for h in hotspots)
