import asyncio
from types import SimpleNamespace

import pytest

from app.agent import ActionPlan
from app.discord_bot import NOCDiscordBot, StatusOverview, parse_discord_operator_request, run_fast_status_check
from app.mcp_runtime import MCPRuntime


class FakeResponse:
    def __init__(self):
        self.deferred = False
        self.messages = []

    async def defer(self, **kwargs):
        self.deferred = True
        self.defer_kwargs = kwargs

    async def send_message(self, content, **kwargs):
        self.messages.append((content, kwargs))


class FakeFollowup:
    def __init__(self):
        self.messages = []

    async def send(self, content, **kwargs):
        self.messages.append((content, kwargs))


class FakeInteraction:
    def __init__(self):
        self.response = FakeResponse()
        self.followup = FakeFollowup()
        self.user = SimpleNamespace(id=42, roles=[])
        self.guild = SimpleNamespace(id=1)
        self.channel = SimpleNamespace(id=2)


class FakeMessage:
    def __init__(self, content="<@123> investigate noc health"):
        self.content = content
        self.author = SimpleNamespace(id=42, bot=False, roles=[])
        self.guild = SimpleNamespace(id=1)
        self.channel = SimpleNamespace(id=2)
        self.messages = []
        self.threads = []

    async def reply(self, content, **kwargs):
        self.messages.append((content, kwargs))

    async def create_thread(self, name):
        thread = FakeThread(name)
        self.threads.append(thread)
        return thread


class FakeThread:
    def __init__(self, name):
        self.name = name
        self.messages = []

    async def send(self, content, **kwargs):
        self.messages.append((content, kwargs))


class FakeMCPSession:
    def __init__(self):
        self.calls = []

    async def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        payload = '{"stdout":"Neighbor        AS MsgRcvd MsgSent State/PfxRcd\\n2a0c::1 215932 10 12 42","stderr":"","exit_code":0}'
        return SimpleNamespace(content=[SimpleNamespace(text=payload)])


class FakeRuntime:
    def __init__(self):
        self.session = FakeMCPSession()
        self.clients = {"hyrule": SimpleNamespace(session=self.session)}

    def health(self):
        return {
            "status": "ok",
            "hyrule_tool_count": 27,
            "xo_tool_count": 8,
            "hyrule": True,
            "xo": True,
        }


def _action_plan() -> ActionPlan:
    return ActionPlan(
        issue_summary="NOC health looks degraded",
        root_cause_analysis="The agent found a degraded check.",
        confidence_score=0.7,
        severity="MEDIUM",
        requires_human=True,
        human_escalation_reason="Review diagnostics before remediation.",
        diagnostic_evidence=["health/mcp degraded"],
        tools_used=["health/mcp"],
        operator_next_steps=["Check MCP daemon logs."],
    )


@pytest.mark.parametrize(
    ("text", "kind", "target", "incident_id", "decision"),
    [
        ("status noc", "status", "noc", "", ""),
        ("status bgp cr1-nl1", "status", "cr1-nl1", "", ""),
        ("status bgp peers cr1-de1", "status", "cr1-de1", "", ""),
        ("status bgp cr1.de1?", "status", "cr1-de1", "", ""),
        ("what is the status of cr1.de1?", "status", "cr1-de1", "", ""),
        ("what's the status of cr1-nl1?", "status", "cr1-nl1", "", ""),
        ("how is noc doing?", "status", "noc", "", ""),
        ("is cr1.de1 healthy?", "status", "cr1-de1", "", ""),
        ("check bgp on cr1-nl1", "status", "cr1-nl1", "", ""),
        ("check bgp on cr1.de1?", "status", "cr1-de1", "", ""),
        ("investigate packet loss to ns2", "investigate", "packet loss to ns2", "", ""),
        ("pending", "pending", "", "", ""),
        ("show pending", "pending", "", "", ""),
        ("status inc-abc123?", "incident_status", "", "inc-abc123", ""),
        ("show incident incident-1", "incident_status", "", "incident-1", ""),
        ("approve inc-abc looks good", "decision", "", "inc-abc", "approved"),
        ("reject inc-abc hold", "decision", "", "inc-abc", "rejected"),
        ("wat", "help", "", "", ""),
    ],
)
def test_parse_discord_operator_request(text, kind, target, incident_id, decision):
    intent = parse_discord_operator_request(text)

    assert intent.type == kind
    assert intent.target == target
    assert intent.incident_id == incident_id
    assert intent.decision == decision


def test_parse_bgp_status_adds_check_qualifier():
    intent = parse_discord_operator_request("status bgp peers cr1-de1")

    assert intent.type == "status"
    assert intent.target == "cr1-de1"
    assert intent.qualifiers == {"check": "bgp peers"}


@pytest.mark.asyncio
async def test_bot_start_uses_shared_mcp_runtime(monkeypatch):
    calls = []

    class FakeRuntime:
        async def connect_tools(self, agent):
            calls.append("connect")

        async def disconnect(self):
            calls.append("disconnect")

    async def fake_start(token):
        calls.append(("start", token))

    monkeypatch.setenv("DISCORD_BOT_TOKEN", "token")
    bot = NOCDiscordBot()
    assert isinstance(bot._mcp_runtime, MCPRuntime)
    bot._mcp_runtime = FakeRuntime()
    monkeypatch.setattr(bot.client, "start", fake_start)

    await bot.start()

    assert calls == ["connect", ("start", "token"), "disconnect"]


@pytest.mark.asyncio
async def test_noc_investigate_sends_acceptance_and_final_followup(monkeypatch):
    async def fake_graph(payload):
        return _action_plan(), {"incident_id": "inc-1"}

    monkeypatch.delenv("DISCORD_ALLOWED_ROLE_IDS", raising=False)
    monkeypatch.setattr("app.discord_bot.run_investigation_graph", fake_graph)
    bot = NOCDiscordBot()
    interaction = FakeInteraction()

    await bot.handle_investigation_interaction(interaction, "check noc health")
    assert interaction.response.deferred is True
    assert "Investigation accepted" in interaction.followup.messages[0][0]

    task = next(iter(bot._tasks))
    await task

    assert any("inc-1" in message for message, _ in interaction.followup.messages)


@pytest.mark.asyncio
async def test_noc_investigate_reports_graph_exception(monkeypatch):
    async def fake_graph(payload):
        raise RuntimeError("provider exploded with details")

    monkeypatch.delenv("DISCORD_ALLOWED_ROLE_IDS", raising=False)
    monkeypatch.setattr("app.discord_bot.run_investigation_graph", fake_graph)
    bot = NOCDiscordBot()
    interaction = FakeInteraction()

    await bot.handle_investigation_interaction(interaction, "check noc health")
    task = next(iter(bot._tasks))
    await task

    assert any("Discord investigation" in message for message, _ in interaction.followup.messages)


@pytest.mark.asyncio
async def test_noc_investigate_reports_timeout(monkeypatch):
    async def fake_graph(payload):
        await asyncio.sleep(1)

    monkeypatch.delenv("DISCORD_ALLOWED_ROLE_IDS", raising=False)
    monkeypatch.setattr("app.discord_bot.run_investigation_graph", fake_graph)
    bot = NOCDiscordBot()
    bot.investigation_timeout_s = 0.01
    interaction = FakeInteraction()

    await bot.handle_investigation_interaction(interaction, "check noc health")
    task = next(iter(bot._tasks))
    await task

    assert any("timed out" in message for message, _ in interaction.followup.messages)


@pytest.mark.asyncio
async def test_mention_investigation_uses_same_safe_runner(monkeypatch):
    async def fake_graph(payload):
        assert payload["source"] == "discord-mention"
        assert payload["commonAnnotations"]["summary"] == "noc health"
        return _action_plan(), {"incident_id": "inc-mention"}

    monkeypatch.delenv("DISCORD_ALLOWED_ROLE_IDS", raising=False)
    monkeypatch.setattr("app.discord_bot.run_investigation_graph", fake_graph)
    bot = NOCDiscordBot()
    bot.client._connection.user = SimpleNamespace(id=123)
    message = FakeMessage()

    await bot.handle_investigation_message(message)
    assert "Investigation accepted" in message.messages[0][0]
    assert message.threads
    task = next(iter(bot._tasks))
    await task

    assert any("inc-mention" in content for content, _ in message.threads[0].messages)


@pytest.mark.asyncio
async def test_status_mention_uses_fast_status_not_graph(monkeypatch):
    graph_called = False

    async def fake_graph(payload):
        nonlocal graph_called
        graph_called = True
        return _action_plan(), {"incident_id": "inc-unexpected"}

    async def fake_status(target, qualifiers, runtime):
        assert target == "noc"
        return StatusOverview(status="ok", target=target, summary="NOC is healthy.", checks=["MCP health: ok"])

    monkeypatch.delenv("DISCORD_ALLOWED_ROLE_IDS", raising=False)
    monkeypatch.setattr("app.discord_bot.run_investigation_graph", fake_graph)
    monkeypatch.setattr("app.discord_bot.run_fast_status_check", fake_status)
    bot = NOCDiscordBot()
    bot.client._connection.user = SimpleNamespace(id=123)
    message = FakeMessage("<@123> status noc")

    await bot.handle_operator_message(message)

    assert graph_called is False
    assert "Status for `noc`: `ok`" in message.messages[0][0]
    assert not message.threads


@pytest.mark.asyncio
async def test_bgp_status_uses_frr_summary_not_generic_host_lookup():
    runtime = FakeRuntime()

    overview = await run_fast_status_check("cr1-nl1", {"check": "bgp"}, runtime)

    assert overview.status == "ok"
    assert "BGP status" in overview.summary
    assert "show bgp summary" in overview.checks[0]
    assert runtime.session.calls == [
        ("frr_vtysh_cmd", {"host": "cr1-nl1", "command": "show bgp summary"})
    ]


@pytest.mark.asyncio
async def test_degraded_status_mention_posts_thread(monkeypatch):
    async def fake_status(target, qualifiers, runtime):
        return StatusOverview(
            status="degraded",
            target=target,
            summary="MCP is degraded.",
            checks=["XO tools unavailable."],
            suggested_next_action='Reply with "investigate noc" to run the full investigation.',
        )

    monkeypatch.delenv("DISCORD_ALLOWED_ROLE_IDS", raising=False)
    monkeypatch.setattr("app.discord_bot.run_fast_status_check", fake_status)
    bot = NOCDiscordBot()
    bot.client._connection.user = SimpleNamespace(id=123)
    message = FakeMessage("<@123> status noc")

    await bot.handle_operator_message(message)

    assert "Details are in the thread" in message.messages[0][0]
    assert message.threads
    assert "Status for `noc`: `degraded`" in message.threads[0].messages[0][0]


@pytest.mark.asyncio
async def test_unknown_mention_returns_help_and_starts_no_work(monkeypatch):
    async def fake_graph(payload):
        raise AssertionError("graph should not run")

    monkeypatch.delenv("DISCORD_ALLOWED_ROLE_IDS", raising=False)
    monkeypatch.setattr("app.discord_bot.run_investigation_graph", fake_graph)
    bot = NOCDiscordBot()
    bot.client._connection.user = SimpleNamespace(id=123)
    message = FakeMessage("<@123> something strange")

    await bot.handle_operator_message(message)

    assert "status noc" in message.messages[0][0]
    assert not bot._tasks


@pytest.mark.asyncio
async def test_unauthorized_mention_rejects_before_work(monkeypatch):
    async def fake_status(target, qualifiers, runtime):
        raise AssertionError("status should not run")

    monkeypatch.setenv("DISCORD_ALLOWED_ROLE_IDS", "99")
    monkeypatch.setattr("app.discord_bot.run_fast_status_check", fake_status)
    bot = NOCDiscordBot()
    bot.client._connection.user = SimpleNamespace(id=123)
    message = FakeMessage("<@123> status noc")

    await bot.handle_operator_message(message)

    assert message.messages == [("Not authorized.", {})]


@pytest.mark.asyncio
async def test_decision_mention_records_operator_decision(monkeypatch):
    calls = []

    def fake_decision(incident_id, decision):
        calls.append((incident_id, decision))
        return {"incident_id": incident_id, "status": "approved", "title": "Done"}

    monkeypatch.delenv("DISCORD_ALLOWED_ROLE_IDS", raising=False)
    monkeypatch.setattr("app.discord_bot.record_operator_decision", fake_decision)
    bot = NOCDiscordBot()
    bot.client._connection.user = SimpleNamespace(id=123)
    message = FakeMessage("<@123> approve inc-1 looks safe")

    await bot.handle_operator_message(message)

    assert calls[0][0] == "inc-1"
    assert calls[0][1]["decision"] == "approved"
    assert calls[0][1]["comment"] == "looks safe"
    assert "Recorded `approved`" in message.messages[0][0]
