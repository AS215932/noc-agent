import asyncio
from types import SimpleNamespace

import pytest

from app.agent import ActionPlan
from app.discord_bot import NOCDiscordBot


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

    async def reply(self, content, **kwargs):
        self.messages.append((content, kwargs))


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
        assert payload["commonAnnotations"]["summary"] == "investigate noc health"
        return _action_plan(), {"incident_id": "inc-mention"}

    monkeypatch.delenv("DISCORD_ALLOWED_ROLE_IDS", raising=False)
    monkeypatch.setattr("app.discord_bot.run_investigation_graph", fake_graph)
    bot = NOCDiscordBot()
    bot.client._connection.user = SimpleNamespace(id=123)
    message = FakeMessage()

    await bot.handle_investigation_message(message)
    assert "Investigation accepted" in message.messages[0][0]
    task = next(iter(bot._tasks))
    await task

    assert any("inc-mention" in content for content, _ in message.messages)
