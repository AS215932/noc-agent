from __future__ import annotations

import os
from typing import Any, Awaitable, Callable

from app.graph_runtime import pending_summaries, record_operator_decision, run_investigation_graph, summary_for

try:  # pragma: no cover - import availability depends on runtime extras
    import discord
    from discord import app_commands
except Exception:  # pragma: no cover
    discord = None
    app_commands = None


Notifier = Callable[..., Awaitable[None]]


class NOCDiscordBot:
    def __init__(self):
        if discord is None or app_commands is None:
            raise RuntimeError("discord.py is not installed")
        intents = discord.Intents.default()
        intents.message_content = True
        self.client = discord.Client(intents=intents)
        self.tree = app_commands.CommandTree(self.client)
        self.channel_id = _optional_int("DISCORD_BOT_CHANNEL_ID")
        self.allowed_guilds = _csv_ints("DISCORD_ALLOWED_GUILD_IDS")
        self.allowed_channels = _csv_ints("DISCORD_ALLOWED_CHANNEL_IDS")
        self.allowed_roles = _csv_ints("DISCORD_ALLOWED_ROLE_IDS")
        self._register_handlers()

    async def start(self):
        token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
        if not token:
            return
        await self.client.start(token)

    async def send_embed(self, title: str, description: str, color: int, fields: list[dict[str, Any]] | None = None):
        if self.channel_id is None:
            return
        channel = self.client.get_channel(self.channel_id)
        if channel is None:
            return
        embed = discord.Embed(title=title, description=description, color=color)
        for field in fields or []:
            embed.add_field(
                name=str(field.get("name", "Field")),
                value=str(field.get("value", "")),
                inline=bool(field.get("inline", False)),
            )
        await channel.send(embed=embed)

    def _register_handlers(self) -> None:
        @self.client.event
        async def on_ready():
            await self.tree.sync()

        @self.tree.command(name="noc_pending", description="List pending NOC proposals.")
        async def noc_pending(interaction):
            if not self._authorized(interaction):
                await interaction.response.send_message("Not authorized.", ephemeral=True)
                return
            incidents = pending_summaries()
            if not incidents:
                await interaction.response.send_message("No pending NOC proposals.", ephemeral=True)
                return
            lines = [f"`{item['incident_id']}` {item['title']}" for item in incidents[:10]]
            await interaction.response.send_message("\n".join(lines), ephemeral=True)

        @self.tree.command(name="noc_investigate", description="Start an operator-requested NOC investigation.")
        async def noc_investigate(interaction, prompt: str):
            if not self._authorized(interaction):
                await interaction.response.send_message("Not authorized.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            payload = {
                "source": "discord-command",
                "status": "firing",
                "groupLabels": {"alertname": "Operator Investigation", "host": "manual"},
                "commonLabels": {"severity": "manual"},
                "commonAnnotations": {"summary": prompt},
                "alerts": [{"labels": {"alertname": "Operator Investigation", "host": "manual"}, "annotations": {"summary": prompt}}],
            }
            plan, state = await run_investigation_graph(payload)
            await interaction.followup.send(
                f"Investigation `{state['incident_id']}` is waiting for review: {plan.issue_summary}",
                ephemeral=True,
            )

        @self.tree.command(name="noc_status", description="Show one NOC incident.")
        async def noc_status(interaction, incident_id: str):
            if not self._authorized(interaction):
                await interaction.response.send_message("Not authorized.", ephemeral=True)
                return
            summary = summary_for(incident_id)
            await interaction.response.send_message(
                "Incident not found." if summary is None else f"`{incident_id}` {summary['status']}: {summary['title']}",
                ephemeral=True,
            )

        @self.tree.command(name="noc_decide", description="Approve, reject, or acknowledge a NOC proposal.")
        async def noc_decide(interaction, incident_id: str, decision: str, comment: str = ""):
            if not self._authorized(interaction):
                await interaction.response.send_message("Not authorized.", ephemeral=True)
                return
            operator = str(getattr(interaction.user, "id", "discord"))
            summary = record_operator_decision(
                incident_id,
                {
                    "incident_id": incident_id,
                    "decision": decision,
                    "operator": operator,
                    "comment": comment,
                },
            )
            await interaction.response.send_message(
                "Incident not found." if summary is None else f"Recorded `{decision}` for `{incident_id}`.",
                ephemeral=True,
            )

        @self.client.event
        async def on_message(message):
            if message.author.bot or self.client.user is None or self.client.user not in message.mentions:
                return
            if not self._message_authorized(message):
                await message.reply("Not authorized.")
                return
            payload = {
                "source": "discord-mention",
                "status": "firing",
                "groupLabels": {"alertname": "Operator Investigation", "host": "manual"},
                "commonLabels": {"severity": "manual"},
                "commonAnnotations": {"summary": message.content},
                "alerts": [{"labels": {"alertname": "Operator Investigation", "host": "manual"}, "annotations": {"summary": message.content}}],
            }
            plan, state = await run_investigation_graph(payload)
            await message.reply(
                f"Investigation `{state['incident_id']}` is waiting for review: {plan.issue_summary}"
            )

    def _authorized(self, interaction) -> bool:
        if self.allowed_guilds and getattr(interaction.guild, "id", None) not in self.allowed_guilds:
            return False
        if self.allowed_channels and getattr(interaction.channel, "id", None) not in self.allowed_channels:
            return False
        if not self.allowed_roles:
            return True
        roles = getattr(interaction.user, "roles", [])
        return any(getattr(role, "id", None) in self.allowed_roles for role in roles)

    def _message_authorized(self, message) -> bool:
        if self.allowed_guilds and getattr(message.guild, "id", None) not in self.allowed_guilds:
            return False
        if self.allowed_channels and getattr(message.channel, "id", None) not in self.allowed_channels:
            return False
        if not self.allowed_roles:
            return True
        return any(getattr(role, "id", None) in self.allowed_roles for role in getattr(message.author, "roles", []))


def build_bot() -> NOCDiscordBot | None:
    if not os.getenv("DISCORD_BOT_TOKEN", "").strip():
        return None
    return NOCDiscordBot()


def _csv_ints(name: str) -> set[int]:
    values = os.getenv(name, "")
    result: set[int] = set()
    for value in values.split(","):
        value = value.strip()
        if value:
            result.add(int(value))
    return result


def _optional_int(name: str) -> int | None:
    value = os.getenv(name, "").strip()
    return int(value) if value else None
