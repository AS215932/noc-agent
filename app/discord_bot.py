from __future__ import annotations

import asyncio
import os
import shlex
from typing import Any, Awaitable, Callable

from app import log
from app.agent import noc_triage_agent
from app.graph_runtime import pending_summaries, record_operator_decision, run_investigation_graph, summary_for
from app.safe_errors import classify_exception, log_exception
from app.tools.mcp_client import HyruleMCPClient

try:  # pragma: no cover - import availability depends on runtime extras
    import discord
    from discord import app_commands
except Exception:  # pragma: no cover
    discord = None
    app_commands = None


Notifier = Callable[..., Awaitable[None]]
DEFAULT_INVESTIGATION_TIMEOUT_SECONDS = 240


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
        self.investigation_timeout_s = float(
            os.getenv("DISCORD_INVESTIGATION_TIMEOUT_SECONDS", str(DEFAULT_INVESTIGATION_TIMEOUT_SECONDS))
        )
        self._mcp_clients: list[HyruleMCPClient] = []
        self._tasks: set[asyncio.Task] = set()
        self._register_handlers()

    async def start(self):
        token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
        if not token:
            log.info("discord_bot_disabled", reason="DISCORD_BOT_TOKEN-not-set")
            return
        log.info("discord_bot_starting")
        try:
            await self._connect_mcp_tools()
            await self.client.start(token)
        finally:
            await self._disconnect_mcp_tools()

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
            log.info(
                "discord_bot_ready",
                user=str(self.client.user),
                guild_count=len(self.client.guilds),
            )

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
            await self.handle_investigation_interaction(interaction, prompt)

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
            await self.handle_investigation_message(message)

    async def handle_investigation_interaction(self, interaction, prompt: str) -> None:
        if not self._authorized(interaction):
            await interaction.response.send_message("Not authorized.", ephemeral=True)
            return
        operator = str(getattr(getattr(interaction, "user", None), "id", "discord"))
        log.info("discord_investigation_command_received", source="discord-command", operator=operator)
        await interaction.response.defer(ephemeral=True)
        await _safe_send(
            interaction.followup.send,
            "Investigation accepted. I will post a follow-up here when the proposal is ready or if diagnostics fail.",
            ephemeral=True,
        )
        self._track_task(
            self._run_investigation(
                prompt=prompt,
                source="discord-command",
                operator=operator,
                send=lambda text: _safe_send(interaction.followup.send, text, ephemeral=True),
            )
        )

    async def handle_investigation_message(self, message) -> None:
        if not self._message_authorized(message):
            await message.reply("Not authorized.")
            return
        prompt = _message_prompt_without_bot_mention(message, self.client.user)
        operator = str(getattr(getattr(message, "author", None), "id", "discord"))
        log.info("discord_investigation_command_received", source="discord-mention", operator=operator)
        await _safe_send(message.reply, "Investigation accepted. I will reply here when the proposal is ready or diagnostics fail.")
        self._track_task(
            self._run_investigation(
                prompt=prompt,
                source="discord-mention",
                operator=operator,
                send=lambda text: _safe_send(message.reply, text),
            )
        )

    async def _run_investigation(self, *, prompt: str, source: str, operator: str, send: Callable[[str], Awaitable[None]]) -> None:
        log.info(
            "discord_investigation_started",
            source=source,
            operator=operator,
            timeout_seconds=self.investigation_timeout_s,
            prompt_preview=prompt[:160],
        )
        try:
            await send("Investigation is running. I am collecting context and building a proposal.")
            payload = _operator_investigation_payload(prompt, source)
            plan, state = await asyncio.wait_for(
                run_investigation_graph(payload),
                timeout=self.investigation_timeout_s,
            )
            incident_id = state["incident_id"]
            log.info(
                "discord_investigation_completed",
                source=source,
                operator=operator,
                incident_id=incident_id,
                confidence=plan.confidence_score,
                severity=plan.severity,
            )
            await send(
                f"Investigation `{incident_id}` is waiting for review: {plan.issue_summary}\n"
                f"Confidence: {plan.confidence_score * 100:.1f}% | Severity: {plan.severity}"
            )
        except TimeoutError:
            log.warning(
                "discord_investigation_timeout",
                source=source,
                operator=operator,
                timeout_seconds=self.investigation_timeout_s,
            )
            await send(
                "Investigation timed out before a proposal was ready. "
                "Please check `/health/mcp`, `/health/model`, and the noc-agent logs before retrying."
            )
        except Exception as exc:
            safe = classify_exception(exc)
            log_exception(
                "discord_investigation_failed",
                exc,
                category=safe.category,
                provider=safe.provider,
                model=safe.model_name,
                source=source,
                operator=operator,
            )
            await send(safe.discord_description("Discord investigation"))

    def _track_task(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._tasks.add(task)

        def _done(done_task: asyncio.Task) -> None:
            self._tasks.discard(done_task)
            try:
                done_task.result()
            except Exception as exc:
                safe = classify_exception(exc)
                log_exception("discord_investigation_task_failed", exc, category=safe.category)

        task.add_done_callback(_done)
        return task

    async def _connect_mcp_tools(self) -> None:
        if os.getenv("NOC_AGENT_DISABLE_MCP") == "1":
            log.info("discord_bot_mcp_disabled")
            return
        await self._connect_one_mcp(
            source="hyrule",
            command=shlex.split(os.environ["HYRULE_MCP_CMD"]) if not os.getenv("HYRULE_MCP_URL", "").strip() else None,
            url=os.getenv("HYRULE_MCP_URL", "").strip() or None,
        )
        xo_env = os.environ.copy()
        xo_env.setdefault("XO_URL", "https://xo.servify.network")
        xo_env.setdefault("XO_MCP_ENABLE_ACTIONS", "0")
        await self._connect_one_mcp(
            source="xo",
            command=shlex.split(os.environ["XO_MCP_CMD"]),
            env=xo_env,
        )

    async def _connect_one_mcp(
        self,
        *,
        source: str,
        command: list[str] | None,
        env: dict[str, str] | None = None,
        url: str | None = None,
    ) -> None:
        client = HyruleMCPClient(command, env=env, url=url)
        try:
            await client.connect()
            tools = await client.get_tools()
            for tool in tools:
                noc_triage_agent._function_toolset.add_tool(tool)
            self._mcp_clients.append(client)
            log.info("discord_bot_mcp_tools_loaded", source=source, count=len(tools))
        except Exception as exc:
            safe = classify_exception(exc)
            log_exception("discord_bot_mcp_connect_failed", exc, category=safe.category, source=source)
            try:
                await client.disconnect()
            except Exception as disconnect_exc:
                disconnect_safe = classify_exception(disconnect_exc)
                log_exception(
                    "discord_bot_mcp_disconnect_failed",
                    disconnect_exc,
                    category=disconnect_safe.category,
                    source=source,
                )

    async def _disconnect_mcp_tools(self) -> None:
        while self._mcp_clients:
            client = self._mcp_clients.pop()
            try:
                await client.disconnect()
            except Exception as exc:
                safe = classify_exception(exc)
                log_exception("discord_bot_mcp_disconnect_failed", exc, category=safe.category)

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


async def amain() -> None:
    bot = build_bot()
    if bot is None:
        log.info("discord_bot_disabled", reason="DISCORD_BOT_TOKEN-not-set")
        return
    await bot.start()


def main() -> None:
    asyncio.run(amain())


def _operator_investigation_payload(prompt: str, source: str) -> dict[str, Any]:
    return {
        "source": source,
        "status": "firing",
        "groupLabels": {"alertname": "Operator Investigation", "host": "manual"},
        "commonLabels": {"severity": "manual"},
        "commonAnnotations": {"summary": prompt},
        "alerts": [
            {
                "labels": {"alertname": "Operator Investigation", "host": "manual"},
                "annotations": {"summary": prompt},
            }
        ],
    }


def _message_prompt_without_bot_mention(message, bot_user) -> str:
    content = str(getattr(message, "content", "") or "")
    bot_id = getattr(bot_user, "id", None)
    if bot_id is not None:
        content = content.replace(f"<@{bot_id}>", "").replace(f"<@!{bot_id}>", "")
    return " ".join(content.split()) or "Operator requested an investigation."


async def _safe_send(send: Callable[..., Awaitable[Any]], content: str, **kwargs: Any) -> None:
    try:
        await send(content, **kwargs)
    except TypeError:
        await send(content)
    except Exception as exc:
        safe = classify_exception(exc)
        log_exception("discord_investigation_reply_failed", exc, category=safe.category)


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
