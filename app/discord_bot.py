from __future__ import annotations

import asyncio
import json
import os
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from app import log
from app.agent import noc_triage_agent
from app.graph_runtime import pending_summaries, record_operator_decision, run_investigation_graph, summary_for
from app.mcp_runtime import MCPRuntime
from app.safe_errors import classify_exception, log_exception

try:  # pragma: no cover - import availability depends on runtime extras
    import discord
    from discord import app_commands
except Exception:  # pragma: no cover
    discord = None
    app_commands = None


Notifier = Callable[..., Awaitable[None]]
DEFAULT_INVESTIGATION_TIMEOUT_SECONDS = 240
THREAD_MESSAGE_LIMIT = 1400
INCIDENT_ID_RE = re.compile(r"^(?:inc|incident)[-_][0-9a-f-]+$", re.IGNORECASE)


@dataclass(frozen=True)
class OperatorIntent:
    type: str
    raw_text: str
    target: str = ""
    qualifiers: dict[str, str] = field(default_factory=dict)
    incident_id: str = ""
    decision: str = ""
    comment: str = ""


@dataclass(frozen=True)
class StatusOverview:
    status: str
    target: str
    summary: str
    checks: list[str] = field(default_factory=list)
    suggested_next_action: str = ""


@dataclass(frozen=True)
class OperatorResponse:
    kind: str
    content: str
    thread_required: bool = False
    incident_id: str | None = None


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
        self._mcp_runtime = MCPRuntime(owner="discord_bot")
        self._tasks: set[asyncio.Task] = set()
        self._max_case_messages = int(os.getenv("DISCORD_CASE_MESSAGE_CACHE_MAX", "1000"))
        self._case_messages = OrderedDict()
        self._register_handlers()

    async def start(self):
        token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
        if not token:
            log.info("discord_bot_disabled", reason="DISCORD_BOT_TOKEN-not-set")
            return
        log.info("discord_bot_starting")
        try:
            await self._mcp_runtime.connect_tools()
            await self.client.start(token)
        finally:
            await self._mcp_runtime.disconnect()

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

    async def send_case_embed(
        self,
        case_id: str,
        title: str,
        description: str,
        color: int,
        fields: list[dict[str, Any]] | None = None,
    ):
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
        message = self._case_messages.get(case_id)
        if message is not None and callable(getattr(message, "edit", None)):
            try:
                await message.edit(embed=embed)
                self._remember_case_message(case_id, message)
                return
            except Exception as exc:
                safe = classify_exception(exc)
                log_exception("discord_case_embed_edit_failed", exc, category=safe.category, case_id=case_id)
        sent = await channel.send(embed=embed)
        self._remember_case_message(case_id, sent)

    def _remember_case_message(self, case_id: str, message) -> None:
        if self._max_case_messages <= 0:
            return
        self._case_messages[case_id] = message
        self._case_messages.move_to_end(case_id)
        while len(self._case_messages) > self._max_case_messages:
            self._case_messages.popitem(last=False)

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
            incidents = await pending_summaries()
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
            summary = await summary_for(incident_id)
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
            summary = await _maybe_await(record_operator_decision(
                incident_id,
                {
                    "incident_id": incident_id,
                    "decision": decision,
                    "operator": operator,
                    "comment": comment,
                },
            ))
            await interaction.response.send_message(
                "Incident not found." if summary is None else f"Recorded `{decision}` for `{incident_id}`.",
                ephemeral=True,
            )

        @self.client.event
        async def on_message(message):
            if message.author.bot or self.client.user is None or self.client.user not in message.mentions:
                return
            await self.handle_operator_message(message)

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
        await self._start_investigation(
            prompt=prompt,
            source="discord-command",
            operator=operator,
            send=lambda text: _safe_send(interaction.followup.send, text, ephemeral=True),
        )

    async def handle_operator_message(self, message) -> None:
        if not self._message_authorized(message):
            await message.reply("Not authorized.")
            return
        prompt = _message_prompt_without_bot_mention(message, self.client.user)
        operator = str(getattr(getattr(message, "author", None), "id", "discord"))
        intent = parse_discord_operator_request(prompt)
        log.info("discord_operator_intent_received", source="discord-mention", operator=operator, intent=intent.type)
        await self.handle_operator_intent(intent, message, operator=operator)

    async def handle_investigation_message(self, message) -> None:
        await self.handle_operator_message(message)

    async def handle_operator_intent(self, intent: OperatorIntent, message, *, operator: str) -> None:
        if intent.type == "investigate":
            thread = await _create_thread(message, _thread_name(intent.target or intent.raw_text))
            send = thread.send if thread is not None else message.reply
            await _safe_send(
                message.reply,
                f"Investigation accepted for `{intent.target or intent.raw_text}`. Progress will be posted"
                f"{' in the thread' if thread is not None else ' here'}.",
            )
            await self._start_investigation(
                prompt=intent.target or intent.raw_text,
                source="discord-mention",
                operator=operator,
                send=lambda text: _safe_send(send, text),
            )
            return

        if intent.type == "status":
            overview = await run_fast_status_check(intent.target, intent.qualifiers, self._mcp_runtime)
            content = _format_status_overview(overview)
            if len(content) > THREAD_MESSAGE_LIMIT or overview.status != "ok":
                thread = await _create_thread(message, _thread_name(f"status-{intent.target}"))
                if thread is not None:
                    await _safe_send(message.reply, f"Status for `{intent.target}` is `{overview.status}`. Details are in the thread.")
                    await _safe_send(thread.send, content)
                    return
            await _safe_send(message.reply, content)
            return

        if intent.type == "pending":
            await _safe_send(message.reply, await _format_pending())
            return

        if intent.type == "incident_status":
            await _safe_send(message.reply, await _format_incident_status(intent.incident_id))
            return

        if intent.type == "decision":
            summary = await _maybe_await(record_operator_decision(
                intent.incident_id,
                {
                    "incident_id": intent.incident_id,
                    "decision": intent.decision,
                    "operator": operator,
                    "comment": intent.comment,
                },
            ))
            await _safe_send(
                message.reply,
                "Incident not found." if summary is None else f"Recorded `{intent.decision}` for `{intent.incident_id}`.",
            )
            return

        await _safe_send(message.reply, _help_text())

    async def _start_investigation(
        self,
        *,
        prompt: str,
        source: str,
        operator: str,
        send: Callable[[str], Awaitable[None]],
    ) -> None:
        self._track_task(
            self._run_investigation(
                prompt=prompt,
                source=source,
                operator=operator,
                send=send,
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
                f"Investigation `{incident_id}` is waiting for review: {plan.incident_summary}\n"
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


def parse_discord_operator_request(text: str) -> OperatorIntent:
    raw = " ".join(str(text or "").split())
    lowered = raw.lower()
    if not raw or lowered in {"help", "?", "commands"}:
        return OperatorIntent(type="help", raw_text=raw)

    natural_status = _natural_status_target(raw)
    if natural_status:
        return OperatorIntent(type="status", raw_text=raw, target=natural_status)

    icinga_problem_intent = _parse_icinga_problem_query(raw)
    if icinga_problem_intent is not None:
        return icinga_problem_intent

    words = raw.split()
    command = words[0].lower()
    rest = " ".join(words[1:]).strip()

    if command in {"pending", "proposals"} or lowered in {"show pending", "list pending"}:
        return OperatorIntent(type="pending", raw_text=raw)

    if command in {"approve", "reject", "acknowledge", "ack"}:
        if not rest:
            return OperatorIntent(type="help", raw_text=raw)
        incident_id, _, comment = rest.partition(" ")
        decision = {
            "approve": "approved",
            "reject": "rejected",
            "ack": "acknowledged",
            "acknowledge": "acknowledged",
        }[command]
        return OperatorIntent(
            type="decision",
            raw_text=raw,
            incident_id=incident_id,
            decision=decision,
            comment=comment.strip(),
        )

    if command in {"investigate", "investigation", "debug"}:
        return OperatorIntent(type="investigate", raw_text=raw, target=_clean_target(rest or raw))

    if command in {"status", "show"}:
        if not rest:
            return OperatorIntent(type="help", raw_text=raw)
        if rest.startswith("incident "):
            incident_id = _clean_target(rest.split(maxsplit=1)[1])
            return OperatorIntent(type="incident_status", raw_text=raw, incident_id=incident_id)
        routed = _parse_structured_status(raw, rest)
        if routed is not None:
            return routed
        target = _clean_target(rest)
        if INCIDENT_ID_RE.match(target):
            return OperatorIntent(type="incident_status", raw_text=raw, incident_id=target)
        return OperatorIntent(type="status", raw_text=raw, target=target)

    if command == "check":
        match = re.match(r"(?P<subject>.+?)\s+on\s+(?P<target>\S+)$", rest, flags=re.IGNORECASE)
        if match:
            return OperatorIntent(
                type="status",
                raw_text=raw,
                target=_clean_target(match.group("target")),
                qualifiers={"check": match.group("subject").strip()},
            )
        return OperatorIntent(type="status", raw_text=raw, target=_clean_target(rest or raw))

    return OperatorIntent(type="help", raw_text=raw)


async def run_fast_status_check(target: str, qualifiers: dict[str, str] | None, runtime: MCPRuntime) -> StatusOverview:
    target = str(target or "").strip()
    qualifiers = qualifiers or {}
    checks: list[str] = []
    status = "ok"

    incident = await summary_for(target)
    if incident is not None:
        checks.append(f"Incident `{target}` is `{incident.get('status', 'unknown')}`: {incident.get('title', 'No title')}")
        return StatusOverview(status="ok", target=target, summary="Incident summary found.", checks=checks)

    runtime_health = runtime.health()
    if _is_noc_target(target):
        checks.append(f"NOC API health: ok")
        checks.append(
            "MCP health: "
            f"{runtime_health['status']} "
            f"(Hyrule {runtime_health['hyrule_tool_count']} tools, XO {runtime_health['xo_tool_count']} tools)"
        )
        missing = _missing_config()
        checks.append("Config health: ok" if not missing else f"Config health: missing {', '.join(missing)}")
        if runtime_health["status"] != "ok" or missing:
            status = "degraded"
        return StatusOverview(
            status=status,
            target=target,
            summary="NOC control-plane status.",
            checks=checks,
            suggested_next_action="" if status == "ok" else f'Reply with "investigate {target}" to run the full investigation.',
        )

    if runtime_health["status"] != "ok":
        status = "degraded"
        checks.append(f"MCP health is {runtime_health['status']}; live host checks may be incomplete.")

    if _is_icinga_problem_check(target, qualifiers):
        object_type = qualifiers.get("object_type", "service")
        output = await _call_mcp_tool(runtime, "hyrule", "icinga_list_problems", {"object_type": object_type, "limit": 20})
        if output:
            overview = _icinga_problem_overview(output, object_type)
            checks.append(overview)
            if "`0`" not in overview:
                status = "degraded"
        else:
            status = "degraded"
            checks.append("Icinga problem query returned no data.")
        return StatusOverview(
            status=status,
            target=target,
            summary="Fast read-only Icinga problem check completed." if status == "ok" else "Icinga has active problems.",
            checks=checks[:6],
            suggested_next_action="" if status == "ok" else 'Reply with "investigate icinga service problems" to run the full investigation.',
        )

    if _is_bgp_check(qualifiers):
        command = _bgp_status_command(qualifiers)
        output = await _call_mcp_tool(runtime, "hyrule", "frr_vtysh_cmd", {"host": target, "command": command})
        if output:
            checks.append(f"FRR `{command}`:\n{_code_block(_command_output_summary(output, preserve_lines=True), limit=1200)}")
        else:
            status = "degraded"
            checks.append(f"FRR `{command}` returned no data for `{target}`.")
        return StatusOverview(
            status=status,
            target=target,
            summary="Fast read-only BGP status check completed." if status == "ok" else "BGP status check needs operator review.",
            checks=checks[:6],
            suggested_next_action="" if status == "ok" else f'Reply with "investigate bgp {target}" to run the full investigation.',
        )

    icinga = await _call_mcp_tool(runtime, "hyrule", "icinga_get_host_state", {"host": target})
    if icinga:
        checks.append(f"Icinga: {_compact_tool_text(icinga)}")

    prometheus = await _call_mcp_tool(runtime, "hyrule", "prometheus_list_targets", {"filter": target})
    if prometheus:
        checks.append(f"Prometheus targets: {_compact_tool_text(prometheus)}")

    if _looks_like_virtualization_query(target, qualifiers):
        xo = await _call_mcp_tool(runtime, "xo", "get_infrastructure_summary", {})
        if xo:
            checks.append(f"XO summary: {_compact_tool_text(xo)}")

    if not checks:
        status = "unknown"
        checks.append("No fast status data was available for this target.")

    return StatusOverview(
        status=status,
        target=target,
        summary="Fast read-only status check completed." if status == "ok" else "Fast status check needs operator review.",
        checks=checks[:6],
        suggested_next_action="" if status == "ok" else f'Reply with "investigate {target}" to run the full investigation.',
    )


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


async def _maybe_await(value):
    return await value if hasattr(value, "__await__") else value


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


async def _create_thread(message, name: str):
    create_thread = getattr(message, "create_thread", None)
    if not callable(create_thread):
        return None
    try:
        return await create_thread(name=name[:90])
    except Exception as exc:
        safe = classify_exception(exc)
        log_exception("discord_thread_create_failed", exc, category=safe.category)
        return None


async def _call_mcp_tool(runtime: MCPRuntime, source: str, tool: str, arguments: dict[str, Any]) -> str:
    client = runtime.clients.get(source)
    session = getattr(client, "session", None)
    if session is None:
        return ""
    try:
        result = await session.call_tool(tool, arguments=arguments)
    except Exception as exc:
        safe = classify_exception(exc)
        log_exception("discord_status_mcp_tool_failed", exc, category=safe.category, source=source, tool=tool)
        return ""
    return _tool_result_text(result)


def _tool_result_text(result: Any) -> str:
    content = getattr(result, "content", [])
    out = []
    for block in content:
        text = getattr(block, "text", "")
        if text:
            out.append(str(text))
    return "\n".join(out).strip()


def _compact_tool_text(text: str, limit: int = 700) -> str:
    value = " ".join(str(text or "").split())
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + "..."


def _compact_lines(text: str, limit: int = 1200) -> str:
    lines = [line.rstrip() for line in str(text or "").splitlines() if line.strip()]
    value = "\n".join(lines)
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + "..."


def _code_block(text: str, *, limit: int = 1200) -> str:
    value = _compact_lines(text, limit=limit).replace("```", "'''")
    return f"```text\n{value or 'no output'}\n```"


def _format_status_overview(overview: StatusOverview) -> str:
    lines = [
        f"Status for `{overview.target}`: `{overview.status}`",
        overview.summary,
    ]
    lines.extend(_format_check_line(check) for check in overview.checks)
    if overview.suggested_next_action:
        lines.append(overview.suggested_next_action)
    return "\n".join(line for line in lines if line)


def _format_check_line(check: str) -> str:
    return f"- {check}" if "\n" not in check else f"- {check}"


async def _format_pending() -> str:
    incidents = await pending_summaries()
    if not incidents:
        return "No pending NOC proposals."
    lines = ["Pending NOC proposals:"]
    lines.extend(f"- `{item['incident_id']}` {item['title']}" for item in incidents[:10])
    return "\n".join(lines)


async def _format_incident_status(incident_id: str) -> str:
    summary = await summary_for(incident_id)
    if summary is None:
        return "Incident not found."
    return f"`{incident_id}` {summary['status']}: {summary['title']}"


def _help_text() -> str:
    return (
        "Try `@NOC Agent status noc`, `@NOC Agent check bgp on cr1-nl1`, "
        "`@NOC Agent investigate packet loss to ns2`, `@NOC Agent pending`, "
        "`@NOC Agent status <incident_id>`, or `@NOC Agent approve <incident_id> <comment>`."
    )


def _thread_name(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_.-]+", "-", str(value or "investigation")).strip("-").lower()
    return f"noc-{cleaned or 'investigation'}"[:90]


def _natural_status_target(text: str) -> str:
    query = str(text or "").strip().strip("?.!")
    patterns = (
        r"^(?:what(?:'s| is)|show me|give me)\s+(?:the\s+)?status\s+(?:of|for)\s+(?P<target>.+)$",
        r"^(?:what(?:'s| is)|show me|give me)\s+(?P<target>.+?)\s+status$",
        r"^(?:how is|how's)\s+(?P<target>.+?)(?:\s+doing)?$",
        r"^is\s+(?P<target>.+?)\s+(?:ok|okay|up|down|healthy|degraded)$",
    )
    for pattern in patterns:
        match = re.match(pattern, query, flags=re.IGNORECASE)
        if match:
            return _clean_target(match.group("target"))
    return ""


def _parse_structured_status(raw: str, rest: str) -> OperatorIntent | None:
    tokens = rest.split()
    if len(tokens) < 2:
        return None
    subject_tokens = tokens[:-1]
    target = _clean_target(tokens[-1])
    subject = " ".join(subject_tokens).strip().lower()
    if subject in {"bgp", "bgp peer", "bgp peers", "bgp summary", "routing bgp"}:
        return OperatorIntent(type="status", raw_text=raw, target=target, qualifiers={"check": subject})
    return None


def _parse_icinga_problem_query(raw: str) -> OperatorIntent | None:
    query = str(raw or "").strip().strip("?.!").lower()
    if "icinga" not in query or "problem" not in query:
        return None
    object_type = "host" if "host" in query and "service" not in query else "service"
    return OperatorIntent(
        type="status",
        raw_text=raw,
        target="icinga",
        qualifiers={"check": "icinga problems", "object_type": object_type},
    )


def _clean_target(value: str) -> str:
    cleaned = str(value or "").strip().strip("`'\".,?!:;()[]{}")
    if re.match(r"^cr\d+\.(?:de\d+|nl\d+)$", cleaned, flags=re.IGNORECASE):
        cleaned = cleaned.replace(".", "-")
    return cleaned


def _is_noc_target(target: str) -> bool:
    lowered = str(target or "").lower()
    return lowered in {"noc", "noc-agent", "agent", "control-plane", "control plane"} or lowered.startswith("noc ")


def _looks_like_virtualization_query(target: str, qualifiers: dict[str, str]) -> bool:
    blob = f"{target} {' '.join(qualifiers.values())}".lower()
    return any(token in blob for token in ("xo", "xoa", "vm", "vms", "pool", "xcp", "xen"))


def _is_icinga_problem_check(target: str, qualifiers: dict[str, str]) -> bool:
    blob = f"{target} {' '.join(qualifiers.values())}".lower()
    return "icinga" in blob and "problem" in blob


def _is_bgp_check(qualifiers: dict[str, str]) -> bool:
    return "bgp" in str(qualifiers.get("check", "")).lower().split()


def _bgp_status_command(qualifiers: dict[str, str]) -> str:
    check = str(qualifiers.get("check", "")).lower()
    if "summary" in check or "peer" in check or "bgp" in check:
        return "show bgp summary"
    return "show bgp summary"


def _command_output_summary(text: str, *, preserve_lines: bool = False) -> str:
    try:
        payload = json.loads(text)
    except Exception:
        return text if preserve_lines else _compact_tool_text(text)
    if not isinstance(payload, dict):
        return text if preserve_lines else _compact_tool_text(text)
    stdout = str(payload.get("stdout") or "").strip()
    stderr = str(payload.get("stderr") or "").strip()
    exit_code = payload.get("exit_code")
    if stdout:
        prefix = f"exit={exit_code}; " if exit_code not in (None, 0) else ""
        return prefix + stdout
    if stderr:
        return f"exit={exit_code}; {stderr}"
    return f"exit={exit_code}; no output"


def _icinga_problem_overview(text: str, object_type: str) -> str:
    try:
        payload = json.loads(text)
    except Exception:
        return f"Icinga {object_type} problems:\n{_code_block(text)}"
    if not isinstance(payload, dict):
        return f"Icinga {object_type} problems:\n{_code_block(text)}"
    count = int(payload.get("count") or 0)
    problems = payload.get("problems") or []
    if count == 0:
        return f"Icinga {object_type} problems: `0`"
    lines = [f"Icinga {object_type} problems: `{count}`"]
    for item in problems[:10]:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("host") or "unknown"
        state = item.get("state", "unknown")
        output = str(item.get("output") or "").strip()
        lines.append(f"{name} state={state}" + (f" - {output}" if output else ""))
    returned = int(payload.get("returned") or len(problems))
    if count > returned:
        lines.append(f"... {count - returned} more not shown")
    return "\n".join(lines)


def _missing_config() -> list[str]:
    required = [
        "DISCORD_WEBHOOK_URL",
        "HYRULE_MCP_URL",
        "XO_MCP_URL",
        "XO_TOKEN",
        "ICINGA_API_USER",
        "ICINGA_API_PASSWORD",
        "MAIL_IMAP_PASSWORD",
    ]
    try:
        from app.model_config import load_model_config

        model_config = load_model_config()
        if any(model.startswith("openrouter:") for model in model_config.active_model_chain):
            required.append(model_config.provider_api_key_envs.get("openrouter", "OPENROUTER_API_KEY"))
        if any(model.startswith(("google:", "google-gla:", "google-vertex:")) or model.startswith("gemini") for model in model_config.active_model_chain):
            if not (os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")):
                required.append("GOOGLE_API_KEY or GEMINI_API_KEY")
    except Exception:
        required.append("OPENROUTER_API_KEY")
    return sorted({name for name in required if " or " in name or not os.getenv(name)})
