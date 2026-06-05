# AS215932 NOC Agent

`noc-agent` is the operator-facing investigation service for AS215932. It
accepts monitoring events, runs structured incident analysis, records
human-review proposals, and keeps a fallback local control plane available even
when chat tooling is unreachable.

## Runtime shape

- FastAPI receives Alertmanager and Icinga webhooks.
- A LangGraph investigation runtime normalizes the alert, correlates repeated
  incidents, routes to a specialist profile, validates confidence, checks
  golden-state drift, and produces a reviewable proposal.
- Redis stores graph checkpoint state plus short-lived incident memory.
- Discord can act as the interactive operator console.
- A loopback-only local control API plus `nocctl` gives operators an SSH/VPN
  fallback for review and decision recording.
- Hyrule MCP provides live diagnostic telemetry; NOC Agent consumes it through
  the configured daemon URL or the legacy stdio path.

The current tranche is intentionally diagnostic-only. Approval records and
resumes operator state, but it does not execute infrastructure changes.

## Primary interfaces

Existing interfaces preserved:

- `POST /webhook/alertmanager`
- `POST /webhook/icinga`
- `POST /task`
- `POST /mail/poll`
- `GET /health`
- `GET /health/mcp`
- `GET /health/config`
- `GET /health/model`
- `GET /health/mail`
- `GET /metrics`

New control-plane interfaces:

- `GET /control/incidents/pending`
- `GET /control/incidents/{incident_id}`
- `POST /control/incidents/{incident_id}/decision`
- `POST /approval/resume`

The `/control/...` endpoints require `X-NOC-Control-Token`. The signed resume
endpoint requires an HMAC signature using `NOC_APPROVAL_SIGNING_SECRET`.

## Operator control

`nocctl` is the local fallback interface:

```bash
nocctl pending
nocctl show <incident-id>
nocctl decide <incident-id> approved --operator svag --comment "reviewed"
```

In production this is intended to run on `noc` over existing SSH/VPN access,
with `NOC_CONTROL_URL=http://127.0.0.1:8000`.

## Discord bot

When `DISCORD_BOT_TOKEN` is present, the service starts a `discord.py` bot that
supports:

- slash-command investigations
- pending/status lookups
- approve/reject/acknowledge decisions
- mention-driven investigations

Guild, channel, and role allowlists are configured with:

- `DISCORD_ALLOWED_GUILD_IDS`
- `DISCORD_ALLOWED_CHANNEL_IDS`
- `DISCORD_ALLOWED_ROLE_IDS`

## Golden-state context

The supervisor prompt is assembled from:

- `app/prompts/supervisor_context.md`
- `app/prompts/golden_state_manifest.json`

The manifest is the machine-readable intended-state anchor. Live MCP telemetry
is compared against it during investigation so proposals can call out drift
instead of inventing a configuration story.

## Key configuration

- `NOC_REDIS_URL`
- `HYRULE_MCP_URL`
- `NOC_CONTROL_TOKEN`
- `NOC_APPROVAL_SIGNING_SECRET`
- `DISCORD_BOT_TOKEN`
- `DISCORD_ALLOWED_GUILD_IDS`
- `DISCORD_ALLOWED_CHANNEL_IDS`
- `DISCORD_ALLOWED_ROLE_IDS`
- `OPENROUTER_API_KEY` for the default model backend
- Optional `OPENROUTER_MANAGEMENT_API_KEY` for account-wide credit monitoring
- Optional `OPENROUTER_APP_TITLE` and `OPENROUTER_APP_URL` for OpenRouter attribution

The legacy `HYRULE_MCP_CMD` path remains accepted for compatibility.

## Model backend configuration

Model selection is configurable via TOML. Lookup order is:

1. `NOC_AGENT_CONFIG`
2. `/etc/noc-agent/config.toml`
3. `config/noc-agent.toml`
4. built-in defaults

The default chain is OpenRouter DeepSeek V4 Pro with Claude Sonnet 4.6 as a fallback:

```toml
[model]
primary = "openrouter:deepseek/deepseek-v4-pro"
fallbacks = ["openrouter:anthropic/claude-sonnet-4.6"]
```

Any OpenRouter model can be selected with `openrouter:<model-slug>`. Secrets stay in environment variables, not in the config file. `AGENT_MODEL` and `AGENT_FALLBACK_MODELS` still override the config file for emergency changes.

Google/Gemini remains supported for future use:

```toml
[model]
primary = "google-gla:gemini-3.1-pro"
fallbacks = []
```

For the new default backend, migrate from `GEMINI_API_KEY` to `OPENROUTER_API_KEY`. `/health/model` reports active models plus OpenRouter key-limit and usage status. `/metrics` exports OpenRouter credit/usage gauges.

## Tests

See [TESTING.md](TESTING.md).
