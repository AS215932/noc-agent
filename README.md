# Hyrule Networks (AS215932) NOC Agent

`noc-agent` is the operator-facing investigation service for Hyrule Networks
(AS215932). It accepts monitoring events, runs structured incident analysis,
records human-review proposals, and keeps a fallback local control plane available
even when chat tooling is unreachable.

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
- `GET /control/proactive/status`
- `POST /control/proactive/pause`
- `POST /control/proactive/resume`
- `POST /control/proactive/run-once`

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

## Approved remediation (gated, no-op first)

Approved remediation execution is **off by default**. An approved proposal only
runs when `NOC_ENABLE_APPROVED_EXECUTION=1`, and even then the first phase is
inert: with `NOC_ENABLE_NOOP_ROLLBACK_GUARDS=1`, execution routes through the
hyrule-mcp no-op rollback guards (`prepare_commit_confirm` →, on verify,
`confirm_change` or `rollback_change`) with `action_class="noop_rollback_guard"`
— **no service restart, no FRR/PF/nftables/WireGuard mutation**. Each execution
record carries `execution_mode`, `guard_id`, an `authorization_fingerprint`
(the raw HMAC signature is never persisted), and an `execution_audit` trail.

| State | Meaning |
|---|---|
| `execution_disabled` | approved but `NOC_ENABLE_APPROVED_EXECUTION` is off |
| `noop_guards_prepared` | no-op guard installed for each action |
| `verified` | guard confirmed (no-op execution finalized) |
| `verification_failed` | guard could not confirm; rolled back |

Operators can mint a signed authorization offline (matching the MCP HMAC scheme)
for manual/smoke use:

```bash
nocctl approvals sign --proposal-id <case> --action-id <id> --operator svag --action-class noop_rollback_guard --ttl 300
```

Signing requires `HYRULE_MCP_ACTION_SIGNING_SECRET` (or `NOC_APPROVAL_SIGNING_SECRET`).

## Proactive loop

Beyond the reactive webhook path, `noc-agent` runs an **active operator loop**
(`app/proactive/`) that continuously looks for trouble *before* an alert fires.
It is **enabled in production** (the deployment env sets `NOC_PROACTIVE_ENABLED=1`);
the in-code default stays `0` so local dev and the test suite never spin it up.
It is read-only, budgeted, and modelled on the production `engineering-loop`
(run-lock + per-day ledger + budgets + Icinga heartbeat) and
`hyperliquid-trading-agent` (continuous observe → propose → evaluate → learn)
loops. The conservative budgets — 1 investigation/cycle, 12/day, $10/day,
`MEDIUM` severity floor, heavy probes proposed not auto-run — are the live
safety rails.

Each cycle:

1. **Scan** — cheap read-only sweep of Prometheus/Icinga (`app/proactive/scanner.py`)
   for precursors the reactive tripwires fire on only *after* they harden: a BGP
   peer that just left Established or is flapping, a filesystem projected to fill
   within 24h, a scrape target flapping, restart churn / failed units,
   intermittent ICMP on a mesh link, certs near expiry.
2. **Rank & gate** — score by severity, snapshot a decision context, and gate
   expensive investigation against the daily ledger and severity floor
   (`app/proactive/governance.py`, `app/proactive/ledger.py`).
3. **Investigate** — render the top hotspot as a synthetic alert and run it
   through the *same* investigation graph the webhooks use, with heavy read-only
   probes (`tcpdump_capture`, `dns_probe_burst`, `multi_source_probe`) stripped
   unless `NOC_PROACTIVE_AUTO_HEAVY_PROBES=1` (`app/proactive/investigate.py`).
4. **Report** — a Discord digest plus an optional Icinga passive heartbeat.
5. **Hand off** — when a finding needs a config/docs change, open an idempotent
   `loop:candidate` GitHub issue (`app/proactive/handoff.py`); a human promotes
   it to `loop:approved` and the engineering-loop drafts the PR.
6. **Learn** — record predictions, correlate them with later real alerts, and
   propose candidate lessons under `NOC_PROACTIVE_MEMORY_DIR`
   (`app/proactive/memory.py`); humans merge proposals into `lessons/`.

To canary cheaply, set `NOC_PROACTIVE_SHADOW=1` (scan-and-report only, no
autonomous investigation) and flip it back to `0` once the scanners look right.
To pause production at any time, set `NOC_PROACTIVE_ENABLED=0` and re-apply, or
hit `POST /control/proactive/pause`.

Proactive config (in-code defaults are conservative; the deployment env enables it):

- `NOC_PROACTIVE_ENABLED` (in-code default `0`, **deployed `1`**; loop not mounted unless `1`)
- `NOC_PROACTIVE_SHADOW` (in-code default `1`, **deployed `0`**; `1` = report hotspots only)
- `NOC_PROACTIVE_INTERVAL_S` (default `120`) / `NOC_PROACTIVE_DEEP_SCAN_S` (default `900`)
- `NOC_PROACTIVE_MAX_INVESTIGATIONS_PER_CYCLE` (default `1`)
- `NOC_PROACTIVE_MAX_INVESTIGATIONS_PER_DAY` (default `12`)
- `NOC_PROACTIVE_MAX_COST_USD_PER_DAY` (default `10`)
- `NOC_PROACTIVE_COST_USD_PER_INVESTIGATION` (default `0.05`; flat estimate charged to the daily $ cap until per-run token→USD metering lands — the count cap is the primary budget)
- `NOC_PROACTIVE_AUTO_HEAVY_PROBES` (default `0`; propose heavy probes instead of auto-running)
- `NOC_PROACTIVE_HANDOFF_ENABLED` (default `0`) + `NOC_PROACTIVE_HANDOFF_REPO` (default `AS215932/network-operations`)
- `NOC_PROACTIVE_SEVERITY_FLOOR` (default `MEDIUM`)
- `NOC_PROACTIVE_MEMORY_DIR` (default `/var/lib/noc-agent/memory`) / `NOC_PROACTIVE_STATE_DIR` (default `/var/lib/noc-agent/proactive`)
- Handoff auth (only needed when `NOC_PROACTIVE_HANDOFF_ENABLED=1`), in preference order:
  - **GitHub App (recommended):** `NOC_GITHUB_APP_ID` + the private key via `NOC_GITHUB_APP_PRIVATE_KEY_PATH` (a PEM file; Vault Agent renders it on `noc`) or inline `NOC_GITHUB_APP_PRIVATE_KEY`. The installation is auto-resolved from the repo; override with `NOC_GITHUB_APP_INSTALLATION_ID`. The app mints short-lived installation tokens at call time (cached). App needs Issues: read/write + Metadata: read on the handoff repo.
  - **PAT fallback:** `NOC_GITHUB_TOKEN` (fine-grained, issues-scoped).
- `NOC_PROACTIVE_ICINGA_URL` / `NOC_PROACTIVE_ICINGA_CHECK` (optional passive heartbeat; reuses `ICINGA_API_USER`/`ICINGA_API_PASSWORD`)

Settings can also be set under a `[proactive]` table in the model-config TOML;
environment variables take precedence.

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
- `NOC_APPROVAL_SIGNING_SECRET` (also accepts `HYRULE_MCP_ACTION_SIGNING_SECRET`)
- `NOC_ENABLE_APPROVED_EXECUTION` (default `0`; master switch for any execution)
- `NOC_ENABLE_NOOP_ROLLBACK_GUARDS` (default `0`; route execution through inert no-op guards)
- `NOC_ACTION_AUTH_TTL_SECONDS` (default `300`)
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

---

*Part of [Hyrule Networks (AS215932)](https://github.com/AS215932).*
