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
- `GET /health/cases`
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
- `GET /control/proactive/suppressions`
- `POST /control/proactive/ack` / `POST /control/proactive/unack`
- `GET /control/case-service/cases`
- `GET /control/case-service/cases/{case_id}`
- `GET /control/case-service/outbox`

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
3. **Investigate** — attach the top hotspot to a CaseService case, render it as
   a synthetic alert, and run it through the *same* investigation graph the
   webhooks use with CaseService-backed graph memory. Heavy read-only probes
   (`tcpdump_capture`, `dns_probe_burst`, `multi_source_probe`) are stripped
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

**Closing the loop — case links + ack/snooze:** each digest hotspot carries an
**ack id** (short fingerprint) and, once investigated, a link to its **NOC case**
(set `NOC_CONTROL_PUBLIC_URL` for a clickable link; else the case number shows).
Mute a known issue you've decided to handle:

```bash
curl -XPOST -H "x-noc-control-token: $TOK" http://127.0.0.1:8000/control/proactive/ack \
  -d '{"fingerprint":"<ack id>","reason":"tracked in network-operations#268","ttl_hours":168}'
```

From the **web dashboard** (`GET /control`): a "Proactive loop" panel lists the
current hotspots (status, ack id, summary) with inline **Ack** buttons + a
reason/hours field, and the muted list with **Unack** — backed by
`/control/proactive/status` (which now returns `hotspots` + `suppressions`).

From the **Discord bot** (the ack id is right there in the digest):
`/noc_ack ack_id:<id> [reason:…] [hours:…]`, `/noc_unack ack_id:<id>`,
`/noc_acks`. The ack id is matched as a prefix (git short-SHA style), so the
short id shown in the digest works.

An acked hotspot drops from the digest **and** from autonomous investigation
until it resolves — when it stops firing the suppression auto-prunes, so a
recurrence re-alerts. `GET /control/proactive/suppressions` lists active acks;
`POST /control/proactive/unack` clears one.

Proactive config (in-code defaults are conservative; the deployment env enables it):

- `NOC_PROACTIVE_ENABLED` (in-code default `0`, **deployed `1`**; loop not mounted unless `1`)
- `NOC_PROACTIVE_SHADOW` (in-code default `1`, **deployed `0`**; `1` = report hotspots only)
- `NOC_PROACTIVE_INTERVAL_S` (default `120`) / `NOC_PROACTIVE_DEEP_SCAN_S` (default `900`)
- `NOC_PROACTIVE_MAX_INVESTIGATIONS_PER_CYCLE` (default `1`)
- `NOC_PROACTIVE_MAX_INVESTIGATIONS_PER_DAY` (default `12`)
- `NOC_PROACTIVE_MAX_COST_USD_PER_DAY` (default `10`)
- `NOC_PROACTIVE_COST_USD_PER_INVESTIGATION` (default `0.05`; flat estimate charged to the daily $ cap until per-run token→USD metering lands — the count cap is the primary budget)
- `NOC_PROACTIVE_INVESTIGATION_COOLDOWN_S` (default `21600`, six hours): minimum interval before the same successfully investigated hotspot fingerprint is re-investigated
- `NOC_PROACTIVE_INVESTIGATION_FAILURE_RETRY_S` / `NOC_CASE_INVESTIGATION_FAILURE_RETRY_S` (default `21600`, six hours): minimum interval before retrying a failed CaseService-grounded investigation; dependency failures must not re-page every scan cycle
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

## Case-grounded shadow runtime

The case-grounded state machine (`app/cases/`) is available as a strangler
foundation for both reactive webhooks and the proactive loop. It stores typed
observations, atomic cases, meta-cases, append-only events, aliases, traces,
operator feedback, and idempotent side-effect outbox intents.

It is **off by default**. Enable best-effort shadow writes with:

```bash
NOC_CASESERVICE_SHADOW=1
```

If `NOC_DATABASE_URL` or `DATABASE_URL` is set, the runtime uses the Postgres
case store and creates the current schema on startup. Install the optional
Postgres support in deployments that enable this path:

```bash
uv sync  # langgraph-checkpoint-postgres is a hard dependency
```

Without a DSN, shadow mode uses in-memory storage for local/dev canaries. Set
`NOC_REQUIRE_POSTGRES=1` in production to fail loud instead of falling back to
in-memory state; this also prevents LangGraph checkpointing from silently using
the in-memory saver when a Postgres checkpoint backend was requested.

When `NOC_PROACTIVE_ENABLED=1`, the CaseService runtime starts even if shadow or
reactive/control primary flags are not set. In the application runtime, the
proactive loop uses CaseService as its case owner: case state gates investigation
cooldown and report stamping instead of `investigations.json`, and graph runs use
CaseService-backed memory. The `/control/cases` surface also reads these
CaseService cases so digest links do not point at disabled legacy routes. Fresh
scans record only freshly scanned raw hotspots, never carried-forward
deep/degraded-cycle hotspots. The older
`NOC_CASESERVICE_CONTROL=1` can still force this behavior in isolated loop tests
or custom embeddings.

Reactive report enqueueing can be canaried separately:

```bash
NOC_CASESERVICE_REACTIVE_REPORT=1
```

With this flag, reactive Alertmanager/Icinga observations still flow through the
non-primary investigation path, but CaseService owns the report idempotency
decision and enqueues `report` outbox intents for firing cases that should be
surfaced.
The enqueued payload uses a bounded `reactive_case_report_v1` schema, marks
monitor text as untrusted/not-for-model-consumption, and strips control/markdown
delimiters from webhook-derived text.

Reactive investigation cooldown is owned by CaseService when reactive-primary
webhook intake is enabled. Successful graph investigations are stamped back onto
CaseService case state so repeated identical signals can be skipped until the
policy cooldown or a signal change.

Reactive webhook intake can be cut over to CaseService with:

```bash
NOC_CASESERVICE_REACTIVE_PRIMARY=1
```

With this flag, Alertmanager/Icinga webhooks create/update CaseService cases.
Firing cases schedule the graph investigation with a CaseService-derived graph
case, use CaseService-backed graph context/summaries, and CaseService owns
duplicate investigation gating. The flag starts the CaseService runtime even if
`NOC_CASESERVICE_SHADOW` is not set.

The control case surface can be cut over to CaseService with:

```bash
NOC_CASESERVICE_CONTROL_PRIMARY=1
```

With this flag, `/control/cases`, `/control/cases/{id}`, case events, comments,
decisions, and manual investigations use CaseService as the primary case store.
It also starts the CaseService runtime even if `NOC_CASESERVICE_SHADOW` is not
set. This path intentionally does not fall back to legacy case storage for old
or unmapped cases. `NOC_CASESERVICE_REACTIVE_PRIMARY=1` and
`NOC_PROACTIVE_ENABLED=1` also route the `/control/cases` surface to CaseService
so reactive/proactive primary cases remain operator-visible without legacy
fallback.

Legacy reactive/control fallbacks have been removed: reactive webhooks require
`NOC_CASESERVICE_REACTIVE_PRIMARY=1`, and control case routes require a
CaseService primary route. See `LEGACY_DEPRECATION.md` for the removal audit.

Outbox side effects are also opt-in:

```bash
NOC_CASE_OUTBOX_ENABLED=1
```

The worker processes pending and retry-due failed intents. The default handlers
send `report` intents to Discord and stamp the case as reported. If
`NOC_KNOWLEDGE_CANDIDATE_DIR` is set, `knowledge_candidate` intents write
review-gated learning events there. If `NOC_CASE_HANDOFF_REPO` (or
`NOC_PROACTIVE_HANDOFF_REPO`) plus GitHub auth are configured, `handoff` intents
open or refresh idempotent `loop:candidate` issues and stamp `issue_url` on the
case.

Offline replay is available for sanitized fixtures:

```bash
nocctl replay path/to/observations.json
```

Fixtures may be either a JSON list of `ObservationRecord` objects or an object
with an `observations` list. Replay reports deterministic metrics without live
network access or production credentials.

`GET /health/cases` reports whether the optional runtime is disabled, healthy,
or degraded, including backend name and pending/failed outbox counts when the
runtime is enabled. The control plane also exposes read-only CaseService views
under `/control/case-service/...` for projections, events, traces, feedback, and
outbox rows. Reactive-primary, proactive, Discord, and control-primary routes
read and write CaseService case projections. Graph execution requires an
explicit CaseService graph case and graph memory adapter;
there is no implicit graph-runtime legacy case fallback.
Prometheus exports `noc_agent_case_service_runtime_enabled`,
`noc_agent_case_service_shadow_observations_total`,
`noc_agent_case_service_shadow_failures_total`, and
`noc_agent_case_service_outbox_processed_total` so canaries can watch shadow
parity/failures before any control flag is enabled.

## Discord bot

### Persistent case messages

Triage starts, reports, and investigation failures share one case card. Both the
bot and webhook delivery paths retain the Discord message ID across restarts;
unchanged content normally produces no request, and changed content edits the
existing message. At least six hours after the last successful delivery, an
identical update refreshes the card quietly so deleted cards can be detected. A confirmed deleted message is replaced. Temporary API failures remain
retryable without posting a duplicate.

Set `DISCORD_CASE_STATE_DIR` to override the state directory. The default is
`${MAIL_DRAFT_DIR}/.notifications/case-cards` when `MAIL_DRAFT_DIR` is configured,
otherwise `data/mail-drafts/.notifications/case-cards`. All workers serving the same
destination must share this writable persistent directory. It contains only
hashed destination/case keys, message IDs, content hashes, event revisions, and last-verified times; retain it during
deployments. Changing the bot account, channel, or webhook creates a separate destination
identity. Bot permissions must allow sending and editing its own messages.

Event revisions prevent delayed outbox reports from replacing newer triage
results. With `NOC_CASE_OUTBOX_ENABLED=1` (the production setting), terminal
investigation updates are queued durably before their immediate delivery attempt.
Immediate delivery and retries share the same atomic outbox claim, completion
record, and delivery metrics; failed sends are retried by the existing worker.
Report claims expire after ten minutes and recover after a cancelled or restarted
worker. Claim tokens fence late completion writes; report handlers have a
three-minute timeout. Recovery covers report delivery only, preserving the
existing policy for other side effects. Storage failures do not change the
investigation result. Delivered-failure metrics are recorded after the winning
claim commits completion (a process crash in that final metric window can still
omit a counter increment). Intentionally filtered
reports complete as suppressed, without being stamped as delivered. Standalone
use without the case outbox has three bounded attempts but no durable retry.

The state file is atomically replaced after successful delivery. A process crash
between Discord accepting a new message and recording its ID can still produce
one duplicate on retry; Discord does not provide a transactional create with the
local state store. Incident escalation/recovery paging and production monitoring
route integration remain separate from persistent card delivery. The case-service
reminder eligibility and retry policy is described with the runtime settings below.


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

## Knowledge shadow fixtures

This repo includes read-only AS215932 knowledge context-pack and learning-event
fixtures under `evals/knowledge_shadow/`. They validate the future
`AS215932/knowledge` integration in **shadow mode only**: fixture shape,
citations, policy result, required NOC sections, null vector-score placeholders,
and sanitized `learning_ledger_v1` summaries. The production NOC runtime does
not consume these fixtures and this tranche adds no live knowledge service calls.

Run the fixture evals with:

```bash
uv run pytest tests/test_knowledge_shadow.py
```

Fixtures must never contain MCP responses, Prometheus/Icinga raw output, logs,
packet data, commands, credentials, authorization headers, or secrets. Learning
events remain A4 fixtures/proposals until human review promotes them elsewhere.

## Key configuration

- `NOC_REDIS_URL`
- `NOC_CASESERVICE_SHADOW` (default `0`; best-effort case-service shadow writes)
- `NOC_CASESERVICE_CONTROL` (deprecated for app runtime; forces proactive case-owned cooldown/report state in custom embeddings)
- `NOC_CASESERVICE_REACTIVE_REPORT` (default `0`; enqueue reactive report intents from case state)
- `NOC_CASE_ATTENTION_ENABLED` (default `0`; requires both reactive reporting and the outbox worker;
  use the separate durable attention clock for reactive incident notifications)
- `NOC_CASE_REPORT_REASSERT_S` (default `21600`; unchanged-case reminders have a six-hour minimum,
  even if a legacy override is shorter). Only unacknowledged `HIGH` cases remain eligible;
  resolved, closed, expired, linked, recovering, snoozed, suppressed and covered child cases do not repeat.
  Acknowledgement is checked again at delivery. Recurrence and escalation to `HIGH` clear the old
  acknowledgement and advance the report generation. Changed case reports still update the persistent card.
  Reminders create a new notification, with a durable outbox identity anchored to the previous successful
  delivery. Retries do not advance that timestamp. Discord delivery and database completion are not atomic:
  a process failure after Discord accepts a reminder but before the database commit can duplicate it.
  This policy requires reactive report enqueueing and the outbox worker; it does not change the separate
  Alertmanager or Icinga direct Discord routes. Those must be coordinated in the infrastructure repository.
- `NOC_CASESERVICE_REACTIVE_PRIMARY` (default `0`; make reactive webhooks use CaseService)
- `NOC_CASESERVICE_CONTROL_PRIMARY` (default `0`; make `/control/cases` use CaseService)
- `NOC_CASE_POLICY_VERSION` (default `case_policy_v1`)
- `NOC_CASE_OUTBOX_ENABLED` (default `0`; process case side-effect outbox intents)
- `NOC_CASE_OUTBOX_INTERVAL_S` / `NOC_CASE_OUTBOX_LIMIT` / `NOC_CASE_OUTBOX_RETRY_BACKOFF_S`

When reactive reporting and the outbox worker are enabled, each worker tick also
scans up to 100 previously reported critical cases for due reminders. It walks
stable case IDs and wraps after the last page, so frequently updated cases cannot
starve older incidents. Delivery timing includes the scan cycle and outbox queue
latency. A restart begins a new scan; durable reminder identities prevent the
scan from creating duplicate pending work. Eligibility and acknowledgement are
checked again at delivery. A scan failure is logged without blocking existing
outbox retries. This scheduler does not activate reporting or change the separate
infrastructure notification routes.

- `NOC_KNOWLEDGE_CANDIDATE_DIR` (optional output dir for review-gated learning events)
- `NOC_CASE_HANDOFF_REPO` (optional handoff repo; falls back to `NOC_PROACTIVE_HANDOFF_REPO`)
- LHP-v1 dormant cross-loop flags (all behavior-changing flags default off):
  - `NOC_LHP_ENABLED`
  - `NOC_ENGINEERING_HANDOFF_DELIVERY_ENABLED` / `NOC_ENGINEERING_HANDOFF_TRANSPORT` / `NOC_ENGINEERING_HANDOFF_REPO`
  - `NOC_KNOWLEDGE_CONTEXT_ENABLED` / `NOC_KNOWLEDGE_EXPORT_SQLITE` / `NOC_KNOWLEDGE_EXPORT_MANIFEST`
  - `NOC_KNOWLEDGE_CONTEXT_MAX_ARTIFACTS` / `NOC_KNOWLEDGE_CONTEXT_MAX_TOKENS_EQUIVALENT` / `NOC_KNOWLEDGE_CONTEXT_TIMEOUT_S`
  - `NOC_CASE_VERIFICATION_ENABLED` / `NOC_CASE_VERIFICATION_DRY_RUN` / `NOC_CASE_AUTO_RESOLVE_ENABLED`
  - `NOC_CASE_VERIFICATION_INTERVAL_S` / `NOC_CASE_VERIFICATION_REQUIRED_CONSECUTIVE_PASSES`
  - `NOC_DISK_ALERT_HANDOFF_ENABLED`
  - `NOC_LHP_CALLBACK_MAX_BYTES` / `NOC_LHP_ENGINEERING_SECRET` (secret value is not exposed through settings)
  - rollout validation/eval documentation: `docs/lhp-v1-rollout-validation.md` and `evals/lhp_v1/`
  - LHP metrics: `noc_agent_lhp_*` counters on `GET /metrics`
- `NOC_DATABASE_URL` / `DATABASE_URL` (optional Postgres case/checkpoint backend DSN)
- `NOC_REQUIRE_POSTGRES` (default `0`; fail loud if Postgres is required but unavailable)
- `NOC_DATABASE_POOL_MIN_SIZE` / `NOC_DATABASE_POOL_MAX_SIZE`
- `NOC_DATABASE_COMMAND_TIMEOUT_S` / `NOC_DATABASE_STATEMENT_TIMEOUT_MS` / `NOC_DATABASE_LOCK_TIMEOUT_MS`
- `NOC_HANDOFF_DELIVERY_LOCK_TIMEOUT_S` (default `120`; bounds cancellation waits while an external handoff delivery holds the cross-process guard)
- `HYRULE_MCP_URL`
- `NOC_CONTROL_TOKEN`
- `NOC_LOOP_CONSOLE_SECRET` (HMAC secret for internal `/loop-console/v1/*` CaseService API calls)
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

The bundled default chain is GLM 5.2 with DeepSeek V4 Flash as the cheaper
alternative, both on OpenRouter:

```toml
[model]
primary = "openrouter:z-ai/glm-5.2"
fallbacks = ["openrouter:deepseek/deepseek-v4-flash"]
```

Any OpenRouter model can be selected with `openrouter:<model-slug>`. Secrets stay in environment variables, not in the config file. `AGENT_MODEL` and `AGENT_FALLBACK_MODELS` still override the config file for emergency changes.

Venice.AI is supported as a provider-redundant fallback with `venice:<model-id>`
(model IDs from `https://api.venice.ai/api/v1/models`). Venice entries are served
through Venice's OpenAI-compatible endpoint and authenticate with `VENICE_API_KEY`
(`VENICE_BASE_URL` overrides the default `https://api.venice.ai/api/v1`). Venice's
default system prompt is disabled on these entries
(`venice_parameters.include_venice_system_prompt: false`) so provider failover
does not alter the agent's instructions. Because Venice is a separate provider,
an OpenRouter key-limit or quota event does not take the whole chain down.
Venice fallbacks are opt-in — enabling them without `VENICE_API_KEY` degrades
`/health/model` with a missing credential. The production chain is:

```toml
[model]
primary = "openrouter:z-ai/glm-5.2"
fallbacks = [
  "venice:zai-org-glm-5-2",
  "openrouter:deepseek/deepseek-v4-flash",
  "venice:deepseek-v4-flash",
]
```

Venice quota is probed via `GET /api_keys/rate_limits`: `/health/model` reports
the key's USD/Diem balances and degrades when `accessPermitted` is false or the
remaining balance crosses the thresholds (`VENICE_WARN_REMAINING_USD` /
`VENICE_CRITICAL_REMAINING_USD`, defaults 5/1; probe timeout and cache via
`VENICE_CREDIT_PROBE_TIMEOUT_SECONDS` / `VENICE_CREDIT_PROBE_CACHE_SECONDS`).

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


### Case delivery health

`GET /health/cases` reports delivery independently of model and intake health. With `NOC_CASE_OUTBOX_ENABLED=1`, it returns HTTP503 if the worker is not running, no outbox tick has completed within `NOC_CASE_OUTBOX_HEALTH_STALE_S` (default300 seconds, minimum30), or an outstanding report is older than that threshold. Pending, failed and in-progress reports all count; unrelated handoffs do not. Heartbeat grace/staleness is the greater of the delivery threshold and twice the configured NOC_CASE_OUTBOX_INTERVAL_S plus 30 seconds. This avoids false alarms during a normal long polling interval without relaxing the overdue-report threshold. Runtime startup gets that same bounded heartbeat grace. Enabling the worker without a CaseService runtime returns degraded HTTP503. An empty successful tick refreshes the heartbeat; a store exception does not. A database-side aggregate returns one small row of counts and the oldest outstanding report timestamp without loading or deserializing queue payloads. Health reads have a five-second deadline and failures return sanitized degraded health. Long batches can legitimately exceed the threshold: this is delivery latency degradation, not proof of process death.

The additive `delivery` object exposes heartbeat timestamps/age, oldest report age, count and reason codes without case content. Heartbeat elapsed time uses a process-local monotonic clock and resets on restart; outstanding report age comes from durable outbox creation timestamps and survives restart. Configure an independent monitor against this endpoint before transferring notification ownership. This change does not add monitor routing, disable direct alerts, or activate case-owned reporting.

Delivery health SQL requires PostgreSQL16 or later (production verified on17). It classifies malformed/non-finite report timestamps as invalid rather than aborting the aggregate. The additive side_effect_outbox_health_idx covers pending, failed and in_progress rows, leaving completed history outside the health scan. It is created through the existing bounded schema setup; rollback binaries can leave this compatible index in place.

## Durable reactive incident attention

Keep `NOC_CASE_ATTENTION_ENABLED=0` until initial-card ownership and the independent
delivery-health monitor have been deployed and validated. Enabling attention also
requires `NOC_CASESERVICE_REACTIVE_REPORT=1` and `NOC_CASE_OUTBOX_ENABLED=1`.
Direct monitor notification routes must remain available until the application
and its independent failure notification path are proven live.

The attention worker scans cases with stable case-ID pagination. Initial attention
shares the persistent facts card; escalation, recurrence, recovery and six-hour
unacknowledged critical reminders have separate durable message identities in the
same destination. Quiet case-card updates never advance the attention clock.
Human acknowledgement suppresses firing attention until a recurrence or severity
increase beyond the severity covered by that acknowledgement; downgrades and
rebounds within its coverage remain acknowledged. Coverage is stored separately
from case JSON and matched to the acknowledgement timestamp. Older acknowledgements
without coverage are adopted at their last known severity before the next
observation; audit any such active acknowledgements before rollout if historical
severity may already have changed. The automatic Icinga investigation acknowledgement is separate.
Positive recovery can notify even if the firing incident was acknowledged.
The new scheduler inhibits the legacy reactive reminder path while ownership is
enabled. Already queued attention intents retain their handler after flags change.

Successful attention is stored separately from strict case JSON, with its phase,
severity, generation, sequence and delivery time. Outbox completion and the
attention projection commit in one transaction. A per-case lease excludes active
competing deliveries. Fresh eligibility is checked after acquiring it, and the
60-second delivery deadline is shorter than the 120-second lease. Failure,
suppression and cancellation do not advance the clock. Temporary suppression or
verbosity filtering can reopen the same undelivered request when it becomes
eligible. PostgreSQL tests cover rollback, lease expiry, stale ownership,
concurrency, restart and pagination.

Discord message identity is persisted locally so ordinary retries after database
failure reuse the same message. This is not an exactly-once guarantee: a crash
after Discord accepts a create but before local identity persistence, or loss of
the local identity directory, can still create a duplicate on retry. Preserve
`DISCORD_CASE_STATE_DIR` (or the default directory under `MAIL_DRAFT_DIR`) across
deployments. No synthetic channel traffic is needed for local tests.

Rollback must disable attention creation and restore reviewed direct monitor
routing if necessary; preserve attention tables, outbox records, and local card
state. Older binaries ignore the additive tables but do not understand attention
payloads: drain retained attention work with a compatible binary before rolling
back the application itself.
