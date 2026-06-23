# LHP-v1 rollout validation

This document is the item-8 handoff for evals, metrics, docs, and production
rollout validation of Loop Handoff Protocol v1.

## CI/CD rollout evidence

- NOC/app promotion run: `network-operations` run `28015929627` — completed successfully.
- Engineering Loop apply run: `network-operations` run `28024789666` — completed successfully.
- Firewall apply run: `network-operations` run `28025377199` — completed successfully.
- Post-Vault-patch Engineering Loop re-apply run: `network-operations` run `28028900192` — completed successfully.
- Automation follow-up: `network-operations#298` merged so future LHP/Engineering Loop path changes are included in app-promotion auto-apply detection.

## Live validation checklist

Run after Vault has the shared `noc_lhp_engineering_secret` value in both
`kv/data/noc-agent` and `kv/data/engineering-loop`, and after NOC/Loop deploys
have rendered those values.

### NOC public health

```bash
for path in /health /health/cases /health/config /health/mcp /health/model; do
  curl -fsS "https://noc.servify.network${path}"
done
```

Expected:

- `/health` status `ok`
- `/health/cases` status `ok`, backend `PostgresCaseStore`
- CaseService outbox worker running
- LHP verifier enabled and running
- outbox pending/failed both zero
- model/MCP/config health `ok`

### Public LHP non-exposure

```bash
curl -sS -o /tmp/noc_public_lhp_body -w '%{http_code}' \
  https://noc.servify.network/loop-handoff/v1/engineering/handoffs/not-real
```

Expected: `404`; LHP fetch/callback endpoints stay internal-only and are not
proxied by Caddy.

### Loop-side service and secret rendering

On `loop`:

```bash
systemctl is-active hyrule-engineering-loop.timer
systemctl is-enabled hyrule-engineering-loop.timer
systemctl is-active hyrule-knowledge-mcp.service
awk -F= '/^ENGINEERING_LOOP_NOC_LHP_SECRET=/{print length($2)}' /opt/engineering-loop/.env
```

Expected: timer active/enabled, Knowledge MCP active, shared secret length at
least 32 bytes. Do not print the secret value.

On `noc`:

```bash
awk -F= '/^NOC_LHP_ENGINEERING_SECRET=/{print length($2)}' /opt/noc-agent/.env
```

Expected: shared secret length at least 32 bytes. Do not print the secret value.

### Internal LHP reachability and auth

From `loop`:

```bash
curl -sS -o /tmp/lhp_fetch_body -w '%{http_code}' --max-time 10 \
  'http://[2a0c:b641:b50:2::a0]:8000/loop-handoff/v1/engineering/handoffs/not-real'
```

Expected: `401` without HMAC headers. A timeout means the loop→noc firewall path
is not applied; `200/404` without HMAC would mean auth is broken.

### Signed fetch/callback smoke

Use the Engineering Loop LHP client or a local HMAC helper to verify:

- stale timestamps are rejected;
- missing/invalid signatures are rejected;
- valid signed fetch for a non-existent handoff returns authenticated `404`;
- Engineering callbacks cannot set `verified` or `resolved`;
- duplicate callback event IDs are deduped by CaseService.

## Metrics

`GET /metrics` exports the CaseService and LHP rollout counters used by the
validation dashboard:

- `noc_agent_case_service_runtime_enabled`
- `noc_agent_case_service_outbox_processed_total`
- `noc_agent_lhp_handoff_requests_total`
- `noc_agent_lhp_handoff_updates_total`
- `noc_agent_lhp_verification_results_total`
- `noc_agent_lhp_handoffs_verified_total`
- `noc_agent_lhp_cases_resolved_total`
- `noc_agent_lhp_knowledge_events_total`

Labels intentionally use low-cardinality dimensions such as target/source loop,
case type, status, update type, objective type, and outcome. Case IDs, handoff
IDs, callback IDs, and secret-derived values must not be metric labels.

## Eval suite

The private acceptance suite is defined in `evals/lhp_v1/manifest.json` and
covered by unit/integration tests in:

- `tests/test_lhp_foundation.py`
- `tests/test_lhp_store_service.py`
- `tests/test_lhp_verifier.py`
- `tests/test_lhp_knowledge.py`
- `tests/test_proactive_lhp.py`
- `tests/test_proactive_loop.py`
- Engineering Loop LHP tests in `engineering-loop/tests/test_phase28_lhp.py`
- network-operations IaC checks for env rendering, firewall, and Caddy exposure

## 2026-06-23 live validation result

Passed after the post-Vault-patch Engineering Loop re-apply and a NOC service
restart to load the newly rendered shared secret into the running process.

Observed results:

- NOC public health: `/health`, `/health/cases`, `/health/config`, `/health/mcp`, `/health/model` all `ok`.
- `/health/cases`: backend `PostgresCaseStore`, outbox worker running, verifier enabled/running, outbox pending/failed `0/0`.
- Public Caddy exposure check: `/loop-handoff/v1/engineering/handoffs/not-real` returned `404`.
- NOC services: `noc-agent.service` active, `noc-agent-bot.service` active.
- Loop services: `hyrule-engineering-loop.timer` active/enabled, `hyrule-knowledge-mcp.service` active.
- Shared secret render check: `NOC_LHP_ENGINEERING_SECRET` length `96`; `ENGINEERING_LOOP_NOC_LHP_SECRET` length `96`; values were not printed.
- Internal unsigned fetch from loop to NOC returned `401`.
- Internal stale signed fetch returned `401`.
- Internal valid signed fetch for a non-existent handoff returned authenticated `404`.
- Internal signed Engineering callback attempting verifier-only `resolved` status returned `422`.

These results validate the internal-only firewall path, HMAC/timestamp enforcement,
public non-exposure, NOC-only verifier authority, and production CaseService
health for the LHP-v1 rollout.
