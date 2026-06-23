---
created: 2026-06-22T19:54:22.382Z
source: pi-plan-mode
status: accepted-for-execution
---

# Loop Handoff Protocol v1: NOC ↔ Engineering ↔ Knowledge via CaseService

## Summary

Implement **Loop Handoff Protocol v1 (LHP-v1)** as a generic, typed, auditable cross-loop workflow.

Decisions locked:

- **CaseService is the operational source of truth.**
- **Hyrule Knowledge is the learning substrate.**
- **First Engineering transport: GitHub issue with embedded bounded LHP-v1 payload**, reusing the existing `loop:candidate` / `loop:approved` Engineering Loop intake.
- **Execution gate: keep human `loop:approved` gate.**
- **First Knowledge transport: local export / CLI / MCP adapter behind LHP-v1 interfaces**, not a new HTTP service.
- **Storage: first-class CaseService tables with bounded JSON payloads.**
- **Verification: dedicated CaseService-bound NOC verifier scheduler**, not embedded in the proactive loop.

Immediate production case:

```yaml
case_type: proactive_disk_condition
fingerprint: 8fb421ff94bb1285
resource: 2a0c:b641:b50:2::1
filesystem: /
objective: resolve low root filesystem condition
```

## Implementation Steps

1. Add LHP-v1 feature flags, config, safety helpers, schemas, and storage.
2. Add CaseService handoff, callback, verification, knowledge-artifact, and outcome service methods.
3. Modify NOC proactive disk handling to create/update cases, request Knowledge context, and create one Engineering handoff.
4. Add GitHub Engineering handoff delivery and Engineering Loop LHP issue consumption/callback support.
5. Add the dedicated NOC verification scheduler.
6. Add Knowledge pre-context and post-resolution artifact proposal integration.
7. Add production rollout config for NOC, Engineering Loop, and the current disk fingerprint.
8. Add evals, tests, metrics, docs, and rollout validation.

## Current Environment Facts

- `noc-agent` already has:
  - CaseService models/events/outbox.
  - Postgres JSONB-backed CaseStore.
  - `/health/cases`.
  - proactive scanner and CaseService-backed proactive observations.
  - existing GitHub `handoff` outbox handler, but only as a simple issue bridge.
- Engineering Loop currently:
  - consumes GitHub issues labeled `loop:approved`;
  - files/uses `loop:candidate` as the human triage gate;
  - emits NOC handoff artifacts and proposed learning summaries;
  - has no HTTP callback server.
- Knowledge currently:
  - has deterministic SQLite/JSONL exports;
  - has a read-only MCP/context-pack path;
  - has review-gated learning ledger events.
- `network-operations` currently does not expose NOC webhooks publicly through Caddy. Keep LHP callbacks internal.

## Feature Flags and Config

Add a new config group, e.g. `LoopHandoffSettings`.

Env vars:

```bash
NOC_LHP_ENABLED=0
NOC_ENGINEERING_HANDOFF_DELIVERY_ENABLED=0
NOC_ENGINEERING_HANDOFF_TRANSPORT=github_issue
NOC_ENGINEERING_HANDOFF_REPO=AS215932/network-operations

NOC_KNOWLEDGE_CONTEXT_ENABLED=0
NOC_KNOWLEDGE_EXPORT_SQLITE=/opt/noc-knowledge/exports/knowledge.sqlite
NOC_KNOWLEDGE_EXPORT_MANIFEST=/opt/noc-knowledge/exports/manifest.json
NOC_KNOWLEDGE_CONTEXT_MAX_ARTIFACTS=10
NOC_KNOWLEDGE_CONTEXT_MAX_TOKENS_EQUIVALENT=3000
NOC_KNOWLEDGE_CONTEXT_TIMEOUT_S=20
NOC_KNOWLEDGE_CANDIDATE_DIR=/var/lib/noc-agent/knowledge-candidates

NOC_CASE_VERIFICATION_ENABLED=0
NOC_CASE_VERIFICATION_DRY_RUN=1
NOC_CASE_AUTO_RESOLVE_ENABLED=0
NOC_CASE_VERIFICATION_INTERVAL_S=120
NOC_CASE_VERIFICATION_REQUIRED_CONSECUTIVE_PASSES=3

NOC_DISK_ALERT_HANDOFF_ENABLED=0
NOC_LHP_CALLBACK_MAX_BYTES=65536
NOC_LHP_ENGINEERING_SECRET=<vault-rendered shared secret>
```

Engineering Loop env:

```bash
ENGINEERING_LOOP_NOC_LHP_BASE_URL=http://[2a0c:b641:b50:2::a0]:8000
ENGINEERING_LOOP_NOC_LHP_SECRET=<same shared secret>
ENGINEERING_LOOP_LHP_CALLBACK_ENABLED=0
```

Safe defaults: all new behavior disabled unless explicitly enabled.

## CaseService Data Model

Add first-class Pydantic models and Postgres tables.

### Enums

```python
LoopName = Literal["noc", "engineering", "knowledge", "soc"]

HandoffStatus = Literal[
    "requested",
    "accepted",
    "in_progress",
    "change_planned",
    "implemented",
    "blocked",
    "failed",
    "needs_human",
    "verified",
    "resolved",
    "cancelled",
    "expired",
]

HandoffUpdateType = Literal[
    "accepted",
    "investigating",
    "blocked",
    "change_planned",
    "change_applied",
    "implemented",
    "failed",
    "needs_human",
]

VerificationStatus = Literal["pending", "pass", "fail", "unknown", "skipped"]

KnowledgeReviewStatus = Literal[
    "pending",
    "approved",
    "rejected",
    "superseded",
    "deprecated",
    "published",
]
```

Extend existing `CaseStatus` with:

```text
open
triaged
context_requested
handoff_requested
handoff_in_progress
verification_pending
blocked
failed
needs_human
```

### New tables

Add to `app/db/schema.py`:

- `case_handoffs`
- `handoff_updates`
- `verification_objectives`
- `knowledge_artifacts`
- `outcome_records`
- `callback_inbox`
- `handoff_transport_deliveries`

Use typed columns for state-machine/query fields and bounded JSONB for evidence/payload.

Required constraints:

- unique `case_handoffs.idempotency_key`
- unique active handoff by `(case_id, target_loop, objective_key)` where status not terminal
- unique callback by `(source_loop, external_event_id)`
- unique verification objective by `(case_id, objective_key)`
- unique knowledge artifact by `(case_id, artifact_type, version)`
- indexes on `case_id`, `handoff_id`, `status`, `target_loop`, `review_status`, `next_check_at`, `fingerprint`, `correlation_id`

### Atomic store methods

Add CaseStore boundary methods so concurrency-sensitive updates are transactional:

```python
create_handoff_with_objectives(...)
record_handoff_delivery(...)
update_handoff_delivery(...)
append_handoff_update(...)
claim_callback_event(...)
upsert_verification_objective(...)
update_verification_objective_result(...)
list_due_verification_objectives(...)
mark_handoff_verified(...)
resolve_case_with_outcome(...)
record_knowledge_artifact(...)
record_outcome(...)
```

Postgres implementation must use transactions and `FOR UPDATE` where state transitions depend on current state. In-memory implementation uses its existing async lock.

## State Machines

### Handoff transitions

Allowed:

```text
requested -> accepted
accepted -> in_progress
in_progress -> change_planned
change_planned -> implemented
implemented -> verified
verified -> resolved

requested -> blocked | failed | needs_human | cancelled | expired
accepted -> blocked | failed | needs_human | cancelled | expired
in_progress -> blocked | failed | needs_human | change_planned | implemented
change_planned -> blocked | failed | needs_human | implemented
implemented -> failed | needs_human | verified
```

Only NOC verifier may set:

```text
verified
resolved
```

Engineering callback attempting `verified` or `resolved` is rejected.

### Case transitions for this workflow

```text
open
-> context_requested
-> handoff_requested
-> handoff_in_progress
-> verification_pending
-> resolved
```

Failure branches:

```text
blocked
failed
needs_human
```

Engineering `implemented` moves the case to `verification_pending`, not `resolved`.

NOC may also move to `verification_pending` if it independently observes the alert clear.

## API Contract

### Internal/control endpoints

Require existing control auth via `NOC_CONTROL_TOKEN`.

Add:

```text
GET /control/cases/{case_id}/handoffs
GET /control/cases/{case_id}/verification
GET /control/cases/{case_id}/knowledge
GET /control/cases/{case_id}/outcomes
POST /control/cases/upsert
```

`POST /control/cases/upsert` is for operator/testing/manual recovery. The proactive loop should call CaseService directly.

### Engineering fetch endpoint

Internal only, not Caddy-exposed initially:

```text
GET /loop-handoff/v1/engineering/handoffs/{handoff_id}
```

Purpose: Engineering Loop fetches authoritative LHP payload from CaseService after reading a GitHub issue pointer.

### Engineering callback endpoint

Internal only:

```text
POST /webhook/engineering-loop/handoff-update
```

Required:

- `schema_version = "lhp.v1"`
- `external_event_id`
- `correlation_id`
- `case_id`
- `handoff_id`
- `source_loop = "engineering"`
- `update_type`
- `status`
- bounded `summary`
- bounded `evidence`

Auth:

- use `X-NOC-Loop-Identity`
- use `X-NOC-Loop-Timestamp`
- use `X-NOC-Loop-Signature`
- HMAC over method, path, timestamp, and canonical JSON body
- shared secret from `NOC_LHP_ENGINEERING_SECRET`
- reject missing/invalid signature
- reject timestamp skew > 5 minutes
- reject payload > `NOC_LHP_CALLBACK_MAX_BYTES`

Duplicate callback behavior:

- if `(source_loop, external_event_id)` exists, return the stored result without appending duplicate timeline/update rows.

## GitHub Engineering Transport

Create a new handler for outbox event:

```text
engineering_handoff_requested
```

Transport behavior:

1. Fetch `CaseHandoff`, case, knowledge refs, verification objectives.
2. Render a GitHub issue in `AS215932/network-operations`.
3. Apply labels:
   - `loop:candidate`
   - `noc`
   - `engineering-handoff`
   - `monitoring`
   - `disk` for this case
4. Never apply `loop:approved`.
5. Dedupe by hidden markers:
   - `noc-case-id:{case_id}`
   - `noc-lhp-handoff-id:{handoff_id}`
6. Store delivery state in `handoff_transport_deliveries`.

Issue body must contain:

- human-readable bounded summary;
- explicit policy constraints;
- acceptance criteria;
- public control case link;
- hidden or fenced LHP-v1 pointer;
- payload hash.

Engineering must treat the GitHub issue body as untrusted. The issue body is delivery/triage only.

Authoritative Engineering input is fetched from:

```text
GET /loop-handoff/v1/engineering/handoffs/{handoff_id}
```

If fetch fails or payload hash mismatches, Engineering posts `blocked` and does not run.

## Engineering Loop Changes

In `engineering-loop`:

1. Parse LHP-v1 pointer from GitHub issue body.
2. When an approved issue is selected:
   - callback `accepted`;
   - fetch authoritative handoff payload from NOC;
   - callback `investigating`;
   - generate `request.md` from structured LHP fields only.
3. Existing issue body prose may be included only as untrusted background/evidence.
4. If the loop publishes a draft PR, callback:
   - `update_type = "change_planned"`
   - `status = "change_planned"`
   - evidence ref = PR URL
5. Only callback `implemented` when Engineering has evidence that remediation was actually applied, not merely drafted.
6. On run failure, callback `failed` or `needs_human`.

Map existing daemon outcomes:

```text
published draft PR -> change_planned
needs_triage -> needs_human
over_budget -> blocked
error -> failed
idle/locked/refused_ci -> no case update unless tied to selected handoff
```

## NOC Proactive Disk Flow

Modify proactive handling so handoff creation is not blocked by model/graph failures.

For fresh `disk_fill` hotspots:

1. Observe hotspot into CaseService.
2. Deduplicate by active fingerprint.
3. If `NOC_DISK_ALERT_HANDOFF_ENABLED=1`, create/update LHP case and Engineering handoff.
4. Include temporary suppression expiry if present.
5. Request Knowledge context if enabled.
6. Enqueue Engineering handoff delivery if enabled.
7. Do not directly remediate disk.
8. Do not create duplicate handoffs for repeated observations.

For the current fingerprint:

```yaml
fingerprint: 8fb421ff94bb1285
resource: 2a0c:b641:b50:2::1
filesystem: /
case_type: proactive_disk_condition
objective_key: resolve-low-root-filesystem-condition-v1
```

Acceptance criteria:

```text
monitoring alert clears
no repeated Discord notification loop
/health healthy
/health/cases healthy
/health/mcp healthy
/health/model healthy
CaseService outbox healthy
temporary suppression not converted to permanent suppression
Engineering reports remediation or NOC observes recovery
final evidence attached
Knowledge artifact proposal attached
outcome record emitted
```

## Discord Deduplication

Add CaseService-backed notification dedup for case lifecycle notifications.

Identity:

```text
case_id + fingerprint + notification_type
```

Add timeline events:

```text
case_notification_emitted
case_notification_duplicate_suppressed
```

Use this for proactive start/failure/report notifications so repeated failed investigations cannot spam Discord for the same active case/fingerprint.

Metric:

```text
noc_agent_discord_duplicate_suppressed_total
```

## NOC Verification Scheduler

Add a dedicated background task in `app/main.py`, started only when:

```bash
NOC_CASE_VERIFICATION_ENABLED=1
```

Use an fcntl singleton lock like the proactive loop:

```bash
CASE_VERIFICATION_LOCK_PATH=/var/lib/noc-agent/case-verifier.lock
```

Dry-run mode:

```bash
NOC_CASE_VERIFICATION_DRY_RUN=1
```

In dry-run, write check evidence but do not advance handoff/case states.

Auto-resolve requires:

```bash
NOC_CASE_AUTO_RESOLVE_ENABLED=1
```

### Disk case verification objectives

Create by default when Engineering handoff is created:

1. `disk_alert_cleared`
2. `health_root_ok`
3. `health_cases_ok`
4. `health_mcp_ok`
5. `health_model_ok`
6. `no_repeated_discord_loop`
7. `no_permanent_suppression`
8. `caseservice_outbox_healthy`

Default:

```text
required_consecutive_passes = 3
interval = 120s
```

### Check implementation

- `disk_alert_cleared`: query Prometheus for the exact host/mount using the same disk rule semantics:
  - pass if free ratio >= 20% and not projected to fill within 24h;
  - fail if still below threshold;
  - unknown if telemetry unavailable.
- health objectives: HTTP GET local NOC endpoints.
- outbox objective: fail if relevant outbox rows are failed or stuck beyond retry threshold.
- Discord loop objective: inspect CaseService notification/timeline events.
- suppression objective: fail if active suppression for fingerprint has no expiry.

Resolution requires all required objectives to pass for 3 consecutive checks.

If permanent suppression is detected, mark case `needs_human`.

## Knowledge Integration

### Pre-handoff context

Implement `KnowledgeTransport` protocol:

```python
request_context(request: KnowledgeContextRequest) -> KnowledgeContextResponse
propose_artifacts(request: KnowledgeArtifactProposalRequest) -> list[KnowledgeArtifact]
```

First NOC backend:

1. local SQLite export via `KnowledgeExportRetriever`;
2. optional CLI/MCP adapter later behind same protocol.

Production NOC should get a read-only knowledge export at:

```text
/opt/noc-knowledge/exports/knowledge.sqlite
/opt/noc-knowledge/exports/manifest.json
```

Network-operations should deploy this from `AS215932/knowledge` at a pinned `noc_knowledge_version`.

Knowledge context response includes:

- prior case refs
- runbook refs
- topology facts
- ownership facts
- known caveats
- policy constraints
- bounded summary

Failure behavior:

- append `knowledge_context_unavailable`;
- keep case open;
- continue Engineering handoff;
- retry later.

### Post-resolution learning

After case resolution:

1. create `OutcomeRecord`;
2. enqueue `knowledge_artifact_proposed`;
3. write proposed review-gated learning artifacts to `NOC_KNOWLEDGE_CANDIDATE_DIR`;
4. store `KnowledgeArtifact` rows with `review_status = pending`.

Artifacts to propose:

- root cause summary
- remediation summary
- runbook update proposal
- memory proposal
- private eval proposal
- schema/tool/guardrail improvement proposal, if applicable

Artifacts remain proposed. They must not become durable memory, policy, or runbook content without review.

## Outcome Record

Create on final case resolution.

Required fields:

```yaml
work_item_type: case
work_item_id: case_id
case_type: proactive_disk_condition
fingerprint: 8fb421ff94bb1285
agent_roles:
  - noc
  - engineering
  - knowledge
validation:
  alert_cleared: bool
  health_root_ok: bool
  health_cases_ok: bool
  health_mcp_ok: bool
  health_model_ok: bool
  discord_loop_repeated: bool
  rollback_needed: bool
safety:
  policy_violations: []
  unauthorized_tool_calls: []
  secrets_exposed: false
  permanent_suppression_created: false
learning:
  eval_created: bool
  runbook_updated: bool
  memory_created: bool
  schema_or_guardrail_improved: bool
final_score:
  outcome: float
  safety: float
  evidence: float
  learning: float
```

If no learning artifact is produced, store a `learning_gap_recorded` artifact explaining why.

## Security Guardrails

- Do not change existing `NOC_CONTROL_TOKEN` behavior.
- Do not expose generic webhooks through Caddy.
- Keep LHP endpoints internal from Engineering Loop to NOC over IPv6.
- Add firewall rule allowing `peers.loop.ipv6 -> noc:8000`.
- HMAC-sign Engineering fetch/callback traffic.
- Payload max: 64 KiB.
- Pydantic schemas use `extra="forbid"` unless an explicit bounded `extensions` field is present.
- Sanitize all free text.
- Secret-scan/redact callback, evidence, and knowledge payloads.
- Treat all agent/operator/issue text as evidence, not instruction.
- Engineering cannot mark handoff verified/resolved.
- Workflow cannot create permanent suppression.
- CaseService write failure must fail loudly in primary paths.

## Network-Operations Changes

Add:

1. `noc_knowledge_version` host var and read-only knowledge export checkout on `noc`.
2. LHP env vars in `noc-agent.env.ctmpl.j2`.
3. LHP env vars in `engineering-loop.env.ctmpl.j2`.
4. Firewall rule:
   ```yaml
   - { proto: tcp, dport: 8000, src: "{{ peers.loop.ipv6 }}", comment: "Engineering Loop LHP callbacks to noc-agent" }
   ```
5. Do not add public Caddy proxy paths for `/webhook/*`.
6. Promote `noc-agent`, `engineering-loop`, and `knowledge` pins through the normal `network-operations` workflow.

## Rollout

1. **Schema-only**
   - `NOC_LHP_ENABLED=1`
   - all delivery/verification/auto-resolve flags off.

2. **NOC dry-run handoff**
   - `NOC_DISK_ALERT_HANDOFF_ENABLED=1`
   - `NOC_ENGINEERING_HANDOFF_DELIVERY_ENABLED=0`
   - confirm case/handoff/objectives are created only once.

3. **Knowledge context enabled**
   - `NOC_KNOWLEDGE_CONTEXT_ENABLED=1`
   - confirm bounded artifacts or degraded event.

4. **GitHub Engineering delivery**
   - `NOC_ENGINEERING_HANDOFF_DELIVERY_ENABLED=1`
   - issue is `loop:candidate`, not `loop:approved`.

5. **Engineering callbacks**
   - enable `ENGINEERING_LOOP_LHP_CALLBACK_ENABLED=1`
   - human applies `loop:approved`
   - Engineering reports structured updates.

6. **Verifier dry-run**
   - `NOC_CASE_VERIFICATION_ENABLED=1`
   - `NOC_CASE_VERIFICATION_DRY_RUN=1`

7. **Verifier state updates**
   - dry-run off
   - auto-resolve still off.

8. **Auto-resolve**
   - `NOC_CASE_AUTO_RESOLVE_ENABLED=1`
   - case resolves only after objectives pass.

## Test Plan

### noc-agent unit/integration tests

Add tests for:

- case upsert by fingerprint;
- duplicate disk alert reuses case;
- duplicate disk alert does not create duplicate handoff;
- handoff state transition validation;
- Engineering cannot set verified/resolved;
- callback HMAC required;
- duplicate callback idempotency;
- callback invalid transition rejected;
- knowledge unavailable does not block handoff;
- verification objectives require 3 consecutive passes;
- alert flapping prevents resolution;
- health endpoint failure prevents resolution;
- outbox failure blocks resolution;
- permanent suppression blocks resolution;
- Discord duplicate notification suppressed;
- outcome record emitted;
- knowledge artifact proposed, not published;
- prompt-injection text stored as evidence only.

### engineering-loop tests

Add tests for:

- LHP pointer parsing;
- authoritative fetch required;
- payload hash mismatch blocks run;
- `published` maps to `change_planned`;
- callbacks are signed;
- callback failures are surfaced but do not expose secrets;
- issue prose is not used as authoritative instruction.

### network-operations tests

Add IaC tests for:

- NOC env flags render safely off by default;
- Engineering callback env renders;
- shared secret fields are Vault-rendered, not hardcoded;
- firewall allows loop to noc:8000;
- Caddy does not expose generic webhooks.

### Private evals

Add eval cases:

- `eval_noc_engineering_handoff_disk_case`
- `eval_no_discord_loop_on_duplicate_fingerprint`
- `eval_engineering_implemented_requires_noc_verification`
- `eval_knowledge_context_is_bounded_and_reviewable`
- `eval_prompt_injection_does_not_override_policy`
- `eval_no_permanent_suppression_without_approval`
- `eval_outbox_failure_blocks_resolution`

## Acceptance Criteria

Complete when:

- CaseService owns handoff, verification, knowledge artifact, and outcome state.
- NOC creates/updates a case for `8fb421ff94bb1285`.
- Repeated observations dedupe by fingerprint.
- NOC does not remediate disk directly.
- NOC requests Knowledge context.
- NOC creates exactly one Engineering handoff.
- GitHub issue is only delivery/triage and carries bounded LHP-v1 data.
- Human `loop:approved` gate remains required for Engineering execution.
- Engineering fetches authoritative payload from CaseService.
- Engineering sends authenticated structured callbacks.
- Engineering cannot resolve the case.
- NOC verifier verifies alert state, health, outbox, suppression, and Discord dedup.
- Case resolves only after objectives pass.
- Knowledge receives final proposed learning artifacts.
- Outcome record exists.
- Private evals exist.
- Existing health endpoints remain healthy.






<!-- pi-plan-progress:start -->
## Progress

Status legend: `[x]` done, `[-]` skipped, `[>]` deferred, `[!]` blocked, `[ ]` pending.

- [x] 1. Add LHP-v1 feature flags, config, safety helpers, schemas... _(done)_
- [x] 2. Add CaseService handoff, callback, verification, knowledg... _(done)_
- [x] 3. Modify NOC proactive disk handling to create/update cases... _(done)_
- [x] 4. Add GitHub Engineering handoff delivery and Engineering L... _(done)_
- [x] 5. Add the dedicated NOC verification scheduler. _(done)_
- [x] 6. Add Knowledge pre-context and post-resolution artifact pr... _(done)_
- [ ] 7. Add production rollout config for NOC, Engineering Loop, ... _(pending)_
- [ ] 8. Add evals, tests, metrics, docs, and rollout validation. _(pending)_

<!-- pi-plan-progress:end -->
