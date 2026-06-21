# Legacy case-memory deletion audit

CaseService is now the source of truth for reactive, control-plane, and
proactive case paths. The old in-memory/Redis incident-memory implementation has
been removed.

## Current cutover state

- `NOC_CASESERVICE_REACTIVE_PRIMARY=1` routes Alertmanager/Icinga intake to CaseService.
- Reactive webhooks fail loudly with `503` when reactive-primary is not enabled; the legacy reactive webhook fallback has been removed.
- `NOC_CASESERVICE_CONTROL_PRIMARY=1` routes `/control/cases` to CaseService.
- `NOC_CASESERVICE_REACTIVE_PRIMARY=1` also routes `/control/cases` list/detail/events/comments/decisions/manual investigations to CaseService so reactive-primary cases remain operator-visible.
- Legacy `/control/cases` and `/control/incidents` fallback paths return `410` when no CaseService primary route is active.
- Proactive investigations create/claim CaseService cases and use CaseService-backed graph memory.
- `NOC_PROACTIVE_ENABLED=1` starts the CaseService runtime for proactive case ownership even when shadow/reactive/control flags are unset, and routes `/control/cases` to CaseService for proactive case links.
- Graph runtime no longer owns legacy intake/list/detail fallbacks. Graph execution, event injection, resume, and operator decisions require explicit case + graph memory.
- Shared alert identity/display helpers live in `app/alert_utils.py`.

## Removed code

- `app/incident_memory.py` and its local/Redis case implementation.
- Legacy-only intake tests have been rewritten against CaseService and shared alert helpers.
- Legacy incident/case-number alias lookup compatibility has been removed from app-mounted control and graph-memory paths.

## Required primary-mode rollout

Use CaseService primary modes for live operation:

```bash
NOC_CASESERVICE_REACTIVE_PRIMARY=1
NOC_CASESERVICE_CONTROL_PRIMARY=1
NOC_PROACTIVE_ENABLED=1
NOC_CASE_OUTBOX_ENABLED=1
```

Reactive-primary alone is enough for webhook intake and `/control/cases` case
visibility. Control-primary remains recommended for explicit control-plane
rollout clarity. Proactive-enabled starts CaseService for proactive case
ownership.

## Remaining cleanup candidates

1. Remove deprecated flag/documentation references once operators no longer need rollout breadcrumbs.
2. Remove any stale compatibility tests that mention legacy control/reactive rejection paths after the next breaking API cleanup.
