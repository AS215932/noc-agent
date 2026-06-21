# Legacy IncidentMemory deprecation audit

CaseService is now the source of truth for new reactive/control/proactive case
paths. This document tracks the remaining legacy code that is intentionally kept
only until the next removal tranches delete the old implementation and tests.

## Current cutover state

- `NOC_CASESERVICE_REACTIVE_PRIMARY=1` routes Alertmanager/Icinga intake to CaseService and does not call legacy `IncidentMemory.intake_alert`.
- Reactive webhooks fail loudly with `503` when reactive-primary is not enabled; the legacy reactive webhook fallback has been removed.
- `NOC_CASESERVICE_CONTROL_PRIMARY=1` routes `/control/cases` to CaseService.
- `NOC_CASESERVICE_REACTIVE_PRIMARY=1` also routes `/control/cases` list/detail/events/comments/decisions/manual investigations to CaseService so reactive-primary cases remain operator-visible without legacy fallback.
- Legacy `/control/cases` and `/control/incidents` fallback paths return `410` when no CaseService primary route is active.
- Proactive investigations create/claim CaseService cases and use CaseService-backed graph memory instead of legacy `IncidentMemory`.
- `NOC_PROACTIVE_ENABLED=1` starts the CaseService runtime for proactive case ownership even when shadow/reactive/control flags are unset, and routes `/control/cases` to CaseService for proactive case links.
- Graph runtime no longer owns legacy intake/list/detail fallbacks. Graph execution, event injection, resume, and operator decisions require explicit case + graph memory.
- Shared alert identity/display helpers now live in `app/alert_utils.py` so app paths no longer import `app.incident_memory`.

## Remaining intentional legacy code

These remain only until the deletion tranche:

- `app/incident_memory.py`: legacy in-memory/Redis case implementation.
- `tests/test_case_intake.py`: legacy-only coverage to be deleted or rewritten.
- A few no-fallback assertions still instantiate `IncidentMemory` directly to prove primary paths do not read it.

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

## Next removal candidates

1. Rewrite/delete legacy-only tests under `tests/test_case_intake.py` and remaining no-fallback direct `IncidentMemory` fixtures.
2. Delete `app/incident_memory.py`.
3. Re-run a repo-wide dead-code sweep for imports, helpers, docs, and flags after deletion.
