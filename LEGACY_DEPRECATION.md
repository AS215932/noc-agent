# Legacy IncidentMemory deprecation audit

CaseService is now the source of truth for new reactive/control case paths.
This document tracks the remaining legacy code that is intentionally kept only
until the next removal tranches delete the old implementation and tests.

## Current cutover state

- `NOC_CASESERVICE_REACTIVE_PRIMARY=1` routes Alertmanager/Icinga intake to CaseService and does not call legacy `IncidentMemory.intake_alert`.
- Reactive webhooks now fail loudly with `503` when reactive-primary is not enabled; the legacy reactive webhook fallback has been removed.
- `NOC_CASESERVICE_CONTROL_PRIMARY=1` routes `/control/cases` to CaseService.
- `NOC_CASESERVICE_REACTIVE_PRIMARY=1` also routes `/control/cases` list/detail/events/comments/decisions/manual investigations to CaseService so reactive-primary cases remain operator-visible without legacy fallback.
- Legacy `/control/cases` and `/control/incidents` fallback paths now return `410` when no CaseService primary route is active.
- CaseService graph runs must pass an explicit CaseService graph memory adapter; graph runtime rejects CaseService graph cases that would otherwise fall back to legacy memory.

## Remaining intentional legacy code

These remain for legacy-only modules/tests and proactive code that has not yet
been moved to CaseService:

- `app/incident_memory.py`: legacy in-memory/Redis case implementation.
- `app/graph_runtime.py`: legacy wrappers and global `INCIDENT_MEMORY` for legacy graph/proactive runs.
- `app/proactive/investigate.py`: proactive investigation intake still calls `graph_runtime.intake_alert`.
- `app/main.py`: startup still passes legacy incident memory into the proactive loop.
- Legacy tests under `tests/test_case_intake.py` and legacy sections of `tests/test_graph_runtime.py`.

## Required primary-mode rollout

Use CaseService primary modes for live reactive/control operation:

```bash
NOC_CASESERVICE_REACTIVE_PRIMARY=1
NOC_CASESERVICE_CONTROL_PRIMARY=1
NOC_CASE_OUTBOX_ENABLED=1
```

Reactive-primary alone is enough for webhook intake and `/control/cases` case
visibility. Control-primary remains recommended for explicit control-plane
rollout clarity.

## Next removal candidates

1. Move proactive investigation intake/history to CaseService so it no longer needs `IncidentMemory`.
2. Delete legacy-only graph runtime wrappers (`intake_alert`, legacy pending summaries, global default memory) after proactive moves.
3. Delete `app/incident_memory.py` and legacy-only tests.
4. Re-run a repo-wide dead-code sweep for imports, helpers, docs, and flags after deletion.
