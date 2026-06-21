# Legacy IncidentMemory deprecation audit

CaseService is now the source of truth for new reactive/control/proactive case
paths. This document tracks the remaining legacy code that is intentionally kept
only until the next removal tranches delete the old implementation and tests.

## Current cutover state

- `NOC_CASESERVICE_REACTIVE_PRIMARY=1` routes Alertmanager/Icinga intake to CaseService and does not call legacy `IncidentMemory.intake_alert`.
- Reactive webhooks now fail loudly with `503` when reactive-primary is not enabled; the legacy reactive webhook fallback has been removed.
- `NOC_CASESERVICE_CONTROL_PRIMARY=1` routes `/control/cases` to CaseService.
- `NOC_CASESERVICE_REACTIVE_PRIMARY=1` also routes `/control/cases` list/detail/events/comments/decisions/manual investigations to CaseService so reactive-primary cases remain operator-visible without legacy fallback.
- Legacy `/control/cases` and `/control/incidents` fallback paths now return `410` when no CaseService primary route is active.
- Proactive investigations now create/claim CaseService cases and use CaseService-backed graph memory instead of legacy `IncidentMemory`.
- `NOC_PROACTIVE_ENABLED=1` starts the CaseService runtime for proactive case ownership even when shadow/reactive/control flags are unset, and routes `/control/cases` to CaseService for proactive case links.
- CaseService graph runs must pass an explicit CaseService graph memory adapter; graph runtime rejects CaseService graph cases that would otherwise fall back to legacy memory.

## Remaining intentional legacy code

These remain for legacy-only modules/tests and compatibility surfaces that have
not yet been deleted:

- `app/incident_memory.py`: legacy in-memory/Redis case implementation.
- `app/graph_runtime.py`: legacy wrappers and global `INCIDENT_MEMORY` for legacy graph tests and any remaining non-CaseService custom embeddings.
- Legacy tests under `tests/test_case_intake.py` and legacy sections of `tests/test_graph_runtime.py`.
- Transitional helper imports in `app/main.py` for rendering graph case titles/events until graph runtime owns those shapes directly.

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

1. Delete legacy-only graph runtime wrappers (`intake_alert`, legacy pending summaries, global default memory) after remaining tests/custom embeddings are moved.
2. Replace `case_display_title` / `case_event_from_alert` imports with CaseService/graph-owned helpers.
3. Delete `app/incident_memory.py` and legacy-only tests.
4. Re-run a repo-wide dead-code sweep for imports, helpers, docs, and flags after deletion.
