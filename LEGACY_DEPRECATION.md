# Legacy IncidentMemory deprecation audit

This is the working removal map for the legacy reactive/control case store.
CaseService is the target source of truth for new reactive cases.

## Current cutover state

- `NOC_CASESERVICE_REACTIVE_PRIMARY=1` routes Alertmanager/Icinga intake to CaseService and does not call legacy `IncidentMemory.intake_alert`.
- `NOC_CASESERVICE_CONTROL_PRIMARY=1` routes `/control/cases` to CaseService.
- `NOC_CASESERVICE_REACTIVE_PRIMARY=1` also routes `/control/cases` list/detail/events/comments/decisions/manual investigations to CaseService so reactive-primary cases remain operator-visible without legacy fallback.
- CaseService graph runs must pass an explicit CaseService graph memory adapter; graph runtime now rejects CaseService graph cases that would otherwise fall back to legacy memory.
- `NOC_LEGACY_INCIDENT_MEMORY_ENABLED=0` is the kill switch for legacy reactive/control paths while retaining default compatibility when unset.

## Remaining intentional legacy entry points

These remain only for deployments that have not enabled CaseService primary modes:

- `app/incident_memory.py`: legacy in-memory/Redis case implementation.
- `app/graph_runtime.py`: legacy wrappers and global `INCIDENT_MEMORY` for legacy graph runs.
- `app/main.py`: legacy webhook intake when `NOC_CASESERVICE_REACTIVE_PRIMARY=0` and the legacy kill switch is enabled.
- `app/main.py`: legacy `/control/incidents` and `/control/cases` paths when no CaseService primary route is active and the legacy kill switch is enabled.
- Legacy tests under `tests/test_case_intake.py`, legacy sections of `tests/test_graph_runtime.py`, `tests/test_control_plane.py`, and `tests/test_webhook.py`.

## Kill switch behavior

Set:

```bash
NOC_LEGACY_INCIDENT_MEMORY_ENABLED=0
NOC_CASESERVICE_REACTIVE_PRIMARY=1
```

Recommended with:

```bash
NOC_CASESERVICE_CONTROL_PRIMARY=1
NOC_CASE_OUTBOX_ENABLED=1
```

When the kill switch is off (`0`):

- Reactive webhooks fail loudly with `503` unless `NOC_CASESERVICE_REACTIVE_PRIMARY=1` is enabled.
- Legacy control routes fail with `410` unless a CaseService primary route is active.
- CaseService primary routes continue to work and do not consult legacy `IncidentMemory`.

## Next removal candidates

1. Remove legacy webhook/control fallback after primary flags are stable in production with `NOC_LEGACY_INCIDENT_MEMORY_ENABLED=0`.
2. Delete `app/incident_memory.py` and legacy-only graph runtime wrappers.
3. Rewrite remaining legacy tests as CaseService tests or remove them with the deleted code.
4. Re-run a repo-wide dead-code sweep for imports, helpers, docs, and flags after deletion.
