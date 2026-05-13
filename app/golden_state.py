from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any


_PROMPT_DIR = Path(__file__).parent / "prompts"


@lru_cache(maxsize=1)
def load_golden_manifest() -> dict[str, Any]:
    return json.loads((_PROMPT_DIR / "golden_state_manifest.json").read_text())


@lru_cache(maxsize=1)
def load_supervisor_context() -> str:
    manifest = json.dumps(load_golden_manifest(), indent=2, sort_keys=True)
    narrative = (_PROMPT_DIR / "supervisor_context.md").read_text()
    return f"{narrative}\n\nGolden-state manifest:\n```json\n{manifest}\n```"


def drift_findings_for(resource_id: str, telemetry_cache: dict[str, Any]) -> list[str]:
    manifest = load_golden_manifest()
    findings: list[str] = []

    if resource_id == "noc" and not telemetry_cache.get("mcp_connected", True):
        findings.append("Golden state requires MCP connectivity on noc, but telemetry reports it unavailable.")

    expected_targets = set(manifest["drift_invariants"]["monitoring_requires_targets"])
    observed_targets = set(telemetry_cache.get("prometheus_target_jobs", []))
    missing_targets = sorted(expected_targets - observed_targets)
    if observed_targets and missing_targets:
        findings.append(f"Prometheus target classes missing from live view: {', '.join(missing_targets)}.")

    return findings
