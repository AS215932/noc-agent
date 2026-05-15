from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.golden_state import load_golden_manifest


class RouterManifestEntry(BaseModel):
    model_config = ConfigDict(extra="allow")

    role: str = ""
    os: str = ""
    routing: list[str] = Field(default_factory=list)


class GoldenManifest(BaseModel):
    model_config = ConfigDict(extra="allow")

    asn: int = 215932
    prefixes: list[str] = Field(default_factory=list)
    routers: dict[str, RouterManifestEntry] = Field(default_factory=dict)
    critical_services: list[str] = Field(default_factory=list)
    drift_invariants: dict[str, Any] = Field(default_factory=dict)
    schema_version: str = "legacy.v1"


class PerimeterContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = "2026-05-15.v1"
    local_asn: int
    internal_prefixes: list[str] = Field(default_factory=list)
    host_roles: dict[str, str] = Field(default_factory=dict)
    host_os: dict[str, str] = Field(default_factory=dict)
    monitoring_endpoints: list[str] = Field(default_factory=list)
    expected_domains: list[str] = Field(default_factory=list)
    critical_services: list[str] = Field(default_factory=list)
    manifest_hash: str

    @classmethod
    def from_settings_and_manifest(cls) -> "PerimeterContext":
        raw = load_golden_manifest()
        manifest = GoldenManifest.model_validate(raw)
        manifest_payload = manifest.model_dump(mode="json")
        manifest_hash = hashlib.sha256(json.dumps(manifest_payload, sort_keys=True).encode()).hexdigest()[:16]
        return cls(
            local_asn=manifest.asn,
            internal_prefixes=list(manifest.prefixes),
            host_roles={name: entry.role for name, entry in manifest.routers.items() if entry.role},
            host_os={name: entry.os for name, entry in manifest.routers.items() if entry.os},
            monitoring_endpoints=["prometheus", "icinga2", "hyrule-mcp"],
            expected_domains=["as215932.net"],
            critical_services=list(manifest.critical_services),
            manifest_hash=manifest_hash,
        )

    def prompt_block(self, *, max_chars: int = 1800) -> str:
        payload = self.model_dump(mode="json")
        text = json.dumps(payload, sort_keys=True)
        if len(text) > max_chars:
            text = text[: max_chars - 3] + "..."
        return f"Perimeter context (non-secret, redacted, bounded):\n{text}"


@dataclass(slots=True)
class RuntimeDeps:
    incident_memory: Any
    mcp_runtime: Any | None = None
    perimeter_context: PerimeterContext | None = None
    model_override: Any | None = None

    @classmethod
    def build(cls, *, incident_memory: Any, mcp_runtime: Any | None = None, model_override: Any | None = None):
        return cls(
            incident_memory=incident_memory,
            mcp_runtime=mcp_runtime,
            perimeter_context=PerimeterContext.from_settings_and_manifest(),
            model_override=model_override,
        )

