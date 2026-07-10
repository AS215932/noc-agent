from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


DEFAULT_PRIMARY_MODEL = "openrouter:z-ai/glm-5.2"
# Venice entries are deliberately absent from the bundled defaults: a missing
# credential degrades /health/model, so deployments opt into venice fallbacks
# via config/env once VENICE_API_KEY is provisioned (production does this in
# network-operations host_vars).
DEFAULT_FALLBACK_MODELS = ["openrouter:deepseek/deepseek-v4-flash"]
DEFAULT_CONFIG_PATHS = (
    Path("/etc/noc-agent/config.toml"),
    Path(__file__).resolve().parent.parent / "config" / "noc-agent.toml",
)
LHP_ENGINEERING_SECRET_ENV = "NOC_LHP_ENGINEERING_SECRET"
LOOP_CONSOLE_SECRET_ENV = "NOC_LOOP_CONSOLE_SECRET"

load_dotenv()


@dataclass(frozen=True)
class ModelSettings:
    primary: str = DEFAULT_PRIMARY_MODEL
    fallbacks: list[str] = field(default_factory=lambda: list(DEFAULT_FALLBACK_MODELS))


@dataclass(frozen=True)
class OpenRouterProviderSettings:
    api_key_env: str = "OPENROUTER_API_KEY"
    management_api_key_env: str = "OPENROUTER_MANAGEMENT_API_KEY"
    app_title: str = "AS215932 NOC Agent"
    app_url: str = ""
    credit_monitoring_enabled: bool = True
    credit_probe_timeout_seconds: float = 5.0
    credit_probe_cache_seconds: int = 60
    warn_remaining_usd: float = 5.0
    critical_remaining_usd: float = 1.0


@dataclass(frozen=True)
class GoogleProviderSettings:
    enabled: bool = True
    api_key_env: str = "GOOGLE_API_KEY"
    gemini_api_key_env: str = "GEMINI_API_KEY"
    quota_project_id_env: str = "GEMINI_QUOTA_PROJECT_ID"


@dataclass(frozen=True)
class ProviderSettings:
    openrouter: OpenRouterProviderSettings = field(default_factory=OpenRouterProviderSettings)
    google: GoogleProviderSettings = field(default_factory=GoogleProviderSettings)


@dataclass(frozen=True)
class ProactiveLoopSettings:
    """Knobs for the proactive NOC loop. Ships disabled; ``load_proactive_settings``
    overlays ``NOC_PROACTIVE_*`` environment variables on top of the ``[proactive]``
    TOML table so deploys can flip flags without editing config files."""

    enabled: bool = False
    shadow: bool = True
    interval_s: int = 120
    deep_scan_s: int = 900
    max_investigations_per_cycle: int = 1
    max_investigations_per_day: int = 12
    max_cost_usd_per_day: float = 10.0
    cost_usd_per_investigation: float = 0.05
    # Re-post an unchanged hotspot digest at most this often (seconds). The
    # digest is otherwise posted only when the hotspot set changes, so a stable
    # set doesn't spam Discord every scan cycle.
    report_reassert_s: int = 3600
    # Don't re-investigate the same hotspot (by fingerprint) more often than
    # this (seconds). A persistent condition is diagnosed once; later cycles
    # report it via the de-dup gate instead of burning the daily investigation
    # budget — and the forced "investigated" digest post — on the same finding.
    investigation_cooldown_s: int = 21600
    # Retry failed investigations no more often than this. Keep this aligned
    # with the success cooldown by default; dependency outages should not page
    # Discord every scan cycle.
    investigation_failure_retry_s: int = 21600
    # Autonomously snooze non-urgent (LOW, not warrants_change) hotspots: mute
    # them from the digest for auto_snooze_ttl_s and best-effort ack the matching
    # Icinga WARNING problem (with an expiry so it auto-clears). Bounded per cycle.
    auto_snooze_enabled: bool = False
    auto_snooze_ttl_s: int = 86400
    auto_snooze_max_per_cycle: int = 3
    auto_snooze_icinga_ack: bool = True
    auto_heavy_probes: bool = False
    handoff_enabled: bool = False
    handoff_repo: str = "AS215932/network-operations"
    severity_floor: str = "MEDIUM"
    # Base URL for linking a hotspot's digest line to its NOC case
    # (e.g. https://noc.servify.network). Empty → show the case number only.
    control_public_url: str = ""
    # Base URL of the Agentic Observatory for linking a hotspot's digest line
    # to its insight-decision history (empty → no link).
    observatory_public_url: str = ""
    memory_dir: str = "/var/lib/noc-agent/memory"
    state_dir: str = "/var/lib/noc-agent/proactive"
    ruleset_version: str = "1"
    # Insight-decision records (agent-core InsightDecisionRecord per surface/
    # withhold decision, incl. deliberate silence). Read-only reporting; ships
    # off. Citations additionally require the pinned knowledge export
    # ([loop_handoff].knowledge_export_sqlite) to be present.
    insight_records_enabled: bool = False
    insight_knowledge_citations: bool = False
    # Re-emit an unchanged per-fingerprint decision at most this often (seconds);
    # the loop runs every interval_s, so this bounds stream volume.
    insight_reassert_s: int = 21600
    insight_max_per_cycle: int = 32
    # Hotspot score at or above which expected_utility saturates at 1.0.
    insight_score_norm: float = 100.0


@dataclass(frozen=True)
class LoopHandoffSettings:
    """Dormant LHP-v1 cross-loop coordination settings.

    All behavior-changing switches default off. The shared secret value is not
    stored here; only whether its env var is configured is surfaced.
    """

    enabled: bool = False
    engineering_handoff_delivery_enabled: bool = False
    engineering_handoff_transport: str = "github_issue"
    engineering_handoff_repo: str = "AS215932/network-operations"
    knowledge_context_enabled: bool = False
    knowledge_export_sqlite: str = "/opt/noc-knowledge/exports/knowledge.sqlite"
    knowledge_export_manifest: str = "/opt/noc-knowledge/exports/manifest.json"
    knowledge_context_max_artifacts: int = 10
    knowledge_context_max_tokens_equivalent: int = 3000
    knowledge_context_timeout_s: int = 20
    knowledge_candidate_dir: str = "/var/lib/noc-agent/knowledge-candidates"
    case_verification_enabled: bool = False
    case_verification_dry_run: bool = True
    case_auto_resolve_enabled: bool = False
    case_verification_interval_s: int = 120
    case_verification_required_consecutive_passes: int = 3
    disk_alert_handoff_enabled: bool = False
    callback_max_bytes: int = 65536
    engineering_secret_configured: bool = False


@dataclass(frozen=True)
class NocAgentSettings:
    model: ModelSettings = field(default_factory=ModelSettings)
    providers: ProviderSettings = field(default_factory=ProviderSettings)
    proactive: ProactiveLoopSettings = field(default_factory=ProactiveLoopSettings)
    loop_handoff: LoopHandoffSettings = field(default_factory=LoopHandoffSettings)
    source_path: str | None = None
    load_errors: list[str] = field(default_factory=list)


def load_settings() -> NocAgentSettings:
    """Load NOC Agent settings from TOML with safe built-in defaults.

    Secrets are intentionally not loaded from this file; provider settings only
    name the environment variables that should hold secret values.
    """

    explicit_path = os.getenv("NOC_AGENT_CONFIG", "").strip()
    errors: list[str] = []
    source: Path | None = None
    data: dict[str, Any] = {}

    candidates = [Path(explicit_path)] if explicit_path else list(DEFAULT_CONFIG_PATHS)
    for path in candidates:
        if not path.exists():
            if explicit_path:
                errors.append(f"Config file does not exist: {path}")
            continue
        source = path
        try:
            with path.open("rb") as handle:
                loaded = tomllib.load(handle)
            if isinstance(loaded, dict):
                data = loaded
            else:  # pragma: no cover - tomllib always returns dict
                errors.append(f"Config file did not contain a TOML table: {path}")
            break
        except Exception as exc:
            errors.append(f"Config file could not be loaded: {path}: {type(exc).__name__}")
            break

    return NocAgentSettings(
        model=_model_settings(data.get("model", {}), errors),
        providers=ProviderSettings(
            openrouter=_openrouter_settings(_provider_table(data, "openrouter"), errors),
            google=_google_settings(_provider_table(data, "google"), errors),
        ),
        proactive=_proactive_settings(data.get("proactive", {}), errors),
        loop_handoff=_loop_handoff_settings(data.get("loop_handoff", {}), errors),
        source_path=str(source) if source else None,
        load_errors=errors,
    )


def load_loop_handoff_settings() -> LoopHandoffSettings:
    """LHP-v1 settings with ``NOC_*`` environment overrides applied.

    The returned object deliberately exposes only whether the Engineering Loop
    shared secret is configured, not the secret value itself.
    """

    base = load_settings().loop_handoff
    transport = _env_str("NOC_ENGINEERING_HANDOFF_TRANSPORT", base.engineering_handoff_transport)
    if transport not in {"github_issue", "http", "queue"}:
        transport = base.engineering_handoff_transport
    return LoopHandoffSettings(
        enabled=_env_bool("NOC_LHP_ENABLED", base.enabled),
        engineering_handoff_delivery_enabled=_env_bool(
            "NOC_ENGINEERING_HANDOFF_DELIVERY_ENABLED", base.engineering_handoff_delivery_enabled
        ),
        engineering_handoff_transport=transport,
        engineering_handoff_repo=_env_str("NOC_ENGINEERING_HANDOFF_REPO", base.engineering_handoff_repo),
        knowledge_context_enabled=_env_bool("NOC_KNOWLEDGE_CONTEXT_ENABLED", base.knowledge_context_enabled),
        knowledge_export_sqlite=_env_str("NOC_KNOWLEDGE_EXPORT_SQLITE", base.knowledge_export_sqlite),
        knowledge_export_manifest=_env_str("NOC_KNOWLEDGE_EXPORT_MANIFEST", base.knowledge_export_manifest),
        knowledge_context_max_artifacts=_env_int(
            "NOC_KNOWLEDGE_CONTEXT_MAX_ARTIFACTS", base.knowledge_context_max_artifacts
        ),
        knowledge_context_max_tokens_equivalent=_env_int(
            "NOC_KNOWLEDGE_CONTEXT_MAX_TOKENS_EQUIVALENT", base.knowledge_context_max_tokens_equivalent
        ),
        knowledge_context_timeout_s=_env_int("NOC_KNOWLEDGE_CONTEXT_TIMEOUT_S", base.knowledge_context_timeout_s),
        knowledge_candidate_dir=_env_str("NOC_KNOWLEDGE_CANDIDATE_DIR", base.knowledge_candidate_dir),
        case_verification_enabled=_env_bool("NOC_CASE_VERIFICATION_ENABLED", base.case_verification_enabled),
        case_verification_dry_run=_env_bool("NOC_CASE_VERIFICATION_DRY_RUN", base.case_verification_dry_run),
        case_auto_resolve_enabled=_env_bool("NOC_CASE_AUTO_RESOLVE_ENABLED", base.case_auto_resolve_enabled),
        case_verification_interval_s=_env_int("NOC_CASE_VERIFICATION_INTERVAL_S", base.case_verification_interval_s),
        case_verification_required_consecutive_passes=_env_int(
            "NOC_CASE_VERIFICATION_REQUIRED_CONSECUTIVE_PASSES", base.case_verification_required_consecutive_passes
        ),
        disk_alert_handoff_enabled=_env_bool("NOC_DISK_ALERT_HANDOFF_ENABLED", base.disk_alert_handoff_enabled),
        callback_max_bytes=_env_int("NOC_LHP_CALLBACK_MAX_BYTES", base.callback_max_bytes),
        engineering_secret_configured=bool(os.getenv(LHP_ENGINEERING_SECRET_ENV, "").strip()),
    )


def load_proactive_settings() -> ProactiveLoopSettings:
    """Proactive-loop settings with ``NOC_PROACTIVE_*`` env overrides applied.

    Precedence: environment variable > ``[proactive]`` TOML table > default.
    This is the entry point the runtime loop uses; ``load_settings().proactive``
    returns the TOML-only view.
    """

    base = load_settings().proactive
    return ProactiveLoopSettings(
        enabled=_env_bool("NOC_PROACTIVE_ENABLED", base.enabled),
        shadow=_env_bool("NOC_PROACTIVE_SHADOW", base.shadow),
        interval_s=_env_int("NOC_PROACTIVE_INTERVAL_S", base.interval_s),
        deep_scan_s=_env_int("NOC_PROACTIVE_DEEP_SCAN_S", base.deep_scan_s),
        max_investigations_per_cycle=_env_int(
            "NOC_PROACTIVE_MAX_INVESTIGATIONS_PER_CYCLE", base.max_investigations_per_cycle
        ),
        max_investigations_per_day=_env_int(
            "NOC_PROACTIVE_MAX_INVESTIGATIONS_PER_DAY", base.max_investigations_per_day
        ),
        max_cost_usd_per_day=_env_float("NOC_PROACTIVE_MAX_COST_USD_PER_DAY", base.max_cost_usd_per_day),
        cost_usd_per_investigation=_env_float(
            "NOC_PROACTIVE_COST_USD_PER_INVESTIGATION", base.cost_usd_per_investigation
        ),
        report_reassert_s=_env_int("NOC_PROACTIVE_REPORT_REASSERT_S", base.report_reassert_s),
        investigation_cooldown_s=_env_int("NOC_PROACTIVE_INVESTIGATION_COOLDOWN_S", base.investigation_cooldown_s),
        investigation_failure_retry_s=_env_int(
            "NOC_PROACTIVE_INVESTIGATION_FAILURE_RETRY_S", base.investigation_failure_retry_s
        ),
        auto_snooze_enabled=_env_bool("NOC_PROACTIVE_AUTO_SNOOZE_ENABLED", base.auto_snooze_enabled),
        auto_snooze_ttl_s=_env_int("NOC_PROACTIVE_AUTO_SNOOZE_TTL_S", base.auto_snooze_ttl_s),
        auto_snooze_max_per_cycle=_env_int("NOC_PROACTIVE_AUTO_SNOOZE_MAX_PER_CYCLE", base.auto_snooze_max_per_cycle),
        auto_snooze_icinga_ack=_env_bool("NOC_PROACTIVE_AUTO_SNOOZE_ICINGA_ACK", base.auto_snooze_icinga_ack),
        auto_heavy_probes=_env_bool("NOC_PROACTIVE_AUTO_HEAVY_PROBES", base.auto_heavy_probes),
        handoff_enabled=_env_bool("NOC_PROACTIVE_HANDOFF_ENABLED", base.handoff_enabled),
        handoff_repo=_env_str("NOC_PROACTIVE_HANDOFF_REPO", base.handoff_repo),
        severity_floor=_env_str("NOC_PROACTIVE_SEVERITY_FLOOR", base.severity_floor).upper(),
        control_public_url=_env_str("NOC_CONTROL_PUBLIC_URL", base.control_public_url),
        observatory_public_url=_env_str("NOC_OBSERVATORY_PUBLIC_URL", base.observatory_public_url),
        memory_dir=_env_str("NOC_PROACTIVE_MEMORY_DIR", base.memory_dir),
        state_dir=_env_str("NOC_PROACTIVE_STATE_DIR", base.state_dir),
        ruleset_version=_env_str("NOC_PROACTIVE_RULESET_VERSION", base.ruleset_version),
        insight_records_enabled=_env_bool("NOC_INSIGHT_RECORDS_ENABLED", base.insight_records_enabled),
        insight_knowledge_citations=_env_bool(
            "NOC_INSIGHT_KNOWLEDGE_CITATIONS", base.insight_knowledge_citations
        ),
        insight_reassert_s=_env_int("NOC_INSIGHT_REASSERT_S", base.insight_reassert_s),
        insight_max_per_cycle=_env_int("NOC_INSIGHT_MAX_PER_CYCLE", base.insight_max_per_cycle),
        insight_score_norm=_env_float("NOC_INSIGHT_SCORE_NORM", base.insight_score_norm),
    )


def _loop_handoff_settings(table: Any, errors: list[str]) -> LoopHandoffSettings:
    if not isinstance(table, dict):
        if table not in ({}, None):
            errors.append("[loop_handoff] must be a TOML table")
        return LoopHandoffSettings()
    defaults = LoopHandoffSettings()
    transport = _str_value(table, "engineering_handoff_transport", defaults.engineering_handoff_transport, errors)
    if transport not in {"github_issue", "http", "queue"}:
        errors.append("[loop_handoff].engineering_handoff_transport must be one of: github_issue, http, queue")
        transport = defaults.engineering_handoff_transport
    return LoopHandoffSettings(
        enabled=_bool_value(table, "enabled", defaults.enabled, errors),
        engineering_handoff_delivery_enabled=_bool_value(
            table, "engineering_handoff_delivery_enabled", defaults.engineering_handoff_delivery_enabled, errors
        ),
        engineering_handoff_transport=transport,
        engineering_handoff_repo=_str_value(
            table, "engineering_handoff_repo", defaults.engineering_handoff_repo, errors
        ),
        knowledge_context_enabled=_bool_value(
            table, "knowledge_context_enabled", defaults.knowledge_context_enabled, errors
        ),
        knowledge_export_sqlite=_str_value(table, "knowledge_export_sqlite", defaults.knowledge_export_sqlite, errors),
        knowledge_export_manifest=_str_value(
            table, "knowledge_export_manifest", defaults.knowledge_export_manifest, errors
        ),
        knowledge_context_max_artifacts=_int_value(
            table, "knowledge_context_max_artifacts", defaults.knowledge_context_max_artifacts, errors
        ),
        knowledge_context_max_tokens_equivalent=_int_value(
            table,
            "knowledge_context_max_tokens_equivalent",
            defaults.knowledge_context_max_tokens_equivalent,
            errors,
        ),
        knowledge_context_timeout_s=_int_value(
            table, "knowledge_context_timeout_s", defaults.knowledge_context_timeout_s, errors
        ),
        knowledge_candidate_dir=_str_value(table, "knowledge_candidate_dir", defaults.knowledge_candidate_dir, errors),
        case_verification_enabled=_bool_value(
            table, "case_verification_enabled", defaults.case_verification_enabled, errors
        ),
        case_verification_dry_run=_bool_value(
            table, "case_verification_dry_run", defaults.case_verification_dry_run, errors
        ),
        case_auto_resolve_enabled=_bool_value(
            table, "case_auto_resolve_enabled", defaults.case_auto_resolve_enabled, errors
        ),
        case_verification_interval_s=_int_value(
            table, "case_verification_interval_s", defaults.case_verification_interval_s, errors
        ),
        case_verification_required_consecutive_passes=_int_value(
            table,
            "case_verification_required_consecutive_passes",
            defaults.case_verification_required_consecutive_passes,
            errors,
        ),
        disk_alert_handoff_enabled=_bool_value(
            table, "disk_alert_handoff_enabled", defaults.disk_alert_handoff_enabled, errors
        ),
        callback_max_bytes=_int_value(table, "callback_max_bytes", defaults.callback_max_bytes, errors),
        engineering_secret_configured=False,
    )


def _proactive_settings(table: Any, errors: list[str]) -> ProactiveLoopSettings:
    if not isinstance(table, dict):
        if table not in ({}, None):
            errors.append("[proactive] must be a TOML table")
        return ProactiveLoopSettings()
    defaults = ProactiveLoopSettings()
    return ProactiveLoopSettings(
        enabled=_bool_value(table, "enabled", defaults.enabled, errors),
        shadow=_bool_value(table, "shadow", defaults.shadow, errors),
        interval_s=_int_value(table, "interval_s", defaults.interval_s, errors),
        deep_scan_s=_int_value(table, "deep_scan_s", defaults.deep_scan_s, errors),
        max_investigations_per_cycle=_int_value(
            table, "max_investigations_per_cycle", defaults.max_investigations_per_cycle, errors
        ),
        max_investigations_per_day=_int_value(
            table, "max_investigations_per_day", defaults.max_investigations_per_day, errors
        ),
        max_cost_usd_per_day=_float_value(table, "max_cost_usd_per_day", defaults.max_cost_usd_per_day, errors),
        cost_usd_per_investigation=_float_value(
            table, "cost_usd_per_investigation", defaults.cost_usd_per_investigation, errors
        ),
        report_reassert_s=_int_value(table, "report_reassert_s", defaults.report_reassert_s, errors),
        investigation_cooldown_s=_int_value(
            table, "investigation_cooldown_s", defaults.investigation_cooldown_s, errors
        ),
        investigation_failure_retry_s=_int_value(
            table, "investigation_failure_retry_s", defaults.investigation_failure_retry_s, errors
        ),
        auto_snooze_enabled=_bool_value(table, "auto_snooze_enabled", defaults.auto_snooze_enabled, errors),
        auto_snooze_ttl_s=_int_value(table, "auto_snooze_ttl_s", defaults.auto_snooze_ttl_s, errors),
        auto_snooze_max_per_cycle=_int_value(
            table, "auto_snooze_max_per_cycle", defaults.auto_snooze_max_per_cycle, errors
        ),
        auto_snooze_icinga_ack=_bool_value(table, "auto_snooze_icinga_ack", defaults.auto_snooze_icinga_ack, errors),
        auto_heavy_probes=_bool_value(table, "auto_heavy_probes", defaults.auto_heavy_probes, errors),
        handoff_enabled=_bool_value(table, "handoff_enabled", defaults.handoff_enabled, errors),
        handoff_repo=_str_value(table, "handoff_repo", defaults.handoff_repo, errors),
        severity_floor=_str_value(table, "severity_floor", defaults.severity_floor, errors),
        control_public_url=_str_value(table, "control_public_url", defaults.control_public_url, errors),
        observatory_public_url=_str_value(table, "observatory_public_url", defaults.observatory_public_url, errors),
        memory_dir=_str_value(table, "memory_dir", defaults.memory_dir, errors),
        state_dir=_str_value(table, "state_dir", defaults.state_dir, errors),
        ruleset_version=_str_value(table, "ruleset_version", defaults.ruleset_version, errors),
        insight_records_enabled=_bool_value(
            table, "insight_records_enabled", defaults.insight_records_enabled, errors
        ),
        insight_knowledge_citations=_bool_value(
            table, "insight_knowledge_citations", defaults.insight_knowledge_citations, errors
        ),
        insight_reassert_s=_int_value(table, "insight_reassert_s", defaults.insight_reassert_s, errors),
        insight_max_per_cycle=_int_value(table, "insight_max_per_cycle", defaults.insight_max_per_cycle, errors),
        insight_score_norm=_float_value(table, "insight_score_norm", defaults.insight_score_norm, errors),
    )


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value.strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return float(value.strip())
    except ValueError:
        return default


def _provider_table(data: dict[str, Any], provider: str) -> dict[str, Any]:
    providers = data.get("providers", {})
    if not isinstance(providers, dict):
        return {}
    table = providers.get(provider, {})
    return table if isinstance(table, dict) else {}


def _model_settings(table: Any, errors: list[str]) -> ModelSettings:
    if not isinstance(table, dict):
        if table not in ({}, None):
            errors.append("[model] must be a TOML table")
        return ModelSettings()

    primary = _str_value(table, "primary", DEFAULT_PRIMARY_MODEL, errors)
    fallbacks = _str_list_value(table, "fallbacks", list(DEFAULT_FALLBACK_MODELS), errors)
    return ModelSettings(primary=primary, fallbacks=fallbacks)


def _openrouter_settings(table: dict[str, Any], errors: list[str]) -> OpenRouterProviderSettings:
    defaults = OpenRouterProviderSettings()
    return OpenRouterProviderSettings(
        api_key_env=_str_value(table, "api_key_env", defaults.api_key_env, errors),
        management_api_key_env=_str_value(table, "management_api_key_env", defaults.management_api_key_env, errors),
        app_title=_str_value(table, "app_title", defaults.app_title, errors),
        app_url=_str_value(table, "app_url", defaults.app_url, errors),
        credit_monitoring_enabled=_bool_value(
            table, "credit_monitoring_enabled", defaults.credit_monitoring_enabled, errors
        ),
        credit_probe_timeout_seconds=_float_value(
            table, "credit_probe_timeout_seconds", defaults.credit_probe_timeout_seconds, errors
        ),
        credit_probe_cache_seconds=_int_value(
            table, "credit_probe_cache_seconds", defaults.credit_probe_cache_seconds, errors
        ),
        warn_remaining_usd=_float_value(table, "warn_remaining_usd", defaults.warn_remaining_usd, errors),
        critical_remaining_usd=_float_value(table, "critical_remaining_usd", defaults.critical_remaining_usd, errors),
    )


def _google_settings(table: dict[str, Any], errors: list[str]) -> GoogleProviderSettings:
    defaults = GoogleProviderSettings()
    return GoogleProviderSettings(
        enabled=_bool_value(table, "enabled", defaults.enabled, errors),
        api_key_env=_str_value(table, "api_key_env", defaults.api_key_env, errors),
        gemini_api_key_env=_str_value(table, "gemini_api_key_env", defaults.gemini_api_key_env, errors),
        quota_project_id_env=_str_value(table, "quota_project_id_env", defaults.quota_project_id_env, errors),
    )


def _str_value(table: dict[str, Any], key: str, default: str, errors: list[str]) -> str:
    value = table.get(key, default)
    if isinstance(value, str):
        return value.strip() or default
    errors.append(f"Config value {key!r} must be a string")
    return default


def _str_list_value(table: dict[str, Any], key: str, default: list[str], errors: list[str]) -> list[str]:
    value = table.get(key, default)
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return [item.strip() for item in value if item.strip()]
    errors.append(f"Config value {key!r} must be a list of strings")
    return default


def _bool_value(table: dict[str, Any], key: str, default: bool, errors: list[str]) -> bool:
    value = table.get(key, default)
    if isinstance(value, bool):
        return value
    errors.append(f"Config value {key!r} must be a boolean")
    return default


def _float_value(table: dict[str, Any], key: str, default: float, errors: list[str]) -> float:
    value = table.get(key, default)
    if isinstance(value, int | float):
        return float(value)
    errors.append(f"Config value {key!r} must be a number")
    return default


def _int_value(table: dict[str, Any], key: str, default: int, errors: list[str]) -> int:
    value = table.get(key, default)
    if isinstance(value, int):
        return value
    errors.append(f"Config value {key!r} must be an integer")
    return default
