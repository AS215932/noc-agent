from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


DEFAULT_PRIMARY_MODEL = "openrouter:deepseek/deepseek-v4-pro"
DEFAULT_FALLBACK_MODELS = ["openrouter:anthropic/claude-sonnet-4.6"]
DEFAULT_CONFIG_PATHS = (
    Path("/etc/noc-agent/config.toml"),
    Path(__file__).resolve().parent.parent / "config" / "noc-agent.toml",
)

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
class NocAgentSettings:
    model: ModelSettings = field(default_factory=ModelSettings)
    providers: ProviderSettings = field(default_factory=ProviderSettings)
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
        source_path=str(source) if source else None,
        load_errors=errors,
    )


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
        critical_remaining_usd=_float_value(
            table, "critical_remaining_usd", defaults.critical_remaining_usd, errors
        ),
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
