import os
from dataclasses import dataclass, field

from pydantic_ai.exceptions import ModelAPIError
from pydantic_ai.models import KnownModelName, Model
from pydantic_ai.models.fallback import FallbackModel

from app.config import DEFAULT_FALLBACK_MODELS, DEFAULT_PRIMARY_MODEL, NocAgentSettings, load_settings
from app.model_metrics import record_fallback_attempt, set_model_config
from app.safe_errors import classify_exception


DEFAULT_MODEL = DEFAULT_PRIMARY_MODEL


@dataclass(frozen=True)
class AgentModelConfig:
    primary_model: str
    fallback_models: list[str]
    configured_models: list[str]
    missing_credentials: list[str]
    unsupported_models: list[str]
    active_model_chain: list[str]
    config_source: str | None = None
    config_errors: list[str] = field(default_factory=list)
    provider_api_key_envs: dict[str, str] = field(default_factory=dict)


def configured_model_names(settings: NocAgentSettings | None = None) -> list[str]:
    settings = settings or load_settings()
    primary_from_env = "AGENT_MODEL" in os.environ
    primary = os.getenv("AGENT_MODEL", settings.model.primary).strip() or DEFAULT_MODEL
    if "AGENT_FALLBACK_MODELS" in os.environ:
        fallbacks = _split_models(os.getenv("AGENT_FALLBACK_MODELS", ""))
    elif primary_from_env:
        fallbacks = []
    else:
        fallbacks = list(settings.model.fallbacks)
    return [primary, *fallbacks]


def load_model_config() -> AgentModelConfig:
    settings = load_settings()
    _sync_provider_env(settings)
    names = configured_model_names(settings)
    missing: list[str] = []
    unsupported: list[str] = []
    active: list[str] = []
    for name in names:
        if _is_unsupported(name):
            unsupported.append(name)
            continue
        missing_reason = _missing_credential_reason(name, settings)
        if missing_reason:
            missing.append(f"{name}: {missing_reason}")
            continue
        active.append(name)

    if not active:
        active = [names[0]]

    config = AgentModelConfig(
        primary_model=names[0],
        fallback_models=names[1:],
        configured_models=names,
        missing_credentials=missing,
        unsupported_models=unsupported,
        active_model_chain=active,
        config_source=settings.source_path,
        config_errors=list(settings.load_errors),
        provider_api_key_envs=_provider_api_key_envs(settings),
    )
    set_model_config(
        configured_models=config.configured_models,
        missing_credentials=config.missing_credentials,
        unsupported_models=config.unsupported_models,
        active_model_chain=config.active_model_chain,
        config_source=config.config_source,
        config_errors=config.config_errors,
        provider_api_key_envs=config.provider_api_key_envs,
    )
    return config


def build_agent_model() -> Model | KnownModelName | str:
    config = load_model_config()
    if len(config.active_model_chain) < 2:
        return config.active_model_chain[0]
    return FallbackModel(
        config.active_model_chain[0],
        *config.active_model_chain[1:],
        fallback_on=[_record_fallback_exception, ModelAPIError],
    )


def _record_fallback_exception(exc: Exception) -> bool:
    safe = classify_exception(exc)
    record_fallback_attempt(getattr(exc, "model_name", "unknown"), safe.category)
    return False


def _split_models(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _sync_provider_env(settings: NocAgentSettings) -> None:
    _sync_env_alias(settings.providers.openrouter.api_key_env, "OPENROUTER_API_KEY")
    _sync_env_alias(settings.providers.openrouter.management_api_key_env, "OPENROUTER_MANAGEMENT_API_KEY")
    if settings.providers.openrouter.app_title and not os.getenv("OPENROUTER_APP_TITLE"):
        os.environ["OPENROUTER_APP_TITLE"] = settings.providers.openrouter.app_title
    if settings.providers.openrouter.app_url and not os.getenv("OPENROUTER_APP_URL"):
        os.environ["OPENROUTER_APP_URL"] = settings.providers.openrouter.app_url

    _sync_env_alias(settings.providers.google.gemini_api_key_env, "GEMINI_API_KEY")
    _sync_env_alias(settings.providers.google.api_key_env, "GOOGLE_API_KEY")
    if os.getenv("GEMINI_API_KEY") and not os.getenv("GOOGLE_API_KEY"):
        os.environ["GOOGLE_API_KEY"] = os.environ["GEMINI_API_KEY"]


def _sync_env_alias(source_env: str, target_env: str) -> None:
    if source_env != target_env and os.getenv(source_env) and not os.getenv(target_env):
        os.environ[target_env] = os.environ[source_env]


def _is_unsupported(model_name: str) -> bool:
    provider = _provider_from_model(model_name)
    if provider in {
        "anthropic",
        "bedrock",
        "cerebras",
        "cohere",
        "deepseek",
        "google",
        "google-gla",
        "google-vertex",
        "groq",
        "mistral",
        "ollama",
        "openai",
        "openai-chat",
        "openai-responses",
        "openrouter",
        "test",
        "xai",
    }:
        return False
    if provider.startswith("gateway/"):
        return False
    return provider == "unknown"


def _missing_credential_reason(model_name: str, settings: NocAgentSettings) -> str | None:
    provider = _provider_from_model(model_name)
    if provider == "openrouter":
        env_name = settings.providers.openrouter.api_key_env
        if not (os.getenv(env_name) or os.getenv("OPENROUTER_API_KEY")):
            return f"{env_name} is required"
    elif provider in {"google", "google-gla"} or model_name.startswith("gemini"):
        google_env = settings.providers.google.api_key_env
        gemini_env = settings.providers.google.gemini_api_key_env
        if not (os.getenv(google_env) or os.getenv(gemini_env) or os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")):
            return f"{google_env} or {gemini_env} is required"
    elif provider == "google-vertex":
        google_env = settings.providers.google.api_key_env
        if not (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.getenv(google_env) or os.getenv("GOOGLE_API_KEY")):
            return f"Google Application Default Credentials or {google_env} is required"
    elif provider == "anthropic":
        if not os.getenv("ANTHROPIC_API_KEY"):
            return "ANTHROPIC_API_KEY is required"
    elif provider in {"openai", "openai-chat", "openai-responses"}:
        if not os.getenv("OPENAI_API_KEY"):
            return "OPENAI_API_KEY is required"
    elif provider == "mistral":
        if not os.getenv("MISTRAL_API_KEY"):
            return "MISTRAL_API_KEY is required"
    elif provider == "groq":
        if not os.getenv("GROQ_API_KEY"):
            return "GROQ_API_KEY is required"
    elif provider == "cohere":
        if not os.getenv("COHERE_API_KEY"):
            return "COHERE_API_KEY is required"
    elif provider == "xai":
        if not os.getenv("XAI_API_KEY"):
            return "XAI_API_KEY is required"
    return None


def _provider_from_model(model_name: str) -> str:
    if ":" in model_name:
        return model_name.split(":", 1)[0]
    if model_name == "test":
        return "test"
    if model_name.startswith("gemini"):
        return "google-gla"
    if model_name.startswith("claude"):
        return "anthropic"
    if model_name.startswith(("gpt", "o1", "o3")):
        return "openai"
    return "unknown"


def _provider_api_key_envs(settings: NocAgentSettings) -> dict[str, str]:
    return {
        "openrouter": settings.providers.openrouter.api_key_env,
        "openrouter_management": settings.providers.openrouter.management_api_key_env,
        "google": f"{settings.providers.google.api_key_env} or {settings.providers.google.gemini_api_key_env}",
        "anthropic": "ANTHROPIC_API_KEY",
        "openai": "OPENAI_API_KEY",
        "mistral": "MISTRAL_API_KEY",
        "groq": "GROQ_API_KEY",
        "cohere": "COHERE_API_KEY",
        "xai": "XAI_API_KEY",
    }
