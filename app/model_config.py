import os
from dataclasses import dataclass

from pydantic_ai.exceptions import ModelAPIError
from pydantic_ai.models import KnownModelName, Model
from pydantic_ai.models.fallback import FallbackModel

from app.model_metrics import record_fallback_attempt, set_model_config
from app.safe_errors import classify_exception


DEFAULT_MODEL = "google-gla:gemini-3.1-pro"


@dataclass(frozen=True)
class AgentModelConfig:
    primary_model: str
    fallback_models: list[str]
    configured_models: list[str]
    missing_credentials: list[str]
    unsupported_models: list[str]
    active_model_chain: list[str]


def configured_model_names() -> list[str]:
    primary = os.getenv("AGENT_MODEL", DEFAULT_MODEL).strip() or DEFAULT_MODEL
    fallbacks = _split_models(os.getenv("AGENT_FALLBACK_MODELS", ""))
    return [primary, *fallbacks]


def load_model_config() -> AgentModelConfig:
    _sync_google_api_key()
    names = configured_model_names()
    missing: list[str] = []
    unsupported: list[str] = []
    active: list[str] = []
    for name in names:
        if _is_unsupported(name):
            unsupported.append(name)
            continue
        missing_reason = _missing_credential_reason(name)
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
    )
    set_model_config(
        configured_models=config.configured_models,
        missing_credentials=config.missing_credentials,
        unsupported_models=config.unsupported_models,
        active_model_chain=config.active_model_chain,
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


def _sync_google_api_key() -> None:
    if os.getenv("GEMINI_API_KEY") and not os.getenv("GOOGLE_API_KEY"):
        os.environ["GOOGLE_API_KEY"] = os.environ["GEMINI_API_KEY"]


def _is_unsupported(model_name: str) -> bool:
    return model_name.startswith("openrouter:")


def _missing_credential_reason(model_name: str) -> str | None:
    provider = model_name.split(":", 1)[0] if ":" in model_name else ""
    if provider == "google-gla" or model_name.startswith("gemini"):
        if not (os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")):
            return "GOOGLE_API_KEY or GEMINI_API_KEY is required"
    elif provider == "google-vertex":
        if not (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.getenv("GOOGLE_API_KEY")):
            return "Google Application Default Credentials or GOOGLE_API_KEY is required"
    elif provider == "anthropic":
        if not os.getenv("ANTHROPIC_API_KEY"):
            return "ANTHROPIC_API_KEY is required"
    elif provider in {"openai", "openai-chat"}:
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
