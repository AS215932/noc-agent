"""Autonomous Icinga acknowledgement.

The NOC agent self-signs a bounded ``acknowledge_icinga`` action (with the same
``HYRULE_MCP_ACTION_SIGNING_SECRET`` hyrule-mcp verifies) and calls the ack tool.
Used for two things:

* **take-ownership** — when an incoming alert starts an investigation, ack the
  Icinga problem so it stops re-paging humans while the agent works it;
* **non-urgent auto-snooze** — when the proactive loop mutes a LOW finding, ack
  the matching Icinga problem with an expiry so it auto-clears.

Both pass an ``expiry`` so an autonomous ack never permanently masks a problem,
and ``notify=False`` so acking doesn't itself page. Best-effort throughout: a
failure here must never break an investigation or a proactive cycle.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any

from app import log
from app.safe_errors import classify_exception, log_exception


def sign_ack_authorization(*, case_id: str, operator: str, auth_ttl_seconds: int = 300) -> dict[str, Any]:
    """HMAC-sign a bounded ``acknowledge_icinga`` authorization. Mirrors the
    graph's ``_action_authorization`` so hyrule-mcp accepts it; the short-lived
    ``expiry`` here is the *authorization* TTL (not the Icinga ack expiry)."""
    payload: dict[str, Any] = {
        "action_id": f"auto-ack-{int(time.time() * 1000)}",
        "case_id": case_id,
        "operator": operator,
        "action_class": "acknowledge_icinga",
        "expiry": int(time.time()) + max(1, auth_ttl_seconds),
    }
    secret = os.getenv("HYRULE_MCP_ACTION_SIGNING_SECRET") or os.getenv("NOC_APPROVAL_SIGNING_SECRET", "")
    if secret:
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        payload["signature"] = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return payload


async def acknowledge_icinga(
    runtime: Any,
    *,
    host_name: str,
    service_name: str | None,
    comment: str,
    case_id: str = "",
    author: str = "noc-agent",
    ack_ttl_seconds: int | None = None,
    notify: bool = False,
) -> dict[str, Any] | None:
    """Best-effort autonomous Icinga ack. ``ack_ttl_seconds`` sets the Icinga ack
    expiry (auto-clears). Returns the tool result, or ``None`` if it couldn't run
    (never raises)."""
    if runtime is None or not host_name:
        return None
    args: dict[str, Any] = {
        "host_name": host_name,
        "service_name": service_name,
        "author": author,
        "comment": comment,
        "notify": notify,
        "action_authorization": sign_ack_authorization(case_id=case_id, operator=author),
    }
    if ack_ttl_seconds is not None:
        args["expiry"] = int(time.time()) + int(ack_ttl_seconds)
    try:
        result = await runtime.call_tool("hyrule", "icinga_acknowledge_alert", args)
    except Exception as exc:  # transport/tooling failure must not propagate
        safe = classify_exception(exc)
        log_exception("icinga_ack_failed", exc, category=safe.category, host=host_name, service=service_name or "")
        return None
    if isinstance(result, dict) and result.get("ok"):
        log.info("icinga_acked", host=host_name, service=service_name or "", case=case_id, notify=notify)
    else:
        detail = result.get("sanitized_error") if isinstance(result, dict) else None
        log.warning("icinga_ack_rejected", host=host_name, service=service_name or "", detail=detail)
    return result
