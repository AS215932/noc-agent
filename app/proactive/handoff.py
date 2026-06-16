"""Phase 4: the engineering-loop handoff bridge.

When a proactive finding needs an actual config/docs change (not just a watch),
the loop opens a ``loop:candidate`` GitHub issue carrying the evidence chain and
golden-state references. A human promotes it to ``loop:approved``; the existing
engineering-loop daemon then drafts the PR (merge stays human). This is the
seam that joins the two loops into one human-gated autonomic system.

Idempotent by a fingerprint marker embedded in the issue body: re-detecting the
same hotspot updates the existing open issue with a "still firing" comment
rather than spawning duplicates. Ships disabled (``NOC_PROACTIVE_HANDOFF_ENABLED``)
and needs an issues-scoped ``NOC_GITHUB_TOKEN``.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable

from app import log
from app.proactive.models import Hotspot, utc_now
from app.safe_errors import classify_exception, log_exception


CANDIDATE_LABELS = ["loop:candidate", "agentic-isp"]
_API_BASE = "https://api.github.com"

# async (method, path, *, params, json[, headers]) -> (status_code, body)
Requester = Callable[..., Awaitable[tuple[int, Any]]]
# async () -> current bearer token
TokenProvider = Callable[[], Awaitable[str]]


def _marker(hotspot: Hotspot) -> str:
    return f"proactive-fingerprint:{hotspot.fingerprint()}"


def build_issue_body(hotspot: Hotspot, *, incident_id: str | None, manifest_hash: str | None) -> str:
    lines = [
        f"_Filed by the AS215932 proactive NOC loop ({hotspot.rule_id})._",
        "",
        f"## Finding\n{hotspot.summary}",
    ]
    if hotspot.change_rationale:
        lines.append(f"\n## Why a change may be needed\n{hotspot.change_rationale}")
    if hotspot.evidence:
        lines.append("\n## Evidence")
        for ev in hotspot.evidence:
            bits = [b for b in (ev.label, ev.value, ev.threshold, ev.query, ev.detail) if b]
            lines.append(f"- {' · '.join(bits)}")
    if hotspot.recommended_checks:
        lines.append("\n## Recommended checks")
        lines.extend(f"- {c}" for c in hotspot.recommended_checks)
    lines.append("\n## Context")
    lines.append(f"- resource: `{hotspot.resource}`  ·  category: `{hotspot.category}`  ·  severity: `{hotspot.severity}`")
    if incident_id:
        lines.append(f"- investigated NOC case: `{incident_id}`")
    if manifest_hash:
        lines.append(f"- golden-state manifest: `{manifest_hash}`")
    lines.append(
        "\n> Read-only proactive finding. Promote to `loop:approved` to let the engineering-loop draft a PR; "
        "merge stays human-gated."
    )
    lines.append(f"\n<!-- {_marker(hotspot)} -->")
    return "\n".join(lines)


class GitHubHandoff:
    def __init__(
        self,
        *,
        repo: str,
        token: str | None = None,
        token_provider: TokenProvider | None = None,
        requester: Requester | None = None,
        api_base: str = _API_BASE,
    ):
        self.repo = repo
        self._token = token
        self._token_provider = token_provider
        self._authed = bool(token) or token_provider is not None
        self.api_base = api_base.rstrip("/")
        self._request = requester or self._default_request

    async def _current_token(self) -> str:
        if self._token_provider is not None:
            return await self._token_provider()
        return self._token or ""

    async def ensure_candidate_issue(
        self, hotspot: Hotspot, *, incident_id: str | None = None, manifest_hash: str | None = None
    ) -> str | None:
        """Open (or refresh) the ``loop:candidate`` issue for this hotspot.
        Returns the issue URL, or ``None`` when disabled / on error."""
        if not self._authed or not self.repo:
            return None
        marker = _marker(hotspot)
        try:
            existing = await self._find_open_issue(marker)
            if existing is not None:
                await self._comment(
                    int(existing["number"]),
                    f"Still firing as of {utc_now()} (NOC case `{incident_id or 'n/a'}`).",
                )
                log.info("proactive_handoff_refreshed", repo=self.repo, number=existing.get("number"))
                return existing.get("html_url")
            url = await self._create_issue(hotspot, incident_id=incident_id, manifest_hash=manifest_hash)
            log.info("proactive_handoff_created", repo=self.repo, url=url)
            return url
        except Exception as exc:  # handoff failure must not break a cycle
            safe = classify_exception(exc)
            log_exception("proactive_handoff_failed", exc, category=safe.category, repo=self.repo)
            return None

    async def _find_open_issue(self, marker: str) -> dict[str, Any] | None:
        query = f'repo:{self.repo} is:issue is:open label:loop:candidate "{marker}"'
        status, body = await self._request("GET", "/search/issues", params={"q": query, "per_page": 20})
        if status != 200 or not isinstance(body, dict):
            return None
        for item in body.get("items", []):
            if isinstance(item, dict) and marker in str(item.get("body") or ""):
                return item
        return None

    async def _create_issue(self, hotspot: Hotspot, *, incident_id: str | None, manifest_hash: str | None) -> str | None:
        payload = {
            "title": f"[proactive] {hotspot.title}"[:240],
            "body": build_issue_body(hotspot, incident_id=incident_id, manifest_hash=manifest_hash),
            "labels": CANDIDATE_LABELS,
        }
        status, body = await self._request("POST", f"/repos/{self.repo}/issues", json=payload)
        if status not in (200, 201) or not isinstance(body, dict):
            log.warn("proactive_handoff_create_rejected", repo=self.repo, status=status)
            return None
        return body.get("html_url")

    async def _comment(self, number: int, text: str) -> None:
        await self._request("POST", f"/repos/{self.repo}/issues/{number}/comments", json={"body": text})

    async def _default_request(self, method: str, path: str, *, params: Any = None, json: Any = None):
        import httpx

        token = await self._current_token()
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.request(
                method, f"{self.api_base}{path}", params=params, json=json, headers=_gh_headers(token)
            )
            try:
                decoded = response.json()
            except Exception:
                decoded = {}
            return response.status_code, decoded


def _gh_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _parse_iso(value: Any) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


class GitHubAppAuth:
    """Mints short-lived GitHub App **installation tokens** (app JWT → installation
    access token) and caches them until ~5 min before expiry. Preferred over a
    static PAT: org-owned, auto-rotating, least-privilege via the app install."""

    def __init__(
        self,
        *,
        app_id: str,
        private_key: str,
        repo: str,
        installation_id: int | None = None,
        requester: Requester | None = None,
        api_base: str = _API_BASE,
    ):
        self.app_id = str(app_id)
        self.private_key = private_key
        self.repo = repo
        self.installation_id = installation_id
        self.api_base = api_base.rstrip("/")
        self._request = requester or self._default_request
        self._cached_token = ""
        self._expires_at = 0.0

    async def token(self) -> str:
        now = time.time()
        if self._cached_token and now < self._expires_at - 300:
            return self._cached_token
        jwt_token = self._make_jwt(now)
        installation_id = self.installation_id or await self._resolve_installation_id(jwt_token)
        tok, exp = await self._mint(jwt_token, installation_id)
        self._cached_token = tok
        self._expires_at = exp
        return tok

    def _make_jwt(self, now: float) -> str:
        import jwt  # PyJWT[crypto]

        # 9-min expiry (< GitHub's 10-min max), 60s back-dated iat for clock skew.
        payload = {"iat": int(now) - 60, "exp": int(now) + 540, "iss": self.app_id}
        return jwt.encode(payload, self.private_key, algorithm="RS256")

    async def _resolve_installation_id(self, jwt_token: str) -> int:
        status, body = await self._request(
            "GET", f"/repos/{self.repo}/installation", headers=_gh_headers(jwt_token)
        )
        if status != 200 or not isinstance(body, dict) or "id" not in body:
            raise RuntimeError(f"could not resolve app installation for {self.repo} (status {status})")
        return int(body["id"])

    async def _mint(self, jwt_token: str, installation_id: int) -> tuple[str, float]:
        status, body = await self._request(
            "POST", f"/app/installations/{installation_id}/access_tokens", headers=_gh_headers(jwt_token)
        )
        if status not in (200, 201) or not isinstance(body, dict) or "token" not in body:
            raise RuntimeError(f"could not mint installation token (status {status})")
        return str(body["token"]), (_parse_iso(body.get("expires_at")) or time.time() + 3600)

    async def _default_request(
        self, method: str, path: str, *, params: Any = None, json: Any = None, headers: Any = None
    ):
        import httpx

        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.request(
                method, f"{self.api_base}{path}", params=params, json=json, headers=headers or {}
            )
            try:
                decoded = response.json()
            except Exception:
                decoded = {}
            return response.status_code, decoded


def _read_app_private_key() -> str:
    """Read the GitHub App private key from a file (preferred, the Vault Agent
    renders it to NOC_GITHUB_APP_PRIVATE_KEY_PATH) or an inline env var."""
    path = os.getenv("NOC_GITHUB_APP_PRIVATE_KEY_PATH", "").strip()
    if path:
        try:
            return Path(path).read_text(encoding="utf-8").strip()
        except OSError:
            return ""
    inline = os.getenv("NOC_GITHUB_APP_PRIVATE_KEY", "")
    return inline.replace("\\n", "\n").strip()


def handoff_from_env(repo: str) -> GitHubHandoff | None:
    """Build a handoff client, preferring GitHub App auth (NOC_GITHUB_APP_ID +
    private key) and falling back to a static PAT (NOC_GITHUB_TOKEN). ``None`` if
    neither is configured."""
    app_id = os.getenv("NOC_GITHUB_APP_ID", "").strip()
    private_key = _read_app_private_key()
    if app_id and private_key:
        installation_env = os.getenv("NOC_GITHUB_APP_INSTALLATION_ID", "").strip()
        auth = GitHubAppAuth(
            app_id=app_id,
            private_key=private_key,
            repo=repo,
            installation_id=int(installation_env) if installation_env.isdigit() else None,
        )
        return GitHubHandoff(repo=repo, token_provider=auth.token)
    token = os.getenv("NOC_GITHUB_TOKEN", "").strip()
    if token:
        return GitHubHandoff(repo=repo, token=token)
    return None
