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
from typing import Any, Awaitable, Callable

from app import log
from app.proactive.models import Hotspot, utc_now
from app.safe_errors import classify_exception, log_exception


CANDIDATE_LABELS = ["loop:candidate", "agentic-isp"]
_API_BASE = "https://api.github.com"

# async (method, path, *, params, json) -> (status_code, body)
Requester = Callable[..., Awaitable[tuple[int, Any]]]


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
        token: str,
        repo: str,
        requester: Requester | None = None,
        api_base: str = _API_BASE,
    ):
        self.token = token
        self.repo = repo
        self.api_base = api_base.rstrip("/")
        self._request = requester or self._default_request

    async def ensure_candidate_issue(
        self, hotspot: Hotspot, *, incident_id: str | None = None, manifest_hash: str | None = None
    ) -> str | None:
        """Open (or refresh) the ``loop:candidate`` issue for this hotspot.
        Returns the issue URL, or ``None`` when disabled / on error."""
        if not self.token or not self.repo:
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

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    async def _default_request(self, method: str, path: str, *, params: Any = None, json: Any = None):
        import httpx

        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.request(
                method, f"{self.api_base}{path}", params=params, json=json, headers=self._headers()
            )
            try:
                decoded = response.json()
            except Exception:
                decoded = {}
            return response.status_code, decoded


def handoff_from_env(repo: str) -> GitHubHandoff | None:
    """Construct a handoff client from ``NOC_GITHUB_TOKEN``; ``None`` if unset."""
    token = os.getenv("NOC_GITHUB_TOKEN", "").strip()
    if not token:
        return None
    return GitHubHandoff(token=token, repo=repo)
