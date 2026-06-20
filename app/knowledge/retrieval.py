"""Deterministic retrieval from a pinned hyrule-knowledge SQLite export.

Production decisions must be replayable from a pinned export without depending
on a live MCP server. This adapter reads `knowledge.sqlite` plus its
`manifest.json`, applies authority filters, and returns trace-ready citations.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


APPROVED_REVIEW_STATUSES = {"approved", "reviewed"}
AUTHORITATIVE_AUTHORITIES = {"canonical", "advisory", "A0", "A1", "A2"}
DEPRECATED_REVIEW_STATUSES = {"deprecated", "rejected"}


@dataclass(frozen=True, slots=True)
class KnowledgeExport:
    sqlite_path: Path
    manifest_path: Path | None
    export_version: str
    retrieval_version: str
    policy_version: str
    source_shas: dict[str, Any]


@dataclass(frozen=True, slots=True)
class KnowledgeCitation:
    doc_id: str
    doc_path: str
    review_status: str
    authority: str
    section: str
    repo_revision: str
    export_version: str
    score: float

    @property
    def authoritative(self) -> bool:
        return self.review_status in APPROVED_REVIEW_STATUSES and self.authority in AUTHORITATIVE_AUTHORITIES

    def as_trace_dict(self) -> dict[str, Any]:
        return {
            "doc_id": self.doc_id,
            "doc_path": self.doc_path,
            "review_status": self.review_status,
            "authority": self.authority,
            "section": self.section,
            "repo_revision": self.repo_revision,
            "export_version": self.export_version,
            "score": self.score,
            "authoritative": self.authoritative,
        }


@dataclass(frozen=True, slots=True)
class KnowledgeSearchResult:
    citation: KnowledgeCitation
    title: str
    description: str
    snippet: str
    tags: list[str]

    def as_trace_dict(self) -> dict[str, Any]:
        return {**self.citation.as_trace_dict(), "title": self.title, "description": self.description}


class KnowledgeExportRetriever:
    """Read-only retriever for a pinned SQLite/JSON manifest export."""

    def __init__(self, sqlite_path: str | Path, *, manifest_path: str | Path | None = None) -> None:
        self.export = load_knowledge_export(sqlite_path, manifest_path=manifest_path)

    def search(
        self,
        query: str,
        *,
        limit: int = 5,
        include_non_authoritative: bool = False,
    ) -> list[KnowledgeSearchResult]:
        terms = _query_terms(query)
        if not terms:
            return []
        rows = self._load_concepts()
        source_refs = self._load_source_refs()
        ranked: list[tuple[float, dict[str, Any]]] = []
        for row in rows:
            score = _score_row(row, terms)
            if score <= 0:
                continue
            review_status = str(row.get("review_status") or "")
            authority = str(row.get("authority") or "")
            authoritative = review_status in APPROVED_REVIEW_STATUSES and authority in AUTHORITATIVE_AUTHORITIES
            if not include_non_authoritative and not authoritative:
                continue
            ranked.append((score, row))
        ranked.sort(key=lambda item: (item[0], str(item[1].get("quality_score") or "")), reverse=True)
        results: list[KnowledgeSearchResult] = []
        for score, row in ranked[: max(0, limit)]:
            doc_id = str(row.get("id") or "")
            body = str(row.get("body") or "")
            refs = source_refs.get(doc_id, [])
            citation = KnowledgeCitation(
                doc_id=doc_id,
                doc_path=str(row.get("path") or ""),
                review_status=str(row.get("review_status") or ""),
                authority=str(row.get("authority") or ""),
                section=_best_section(body, terms),
                repo_revision=_repo_revision(refs, self.export.source_shas),
                export_version=self.export.export_version,
                score=round(score, 6),
            )
            results.append(
                KnowledgeSearchResult(
                    citation=citation,
                    title=str(row.get("title") or ""),
                    description=str(row.get("description") or ""),
                    snippet=_snippet(body, terms),
                    tags=_parse_tags(row.get("tags_json")),
                )
            )
        return results

    def search_case_context(
        self,
        context: dict[str, Any],
        *,
        limit: int = 5,
        include_non_authoritative: bool = False,
    ) -> list[KnowledgeSearchResult]:
        """Build a deterministic text query from a case/meta-case context."""

        parts: list[str] = []
        for key in (
            "fingerprint",
            "rule_id",
            "detector",
            "resource",
            "entity",
            "site",
            "customer",
            "service",
            "severity",
            "summary",
            "title",
            "event_type",
            "root_cause_hypothesis",
        ):
            value = context.get(key)
            if value:
                parts.append(str(value))
        for item in context.get("symptoms") or []:
            parts.append(str(item))
        return self.search(" ".join(parts), limit=limit, include_non_authoritative=include_non_authoritative)

    def _load_concepts(self) -> list[dict[str, Any]]:
        with _connect(self.export.sqlite_path) as conn:
            if not _table_exists(conn, "concepts"):
                raise ValueError(f"knowledge export missing concepts table: {self.export.sqlite_path}")
            return [dict(row) for row in conn.execute("SELECT * FROM concepts")]

    def _load_source_refs(self) -> dict[str, list[dict[str, Any]]]:
        with _connect(self.export.sqlite_path) as conn:
            if not _table_exists(conn, "source_refs"):
                return {}
            refs: dict[str, list[dict[str, Any]]] = {}
            for row in conn.execute("SELECT * FROM source_refs"):
                item = dict(row)
                refs.setdefault(str(item.get("concept_id") or ""), []).append(item)
            return refs


def load_knowledge_export(sqlite_path: str | Path, *, manifest_path: str | Path | None = None) -> KnowledgeExport:
    sqlite = Path(sqlite_path)
    if not sqlite.exists():
        raise FileNotFoundError(sqlite)
    manifest = Path(manifest_path) if manifest_path is not None else sqlite.parent / "manifest.json"
    data: dict[str, Any] = {}
    if manifest.exists():
        loaded = json.loads(manifest.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            data = loaded
    run_id = str(data.get("run_id") or sqlite.stat().st_mtime_ns)
    retrieval_version = str(data.get("retrieval_version") or "unknown_retrieval")
    policy_version = str(data.get("policy_version") or "unknown_policy")
    raw_source_shas = data.get("source_shas")
    source_shas: dict[str, Any] = (
        {str(key): value for key, value in raw_source_shas.items()} if isinstance(raw_source_shas, dict) else {}
    )
    export_version = f"{run_id}:{retrieval_version}:{policy_version}"
    return KnowledgeExport(
        sqlite_path=sqlite,
        manifest_path=manifest if manifest.exists() else None,
        export_version=export_version,
        retrieval_version=retrieval_version,
        policy_version=policy_version,
        source_shas=source_shas,
    )


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _query_terms(query: str) -> list[str]:
    rendered = " ".join(str(query or "").replace("_", " ").replace("/", " ").split()).lower()
    return [part for part in rendered.split(" ") if len(part) >= 2]


def _score_row(row: dict[str, Any], terms: Iterable[str]) -> float:
    title = str(row.get("title") or "").lower()
    description = str(row.get("description") or "").lower()
    body = str(row.get("body") or "").lower()
    path = str(row.get("path") or "").lower()
    tags = " ".join(_parse_tags(row.get("tags_json"))).lower()
    score = 0.0
    for term in terms:
        score += 5.0 * title.count(term)
        score += 3.0 * description.count(term)
        score += 2.0 * tags.count(term)
        score += 1.0 * path.count(term)
        score += 0.5 * body.count(term)
    return score


def _parse_tags(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if not value:
        return []
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if isinstance(loaded, list):
        return [str(item) for item in loaded]
    return []


def _best_section(body: str, terms: list[str]) -> str:
    current = ""
    best = ""
    best_score = 0
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            current = stripped.lstrip("#").strip()
            continue
        lowered = stripped.lower()
        score = sum(lowered.count(term) for term in terms)
        if score > best_score:
            best = current
            best_score = score
    return best


def _snippet(body: str, terms: list[str], *, limit: int = 320) -> str:
    lowered = body.lower()
    first = min((lowered.find(term) for term in terms if lowered.find(term) >= 0), default=0)
    start = max(0, first - 80)
    snippet = " ".join(body[start : start + limit].split())
    return snippet


def _repo_revision(refs: list[dict[str, Any]], source_shas: dict[str, Any]) -> str:
    for ref in refs:
        commit = str(ref.get("commit_sha") or "")
        if commit:
            return commit
    for value in source_shas.values():
        if value:
            return str(value)
    return ""
