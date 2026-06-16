import pytest

from app.proactive.handoff import GitHubHandoff, build_issue_body
from app.proactive.models import Hotspot, HotspotEvidence


def _hotspot():
    return Hotspot(
        rule_id="bgp_risk",
        key="rtr:peer1",
        category="bgp",
        severity="HIGH",
        title="BGP peer peer1 flapping",
        resource="rtr",
        summary="Peer peer1 on rtr flapped 6 times in the last hour.",
        evidence=[HotspotEvidence(label="changes/1h", value="6", threshold=">=4")],
        recommended_checks=["show bgp neighbor peer1"],
        warrants_change=True,
        change_rationale="Recurrent flap may need timer/policy coordination.",
    )


class FakeGitHub:
    """Records requests and returns scripted responses; flips search results
    from empty → the created issue so we can assert idempotency."""

    def __init__(self):
        self.created: list[dict] = []
        self.comments: list[tuple[int, str]] = []
        self.search_returns_existing = False

    async def request(self, method, path, *, params=None, json=None):
        if method == "GET" and path == "/search/issues":
            if self.search_returns_existing:
                return 200, {"items": [self.created[0]]}
            return 200, {"items": []}
        if method == "POST" and path.endswith("/issues"):
            issue = {
                "number": 101,
                "html_url": "https://github.com/AS215932/network-operations/issues/101",
                "body": json["body"],
            }
            self.created.append(issue)
            self.search_returns_existing = True
            return 201, issue
        if method == "POST" and path.endswith("/comments"):
            number = int(path.split("/issues/")[1].split("/")[0])
            self.comments.append((number, json["body"]))
            return 201, {}
        raise AssertionError(f"unexpected request {method} {path}")


def test_issue_body_carries_marker_and_evidence():
    hs = _hotspot()
    body = build_issue_body(hs, incident_id="INC-1", manifest_hash="sha256:abc")
    assert f"proactive-fingerprint:{hs.fingerprint()}" in body
    assert "INC-1" in body and "sha256:abc" in body
    assert "loop:approved" in body  # promotion instruction present


@pytest.mark.asyncio
async def test_handoff_is_idempotent():
    gh = FakeGitHub()
    handoff = GitHubHandoff(token="t", repo="AS215932/network-operations", requester=gh.request)
    hs = _hotspot()

    url1 = await handoff.ensure_candidate_issue(hs, incident_id="INC-1")
    assert url1.endswith("/issues/101")
    assert len(gh.created) == 1
    assert gh.comments == []

    # Same hotspot again → finds the open issue, comments, does not recreate.
    url2 = await handoff.ensure_candidate_issue(hs, incident_id="INC-2")
    assert url2 == url1
    assert len(gh.created) == 1
    assert len(gh.comments) == 1 and gh.comments[0][0] == 101


@pytest.mark.asyncio
async def test_handoff_noop_without_token():
    handoff = GitHubHandoff(token="", repo="AS215932/network-operations", requester=None)
    assert await handoff.ensure_candidate_issue(_hotspot()) is None


@pytest.mark.asyncio
async def test_handoff_swallows_errors():
    async def boom(*a, **k):
        raise RuntimeError("api down")

    handoff = GitHubHandoff(token="t", repo="r", requester=boom)
    assert await handoff.ensure_candidate_issue(_hotspot()) is None
