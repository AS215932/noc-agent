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


# --- GitHub App auth ------------------------------------------------------


def _test_pem() -> str:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()


@pytest.mark.asyncio
async def test_github_app_auth_mints_and_caches():
    from app.proactive.handoff import GitHubAppAuth

    calls = []

    async def fake_request(method, path, *, params=None, json=None, headers=None):
        calls.append((method, path))
        assert headers and headers["Authorization"].startswith("Bearer ")
        if path.endswith("/installation"):
            return 200, {"id": 42}
        if path.endswith("/access_tokens"):
            return 201, {"token": "ghs_minted", "expires_at": "2999-01-01T00:00:00Z"}
        raise AssertionError(path)

    auth = GitHubAppAuth(
        app_id="4071799", private_key=_test_pem(), repo="AS215932/network-operations", requester=fake_request
    )
    assert await auth.token() == "ghs_minted"
    assert ("GET", "/repos/AS215932/network-operations/installation") in calls
    # second call is served from cache (no extra resolve/mint round-trips)
    n = len(calls)
    assert await auth.token() == "ghs_minted"
    assert len(calls) == n


@pytest.mark.asyncio
async def test_github_app_auth_skips_lookup_with_explicit_installation_id():
    from app.proactive.handoff import GitHubAppAuth

    async def fake_request(method, path, *, params=None, json=None, headers=None):
        assert not path.endswith("/installation")  # must not look it up
        return 201, {"token": "ghs_x", "expires_at": "2999-01-01T00:00:00Z"}

    auth = GitHubAppAuth(app_id="1", private_key=_test_pem(), repo="o/r", installation_id=99, requester=fake_request)
    assert await auth.token() == "ghs_x"


def test_handoff_from_env_prefers_app(monkeypatch, tmp_path):
    from app.proactive.handoff import handoff_from_env

    pem_file = tmp_path / "app.pem"
    pem_file.write_text("-----BEGIN PRIVATE KEY-----\nstub\n-----END PRIVATE KEY-----")
    monkeypatch.setenv("NOC_GITHUB_APP_ID", "4071799")
    monkeypatch.setenv("NOC_GITHUB_APP_PRIVATE_KEY_PATH", str(pem_file))
    monkeypatch.setenv("NOC_GITHUB_TOKEN", "pat_should_be_ignored")
    handoff = handoff_from_env("AS215932/network-operations")
    assert handoff is not None
    assert handoff._token_provider is not None  # App mode wins over PAT


def test_handoff_from_env_falls_back_to_pat(monkeypatch):
    from app.proactive.handoff import handoff_from_env

    for var in ("NOC_GITHUB_APP_ID", "NOC_GITHUB_APP_PRIVATE_KEY", "NOC_GITHUB_APP_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("NOC_GITHUB_TOKEN", "pat_x")
    handoff = handoff_from_env("o/r")
    assert handoff is not None and handoff._token_provider is None and handoff._token == "pat_x"


def test_handoff_from_env_none_when_unconfigured(monkeypatch):
    from app.proactive.handoff import handoff_from_env

    for var in (
        "NOC_GITHUB_APP_ID",
        "NOC_GITHUB_APP_PRIVATE_KEY",
        "NOC_GITHUB_APP_PRIVATE_KEY_PATH",
        "NOC_GITHUB_TOKEN",
    ):
        monkeypatch.delenv(var, raising=False)
    assert handoff_from_env("o/r") is None
