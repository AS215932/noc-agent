from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = os.getenv("NOC_CONTROL_URL", "http://127.0.0.1:8000")


def _sign_authorization(*, action_id: str, case_id: str, operator: str, action_class: str, ttl: int) -> dict[str, object]:
    """Build a signed action authorization, matching the HMAC scheme in
    app.graph.nodes._action_authorization and hyrule-mcp's validate_signed_authorization."""
    secret = os.getenv("HYRULE_MCP_ACTION_SIGNING_SECRET") or os.getenv("NOC_APPROVAL_SIGNING_SECRET", "")
    if not secret:
        raise SystemExit("HYRULE_MCP_ACTION_SIGNING_SECRET or NOC_APPROVAL_SIGNING_SECRET is required to sign")
    payload: dict[str, object] = {
        "action_id": action_id,
        "case_id": case_id,
        "operator": operator,
        "action_class": action_class,
        "expiry": int(time.time()) + ttl,
    }
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    payload["signature"] = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(prog="nocctl", description="Local AS215932 NOC control-plane helper.")
    parser.add_argument("--url", default=DEFAULT_BASE_URL)
    parser.add_argument("--token", default=os.getenv("NOC_CONTROL_TOKEN", ""))
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("pending", help="List pending incident proposals.")

    show = sub.add_parser("show", help="Show one incident summary.")
    show.add_argument("incident_id")

    decide = sub.add_parser("decide", help="Record an approval/rejection decision.")
    decide.add_argument("incident_id")
    decide.add_argument("decision", choices=["approved", "rejected", "acknowledged"])
    decide.add_argument("--operator", required=True)
    decide.add_argument("--comment", default="")

    replay = sub.add_parser("replay", help="Replay a sanitized observation fixture (offline).")
    replay.add_argument("fixture")

    approvals = sub.add_parser("approvals", help="Operator signing helpers (offline).")
    approvals_sub = approvals.add_subparsers(dest="approvals_command", required=True)
    sign = approvals_sub.add_parser("sign", help="Emit a signed action authorization (no HTTP, no token).")
    sign.add_argument("--proposal-id", required=True, help="NOC case/incident id (becomes case_id)")
    sign.add_argument("--action-id", required=True)
    sign.add_argument("--operator", required=True)
    sign.add_argument("--action-class", default="noop_rollback_guard")
    sign.add_argument("--ttl", type=int, default=300)

    args = parser.parse_args()

    # Offline signing helper: no control token / HTTP required.
    if args.command == "approvals":
        authorization = _sign_authorization(
            action_id=args.action_id, case_id=args.proposal_id, operator=args.operator,
            action_class=args.action_class, ttl=args.ttl,
        )
        print(json.dumps(authorization, indent=2, sort_keys=True))
        return
    if args.command == "replay":
        print(json.dumps(asyncio.run(_run_replay(args.fixture)), indent=2, sort_keys=True))
        return

    if not args.token:
        parser.error("NOC_CONTROL_TOKEN or --token is required")

    if args.command == "pending":
        payload = _request(args.url, "/control/incidents/pending", args.token)
    elif args.command == "show":
        payload = _request(args.url, f"/control/incidents/{args.incident_id}", args.token)
    else:
        body = {"decision": args.decision, "operator": args.operator, "comment": args.comment}
        payload = _request(
            args.url,
            f"/control/incidents/{args.incident_id}/decision",
            args.token,
            method="POST",
            body=body,
        )
    print(json.dumps(payload, indent=2, sort_keys=True))


async def _run_replay(fixture: str) -> dict:
    from app.cases.replay import load_observation_fixture, replay_observations

    observations = load_observation_fixture(fixture)
    result = await replay_observations(observations)
    metrics = await result.metrics()
    return {"fixture": fixture, "metrics": metrics}


def _request(base_url: str, path: str, token: str, *, method: str = "GET", body: dict | None = None):
    data = None if body is None else json.dumps(body).encode()
    request = Request(
        f"{base_url.rstrip('/')}{path}",
        data=data,
        method=method,
        headers={
            "Content-Type": "application/json",
            "X-NOC-Control-Token": token,
        },
    )
    try:
        with urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode())
    except HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise SystemExit(f"HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise SystemExit(f"Control plane unavailable: {exc.reason}") from exc


if __name__ == "__main__":
    main()
