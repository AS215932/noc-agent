from __future__ import annotations

import argparse
import json
import os
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = os.getenv("NOC_CONTROL_URL", "http://127.0.0.1:8000")


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

    args = parser.parse_args()
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
