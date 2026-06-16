"""Proactive NOC loop.

The reactive NOC agent (``app/main.py`` webhooks → ``app/graph``) waits for an
alert to fire. This package adds the *active* operator: a budgeted, governed,
self-paced loop that continuously sweeps Prometheus / Icinga for early-warning
signals, autonomously investigates the most concerning ones through the existing
investigation graph, reports proactive risk findings, and hands changes that
need a config edit to the engineering-loop as ``loop:candidate`` issues.

Design mirrors the two production loops:

- ``engineering-loop`` ``daemon_once`` — run-lock + per-day ledger + budgets +
  passive-check reporting (ported in :mod:`app.proactive.ledger`).
- ``hyperliquid-trading-agent`` autonomy loop — continuous async observe →
  propose → evaluate → learn with governance (mirrored across
  :mod:`app.proactive.loop`, :mod:`app.proactive.governance`,
  :mod:`app.proactive.memory`).

Everything here is **read-only** and ships **disabled by default**
(``NOC_PROACTIVE_ENABLED=0``). It never executes remediation.
"""
