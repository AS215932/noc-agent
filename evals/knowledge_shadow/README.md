# NOC knowledge shadow eval fixtures

Committed read-only context-pack fixtures for the future AS215932 knowledge
control-plane integration. These files are not live traces and must not include
MCP responses, Prometheus/Icinga output, logs, packet data, or secrets.

The NOC runtime does not consume these fixtures in production. Tests validate
only shape, citations, policy decisions, null vector score placeholders, and
shadow/read-only boundaries.
