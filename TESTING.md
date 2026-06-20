# Testing

Run the hermetic characterization and refactor regression suite with:

```bash
uv run --group dev python -m pytest -q
```

Read-only live smoke coverage is opt-in:

```bash
NOC_AGENT_LIVE_SMOKE=1 uv run --group dev python -m pytest -q tests/test_live_smoke.py
```

Case-grounded replay fixtures are also offline and deterministic:

```bash
uv run --group dev nocctl replay path/to/observations.json
```

The default suite must stay deterministic and must not require live AS215932
network access or production credentials.
