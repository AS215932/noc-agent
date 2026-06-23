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

LHP-v1 private acceptance scenarios are defined in `evals/lhp_v1/manifest.json`.
The public deterministic coverage lives in the normal pytest suite, especially:

```bash
uv run --group dev python -m pytest -q \
  tests/test_lhp_foundation.py \
  tests/test_lhp_store_service.py \
  tests/test_lhp_verifier.py \
  tests/test_lhp_knowledge.py \
  tests/test_proactive_lhp.py \
  tests/test_proactive_loop.py \
  tests/test_case_metrics.py
```

The default suite must stay deterministic and must not require live AS215932
network access or production credentials.
