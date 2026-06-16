import json

from app.proactive import ledger


def test_load_empty_ledger(tmp_path):
    led = ledger.load_ledger(tmp_path, "2026-06-16")
    assert led == {"cycles": 0, "investigations": 0, "cost_usd": 0.0, "handoffs": 0}


def test_update_ledger_accumulates_and_persists(tmp_path):
    ledger.update_ledger(tmp_path, "2026-06-16", cycles=1, investigations=1, cost_usd=0.5)
    led = ledger.update_ledger(tmp_path, "2026-06-16", cycles=1, investigations=2, cost_usd=0.25, handoffs=1)
    assert led == {"cycles": 2, "investigations": 3, "cost_usd": 0.75, "handoffs": 1}
    on_disk = json.loads((tmp_path / "ledger-2026-06-16.json").read_text())
    assert on_disk["investigations"] == 3


def test_corrupt_ledger_resets(tmp_path):
    (tmp_path / "ledger-2026-06-16.json").write_text("{not json")
    assert ledger.load_ledger(tmp_path, "2026-06-16")["investigations"] == 0


def test_lock_is_exclusive_then_released(tmp_path):
    first = ledger.acquire_lock(tmp_path)
    assert first is not None
    # Held by this live PID → second acquire is refused.
    assert ledger.acquire_lock(tmp_path) is None
    ledger.release_lock(first)
    assert ledger.acquire_lock(tmp_path) is not None


def test_stale_lock_is_broken(tmp_path):
    lock_path = tmp_path / "proactive.lock"
    tmp_path.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(json.dumps({"pid": 999999, "started_at": 0.0}))
    # Dead PID + ancient timestamp → broken and re-taken.
    assert ledger.acquire_lock(tmp_path) is not None
