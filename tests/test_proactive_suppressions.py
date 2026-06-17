import time

from app.proactive.suppressions import SuppressionStore


def test_add_active_remove_list(tmp_path):
    store = SuppressionStore(tmp_path / "s.json")
    store.add(fingerprint="abc123", key="api:monero", reason="tracked in #268", operator="svag")
    assert "abc123" in store.active()
    assert store.active()["abc123"]["issue"] == ""
    assert [e["fingerprint"] for e in store.entries()] == ["abc123"]
    assert store.remove("abc123") is True
    assert store.active() == {}
    assert store.remove("abc123") is False  # already gone


def test_ttl_expiry(tmp_path):
    store = SuppressionStore(tmp_path / "s.json")
    store.add(fingerprint="short", key="k", ttl_seconds=-1)  # already expired
    assert "short" not in store.active()
    store.add(fingerprint="long", key="k", ttl_seconds=3600)
    assert "long" in store.active()


def test_prune_resolved(tmp_path):
    store = SuppressionStore(tmp_path / "s.json")
    store.add(fingerprint="firing", key="k1")
    store.add(fingerprint="gone", key="k2")
    pruned = store.prune_resolved({"firing"})  # only "firing" still observed
    assert pruned == ["gone"]
    assert set(store.active()) == {"firing"}


def test_persistence_across_instances(tmp_path):
    SuppressionStore(tmp_path / "s.json").add(fingerprint="zzz", key="k")
    assert "zzz" in SuppressionStore(tmp_path / "s.json").active()


def test_corrupt_file_resets(tmp_path):
    (tmp_path / "s.json").write_text("{not json")
    assert SuppressionStore(tmp_path / "s.json").active() == {}
