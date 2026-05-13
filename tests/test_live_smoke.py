import os

import pytest


pytestmark = pytest.mark.skipif(
    os.getenv("NOC_AGENT_LIVE_SMOKE") != "1",
    reason="Set NOC_AGENT_LIVE_SMOKE=1 to run read-only live NOC smoke tests.",
)


def test_live_smoke_marker_is_explicit():
    assert os.getenv("NOC_AGENT_LIVE_SMOKE") == "1"
