from __future__ import annotations

import pytest

from app.claims import claim_with_atomic_update
from app.db import BUSY, get_driver, reset_driver
from app.experiment import run_race


def test_unsafe_check_then_update_allows_multiple_winners(tmp_path):
    db_path = tmp_path / "unsafe.sqlite3"

    result = run_race(db_path, strategy="unsafe", workers=24, mode="threads", delay_seconds=0.04)

    assert result.successes > 1
    assert result.final_driver["status"] == "OFFERED"
    assert result.final_driver["version"] == result.successes


@pytest.mark.parametrize("mode", ["threads", "processes"])
@pytest.mark.parametrize("strategy", ["transaction", "atomic"])
def test_safe_strategies_allow_only_one_winner(tmp_path, strategy, mode):
    db_path = tmp_path / f"{strategy}-{mode}.sqlite3"

    result = run_race(db_path, strategy=strategy, workers=16, mode=mode, delay_seconds=0.02)

    assert result.successes == 1
    assert result.failures == 15
    assert result.final_driver["status"] == "OFFERED"
    assert result.final_driver["version"] == 1


def test_atomic_update_rejects_invalid_state_transition(tmp_path):
    db_path = tmp_path / "busy.sqlite3"
    reset_driver(db_path, status=BUSY)

    result = claim_with_atomic_update(db_path, "driver-1", "worker-1", delay_seconds=0)
    driver = get_driver(db_path)

    assert result.success is False
    assert result.reason == "affected-zero-rows"
    assert driver["status"] == BUSY
    assert driver["version"] == 0

