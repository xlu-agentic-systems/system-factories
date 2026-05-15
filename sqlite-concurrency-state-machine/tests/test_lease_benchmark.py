from __future__ import annotations

import pytest

from app.lease_benchmark import run_crash_recovery, run_lease_contention


def test_app_lock_is_only_process_local(tmp_path):
    thread_result = run_lease_contention(tmp_path / "threads", "app-lock", workers=12, mode="threads", ttl_seconds=2.0)
    process_result = run_lease_contention(tmp_path / "processes", "app-lock", workers=8, mode="processes", ttl_seconds=2.0)

    assert thread_result.winners == 1
    assert process_result.winners == 8
    assert process_result.duplicate_winners is True


@pytest.mark.parametrize("strategy", ["db-ttl", "redis-ttl"])
@pytest.mark.parametrize("mode", ["threads", "processes"])
def test_shared_ttl_strategies_allow_one_concurrent_owner(tmp_path, strategy, mode):
    result = run_lease_contention(tmp_path / f"{strategy}-{mode}", strategy, workers=12, mode=mode, ttl_seconds=2.0)

    assert result.winners == 1
    assert result.duplicate_winners is False


@pytest.mark.parametrize("strategy", ["db-ttl", "redis-ttl"])
def test_ttl_strategies_recover_after_owner_crash(tmp_path, strategy):
    result = run_crash_recovery(tmp_path / strategy, strategy, ttl_seconds=0.05)

    assert result.first_acquired is True
    assert result.before_ttl_acquired is False
    assert result.after_ttl_acquired is True
    assert result.recovered_after_ttl is True


def test_app_lock_does_not_recover_without_process_restart(tmp_path):
    result = run_crash_recovery(tmp_path / "app-lock", "app-lock", ttl_seconds=0.05)

    assert result.first_acquired is True
    assert result.before_ttl_acquired is False
    assert result.after_ttl_acquired is False
    assert result.recovered_after_ttl is False
    assert result.restart_loses_state is True
