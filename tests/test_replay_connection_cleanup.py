"""Regression checks for SQLite file handles in reconciliation replay.

Track connections explicitly so this catches Windows file-lock leaks on Linux too.
"""
from contextlib import contextmanager
import sqlite3
import tempfile

import pytest

import src.warehouse.reconcile as reconciliation
from src.warehouse.pipeline import run_pipeline


@pytest.fixture
def tracked_sqlite_connections(monkeypatch):
    active = set()
    original_connect = sqlite3.connect
    original_temporary_directory = tempfile.TemporaryDirectory

    class TrackedConnection(sqlite3.Connection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            active.add(self)

        def close(self):
            try:
                return super().close()
            finally:
                active.discard(self)

    def connect_tracked(*args, **kwargs):
        return original_connect(*args, factory=TrackedConnection, **kwargs)

    @contextmanager
    def checked_temporary_directory(*args, **kwargs):
        with original_temporary_directory(*args, **kwargs) as folder:
            try:
                yield folder
            finally:
                assert not active, "SQLite connections must close before replay.db cleanup"

    monkeypatch.setattr(reconciliation.sqlite3, "connect", connect_tracked)
    monkeypatch.setattr(
        reconciliation.tempfile, "TemporaryDirectory", checked_temporary_directory
    )
    return active


def test_replay_closes_sqlite_handles_before_temp_cleanup(sample, tracked_sqlite_connections):
    source, db = sample
    run_pipeline(source, db)

    report = reconciliation.reconcile(source, db, replay=True)

    assert report["summary"]["failed"] == 0
    assert report["idempotency_copy_replay"]["same_business_snapshot"]
    assert not tracked_sqlite_connections


def test_replay_closes_sqlite_handles_when_etl_raises(sample, tracked_sqlite_connections, monkeypatch):
    source, db = sample
    run_pipeline(source, db)

    def fail_replay(*args, **kwargs):
        raise RuntimeError("intentional replay failure")

    monkeypatch.setattr(reconciliation, "run_pipeline", fail_replay)
    with pytest.raises(RuntimeError, match="intentional replay failure"):
        reconciliation.reconcile(source, db, replay=True)

    assert not tracked_sqlite_connections
