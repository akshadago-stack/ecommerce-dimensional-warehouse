"""Regression checks for Windows temporary SQLite file locks in benchmarks."""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

import src.warehouse.benchmark as benchmark_module


@pytest.fixture
def sqlite_handle_tracker(monkeypatch):
    """Track all SQLite handles, including those opened by the ETL pipeline."""
    active = set()
    original_connect = sqlite3.connect

    class TrackedConnection(sqlite3.Connection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            active.add(self)

        def close(self):
            try:
                return super().close()
            finally:
                active.discard(self)

    def tracked_connect(*args, **kwargs):
        return original_connect(*args, factory=TrackedConnection, **kwargs)

    monkeypatch.setattr(benchmark_module.sqlite3, "connect", tracked_connect)
    return active, TrackedConnection


def test_benchmark_closes_connection_before_temporary_database_cleanup(sample, sqlite_handle_tracker):
    source, _ = sample
    active, _connection_class = sqlite_handle_tracker

    with tempfile.TemporaryDirectory(prefix="benchmark_handle_regression_") as folder:
        db = Path(folder) / "large.db"
        result = benchmark_module.benchmark(source, db, "test")
        assert result["final_fact_rows"] == 30
        assert not active, "SQLite connection must be closed before large.db is deleted"


def test_benchmark_closes_connection_if_inspection_raises(sample, sqlite_handle_tracker, monkeypatch):
    source, _ = sample
    active, connection_class = sqlite_handle_tracker
    original_execute = connection_class.execute

    def fail_foreign_key_check(self, sql, *args, **kwargs):
        if sql == "PRAGMA foreign_key_check":
            raise RuntimeError("injected benchmark inspection failure")
        return original_execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(connection_class, "execute", fail_foreign_key_check)
    with tempfile.TemporaryDirectory(prefix="benchmark_handle_regression_") as folder:
        db = Path(folder) / "large.db"
        with pytest.raises(RuntimeError, match="injected benchmark inspection failure"):
            benchmark_module.benchmark(source, db, "test")
        assert not active, "SQLite connection must close even if inspection raises"
