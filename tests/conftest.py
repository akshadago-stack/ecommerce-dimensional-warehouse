"""Shared isolated test fixtures: no test writes to the real project database."""
from __future__ import annotations

import csv
from pathlib import Path

import pytest
from src.warehouse.generator import generate_dataset


@pytest.fixture
def sample(tmp_path: Path):
    source = tmp_path / "input"
    generate_dataset(source, lines=30, seed=42)
    return source, tmp_path / "test_warehouse.db"


def read_rows(path: Path):
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def rewrite(path: Path, rows: list[dict]):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def append(path: Path, *rows: dict):
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writerows(rows)
