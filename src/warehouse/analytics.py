"""Execute the project's named read-only SQLite analytics queries."""
from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path


def named_queries(sql_file: Path) -> list[tuple[str, str]]:
    """Read sections starting with '-- name: label' from an SQL file."""
    sections = re.split(r"(?m)^-- name: ([a-z][a-z_0-9]*)\s*$", sql_file.read_text(encoding="utf-8"))
    if len(sections) < 3 or sections[0].strip():
        raise ValueError("SQL file must start with a '-- name: label' heading")
    queries = [(sections[i], sections[i + 1].strip()) for i in range(1, len(sections), 2)]
    if any(not sql for _, sql in queries):
        raise ValueError("Named SQL section cannot be empty")
    return queries


def run_analytics(db: Path, sql_file: Path) -> str:
    if not db.is_file():
        raise FileNotFoundError(f"Warehouse database does not exist: {db}")
    lines = [f"Database: {db}", f"SQL file: {sql_file}"]
    with sqlite3.connect(f"file:{db.resolve().as_posix()}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        for name, sql in named_queries(sql_file):
            # sqlite3.execute() rejects multiple SQL statements.
            cursor = conn.execute(sql)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            lines.extend(["", f"== {name} ({len(rows)} rows) ==", " | ".join(columns)])
            lines.extend(" | ".join(str(row[col]) for col in columns) for row in rows)
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("output/warehouse.db"))
    parser.add_argument("--sql", type=Path, default=Path("sql/analytics.sql"))
    parser.add_argument("--out", type=Path, help="Optional text evidence path")
    args = parser.parse_args()
    result = run_analytics(args.db, args.sql)
    print(result, end="")
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(result, encoding="utf-8")


if __name__ == "__main__":
    main()
