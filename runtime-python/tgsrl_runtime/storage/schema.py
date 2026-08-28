"""SQLite schema migration utilities."""

from __future__ import annotations

import sqlite3
from pathlib import Path


def _apply_migration_script(connection: sqlite3.Connection, script: str) -> None:
    statements = [statement.strip() for statement in script.split(";")]
    for statement in statements:
        if statement:
            connection.execute(statement)


def apply_migrations(connection: sqlite3.Connection, *, migrations_dir: Path) -> None:
    connection.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY)")
    applied = {
        row["version"]
        for row in connection.execute("SELECT version FROM schema_migrations ORDER BY version")
    }
    for migration in sorted(migrations_dir.glob("*.sql")):
        if migration.name in applied:
            continue
        script = migration.read_text(encoding="utf-8")
        connection.execute("BEGIN")
        try:
            _apply_migration_script(connection, script)
            connection.execute(
                "INSERT INTO schema_migrations(version) VALUES (?)",
                (migration.name,),
            )
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()
