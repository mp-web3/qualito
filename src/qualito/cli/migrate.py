"""Migrate Qualito data from local SQLite to PostgreSQL.

Reads all tables from a local SQLite database and writes them to PostgreSQL
via DATABASE_URL. Idempotent — uses INSERT ... ON CONFLICT DO NOTHING so
re-running is safe.

Usage:
    DATABASE_URL=postgresql://user:pass@host/db qualito-migrate [--sqlite-path path]

Requires: qualito[postgres]  (psycopg2-binary)
"""

import os
from pathlib import Path

import click
from sqlalchemy import create_engine, inspect, select, text

from qualito.core.db import (
    api_keys_table,
    artifacts_table,
    baselines_table,
    benchmark_suites_table,
    evaluations_table,
    experiment_comparisons_table,
    experiments_table,
    file_activity_table,
    incident_events_table,
    incidents_table,
    metadata,
    runs_table,
    system_changes_table,
    tool_calls_table,
    users_table,
)

# FK-safe insertion order: parents before children
TABLES_IN_ORDER = [
    runs_table,
    tool_calls_table,
    file_activity_table,
    evaluations_table,
    artifacts_table,
    baselines_table,
    system_changes_table,
    benchmark_suites_table,
    experiments_table,
    experiment_comparisons_table,
    incidents_table,
    incident_events_table,
    users_table,
    api_keys_table,
]


def _resolve_sqlite_path(path: str | None) -> Path:
    """Resolve the SQLite database path."""
    if path:
        return Path(path).expanduser().resolve()

    qualito_dir = os.environ.get("QUALITO_DIR")
    if qualito_dir:
        return Path(qualito_dir) / "qualito.db"

    return Path.home() / ".qualito" / "qualito.db"


def _migrate_table(src_conn, dst_conn, table, *, batch_size: int = 500) -> int:
    """Copy all rows from src to dst for one table. Returns row count."""
    rows = src_conn.execute(select(table)).mappings().fetchall()
    if not rows:
        return 0

    from sqlalchemy.dialects.postgresql import insert as pg_insert

    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        stmt = pg_insert(table).values([dict(r) for r in batch])
        pk_cols = [c.name for c in table.primary_key.columns]
        stmt = stmt.on_conflict_do_nothing(index_elements=pk_cols)
        dst_conn.execute(stmt)
        total += len(batch)

    return total


@click.command()
@click.option(
    "--sqlite-path",
    default=None,
    help="Path to SQLite database. Defaults to ~/.qualito/qualito.db or QUALITO_DIR.",
)
@click.option(
    "--database-url",
    default=None,
    envvar="DATABASE_URL",
    help="PostgreSQL connection URL. Defaults to DATABASE_URL env var.",
)
def migrate(sqlite_path: str | None, database_url: str | None) -> None:
    """Migrate Qualito data from SQLite to PostgreSQL."""
    if not database_url:
        click.echo("Error: DATABASE_URL is required (env var or --database-url).", err=True)
        raise SystemExit(1)

    src_path = _resolve_sqlite_path(sqlite_path)
    if not src_path.exists():
        click.echo(f"Error: SQLite database not found at {src_path}", err=True)
        raise SystemExit(1)

    click.echo(f"Source:  sqlite:///{src_path}")
    # Mask credentials in output
    at_idx = database_url.find("@")
    if at_idx > 0:
        scheme_end = database_url.find("://") + 3
        click.echo(f"Target:  {database_url[:scheme_end]}***@{database_url[at_idx + 1:]}")
    else:
        click.echo(f"Target:  {database_url}")
    click.echo()

    src_engine = create_engine(f"sqlite:///{src_path}")
    dst_engine = create_engine(database_url)

    # Create all tables in PostgreSQL (idempotent)
    metadata.create_all(dst_engine)
    click.echo("PostgreSQL schema created/verified.")

    # Check which tables exist in source
    src_inspector = inspect(src_engine)
    src_tables = set(src_inspector.get_table_names())

    with src_engine.connect() as src_conn, dst_engine.connect() as dst_conn:
        for table in TABLES_IN_ORDER:
            if table.name not in src_tables:
                click.echo(f"  {table.name:30s} — skipped (not in source)")
                continue

            count = _migrate_table(src_conn, dst_conn, table)
            dst_conn.commit()
            click.echo(f"  {table.name:30s} — {count:,} rows migrated")

    # Reset PostgreSQL auto-increment sequences to max(id) + 1
    with dst_engine.connect() as conn:
        for table in TABLES_IN_ORDER:
            for col in table.primary_key.columns:
                if col.autoincrement and col.type.__class__.__name__ == "Integer":
                    seq_name = f"{table.name}_{col.name}_seq"
                    conn.execute(
                        text(
                            f"SELECT setval('{seq_name}', COALESCE("
                            f"(SELECT MAX({col.name}) FROM {table.name}), 0) + 1, false)"
                        )
                    )
        conn.commit()
        click.echo("\nSequences reset.")

    click.echo("\nMigration complete.")


def main() -> None:
    """Entry point for the qualito-migrate script."""
    migrate()
