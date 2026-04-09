"""Tests for SQLAlchemy table definitions and engine setup."""

import pytest
from sqlalchemy import inspect

from qualito.core.db import get_engine, init_db, metadata


EXPECTED_TABLES = {
    "runs",
    "tool_calls",
    "file_activity",
    "evaluations",
    "artifacts",
    "baselines",
    "system_changes",
    "benchmark_suites",
    "experiments",
    "experiment_comparisons",
    "incidents",
    "incident_events",
    "users",
    "api_keys",
}


def test_metadata_has_14_tables():
    """metadata.tables has exactly 14 entries with correct names."""
    assert len(metadata.tables) == 14
    assert set(metadata.tables.keys()) == EXPECTED_TABLES


def test_get_engine_sqlite_default(monkeypatch):
    """get_engine returns SQLite engine when no DATABASE_URL."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    engine = get_engine()
    assert "sqlite" in str(engine.url)


def test_get_engine_postgres_url(monkeypatch):
    """get_engine translates postgres:// to postgresql:// for SA."""
    monkeypatch.setenv("DATABASE_URL", "postgres://user:pass@host:5432/db")
    engine = get_engine()
    url = engine.url
    assert url.drivername == "postgresql"
    assert url.username == "user"
    assert url.host == "host"
    assert url.port == 5432
    assert url.database == "db"


def test_get_engine_postgresql_url(monkeypatch):
    """get_engine passes through postgresql:// unchanged."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@host:5432/db")
    engine = get_engine()
    url = engine.url
    assert url.drivername == "postgresql"
    assert url.username == "user"
    assert url.host == "host"


def test_get_engine_explicit_path(tmp_path):
    """get_engine accepts an explicit file path for SQLite."""
    db_file = str(tmp_path / "test.db")
    engine = get_engine(db_file)
    assert "sqlite" in str(engine.url)
    assert "test.db" in str(engine.url)


def test_init_db_creates_all_tables(tmp_path):
    """init_db creates all 14 tables in a fresh SQLite database."""
    db_file = str(tmp_path / "test.db")
    engine = get_engine(db_file)
    init_db(engine)

    insp = inspect(engine)
    table_names = set(insp.get_table_names())
    assert table_names == EXPECTED_TABLES


def test_init_db_idempotent(tmp_path):
    """init_db can be called multiple times without error."""
    db_file = str(tmp_path / "test.db")
    engine = get_engine(db_file)
    init_db(engine)
    init_db(engine)  # Should not raise

    insp = inspect(engine)
    assert len(insp.get_table_names()) == 14


def test_init_db_returns_engine(tmp_path):
    """init_db returns the engine it was given (or created)."""
    db_file = str(tmp_path / "test.db")
    engine = get_engine(db_file)
    returned = init_db(engine)
    assert returned is engine


def test_runs_table_columns():
    """Verify runs table has all expected columns."""
    cols = {c.name for c in metadata.tables["runs"].columns}
    expected = {
        "id", "workspace", "task", "task_type", "model", "pipeline_mode",
        "status", "summary", "files_changed", "cost_usd", "input_tokens",
        "output_tokens", "cache_read_tokens", "duration_ms", "branch",
        "prompt", "original_prompt", "started_at", "completed_at",
        "researcher_summary", "implementer_summary", "verifier_verdict",
        "paper_live_gap", "skill_name", "source", "prompt_components",
    }
    assert cols == expected


def test_incidents_table_columns():
    """Verify incidents table has all expected columns."""
    cols = {c.name for c in metadata.tables["incidents"].columns}
    expected = {
        "id", "incident_key", "category", "severity", "status", "workspace",
        "task_type", "title", "description", "detection_method",
        "trigger_metric", "trigger_value", "baseline_value", "burn_rate",
        "affected_run_ids", "total_affected_runs", "cost_impact_usd",
        "fix_experiment_id", "fix_description", "resolution_type",
        "created_at", "confirmed_at", "resolved_at", "time_to_detect_runs",
        "time_to_resolve_runs",
    }
    assert cols == expected


def test_users_table_columns():
    """Verify users table has all expected columns."""
    cols = {c.name for c in metadata.tables["users"].columns}
    expected = {
        "id", "email", "password_hash", "name", "stripe_customer_id",
        "plan", "created_at", "email_verified",
    }
    assert cols == expected


def test_api_keys_table_columns():
    """Verify api_keys table has all expected columns."""
    cols = {c.name for c in metadata.tables["api_keys"].columns}
    expected = {
        "id", "user_id", "key_hash", "key_prefix", "name",
        "last_used_at", "created_at", "revoked_at",
    }
    assert cols == expected
