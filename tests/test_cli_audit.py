"""Tests for `qualito audit` command group (Phase 7 T8).

Covers the four subcommands — secrets (interactive review), list, unflag,
drop — using CliRunner and an isolated tmp_path DB. Every test seeds runs
directly via SA Core, so the scanner (Task 2) and flag columns (Task 1) are
exercised end-to-end without hitting the network.
"""

from datetime import datetime, timedelta
from pathlib import Path

from click.testing import CliRunner
from sqlalchemy import insert

from qualito.cli.main import cli
from qualito.config import init_project
from qualito.core.db import (
    artifacts_table,
    conversations_table,
    evaluations_table,
    file_activity_table,
    get_engine,
    get_sa_connection,
    runs_table,
    tool_calls_table,
)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


# Real AWS access-key format (AKIA + 16 alphanumeric) — matches the
# `aws_access_key` regex in core/secret_scanner.py. Used across tests so we
# stay consistent with the scanner contract.
SEEDED_AWS_KEY = "AKIAIOSFODNN7EXAMPLE"


def _setup(tmp_path, monkeypatch):
    """Create an isolated local .qualito/ DB rooted at tmp_path."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    init_project(project_dir=tmp_path, local=True)
    db_path = tmp_path / ".qualito" / "qualito.db"
    return db_path


def _seed_run(
    conn,
    run_id: str,
    *,
    workspace: str = "alpha",
    task: str = "implement feature",
    started_offset_hours: int = 0,
    flagged: bool = False,
    flag_reason: str | None = None,
) -> None:
    now = datetime.now() - timedelta(hours=started_offset_hours)
    conn.execute(
        insert(runs_table).values(
            id=run_id,
            workspace=workspace,
            task=task,
            status="completed",
            session_type="interactive",
            model="claude-opus-4-6",
            cost_usd=1.0,
            input_tokens=10_000,
            output_tokens=2_000,
            cache_read_tokens=0,
            started_at=now.isoformat(),
            flagged=flagged,
            flag_reason=flag_reason,
        )
    )
    conn.commit()


def _seed_child_rows(conn, run_id: str) -> None:
    """Seed one child row in each child table for cascade-delete tests."""
    conn.execute(
        insert(tool_calls_table).values(
            run_id=run_id,
            tool_name="Bash",
            arguments_summary="ls",
            result_summary="ok",
            is_error=False,
            phase="single",
            timestamp=datetime.now().isoformat(),
            duration_ms=10,
        )
    )
    conn.execute(
        insert(file_activity_table).values(
            run_id=run_id,
            file_path="/tmp/x",
            action="edit",
            timestamp=datetime.now().isoformat(),
        )
    )
    conn.execute(
        insert(evaluations_table).values(
            run_id=run_id,
            eval_type="auto",
            checks=None,
            score=0.75,
            categories=None,
            notes=None,
        )
    )
    conn.execute(
        insert(artifacts_table).values(
            id=f"art-{run_id}",
            run_id=run_id,
            artifact_type="summary",
            title="t",
            content="c",
            content_type="text/markdown",
        )
    )
    conn.execute(
        insert(conversations_table).values(
            run_id=run_id,
            messages="[]",
            message_count=0,
        )
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Help / usage
# ---------------------------------------------------------------------------


class TestAuditUsage:
    def test_audit_no_subcommand_prints_usage(self, tmp_path, monkeypatch):
        _setup(tmp_path, monkeypatch)
        runner = CliRunner()
        result = runner.invoke(cli, ["audit"])
        assert result.exit_code == 0, result.output
        assert "secrets" in result.output
        assert "list" in result.output
        assert "unflag" in result.output
        assert "drop" in result.output


# ---------------------------------------------------------------------------
# Mode 1 — secrets (interactive review)
# ---------------------------------------------------------------------------


class TestAuditSecrets:
    def test_audit_secrets_finds_seeded_secret(self, tmp_path, monkeypatch):
        db_path = _setup(tmp_path, monkeypatch)
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            _seed_run(
                conn,
                run_id="run-with-secret",
                task=f"using {SEEDED_AWS_KEY} in config",
            )
        finally:
            conn.close()

        runner = CliRunner()
        result = runner.invoke(cli, ["audit", "secrets"], input="t\n")
        assert result.exit_code == 0, result.output
        assert "aws_access_key" in result.output
        assert "Flagged: 1" in result.output

        # Verify the DB was updated
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            row = conn.execute(
                runs_table.select().where(runs_table.c.id == "run-with-secret")
            ).mappings().fetchone()
            assert row["flagged"] is True or row["flagged"] == 1
            assert "secret_detected:aws_access_key" in (row["flag_reason"] or "")
        finally:
            conn.close()

    def test_audit_secrets_false_positive_does_not_flag(self, tmp_path, monkeypatch):
        db_path = _setup(tmp_path, monkeypatch)
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            _seed_run(
                conn,
                run_id="fp-run",
                task=f"example {SEEDED_AWS_KEY}",
            )
        finally:
            conn.close()

        runner = CliRunner()
        result = runner.invoke(cli, ["audit", "secrets"], input="f\n")
        assert result.exit_code == 0, result.output
        assert "false positives: 1" in result.output

        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            row = conn.execute(
                runs_table.select().where(runs_table.c.id == "fp-run")
            ).mappings().fetchone()
            assert not row["flagged"]
            assert row["flag_reason"] is None
        finally:
            conn.close()

    def test_audit_secrets_skip_does_not_flag(self, tmp_path, monkeypatch):
        db_path = _setup(tmp_path, monkeypatch)
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            _seed_run(
                conn,
                run_id="skip-run",
                task=f"has {SEEDED_AWS_KEY} here",
            )
        finally:
            conn.close()

        runner = CliRunner()
        result = runner.invoke(cli, ["audit", "secrets"], input="s\n")
        assert result.exit_code == 0, result.output
        assert "Flagged: 0" in result.output
        assert "skipped: 1" in result.output

        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            row = conn.execute(
                runs_table.select().where(runs_table.c.id == "skip-run")
            ).mappings().fetchone()
            assert not row["flagged"]
        finally:
            conn.close()

    def test_audit_secrets_quit_stops_review(self, tmp_path, monkeypatch):
        db_path = _setup(tmp_path, monkeypatch)
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            # Three runs with secrets — ordered by started_at DESC so run-a
            # (offset=0) comes first.
            _seed_run(
                conn, run_id="run-a",
                task=f"first {SEEDED_AWS_KEY}", started_offset_hours=0,
            )
            _seed_run(
                conn, run_id="run-b",
                task=f"second {SEEDED_AWS_KEY}", started_offset_hours=1,
            )
            _seed_run(
                conn, run_id="run-c",
                task=f"third {SEEDED_AWS_KEY}", started_offset_hours=2,
            )
        finally:
            conn.close()

        runner = CliRunner()
        result = runner.invoke(cli, ["audit", "secrets"], input="q\n")
        assert result.exit_code == 0, result.output
        assert "quit early" in result.output

        # No run should have been flagged — quit before any 't'
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            rows = conn.execute(
                runs_table.select().where(runs_table.c.flagged.is_(True))
            ).fetchall()
            assert rows == []
        finally:
            conn.close()

    def test_audit_secrets_no_findings(self, tmp_path, monkeypatch):
        db_path = _setup(tmp_path, monkeypatch)
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            _seed_run(conn, run_id="clean-run", task="harmless task description")
        finally:
            conn.close()

        runner = CliRunner()
        result = runner.invoke(cli, ["audit", "secrets"])
        assert result.exit_code == 0, result.output
        assert "Flagged: 0" in result.output
        assert "Scanned 1 run" in result.output


# ---------------------------------------------------------------------------
# Mode 2 — list
# ---------------------------------------------------------------------------


class TestAuditList:
    def test_audit_list_empty(self, tmp_path, monkeypatch):
        _setup(tmp_path, monkeypatch)
        runner = CliRunner()
        result = runner.invoke(cli, ["audit", "list"])
        assert result.exit_code == 0, result.output
        assert "No flagged runs" in result.output

    def test_audit_list_shows_flagged_runs(self, tmp_path, monkeypatch):
        db_path = _setup(tmp_path, monkeypatch)
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            _seed_run(
                conn,
                run_id="flagged-one",
                task="task with secret",
                flagged=True,
                flag_reason="secret_detected:aws_access_key",
            )
            _seed_run(
                conn,
                run_id="not-flagged",
                task="clean task",
            )
        finally:
            conn.close()

        runner = CliRunner()
        result = runner.invoke(cli, ["audit", "list"])
        assert result.exit_code == 0, result.output
        assert "flagged" in result.output.lower()
        assert "flagged-" in result.output  # short id prefix
        assert "aws_access_key" in result.output
        assert "not-flag" not in result.output


# ---------------------------------------------------------------------------
# Mode 3 — unflag
# ---------------------------------------------------------------------------


class TestAuditUnflag:
    def test_audit_unflag_clears_flag(self, tmp_path, monkeypatch):
        db_path = _setup(tmp_path, monkeypatch)
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            _seed_run(
                conn,
                run_id="unflag-target",
                flagged=True,
                flag_reason="secret_detected:aws_access_key",
            )
        finally:
            conn.close()

        runner = CliRunner()
        # Full ID should match
        result = runner.invoke(cli, ["audit", "unflag", "unflag-target"])
        assert result.exit_code == 0, result.output
        assert "Unflagged" in result.output

        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            row = conn.execute(
                runs_table.select().where(runs_table.c.id == "unflag-target")
            ).mappings().fetchone()
            assert not row["flagged"]
            assert row["flag_reason"] is None
        finally:
            conn.close()

    def test_audit_unflag_not_found_returns_error(self, tmp_path, monkeypatch):
        _setup(tmp_path, monkeypatch)
        runner = CliRunner()
        result = runner.invoke(cli, ["audit", "unflag", "does-not-exist"])
        assert result.exit_code == 1
        assert "not found" in result.output.lower()


# ---------------------------------------------------------------------------
# Mode 4 — drop (cascade)
# ---------------------------------------------------------------------------


class TestAuditDrop:
    def test_audit_drop_confirmation_yes_cascades(self, tmp_path, monkeypatch):
        db_path = _setup(tmp_path, monkeypatch)
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            _seed_run(
                conn,
                run_id="drop-me",
                flagged=True,
                flag_reason="secret_detected:aws_access_key",
            )
            _seed_child_rows(conn, "drop-me")
            _seed_run(conn, run_id="keep-me")
        finally:
            conn.close()

        runner = CliRunner()
        result = runner.invoke(cli, ["audit", "drop", "--yes"])
        assert result.exit_code == 0, result.output
        assert "Deleted 1" in result.output

        # Verify cascade — child rows for drop-me are gone, keep-me remains
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            assert conn.execute(
                runs_table.select().where(runs_table.c.id == "drop-me")
            ).fetchone() is None
            assert conn.execute(
                runs_table.select().where(runs_table.c.id == "keep-me")
            ).fetchone() is not None
            assert conn.execute(
                tool_calls_table.select().where(
                    tool_calls_table.c.run_id == "drop-me"
                )
            ).fetchone() is None
            assert conn.execute(
                file_activity_table.select().where(
                    file_activity_table.c.run_id == "drop-me"
                )
            ).fetchone() is None
            assert conn.execute(
                evaluations_table.select().where(
                    evaluations_table.c.run_id == "drop-me"
                )
            ).fetchone() is None
            assert conn.execute(
                artifacts_table.select().where(
                    artifacts_table.c.run_id == "drop-me"
                )
            ).fetchone() is None
            assert conn.execute(
                conversations_table.select().where(
                    conversations_table.c.run_id == "drop-me"
                )
            ).fetchone() is None
        finally:
            conn.close()

    def test_audit_drop_confirmation_no(self, tmp_path, monkeypatch):
        db_path = _setup(tmp_path, monkeypatch)
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            _seed_run(
                conn,
                run_id="keep-after-abort",
                flagged=True,
                flag_reason="secret_detected:aws_access_key",
            )
        finally:
            conn.close()

        runner = CliRunner()
        result = runner.invoke(cli, ["audit", "drop"], input="no\n")
        assert result.exit_code == 0, result.output
        assert "Aborted" in result.output

        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            row = conn.execute(
                runs_table.select().where(runs_table.c.id == "keep-after-abort")
            ).mappings().fetchone()
            assert row is not None
            assert row["flagged"]
        finally:
            conn.close()

    def test_audit_drop_no_flagged_runs(self, tmp_path, monkeypatch):
        db_path = _setup(tmp_path, monkeypatch)
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            _seed_run(conn, run_id="clean-run")
        finally:
            conn.close()

        runner = CliRunner()
        result = runner.invoke(cli, ["audit", "drop", "--yes"])
        assert result.exit_code == 0, result.output
        assert "No flagged runs" in result.output
