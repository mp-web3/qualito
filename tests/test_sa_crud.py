"""Tests for SQLAlchemy Core CRUD functions (Phase 2).

Tests all SA bridge functions in db.py, measure.py, incident_detector.py, and benchmark.py
using a fresh in-memory SQLite engine via SA. Verifies that the SA path produces correct
results (insert/query/update) without touching the legacy raw-SQL path.
"""

import json

import pytest
from sqlalchemy import select

from qualito.core.db import (
    baselines_table,
    benchmark_suites_table,
    evaluations_table,
    experiment_comparisons_table,
    experiments_table,
    get_artifacts,
    get_engine,
    get_incident,
    get_incidents,
    get_metrics,
    get_run,
    incidents_table,
    incident_events_table,
    init_db,
    insert_artifact,
    insert_evaluation,
    insert_file_activity,
    insert_incident,
    insert_incident_event,
    insert_run,
    insert_tool_calls,
    runs_table,
    system_changes_table,
    tool_calls_table,
    update_incident,
    update_run,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sa_conn(tmp_path):
    """Provide a fresh SA connection with all tables created."""
    db_file = str(tmp_path / "test.db")
    engine = get_engine(db_file)
    init_db(engine)
    conn = engine.connect()
    yield conn
    conn.close()


@pytest.fixture
def sample_run():
    """Minimal run dict for insertion."""
    return {
        "id": "20260408-120000",
        "workspace": "test-ws",
        "task": "fix a bug",
        "task_type": "bugfix",
        "model": "claude-sonnet-4-20250514",
        "pipeline_mode": "single",
        "status": "completed",
        "started_at": "2026-04-08T12:00:00Z",
        "cost_usd": 0.42,
        "duration_ms": 15000,
    }


@pytest.fixture
def populated_db(sa_conn, sample_run):
    """DB with one run, one evaluation, one tool call, one file activity."""
    insert_run(sa_conn, sample_run)
    insert_evaluation(sa_conn, sample_run["id"], "auto",
                      checks={"completion": True}, score=0.75,
                      categories={"completion": 0.8, "quality": 0.7})
    insert_evaluation(sa_conn, sample_run["id"], "dqi",
                      score=0.65,
                      categories={"completion": 0.7, "quality": 0.6,
                                  "efficiency": 0.7, "cost_score": 0.5})

    class FakeTool:
        tool_name = "Read"
        arguments_summary = "file.py"
        result_summary = "ok"
        is_error = False
        phase = "single"
        timestamp = "2026-04-08T12:01:00Z"
        duration_ms = 200

    insert_tool_calls(sa_conn, sample_run["id"], [FakeTool()])

    class FakeFile:
        file_path = "src/app.py"
        action = "edit"
        timestamp = "2026-04-08T12:02:00Z"

    insert_file_activity(sa_conn, sample_run["id"], [FakeFile()])
    return sa_conn


# ---------------------------------------------------------------------------
# db.py CRUD tests
# ---------------------------------------------------------------------------


class TestInsertRun:
    def test_basic_insert(self, sa_conn, sample_run):
        insert_run(sa_conn, sample_run)
        row = sa_conn.execute(
            select(runs_table).where(runs_table.c.id == sample_run["id"])
        ).mappings().fetchone()
        assert row is not None
        assert row["workspace"] == "test-ws"
        assert row["task"] == "fix a bug"
        assert row["status"] == "completed"

    def test_optional_fields_default_to_none(self, sa_conn):
        minimal = {
            "id": "run-minimal",
            "workspace": "ws",
            "task": "t",
            "started_at": "2026-01-01T00:00:00Z",
        }
        insert_run(sa_conn, minimal)
        row = sa_conn.execute(
            select(runs_table).where(runs_table.c.id == "run-minimal")
        ).mappings().fetchone()
        assert row["task_type"] is None
        assert row["model"] is None


class TestUpdateRun:
    def test_update_status(self, sa_conn, sample_run):
        insert_run(sa_conn, sample_run)
        update_run(sa_conn, sample_run["id"], status="failed", summary="broke")
        row = sa_conn.execute(
            select(runs_table).where(runs_table.c.id == sample_run["id"])
        ).mappings().fetchone()
        assert row["status"] == "failed"
        assert row["summary"] == "broke"

    def test_update_noop(self, sa_conn, sample_run):
        insert_run(sa_conn, sample_run)
        update_run(sa_conn, sample_run["id"])  # no fields — should not raise


class TestInsertEvaluation:
    def test_with_checks_and_score(self, sa_conn, sample_run):
        insert_run(sa_conn, sample_run)
        insert_evaluation(sa_conn, sample_run["id"], "auto",
                          checks={"completion": True}, score=0.8)
        row = sa_conn.execute(
            select(evaluations_table)
            .where(evaluations_table.c.run_id == sample_run["id"])
        ).mappings().fetchone()
        assert row["eval_type"] == "auto"
        assert row["score"] == 0.8
        assert json.loads(row["checks"]) == {"completion": True}


class TestGetRun:
    def test_returns_full_run(self, populated_db, sample_run):
        run = get_run(populated_db, sample_run["id"])
        assert run is not None
        assert run["id"] == sample_run["id"]
        assert len(run["evaluations"]) == 2
        assert len(run["tool_calls"]) == 1
        assert len(run["file_activity"]) == 1

    def test_missing_run_returns_none(self, sa_conn):
        assert get_run(sa_conn, "nonexistent") is None


class TestGetMetrics:
    def test_overall_stats(self, populated_db):
        metrics = get_metrics(populated_db)
        assert metrics["total"]["total"] == 1
        assert metrics["total"]["completed"] == 1
        assert metrics["avg_score"] == 0.75  # from auto eval
        assert len(metrics["by_workspace"]) == 1
        assert metrics["by_workspace"][0]["workspace"] == "test-ws"

    def test_filter_by_workspace(self, populated_db):
        metrics = get_metrics(populated_db, workspace="nonexistent")
        assert metrics["total"]["total"] == 0


class TestArtifacts:
    def test_insert_and_get(self, sa_conn, sample_run):
        insert_run(sa_conn, sample_run)
        artifact = {
            "id": "art-1",
            "run_id": sample_run["id"],
            "artifact_type": "report",
            "title": "Test Report",
            "content": "some content",
            "workspace": "test-ws",
        }
        insert_artifact(sa_conn, artifact)
        results = get_artifacts(sa_conn, run_id=sample_run["id"])
        assert len(results) == 1
        assert results[0]["title"] == "Test Report"

    def test_filter_by_type(self, sa_conn, sample_run):
        insert_run(sa_conn, sample_run)
        insert_artifact(sa_conn, {
            "id": "a1", "run_id": sample_run["id"],
            "artifact_type": "report", "title": "R1",
        })
        insert_artifact(sa_conn, {
            "id": "a2", "run_id": sample_run["id"],
            "artifact_type": "log", "title": "L1",
        })
        reports = get_artifacts(sa_conn, artifact_type="report")
        assert len(reports) == 1
        assert reports[0]["artifact_type"] == "report"


# ---------------------------------------------------------------------------
# Incident CRUD tests
# ---------------------------------------------------------------------------


class TestIncidentCRUD:
    def test_insert_and_get_incident(self, sa_conn):
        inc = {
            "incident_key": "test_inc_1",
            "category": "quality",
            "severity": "warning",
            "status": "detected",
            "workspace": "test-ws",
            "title": "DQI dropped",
        }
        inc_id = insert_incident(sa_conn, inc)
        assert isinstance(inc_id, int)

        fetched = get_incident(sa_conn, inc_id)
        assert fetched is not None
        assert fetched["incident_key"] == "test_inc_1"
        assert fetched["events"] == []

    def test_insert_incident_event(self, sa_conn):
        inc_id = insert_incident(sa_conn, {
            "incident_key": "test_inc_2",
            "category": "cost",
            "severity": "info",
            "status": "detected",
            "workspace": "ws",
            "title": "Cost spike",
        })
        insert_incident_event(sa_conn, inc_id, "status_change",
                              old_status="detected", new_status="confirmed",
                              data={"reason": "manual review"})
        fetched = get_incident(sa_conn, inc_id)
        assert len(fetched["events"]) == 1
        assert fetched["events"][0]["event_type"] == "status_change"
        assert json.loads(fetched["events"][0]["data"]) == {"reason": "manual review"}

    def test_update_incident(self, sa_conn):
        inc_id = insert_incident(sa_conn, {
            "incident_key": "test_inc_3",
            "category": "availability",
            "severity": "critical",
            "status": "detected",
            "workspace": "ws",
            "title": "Failures",
        })
        update_incident(sa_conn, inc_id, status="resolved",
                        resolution_type="auto",
                        affected_run_ids=["r1", "r2"])
        fetched = get_incident(sa_conn, inc_id)
        assert fetched["status"] == "resolved"
        assert fetched["resolution_type"] == "auto"
        assert json.loads(fetched["affected_run_ids"]) == ["r1", "r2"]

    def test_get_incidents_with_filters(self, sa_conn):
        for i, sev in enumerate(["warning", "critical", "warning"]):
            insert_incident(sa_conn, {
                "incident_key": f"k{i}",
                "category": "quality",
                "severity": sev,
                "status": "detected",
                "workspace": "ws",
                "title": f"Inc {i}",
            })
        all_incs = get_incidents(sa_conn)
        assert len(all_incs) == 3

        warnings = get_incidents(sa_conn, severity="warning")
        assert len(warnings) == 2

        critical = get_incidents(sa_conn, severity="critical")
        assert len(critical) == 1

    def test_get_incident_missing(self, sa_conn):
        assert get_incident(sa_conn, 9999) is None


# ---------------------------------------------------------------------------
# incident_detector.py tests
# ---------------------------------------------------------------------------


class TestComputeWorkspaceBaselines:
    def test_fallback_baselines_with_few_runs(self, sa_conn):
        """With < 5 scored runs, returns SLO defaults."""
        from qualito.core.incident_detector import (
            DEFAULT_SLO_AVAILABILITY,
            DEFAULT_SLO_COST,
            DEFAULT_SLO_QUALITY,
            _baseline_cache,
            compute_workspace_baselines,
        )
        _baseline_cache.clear()

        # Insert 3 runs (below the 5-run minimum)
        for i in range(3):
            insert_run(sa_conn, {
                "id": f"base-run-{i}",
                "workspace": "baseline-ws",
                "task": f"task {i}",
                "status": "completed",
                "started_at": f"2026-04-0{i+1}T00:00:00Z",
            })
            insert_evaluation(sa_conn, f"base-run-{i}", "dqi", score=0.7)

        baselines = compute_workspace_baselines(sa_conn, "baseline-ws")
        assert baselines["mean_dqi"] == DEFAULT_SLO_QUALITY
        assert baselines["completion_rate"] == DEFAULT_SLO_AVAILABILITY
        assert baselines["mean_cost"] == DEFAULT_SLO_COST
        assert baselines["sample_size"] == 3

    def test_computed_baselines_with_enough_runs(self, sa_conn):
        """With >= 5 scored runs, computes real baselines."""
        from qualito.core.incident_detector import (
            _baseline_cache,
            compute_workspace_baselines,
        )
        _baseline_cache.clear()

        for i in range(6):
            insert_run(sa_conn, {
                "id": f"comp-run-{i}",
                "workspace": "comp-ws",
                "task": f"task {i}",
                "status": "completed",
                "started_at": f"2026-04-0{i+1}T00:00:00Z",
                "cost_usd": 0.5 + i * 0.1,
            })
            insert_evaluation(sa_conn, f"comp-run-{i}", "dqi", score=0.5 + i * 0.05)

        baselines = compute_workspace_baselines(sa_conn, "comp-ws")
        assert baselines["sample_size"] == 6
        assert 0.5 <= baselines["mean_dqi"] <= 0.8
        assert baselines["stddev_dqi"] > 0
        assert baselines["completion_rate"] == 1.0


class TestCheckConsecutiveFailures:
    def test_no_incident_when_runs_complete(self, sa_conn):
        from qualito.core.incident_detector import check_consecutive_failures

        for i in range(5):
            insert_run(sa_conn, {
                "id": f"ok-run-{i}",
                "workspace": "ws-ok",
                "task": f"task {i}",
                "status": "completed",
                "started_at": f"2026-04-0{i+1}T00:00:00Z",
            })

        result = check_consecutive_failures(sa_conn, "ok-run-4", "ws-ok")
        assert result is None

    def test_detects_consecutive_failures(self, sa_conn):
        from qualito.core.incident_detector import check_consecutive_failures

        for i in range(4):
            insert_run(sa_conn, {
                "id": f"fail-run-{i}",
                "workspace": "ws-fail",
                "task": f"task {i}",
                "status": "failed",
                "started_at": f"2026-04-0{i+1}T00:00:00Z",
            })

        result = check_consecutive_failures(sa_conn, "fail-run-3", "ws-fail")
        assert result is not None
        assert result["severity"] == "critical"
        assert result["category"] == "availability"
        assert result["trigger_value"] == 4.0


class TestCheckCostAnomaly:
    def test_no_anomaly_when_costs_normal(self, sa_conn):
        from qualito.core.incident_detector import (
            _baseline_cache,
            check_cost_anomaly,
        )
        _baseline_cache.clear()

        for i in range(10):
            insert_run(sa_conn, {
                "id": f"normal-cost-{i}",
                "workspace": "ws-cost",
                "task": f"task {i}",
                "status": "completed",
                "started_at": f"2026-04-0{i+1}T00:00:00Z" if i < 9 else f"2026-04-{i+1}T00:00:00Z",
                "cost_usd": 0.50,
            })
            insert_evaluation(sa_conn, f"normal-cost-{i}", "dqi", score=0.7)

        result = check_cost_anomaly(sa_conn, "normal-cost-9", "ws-cost")
        assert result is None


# ---------------------------------------------------------------------------
# measure.py tests
# ---------------------------------------------------------------------------


class TestTakeBaseline:
    def test_creates_baseline_record(self, sa_conn):
        from qualito.core.measure import take_baseline

        # Need DQI-scored runs
        for i in range(5):
            insert_run(sa_conn, {
                "id": f"bl-run-{i}",
                "workspace": "bl-ws",
                "task": f"task {i}",
                "status": "completed",
                "started_at": "2026-04-01T00:00:00Z",
                "cost_usd": 0.30 + i * 0.1,
            })
            insert_evaluation(sa_conn, f"bl-run-{i}", "dqi",
                              score=0.6 + i * 0.05,
                              categories={
                                  "completion": 0.7, "quality": 0.6,
                                  "efficiency": 0.5, "cost_score": 0.5,
                              })

        take_baseline("test-baseline", description="test", days=365, conn=sa_conn)

        row = sa_conn.execute(
            select(baselines_table)
            .where(baselines_table.c.name == "test-baseline")
        ).mappings().fetchone()
        assert row is not None
        metrics = json.loads(row["metrics"])
        assert metrics["run_count"] == 5
        assert 0.5 <= metrics["avg_dqi"] <= 1.0

    def test_no_data_prints_message(self, sa_conn, capsys):
        from qualito.core.measure import take_baseline

        take_baseline("empty-baseline", days=30, conn=sa_conn)
        captured = capsys.readouterr()
        assert "No DQI data" in captured.out


class TestRegisterChange:
    def test_creates_change_record(self, sa_conn):
        from qualito.core.measure import register_change, take_baseline

        # Create a baseline first
        for i in range(5):
            insert_run(sa_conn, {
                "id": f"rc-run-{i}",
                "workspace": "rc-ws",
                "task": f"task {i}",
                "status": "completed",
                "started_at": "2026-04-01T00:00:00Z",
                "cost_usd": 0.30,
            })
            insert_evaluation(sa_conn, f"rc-run-{i}", "dqi",
                              score=0.7,
                              categories={
                                  "completion": 0.7, "quality": 0.7,
                                  "efficiency": 0.7, "cost_score": 0.7,
                              })

        take_baseline("rc-baseline", days=365, conn=sa_conn)
        register_change("test-change", description="testing", conn=sa_conn)

        row = sa_conn.execute(
            select(system_changes_table)
            .where(system_changes_table.c.change_name == "test-change")
        ).mappings().fetchone()
        assert row is not None
        assert row["status"] == "measuring"
        assert row["before_metrics"] is not None

    def test_no_baseline_prints_message(self, sa_conn, capsys):
        from qualito.core.measure import register_change

        register_change("orphan-change", conn=sa_conn)
        captured = capsys.readouterr()
        assert "No baseline found" in captured.out


class TestMonitor:
    def test_needs_minimum_runs(self, sa_conn, capsys):
        from qualito.core.measure import monitor

        # Only 3 runs — below the 10-run minimum
        for i in range(3):
            insert_run(sa_conn, {
                "id": f"mon-run-{i}",
                "workspace": "mon-ws",
                "task": f"task {i}",
                "status": "completed",
                "started_at": "2026-04-01T00:00:00Z",
            })
            insert_evaluation(sa_conn, f"mon-run-{i}", "dqi", score=0.7)

        monitor(conn=sa_conn)
        captured = capsys.readouterr()
        assert "Need at least 10" in captured.out

    def test_runs_with_enough_data(self, sa_conn, capsys):
        from qualito.core.measure import monitor

        for i in range(15):
            insert_run(sa_conn, {
                "id": f"monok-run-{i}",
                "workspace": "monok-ws",
                "task": f"task {i}",
                "status": "completed",
                "started_at": f"2026-04-{i+1:02d}T00:00:00Z",
            })
            insert_evaluation(sa_conn, f"monok-run-{i}", "dqi",
                              score=0.6 + (i % 5) * 0.05)

        monitor(conn=sa_conn)
        captured = capsys.readouterr()
        assert "DQI Monitor" in captured.out
        assert "Target DQI" in captured.out


class TestShowStatus:
    def test_shows_baselines_and_changes(self, sa_conn, capsys):
        from qualito.core.measure import show_status

        show_status(conn=sa_conn)
        captured = capsys.readouterr()
        assert "Baselines (0)" in captured.out
        assert "System Changes (0)" in captured.out


# ---------------------------------------------------------------------------
# benchmark.py tests
# ---------------------------------------------------------------------------


class TestDefineSuite:
    def test_creates_suite(self, sa_conn):
        from qualito.core.benchmark import define_suite

        tasks = [
            {"label": "t1", "workspace": "ws", "pipeline_mode": "single", "task": "do X"},
            {"label": "t2", "workspace": "ws", "pipeline_mode": "single", "task": "do Y"},
        ]
        define_suite("test-suite", tasks, description="testing", conn=sa_conn)

        row = sa_conn.execute(
            select(benchmark_suites_table)
            .where(benchmark_suites_table.c.name == "test-suite")
        ).mappings().fetchone()
        assert row is not None
        assert json.loads(row["tasks"]) == tasks
        assert row["description"] == "testing"

    def test_duplicate_suite_no_error(self, sa_conn, capsys):
        from qualito.core.benchmark import define_suite

        tasks = [{"label": "t1", "workspace": "ws", "pipeline_mode": "single", "task": "X"}]
        define_suite("dup-suite", tasks, conn=sa_conn)
        define_suite("dup-suite", tasks, conn=sa_conn)  # should not raise
        captured = capsys.readouterr()
        assert "already exists" in captured.out


class TestCompareExperiments:
    def test_missing_experiment_prints_error(self, sa_conn, capsys):
        from qualito.core.benchmark import compare_experiments

        compare_experiments("nonexistent-a", "nonexistent-b", conn=sa_conn)
        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_compares_two_completed_experiments(self, sa_conn, capsys):
        from qualito.core.benchmark import compare_experiments

        # Create suite
        sa_conn.execute(
            benchmark_suites_table.insert().values(
                name="cmp-suite", description="", tasks="[]"
            )
        )
        sa_conn.commit()
        suite_row = sa_conn.execute(
            select(benchmark_suites_table.c.id)
            .where(benchmark_suites_table.c.name == "cmp-suite")
        ).mappings().fetchone()
        suite_id = suite_row["id"]

        # Create two completed experiments
        per_task_a = json.dumps({"t1": 0.6, "t2": 0.5})
        per_task_b = json.dumps({"t1": 0.8, "t2": 0.7})

        sa_conn.execute(experiments_table.insert().values(
            name="exp-before", suite_id=suite_id, status="completed",
            avg_dqi=0.55, per_task_dqi=per_task_a, run_ids="[]",
        ))
        sa_conn.execute(experiments_table.insert().values(
            name="exp-after", suite_id=suite_id, status="completed",
            avg_dqi=0.75, per_task_dqi=per_task_b, run_ids="[]",
        ))
        sa_conn.commit()

        compare_experiments("exp-before", "exp-after", conn=sa_conn)
        captured = capsys.readouterr()
        assert "Comparison" in captured.out
        assert "exp-before" in captured.out

        # Verify comparison record was stored
        comp = sa_conn.execute(
            select(experiment_comparisons_table)
        ).mappings().fetchone()
        assert comp is not None
        assert comp["verdict"] in ("improved", "degraded", "inconclusive")


class TestBenchmarkShowStatus:
    def test_shows_empty_status(self, sa_conn, capsys):
        from qualito.core.benchmark import show_status

        show_status(conn=sa_conn)
        captured = capsys.readouterr()
        assert "Benchmark Status" in captured.out
        assert "Suites (0)" in captured.out
        assert "Experiments (0)" in captured.out

    def test_shows_populated_status(self, sa_conn, capsys):
        from qualito.core.benchmark import define_suite, show_status

        tasks = [{"label": "t1", "workspace": "ws", "pipeline_mode": "single", "task": "X"}]
        define_suite("status-suite", tasks, conn=sa_conn)

        show_status(conn=sa_conn)
        captured = capsys.readouterr()
        assert "Suites (1)" in captured.out
        assert "status-suite" in captured.out
