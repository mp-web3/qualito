"""Smoke tests — verify all core modules import and basic functions work."""

import tempfile
from pathlib import Path


def test_import_db():
    from qualito.core import db
    assert hasattr(db, "get_engine")
    assert hasattr(db, "get_sa_connection")


def test_import_dqi():
    from qualito.core import dqi
    assert hasattr(dqi, "calculate_dqi")
    assert hasattr(dqi, "store_dqi")


def test_import_evaluator():
    from qualito.core import evaluator
    assert hasattr(evaluator, "auto_evaluate")
    assert hasattr(evaluator, "human_score")


def test_import_stream_parser():
    from qualito.core import stream_parser
    assert hasattr(stream_parser, "parse_stream")
    assert hasattr(stream_parser, "ParsedStream")
    assert hasattr(stream_parser, "ToolCall")
    assert hasattr(stream_parser, "FileActivity")


def test_import_measure():
    from qualito.core import measure
    assert hasattr(measure, "take_baseline")
    assert hasattr(measure, "evaluate_change")
    assert hasattr(measure, "monitor")


def test_import_benchmark():
    from qualito.core import benchmark
    assert hasattr(benchmark, "define_suite")
    assert hasattr(benchmark, "run_experiment")
    assert hasattr(benchmark, "compare_experiments")


def test_import_pattern_detector():
    from qualito.core import pattern_detector
    assert hasattr(pattern_detector, "detect_patterns")
    assert hasattr(pattern_detector, "normalize_task")


def test_import_feedback_loop():
    from qualito.core import feedback_loop
    assert hasattr(feedback_loop, "run_feedback_loop")


def test_import_core_package():
    """Test that the core __init__.py exports work."""
    from qualito.core import (
        calculate_dqi, store_dqi, auto_evaluate, human_score,
        parse_stream, ParsedStream, ToolCall, FileActivity,
        get_engine, get_run, get_metrics, insert_run, update_run,
        take_baseline, evaluate_change, monitor,
        define_suite, run_experiment, compare_experiments,
        detect_patterns, normalize_task,
        run_feedback_loop,
    )
    assert callable(calculate_dqi)
    assert callable(parse_stream)


def test_calculate_dqi_mock_run():
    """Test calculate_dqi with a mock run dict."""
    from qualito.core.dqi import calculate_dqi

    mock_run = {
        "status": "completed",
        "cost_usd": 0.25,
        "duration_ms": 45000,
        "evaluations": [
            {
                "eval_type": "auto",
                "checks": '{"completed": {"passed": true}, "has_summary": {"passed": true}, '
                          '"tool_calls_made": {"passed": true}, "chains_recorded": {"passed": true}, '
                          '"cost_reasonable": {"passed": true}, "within_timeout": {"passed": true}, '
                          '"has_findings": {"passed": true}, "has_output": {"passed": true}}',
            }
        ],
    }

    result = calculate_dqi(mock_run, task_type="code")
    assert "dqi" in result
    assert 0.0 <= result["dqi"] <= 1.0
    assert result["completion"] == 1.0
    assert result["tier"] == 2
    assert result["tier_label"] == "standard"


def test_parse_stream_empty():
    """Test parse_stream with a non-existent path returns empty ParsedStream."""
    from qualito.core.stream_parser import parse_stream, ParsedStream

    result = parse_stream(Path("/nonexistent/path/stream.jsonl"))
    assert isinstance(result, ParsedStream)
    assert result.tool_calls == []
    assert result.file_activity == []
    assert result.result is None


def test_normalize_task():
    """Test normalize_task strips IDs and lowercases."""
    from qualito.core.pattern_detector import normalize_task

    assert normalize_task("Review PR #624 on propellerswap-frontend") == "review pr #n on propellerswap-frontend"
    assert normalize_task("Read Jira ticket 1234567890") == "read jira ticket id"
    # First 8 words only
    long_task = "one two three four five six seven eight nine ten"
    assert normalize_task(long_task) == "one two three four five six seven eight"


def test_get_engine_creates_file(tmp_path):
    """Test get_engine + init_db creates a DB file with all tables."""
    from qualito.core.db import get_engine, get_sa_connection, init_db

    db_path = tmp_path / "test.db"
    engine = get_engine(str(db_path))
    init_db(engine)
    assert db_path.exists()

    conn = get_sa_connection(engine)
    # Verify tables via SA inspection
    from sqlalchemy import inspect
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    assert "runs" in table_names
    assert "evaluations" in table_names
    assert "tool_calls" in table_names
    assert "file_activity" in table_names
    assert "baselines" in table_names
    assert "experiments" in table_names

    conn.close()


def test_import_incident_detector():
    """Verify incident detector module imports."""
    from qualito.core import incident_detector
    assert hasattr(incident_detector, "check_run")
    assert hasattr(incident_detector, "check_auto_resolve")
    assert hasattr(incident_detector, "check_monitoring_close")
    assert hasattr(incident_detector, "compute_workspace_baselines")
    assert hasattr(incident_detector, "check_consecutive_failures")
    assert hasattr(incident_detector, "check_dqi_burn_rate")
    assert hasattr(incident_detector, "check_cost_anomaly")
    assert hasattr(incident_detector, "check_error_pattern_spike")


def test_incidents_table_exists(tmp_path):
    """Verify incidents and incident_events tables are in the schema."""
    from qualito.core.db import get_engine, init_db

    db_path = tmp_path / "test.db"
    engine = get_engine(str(db_path))
    init_db(engine)

    from sqlalchemy import inspect
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    assert "incidents" in table_names
    assert "incident_events" in table_names

    # Verify key columns exist
    columns = {c["name"] for c in inspector.get_columns("incidents")}
    assert "incident_key" in columns
    assert "severity" in columns
    assert "workspace" in columns
    assert "detection_method" in columns


def test_check_run_with_no_data(tmp_path):
    """Call check_run with empty DB — should return empty list, not crash."""
    from qualito.core.db import get_engine, get_sa_connection, init_db
    from qualito.core.incident_detector import check_run

    db_path = tmp_path / "test.db"
    engine = get_engine(str(db_path))
    init_db(engine)
    conn = get_sa_connection(engine)

    # Non-existent run_id
    results = check_run(conn, "nonexistent-run-id")
    assert results == []

    conn.close()
