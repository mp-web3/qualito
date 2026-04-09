"""Core Qualito modules — calculator, evaluator, parser, database, measurement, incidents."""

from qualito.core.benchmark import compare_experiments, define_suite, run_experiment
from qualito.core.db import (
    get_artifacts,
    get_engine,
    get_incident,
    get_incidents,
    get_metrics,
    get_run,
    get_sa_connection,
    init_db,
    insert_artifact,
    insert_evaluation,
    insert_file_activity,
    insert_incident,
    insert_incident_event,
    insert_run,
    insert_tool_calls,
    metadata,
    update_incident,
    update_run,
)
from qualito.core.dqi import calculate_dqi, store_dqi
from qualito.core.evaluator import auto_evaluate, human_score
from qualito.core.feedback_loop import run_feedback_loop
from qualito.core.incident_detector import (
    check_auto_resolve,
    check_run,
    compute_workspace_baselines,
)
from qualito.core.measure import evaluate_change, monitor, take_baseline
from qualito.core.pattern_detector import detect_patterns, normalize_task
from qualito.core.stream_parser import FileActivity, ParsedStream, ToolCall, parse_stream

__all__ = [
    # db
    "get_engine",
    "get_sa_connection",
    "init_db",
    "metadata",
    "get_artifacts",
    "get_incident",
    "get_incidents",
    "get_metrics",
    "get_run",
    "insert_artifact",
    "insert_evaluation",
    "insert_file_activity",
    "insert_incident",
    "insert_incident_event",
    "insert_run",
    "insert_tool_calls",
    "update_incident",
    "update_run",
    # dqi
    "calculate_dqi",
    "store_dqi",
    # evaluator
    "auto_evaluate",
    "human_score",
    # stream_parser
    "FileActivity",
    "ParsedStream",
    "ToolCall",
    "parse_stream",
    # measure
    "evaluate_change",
    "monitor",
    "take_baseline",
    # benchmark
    "compare_experiments",
    "define_suite",
    "run_experiment",
    # pattern_detector
    "detect_patterns",
    "normalize_task",
    # feedback_loop
    "run_feedback_loop",
    # incident_detector
    "check_auto_resolve",
    "check_run",
    "compute_workspace_baselines",
]
