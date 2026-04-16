"""Auto-evaluation engine for Claude Code sessions.

Runs 8 checks on every completed session and stores results in the evaluations table.
Also handles human scoring with per-task-type rubrics.
"""

import json

from qualito.core.db import get_run, insert_evaluation

# Cost thresholds per task type (USD)
COST_THRESHOLDS = {
    "pr_review": 5.0,
    "code": 8.0,
    "slack": 2.0,
    "jira": 2.0,
    "confluence": 3.0,
    "research": 5.0,
    "test": 5.0,
    "other": 5.0,
}

# Human scoring rubrics per task type
RUBRICS = {
    "pr_review": [
        "findings_accuracy", "false_positive_rate", "actionability",
        "security_coverage", "scope_discipline",
    ],
    "code": [
        "correctness", "test_coverage", "style_adherence",
        "scope_discipline", "performance_awareness",
    ],
    "slack": [
        "tone_accuracy", "factual_correctness", "actionability", "formatting",
    ],
    "research": [
        "depth", "source_quality", "actionability", "completeness",
    ],
}


def _check_completed(run: dict) -> tuple[bool, str]:
    return run["status"] == "completed", run.get("status", "unknown")


def _check_error_rate(run: dict) -> tuple[bool, str]:
    """Low error rate: fewer than 25% of tool calls errored."""
    error_count = run.get("error_count") or 0
    tool_count = run.get("tool_count") or 0
    if tool_count == 0:
        return True, "no tool calls"
    rate = error_count / tool_count
    return rate < 0.25, f"{rate:.0%} error rate ({error_count}/{tool_count})"


def _check_tool_calls_made(run: dict) -> tuple[bool, str]:
    count = len(run.get("tool_calls", []))
    return count > 0, f"{count} tool calls"


def _check_tool_diversity(run: dict) -> tuple[bool, str]:
    """Used 3+ distinct tool types (indicates productive session, not thrashing)."""
    tool_calls = run.get("tool_calls", [])
    distinct_tools = set(tc.get("tool_name", "") for tc in tool_calls if tc.get("tool_name"))
    count = len(distinct_tools)
    return count >= 3, f"{count} distinct tools"


def _check_cache_utilization(run: dict) -> tuple[bool, str]:
    """Cache read tokens are at least 10% of input tokens (efficient context reuse)."""
    cache = run.get("cache_read_tokens") or 0
    input_tokens = run.get("input_tokens") or 0
    if input_tokens == 0:
        return True, "no input tokens"
    rate = cache / input_tokens
    return rate >= 0.10, f"{rate:.0%} cache hit rate"


def _check_cost_reasonable(run: dict) -> tuple[bool, str]:
    cost = run.get("cost_usd") or 0
    task_type = run.get("task_type", "other")
    threshold = COST_THRESHOLDS.get(task_type, 5.0)
    return cost <= threshold, f"${cost:.2f} (limit ${threshold:.2f})"


def _check_within_timeout(run: dict) -> tuple[bool, str]:
    duration = run.get("duration_ms")
    if duration is None:
        return True, "no duration"
    timeout_ms = 600_000  # 10 min default
    return duration <= timeout_ms, f"{duration/1000:.0f}s (limit {timeout_ms/1000:.0f}s)"


def _check_completion_with_work(run: dict) -> tuple[bool, str]:
    """Session completed AND did actual work (tool_count > 0)."""
    completed = run.get("status") == "completed"
    tool_count = run.get("tool_count") or 0
    if not completed:
        return False, f"status={run.get('status')}"
    if tool_count == 0:
        return False, "completed but 0 tool calls"
    return True, f"completed with {tool_count} tool calls"


ALL_CHECKS = [
    ("completed", _check_completed),
    ("error_rate", _check_error_rate),
    ("tool_calls_made", _check_tool_calls_made),
    ("tool_diversity", _check_tool_diversity),
    ("cost_reasonable", _check_cost_reasonable),
    ("within_timeout", _check_within_timeout),
    ("cache_utilization", _check_cache_utilization),
    ("completion_with_work", _check_completion_with_work),
]


def auto_evaluate(run_id: str, conn=None) -> dict:
    """Run all auto-eval checks on a completed run. Returns checks dict.

    Args:
        run_id: The run ID to evaluate.
        conn: Optional DB connection. If None, opens and closes its own.
    """
    owns_conn = conn is None
    if owns_conn:
        from qualito.core.db import get_sa_connection
        conn = get_sa_connection()

    run = get_run(conn, run_id)
    if not run:
        if owns_conn:
            conn.close()
        return {}

    checks = {}
    passed = 0
    total = 0

    for name, check_fn in ALL_CHECKS:
        try:
            ok, detail = check_fn(run)
        except Exception as e:
            ok, detail = False, f"error: {e}"
        checks[name] = {"passed": ok, "detail": detail}
        if ok:
            passed += 1
        total += 1

    score = passed / total if total > 0 else 0.0

    insert_evaluation(
        conn, run_id,
        eval_type="auto",
        checks=checks,
        score=score,
    )
    if owns_conn:
        conn.close()
    return checks


def human_score(run_id: str, quality: int, notes: str = "",
                categories: dict | None = None, conn=None) -> dict:
    """Record a human evaluation for a run.

    Args:
        run_id: Delegation run ID.
        quality: Overall quality score 0-10.
        notes: Free-form notes.
        categories: Optional per-category scores (0-10 each).
        conn: Optional DB connection. If None, opens and closes its own.
    """
    owns_conn = conn is None
    if owns_conn:
        from qualito.core.db import get_sa_connection
        conn = get_sa_connection()

    run = get_run(conn, run_id)
    if not run:
        if owns_conn:
            conn.close()
        return {"error": f"Run {run_id} not found"}

    # Validate categories against rubric
    task_type = run.get("task_type", "other")
    rubric = RUBRICS.get(task_type)
    if categories and rubric:
        invalid = [k for k in categories if k not in rubric]
        if invalid:
            if owns_conn:
                conn.close()
            return {"error": f"Invalid categories for {task_type}: {invalid}. Valid: {rubric}"}

    score = quality / 10.0  # Normalize to 0-1

    insert_evaluation(
        conn, run_id,
        eval_type="human",
        score=score,
        categories=categories,
        notes=notes,
    )
    if owns_conn:
        conn.close()
    return {"status": "ok", "score": score}
