"""Auto-evaluation engine for delegation runs.

Runs 8 checks on every completed delegation and stores results in the evaluations table.
Also handles human scoring with per-task-type rubrics.
"""

import json
import subprocess
from pathlib import Path

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


def _check_has_summary(run: dict) -> tuple[bool, str]:
    summary = run.get("summary") or ""
    return len(summary) > 0, f"{len(summary)} chars"


def _check_tool_calls_made(run: dict) -> tuple[bool, str]:
    count = len(run.get("tool_calls", []))
    return count > 0, f"{count} tool calls"


def _check_committed(run: dict) -> tuple[bool, str]:
    """Check if code was committed on a branch."""
    if run.get('pipeline_mode') == 'research-only':
        return True, 'n/a (research-only)'
    branch = run.get("branch")
    if not branch:
        return False, "no branch"
    try:
        result = subprocess.run(
            ["git", "log", f"main..{branch}", "--oneline"],
            capture_output=True, text=True, timeout=10,
            cwd=run.get("cwd", Path.home()),
        )
        commits = result.stdout.strip().split("\n") if result.stdout.strip() else []
        return len(commits) > 0, f"{len(commits)} commits"
    except (subprocess.TimeoutExpired, OSError):
        return False, "git check failed"


def _check_chains_recorded(run: dict) -> tuple[bool, str]:
    """Check if si_reason was called (reasoning chain recorded)."""
    si_calls = [tc for tc in run.get("tool_calls", []) if "si_reason" in tc.get("tool_name", "")]
    return len(si_calls) > 0, f"{len(si_calls)} chains"


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


def _check_has_findings(run: dict) -> tuple[bool, str]:
    """Task-type specific: PR reviews should have findings."""
    if run.get("task_type") != "pr_review":
        return True, "n/a"
    summary = run.get("summary", "").lower()
    has = any(w in summary for w in ["finding", "issue", "bug", "concern", "suggestion", "change"])
    return has, "findings in summary" if has else "no findings detected"


def _check_has_output(run: dict) -> tuple[bool, str]:
    """Task-type specific: code tasks should produce files."""
    if run.get('pipeline_mode') == 'research-only':
        return True, 'n/a (research-only)'
    if run.get("task_type") != "code":
        return True, "n/a"
    files = run.get("files_changed")
    if isinstance(files, str):
        try:
            files = json.loads(files)
        except json.JSONDecodeError:
            files = []
    count = len(files or [])
    return count > 0, f"{count} files"


ALL_CHECKS = [
    ("completed", _check_completed),
    ("has_summary", _check_has_summary),
    ("tool_calls_made", _check_tool_calls_made),
    ("chains_recorded", _check_chains_recorded),
    ("cost_reasonable", _check_cost_reasonable),
    ("within_timeout", _check_within_timeout),
    ("has_findings", _check_has_findings),
    ("has_output", _check_has_output),
]


def auto_evaluate(run_id: str, conn=None) -> dict:
    """Run all auto-eval checks on a completed run. Returns checks dict.

    Args:
        run_id: The run ID to evaluate.
        conn: Optional DB connection. If None, opens and closes its own.
    """
    owns_conn = conn is None
    if owns_conn:
        from qualito.core.db import get_db
        conn = get_db()

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
        from qualito.core.db import get_db
        conn = get_db()

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
