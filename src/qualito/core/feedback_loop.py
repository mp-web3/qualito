"""Analyze delegation patterns and generate actionable warnings.

Queries workspace/task_type combos from runs + evaluations, identifies
underperforming patterns, and writes WARNINGS.md files per workspace.
"""

import json
from datetime import date
from pathlib import Path

from qualito.core.db import get_db as _get_db

# Map failing auto-eval checks to actionable suggestions
CHECK_SUGGESTIONS = {
    "has_summary": "Ensure task asks for a structured summary",
    "has_output": "Task may be too vague - add specific deliverables",
    "has_findings": "Add 'Report your findings' to the task",
    "cost_reasonable": "Consider breaking into smaller tasks",
    "completed": "Tasks failing to complete - check timeout or scope",
    "tool_calls_made": "Delegate may not be using available tools",
    "chains_recorded": "Add explicit si_reason instruction to the task",
    "within_timeout": "Tasks timing out - reduce scope or increase timeout",
}


def get_flagged_combos(conn, threshold: float, min_runs: int,
                       workspace: str | None = None) -> list[dict]:
    """Find workspace/task_type combos with avg DQI below threshold."""
    params: list = []
    ws_filter = ""
    if workspace:
        ws_filter = "AND r.workspace = ?"
        params.append(workspace)
    params.extend([min_runs, threshold])

    rows = conn.execute(f"""
        SELECT r.workspace, r.task_type, COUNT(*) as cnt,
               AVG(e.score) as avg_dqi,
               SUM(CASE WHEN e.score < 0.70 THEN 1 ELSE 0 END) as low_count
        FROM runs r
        JOIN evaluations e ON e.run_id = r.id AND e.eval_type = 'dqi'
        WHERE r.status = 'completed' AND r.source = 'delegation'
              {ws_filter}
        GROUP BY r.workspace, r.task_type
        HAVING COUNT(*) >= ? AND AVG(e.score) < ?
    """, params).fetchall()
    return [dict(r) for r in rows]


def analyze_failure_patterns(conn, workspace: str, task_type: str) -> dict:
    """Analyze which auto-eval checks fail most often for a combo."""
    rows = conn.execute("""
        SELECT e.checks
        FROM evaluations e
        JOIN runs r ON r.id = e.run_id
        WHERE r.workspace = ? AND r.task_type = ? AND e.eval_type = 'auto'
              AND r.status = 'completed' AND r.source = 'delegation'
    """, (workspace, task_type)).fetchall()

    if not rows:
        return {"top_check": None, "top_pct": 0, "total_evals": 0}

    check_fail_counts: dict[str, int] = {}
    total_evals = 0

    for row in rows:
        checks_json = row["checks"]
        if not checks_json:
            continue
        try:
            checks = json.loads(checks_json)
        except (json.JSONDecodeError, TypeError):
            continue
        total_evals += 1
        for check_name, check_data in checks.items():
            if isinstance(check_data, dict) and not check_data.get("passed", True):
                check_fail_counts[check_name] = check_fail_counts.get(check_name, 0) + 1

    if not check_fail_counts or total_evals == 0:
        return {"top_check": None, "top_pct": 0, "total_evals": total_evals}

    top_check = max(check_fail_counts, key=check_fail_counts.get)
    top_pct = (check_fail_counts[top_check] / total_evals) * 100

    return {
        "top_check": top_check,
        "top_pct": top_pct,
        "total_evals": total_evals,
        "all_failures": check_fail_counts,
    }


def analyze_cost_gap(conn, workspace: str, task_type: str) -> dict:
    """Compare avg cost of low-DQI runs vs high-DQI runs."""
    row = conn.execute("""
        SELECT
            AVG(CASE WHEN e.score < 0.70 THEN r.cost_usd END) as avg_cost_low,
            AVG(CASE WHEN e.score >= 0.70 THEN r.cost_usd END) as avg_cost_high
        FROM runs r
        JOIN evaluations e ON e.run_id = r.id AND e.eval_type = 'dqi'
        WHERE r.workspace = ? AND r.task_type = ?
              AND r.status = 'completed' AND r.source = 'delegation'
    """, (workspace, task_type)).fetchone()

    return {
        "avg_cost_low": row["avg_cost_low"],
        "avg_cost_high": row["avg_cost_high"],
    }


def generate_warning(combo: dict, patterns: dict, costs: dict) -> str:
    """Generate a single warning block for a flagged combo."""
    task_type = combo["task_type"]
    cnt = combo["cnt"]
    low_count = combo["low_count"]
    avg_dqi = combo["avg_dqi"]

    lines = [
        f"### {task_type} (avg DQI: {avg_dqi:.2f})",
        "",
        f"{low_count} of {cnt} runs scored below 0.70.",
    ]

    # Top failing check + suggestion
    top_check = patterns.get("top_check")
    if top_check:
        top_pct = patterns.get("top_pct", 0)
        suggestion = CHECK_SUGGESTIONS.get(top_check, f"Investigate '{top_check}' failures")
        lines.append(f"Most common issue: `{top_check}` fails {top_pct:.0f}% of the time.")
        lines.append(f"Suggestion: {suggestion}.")

    # Cost comparison
    avg_low = costs.get("avg_cost_low")
    avg_high = costs.get("avg_cost_high")
    if avg_low is not None and avg_high is not None:
        lines.append(f"Avg cost: ${avg_low:.2f} (low-DQI runs) vs ${avg_high:.2f} (high-DQI runs).")
    elif avg_low is not None:
        lines.append(f"Avg cost of low-DQI runs: ${avg_low:.2f}.")

    return "\n".join(lines)


def run_feedback_loop(threshold: float = 0.75, min_runs: int = 5,
                      dry_run: bool = False, workspace: str | None = None,
                      conn=None, output_dir: Path | None = None):
    """Main entry point: analyze patterns and write WARNINGS.md files.

    Args:
        threshold: DQI threshold for flagging combos.
        min_runs: Minimum runs to evaluate a combo.
        dry_run: If True, print warnings without writing files.
        workspace: Optional workspace filter.
        conn: Optional DB connection. If None, opens and closes its own.
        output_dir: Directory where workspace subdirs with WARNINGS.md are written.
                    Defaults to .qualito/warnings/ in cwd.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = _get_db()

    combos = get_flagged_combos(conn, threshold, min_runs, workspace)

    if not combos:
        print("No underperforming workspace/task_type combos found.")
        if owns_conn:
            conn.close()
        return

    # Group warnings by workspace
    warnings_by_ws: dict[str, list[str]] = {}
    for combo in combos:
        ws = combo["workspace"]
        patterns = analyze_failure_patterns(conn, ws, combo["task_type"])
        costs = analyze_cost_gap(conn, ws, combo["task_type"])
        warning = generate_warning(combo, patterns, costs)

        if ws not in warnings_by_ws:
            warnings_by_ws[ws] = []
        warnings_by_ws[ws].append(warning)

    if owns_conn:
        conn.close()

    # Resolve output directory
    if output_dir is None:
        output_dir = Path.cwd() / ".dqi" / "warnings"

    # Write or print warnings
    today = date.today().isoformat()
    for ws, warnings in warnings_by_ws.items():
        header = (
            f"# Auto-Generated Warnings\n\n"
            f"Generated by dqi feedback_loop on {today}. Do not edit manually.\n"
        )
        content = header + "\n" + "\n\n".join(warnings) + "\n"

        if dry_run:
            print(f"\n--- {ws}/WARNINGS.md (dry run) ---")
            print(content)
        else:
            ws_dir = output_dir / ws
            ws_dir.mkdir(parents=True, exist_ok=True)
            warnings_path = ws_dir / "WARNINGS.md"
            warnings_path.write_text(content)
            print(f"Wrote {warnings_path}")

    # Summary
    total_warnings = sum(len(w) for w in warnings_by_ws.values())
    total_workspaces = len(warnings_by_ws)
    print(f"\nSummary: {total_warnings} warning(s) across {total_workspaces} workspace(s).")
