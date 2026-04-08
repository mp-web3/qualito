"""Qualito MCP server — quality metrics for AI-assisted development.

8 tools: dqi_score, dqi_cost, dqi_patterns, dqi_warnings,
dqi_templates, dqi_incidents, dqi_slo, qualito_setup.
stdio transport. Never print() to stdout. All tools return str (JSON).
"""

import json
import logging
import sys
from datetime import datetime, timedelta

from mcp.server.fastmcp import FastMCP

# Configure logging to stderr — stdout is reserved for JSON-RPC
logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(levelname)s: %(message)s")
logger = logging.getLogger("qualito-mcp")

mcp = FastMCP("qualito")


def _get_db():
    """Get a short-lived DB connection using DQI's standard resolution."""
    from qualito.core.db import get_db
    return get_db()


def _since_date(days: int) -> str:
    """Return ISO date string for N days ago."""
    return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")


def _infer_task_type(description: str) -> str:
    """Infer task type from a description using keyword matching."""
    desc = description.lower()
    keywords = {
        "test": ["test", "spec", "coverage", "pytest", "jest"],
        "pr_review": ["review", "pr ", "pull request", "code review"],
        "code": ["implement", "build", "create", "add feature", "write code", "develop"],
        "refactor": ["refactor", "clean up", "reorganize", "restructure", "simplify"],
        "research": ["research", "investigate", "explore", "analyze", "audit"],
        "jira": ["jira", "ticket", "issue", "backlog"],
        "confluence": ["confluence", "wiki", "documentation", "docs"],
        "slack": ["slack", "message", "draft", "notify"],
    }
    for task_type, kws in keywords.items():
        if any(kw in desc for kw in kws):
            return task_type
    return "other"


# ---------------------------------------------------------------------------
# Tool 1: dqi_score
# ---------------------------------------------------------------------------

@mcp.tool()
def dqi_score(workspace: str = "", days: int = 30) -> str:
    """Get DQI score summary: average, trend, component breakdown, tier distribution.

    Use this to understand overall delegation quality for a workspace over time.

    Args:
        workspace: Filter by workspace name. Empty string = all workspaces.
        days: Number of days to look back (default 30).
    """
    conn = _get_db()
    try:
        since = _since_date(days)
        ws_filter = "AND r.workspace = ?" if workspace else ""
        params: list = [since]
        if workspace:
            params.append(workspace)

        # Average DQI from evaluations
        avg_row = conn.execute(f"""
            SELECT AVG(e.score) as avg_dqi, COUNT(e.id) as scored_runs
            FROM evaluations e
            JOIN runs r ON r.id = e.run_id
            WHERE e.eval_type = 'dqi' AND r.started_at >= ? {ws_filter}
        """, params).fetchone()

        # Last 10 runs for trend
        trend_rows = conn.execute(f"""
            SELECT e.score, r.started_at
            FROM evaluations e
            JOIN runs r ON r.id = e.run_id
            WHERE e.eval_type = 'dqi' AND r.started_at >= ? {ws_filter}
            ORDER BY r.started_at DESC
            LIMIT 10
        """, params).fetchall()

        # Component breakdown (avg of categories JSON)
        cat_rows = conn.execute(f"""
            SELECT e.categories
            FROM evaluations e
            JOIN runs r ON r.id = e.run_id
            WHERE e.eval_type = 'dqi' AND r.started_at >= ? {ws_filter}
                  AND e.categories IS NOT NULL
        """, params).fetchall()

        components = {"completion": [], "quality": [], "efficiency": [], "cost_score": []}
        tier_counts = {}
        for row in cat_rows:
            try:
                raw = row["categories"]
                cats = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                continue
            for key in components:
                if key in cats:
                    components[key].append(cats[key])
            tier_label = cats.get("tier_label", "unknown")
            tier_counts[tier_label] = tier_counts.get(tier_label, 0) + 1

        component_avgs = {}
        for key, vals in components.items():
            component_avgs[key] = round(sum(vals) / len(vals), 4) if vals else None

        trend = [{"dqi": round(r["score"], 4), "date": r["started_at"]} for r in trend_rows]

        return json.dumps({
            "avg_dqi": round(avg_row["avg_dqi"], 4) if avg_row["avg_dqi"] else None,
            "scored_runs": avg_row["scored_runs"],
            "days": days,
            "workspace": workspace or "all",
            "trend": trend,
            "components": component_avgs,
            "tier_distribution": tier_counts,
        }, indent=2)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tool 2: dqi_cost
# ---------------------------------------------------------------------------

@mcp.tool()
def dqi_cost(workspace: str = "", days: int = 30) -> str:
    """Get cost analysis: total spend, average per run, daily trend, waste estimate.

    Runs with DQI < 0.5 are classified as waste. Use this to track delegation costs.

    Args:
        workspace: Filter by workspace name. Empty string = all workspaces.
        days: Number of days to look back (default 30).
    """
    conn = _get_db()
    try:
        since = _since_date(days)
        ws_filter = "AND workspace = ?" if workspace else ""
        params: list = [since]
        if workspace:
            params.append(workspace)

        # Overall cost stats
        stats = conn.execute(f"""
            SELECT COUNT(*) as total_runs,
                   COALESCE(SUM(cost_usd), 0) as total_spend,
                   AVG(cost_usd) as avg_per_run
            FROM runs
            WHERE started_at >= ? {ws_filter} AND source = 'delegation'
        """, params).fetchone()

        # Daily trend (last N days)
        daily = conn.execute(f"""
            SELECT DATE(started_at) as day,
                   COUNT(*) as runs,
                   COALESCE(SUM(cost_usd), 0) as spend
            FROM runs
            WHERE started_at >= ? {ws_filter} AND source = 'delegation'
            GROUP BY DATE(started_at)
            ORDER BY day DESC
        """, params).fetchall()

        # Waste: runs with DQI < 0.5
        waste_params: list = [since]
        ws_filter_r = "AND r.workspace = ?" if workspace else ""
        if workspace:
            waste_params.append(workspace)

        waste = conn.execute(f"""
            SELECT COUNT(*) as waste_runs,
                   COALESCE(SUM(r.cost_usd), 0) as waste_cost
            FROM runs r
            JOIN evaluations e ON e.run_id = r.id AND e.eval_type = 'dqi'
            WHERE r.started_at >= ? {ws_filter_r}
                  AND r.source = 'delegation' AND e.score < 0.5
        """, waste_params).fetchone()

        return json.dumps({
            "total_runs": stats["total_runs"],
            "total_spend": round(stats["total_spend"], 2),
            "avg_per_run": round(stats["avg_per_run"], 2) if stats["avg_per_run"] else None,
            "days": days,
            "workspace": workspace or "all",
            "daily_trend": [
                {"day": r["day"], "runs": r["runs"], "spend": round(r["spend"], 2)}
                for r in daily
            ],
            "waste": {
                "runs": waste["waste_runs"],
                "cost": round(waste["waste_cost"], 2),
                "pct_of_total": round(
                    waste["waste_cost"] / stats["total_spend"] * 100, 1
                ) if stats["total_spend"] > 0 else 0,
            },
        }, indent=2)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tool 3: dqi_patterns
# ---------------------------------------------------------------------------

@mcp.tool()
def dqi_patterns(min_count: int = 3) -> str:
    """Detect repeated task patterns with classification and recommendations.

    Groups runs by normalized task text and identifies patterns that should
    become scripts, skills, or be reviewed. Use to find automation opportunities.

    Args:
        min_count: Minimum occurrences to include (default 3).
    """
    from qualito.core.pattern_detector import detect_patterns

    results = detect_patterns(min_count=min_count)
    return json.dumps({
        "pattern_count": len(results),
        "patterns": results,
    }, indent=2)


# ---------------------------------------------------------------------------
# Tool 4: dqi_warnings
# ---------------------------------------------------------------------------

@mcp.tool()
def dqi_warnings(workspace: str = "") -> str:
    """Get warnings for underperforming workspace/task_type combinations.

    Shows combos with low average DQI, their most common failing check,
    and actionable suggestions to improve. Use to identify delegation weak spots.

    Args:
        workspace: Filter by workspace name. Empty string = all workspaces.
    """
    from qualito.core.feedback_loop import (
        CHECK_SUGGESTIONS,
        analyze_cost_gap,
        analyze_failure_patterns,
        get_flagged_combos,
    )

    conn = _get_db()
    try:
        combos = get_flagged_combos(
            conn, threshold=0.75, min_runs=3,
            workspace=workspace or None,
        )

        warnings = []
        for combo in combos:
            ws = combo["workspace"]
            task_type = combo["task_type"]
            patterns = analyze_failure_patterns(conn, ws, task_type)
            costs = analyze_cost_gap(conn, ws, task_type)

            top_check = patterns.get("top_check")
            suggestion = CHECK_SUGGESTIONS.get(
                top_check, f"Investigate '{top_check}' failures"
            ) if top_check else None

            warnings.append({
                "workspace": ws,
                "task_type": task_type,
                "avg_dqi": round(combo["avg_dqi"], 3),
                "run_count": combo["cnt"],
                "low_dqi_runs": combo["low_count"],
                "top_failing_check": top_check,
                "top_fail_pct": round(patterns.get("top_pct", 0), 1),
                "suggestion": suggestion,
                "avg_cost_low_dqi": (
                    round(costs["avg_cost_low"], 2) if costs["avg_cost_low"] else None
                ),
                "avg_cost_high_dqi": (
                    round(costs["avg_cost_high"], 2) if costs["avg_cost_high"] else None
                ),
            })

        return json.dumps({
            "warning_count": len(warnings),
            "warnings": warnings,
        }, indent=2)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tool 5: dqi_templates
# ---------------------------------------------------------------------------

@mcp.tool()
def dqi_templates(task_description: str) -> str:
    """Recommend a delegation template based on a task description.

    Infers the task type from keywords and suggests a matching template
    with best practices. Use before delegating to get a better prompt structure.

    Args:
        task_description: Description of the task you want to delegate.
    """
    task_type = _infer_task_type(task_description)

    # Built-in template recommendations
    templates = {
        "code": {
            "name": "code-implementation",
            "description": "Standard code implementation task",
            "structure": [
                "## Task",
                "<clear description of what to build>",
                "",
                "## Context",
                "- Read <relevant files> first",
                "- Follow existing patterns in <module>",
                "",
                "## Requirements",
                "- <specific acceptance criteria>",
                "",
                "## Verification",
                "- Run tests: <command>",
                "- Verify: <what to check>",
            ],
        },
        "refactor": {
            "name": "refactoring",
            "description": "Code refactoring task",
            "structure": [
                "## Task",
                "Refactor <target> to <goal>",
                "",
                "## Constraints",
                "- No behavior changes",
                "- Keep existing tests passing",
                "- Follow <convention>",
                "",
                "## Verification",
                "- All tests pass",
                "- No regressions",
            ],
        },
        "test": {
            "name": "test-writing",
            "description": "Write tests for existing code",
            "structure": [
                "## Task",
                "Write tests for <module>",
                "",
                "## Coverage",
                "- <happy path scenarios>",
                "- <edge cases>",
                "- <error cases>",
                "",
                "## Stack",
                "- Use <test framework>",
                "- Follow patterns in <existing test file>",
            ],
        },
        "research": {
            "name": "research-investigation",
            "description": "Research or investigation task",
            "structure": [
                "## Task",
                "Investigate <topic>",
                "",
                "## Questions",
                "1. <specific question>",
                "2. <specific question>",
                "",
                "## Output",
                "- Report findings in structured format",
                "- Include evidence and sources",
                "- Record reasoning via si_reason",
            ],
        },
        "pr_review": {
            "name": "pr-review",
            "description": "Pull request review task",
            "structure": [
                "## Task",
                "Review PR #<number> on <repo>",
                "",
                "## Focus Areas",
                "- Correctness",
                "- Code quality",
                "- Security concerns",
                "- Test coverage",
                "",
                "## Output",
                "- Post review comments on the PR",
            ],
        },
    }

    template = templates.get(task_type, {
        "name": "generic",
        "description": "Generic delegation task",
        "structure": [
            "## Task",
            "<clear description>",
            "",
            "## Context",
            "- Read relevant files first",
            "",
            "## Requirements",
            "- <specific deliverables>",
            "",
            "## Verification",
            "- <how to verify completion>",
        ],
    })

    return json.dumps({
        "inferred_task_type": task_type,
        "template_name": template["name"],
        "template_description": template["description"],
        "template_content": "\n".join(template["structure"]),
    }, indent=2)


# ---------------------------------------------------------------------------
# Tool 6: dqi_incidents
# ---------------------------------------------------------------------------

@mcp.tool()
def dqi_incidents(workspace: str = "", status: str = "active") -> str:
    """Get DQI incidents — quality regressions and anomalies detected by monitoring.

    Returns incidents filtered by status. 'active' shows non-resolved incidents
    (detected, confirmed, investigating). Use to track ongoing quality issues.

    Args:
        workspace: Filter by workspace name. Empty string = all workspaces.
        status: Filter by status — 'active' (non-resolved), 'resolved', or 'all' (default 'active').
    """
    conn = _get_db()
    try:
        where_parts = []
        params: list = []

        if status == "active":
            where_parts.append("status NOT IN ('resolved', 'false_positive')")
        elif status == "resolved":
            where_parts.append("status = 'resolved'")
        # 'all' — no status filter

        if workspace:
            where_parts.append("workspace = ?")
            params.append(workspace)

        where = "WHERE " + " AND ".join(where_parts) if where_parts else ""

        rows = conn.execute(f"""
            SELECT id, incident_key, category, severity, status, workspace,
                   task_type, title, description, detection_method,
                   trigger_metric, trigger_value, baseline_value,
                   total_affected_runs, cost_impact_usd,
                   created_at, confirmed_at, resolved_at
            FROM incidents
            {where}
            ORDER BY
                CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                     WHEN 'medium' THEN 2 ELSE 3 END,
                created_at DESC
        """, params).fetchall()

        incidents = [dict(r) for r in rows]

        return json.dumps({
            "incident_count": len(incidents),
            "filter": {"workspace": workspace or "all", "status": status},
            "incidents": incidents,
        }, indent=2)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tool 7: dqi_slo
# ---------------------------------------------------------------------------

@mcp.tool()
def dqi_slo(workspace: str = "") -> str:
    """Check SLO compliance: quality, availability, and cost targets over last 30 days.

    Compares current performance against configured thresholds:
    - Quality: % of runs with DQI >= 0.60
    - Availability: completion rate (completed / total)
    - Cost: % of runs under $3.00

    Args:
        workspace: Filter by workspace name. Empty string = all workspaces.
    """
    conn = _get_db()
    try:
        since = _since_date(30)
        ws_filter = "AND workspace = ?" if workspace else ""
        ws_filter_r = "AND r.workspace = ?" if workspace else ""
        params: list = [since]
        if workspace:
            params.append(workspace)

        # Total runs
        total_row = conn.execute(f"""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                   SUM(CASE WHEN cost_usd < 3.00 THEN 1 ELSE 0 END) as under_cost
            FROM runs
            WHERE started_at >= ? {ws_filter} AND source = 'delegation'
        """, params).fetchone()

        total = total_row["total"] or 0
        completed = total_row["completed"] or 0
        under_cost = total_row["under_cost"] or 0

        # Quality: runs with DQI >= 0.60
        quality_params: list = [since]
        if workspace:
            quality_params.append(workspace)

        quality_row = conn.execute(f"""
            SELECT COUNT(*) as total_scored,
                   SUM(CASE WHEN e.score >= 0.60 THEN 1 ELSE 0 END) as quality_ok
            FROM evaluations e
            JOIN runs r ON r.id = e.run_id
            WHERE e.eval_type = 'dqi' AND r.started_at >= ? {ws_filter_r}
                  AND r.source = 'delegation'
        """, quality_params).fetchone()

        total_scored = quality_row["total_scored"] or 0
        quality_ok = quality_row["quality_ok"] or 0

        # Compute percentages
        quality_pct = (quality_ok / total_scored * 100) if total_scored > 0 else None
        availability_pct = (completed / total * 100) if total > 0 else None
        cost_pct = (under_cost / total * 100) if total > 0 else None

        # Load SLO targets from config (defaults match DqiConfig)
        slo_quality = 60.0  # % runs >= 0.60
        slo_availability = 95.0  # % completion rate
        slo_cost = 80.0  # % runs under $3.00

        try:
            from qualito.config import load_config
            cfg = load_config()
            if cfg.slo_quality <= 1.0:
                slo_quality = cfg.slo_quality * 100
            else:
                slo_quality = cfg.slo_quality
            if cfg.slo_availability <= 1.0:
                slo_availability = cfg.slo_availability * 100
            else:
                slo_availability = cfg.slo_availability
            # slo_cost in config is the dollar threshold, not a percentage
            # We use 80% as default target for "% of runs under threshold"
        except Exception:
            pass

        slos = {
            "quality": {
                "current": round(quality_pct, 1) if quality_pct is not None else None,
                "target": slo_quality,
                "met": quality_pct >= slo_quality if quality_pct is not None else None,
                "description": f"% of runs with DQI >= 0.60 (target: {slo_quality}%)",
                "scored_runs": total_scored,
            },
            "availability": {
                "current": round(availability_pct, 1) if availability_pct is not None else None,
                "target": slo_availability,
                "met": (
                    availability_pct >= slo_availability
                    if availability_pct is not None else None
                ),
                "description": f"Completion rate (target: {slo_availability}%)",
                "total_runs": total,
                "completed_runs": completed,
            },
            "cost": {
                "current": round(cost_pct, 1) if cost_pct is not None else None,
                "target": slo_cost,
                "met": cost_pct >= slo_cost if cost_pct is not None else None,
                "description": f"% of runs under $3.00 (target: {slo_cost}%)",
                "total_runs": total,
                "under_threshold": under_cost,
            },
        }

        all_met = all(
            s["met"] for s in slos.values() if s["met"] is not None
        )

        return json.dumps({
            "workspace": workspace or "all",
            "period_days": 30,
            "all_slos_met": all_met if any(s["met"] is not None for s in slos.values()) else None,
            "slos": slos,
        }, indent=2)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tool 8: qualito_setup
# ---------------------------------------------------------------------------

@mcp.tool()
def qualito_setup() -> str:
    """Scan for Claude Code sessions and return setup overview.

    Returns one of three states:
    - "configured": ~/.qualito/ exists with data — shows workspace stats.
    - "not_configured": No data yet but Claude Code sessions found — shows
      what can be imported and suggests running `uvx qualito setup`.
    - "no_sessions": No Claude Code sessions found at all.

    This tool is read-only and fast (no imports, no scoring).
    """
    from pathlib import Path

    global_dir = Path.home() / ".qualito"
    claude_projects_dir = Path.home() / ".claude" / "projects"

    # Case A: Already configured — read stats from DB
    if global_dir.exists() and (global_dir / "qualito.db").exists():
        conn = _get_db()
        try:
            # Per-workspace stats
            ws_rows = conn.execute("""
                SELECT r.workspace,
                       COUNT(r.id) as run_count,
                       AVG(e.score) as avg_dqi,
                       COALESCE(SUM(r.cost_usd), 0) as total_cost
                FROM runs r
                LEFT JOIN evaluations e ON e.run_id = r.id AND e.eval_type = 'dqi'
                GROUP BY r.workspace
                ORDER BY run_count DESC
            """).fetchall()

            workspaces = []
            total_runs = 0
            total_cost = 0.0
            dqi_sum = 0.0
            dqi_count = 0

            for row in ws_rows:
                run_count = row["run_count"]
                avg_dqi = round(row["avg_dqi"], 2) if row["avg_dqi"] is not None else None
                ws_cost = round(row["total_cost"], 2)

                workspaces.append({
                    "name": row["workspace"],
                    "run_count": run_count,
                    "avg_dqi": avg_dqi,
                    "total_cost": ws_cost,
                })
                total_runs += run_count
                total_cost += ws_cost
                if avg_dqi is not None:
                    dqi_sum += avg_dqi * run_count
                    dqi_count += run_count

            overall_avg_dqi = round(dqi_sum / dqi_count, 2) if dqi_count > 0 else None

            return json.dumps({
                "status": "configured",
                "global_dir": str(global_dir),
                "workspaces": workspaces,
                "total_runs": total_runs,
                "overall_avg_dqi": overall_avg_dqi,
                "total_cost": round(total_cost, 2),
            }, indent=2)
        finally:
            conn.close()

    # Case B/C: Not configured — check for Claude Code sessions
    if not claude_projects_dir.exists() or not any(claude_projects_dir.iterdir()):
        return json.dumps({
            "status": "no_sessions",
            "message": "No Claude Code sessions found. Use Claude Code to generate session data first.",
        }, indent=2)

    # Case B: Sessions exist but not yet imported
    from qualito.importer import discover_all_projects

    projects = discover_all_projects(claude_projects_dir)
    total_sessions = sum(p["session_count"] for p in projects)

    discovered = [
        {
            "name": p["name"],
            "folder": p["folder"],
            "session_count": p["session_count"],
        }
        for p in projects
        if p["session_count"] > 0
    ]

    return json.dumps({
        "status": "not_configured",
        "discovered_projects": discovered,
        "total_sessions": total_sessions,
        "suggestion": "Run `uvx qualito setup` to import and score these sessions",
    }, indent=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    """Start the Qualito MCP server (stdio transport)."""
    mcp.run()


if __name__ == "__main__":
    main()
