"""Rule-based recommendations engine for session quality improvement.

Analyzes synced runs and produces actionable suggestions. No LLM needed —
pure computation from metadata fields (works for both full-content and
metadata-only users).

Runs server-side after sync or on-demand via MCP tool / API endpoint.
"""

import json
from datetime import datetime, timedelta
from statistics import mean, stdev

from sqlalchemy import and_, case, func, select

from qualito.core.db import (
    evaluations_table,
    runs_table,
    tool_calls_table,
)


def generate_recommendations(
    conn, user_id: int, days: int = 30, workspace: str | None = None
) -> list[dict]:
    """Generate top recommendations for a user based on recent sessions.

    Returns list of recommendation dicts sorted by severity (high first).
    Each has: type, severity, title, detail, metric, action.
    """
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    recommendations = []

    base_conds = [
        runs_table.c.user_id == user_id,
        runs_table.c.started_at >= since,
    ]
    if workspace:
        base_conds.append(runs_table.c.workspace == workspace)

    # --- 1. Bash error rate ---
    tool_stats = conn.execute(
        select(
            tool_calls_table.c.tool_name,
            func.count().label("total"),
            func.sum(
                case((tool_calls_table.c.is_error == True, 1), else_=0)  # noqa: E712
            ).label("errors"),
        )
        .select_from(
            tool_calls_table.join(runs_table, runs_table.c.id == tool_calls_table.c.run_id)
        )
        .where(and_(*base_conds))
        .group_by(tool_calls_table.c.tool_name)
        .having(func.count() >= 10)
    ).mappings().fetchall()

    for ts in tool_stats:
        total = ts["total"] or 0
        errors = ts["errors"] or 0
        if total > 0 and errors / total > 0.25:
            rate = errors / total
            recommendations.append({
                "type": "error_rate",
                "severity": "high",
                "title": f"High {ts['tool_name']} error rate: {rate:.0%}",
                "detail": (
                    f"{errors} of {total} {ts['tool_name']} calls failed "
                    f"in the last {days} days. "
                    f"{'Consider using Read/Grep to check files before running shell commands.' if ts['tool_name'] == 'Bash' else 'Investigate recurring failure patterns.'}"
                ),
                "metric": {"tool": ts["tool_name"], "rate": round(rate, 3), "errors": errors, "total": total},
                "action": (
                    "Add to CLAUDE.md: 'Always use Read to verify file existence before Bash operations'"
                    if ts["tool_name"] == "Bash"
                    else f"Review {ts['tool_name']} error patterns in session detail"
                ),
            })

    # --- 2. Cache utilization ---
    cache_row = conn.execute(
        select(
            func.sum(runs_table.c.cache_read_tokens).label("cache"),
            func.sum(runs_table.c.input_tokens).label("input"),
        )
        .where(and_(*base_conds))
    ).mappings().fetchone()

    cache_total = cache_row["cache"] or 0
    input_total = cache_row["input"] or 0
    if input_total > 0:
        cache_rate = cache_total / input_total
        if cache_rate < 0.20:
            recommendations.append({
                "type": "cache_utilization",
                "severity": "medium",
                "title": f"Low cache hit rate: {cache_rate:.0%}",
                "detail": (
                    f"Only {cache_rate:.0%} of input tokens are cached reads. "
                    f"Adding a CLAUDE.md file with project context to your repos "
                    f"improves prompt caching significantly."
                ),
                "metric": {"cache_rate": round(cache_rate, 3), "cache_tokens": cache_total, "input_tokens": input_total},
                "action": "Add a CLAUDE.md file to each project with codebase context",
            })

    # --- 3. Expensive model for quick sessions ---
    expensive_quick = conn.execute(
        select(func.count().label("cnt"), func.sum(runs_table.c.cost_usd).label("cost"))
        .where(and_(
            *base_conds,
            runs_table.c.model.like("%opus%"),
            runs_table.c.duration_ms < 120_000,  # < 2 minutes
        ))
    ).mappings().fetchone()

    quick_count = expensive_quick["cnt"] or 0
    quick_cost = expensive_quick["cost"] or 0
    if quick_count >= 3:
        # Estimate savings: Haiku is ~60x cheaper than Opus
        estimated_savings = quick_cost * 0.95
        recommendations.append({
            "type": "model_waste",
            "severity": "medium",
            "title": f"{quick_count} Opus sessions under 2 minutes",
            "detail": (
                f"You used Opus for {quick_count} sessions shorter than 2 minutes, "
                f"costing ~${quick_cost:.2f}. Using Haiku or Sonnet for quick lookups "
                f"would save ~${estimated_savings:.2f}."
            ),
            "metric": {"sessions": quick_count, "cost": round(quick_cost, 2), "potential_savings": round(estimated_savings, 2)},
            "action": "Use /model haiku for quick questions and lookups",
        })

    # --- 4. Score declining in a workspace ---
    if not workspace:
        ws_rows = conn.execute(
            select(runs_table.c.workspace.distinct())
            .where(and_(*base_conds))
        ).fetchall()
        workspaces = [r[0] for r in ws_rows if r[0]]
    else:
        workspaces = [workspace]

    r = runs_table
    e = evaluations_table
    join = r.join(e, and_(e.c.run_id == r.c.id, e.c.eval_type == "dqi"))

    for ws in workspaces:
        scores = conn.execute(
            select(e.c.score, r.c.started_at)
            .select_from(join)
            .where(and_(r.c.workspace == ws, r.c.user_id == user_id, r.c.started_at >= since))
            .order_by(r.c.started_at.asc())
        ).mappings().fetchall()

        if len(scores) < 6:
            continue

        mid = len(scores) // 2
        first_half = [s["score"] for s in scores[:mid] if s["score"] is not None]
        second_half = [s["score"] for s in scores[mid:] if s["score"] is not None]

        if first_half and second_half:
            avg_first = mean(first_half)
            avg_second = mean(second_half)
            delta = avg_second - avg_first
            if delta < -0.10:
                recommendations.append({
                    "type": "score_decline",
                    "severity": "high",
                    "title": f"Score declining in {ws}: {avg_first:.2f} → {avg_second:.2f}",
                    "detail": (
                        f"Average session score in '{ws}' dropped from {avg_first:.2f} "
                        f"to {avg_second:.2f} ({delta:+.2f}) over the last {days} days. "
                        f"Review recent sessions for recurring error patterns."
                    ),
                    "metric": {"workspace": ws, "before": round(avg_first, 3), "after": round(avg_second, 3), "delta": round(delta, 3)},
                    "action": f"Review error patterns in '{ws}' sessions",
                })

    # --- 5. Cost spike (z-score > 2 on any recent day) ---
    daily_costs = conn.execute(
        select(
            func.date(runs_table.c.started_at).label("day"),
            func.sum(runs_table.c.cost_usd).label("spend"),
        )
        .where(and_(*base_conds))
        .group_by(func.date(runs_table.c.started_at))
        .order_by(func.date(runs_table.c.started_at))
    ).mappings().fetchall()

    if len(daily_costs) >= 7:
        spends = [d["spend"] for d in daily_costs if d["spend"] is not None]
        if len(spends) >= 7:
            avg_spend = mean(spends)
            std_spend = stdev(spends) if len(spends) >= 2 else 0
            if std_spend > 0:
                latest = daily_costs[-1]
                z = (latest["spend"] - avg_spend) / std_spend
                if z > 2:
                    recommendations.append({
                        "type": "cost_spike",
                        "severity": "medium",
                        "title": f"Cost spike on {latest['day']}: ${latest['spend']:.2f}",
                        "detail": (
                            f"Spending on {latest['day']} was ${latest['spend']:.2f}, "
                            f"which is {z:.1f} standard deviations above your daily average "
                            f"of ${avg_spend:.2f}."
                        ),
                        "metric": {"day": latest["day"], "spend": round(latest["spend"], 2), "avg": round(avg_spend, 2), "z_score": round(z, 2)},
                        "action": "Review sessions from that day to identify cost drivers",
                    })

    # --- 6. Error count increasing week-over-week ---
    two_weeks_ago = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    one_week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    week1_errors = conn.execute(
        select(func.coalesce(func.sum(runs_table.c.error_count), 0).label("errors"))
        .where(and_(
            runs_table.c.user_id == user_id,
            runs_table.c.started_at >= two_weeks_ago,
            runs_table.c.started_at < one_week_ago,
            *(([runs_table.c.workspace == workspace] if workspace else [])),
        ))
    ).mappings().fetchone()["errors"]

    week2_errors = conn.execute(
        select(func.coalesce(func.sum(runs_table.c.error_count), 0).label("errors"))
        .where(and_(
            runs_table.c.user_id == user_id,
            runs_table.c.started_at >= one_week_ago,
            *(([runs_table.c.workspace == workspace] if workspace else [])),
        ))
    ).mappings().fetchone()["errors"]

    if week1_errors > 0 and week2_errors > week1_errors * 1.5:
        pct_increase = ((week2_errors - week1_errors) / week1_errors) * 100
        recommendations.append({
            "type": "error_increase",
            "severity": "medium",
            "title": f"Error count up {pct_increase:.0f}% week-over-week",
            "detail": (
                f"Tool errors increased from {week1_errors} last week to "
                f"{week2_errors} this week ({pct_increase:.0f}% increase)."
            ),
            "metric": {"last_week": week1_errors, "this_week": week2_errors, "pct_increase": round(pct_increase, 1)},
            "action": "Check if a new pattern of errors emerged this week",
        })

    # Sort by severity
    severity_order = {"high": 0, "medium": 1, "low": 2}
    recommendations.sort(key=lambda r: severity_order.get(r["severity"], 3))

    return recommendations
