"""Measurement framework for delegation quality.

Provides baseline snapshots, Bayesian before/after comparison,
CUSUM control chart monitoring, and status reporting.
"""

import json
import random
from datetime import datetime, timedelta, timezone
from statistics import mean, median

from sqlalchemy import and_, select

from qualito.core.db import (
    baselines_table,
    evaluations_table,
    get_sa_connection,
    runs_table,
    system_changes_table,
)


def _beta_samples(alpha: float, beta: float, n: int = 10000) -> list[float]:
    """Generate Beta distribution samples using Gamma variates (no scipy needed)."""
    samples = []
    a = max(alpha, 0.01)
    b = max(beta, 0.01)
    for _ in range(n):
        x = random.gammavariate(a, 1)
        y = random.gammavariate(b, 1)
        samples.append(x / (x + y) if (x + y) > 0 else 0.5)
    return samples


def _percentile(data: list[float], p: float) -> float:
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (k - f) * (s[c] - s[f])


# ---------------------------------------------------------------------------
# SA Core bridge functions (Phase 2)
# ---------------------------------------------------------------------------


def take_baseline(name: str, description: str = "", days: int = 30, conn=None):
    """Snapshot current DQI metrics as a named baseline.

    Args:
        name: Name for this baseline.
        description: Optional description.
        days: Number of days to look back.
        conn: Optional DB connection. If None, opens and closes its own.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = get_sa_connection()

    window_end = datetime.now(timezone.utc).isoformat()
    window_start = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    rows = conn.execute(
        select(
            evaluations_table.c.score,
            evaluations_table.c.categories,
            runs_table.c.status,
            runs_table.c.cost_usd,
            runs_table.c.duration_ms,
            runs_table.c.workspace,
            runs_table.c.task_type,
        )
        .select_from(
            evaluations_table.join(runs_table, runs_table.c.id == evaluations_table.c.run_id)
        )
        .where(
            and_(
                evaluations_table.c.eval_type == "dqi",
                runs_table.c.started_at >= window_start,
                runs_table.c.started_at <= window_end,
            )
        )
    ).mappings().fetchall()

    if not rows:
        print(f"No DQI data in the last {days} days.")
        if owns_conn:
            conn.close()
        return

    scores = [r["score"] for r in rows]
    costs = [r["cost_usd"] for r in rows if r["cost_usd"] is not None]
    completed = sum(1 for r in rows if r["status"] == "completed")

    # Parse categories for component averages
    components = {"completion": [], "quality": [], "efficiency": [], "cost_score": []}
    for r in rows:
        cats = r["categories"]
        if cats:
            if isinstance(cats, str):
                cats = json.loads(cats)
            for k in components:
                if k in cats:
                    components[k].append(cats[k])

    # Group by workspace
    by_ws = {}
    for r in rows:
        ws = r["workspace"] or "unknown"
        by_ws.setdefault(ws, []).append(r["score"])

    # Group by task type
    by_tt = {}
    for r in rows:
        tt = r["task_type"] or "unknown"
        by_tt.setdefault(tt, []).append(r["score"])

    metrics = {
        "avg_dqi": round(mean(scores), 4),
        "median_dqi": round(median(scores), 4),
        "min_dqi": round(min(scores), 4),
        "max_dqi": round(max(scores), 4),
        "completion_rate": round(completed / len(rows), 4),
        "avg_cost": round(mean(costs), 4) if costs else 0,
        "run_count": len(rows),
        "avg_completion": round(mean(components["completion"]), 4) if components["completion"] else 0,
        "avg_quality": round(mean(components["quality"]), 4) if components["quality"] else 0,
        "avg_efficiency": round(mean(components["efficiency"]), 4) if components["efficiency"] else 0,
        "avg_cost_score": round(mean(components["cost_score"]), 4) if components["cost_score"] else 0,
        "by_workspace": {k: round(mean(v), 4) for k, v in by_ws.items()},
        "by_task_type": {k: round(mean(v), 4) for k, v in by_tt.items()},
    }

    conn.execute(
        baselines_table.insert().values(
            name=name,
            description=description,
            window_start=window_start,
            window_end=window_end,
            run_count=len(rows),
            metrics=json.dumps(metrics),
        )
    )
    conn.commit()

    if owns_conn:
        conn.close()

    print(f"\n=== Baseline: {name} ===")
    print(f"Window: {days} days ({len(rows)} runs)")
    print(f"DQI:  avg={metrics['avg_dqi']:.3f}  median={metrics['median_dqi']:.3f}  "
          f"min={metrics['min_dqi']:.3f}  max={metrics['max_dqi']:.3f}")
    print(f"Components:  completion={metrics['avg_completion']:.3f}  quality={metrics['avg_quality']:.3f}  "
          f"efficiency={metrics['avg_efficiency']:.3f}  cost={metrics['avg_cost_score']:.3f}")
    print(f"Completion rate: {metrics['completion_rate']:.1%}  Avg cost: ${metrics['avg_cost']:.2f}")
    print(f"\nBy workspace:")
    for ws, avg in sorted(metrics["by_workspace"].items(), key=lambda x: -x[1]):
        print(f"  {ws:25} {avg:.3f}")
    print(f"\nBy task type:")
    for tt, avg in sorted(metrics["by_task_type"].items(), key=lambda x: -x[1]):
        print(f"  {tt:25} {avg:.3f}")


def register_change(change_name: str, description: str = "",
                    baseline_name: str = "", conn=None):
    """Register a system change for before/after tracking.

    Args:
        change_name: Name of the change.
        description: Optional description.
        baseline_name: Specific baseline to compare against. If empty, uses latest.
        conn: Optional DB connection. If None, opens and closes its own.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = get_sa_connection()

    baseline = None
    if baseline_name:
        baseline = conn.execute(
            select(baselines_table)
            .where(baselines_table.c.name == baseline_name)
            .order_by(baselines_table.c.created_at.desc())
            .limit(1)
        ).mappings().fetchone()
    else:
        baseline = conn.execute(
            select(baselines_table)
            .order_by(baselines_table.c.created_at.desc())
            .limit(1)
        ).mappings().fetchone()

    if not baseline:
        print("No baseline found. Take a baseline first.")
        if owns_conn:
            conn.close()
        return

    conn.execute(
        system_changes_table.insert().values(
            change_name=change_name,
            description=description,
            baseline_id=baseline["id"],
            implemented_at=datetime.now(timezone.utc).isoformat(),
            before_metrics=baseline["metrics"],
        )
    )
    conn.commit()

    if owns_conn:
        conn.close()
    print(f"Registered change '{change_name}' against baseline '{baseline['name']}'")


def evaluate_change(change_name: str, conn=None):
    """Bayesian before/after comparison for a registered change.

    Args:
        change_name: Name of the registered change to evaluate.
        conn: Optional DB connection. If None, opens and closes its own.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = get_sa_connection()

    change = conn.execute(
        select(system_changes_table)
        .where(system_changes_table.c.change_name == change_name)
    ).mappings().fetchone()
    if not change:
        print(f"Change '{change_name}' not found.")
        if owns_conn:
            conn.close()
        return

    before_metrics = json.loads(change["before_metrics"])

    # Get DQI scores after the change
    after_rows = conn.execute(
        select(evaluations_table.c.score)
        .select_from(
            evaluations_table.join(runs_table, runs_table.c.id == evaluations_table.c.run_id)
        )
        .where(
            and_(
                evaluations_table.c.eval_type == "dqi",
                runs_table.c.started_at >= change["implemented_at"],
            )
        )
    ).mappings().fetchall()

    if len(after_rows) < 5:
        print(f"Only {len(after_rows)} runs since change. Need at least 5 for meaningful comparison.")
        if owns_conn:
            conn.close()
        return

    after_scores = [r["score"] for r in after_rows]
    before_mean = before_metrics["avg_dqi"]
    before_n = before_metrics["run_count"]
    after_mean = mean(after_scores)
    after_n = len(after_scores)

    # Bayesian comparison using Beta distributions
    before_alpha = before_n * before_mean + 1
    before_beta = before_n * (1 - before_mean) + 1
    after_alpha = after_n * after_mean + 1
    after_beta = after_n * (1 - after_mean) + 1

    before_samples = _beta_samples(before_alpha, before_beta)
    after_samples = _beta_samples(after_alpha, after_beta)

    p_improvement = sum(1 for a, b in zip(after_samples, before_samples) if a > b) / len(after_samples)
    diff_samples = [a - b for a, b in zip(after_samples, before_samples)]
    effect_size = after_mean - before_mean
    hdi_low = _percentile(diff_samples, 2.5)
    hdi_high = _percentile(diff_samples, 97.5)

    # Decision
    if p_improvement > 0.95 and hdi_low > 0:
        verdict = "IMPROVED"
    elif p_improvement < 0.50:
        verdict = "DEGRADED"
    else:
        verdict = "INCONCLUSIVE"

    conn.execute(
        system_changes_table.update()
        .where(system_changes_table.c.change_name == change_name)
        .values(
            status=verdict.lower(),
            after_metrics=json.dumps({"avg_dqi": round(after_mean, 4), "run_count": after_n}),
            p_improvement=round(p_improvement, 4),
            effect_size=round(effect_size, 4),
        )
    )
    conn.commit()

    if owns_conn:
        conn.close()

    print(f"\n=== Change Evaluation: {change_name} ===")
    print(f"Before: DQI={before_mean:.3f} (n={before_n})")
    print(f"After:  DQI={after_mean:.3f} (n={after_n})")
    print(f"P(improvement): {p_improvement:.3f}")
    print(f"Effect size: {effect_size:+.4f}")
    print(f"95% HDI: [{hdi_low:+.4f}, {hdi_high:+.4f}]")
    print(f"Verdict: {verdict}")


def monitor(conn=None):
    """CUSUM control chart + EWMA for ongoing quality monitoring.

    Args:
        conn: Optional DB connection. If None, opens and closes its own.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = get_sa_connection()

    rows = conn.execute(
        select(
            evaluations_table.c.score,
            runs_table.c.started_at,
            runs_table.c.id,
        )
        .select_from(
            evaluations_table.join(runs_table, runs_table.c.id == evaluations_table.c.run_id)
        )
        .where(evaluations_table.c.eval_type == "dqi")
        .order_by(runs_table.c.started_at.desc())
        .limit(200)
    ).mappings().fetchall()

    if owns_conn:
        conn.close()

    if len(rows) < 10:
        print("Need at least 10 DQI scores for monitoring.")
        return

    scores = [r["score"] for r in reversed(rows)]
    target = mean(scores[:50]) if len(scores) >= 50 else mean(scores)

    # CUSUM (tuned for 0-1 DQI scale)
    k, h = 0.03, 0.4
    s_pos = s_neg = 0
    alarms = []
    for i, score in enumerate(scores):
        s_pos = max(0, s_pos + (score - target - k))
        s_neg = max(0, s_neg + (target - k - score))
        if s_neg > h:
            alarms.append({"index": i, "type": "degradation", "cusum": round(s_neg, 3)})
            s_neg = 0
        if s_pos > h:
            alarms.append({"index": i, "type": "improvement", "cusum": round(s_pos, 3)})
            s_pos = 0

    # EWMA
    ewma = scores[0]
    lam = 0.1
    for s in scores:
        ewma = lam * s + (1 - lam) * ewma

    # Recent window
    last_7d = scores[-126:] if len(scores) >= 126 else scores  # ~18/day * 7
    last_1d = scores[-18:] if len(scores) >= 18 else scores

    print(f"\n=== DQI Monitor ===")
    print(f"Target DQI (baseline): {target:.3f}")
    print(f"Current EWMA:          {ewma:.3f}")
    print(f"Last 7 days avg:       {mean(last_7d):.3f} (n={len(last_7d)})")
    print(f"Last 1 day avg:        {mean(last_1d):.3f} (n={len(last_1d)})")
    print(f"Total scored runs:     {len(scores)}")

    if alarms:
        print(f"\nAlarms ({len(alarms)} total, showing last 5):")
        for a in alarms[-5:]:
            print(f"  {a['type']:12} at run index {a['index']} (CUSUM={a['cusum']})")
    else:
        print("\nNo alarms — system stable")


def show_status(conn=None):
    """Show all baselines and system changes.

    Args:
        conn: Optional DB connection. If None, opens and closes its own.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = get_sa_connection()

    baselines = conn.execute(
        select(baselines_table).order_by(baselines_table.c.created_at.desc())
    ).mappings().fetchall()

    changes = conn.execute(
        select(system_changes_table).order_by(system_changes_table.c.created_at.desc())
    ).mappings().fetchall()

    if owns_conn:
        conn.close()

    print(f"\n=== Baselines ({len(baselines)}) ===")
    for b in baselines:
        metrics = json.loads(b["metrics"])
        print(f"  {b['name']:30} DQI={metrics['avg_dqi']:.3f}  n={metrics['run_count']}  ({b['created_at'][:10]})")

    print(f"\n=== System Changes ({len(changes)}) ===")
    for c in changes:
        status = c["status"] or "measuring"
        p = f"P={c['p_improvement']:.2f}" if c["p_improvement"] else "pending"
        print(f"  {c['change_name']:30} {status:14} {p}  ({c['implemented_at'][:10]})")
