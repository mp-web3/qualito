"""Benchmark suite & experiment tracker for session quality.

Provides suite definition, experiment execution, and statistical comparison
(Wilcoxon signed-rank + Bayesian) between experiments.
"""

import json
import math
import random
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

from sqlalchemy import and_, func, select

from qualito.core.db import (
    benchmark_suites_table,
    evaluations_table,
    experiment_comparisons_table,
    experiments_table,
    get_sa_connection,
    runs_table,
)
from qualito.core.dqi import store_dqi


# ── Statistical helpers ─────────────────────────────────────────────

def _wilcoxon_signed_rank(before: list[float], after: list[float]) -> float:
    """Pure-Python Wilcoxon signed-rank test. Returns approximate p-value."""
    diffs = [(a - b) for a, b in zip(after, before) if abs(a - b) > 1e-9]
    n = len(diffs)
    if n == 0:
        return 1.0

    # Rank absolute differences
    indexed = sorted(enumerate(diffs), key=lambda x: abs(x[1]))
    ranks = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j < n - 1 and abs(abs(indexed[j + 1][1]) - abs(indexed[i][1])) < 1e-9:
            j += 1
        avg_rank = (i + 1 + j + 1) / 2
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg_rank
        i = j + 1

    # Sum positive and negative ranks
    w_plus = sum(ranks[i] for i in range(n) if diffs[i] > 0)
    w_minus = sum(ranks[i] for i in range(n) if diffs[i] < 0)
    w = min(w_plus, w_minus)

    # Normal approximation (valid for n >= 6)
    if n < 6:
        return 0.5 if w_plus > w_minus else 1.0

    mu = n * (n + 1) / 4
    sigma = math.sqrt(n * (n + 1) * (2 * n + 1) / 24)
    z = (w - mu) / sigma if sigma > 0 else 0
    p = 2 * (1 - _norm_cdf(abs(z)))
    return max(0.0, min(1.0, p))


def _norm_cdf(x: float) -> float:
    """Approximation of standard normal CDF."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _bayesian_p_improvement(after_wins: int, n: int, samples: int = 10000) -> float:
    """P(after > before) using Beta-Binomial model."""
    alpha_a = after_wins + 1
    beta_a = (n - after_wins) + 1
    count = 0
    for _ in range(samples):
        x = random.gammavariate(alpha_a, 1)
        y = random.gammavariate(beta_a, 1)
        if x / (x + y) > 0.5:
            count += 1
    return count / samples


# ── Suite loading ──────────────────────────────────────────────────

def load_suite_tasks(suite_path: Path) -> list[dict]:
    """Load benchmark tasks from a JSON file.

    Args:
        suite_path: Path to a JSON file containing a list of task dicts.
                    Each dict should have: label, workspace, pipeline_mode, task.

    Returns:
        List of task definition dicts.
    """
    with open(suite_path) as f:
        tasks = json.load(f)
    if not isinstance(tasks, list):
        raise ValueError(f"Suite file must contain a JSON array, got {type(tasks).__name__}")
    return tasks


# ── SA Core bridge functions (Phase 2) ─────────────────────────────


def define_suite(name: str, tasks: list[dict], description: str = "", conn=None):
    """Register a benchmark suite.

    Args:
        name: Suite name (must be unique).
        tasks: List of task dicts with label, workspace, pipeline_mode, task.
        description: Optional description.
        conn: Optional DB connection. If None, opens and closes its own.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = get_sa_connection()

    existing = conn.execute(
        select(benchmark_suites_table.c.id)
        .where(benchmark_suites_table.c.name == name)
    ).mappings().fetchone()

    if existing:
        print(f"Suite '{name}' already exists (id={existing['id']})")
        if owns_conn:
            conn.close()
        return

    conn.execute(
        benchmark_suites_table.insert().values(
            name=name,
            description=description or f"Benchmark suite {name}",
            tasks=json.dumps(tasks),
        )
    )
    conn.commit()
    if owns_conn:
        conn.close()
    print(f"Defined suite '{name}' with {len(tasks)} tasks")


def run_experiment(name: str, suite_name: str = "v1", description: str = "",
                   delegate_command: str = "qualito delegate", conn=None):
    """Run a benchmark experiment: launch all tasks, poll, compute DQI.

    Args:
        name: Experiment name (must be unique).
        suite_name: Name of the benchmark suite to run.
        description: Optional description.
        delegate_command: Command to use for delegation (default: 'dqi delegate').
        conn: Optional DB connection. If None, opens and closes its own.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = get_sa_connection()

    # Check name not taken
    existing = conn.execute(
        select(experiments_table.c.id).where(experiments_table.c.name == name)
    ).mappings().fetchone()
    if existing:
        print(f"Error: experiment '{name}' already exists")
        if owns_conn:
            conn.close()
        return

    # Load suite
    suite = conn.execute(
        select(benchmark_suites_table).where(benchmark_suites_table.c.name == suite_name)
    ).mappings().fetchone()
    if not suite:
        print(f"Error: suite '{suite_name}' not found. Define it first.")
        if owns_conn:
            conn.close()
        return

    tasks = json.loads(suite["tasks"])

    # Create experiment row
    conn.execute(
        experiments_table.insert().values(
            name=name,
            description=description,
            suite_id=suite["id"],
            status="running",
            config_snapshot=json.dumps({
                "suite": suite_name,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }),
        )
    )
    conn.commit()

    print(f"\n=== Running experiment: {name} ({len(tasks)} tasks) ===\n")

    # Parse delegate command into parts
    cmd_parts = delegate_command.split()

    # Launch all tasks
    run_ids = []
    for i, task_def in enumerate(tasks):
        if i > 0:
            time.sleep(3)

        args = cmd_parts + [
            "--workspace", task_def["workspace"],
            "--skill", "benchmark",
            "--task", task_def["task"],
        ]
        if task_def.get("pipeline_mode") == "research-only":
            args.append("--research-only")

        try:
            launch_timeout = 600 if task_def.get("pipeline_mode") == "research-only" else 30
            result = subprocess.run(args, capture_output=True, text=True, timeout=launch_timeout)
            run_id = _parse_run_id(result.stdout)
            if run_id:
                run_ids.append({"label": task_def["label"], "run_id": run_id})
                print(f"  [{i+1}/{len(tasks)}] {task_def['label']:20} -> {run_id}")
            else:
                print(f"  [{i+1}/{len(tasks)}] {task_def['label']:20} -> FAILED TO LAUNCH")
                print(f"    stdout: {result.stdout[:200]}")
                print(f"    stderr: {result.stderr[:200]}")
                run_ids.append({"label": task_def["label"], "run_id": None, "error": "launch failed"})
        except Exception as e:
            print(f"  [{i+1}/{len(tasks)}] {task_def['label']:20} -> ERROR: {e}")
            run_ids.append({"label": task_def["label"], "run_id": None, "error": str(e)})

    # Store run IDs
    conn.execute(
        experiments_table.update()
        .where(experiments_table.c.name == name)
        .values(run_ids=json.dumps(run_ids))
    )
    conn.commit()

    # Poll until all complete
    print(f"\nPolling for completion (timeout: 10 min)...")
    _poll_until_complete(conn, run_ids, timeout=600)

    # Ensure DQI is computed for all completed runs
    # Note: store_dqi is not yet migrated, so don't pass SA conn to it
    for item in run_ids:
        if item.get("run_id"):
            try:
                store_dqi(item["run_id"])
            except Exception:
                pass

    # Compute per-task DQI
    per_task, avg = _compute_experiment_dqi(conn, run_ids)

    conn.execute(
        experiments_table.update()
        .where(experiments_table.c.name == name)
        .values(
            status="completed",
            avg_dqi=avg,
            per_task_dqi=json.dumps(per_task),
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
    )
    conn.commit()

    if owns_conn:
        conn.close()

    _print_experiment_report(name, per_task, avg)


def compare_experiments(before_name: str, after_name: str, conn=None):
    """Compare two experiments with Wilcoxon + Bayesian tests.

    Args:
        before_name: Name of the baseline experiment.
        after_name: Name of the comparison experiment.
        conn: Optional DB connection. If None, opens and closes its own.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = get_sa_connection()

    before = conn.execute(
        select(experiments_table).where(experiments_table.c.name == before_name)
    ).mappings().fetchone()
    after = conn.execute(
        select(experiments_table).where(experiments_table.c.name == after_name)
    ).mappings().fetchone()

    if not before or not after:
        print(f"Error: experiment not found. "
              f"{before_name}={'found' if before else 'missing'}, "
              f"{after_name}={'found' if after else 'missing'}")
        if owns_conn:
            conn.close()
        return

    if before["status"] != "completed" or after["status"] != "completed":
        print("Error: both experiments must be completed before comparing.")
        if owns_conn:
            conn.close()
        return

    before_tasks = json.loads(before["per_task_dqi"])
    after_tasks = json.loads(after["per_task_dqi"])

    # Build paired observations
    labels = sorted(set(before_tasks.keys()) & set(after_tasks.keys()))
    if not labels:
        print("Error: no overlapping task labels between experiments.")
        if owns_conn:
            conn.close()
        return

    before_scores = [before_tasks[l] for l in labels]
    after_scores = [after_tasks[l] for l in labels]

    # Statistical tests
    wilcoxon_p = _wilcoxon_signed_rank(before_scores, after_scores)

    after_wins = sum(1 for a, b in zip(after_scores, before_scores) if a > b)
    bayesian_p = _bayesian_p_improvement(after_wins, len(labels))

    diffs = [a - b for a, b in zip(after_scores, before_scores)]
    effect = mean(diffs)

    # Per-task delta
    per_task_delta = {}
    for label in labels:
        b, a = before_tasks[label], after_tasks[label]
        per_task_delta[label] = {"before": round(b, 4), "after": round(a, 4), "delta": round(a - b, 4)}

    # Verdict
    if bayesian_p > 0.90 and effect > 0:
        verdict = "improved"
    elif bayesian_p < 0.10 and effect < 0:
        verdict = "degraded"
    else:
        verdict = "inconclusive"

    # Store
    comp_name = f"{before_name}_vs_{after_name}"
    conn.execute(
        experiment_comparisons_table.insert().values(
            name=comp_name,
            before_experiment_id=before["id"],
            after_experiment_id=after["id"],
            per_task_delta=json.dumps(per_task_delta),
            wilcoxon_p=round(wilcoxon_p, 4),
            bayesian_p_improvement=round(bayesian_p, 4),
            effect_size=round(effect, 4),
            verdict=verdict,
        )
    )
    conn.commit()

    if owns_conn:
        conn.close()

    # Print report
    print(f"\n{'=' * 60}")
    print(f"  Comparison: {before_name} vs {after_name}")
    print(f"{'=' * 60}")
    print(f"\n{'Task':<20} {'Before':>8} {'After':>8} {'Delta':>8}")
    print(f"{'-' * 48}")
    for label in labels:
        d = per_task_delta[label]
        sign = "+" if d["delta"] > 0 else ""
        print(f"{label:<20} {d['before']:>8.3f} {d['after']:>8.3f} {sign}{d['delta']:>7.4f}")
    print(f"{'-' * 48}")
    sign = "+" if effect > 0 else ""
    print(f"{'AVERAGE':<20} {before['avg_dqi']:>8.3f} {after['avg_dqi']:>8.3f} {sign}{effect:>7.4f}")
    print(f"\n  Wilcoxon p-value:      {wilcoxon_p:.4f}")
    print(f"  P(improvement):        {bayesian_p:.3f}")
    print(f"  Effect size:           {sign}{effect:.4f}")
    print(f"  Verdict:               {verdict.upper()}")
    print(f"{'=' * 60}")


def show_status(conn=None):
    """Show all suites, experiments, and comparisons.

    Args:
        conn: Optional DB connection. If None, opens and closes its own.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = get_sa_connection()

    suites = conn.execute(
        select(benchmark_suites_table).order_by(benchmark_suites_table.c.created_at)
    ).mappings().fetchall()

    experiments = conn.execute(
        select(experiments_table).order_by(experiments_table.c.created_at)
    ).mappings().fetchall()

    # Comparison query with JOINs
    before_exp = experiments_table.alias("b")
    after_exp = experiments_table.alias("a")
    comparisons = conn.execute(
        select(
            experiment_comparisons_table,
            before_exp.c.name.label("before_name"),
            after_exp.c.name.label("after_name"),
        )
        .select_from(
            experiment_comparisons_table
            .join(before_exp, before_exp.c.id == experiment_comparisons_table.c.before_experiment_id)
            .join(after_exp, after_exp.c.id == experiment_comparisons_table.c.after_experiment_id)
        )
        .order_by(experiment_comparisons_table.c.created_at)
    ).mappings().fetchall()

    if owns_conn:
        conn.close()

    print(f"\n{'=' * 70}")
    print(f"  Benchmark Status")
    print(f"{'=' * 70}")

    print(f"\n  Suites ({len(suites)})")
    print(f"  {'-' * 60}")
    for s in suites:
        tasks = json.loads(s["tasks"])
        print(f"  {s['name']:20} {len(tasks)} tasks    {s['created_at'][:10]}")

    print(f"\n  Experiments ({len(experiments)})")
    print(f"  {'-' * 60}")
    for e in experiments:
        run_ids = json.loads(e["run_ids"]) if e["run_ids"] else []
        completed = sum(1 for r in run_ids if r.get("run_id"))
        dqi_str = f"DQI={e['avg_dqi']:.3f}" if e["avg_dqi"] else "DQI=-"
        print(f"  {e['name']:25} {e['status']:12} {dqi_str:12} {completed}/{len(run_ids)} tasks   {e['created_at'][:10]}")

    if comparisons:
        print(f"\n  Comparisons ({len(comparisons)})")
        print(f"  {'-' * 60}")
        for c in comparisons:
            p = f"P={c['bayesian_p_improvement']:.2f}" if c["bayesian_p_improvement"] is not None else "P=-"
            eff = f"delta={c['effect_size']:+.3f}" if c["effect_size"] is not None else "delta=-"
            verdict = (c["verdict"] or "-").upper()
            print(f"  {c['before_name']} vs {c['after_name']}")
            print(f"    {p:12} {eff:16} {verdict}")

    print(f"\n{'=' * 70}")


# ── Helpers ─────────────────────────────────────────────────────────

def _parse_run_id(stdout: str) -> str | None:
    """Extract run ID from delegate output."""
    match = re.search(r"(?:Delegated|complete):\s+(\d{8}-\d{6}(?:-\d+)?)", stdout)
    return match.group(1) if match else None


def _poll_until_complete(conn, run_ids: list[dict], timeout: int = 600):
    """Poll run statuses until all complete or timeout."""
    valid_ids = [r["run_id"] for r in run_ids if r.get("run_id")]
    if not valid_ids:
        return

    start = time.time()
    while time.time() - start < timeout:
        pending = []
        for rid in valid_ids:
            row = conn.execute(
                select(runs_table.c.status).where(runs_table.c.id == rid)
            ).mappings().fetchone()
            if not row or row["status"] in ("running", None, ""):
                pending.append(rid)

        if not pending:
            print(f"  All {len(valid_ids)} tasks completed.")
            return

        elapsed = int(time.time() - start)
        print(f"  [{elapsed}s] {len(valid_ids) - len(pending)}/{len(valid_ids)} done, {len(pending)} pending...")
        time.sleep(15)

    print(f"  Timeout after {timeout}s. Some tasks still pending.")


def _compute_experiment_dqi(conn, run_ids: list[dict]) -> tuple[dict, float]:
    """Compute per-task DQI from experiment run IDs."""
    per_task = {}
    scores = []

    for item in run_ids:
        rid = item.get("run_id")
        label = item["label"]
        if not rid:
            per_task[label] = 0.0
            scores.append(0.0)
            continue

        row = conn.execute(
            select(evaluations_table.c.score)
            .where(
                and_(
                    evaluations_table.c.run_id == rid,
                    evaluations_table.c.eval_type == "dqi",
                )
            )
        ).mappings().fetchone()

        dqi = row["score"] if row and row["score"] is not None else 0.0
        per_task[label] = round(dqi, 4)
        scores.append(dqi)

    avg = mean(scores) if scores else 0.0
    return per_task, round(avg, 4)


def _print_experiment_report(name: str, per_task: dict, avg: float):
    """Print formatted experiment results."""
    print(f"\n{'=' * 50}")
    print(f"  Experiment: {name}")
    print(f"{'=' * 50}")
    print(f"\n{'Task':<20} {'DQI':>8}")
    print(f"{'-' * 30}")
    for label, score in sorted(per_task.items(), key=lambda x: -x[1]):
        indicator = "ok" if score >= 0.7 else "mid" if score >= 0.4 else "low"
        print(f"{label:<20} {score:>7.3f} {indicator}")
    print(f"{'-' * 30}")
    print(f"{'AVERAGE':<20} {avg:>7.3f}")
    print(f"{'=' * 50}")
