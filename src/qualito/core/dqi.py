"""DQI (Delegation Quality Index) calculator.

Composite quality score per run: completion (30%), quality (25%), efficiency (25%), cost (20%).
Stored in evaluations table with eval_type='dqi'.
"""

import json

from qualito.core.db import get_run, insert_evaluation

# DQI component weights
W_COMPLETION = 0.30
W_QUALITY = 0.25
W_EFFICIENCY = 0.25
W_COST = 0.20

# Tier mapping: task types -> complexity tier
TASK_TYPE_TIERS = {
    'test': 1, 'other': 1,
    'pr_review': 2, 'code': 2, 'jira': 2, 'confluence': 2,
    'research': 3, 'slack': 3, 'refactor': 3,
}

TIER_LABELS = {1: 'simple', 2: 'standard', 3: 'complex'}

# Cost thresholds per tier: list of (max_usd, score) — first match wins
COST_THRESHOLDS = {
    1: [(0.30, 1.0), (0.50, 0.8), (0.80, 0.6), (1.50, 0.4), (3.00, 0.2)],
    2: [(0.50, 1.0), (1.00, 0.8), (1.50, 0.6), (2.50, 0.4), (5.00, 0.2)],
    3: [(0.50, 1.0), (1.00, 0.8), (2.00, 0.6), (4.00, 0.4), (6.00, 0.2)],
}

# Duration thresholds per tier: list of (max_seconds, score) — first match wins
DURATION_THRESHOLDS = {
    1: [(30, 1.0), (60, 0.8), (120, 0.6), (300, 0.4), (600, 0.2)],
    2: [(60, 1.0), (150, 0.8), (300, 0.6), (600, 0.4), (900, 0.2)],
    3: [(60, 1.0), (180, 0.8), (360, 0.6), (600, 0.4), (900, 0.2)],
}

# Quality check weights (must sum to ~1.0)
QUALITY_WEIGHTS = {
    "completed": 0.05,
    "has_summary": 0.20,
    "tool_calls_made": 0.15,
    "chains_recorded": 0.10,
    "cost_reasonable": 0.05,
    "within_timeout": 0.05,
    "has_findings": 0.20,
    "has_output": 0.20,
}


def _score_completion(run: dict) -> float:
    status = run.get("status")
    if status == "completed":
        return 1.0
    if status == "partial":
        return 0.5
    return 0.0


def _score_quality(run: dict) -> float:
    auto_eval = next(
        (e for e in run.get("evaluations", []) if e.get("eval_type") == "auto"),
        None,
    )
    if not auto_eval:
        return 0.0

    checks = auto_eval.get("checks")
    if not checks:
        return 0.0
    if isinstance(checks, str):
        checks = json.loads(checks)

    score = sum(
        QUALITY_WEIGHTS.get(name, 0) * (1.0 if check.get("passed") else 0.0)
        for name, check in checks.items()
    )

    # Incorporate human score if available (40% override)
    human_eval = next(
        (e for e in run.get("evaluations", []) if e.get("eval_type") == "human"),
        None,
    )
    if human_eval and human_eval.get("score") is not None:
        score = 0.60 * score + 0.40 * human_eval["score"]

    return score


def _score_efficiency(run: dict, task_type: str = 'other') -> float:
    duration_ms = run.get("duration_ms")
    if not duration_ms:
        return 0.5
    secs = duration_ms / 1000
    tier = TASK_TYPE_TIERS.get(task_type, 2)
    for threshold, score in DURATION_THRESHOLDS[tier]:
        if secs <= threshold:
            return score
    return 0.0


def _score_cost(run: dict, task_type: str = 'other') -> float:
    cost = run.get("cost_usd")
    if cost is None:
        return 0.5
    tier = TASK_TYPE_TIERS.get(task_type, 2)
    for threshold, score in COST_THRESHOLDS[tier]:
        if cost <= threshold:
            return score
    return 0.0


def calculate_dqi(run: dict, task_type: str = 'other') -> dict:
    """Calculate DQI components and composite for a run dict.

    Pure function — no DB access. Pass in a full run dict with evaluations attached.
    """
    completion = _score_completion(run)
    quality = _score_quality(run)
    efficiency = _score_efficiency(run, task_type)
    cost_score = _score_cost(run, task_type)

    dqi = (
        W_COMPLETION * completion
        + W_QUALITY * quality
        + W_EFFICIENCY * efficiency
        + W_COST * cost_score
    )

    tier = TASK_TYPE_TIERS.get(task_type, 2)

    return {
        "dqi": round(dqi, 4),
        "completion": round(completion, 4),
        "quality": round(quality, 4),
        "efficiency": round(efficiency, 4),
        "cost_score": round(cost_score, 4),
        "tier": tier,
        "tier_label": TIER_LABELS[tier],
        "task_type": task_type,
    }


def store_dqi(run_id: str, conn=None) -> dict:
    """Calculate DQI and store in evaluations table. Returns scores dict.

    Args:
        run_id: The run ID to score.
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

    task_type = run.get("task_type", "other") or "other"
    scores = calculate_dqi(run, task_type)
    insert_evaluation(
        conn,
        run_id,
        eval_type="dqi",
        score=scores["dqi"],
        categories=scores,
    )
    if owns_conn:
        conn.close()
    return scores
