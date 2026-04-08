"""Incident detection engine for delegation quality.

Detects quality incidents from delegation run data:
- Consecutive failures (critical)
- DQI burn rate (warning)
- Cost anomalies (warning)
- Error pattern spikes (info)

All functions accept a conn parameter (no internal get_db() calls).
SLO constants are parameters with defaults so they can come from config.
"""

import hashlib
import json
import sqlite3
from datetime import date, datetime, timezone
from statistics import mean, stdev

from qualito.core.db import (
    insert_incident,
    insert_incident_event,
    update_incident,
)

# --- Default SLO constants ---

DEFAULT_SLO_QUALITY = 0.60
DEFAULT_SLO_AVAILABILITY = 0.95
DEFAULT_SLO_COST = 3.00
DEFAULT_BASELINE_WINDOW = 50
DEFAULT_FAST_WINDOW = 10
DEFAULT_CONSECUTIVE_THRESHOLD = 3
DEFAULT_MONITORING_CLEAN_THRESHOLD = 20

# --- Baseline cache ---

_baseline_cache: dict[str, dict] = {}
_baseline_run_counts: dict[str, int] = {}


def compute_workspace_baselines(
    conn: sqlite3.Connection,
    workspace: str,
    *,
    slo_quality: float = DEFAULT_SLO_QUALITY,
    slo_cost: float = DEFAULT_SLO_COST,
    baseline_window: int = DEFAULT_BASELINE_WINDOW,
) -> dict:
    """Compute quality baselines for a workspace from last N DQI-scored runs.

    Returns dict with mean_dqi, stddev_dqi, mean_cost, stddev_cost,
    completion_rate, error_rate. Cached until baseline_window+ new runs.
    """
    # Check cache freshness
    total_runs = conn.execute(
        "SELECT COUNT(*) as cnt FROM runs WHERE workspace = ?", (workspace,)
    ).fetchone()["cnt"]

    if workspace in _baseline_cache:
        last_count = _baseline_run_counts.get(workspace, 0)
        if total_runs - last_count < baseline_window:
            return _baseline_cache[workspace]

    # Get last baseline_window DQI-scored completed runs
    rows = conn.execute("""
        SELECT r.id, r.status, r.cost_usd, e.score as dqi
        FROM runs r
        JOIN evaluations e ON e.run_id = r.id AND e.eval_type = 'dqi'
        WHERE r.workspace = ?
        ORDER BY r.started_at DESC
        LIMIT ?
    """, (workspace, baseline_window)).fetchall()

    if len(rows) < 5:
        baseline = {
            "mean_dqi": slo_quality,
            "stddev_dqi": 0.15,
            "mean_cost": slo_cost,
            "stddev_cost": 1.0,
            "completion_rate": DEFAULT_SLO_AVAILABILITY,
            "error_rate": 0.1,
            "sample_size": len(rows),
        }
        _baseline_cache[workspace] = baseline
        _baseline_run_counts[workspace] = total_runs
        return baseline

    dqi_scores = [r["dqi"] for r in rows if r["dqi"] is not None]
    costs = [r["cost_usd"] for r in rows if r["cost_usd"] is not None]
    completed = sum(1 for r in rows if r["status"] == "completed")

    # Error rate: runs with any is_error tool calls
    run_ids = [r["id"] for r in rows]
    placeholders = ",".join("?" for _ in run_ids)
    error_runs = conn.execute(f"""
        SELECT COUNT(DISTINCT run_id) as cnt
        FROM tool_calls
        WHERE run_id IN ({placeholders}) AND is_error = 1
    """, run_ids).fetchone()["cnt"]

    mean_dqi = mean(dqi_scores) if dqi_scores else slo_quality
    stddev_dqi = stdev(dqi_scores) if len(dqi_scores) >= 2 else 0.15

    mean_cost = mean(costs) if costs else slo_cost
    stddev_cost = stdev(costs) if len(costs) >= 2 else 1.0

    baseline = {
        "mean_dqi": round(mean_dqi, 4),
        "stddev_dqi": round(stddev_dqi, 4),
        "mean_cost": round(mean_cost, 4),
        "stddev_cost": round(stddev_cost, 4),
        "completion_rate": round(completed / len(rows), 4),
        "error_rate": round(error_runs / len(rows), 4),
        "sample_size": len(rows),
    }

    _baseline_cache[workspace] = baseline
    _baseline_run_counts[workspace] = total_runs
    return baseline


def check_consecutive_failures(
    conn: sqlite3.Connection,
    run_id: str,
    workspace: str,
    *,
    consecutive_threshold: int = DEFAULT_CONSECUTIVE_THRESHOLD,
) -> dict | None:
    """Detect N+ consecutive non-completed runs. Returns critical incident or None."""
    rows = conn.execute("""
        SELECT id, status FROM runs
        WHERE workspace = ?
        ORDER BY started_at DESC
        LIMIT 5
    """, (workspace,)).fetchall()

    if len(rows) < consecutive_threshold:
        return None

    consecutive = 0
    affected_ids = []
    for r in rows:
        if r["status"] != "completed":
            consecutive += 1
            affected_ids.append(r["id"])
        else:
            break

    if consecutive < consecutive_threshold:
        return None

    today = date.today().isoformat()
    return {
        "incident_key": f"consec_fail_{workspace}_{today}",
        "category": "availability",
        "severity": "critical",
        "workspace": workspace,
        "title": f"{consecutive} consecutive failures in {workspace}",
        "description": (
            f"Last {consecutive} runs in workspace '{workspace}' did not complete. "
            f"Affected runs: {', '.join(affected_ids[:5])}"
        ),
        "detection_method": "consecutive_failures",
        "trigger_metric": "consecutive_non_completed",
        "trigger_value": float(consecutive),
        "baseline_value": float(consecutive_threshold),
        "affected_run_ids": affected_ids,
        "total_affected_runs": consecutive,
    }


def check_dqi_burn_rate(
    conn: sqlite3.Connection,
    run_id: str,
    workspace: str,
    *,
    fast_window: int = DEFAULT_FAST_WINDOW,
    slo_quality: float = DEFAULT_SLO_QUALITY,
    slo_cost: float = DEFAULT_SLO_COST,
    baseline_window: int = DEFAULT_BASELINE_WINDOW,
) -> dict | None:
    """Detect DQI dropping below baseline - 1.5 * stddev. Returns warning or None."""
    baselines = compute_workspace_baselines(
        conn, workspace,
        slo_quality=slo_quality, slo_cost=slo_cost,
        baseline_window=baseline_window,
    )
    if baselines["sample_size"] < 5:
        return None

    rows = conn.execute("""
        SELECT e.score as dqi, r.id
        FROM runs r
        JOIN evaluations e ON e.run_id = r.id AND e.eval_type = 'dqi'
        WHERE r.workspace = ?
        ORDER BY r.started_at DESC
        LIMIT ?
    """, (workspace, fast_window)).fetchall()

    if len(rows) < 3:
        return None

    rolling_avg = mean(r["dqi"] for r in rows)
    threshold = baselines["mean_dqi"] - 1.5 * baselines["stddev_dqi"]

    if rolling_avg >= threshold:
        return None

    stddev_dqi = baselines["stddev_dqi"]
    burn_rate = (
        (baselines["mean_dqi"] - rolling_avg) / stddev_dqi
        if stddev_dqi > 0
        else 0.0
    )

    today = date.today().isoformat()
    affected_ids = [r["id"] for r in rows]
    return {
        "incident_key": f"dqi_burn_{workspace}_{today}",
        "category": "quality",
        "severity": "warning",
        "workspace": workspace,
        "title": (
            f"DQI declining in {workspace}: {rolling_avg:.3f} avg "
            f"(baseline {baselines['mean_dqi']:.3f})"
        ),
        "description": (
            f"Rolling {fast_window}-run DQI average ({rolling_avg:.3f}) is below "
            f"threshold ({threshold:.3f}). Baseline mean: {baselines['mean_dqi']:.3f}, "
            f"stddev: {stddev_dqi:.3f}."
        ),
        "detection_method": "dqi_burn_rate",
        "trigger_metric": "rolling_avg_dqi",
        "trigger_value": round(rolling_avg, 4),
        "baseline_value": round(baselines["mean_dqi"], 4),
        "burn_rate": round(burn_rate, 4),
        "affected_run_ids": affected_ids,
        "total_affected_runs": len(affected_ids),
    }


def check_cost_anomaly(
    conn: sqlite3.Connection,
    run_id: str,
    workspace: str,
    *,
    fast_window: int = DEFAULT_FAST_WINDOW,
    slo_quality: float = DEFAULT_SLO_QUALITY,
    slo_cost: float = DEFAULT_SLO_COST,
    baseline_window: int = DEFAULT_BASELINE_WINDOW,
) -> dict | None:
    """Detect cost anomalies (3+ of last N runs with z-score > 2). Returns warning or None."""
    baselines = compute_workspace_baselines(
        conn, workspace,
        slo_quality=slo_quality, slo_cost=slo_cost,
        baseline_window=baseline_window,
    )
    if baselines["sample_size"] < 5 or baselines["stddev_cost"] <= 0:
        return None

    rows = conn.execute("""
        SELECT id, cost_usd FROM runs
        WHERE workspace = ? AND cost_usd IS NOT NULL
        ORDER BY started_at DESC
        LIMIT ?
    """, (workspace, fast_window)).fetchall()

    if len(rows) < 3:
        return None

    anomalous = []
    for r in rows:
        z = (r["cost_usd"] - baselines["mean_cost"]) / baselines["stddev_cost"]
        if z > 2:
            anomalous.append(r["id"])

    if len(anomalous) < 3:
        return None

    total_excess = sum(
        r["cost_usd"] - baselines["mean_cost"]
        for r in rows
        if (r["cost_usd"] - baselines["mean_cost"]) / baselines["stddev_cost"] > 2
    )

    today = date.today().isoformat()
    return {
        "incident_key": f"cost_anomaly_{workspace}_{today}",
        "category": "cost",
        "severity": "warning",
        "workspace": workspace,
        "title": (
            f"Cost spike in {workspace}: {len(anomalous)} of "
            f"{len(rows)} runs above "
            f"${baselines['mean_cost'] + 2*baselines['stddev_cost']:.2f}"
        ),
        "description": (
            f"{len(anomalous)} of last {len(rows)} runs have costs >2 standard deviations "
            f"above baseline (mean: ${baselines['mean_cost']:.2f}, "
            f"\u03c3: ${baselines['stddev_cost']:.2f}). "
            f"Excess cost: ${total_excess:.2f}."
        ),
        "detection_method": "cost_anomaly",
        "trigger_metric": "anomalous_run_count",
        "trigger_value": float(len(anomalous)),
        "baseline_value": round(baselines["mean_cost"], 4),
        "cost_impact_usd": round(total_excess, 2),
        "affected_run_ids": anomalous,
        "total_affected_runs": len(anomalous),
    }


def check_error_pattern_spike(
    conn: sqlite3.Connection,
    run_id: str,
    workspace: str,
) -> dict | None:
    """Detect error pattern frequency spikes. Returns info incident or None."""
    # Recent window: last 20 runs
    recent_runs = conn.execute("""
        SELECT id FROM runs
        WHERE workspace = ?
        ORDER BY started_at DESC
        LIMIT 20
    """, (workspace,)).fetchall()

    if len(recent_runs) < 5:
        return None

    recent_ids = [r["id"] for r in recent_runs]
    placeholders = ",".join("?" for _ in recent_ids)

    recent_errors = conn.execute(f"""
        SELECT SUBSTR(result_summary, 1, 300) as pattern
        FROM tool_calls
        WHERE run_id IN ({placeholders}) AND is_error = 1
    """, recent_ids).fetchall()

    if not recent_errors:
        return None

    # Count recent patterns
    recent_counts: dict[str, int] = {}
    for r in recent_errors:
        p = r["pattern"] or "unknown"
        recent_counts[p] = recent_counts.get(p, 0) + 1

    # Historical window: last 100 runs
    historical_runs = conn.execute("""
        SELECT id FROM runs
        WHERE workspace = ?
        ORDER BY started_at DESC
        LIMIT 100
    """, (workspace,)).fetchall()

    hist_ids = [r["id"] for r in historical_runs]
    hist_placeholders = ",".join("?" for _ in hist_ids)

    hist_errors = conn.execute(f"""
        SELECT SUBSTR(result_summary, 1, 300) as pattern
        FROM tool_calls
        WHERE run_id IN ({hist_placeholders}) AND is_error = 1
    """, hist_ids).fetchall()

    hist_counts: dict[str, int] = {}
    for r in hist_errors:
        p = r["pattern"] or "unknown"
        hist_counts[p] = hist_counts.get(p, 0) + 1

    # Normalize historical to same window size as recent
    ratio = len(recent_runs) / max(len(historical_runs), 1)

    spiking_patterns = []
    for pattern, count in recent_counts.items():
        hist_normalized = hist_counts.get(pattern, 0) * ratio
        # New pattern appearing 5+ times
        if hist_counts.get(pattern, 0) == 0 and count >= 5:
            spiking_patterns.append((pattern, count, 0))
        # Existing pattern doubling in frequency
        elif hist_normalized > 0 and count > 2 * hist_normalized:
            spiking_patterns.append((pattern, count, hist_normalized))

    if not spiking_patterns:
        return None

    top_pattern, top_count, top_hist = spiking_patterns[0]
    pattern_hash = hashlib.md5(top_pattern.encode()).hexdigest()[:8]
    today = date.today().isoformat()

    description = f"{len(spiking_patterns)} error pattern(s) spiking in last 20 runs.\n"
    for pattern, count, hist in spiking_patterns[:5]:
        description += f'- "{pattern[:200]}" ({count}x recent vs {hist:.1f}x historical)\n'

    if len(spiking_patterns) > 1:
        extra = len(spiking_patterns) - 1
        title = (
            f'Error spike in {workspace}: '
            f'"{top_pattern[:60]}" (+{extra} more)'
        )
    else:
        title = f'Error spike in {workspace}: "{top_pattern[:60]}"'

    return {
        "incident_key": f"error_spike_{workspace}_{pattern_hash}_{today}",
        "category": "error_pattern",
        "severity": "info",
        "workspace": workspace,
        "title": title,
        "description": description,
        "detection_method": "error_pattern_spike",
        "trigger_metric": "spiking_pattern_count",
        "trigger_value": float(len(spiking_patterns)),
        "baseline_value": 0.0,
        "affected_run_ids": recent_ids[:10],
        "total_affected_runs": len(recent_ids),
    }


def check_run(conn: sqlite3.Connection, run_id: str) -> list[dict]:
    """Main entry point: run all checks for a given run.

    Returns list of new/updated incidents.
    """
    row = conn.execute(
        "SELECT workspace FROM runs WHERE id = ?", (run_id,)
    ).fetchone()
    if not row:
        return []

    workspace = row["workspace"]
    results = []

    checks = [
        check_consecutive_failures,
        check_dqi_burn_rate,
        check_cost_anomaly,
        check_error_pattern_spike,
    ]

    for check_fn in checks:
        incident = check_fn(conn, run_id, workspace)
        if incident is None:
            continue

        # Check for existing open incident with same key
        existing = conn.execute(
            "SELECT * FROM incidents WHERE incident_key = ? "
            "AND status IN ('detected', 'confirmed')",
            (incident["incident_key"],),
        ).fetchone()

        if existing:
            # Update existing: append run_id, increment counts
            existing_ids = json.loads(existing["affected_run_ids"] or "[]")
            if run_id not in existing_ids:
                existing_ids.append(run_id)
            new_total = existing["total_affected_runs"] + 1
            new_cost = (existing["cost_impact_usd"] or 0) + incident.get("cost_impact_usd", 0)
            update_incident(
                conn,
                existing["id"],
                affected_run_ids=existing_ids,
                total_affected_runs=new_total,
                cost_impact_usd=round(new_cost, 2),
            )
            results.append({
                "action": "updated",
                "incident_id": existing["id"],
                "incident_key": incident["incident_key"],
            })
        else:
            # Insert new incident
            incident_id = insert_incident(conn, incident)
            insert_incident_event(
                conn,
                incident_id,
                event_type="detected",
                new_status="detected",
                data={"run_id": run_id, "detection_method": incident["detection_method"]},
            )
            results.append({
                "action": "created",
                "incident_id": incident_id,
                "incident_key": incident["incident_key"],
                "severity": incident["severity"],
            })

    return results


def check_monitoring_close(
    conn: sqlite3.Connection,
    *,
    monitoring_clean_threshold: int = DEFAULT_MONITORING_CLEAN_THRESHOLD,
    slo_quality: float = DEFAULT_SLO_QUALITY,
) -> list[dict]:
    """Auto-close incidents in 'monitoring' status when quality is sustained.

    For incidents with status='monitoring', checks runs completed AFTER the most
    recent 'fix_deployed' or 'monitoring_started' event. If N+ consecutive
    completed runs for that workspace have DQI above slo_quality, transitions
    the incident to 'resolved' with resolution_type='experiment_fix'.

    Returns list of resolved incidents.
    """
    monitoring_incidents = conn.execute(
        "SELECT * FROM incidents WHERE status = 'monitoring'"
    ).fetchall()

    resolved = []
    for inc in monitoring_incidents:
        ws = inc["workspace"]
        incident_id = inc["id"]

        # Find the most recent 'fix_deployed' or 'monitoring' transition event
        event = conn.execute("""
            SELECT created_at FROM incident_events
            WHERE incident_id = ? AND event_type = 'status_change'
              AND new_status IN ('monitoring', 'fix_deployed')
            ORDER BY id DESC LIMIT 1
        """, (incident_id,)).fetchone()

        if not event:
            continue

        since_ts = event["created_at"]

        # Get runs completed after that event for this workspace
        recent = conn.execute("""
            SELECT r.id, r.status, e.score as dqi
            FROM runs r
            LEFT JOIN evaluations e ON e.run_id = r.id AND e.eval_type = 'dqi'
            WHERE r.workspace = ? AND r.started_at >= ?
            ORDER BY r.started_at ASC
        """, (ws, since_ts)).fetchall()

        if len(recent) < monitoring_clean_threshold:
            continue

        # Check for N+ consecutive completed runs with DQI above threshold
        consecutive_clean = 0
        for r in recent:
            if r["status"] == "completed" and r["dqi"] is not None and r["dqi"] >= slo_quality:
                consecutive_clean += 1
            else:
                consecutive_clean = 0

        if consecutive_clean < monitoring_clean_threshold:
            continue

        # Resolve the incident
        update_incident(
            conn,
            incident_id,
            status="resolved",
            resolution_type="experiment_fix",
            resolved_at=datetime.now(timezone.utc).isoformat(),
        )
        insert_incident_event(
            conn,
            incident_id,
            event_type="monitoring_closed",
            old_status="monitoring",
            new_status="resolved",
            data={
                "clean_run_count": consecutive_clean,
                "threshold": monitoring_clean_threshold,
                "slo_quality": slo_quality,
            },
        )
        resolved.append({
            "incident_id": incident_id,
            "incident_key": inc["incident_key"],
            "workspace": ws,
            "clean_runs": consecutive_clean,
        })

    return resolved


def check_auto_resolve(
    conn: sqlite3.Connection,
    *,
    fast_window: int = DEFAULT_FAST_WINDOW,
    slo_quality: float = DEFAULT_SLO_QUALITY,
) -> list[dict]:
    """Auto-resolve incidents where the last N runs are all clean.

    Returns list of auto-resolved incidents.
    """
    open_incidents = conn.execute(
        "SELECT * FROM incidents WHERE status = 'detected'"
    ).fetchall()

    resolved = []
    for inc in open_incidents:
        ws = inc["workspace"]

        # Check last N runs for this workspace
        recent = conn.execute("""
            SELECT r.id, r.status, e.score as dqi
            FROM runs r
            LEFT JOIN evaluations e ON e.run_id = r.id AND e.eval_type = 'dqi'
            WHERE r.workspace = ?
            ORDER BY r.started_at DESC
            LIMIT ?
        """, (ws, fast_window)).fetchall()

        if len(recent) < fast_window:
            continue

        # All must be completed
        all_completed = all(r["status"] == "completed" for r in recent)
        if not all_completed:
            continue

        # All DQI scores must be above slo_quality
        dqi_scores = [r["dqi"] for r in recent if r["dqi"] is not None]
        if not dqi_scores or min(dqi_scores) < slo_quality:
            continue

        # Auto-resolve
        update_incident(
            conn,
            inc["id"],
            status="auto_resolved",
            resolution_type="auto_resolved",
            resolved_at=datetime.now(timezone.utc).isoformat(),
        )
        insert_incident_event(
            conn,
            inc["id"],
            event_type="auto_resolved",
            old_status="detected",
            new_status="auto_resolved",
            data={"clean_run_count": fast_window},
        )
        resolved.append({
            "incident_id": inc["id"],
            "incident_key": inc["incident_key"],
            "workspace": ws,
        })

    return resolved
