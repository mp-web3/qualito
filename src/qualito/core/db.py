"""Database module for Qualito.

SQLAlchemy Core tables and CRUD functions. Supports both SQLite (local dev)
and PostgreSQL (production) via the DATABASE_URL environment variable.
"""

import json
import os
from pathlib import Path

from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    and_,
    case,
    create_engine,
    func,
    or_,
    select,
)
from sqlalchemy import false as sa_false

# ---------------------------------------------------------------------------
# SQLAlchemy Core table definitions
# ---------------------------------------------------------------------------

metadata = MetaData()

runs_table = Table(
    "runs",
    metadata,
    Column("id", String, primary_key=True),
    Column("workspace", String, nullable=False, index=True),
    Column("task", String, nullable=False),
    Column("task_type", String),
    Column("model", String),
    Column("pipeline_mode", String, server_default="single"),
    Column("status", String, index=True),
    Column("summary", String),
    Column("files_changed", String),
    Column("cost_usd", Float),
    Column("input_tokens", Integer),
    Column("output_tokens", Integer),
    Column("cache_read_tokens", Integer),
    Column("duration_ms", Integer),
    Column("branch", String),
    Column("prompt", String),
    Column("original_prompt", String),
    Column("started_at", String, nullable=False),
    Column("completed_at", String),
    Column("researcher_summary", String),
    Column("implementer_summary", String),
    Column("verifier_verdict", String),
    Column("paper_live_gap", Integer),
    Column("skill_name", String),
    Column("source", String, server_default="delegation"),
    Column("prompt_components", String),
    Column("user_id", Integer, ForeignKey("users.id"), index=True),
)

tool_calls_table = Table(
    "tool_calls",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String, ForeignKey("runs.id"), nullable=False, index=True),
    Column("tool_name", String, nullable=False),
    Column("arguments_summary", String),
    Column("result_summary", String),
    Column("is_error", Boolean, server_default=sa_false()),
    Column("phase", String, server_default="single"),
    Column("timestamp", String),
    Column("duration_ms", Integer),
)

file_activity_table = Table(
    "file_activity",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String, ForeignKey("runs.id"), nullable=False, index=True),
    Column("file_path", String, nullable=False),
    Column("action", String, nullable=False),
    Column("timestamp", String),
)

evaluations_table = Table(
    "evaluations",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String, ForeignKey("runs.id"), nullable=False, index=True),
    Column("eval_type", String, nullable=False),
    Column("checks", String),
    Column("score", Float),
    Column("categories", String),
    Column("notes", String),
    Column("created_at", String, server_default=func.now()),
)

artifacts_table = Table(
    "artifacts",
    metadata,
    Column("id", String, primary_key=True),
    Column("run_id", String, ForeignKey("runs.id"), nullable=False, index=True),
    Column("artifact_type", String, nullable=False, index=True),
    Column("title", String, nullable=False),
    Column("content", String),
    Column("content_type", String, server_default="text/markdown"),
    Column("file_path", String),
    Column("metadata", String),
    Column("phase", String),
    Column("workspace", String, index=True),
    Column("created_at", String, nullable=False, server_default=func.now()),
)

baselines_table = Table(
    "baselines",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False),
    Column("description", String),
    Column("window_start", String, nullable=False),
    Column("window_end", String, nullable=False),
    Column("run_count", Integer),
    Column("metrics", String, nullable=False),
    Column("created_at", String, server_default=func.now()),
)

system_changes_table = Table(
    "system_changes",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("change_name", String, nullable=False),
    Column("description", String),
    Column("baseline_id", Integer, ForeignKey("baselines.id")),
    Column("implemented_at", String, nullable=False),
    Column("measurement_window_days", Integer, server_default="10"),
    Column("status", String, server_default="measuring"),
    Column("before_metrics", String),
    Column("after_metrics", String),
    Column("p_improvement", Float),
    Column("effect_size", Float),
    Column("created_at", String, server_default=func.now()),
)

benchmark_suites_table = Table(
    "benchmark_suites",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False, unique=True),
    Column("description", String),
    Column("tasks", String, nullable=False),
    Column("created_at", String, server_default=func.now()),
)

experiments_table = Table(
    "experiments",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False, unique=True),
    Column("description", String),
    Column("suite_id", Integer, ForeignKey("benchmark_suites.id"), nullable=False),
    Column("status", String, server_default="running"),
    Column("run_ids", String),
    Column("avg_dqi", Float),
    Column("per_task_dqi", String),
    Column("config_snapshot", String),
    Column("created_at", String, server_default=func.now()),
    Column("completed_at", String),
)

experiment_comparisons_table = Table(
    "experiment_comparisons",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False),
    Column(
        "before_experiment_id",
        Integer,
        ForeignKey("experiments.id"),
        nullable=False,
    ),
    Column(
        "after_experiment_id",
        Integer,
        ForeignKey("experiments.id"),
        nullable=False,
    ),
    Column("per_task_delta", String),
    Column("wilcoxon_p", Float),
    Column("bayesian_p_improvement", Float),
    Column("effect_size", Float),
    Column("verdict", String),
    Column("created_at", String, server_default=func.now()),
)

incidents_table = Table(
    "incidents",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("incident_key", String, unique=True, nullable=False),
    Column("category", String, nullable=False, index=True),
    Column("severity", String, nullable=False),
    Column("status", String, nullable=False, server_default="detected", index=True),
    Column("workspace", String, nullable=False, index=True),
    Column("task_type", String),
    Column("title", String, nullable=False),
    Column("description", String),
    Column("detection_method", String),
    Column("trigger_metric", String),
    Column("trigger_value", Float),
    Column("baseline_value", Float),
    Column("burn_rate", Float),
    Column("affected_run_ids", String),
    Column("total_affected_runs", Integer, server_default="0"),
    Column("cost_impact_usd", Float, server_default="0"),
    Column("fix_experiment_id", Integer, ForeignKey("experiments.id")),
    Column("fix_description", String),
    Column("resolution_type", String),
    Column("created_at", String, server_default=func.now()),
    Column("confirmed_at", String),
    Column("resolved_at", String),
    Column("time_to_detect_runs", Integer),
    Column("time_to_resolve_runs", Integer),
)

incident_events_table = Table(
    "incident_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "incident_id",
        Integer,
        ForeignKey("incidents.id"),
        nullable=False,
        index=True,
    ),
    Column("event_type", String, nullable=False),
    Column("old_status", String),
    Column("new_status", String),
    Column("data", String),
    Column("created_at", String, server_default=func.now()),
)

users_table = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("email", String, unique=True, nullable=False),
    Column("password_hash", String, nullable=False),
    Column("name", String),
    Column("stripe_customer_id", String),
    Column("plan", String, server_default="free"),
    Column("created_at", String, server_default=func.now()),
    Column("email_verified", Boolean, server_default=sa_false()),
    Column("role", String, server_default="user"),
    Column("suspended", Boolean, server_default=sa_false()),
    Column("stripe_subscription_id", String),
    Column("marketing_consent", Boolean, server_default=sa_false()),
    Column("marketing_consent_date", String),
)

api_keys_table = Table(
    "api_keys",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, ForeignKey("users.id"), nullable=False, index=True),
    Column("key_hash", String, nullable=False, index=True),
    Column("key_prefix", String, nullable=False),
    Column("name", String),
    Column("last_used_at", String),
    Column("created_at", String, server_default=func.now()),
    Column("revoked_at", String),
)

processed_events_table = Table(
    "processed_events",
    metadata,
    Column("event_id", String, primary_key=True),
    Column("event_type", String, nullable=False),
    Column("processed_at", String, server_default=func.now()),
)

consent_table = Table(
    "consent",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, ForeignKey("users.id"), nullable=False, index=True),
    Column("tos_accepted", Boolean, server_default=sa_false()),
    Column("privacy_accepted", Boolean, server_default=sa_false()),
    Column("marketing_opt_in", Boolean, server_default=sa_false()),
    Column("tos_version", String, server_default="v1.0"),
    Column("privacy_version", String, server_default="v1.0"),
    Column("ip_address", String),
    Column("user_agent", String),
    Column("created_at", String, server_default=func.now()),
)

email_logs_table = Table(
    "email_logs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("email_type", String, nullable=False),
    Column("recipient_email", String, nullable=False),
    Column("status", String, nullable=False),
    Column("provider_id", String),
    Column("error_message", String),
    Column("sent_at", String, server_default=func.now()),
)

setup_tokens_table = Table(
    "setup_tokens",
    metadata,
    Column("token", String, primary_key=True),
    Column("user_id", Integer, ForeignKey("users.id"), nullable=False),
    Column("email", String, nullable=False),
    Column("created_at", String, server_default=func.now()),
    Column("expires_at", String, nullable=False),
    Column("used", Boolean, server_default=sa_false()),
)

# ---------------------------------------------------------------------------
# SQLAlchemy engine helpers
# ---------------------------------------------------------------------------


def _resolve_db_path(db_path: Path | None = None) -> Path:
    """Resolve the database file path.

    Priority: explicit path > QUALITO_DIR env var > global ~/.qualito/qualito.db
    Falls back to cwd/.qualito/qualito.db only if it exists (backward compat).
    """
    if db_path:
        return db_path
    qualito_dir = os.environ.get("QUALITO_DIR")
    if qualito_dir:
        return Path(qualito_dir) / "qualito.db"
    # Check local .qualito/ first for backward compat
    local_db = Path.cwd() / ".qualito" / "qualito.db"
    if local_db.exists():
        return local_db
    # Default to global
    return Path.home() / ".qualito" / "qualito.db"


def get_engine(db_url=None):
    """Create SA engine. DATABASE_URL -> PostgreSQL, else -> SQLite."""
    if db_url is None:
        db_url = os.environ.get("DATABASE_URL")
    if db_url and db_url.startswith("postgres"):
        # Railway uses postgres:// but SA needs postgresql://
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        return create_engine(db_url)
    else:
        db_path = _resolve_db_path() if db_url is None else db_url
        return create_engine(f"sqlite:///{db_path}")


def init_db(engine=None):
    """Create all tables via SA. Safe to call multiple times."""
    if engine is None:
        engine = get_engine()
    metadata.create_all(engine)
    return engine


def get_sa_connection(engine=None):
    """Get a SQLAlchemy connection with tables created."""
    if engine is None:
        engine = get_engine()
    init_db(engine)
    return engine.connect()


# ---------------------------------------------------------------------------
# SQLAlchemy Core CRUD functions
# ---------------------------------------------------------------------------


def insert_run(conn, run: dict):
    """Insert a run record."""
    conn.execute(
        runs_table.insert().values(
            id=run["id"],
            workspace=run["workspace"],
            task=run["task"],
            task_type=run.get("task_type"),
            model=run.get("model"),
            pipeline_mode=run.get("pipeline_mode", "single"),
            status=run.get("status", "running"),
            prompt=run.get("prompt"),
            original_prompt=run.get("original_prompt"),
            started_at=run["started_at"],
            skill_name=run.get("skill_name"),
            prompt_components=run.get("prompt_components"),
        )
    )
    conn.commit()


def update_run(conn, run_id: str, **fields):
    """Update run fields by name."""
    if not fields:
        return
    conn.execute(
        runs_table.update().where(runs_table.c.id == run_id).values(**fields)
    )
    conn.commit()


def insert_tool_calls(conn, run_id: str, tool_calls: list):
    """Insert tool call records from parsed stream."""
    for tc in tool_calls:
        conn.execute(
            tool_calls_table.insert().values(
                run_id=run_id,
                tool_name=tc.tool_name,
                arguments_summary=tc.arguments_summary,
                result_summary=tc.result_summary,
                is_error=getattr(tc, "is_error", False),
                phase=tc.phase,
                timestamp=tc.timestamp,
                duration_ms=tc.duration_ms,
            )
        )
    conn.commit()


def insert_file_activity(conn, run_id: str, file_activity: list):
    """Insert file activity records from parsed stream."""
    for fa in file_activity:
        conn.execute(
            file_activity_table.insert().values(
                run_id=run_id,
                file_path=fa.file_path,
                action=fa.action,
                timestamp=fa.timestamp,
            )
        )
    conn.commit()


def insert_evaluation(conn, run_id: str, eval_type: str,
                      checks: dict | None = None, score: float | None = None,
                      categories: dict | None = None, notes: str | None = None):
    """Insert an evaluation record."""
    conn.execute(
        evaluations_table.insert().values(
            run_id=run_id,
            eval_type=eval_type,
            checks=json.dumps(checks) if checks else None,
            score=score,
            categories=json.dumps(categories) if categories else None,
            notes=notes,
        )
    )
    conn.commit()


def insert_artifact(conn, artifact: dict):
    """Insert an artifact record."""
    conn.execute(
        artifacts_table.insert().values(**{
            "id": artifact["id"],
            "run_id": artifact["run_id"],
            "artifact_type": artifact["artifact_type"],
            "title": artifact["title"],
            "content": artifact.get("content"),
            "content_type": artifact.get("content_type", "text/markdown"),
            "file_path": artifact.get("file_path"),
            "metadata": artifact.get("metadata"),
            "phase": artifact.get("phase"),
            "workspace": artifact.get("workspace"),
        })
    )
    conn.commit()


def get_artifacts(conn, run_id: str | None = None,
                  artifact_type: str | None = None, workspace: str | None = None,
                  q: str | None = None, limit: int = 50) -> list[dict]:
    """Query artifacts with optional filters."""
    conditions = []
    if run_id:
        conditions.append(artifacts_table.c.run_id == run_id)
    if artifact_type:
        conditions.append(artifacts_table.c.artifact_type == artifact_type)
    if workspace:
        conditions.append(artifacts_table.c.workspace == workspace)
    if q:
        conditions.append(
            or_(
                artifacts_table.c.title.ilike(f"%{q}%"),
                artifacts_table.c.content.ilike(f"%{q}%"),
            )
        )
    stmt = select(artifacts_table)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    stmt = stmt.order_by(artifacts_table.c.created_at.desc()).limit(limit)
    rows = conn.execute(stmt).mappings().fetchall()
    return [dict(r) for r in rows]


def get_run(conn, run_id: str) -> dict | None:
    """Get a single run with all related data."""
    row = conn.execute(
        select(runs_table).where(runs_table.c.id == run_id)
    ).mappings().fetchone()
    if not row:
        return None
    run = dict(row)

    tcs = conn.execute(
        select(tool_calls_table)
        .where(tool_calls_table.c.run_id == run_id)
        .order_by(tool_calls_table.c.id)
    ).mappings().fetchall()
    run["tool_calls"] = [dict(r) for r in tcs]

    fas = conn.execute(
        select(file_activity_table)
        .where(file_activity_table.c.run_id == run_id)
        .order_by(file_activity_table.c.id)
    ).mappings().fetchall()
    run["file_activity"] = [dict(r) for r in fas]

    evals = conn.execute(
        select(evaluations_table)
        .where(evaluations_table.c.run_id == run_id)
        .order_by(evaluations_table.c.id)
    ).mappings().fetchall()
    run["evaluations"] = [dict(r) for r in evals]

    return run


def get_metrics(conn, workspace: str | None = None,
                task_type: str | None = None, since: str | None = None) -> dict:
    """Compute aggregate metrics with optional filters."""
    conditions = []
    if workspace:
        conditions.append(runs_table.c.workspace == workspace)
    if task_type:
        conditions.append(runs_table.c.task_type == task_type)
    if since:
        conditions.append(runs_table.c.started_at >= since)

    # Overall stats
    stmt = select(
        func.count().label("total"),
        func.sum(case((runs_table.c.status == "completed", 1), else_=0)).label("completed"),
        func.sum(case((runs_table.c.status == "failed", 1), else_=0)).label("failed"),
        func.avg(runs_table.c.cost_usd).label("avg_cost"),
        func.sum(runs_table.c.cost_usd).label("total_cost"),
        func.avg(runs_table.c.duration_ms).label("avg_duration"),
    ).select_from(runs_table)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    row = conn.execute(stmt).mappings().fetchone()

    # Average eval score
    eval_conditions = [evaluations_table.c.eval_type == "auto"]
    if workspace:
        eval_conditions.append(runs_table.c.workspace == workspace)
    if task_type:
        eval_conditions.append(runs_table.c.task_type == task_type)
    if since:
        eval_conditions.append(runs_table.c.started_at >= since)

    eval_stmt = (
        select(func.avg(evaluations_table.c.score).label("avg_score"))
        .select_from(
            evaluations_table.join(runs_table, evaluations_table.c.run_id == runs_table.c.id)
        )
        .where(and_(*eval_conditions))
    )
    eval_row = conn.execute(eval_stmt).mappings().fetchone()

    # By task type
    type_stmt = select(
        runs_table.c.task_type,
        func.count().label("count"),
        func.sum(case((runs_table.c.status == "completed", 1), else_=0)).label("ok"),
        func.avg(runs_table.c.cost_usd).label("avg_cost"),
        func.sum(runs_table.c.cost_usd).label("total_cost"),
    ).select_from(runs_table)
    if conditions:
        type_stmt = type_stmt.where(and_(*conditions))
    type_stmt = type_stmt.group_by(runs_table.c.task_type).order_by(func.count().desc())
    type_rows = conn.execute(type_stmt).mappings().fetchall()

    # By workspace
    ws_stmt = select(
        runs_table.c.workspace,
        func.count().label("count"),
        func.sum(case((runs_table.c.status == "completed", 1), else_=0)).label("ok"),
        func.sum(runs_table.c.cost_usd).label("total_cost"),
    ).select_from(runs_table)
    if conditions:
        ws_stmt = ws_stmt.where(and_(*conditions))
    ws_stmt = ws_stmt.group_by(runs_table.c.workspace).order_by(func.count().desc())
    ws_rows = conn.execute(ws_stmt).mappings().fetchall()

    return {
        "total": dict(row),
        "avg_score": eval_row["avg_score"] if eval_row else None,
        "by_task_type": [dict(r) for r in type_rows],
        "by_workspace": [dict(r) for r in ws_rows],
    }


# --- Incident CRUD ---


def insert_incident(conn, incident: dict) -> int:
    """Insert an incident record. Returns the new incident id."""
    cols = [
        "incident_key", "category", "severity", "status", "workspace",
        "task_type", "title", "description", "detection_method",
        "trigger_metric", "trigger_value", "baseline_value", "burn_rate",
        "affected_run_ids", "total_affected_runs", "cost_impact_usd",
        "fix_experiment_id", "fix_description", "resolution_type",
        "confirmed_at", "resolved_at", "time_to_detect_runs",
        "time_to_resolve_runs",
    ]
    values = {}
    for c in cols:
        if c in incident:
            v = incident[c]
            if c == "affected_run_ids" and isinstance(v, list):
                v = json.dumps(v)
            values[c] = v
    result = conn.execute(incidents_table.insert().values(**values))
    conn.commit()
    return result.inserted_primary_key[0]


def update_incident(conn, incident_id: int, **fields):
    """Update incident fields by name."""
    if not fields:
        return
    if "affected_run_ids" in fields and isinstance(fields["affected_run_ids"], list):
        fields["affected_run_ids"] = json.dumps(fields["affected_run_ids"])
    conn.execute(
        incidents_table.update()
        .where(incidents_table.c.id == incident_id)
        .values(**fields)
    )
    conn.commit()


def insert_incident_event(conn, incident_id: int,
                          event_type: str, old_status: str | None = None,
                          new_status: str | None = None,
                          data: dict | None = None):
    """Insert an incident event record."""
    conn.execute(
        incident_events_table.insert().values(
            incident_id=incident_id,
            event_type=event_type,
            old_status=old_status,
            new_status=new_status,
            data=json.dumps(data) if data else None,
        )
    )
    conn.commit()


def get_incident(conn, incident_id: int) -> dict | None:
    """Get a single incident with all events attached."""
    row = conn.execute(
        select(incidents_table).where(incidents_table.c.id == incident_id)
    ).mappings().fetchone()
    if not row:
        return None
    incident = dict(row)

    events = conn.execute(
        select(incident_events_table)
        .where(incident_events_table.c.incident_id == incident_id)
        .order_by(incident_events_table.c.id)
    ).mappings().fetchall()
    incident["events"] = [dict(r) for r in events]

    return incident


def get_incidents(conn, workspace: str | None = None,
                  status: str | None = None, category: str | None = None,
                  severity: str | None = None, since: str | None = None,
                  limit: int = 50) -> list[dict]:
    """Query incidents with optional filters."""
    conditions = []
    if workspace:
        conditions.append(incidents_table.c.workspace == workspace)
    if status:
        conditions.append(incidents_table.c.status == status)
    if category:
        conditions.append(incidents_table.c.category == category)
    if severity:
        conditions.append(incidents_table.c.severity == severity)
    if since:
        conditions.append(incidents_table.c.created_at >= since)

    stmt = select(incidents_table)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    stmt = stmt.order_by(incidents_table.c.created_at.desc()).limit(limit)

    rows = conn.execute(stmt).mappings().fetchall()
    return [dict(r) for r in rows]
