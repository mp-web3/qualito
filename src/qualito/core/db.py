"""Database module for Qualito.

Single SQLite file per project at .qualito/qualito.db with tables:
runs, tool_calls, file_activity, evaluations, artifacts,
baselines, system_changes, benchmark_suites, experiments, experiment_comparisons.
"""

import json
import os
import sqlite3
from pathlib import Path

from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    func,
)
from sqlalchemy import false as sa_false

# ---------------------------------------------------------------------------
# SQLAlchemy Core table definitions (Phase 1 — PostgreSQL migration)
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

# ---------------------------------------------------------------------------
# SQLAlchemy engine helpers
# ---------------------------------------------------------------------------


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
    """Get a SQLAlchemy connection."""
    if engine is None:
        engine = get_engine()
    return engine.connect()


# ---------------------------------------------------------------------------
# Legacy SQLite schema + raw-SQL functions (unchanged — will migrate in Phase 2+)
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    workspace TEXT NOT NULL,
    task TEXT NOT NULL,
    task_type TEXT,
    model TEXT,
    pipeline_mode TEXT DEFAULT 'single',
    status TEXT,
    summary TEXT,
    files_changed TEXT,
    cost_usd REAL,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cache_read_tokens INTEGER,
    duration_ms INTEGER,
    branch TEXT,
    prompt TEXT,
    original_prompt TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    researcher_summary TEXT,
    implementer_summary TEXT,
    verifier_verdict TEXT,
    paper_live_gap INTEGER,
    skill_name TEXT,
    source TEXT DEFAULT 'delegation',
    prompt_components TEXT
);

CREATE TABLE IF NOT EXISTS tool_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id),
    tool_name TEXT NOT NULL,
    arguments_summary TEXT,
    result_summary TEXT,
    is_error BOOLEAN DEFAULT FALSE,
    phase TEXT DEFAULT 'single',
    timestamp TEXT,
    duration_ms INTEGER
);

CREATE TABLE IF NOT EXISTS file_activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id),
    file_path TEXT NOT NULL,
    action TEXT NOT NULL,
    timestamp TEXT
);

CREATE TABLE IF NOT EXISTS evaluations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id),
    eval_type TEXT NOT NULL,
    checks TEXT,
    score REAL,
    categories TEXT,
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    artifact_type TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    content_type TEXT DEFAULT 'text/markdown',
    file_path TEXT,
    metadata TEXT,
    phase TEXT,
    workspace TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_tool_calls_run ON tool_calls(run_id);
CREATE INDEX IF NOT EXISTS idx_file_activity_run ON file_activity(run_id);
CREATE INDEX IF NOT EXISTS idx_evaluations_run ON evaluations(run_id);
CREATE INDEX IF NOT EXISTS idx_runs_workspace ON runs(workspace);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_artifacts_run ON artifacts(run_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_type ON artifacts(artifact_type);
CREATE INDEX IF NOT EXISTS idx_artifacts_workspace ON artifacts(workspace);

CREATE TABLE IF NOT EXISTS baselines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    run_count INTEGER,
    metrics TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS system_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    change_name TEXT NOT NULL,
    description TEXT,
    baseline_id INTEGER REFERENCES baselines(id),
    implemented_at TEXT NOT NULL,
    measurement_window_days INTEGER DEFAULT 10,
    status TEXT DEFAULT 'measuring',
    before_metrics TEXT,
    after_metrics TEXT,
    p_improvement REAL,
    effect_size REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS benchmark_suites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    tasks TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS experiments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    suite_id INTEGER NOT NULL REFERENCES benchmark_suites(id),
    status TEXT DEFAULT 'running',
    run_ids TEXT,
    avg_dqi REAL,
    per_task_dqi TEXT,
    config_snapshot TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS experiment_comparisons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    before_experiment_id INTEGER NOT NULL REFERENCES experiments(id),
    after_experiment_id INTEGER NOT NULL REFERENCES experiments(id),
    per_task_delta TEXT,
    wilcoxon_p REAL,
    bayesian_p_improvement REAL,
    effect_size REAL,
    verdict TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS incidents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_key TEXT UNIQUE NOT NULL,
    category TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'detected',
    workspace TEXT NOT NULL,
    task_type TEXT,
    title TEXT NOT NULL,
    description TEXT,
    detection_method TEXT,
    trigger_metric TEXT,
    trigger_value REAL,
    baseline_value REAL,
    burn_rate REAL,
    affected_run_ids TEXT,
    total_affected_runs INTEGER DEFAULT 0,
    cost_impact_usd REAL DEFAULT 0,
    fix_experiment_id INTEGER REFERENCES experiments(id),
    fix_description TEXT,
    resolution_type TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    confirmed_at TEXT,
    resolved_at TEXT,
    time_to_detect_runs INTEGER,
    time_to_resolve_runs INTEGER
);

CREATE TABLE IF NOT EXISTS incident_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id INTEGER NOT NULL REFERENCES incidents(id),
    event_type TEXT NOT NULL,
    old_status TEXT,
    new_status TEXT,
    data TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_incidents_status ON incidents(status);
CREATE INDEX IF NOT EXISTS idx_incidents_workspace ON incidents(workspace);
CREATE INDEX IF NOT EXISTS idx_incidents_category ON incidents(category);
CREATE INDEX IF NOT EXISTS idx_incident_events_incident ON incident_events(incident_id);
"""


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


def get_db(db_path: Path | None = None) -> sqlite3.Connection:
    """Get a database connection, creating schema if needed.

    Args:
        db_path: Explicit path to the SQLite file. If None, resolves via
                 QUALITO_DIR env var or defaults to .qualito/qualito.db in cwd.
    """
    resolved = _resolve_db_path(db_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(resolved))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def insert_run(conn: sqlite3.Connection, run: dict):
    """Insert a run record."""
    conn.execute(
        """INSERT INTO runs (id, workspace, task, task_type, model, pipeline_mode,
           status, prompt, original_prompt, started_at, skill_name, prompt_components)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            run["id"], run["workspace"], run["task"], run.get("task_type"),
            run.get("model"), run.get("pipeline_mode", "single"),
            run.get("status", "running"), run.get("prompt"),
            run.get("original_prompt"), run["started_at"],
            run.get("skill_name"), run.get("prompt_components"),
        ),
    )
    conn.commit()


def update_run(conn: sqlite3.Connection, run_id: str, **fields):
    """Update run fields by name."""
    if not fields:
        return
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values())
    values.append(run_id)
    conn.execute(f"UPDATE runs SET {set_clause} WHERE id = ?", values)
    conn.commit()


def insert_tool_calls(conn: sqlite3.Connection, run_id: str, tool_calls: list):
    """Insert tool call records from parsed stream."""
    for tc in tool_calls:
        conn.execute(
            """INSERT INTO tool_calls (run_id, tool_name, arguments_summary,
               result_summary, is_error, phase, timestamp, duration_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run_id, tc.tool_name, tc.arguments_summary,
                tc.result_summary, getattr(tc, 'is_error', False),
                tc.phase, tc.timestamp, tc.duration_ms,
            ),
        )
    conn.commit()


def insert_file_activity(conn: sqlite3.Connection, run_id: str, file_activity: list):
    """Insert file activity records from parsed stream."""
    for fa in file_activity:
        conn.execute(
            """INSERT INTO file_activity (run_id, file_path, action, timestamp)
               VALUES (?, ?, ?, ?)""",
            (run_id, fa.file_path, fa.action, fa.timestamp),
        )
    conn.commit()


def insert_evaluation(conn: sqlite3.Connection, run_id: str, eval_type: str,
                      checks: dict | None = None, score: float | None = None,
                      categories: dict | None = None, notes: str | None = None):
    """Insert an evaluation record."""
    conn.execute(
        """INSERT INTO evaluations (run_id, eval_type, checks, score, categories, notes)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            run_id, eval_type,
            json.dumps(checks) if checks else None,
            score,
            json.dumps(categories) if categories else None,
            notes,
        ),
    )
    conn.commit()


def insert_artifact(conn: sqlite3.Connection, artifact: dict):
    """Insert an artifact record."""
    conn.execute(
        """INSERT INTO artifacts (id, run_id, artifact_type, title, content,
           content_type, file_path, metadata, phase, workspace)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            artifact["id"], artifact["run_id"], artifact["artifact_type"],
            artifact["title"], artifact.get("content"),
            artifact.get("content_type", "text/markdown"),
            artifact.get("file_path"), artifact.get("metadata"),
            artifact.get("phase"), artifact.get("workspace"),
        ),
    )
    conn.commit()


def get_artifacts(conn: sqlite3.Connection, run_id: str | None = None,
                  artifact_type: str | None = None, workspace: str | None = None,
                  q: str | None = None, limit: int = 50) -> list[dict]:
    """Query artifacts with optional filters."""
    where_parts = []
    params = []
    if run_id:
        where_parts.append("run_id = ?")
        params.append(run_id)
    if artifact_type:
        where_parts.append("artifact_type = ?")
        params.append(artifact_type)
    if workspace:
        where_parts.append("workspace = ?")
        params.append(workspace)
    if q:
        where_parts.append(
            "(title LIKE '%' || ? || '%' OR content LIKE '%' || ? || '%') COLLATE NOCASE"
        )
        params.extend([q, q])

    where = "WHERE " + " AND ".join(where_parts) if where_parts else ""
    rows = conn.execute(
        f"SELECT * FROM artifacts {where} ORDER BY created_at DESC LIMIT ?",
        params + [limit],
    ).fetchall()
    return [dict(r) for r in rows]


def get_run(conn: sqlite3.Connection, run_id: str) -> dict | None:
    """Get a single run with all related data."""
    row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if not row:
        return None
    run = dict(row)

    # Attach tool calls
    tcs = conn.execute(
        "SELECT * FROM tool_calls WHERE run_id = ? ORDER BY id", (run_id,)
    ).fetchall()
    run["tool_calls"] = [dict(r) for r in tcs]

    # Attach file activity
    fas = conn.execute(
        "SELECT * FROM file_activity WHERE run_id = ? ORDER BY id", (run_id,)
    ).fetchall()
    run["file_activity"] = [dict(r) for r in fas]

    # Attach evaluations
    evals = conn.execute(
        "SELECT * FROM evaluations WHERE run_id = ? ORDER BY id", (run_id,)
    ).fetchall()
    run["evaluations"] = [dict(r) for r in evals]

    return run


def get_metrics(conn: sqlite3.Connection, workspace: str | None = None,
                task_type: str | None = None, since: str | None = None) -> dict:
    """Compute aggregate metrics with optional filters."""
    where_parts = []
    params = []
    if workspace:
        where_parts.append("workspace = ?")
        params.append(workspace)
    if task_type:
        where_parts.append("task_type = ?")
        params.append(task_type)
    if since:
        where_parts.append("started_at >= ?")
        params.append(since)

    where = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    # Overall stats
    row = conn.execute(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
            AVG(cost_usd) as avg_cost,
            SUM(cost_usd) as total_cost,
            AVG(duration_ms) as avg_duration
        FROM runs {where}
    """, params).fetchone()

    # Average eval score
    eval_row = conn.execute(f"""
        SELECT AVG(e.score) as avg_score
        FROM evaluations e
        JOIN runs r ON r.id = e.run_id
        {where.replace('workspace', 'r.workspace').replace('task_type', 'r.task_type').replace('started_at', 'r.started_at')}
        AND e.eval_type = 'auto'
    """, params).fetchone()

    # By task type
    type_rows = conn.execute(f"""
        SELECT task_type,
            COUNT(*) as count,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as ok,
            AVG(cost_usd) as avg_cost,
            SUM(cost_usd) as total_cost
        FROM runs {where}
        GROUP BY task_type
        ORDER BY count DESC
    """, params).fetchall()

    # By workspace
    ws_rows = conn.execute(f"""
        SELECT workspace,
            COUNT(*) as count,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as ok,
            SUM(cost_usd) as total_cost
        FROM runs {where}
        GROUP BY workspace
        ORDER BY count DESC
    """, params).fetchall()

    return {
        "total": dict(row),
        "avg_score": eval_row["avg_score"] if eval_row else None,
        "by_task_type": [dict(r) for r in type_rows],
        "by_workspace": [dict(r) for r in ws_rows],
    }


# --- Incident CRUD ---


def insert_incident(conn: sqlite3.Connection, incident: dict) -> int:
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
    present = [c for c in cols if c in incident]
    placeholders = ", ".join("?" for _ in present)
    col_names = ", ".join(present)
    values = []
    for c in present:
        v = incident[c]
        if c == "affected_run_ids" and isinstance(v, list):
            v = json.dumps(v)
        values.append(v)
    cur = conn.execute(
        f"INSERT INTO incidents ({col_names}) VALUES ({placeholders})",
        values,
    )
    conn.commit()
    return cur.lastrowid


def update_incident(conn: sqlite3.Connection, incident_id: int, **fields):
    """Update incident fields by name."""
    if not fields:
        return
    if "affected_run_ids" in fields and isinstance(fields["affected_run_ids"], list):
        fields["affected_run_ids"] = json.dumps(fields["affected_run_ids"])
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values())
    values.append(incident_id)
    conn.execute(f"UPDATE incidents SET {set_clause} WHERE id = ?", values)
    conn.commit()


def insert_incident_event(conn: sqlite3.Connection, incident_id: int,
                          event_type: str, old_status: str | None = None,
                          new_status: str | None = None,
                          data: dict | None = None):
    """Insert an incident event record."""
    conn.execute(
        """INSERT INTO incident_events (incident_id, event_type, old_status,
           new_status, data) VALUES (?, ?, ?, ?, ?)""",
        (
            incident_id, event_type, old_status, new_status,
            json.dumps(data) if data else None,
        ),
    )
    conn.commit()


def get_incident(conn: sqlite3.Connection, incident_id: int) -> dict | None:
    """Get a single incident with all events attached."""
    row = conn.execute(
        "SELECT * FROM incidents WHERE id = ?", (incident_id,)
    ).fetchone()
    if not row:
        return None
    incident = dict(row)

    # Attach events
    events = conn.execute(
        "SELECT * FROM incident_events WHERE incident_id = ? ORDER BY id",
        (incident_id,),
    ).fetchall()
    incident["events"] = [dict(r) for r in events]

    return incident


def get_incidents(conn: sqlite3.Connection, workspace: str | None = None,
                  status: str | None = None, category: str | None = None,
                  severity: str | None = None, since: str | None = None,
                  limit: int = 50) -> list[dict]:
    """Query incidents with optional filters."""
    where_parts = []
    params = []
    if workspace:
        where_parts.append("workspace = ?")
        params.append(workspace)
    if status:
        where_parts.append("status = ?")
        params.append(status)
    if category:
        where_parts.append("category = ?")
        params.append(category)
    if severity:
        where_parts.append("severity = ?")
        params.append(severity)
    if since:
        where_parts.append("created_at >= ?")
        params.append(since)

    where = "WHERE " + " AND ".join(where_parts) if where_parts else ""
    rows = conn.execute(
        f"SELECT * FROM incidents {where} ORDER BY created_at DESC LIMIT ?",
        params + [limit],
    ).fetchall()
    return [dict(r) for r in rows]
