"""Import existing Claude Code sessions into Qualito.

Scans ~/.claude/projects/ for session JSONL files and imports them
into the local .qualito/qualito.db with tool call tracking, evaluation, and DQI scoring.

Claude Code stores sessions at:
  ~/.claude/projects/-Users-<username>-<project-path>/<uuid>.jsonl

Each JSONL file is one conversation session with events:
  user, assistant, system, file-history-snapshot, last-prompt
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from qualito.core.db import (
    insert_file_activity,
    insert_run,
    insert_tool_calls,
    update_run,
)
from qualito.core.dqi import store_dqi
from qualito.core.evaluator import auto_evaluate
from qualito.core.stream_parser import parse_stream

# Claude Code projects root
CLAUDE_PROJECTS_DIR = Path.home() / ".claude" / "projects"


def _project_dir_to_claude_key(project_dir: Path) -> str:
    """Convert a project directory path to the Claude projects key format.

    /Users/foo/my-project -> -Users-foo-my-project
    """
    return "-" + str(project_dir).replace("/", "-").lstrip("-")


def find_session_files(project_dir: Path | None = None) -> list[Path]:
    """Find Claude Code session JSONL files for a project.

    Args:
        project_dir: Project directory to find sessions for. If None,
                     uses the current working directory.

    Returns:
        Sorted list of JSONL file paths (excludes subagent files).
    """
    if project_dir is None:
        project_dir = Path.cwd()

    project_dir = project_dir.resolve()
    key = _project_dir_to_claude_key(project_dir)
    claude_dir = CLAUDE_PROJECTS_DIR / key

    if not claude_dir.exists():
        return []

    # Only top-level JSONL files (subagent files are in subdirectories)
    files = sorted(claude_dir.glob("*.jsonl"))
    return files


def _aggregate_session_usage(session_path: Path) -> dict:
    """Aggregate token usage and extract metadata from a session JSONL file.

    Claude Code sessions store usage per-assistant-message rather than
    in a final result event. This function sums all usage across messages.

    Returns:
        Dict with input_tokens, output_tokens, cache_read_tokens,
        started_at, completed_at, task (first user prompt).
    """
    total_input = 0
    total_output = 0
    total_cache_read = 0
    first_timestamp = None
    last_timestamp = None
    task = ""
    tool_count = 0

    raw = session_path.read_text().strip()
    if not raw:
        return {}

    for line in raw.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue

        etype = event.get("type", "")
        timestamp = event.get("timestamp", "")

        if timestamp:
            if first_timestamp is None:
                first_timestamp = timestamp
            last_timestamp = timestamp

        if etype == "assistant":
            msg = event.get("message", {})
            usage = msg.get("usage", {})
            if usage:
                total_input += usage.get("input_tokens", 0)
                total_output += usage.get("output_tokens", 0)
                total_cache_read += usage.get("cache_read_input_tokens", 0)

            # Count tool calls
            content = msg.get("content", [])
            for block in content:
                if isinstance(block, dict) and block.get("type") == "tool_use":
                    tool_count += 1

        elif etype == "user" and not task:
            # Capture first real user message as the task description
            msg = event.get("message", {})
            content = msg.get("content", "")
            if isinstance(content, str) and content and not content.startswith("<"):
                task = content[:500]
            elif isinstance(content, list):
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        text = block.get("text", "")
                        if text and not text.startswith("<"):
                            task = text[:500]
                            break
                    elif isinstance(block, str) and not block.startswith("<"):
                        task = block[:500]
                        break

    # Estimate cost from tokens (rough: $15/M input, $75/M output for Opus,
    # cache reads at ~10% of input cost)
    cost_usd = None
    if total_input or total_output:
        cost_usd = (
            (total_input * 15.0 / 1_000_000)
            + (total_output * 75.0 / 1_000_000)
            + (total_cache_read * 1.5 / 1_000_000)
        )
        cost_usd = round(cost_usd, 4)

    # Calculate duration
    duration_ms = None
    if first_timestamp and last_timestamp:
        try:
            t0 = datetime.fromisoformat(first_timestamp.replace("Z", "+00:00"))
            t1 = datetime.fromisoformat(last_timestamp.replace("Z", "+00:00"))
            duration_ms = int((t1 - t0).total_seconds() * 1000)
        except (ValueError, TypeError):
            pass

    return {
        "input_tokens": total_input,
        "output_tokens": total_output,
        "cache_read_tokens": total_cache_read,
        "cost_usd": cost_usd,
        "duration_ms": duration_ms,
        "started_at": first_timestamp or "",
        "completed_at": last_timestamp or "",
        "task": task or "(imported session)",
        "tool_count": tool_count,
    }


def import_session(
    conn: sqlite3.Connection,
    session_path: Path,
    workspace: str,
) -> dict | None:
    """Import a single Claude Code session into the DQI database.

    Args:
        conn: Database connection.
        session_path: Path to the session JSONL file.
        workspace: Workspace name for the run.

    Returns:
        Summary dict {id, tool_calls, cost, dqi} or None if skipped.
    """
    run_id = session_path.stem  # UUID from filename

    # Skip if already imported
    existing = conn.execute(
        "SELECT id FROM runs WHERE id = ?", (run_id,)
    ).fetchone()
    if existing:
        return None

    # Parse tool calls and file activity using existing parser
    parsed = parse_stream(session_path)

    # Aggregate usage from session (parse_stream won't get this from session format)
    meta = _aggregate_session_usage(session_path)
    if not meta:
        return None

    # Skip empty sessions (no tool calls and no meaningful content)
    if not parsed.tool_calls and meta.get("tool_count", 0) == 0:
        return None

    # Insert run
    insert_run(conn, {
        "id": run_id,
        "workspace": workspace,
        "task": meta["task"],
        "task_type": "other",
        "model": "claude-opus-4-6",
        "pipeline_mode": "single",
        "status": "completed",
        "started_at": meta["started_at"],
    })

    # Update with computed fields
    update_run(
        conn,
        run_id,
        completed_at=meta["completed_at"],
        cost_usd=meta["cost_usd"],
        input_tokens=meta["input_tokens"],
        output_tokens=meta["output_tokens"],
        cache_read_tokens=meta["cache_read_tokens"],
        duration_ms=meta["duration_ms"],
        source="import",
    )

    # Insert tool calls and file activity
    if parsed.tool_calls:
        insert_tool_calls(conn, run_id, parsed.tool_calls)
    if parsed.file_activity:
        insert_file_activity(conn, run_id, parsed.file_activity)

    # Auto-evaluate and compute DQI
    auto_evaluate(run_id, conn=conn)
    scores = store_dqi(run_id, conn=conn)

    return {
        "id": run_id,
        "tool_calls": len(parsed.tool_calls),
        "cost": meta["cost_usd"],
        "dqi": scores.get("dqi", 0),
    }


def import_all(
    conn: sqlite3.Connection,
    project_dir: Path | None = None,
    workspace: str = "default",
) -> dict:
    """Import all Claude Code sessions for a project.

    Args:
        conn: Database connection.
        project_dir: Project directory. Defaults to cwd.
        workspace: Workspace name for imported runs.

    Returns:
        Summary dict {imported, skipped, total_cost, avg_dqi}.
    """
    files = find_session_files(project_dir)

    imported = 0
    skipped = 0
    total_cost = 0.0
    dqi_sum = 0.0

    for f in files:
        result = import_session(conn, f, workspace)
        if result is None:
            skipped += 1
        else:
            imported += 1
            total_cost += result.get("cost") or 0
            dqi_sum += result.get("dqi") or 0

    avg_dqi = (dqi_sum / imported) if imported > 0 else 0.0

    return {
        "imported": imported,
        "skipped": skipped,
        "total_cost": round(total_cost, 4),
        "avg_dqi": round(avg_dqi, 4),
    }
