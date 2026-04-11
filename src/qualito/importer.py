"""Import existing Claude Code sessions into Qualito.

Scans ~/.claude/projects/ for session JSONL files and imports them
into the local .qualito/qualito.db with tool call tracking, evaluation, and DQI scoring.

Claude Code stores sessions at:
  ~/.claude/projects/-Users-<username>-<project-path>/<uuid>.jsonl

Each JSONL file is one conversation session with events:
  user, assistant, system, file-history-snapshot, last-prompt, progress, etc.

Session classification (Phase 8):
  - interactive: CLI terminal sessions (real model, human interaction)
  - delegated: SDK/programmatic sessions (model == "<synthetic>")
  - vscode: VS Code extension sessions (entrypoint == "claude-vscode") — skipped
  - unknown: empty/corrupt sessions (<5 events, no model) — skipped
"""

import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path

from sqlalchemy import select

from qualito.core.db import (
    conversations_table,
    insert_conversation,
    insert_file_activity,
    insert_run,
    insert_tool_calls,
    runs_table,
    update_run,
)
from qualito.core.dqi import store_dqi
from qualito.core.evaluator import auto_evaluate
from qualito.core.stream_parser import parse_stream

# Claude Code projects root
CLAUDE_PROJECTS_DIR = Path.home() / ".claude" / "projects"

# ---------------------------------------------------------------------------
# Model-aware pricing (per million tokens)
# ---------------------------------------------------------------------------

MODEL_PRICING = {
    "claude-opus-4-6": {"input": 15.0, "output": 75.0, "cache_read": 1.5},
    "claude-sonnet-4-6": {"input": 3.0, "output": 15.0, "cache_read": 0.3},
    "claude-haiku-4-5-20251001": {"input": 0.8, "output": 4.0, "cache_read": 0.08},
    # Older models (same pricing as their current equivalents)
    "claude-opus-4-5-20251101": {"input": 15.0, "output": 75.0, "cache_read": 1.5},
    "claude-sonnet-4-5-20250929": {"input": 3.0, "output": 15.0, "cache_read": 0.3},
}
DEFAULT_PRICING = MODEL_PRICING["claude-opus-4-6"]


def _calculate_cost(
    model: str, input_tokens: int, output_tokens: int, cache_read: int
) -> float | None:
    """Calculate cost based on model-specific pricing.

    Returns None for <synthetic> sessions (no real API cost).
    """
    if model == "<synthetic>":
        return None
    pricing = MODEL_PRICING.get(model, DEFAULT_PRICING)
    cost = (
        (input_tokens * pricing["input"] / 1_000_000)
        + (output_tokens * pricing["output"] / 1_000_000)
        + (cache_read * pricing["cache_read"] / 1_000_000)
    )
    return round(cost, 4)


# ---------------------------------------------------------------------------
# Session classification
# ---------------------------------------------------------------------------


def _classify_session(
    model: str,
    entrypoint: str,
    has_non_error_assistant: bool,
    event_count: int,
) -> str:
    """Classify a session based on model, entrypoint, and content.

    Returns: "interactive", "delegated", "vscode", or "unknown".
    """
    # VS Code sessions — skip
    if entrypoint == "claude-vscode":
        return "vscode"

    # Delegated: <synthetic> model with real (non-error) assistant messages
    if model == "<synthetic>" and has_non_error_assistant:
        return "delegated"

    # Unknown/corrupt: no model and very few events
    if not model and event_count < 5:
        return "unknown"

    # <synthetic> with only error messages — also unknown
    if model == "<synthetic>" and not has_non_error_assistant:
        return "unknown"

    # Everything else is interactive
    return "interactive"


# ---------------------------------------------------------------------------
# Conversation condensing
# ---------------------------------------------------------------------------

# Pattern to strip system XML tags like <system-reminder>...</system-reminder>
_SYSTEM_TAG_RE = re.compile(r"<[a-z_-]+>.*?</[a-z_-]+>", re.DOTALL)


def _condense_text(text: str) -> str:
    """Strip system XML tags and tool result blocks from text content."""
    if not text:
        return ""
    # Remove system XML tags
    cleaned = _SYSTEM_TAG_RE.sub("", text).strip()
    return cleaned


def _extract_text_from_content(content) -> str:
    """Extract readable text from message content (string or array)."""
    if isinstance(content, str):
        return _condense_text(content)
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(_condense_text(block))
            elif isinstance(block, dict):
                if block.get("type") == "text":
                    parts.append(_condense_text(block.get("text", "")))
                # Skip tool_use and tool_result blocks
        return "\n".join(p for p in parts if p)
    return ""


# ---------------------------------------------------------------------------
# Single-pass metadata extraction
# ---------------------------------------------------------------------------


def _extract_session_metadata(session_path: Path) -> dict | None:
    """Single-pass extraction of all metadata from a Claude Code session JSONL.

    Returns dict with all classification, metadata, and conversation fields,
    or None if the file is empty/unreadable.
    """
    total_input = 0
    total_output = 0
    total_cache_read = 0
    first_timestamp = None
    last_timestamp = None
    task = ""
    tool_count = 0
    event_count = 0

    # Classification fields
    model = ""
    entrypoint = ""
    claude_version = ""
    session_name = ""
    git_branch = ""

    # Subagent tracking
    subagent_count = 0
    subagent_types = []

    # Error tracking
    error_count = 0
    has_non_error_assistant = False

    # File tracking
    files_touched = []
    file_counter = Counter()  # path -> action counts

    # Conversation messages (condensed)
    conversation = []

    try:
        raw = session_path.read_text().strip()
    except (OSError, UnicodeDecodeError):
        return None

    if not raw:
        return None

    for line in raw.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue

        event_count += 1
        etype = event.get("type", "")
        timestamp = event.get("timestamp", "")

        if timestamp:
            if first_timestamp is None:
                first_timestamp = timestamp
            last_timestamp = timestamp

        # Extract enrichment fields from any event that has them
        if not entrypoint and event.get("entrypoint"):
            entrypoint = event["entrypoint"]
        if not claude_version and event.get("version"):
            claude_version = event["version"]
        if not session_name and event.get("slug"):
            session_name = event["slug"]
        if not git_branch and event.get("gitBranch"):
            git_branch = event["gitBranch"]

        # --- User events ---
        if etype == "user":
            msg = event.get("message", {})

            # Skip tool results and meta messages for task extraction
            is_meta = event.get("isMeta", False)
            has_tool_result = "toolUseResult" in event or (
                isinstance(msg.get("content"), list)
                and any(
                    isinstance(b, dict) and b.get("type") == "tool_result"
                    for b in msg.get("content", [])
                )
            )

            # Task extraction: first substantive user message
            if not task and not is_meta and not has_tool_result:
                content = msg.get("content", "")
                if isinstance(content, str):
                    if content and not content.startswith("<") and len(content) >= 20:
                        task = content[:500]
                elif isinstance(content, list):
                    for block in content:
                        if isinstance(block, dict) and block.get("type") == "text":
                            text = block.get("text", "")
                            if text and not text.startswith("<") and len(text) >= 20:
                                task = text[:500]
                                break
                        elif isinstance(block, str):
                            if block and not block.startswith("<") and len(block) >= 20:
                                task = block[:500]
                                break

            # Check for tool use errors in user events (tool results)
            if has_tool_result:
                content = msg.get("content", [])
                if isinstance(content, list):
                    for block in content:
                        if isinstance(block, dict) and block.get("is_error"):
                            error_count += 1

            # Conversation: add user messages (skip tool results and meta)
            if not is_meta and not has_tool_result:
                text = _extract_text_from_content(msg.get("content", ""))
                if text and len(text) >= 10:
                    conversation.append({
                        "role": "user",
                        "content": text[:2000],
                        "timestamp": timestamp,
                    })

        # --- Assistant events ---
        elif etype == "assistant":
            msg = event.get("message", {})
            usage = msg.get("usage", {})

            # Token accumulation
            if usage:
                total_input += usage.get("input_tokens", 0)
                total_output += usage.get("output_tokens", 0)
                total_cache_read += usage.get("cache_read_input_tokens", 0)

            # Model extraction — take first real model
            msg_model = msg.get("model", "")
            if msg_model and not model:
                model = msg_model

            # Check if this is a non-error assistant message
            is_api_error = event.get("isApiErrorMessage", False)
            if msg_model and not is_api_error:
                has_non_error_assistant = True

            # Scan content blocks for tool calls, subagents, files
            content = msg.get("content", [])
            if isinstance(content, list):
                text_parts = []
                for block in content:
                    if not isinstance(block, dict):
                        if isinstance(block, str):
                            text_parts.append(block)
                        continue

                    block_type = block.get("type", "")

                    if block_type == "text":
                        text_parts.append(block.get("text", ""))

                    elif block_type == "tool_use":
                        tool_count += 1
                        name = block.get("name", "")
                        args = block.get("input", {})

                        # Subagent detection
                        if name == "Agent":
                            subagent_count += 1
                            sub_type = args.get("subagent_type", "general-purpose")
                            subagent_types.append(sub_type)

                        # File extraction
                        if name in ("Read", "read_file"):
                            path = args.get("file_path", "")
                            if path:
                                files_touched.append({"path": path, "action": "read"})
                                file_counter[path] += 1
                        elif name in ("Edit", "edit_file"):
                            path = args.get("file_path", "")
                            if path:
                                files_touched.append({"path": path, "action": "edit"})
                                file_counter[path] += 1
                        elif name in ("Write", "write_file"):
                            path = args.get("file_path", "")
                            if path:
                                files_touched.append({"path": path, "action": "write"})
                                file_counter[path] += 1
                        elif name == "Glob":
                            pattern = args.get("pattern", "")
                            if pattern:
                                files_touched.append({"path": pattern, "action": "glob"})

                # Conversation: add assistant text (skip tool_use blocks)
                combined_text = "\n".join(p for p in text_parts if p.strip())
                if combined_text and len(combined_text.strip()) >= 10:
                    conversation.append({
                        "role": "assistant",
                        "content": _condense_text(combined_text)[:2000],
                        "timestamp": timestamp,
                    })

        # --- System events ---
        elif etype == "system":
            subtype = event.get("subtype", "")
            if subtype == "api_error":
                error_count += 1

        # Skip other event types (progress, file-history-snapshot, etc.) gracefully

    # --- Post-processing ---

    if event_count == 0:
        return None

    # Classify session
    session_type = _classify_session(
        model=model,
        entrypoint=entrypoint,
        has_non_error_assistant=has_non_error_assistant,
        event_count=event_count,
    )

    # Calculate cost (model-aware)
    cost_usd = None
    if total_input or total_output:
        cost_usd = _calculate_cost(model, total_input, total_output, total_cache_read)

    # Calculate duration
    duration_ms = None
    if first_timestamp and last_timestamp:
        try:
            t0 = datetime.fromisoformat(first_timestamp.replace("Z", "+00:00"))
            t1 = datetime.fromisoformat(last_timestamp.replace("Z", "+00:00"))
            duration_ms = int((t1 - t0).total_seconds() * 1000)
        except (ValueError, TypeError):
            pass

    # Build files summary (deduplicated with action counts)
    files_summary = {}
    for ft in files_touched:
        path = ft["path"]
        action = ft["action"]
        if path not in files_summary:
            files_summary[path] = {}
        files_summary[path][action] = files_summary[path].get(action, 0) + 1

    return {
        # Existing fields
        "input_tokens": total_input,
        "output_tokens": total_output,
        "cache_read_tokens": total_cache_read,
        "cost_usd": cost_usd,
        "duration_ms": duration_ms,
        "started_at": first_timestamp or "",
        "completed_at": last_timestamp or "",
        "task": task or "(imported session)",
        "tool_count": tool_count,
        # Classification
        "session_type": session_type,
        "entrypoint": entrypoint or None,
        "model": model or None,
        # Metadata
        "claude_version": claude_version or None,
        "session_name": session_name or None,
        "has_subagents": subagent_count > 0,
        "subagent_count": subagent_count,
        "subagent_types": subagent_types,
        "error_count": error_count,
        "git_branch": git_branch or None,
        "files_touched": files_touched,
        "files_summary": files_summary,
        # Event stats
        "event_count": event_count,
        # Conversation
        "conversation": conversation,
    }


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


def import_session(
    conn,
    session_path: Path,
    workspace: str,
) -> dict | None:
    """Import a single Claude Code session into the DQI database.

    Uses single-pass metadata extraction with session classification.
    Skips VS Code sessions and empty/corrupt sessions.

    Args:
        conn: Database connection (SA Connection).
        session_path: Path to the session JSONL file.
        workspace: Workspace name for the run.

    Returns:
        Summary dict {id, tool_calls, cost, dqi, session_type} or None if skipped.
    """
    run_id = session_path.stem  # UUID from filename

    # Skip if already imported
    existing = conn.execute(
        select(runs_table.c.id).where(runs_table.c.id == run_id)
    ).fetchone()
    if existing:
        return None

    # Single-pass metadata extraction
    meta = _extract_session_metadata(session_path)
    if not meta:
        return None

    # Skip vscode and unknown sessions
    if meta["session_type"] in ("vscode", "unknown"):
        return {"id": run_id, "session_type": meta["session_type"], "skipped": True}

    # Skip empty sessions (no tool calls and no meaningful content)
    if meta["tool_count"] == 0 and meta["event_count"] < 5:
        return {"id": run_id, "session_type": "unknown", "skipped": True}

    # Parse tool calls and file activity using existing parser
    parsed = parse_stream(session_path)

    # Insert run with all new fields
    insert_run(conn, {
        "id": run_id,
        "workspace": workspace,
        "task": meta["task"],
        "task_type": "other",
        "model": meta.get("model") or "unknown",
        "pipeline_mode": "single",
        "status": "completed",
        "started_at": meta["started_at"],
        "source": "import",
        "session_type": meta["session_type"],
        "entrypoint": meta.get("entrypoint"),
        "claude_version": meta.get("claude_version"),
        "session_name": meta.get("session_name"),
        "has_subagents": meta.get("has_subagents", False),
        "subagent_count": meta.get("subagent_count", 0),
        "error_count": meta.get("error_count", 0),
        "tool_count": meta.get("tool_count", 0),
        "branch": meta.get("git_branch"),
        "files_changed": json.dumps(meta.get("files_summary")) if meta.get("files_summary") else None,
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
    )

    # Insert tool calls and file activity from stream parser
    if parsed.tool_calls:
        insert_tool_calls(conn, run_id, parsed.tool_calls)
    if parsed.file_activity:
        insert_file_activity(conn, run_id, parsed.file_activity)

    # Store condensed conversation
    if meta.get("conversation"):
        try:
            insert_conversation(conn, run_id, meta["conversation"])
        except Exception:
            pass  # Non-critical — conversation storage failure shouldn't block import

    # Auto-evaluate and compute DQI
    auto_evaluate(run_id, conn=conn)
    scores = store_dqi(run_id, conn=conn)

    return {
        "id": run_id,
        "tool_calls": len(parsed.tool_calls),
        "cost": meta["cost_usd"],
        "dqi": scores.get("dqi", 0),
        "session_type": meta["session_type"],
    }


def _folder_to_display_name(folder_name: str) -> str:
    """Derive a human-readable project name from a Claude projects folder name.

    Algorithm: strip leading dash, replace dashes with path separators,
    take the last path component.

    Examples:
        -Users-mattiapapa-qualito -> qualito
        -home-user-my-project -> my-project  (no, actually: project)
    """
    # Strip leading dash
    name = folder_name.lstrip("-")
    # Replace dashes with path separators
    parts = name.split("-")
    # Reconstruct as path and take last component
    # This is a heuristic — project dirs with hyphens lose those hyphens
    if parts:
        return parts[-1]
    return folder_name


def discover_all_projects(
    claude_projects_dir: Path | None = None,
) -> list[dict]:
    """Scan Claude Code projects directory and return discoverable projects.

    Each project folder in ~/.claude/projects/ corresponds to a workspace
    the user has used Claude Code in. Folder names encode the absolute path
    (e.g. -Users-mattiapapa-qualito for /Users/mattiapapa/qualito).

    Args:
        claude_projects_dir: Override for testing. Defaults to ~/.claude/projects/.

    Returns:
        List of dicts: {name, path, session_count, estimated_cost}.
    """
    if claude_projects_dir is None:
        claude_projects_dir = CLAUDE_PROJECTS_DIR

    if not claude_projects_dir.exists():
        return []

    projects = []
    for entry in sorted(claude_projects_dir.iterdir()):
        if not entry.is_dir():
            continue

        folder_name = entry.name
        display_name = _folder_to_display_name(folder_name)

        # Count session files (top-level JSONL only, exclude subagent dirs)
        session_files = list(entry.glob("*.jsonl"))
        session_count = len(session_files)

        # Quick cost estimate from file sizes (rough: ~$0.01 per KB of JSONL)
        # More accurate: parse a few files. For discovery, just count sessions.
        estimated_cost = None

        projects.append({
            "name": display_name,
            "path": str(entry),
            "folder": folder_name,
            "session_count": session_count,
            "estimated_cost": estimated_cost,
        })

    return projects


def import_project(
    project_key: str,
    workspace_name: str,
    conn,
    date_range: tuple | None = None,
    claude_projects_dir: Path | None = None,
) -> dict:
    """Import sessions from a specific Claude Code project folder into the DB.

    Args:
        project_key: The folder name in ~/.claude/projects/ (e.g. -Users-mattiapapa-qualito).
        workspace_name: Workspace name to assign to imported runs.
        conn: Database connection.
        date_range: Optional (start, end) ISO date strings to filter sessions by modification time.
        claude_projects_dir: Override for testing. Defaults to ~/.claude/projects/.

    Returns:
        Summary dict {imported, skipped, total_cost, avg_dqi, by_type}.
    """
    if claude_projects_dir is None:
        claude_projects_dir = CLAUDE_PROJECTS_DIR

    project_dir = claude_projects_dir / project_key
    if not project_dir.exists():
        return {"imported": 0, "skipped": 0, "total_cost": 0.0, "avg_dqi": 0.0, "by_type": {}}

    session_files = sorted(project_dir.glob("*.jsonl"))

    # Filter by date range if specified
    if date_range:
        start_str, end_str = date_range
        filtered = []
        for f in session_files:
            mtime = datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d")
            if start_str and mtime < start_str:
                continue
            if end_str and mtime > end_str:
                continue
            filtered.append(f)
        session_files = filtered

    imported = 0
    skipped = 0
    total_cost = 0.0
    dqi_sum = 0.0
    type_counts = Counter()

    for f in session_files:
        result = import_session(conn, f, workspace_name)
        if result is None:
            skipped += 1
        elif result.get("skipped"):
            skipped += 1
            type_counts[result.get("session_type", "unknown")] += 1
        else:
            imported += 1
            total_cost += result.get("cost") or 0
            dqi_sum += result.get("dqi") or 0
            type_counts[result.get("session_type", "unknown")] += 1

    avg_dqi = (dqi_sum / imported) if imported > 0 else 0.0

    return {
        "imported": imported,
        "skipped": skipped,
        "total_cost": round(total_cost, 4),
        "avg_dqi": round(avg_dqi, 4),
        "by_type": dict(type_counts),
    }


def import_all(
    conn,
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
        elif result.get("skipped"):
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


def reimport_all(conn, claude_projects_dir: Path | None = None) -> dict:
    """Re-import all sessions with the new extraction logic.

    Deletes existing run records and re-imports from JSONL files.

    Args:
        conn: Database connection.
        claude_projects_dir: Override for testing. Defaults to ~/.claude/projects/.

    Returns:
        Summary dict with counts by session type.
    """
    from qualito.core.db import (
        evaluations_table,
        file_activity_table,
        tool_calls_table,
    )

    if claude_projects_dir is None:
        claude_projects_dir = CLAUDE_PROJECTS_DIR

    projects = discover_all_projects(claude_projects_dir)
    if not projects:
        return {
            "total": 0, "interactive": 0, "delegated": 0,
            "skipped_vscode": 0, "skipped_unknown": 0, "skipped_empty": 0,
        }

    # Collect all session file stems (run IDs) that we'll reimport
    all_session_ids = []
    session_map = {}  # run_id -> (session_path, workspace)
    for p in projects:
        project_dir = claude_projects_dir / p["folder"]
        for f in sorted(project_dir.glob("*.jsonl")):
            run_id = f.stem
            all_session_ids.append(run_id)
            session_map[run_id] = (f, p["name"])

    if not all_session_ids:
        return {
            "total": 0, "interactive": 0, "delegated": 0,
            "skipped_vscode": 0, "skipped_unknown": 0, "skipped_empty": 0,
        }

    # Delete existing records in batches to avoid SQLite limits
    batch_size = 500
    for i in range(0, len(all_session_ids), batch_size):
        batch = all_session_ids[i:i + batch_size]
        # Cascade delete children first
        conn.execute(conversations_table.delete().where(conversations_table.c.run_id.in_(batch)))
        conn.execute(evaluations_table.delete().where(evaluations_table.c.run_id.in_(batch)))
        conn.execute(tool_calls_table.delete().where(tool_calls_table.c.run_id.in_(batch)))
        conn.execute(file_activity_table.delete().where(file_activity_table.c.run_id.in_(batch)))
        conn.execute(runs_table.delete().where(runs_table.c.id.in_(batch)))
    conn.commit()

    # Re-import all sessions
    counts = Counter()
    for run_id, (session_path, workspace) in session_map.items():
        result = import_session(conn, session_path, workspace)
        if result is None:
            counts["skipped_empty"] += 1
        elif result.get("skipped"):
            st = result.get("session_type", "unknown")
            if st == "vscode":
                counts["skipped_vscode"] += 1
            else:
                counts["skipped_unknown"] += 1
        else:
            counts[result.get("session_type", "interactive")] += 1

    return {
        "total": len(all_session_ids),
        "interactive": counts.get("interactive", 0),
        "delegated": counts.get("delegated", 0),
        "skipped_vscode": counts.get("skipped_vscode", 0),
        "skipped_unknown": counts.get("skipped_unknown", 0),
        "skipped_empty": counts.get("skipped_empty", 0),
    }
