"""Tests for global ~/.qualito/ support and cross-project discovery."""

import json
import sqlite3
from pathlib import Path

from qualito.config import QualityConfig, get_global_dir, init_project, load_config
from qualito.importer import (
    _folder_to_display_name,
    discover_all_projects,
    import_project,
    import_session,
)


# ---------------------------------------------------------------------------
# get_global_dir
# ---------------------------------------------------------------------------


def test_get_global_dir_creates_dir(tmp_path, monkeypatch):
    """get_global_dir creates ~/.qualito/ if missing."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    result = get_global_dir()
    assert result == tmp_path / ".qualito"
    assert result.is_dir()


def test_get_global_dir_idempotent(tmp_path, monkeypatch):
    """Calling get_global_dir twice doesn't fail."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    get_global_dir()
    result = get_global_dir()
    assert result.is_dir()


# ---------------------------------------------------------------------------
# Global config defaults
# ---------------------------------------------------------------------------


def test_quality_config_default_db_is_global(tmp_path, monkeypatch):
    """Default QualityConfig.db_path points to ~/.qualito/qualito.db."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    config = QualityConfig()
    assert config.db_path == tmp_path / ".qualito" / "qualito.db"


def test_load_config_global_toml(tmp_path, monkeypatch):
    """load_config reads global ~/.qualito/config.toml."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    global_dir = tmp_path / ".qualito"
    global_dir.mkdir()
    (global_dir / "config.toml").write_text('slo_cost = 10.00\n')

    config = load_config(project_dir=tmp_path / "some-project")
    assert config.slo_cost == 10.00


def test_load_config_project_overrides_global(tmp_path, monkeypatch):
    """Project config overrides global config."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    # Global config
    global_dir = tmp_path / ".qualito"
    global_dir.mkdir()
    (global_dir / "config.toml").write_text('slo_cost = 10.00\nslo_quality = 0.80\n')

    # Project config
    project_dir = tmp_path / "my-project"
    project_dir.mkdir()
    qualito_dir = project_dir / ".qualito"
    qualito_dir.mkdir()
    (qualito_dir / "config.toml").write_text('slo_cost = 5.00\n')

    config = load_config(project_dir=project_dir)
    assert config.slo_cost == 5.00       # project override
    assert config.slo_quality == 0.80    # from global


# ---------------------------------------------------------------------------
# init_project (global vs local)
# ---------------------------------------------------------------------------


def test_init_project_global_mode(tmp_path, monkeypatch):
    """init_project() defaults to creating ~/.qualito/."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    project_dir = tmp_path / "my-project"
    project_dir.mkdir()

    config, qualito_dir = init_project(project_dir=project_dir)

    assert qualito_dir == tmp_path / ".qualito"
    assert qualito_dir.is_dir()
    assert (qualito_dir / "config.toml").exists()
    assert config.db_path.is_absolute()


def test_init_project_local_mode(tmp_path, monkeypatch):
    """init_project(local=True) creates .qualito/ in the project dir."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    project_dir = tmp_path / "my-project"
    project_dir.mkdir()

    config, qualito_dir = init_project(project_dir=project_dir, local=True)

    assert qualito_dir == project_dir / ".qualito"
    assert qualito_dir.is_dir()
    assert (qualito_dir / "config.toml").exists()


# ---------------------------------------------------------------------------
# discover_all_projects
# ---------------------------------------------------------------------------


def test_discover_all_projects_empty(tmp_path):
    """discover_all_projects returns [] when directory doesn't exist."""
    result = discover_all_projects(claude_projects_dir=tmp_path / "nonexistent")
    assert result == []


def test_discover_all_projects_no_folders(tmp_path):
    """discover_all_projects returns [] when directory is empty."""
    result = discover_all_projects(claude_projects_dir=tmp_path)
    assert result == []


def test_discover_all_projects_finds_projects(tmp_path):
    """discover_all_projects scans folders and counts sessions."""
    # Create mock project folders
    p1 = tmp_path / "-Users-alice-project-a"
    p1.mkdir()
    (p1 / "session1.jsonl").write_text('{"type":"user"}\n')
    (p1 / "session2.jsonl").write_text('{"type":"user"}\n')

    p2 = tmp_path / "-Users-alice-project-b"
    p2.mkdir()
    (p2 / "session1.jsonl").write_text('{"type":"user"}\n')

    # Empty project
    p3 = tmp_path / "-home-bob-empty"
    p3.mkdir()

    # Non-directory file (should be ignored)
    (tmp_path / "some-file.txt").write_text("ignore")

    result = discover_all_projects(claude_projects_dir=tmp_path)
    assert len(result) == 3

    # Check sorted order (alphabetical by folder name)
    names = [p["name"] for p in result]
    assert names == ["a", "b", "empty"]

    # Check session counts
    by_name = {p["folder"]: p for p in result}
    assert by_name["-Users-alice-project-a"]["session_count"] == 2
    assert by_name["-Users-alice-project-b"]["session_count"] == 1
    assert by_name["-home-bob-empty"]["session_count"] == 0


def test_discover_ignores_subagent_dirs(tmp_path):
    """discover_all_projects only counts top-level JSONL, not subagent dirs."""
    p1 = tmp_path / "-Users-alice-myproject"
    p1.mkdir()
    (p1 / "main-session.jsonl").write_text('{"type":"user"}\n')
    subdir = p1 / "subagent-abc123"
    subdir.mkdir()
    (subdir / "sub-session.jsonl").write_text('{"type":"user"}\n')

    result = discover_all_projects(claude_projects_dir=tmp_path)
    assert len(result) == 1
    assert result[0]["session_count"] == 1  # only top-level


# ---------------------------------------------------------------------------
# _folder_to_display_name
# ---------------------------------------------------------------------------


def test_folder_to_display_name_macos():
    assert _folder_to_display_name("-Users-mattiapapa-qualito") == "qualito"


def test_folder_to_display_name_linux():
    assert _folder_to_display_name("-home-user-my-project") == "project"


def test_folder_to_display_name_windows():
    assert _folder_to_display_name("D-code-myapp") == "myapp"


# ---------------------------------------------------------------------------
# Idempotent imports
# ---------------------------------------------------------------------------


def _make_session_jsonl(path: Path, session_id: str, task: str = "This is a test task with enough characters for extraction"):
    """Create a minimal valid session JSONL file."""
    events = [
        {
            "type": "user",
            "timestamp": "2024-01-01T10:00:00Z",
            "sessionId": session_id,
            "message": {"role": "user", "content": task},
        },
        {
            "type": "assistant",
            "timestamp": "2024-01-01T10:00:30Z",
            "sessionId": session_id,
            "message": {
                "role": "assistant",
                "model": "claude-sonnet-4-6",
                "content": [{"type": "text", "text": "I'll help you with that task."}],
                "usage": {"input_tokens": 100, "output_tokens": 50, "cache_read_input_tokens": 0},
            },
        },
        {
            "type": "assistant",
            "timestamp": "2024-01-01T10:00:45Z",
            "sessionId": session_id,
            "message": {
                "role": "assistant",
                "model": "claude-sonnet-4-6",
                "content": [{"type": "tool_use", "id": "tu_1", "name": "Read", "input": {"file_path": "/tmp/test.py"}}],
                "usage": {"input_tokens": 50, "output_tokens": 30, "cache_read_input_tokens": 0},
            },
        },
        {
            "type": "user",
            "timestamp": "2024-01-01T10:00:50Z",
            "sessionId": session_id,
            "message": {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "tu_1", "content": "file contents"}]},
        },
        {
            "type": "assistant",
            "timestamp": "2024-01-01T10:01:00Z",
            "sessionId": session_id,
            "message": {
                "role": "assistant",
                "model": "claude-sonnet-4-6",
                "content": [{"type": "text", "text": "I've read the file and completed the task."}],
                "usage": {"input_tokens": 80, "output_tokens": 40, "cache_read_input_tokens": 0},
            },
        },
    ]
    lines = [json.dumps(e) for e in events]
    path.write_text("\n".join(lines) + "\n")


def test_idempotent_import(tmp_path):
    """Importing the same session twice produces no duplicates."""
    from sqlalchemy import func, select
    from qualito.core.db import get_engine, get_sa_connection, runs_table

    db_path = tmp_path / "test.db"
    engine = get_engine(str(db_path))
    conn = get_sa_connection(engine)

    session_file = tmp_path / "abc12345-6789-0000-0000-000000000001.jsonl"
    _make_session_jsonl(session_file, "abc12345-6789-0000-0000-000000000001")

    # First import
    result1 = import_session(conn, session_file, "test-workspace")
    assert result1 is not None
    assert result1["id"] == "abc12345-6789-0000-0000-000000000001"

    # Second import — should be skipped
    result2 = import_session(conn, session_file, "test-workspace")
    assert result2 is None

    # Verify only one run in DB
    count = conn.execute(
        select(func.count().label("n")).select_from(runs_table)
    ).mappings().fetchone()["n"]
    assert count == 1

    conn.close()


def test_import_project_function(tmp_path):
    """import_project imports sessions from a project folder."""
    from qualito.core.db import get_engine, get_sa_connection

    db_path = tmp_path / "test.db"
    engine = get_engine(str(db_path))
    conn = get_sa_connection(engine)

    # Create mock claude projects structure
    claude_dir = tmp_path / "claude-projects"
    project_folder = claude_dir / "-Users-alice-myapp"
    project_folder.mkdir(parents=True)

    _make_session_jsonl(
        project_folder / "session-001.jsonl", "session-001"
    )
    _make_session_jsonl(
        project_folder / "session-002.jsonl", "session-002"
    )

    result = import_project(
        project_key="-Users-alice-myapp",
        workspace_name="myapp",
        conn=conn,
        claude_projects_dir=claude_dir,
    )

    assert result["imported"] == 2
    assert result["skipped"] == 0

    # Import again — all skipped
    result2 = import_project(
        project_key="-Users-alice-myapp",
        workspace_name="myapp",
        conn=conn,
        claude_projects_dir=claude_dir,
    )
    assert result2["imported"] == 0
    assert result2["skipped"] == 2

    conn.close()


def test_import_project_nonexistent(tmp_path):
    """import_project with nonexistent folder returns empty result."""
    from qualito.core.db import get_engine, get_sa_connection

    db_path = tmp_path / "test.db"
    engine = get_engine(str(db_path))
    conn = get_sa_connection(engine)

    result = import_project(
        project_key="-does-not-exist",
        workspace_name="x",
        conn=conn,
        claude_projects_dir=tmp_path,
    )
    assert result["imported"] == 0
    assert result["skipped"] == 0

    conn.close()


# ---------------------------------------------------------------------------
# DB path resolution with global fallback
# ---------------------------------------------------------------------------


def test_db_resolve_prefers_local(tmp_path, monkeypatch):
    """_resolve_db_path prefers local .qualito/qualito.db if it exists."""
    from qualito.core.db import _resolve_db_path

    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    monkeypatch.chdir(tmp_path)

    # Create local DB
    local_dir = tmp_path / ".qualito"
    local_dir.mkdir()
    local_db = local_dir / "qualito.db"
    local_db.touch()

    result = _resolve_db_path()
    assert result == local_db


def test_db_resolve_falls_back_to_global(tmp_path, monkeypatch):
    """_resolve_db_path falls back to ~/.qualito/qualito.db when no local exists."""
    from qualito.core.db import _resolve_db_path

    home = tmp_path / "home"
    home.mkdir()
    project_dir = tmp_path / "some-project"
    project_dir.mkdir()
    monkeypatch.setattr(Path, "home", lambda: home)
    monkeypatch.chdir(project_dir)

    result = _resolve_db_path()
    assert result == home / ".qualito" / "qualito.db"
