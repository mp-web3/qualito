"""Tests for config system and CLI init/status commands."""

from datetime import datetime, timedelta
from pathlib import Path

from click.testing import CliRunner
from sqlalchemy import insert

from qualito.cli.main import _fmt_relative_time, _fmt_tokens, cli
from qualito.config import QualityConfig, init_project, load_config
from qualito.core.db import get_engine, get_sa_connection, runs_table


def test_quality_config_defaults(tmp_path, monkeypatch):
    """Default config values are sensible."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    config = QualityConfig()
    assert config.db_path == tmp_path / ".qualito" / "qualito.db"
    assert config.slo_quality == 0.60
    assert config.slo_availability == 0.95
    assert config.slo_cost == 3.00
    assert config.templates_dir is None


def test_load_config_no_files(tmp_path, monkeypatch):
    """load_config works when no config files exist."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    project_dir = tmp_path / "myproject"
    project_dir.mkdir()
    config = load_config(project_dir=project_dir)
    assert config.workspace == "myproject"
    # Default db_path is global
    assert config.db_path == tmp_path / ".qualito" / "qualito.db"


def test_load_config_reads_project_toml(tmp_path, monkeypatch):
    """load_config reads project .qualito/config.toml."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    qualito_dir = tmp_path / ".qualito"
    qualito_dir.mkdir()
    (qualito_dir / "config.toml").write_text('workspace = "my-project"\nslo_cost = 5.00\n')

    config = load_config(project_dir=tmp_path)
    assert config.workspace == "my-project"
    assert config.slo_cost == 5.00
    assert config.slo_quality == 0.60  # unchanged default


def test_init_project_creates_files_global(tmp_path, monkeypatch):
    """init_project creates ~/.qualito/ by default (global mode)."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    project_dir = tmp_path / "my-project"
    project_dir.mkdir()

    config, qualito_dir = init_project(project_dir=project_dir)

    assert qualito_dir == tmp_path / ".qualito"
    assert qualito_dir.exists()
    assert (qualito_dir / "config.toml").exists()
    assert config.db_path.is_absolute()


def test_init_project_creates_files_local(tmp_path, monkeypatch):
    """init_project(local=True) creates .qualito/ in the project dir."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    project_dir = tmp_path / "my-project"
    project_dir.mkdir()

    config, qualito_dir = init_project(project_dir=project_dir, local=True)

    assert qualito_dir == project_dir / ".qualito"
    assert qualito_dir.exists()
    assert (qualito_dir / "config.toml").exists()
    assert (project_dir / config.db_path).exists() or config.db_path.exists()


def test_init_project_idempotent(tmp_path, monkeypatch):
    """Running init_project twice doesn't overwrite config."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    init_project(project_dir=tmp_path, local=True)

    # Modify config
    config_path = tmp_path / ".qualito" / "config.toml"
    config_path.write_text('workspace = "custom"\n')

    # Init again — should not overwrite
    config, _ = init_project(project_dir=tmp_path, local=True)
    assert config.workspace == "custom"


def test_cli_status_not_initialized(tmp_path, monkeypatch):
    """qualito status fails when not initialized."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--dir", str(tmp_path)])
    assert result.exit_code == 1
    assert "not initialized" in result.output


def test_cli_status_after_init(tmp_path, monkeypatch):
    """qualito status shows local + cloud breakdown and cost disclaimer."""
    # Leak guard: Path.home + HOME env + cloud.CREDENTIALS_PATH all scoped to
    # tmp_path. The CREDENTIALS_PATH patch is required because it is a
    # module-level constant resolved at import time — relying on Path.home
    # alone leaks whenever any other test file eagerly imports qualito.cloud.
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    import qualito.cloud as cloud_mod

    monkeypatch.setattr(
        cloud_mod,
        "CREDENTIALS_PATH",
        tmp_path / ".qualito" / "credentials.json",
        raising=True,
    )
    init_project(project_dir=tmp_path, local=True)
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "Qualito Status" in result.output
    assert "Local" in result.output
    assert "Sessions: 0 imported" in result.output
    assert "Interactive: 0" in result.output
    assert "Delegated: 0" in result.output
    assert "Workspaces: 0" in result.output
    # Not logged in → cloud section shows login prompt
    assert "Cloud: not logged in" in result.output
    # Cost disclaimer footer
    assert "Costs are estimates" in result.output
    assert "qualito costs --explain" in result.output
    # SLOs must be gone
    assert "SLOs:" not in result.output


def _seed_runs(db_path: Path):
    """Seed a database with sample runs across two workspaces."""
    engine = get_engine(str(db_path))
    conn = get_sa_connection(engine)
    now = datetime.now()
    try:
        rows = [
            {
                "id": "run-a-1",
                "workspace": "assistant",
                "task": "refactor",
                "status": "completed",
                "session_type": "interactive",
                "cost_usd": 8.50,
                "input_tokens": 4_200_000,
                "output_tokens": 1_100_000,
                "cache_read_tokens": 0,
                "started_at": (now - timedelta(hours=2)).isoformat(),
            },
            {
                "id": "run-a-2",
                "workspace": "assistant",
                "task": "delegate",
                "status": "completed",
                "session_type": "delegated",
                "cost_usd": 0.25,
                "input_tokens": 10_000,
                "output_tokens": 5_000,
                "cache_read_tokens": 0,
                "started_at": (now - timedelta(hours=3)).isoformat(),
            },
            {
                "id": "run-b-1",
                "workspace": "claude",
                "task": "fix",
                "status": "completed",
                "session_type": "interactive",
                "cost_usd": 1.40,
                "input_tokens": 125_000,
                "output_tokens": 20_000,
                "cache_read_tokens": 0,
                "started_at": (now - timedelta(days=3)).isoformat(),
            },
        ]
        for r in rows:
            conn.execute(insert(runs_table).values(**r))
        conn.commit()
    finally:
        conn.close()


def test_cli_status_local_with_runs(tmp_path, monkeypatch):
    """State 3: not logged in + runs seeded → local table shows rows, no cloud."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    import qualito.cloud as cloud_mod

    monkeypatch.setattr(
        cloud_mod,
        "CREDENTIALS_PATH",
        tmp_path / ".qualito" / "credentials.json",
        raising=True,
    )
    init_project(project_dir=tmp_path, local=True)
    runner = CliRunner()

    db_path = tmp_path / ".qualito" / "qualito.db"
    _seed_runs(db_path)

    result = runner.invoke(cli, ["status", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output

    # Counts
    assert "Sessions: 3 imported" in result.output
    assert "Interactive: 2" in result.output
    assert "Delegated: 1" in result.output
    assert "Workspaces: 2" in result.output

    # Per-workspace table
    assert "Per workspace:" in result.output
    assert "assistant" in result.output
    assert "claude" in result.output
    assert "in tokens" in result.output
    assert "out tokens" in result.output

    # Cloud: not logged in
    assert "Cloud: not logged in" in result.output

    # Disclaimer
    assert "Costs are estimates" in result.output
    # No SLOs
    assert "SLOs:" not in result.output


def test_cli_status_logged_in_no_sync(tmp_path, monkeypatch):
    """State 2: logged in, no workspaces synced → shows 'No workspaces synced yet'."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    init_project(project_dir=tmp_path, local=True)
    runner = CliRunner()

    # Fake credentials path and content
    creds_path = tmp_path / ".qualito" / "credentials.json"
    creds_path.parent.mkdir(parents=True, exist_ok=True)
    creds_path.write_text(
        '{"api_key": "test-key", "api_url": "https://api.qualito.ai"}'
    )

    import qualito.cloud as cloud_mod

    monkeypatch.setattr(cloud_mod, "CREDENTIALS_PATH", creds_path, raising=True)
    monkeypatch.setattr(
        cloud_mod,
        "fetch_user",
        lambda: {"email": "test@example.com", "plan": "free"},
        raising=True,
    )
    monkeypatch.setattr(cloud_mod, "fetch_synced_workspaces", lambda: [], raising=True)

    result = runner.invoke(cli, ["status", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "Cloud (test@example.com, free plan)" in result.output
    assert "No workspaces synced yet" in result.output
    assert "qualito sync" in result.output


def test_cli_status_logged_in_with_sync(tmp_path, monkeypatch):
    """State 1: logged in + some workspaces synced → cloud section full, local-only list."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    init_project(project_dir=tmp_path, local=True)
    runner = CliRunner()

    db_path = tmp_path / ".qualito" / "qualito.db"
    _seed_runs(db_path)

    creds_path = tmp_path / ".qualito" / "credentials.json"
    creds_path.write_text(
        '{"api_key": "test-key", "api_url": "https://api.qualito.ai"}'
    )

    import qualito.cloud as cloud_mod

    monkeypatch.setattr(cloud_mod, "CREDENTIALS_PATH", creds_path, raising=True)
    now_iso = (datetime.now() - timedelta(minutes=2)).isoformat()
    monkeypatch.setattr(
        cloud_mod,
        "fetch_user",
        lambda: {"email": "mp@example.com", "plan": "free"},
        raising=True,
    )
    monkeypatch.setattr(
        cloud_mod,
        "fetch_synced_workspaces",
        lambda: [
            {
                "workspace_name": "assistant",
                "first_synced_at": now_iso,
                "last_synced_at": now_iso,
                "session_count": 2,
            }
        ],
        raising=True,
    )

    result = runner.invoke(cli, ["status", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output

    assert "Cloud (mp@example.com, free plan)" in result.output
    assert "Synced workspaces: assistant" in result.output
    assert "Synced sessions: 2" in result.output
    assert "Last sync:" in result.output
    assert "https://app.qualito.ai/runs" in result.output

    # Local-only workspaces section lists 'claude' (not synced), not 'assistant'
    assert "Local-only workspaces" in result.output
    assert "claude" in result.output

    # Free plan upgrade hint
    assert "Upgrade to Pro" in result.output


def test_fmt_tokens():
    assert _fmt_tokens(None) == "0"
    assert _fmt_tokens(0) == "0"
    assert _fmt_tokens(500) == "500"
    assert _fmt_tokens(1_500) == "1.5k"
    assert _fmt_tokens(125_000) == "125k"
    assert _fmt_tokens(4_200_000) == "4.20M"
    assert _fmt_tokens(50_000) == "50.0k"


def test_fmt_relative_time():
    now = datetime.now()
    assert _fmt_relative_time(None) == "never"
    assert _fmt_relative_time("") == "never"
    assert "hour" in _fmt_relative_time((now - timedelta(hours=2)).isoformat())
    assert "day" in _fmt_relative_time((now - timedelta(days=3)).isoformat())
    assert "week" in _fmt_relative_time((now - timedelta(days=10)).isoformat())
    assert "minute" in _fmt_relative_time((now - timedelta(minutes=5)).isoformat())
