"""Tests for config system and CLI init/status commands."""

from pathlib import Path

from click.testing import CliRunner

from qualito.cli.main import cli
from qualito.config import QualityConfig, load_config, init_project


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


def test_cli_init_global(tmp_path, monkeypatch):
    """qualito init creates ~/.qualito/ directory by default."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "global" in result.output
    assert (tmp_path / ".qualito" / "config.toml").exists()


def test_cli_init_local(tmp_path, monkeypatch):
    """qualito init --local creates .qualito/ in the project dir."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--local", "--dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "local" in result.output
    assert (tmp_path / ".qualito" / "config.toml").exists()
    assert (tmp_path / ".qualito" / "qualito.db").exists()


def test_cli_status_not_initialized(tmp_path, monkeypatch):
    """qualito status fails when not initialized."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--dir", str(tmp_path)])
    assert result.exit_code == 1
    assert "not initialized" in result.output


def test_cli_status_after_init(tmp_path, monkeypatch):
    """qualito status shows correct info after init."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    runner = CliRunner()
    runner.invoke(cli, ["init", "--dir", str(tmp_path)])
    result = runner.invoke(cli, ["status", "--dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "Workspace:" in result.output
    assert "Runs: 0" in result.output
    assert "SLOs:" in result.output
