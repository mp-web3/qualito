"""Tests for config system and CLI init/status commands."""

from pathlib import Path

from click.testing import CliRunner

from dqi.cli.main import cli
from dqi.config import DqiConfig, load_config, init_project


def test_dqi_config_defaults():
    """Default config values are sensible."""
    config = DqiConfig()
    assert config.db_path == Path(".dqi/dqi.db")
    assert config.slo_quality == 0.60
    assert config.slo_availability == 0.95
    assert config.slo_cost == 3.00
    assert config.templates_dir is None


def test_load_config_no_files(tmp_path):
    """load_config works when no config files exist."""
    config = load_config(project_dir=tmp_path)
    assert config.workspace == tmp_path.name
    assert config.db_path == Path(".dqi/dqi.db")


def test_load_config_reads_project_toml(tmp_path):
    """load_config reads project .dqi/config.toml."""
    dqi_dir = tmp_path / ".dqi"
    dqi_dir.mkdir()
    (dqi_dir / "config.toml").write_text('workspace = "my-project"\nslo_cost = 5.00\n')

    config = load_config(project_dir=tmp_path)
    assert config.workspace == "my-project"
    assert config.slo_cost == 5.00
    assert config.slo_quality == 0.60  # unchanged default


def test_init_project_creates_files(tmp_path):
    """init_project creates .dqi/, config.toml, and database."""
    config, dqi_dir = init_project(project_dir=tmp_path)

    assert dqi_dir == tmp_path / ".dqi"
    assert dqi_dir.exists()
    assert (dqi_dir / "config.toml").exists()
    assert (tmp_path / config.db_path).exists()
    assert config.workspace == tmp_path.name


def test_init_project_idempotent(tmp_path):
    """Running init_project twice doesn't overwrite config."""
    init_project(project_dir=tmp_path)

    # Modify config
    config_path = tmp_path / ".dqi" / "config.toml"
    config_path.write_text('workspace = "custom"\n')

    # Init again — should not overwrite
    config, _ = init_project(project_dir=tmp_path)
    assert config.workspace == "custom"


def test_cli_init(tmp_path):
    """dqi init creates .dqi/ directory."""
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "Initialized DQI" in result.output
    assert (tmp_path / ".dqi" / "config.toml").exists()
    assert (tmp_path / ".dqi" / "dqi.db").exists()


def test_cli_status_not_initialized(tmp_path):
    """dqi status fails when not initialized."""
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--dir", str(tmp_path)])
    assert result.exit_code == 1
    assert "not initialized" in result.output


def test_cli_status_after_init(tmp_path):
    """dqi status shows correct info after init."""
    runner = CliRunner()
    runner.invoke(cli, ["init", "--dir", str(tmp_path)])
    result = runner.invoke(cli, ["status", "--dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "Workspace:" in result.output
    assert "Runs: 0" in result.output
    assert "SLOs:" in result.output
