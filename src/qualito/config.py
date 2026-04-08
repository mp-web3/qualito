"""Qualito configuration system.

Loads config from project (.qualito/config.toml) and global (~/.qualito/config.toml).
Project config overrides global. Environment variables override both.
"""

import os
import subprocess
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class QualityConfig:
    """Qualito configuration."""

    db_path: Path = field(default_factory=lambda: Path.home() / ".qualito" / "qualito.db")
    workspace: str = ""
    slo_quality: float = 0.60
    slo_availability: float = 0.95
    slo_cost: float = 3.00
    templates_dir: Path | None = None


# Backward compat alias
DqiConfig = QualityConfig


def get_global_dir() -> Path:
    """Return the global Qualito directory (~/.qualito/), creating it if missing."""
    global_dir = Path.home() / ".qualito"
    global_dir.mkdir(parents=True, exist_ok=True)
    return global_dir

# Keys that map to Path fields
_PATH_KEYS = {"db_path", "templates_dir"}


def _apply_toml(config: QualityConfig, data: dict) -> None:
    """Apply TOML data onto a config dataclass."""
    for key, value in data.items():
        if hasattr(config, key):
            if key in _PATH_KEYS and value is not None:
                setattr(config, key, Path(value).expanduser())
            else:
                setattr(config, key, value)


def _detect_workspace(project_dir: Path) -> str:
    """Detect workspace name from git remote or directory name."""
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            capture_output=True, text=True, cwd=project_dir, timeout=5,
        )
        if result.returncode == 0:
            url = result.stdout.strip()
            # Extract repo name from git URL (handles both HTTPS and SSH)
            name = url.rstrip("/").rsplit("/", 1)[-1]
            if name.endswith(".git"):
                name = name[:-4]
            return name
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return project_dir.name


def _write_default_config(path: Path, workspace: str) -> None:
    """Write a default config.toml file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    content = f"""\
# Qualito Configuration
# Project-level settings (override global ~/.qualito/config.toml)

workspace = "{workspace}"

# SLO thresholds
slo_quality = 0.60
slo_availability = 0.95
slo_cost = 3.00

# Database path (relative to project root)
db_path = ".qualito/qualito.db"

# Custom templates directory (optional)
# templates_dir = ".qualito/templates"
"""
    path.write_text(content)


def _write_global_config(path: Path) -> None:
    """Write a default global config.toml file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    content = """\
# Qualito Global Configuration
# Applies to all projects. Per-project .qualito/config.toml overrides these.

# SLO thresholds
slo_quality = 0.60
slo_availability = 0.95
slo_cost = 3.00

# Database path (global, shared across all projects)
db_path = "~/.qualito/qualito.db"

# Custom templates directory (optional)
# templates_dir = "~/.qualito/templates"
"""
    path.write_text(content)


def load_config(project_dir: Path | None = None) -> QualityConfig:
    """Load Qualito config, merging global and project settings.

    Priority: env vars > project .qualito/config.toml > global ~/.qualito/config.toml > defaults.

    The default db_path is ~/.qualito/qualito.db (global). A project-level
    config can override this to a local path.

    Args:
        project_dir: Project root. Defaults to cwd.
    """
    if project_dir is None:
        project_dir = Path.cwd()

    config = QualityConfig()

    # 1. Global config
    global_path = Path.home() / ".qualito" / "config.toml"
    if global_path.exists():
        with open(global_path, "rb") as f:
            _apply_toml(config, tomllib.load(f))

    # 2. Project config (overrides global)
    project_path = project_dir / ".qualito" / "config.toml"
    if project_path.exists():
        with open(project_path, "rb") as f:
            _apply_toml(config, tomllib.load(f))

    # 3. Env var overrides
    qualito_dir = os.environ.get("QUALITO_DIR")
    if qualito_dir:
        config.db_path = Path(qualito_dir) / "qualito.db"

    # 4. Default workspace if not set
    if not config.workspace:
        config.workspace = _detect_workspace(project_dir)

    return config


def init_project(project_dir: Path | None = None, *, local: bool = False) -> tuple[QualityConfig, Path]:
    """Initialize Qualito, either globally (~/.qualito/) or locally (.qualito/).

    Global mode (default): Creates ~/.qualito/ with config.toml and database.
    Local mode (--local): Creates .qualito/ in the project directory (backward compat).

    Args:
        project_dir: Project root. Defaults to cwd.
        local: If True, create per-project .qualito/ instead of global.

    Returns:
        Tuple of (config, qualito_dir_path).
    """
    if project_dir is None:
        project_dir = Path.cwd()

    if local:
        qualito_dir = project_dir / ".qualito"
    else:
        qualito_dir = get_global_dir()

    qualito_dir.mkdir(parents=True, exist_ok=True)

    # Detect workspace
    workspace = _detect_workspace(project_dir)

    # Write config
    config_path = qualito_dir / "config.toml"
    if not config_path.exists():
        if local:
            _write_default_config(config_path, workspace)
        else:
            _write_global_config(config_path)

    # Load config (picks up what we just wrote)
    config = load_config(project_dir)

    # Create database
    from qualito.core.db import get_db

    db_path = config.db_path
    if not db_path.is_absolute():
        db_path = project_dir / db_path
    conn = get_db(db_path=db_path)
    conn.close()

    return config, qualito_dir
