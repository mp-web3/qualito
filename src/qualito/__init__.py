"""Qualito — Quality metrics for AI-assisted development.

See what your Claude Code sessions cost, detect quality issues,
and run experiments to improve.
"""

try:
    from importlib.metadata import version as _get_version
    __version__ = _get_version("qualito")
except Exception:
    __version__ = "0.0.0"
