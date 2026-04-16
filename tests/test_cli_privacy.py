"""Tests for `qualito privacy` command (Phase 7 T6).

The command has 4 modes — list, show, set sync_content, toggle allow_llm.
All network calls go through qualito.cloud helpers which are patched so
the tests never hit HTTP.
"""

from datetime import datetime
from unittest.mock import patch

from click.testing import CliRunner

import qualito.cloud as cloud_mod
from qualito.cli.main import cli
from qualito.cloud import CloudError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fake_login(monkeypatch, tmp_path):
    """Write a fake credentials file and point CREDENTIALS_PATH at it."""
    creds_path = tmp_path / ".qualito" / "credentials.json"
    creds_path.parent.mkdir(parents=True, exist_ok=True)
    creds_path.write_text(
        '{"api_key": "test-key", "api_url": "https://api.qualito.ai"}'
    )
    monkeypatch.setattr(cloud_mod, "CREDENTIALS_PATH", creds_path, raising=True)
    return creds_path


def _synced_row(name: str, last_iso: str | None = None) -> dict:
    return {
        "workspace_name": name,
        "first_synced_at": last_iso,
        "last_synced_at": last_iso,
        "session_count": 2,
    }


# ---------------------------------------------------------------------------
# Mode 1 — list
# ---------------------------------------------------------------------------


class TestPrivacyList:
    def test_privacy_list_empty(self, tmp_path, monkeypatch):
        _fake_login(monkeypatch, tmp_path)

        with patch(
            "qualito.cloud.fetch_synced_workspaces", return_value=[]
        ), patch(
            "qualito.cloud.fetch_user",
            return_value={"email": "u@example.com", "plan": "free"},
        ):
            runner = CliRunner()
            result = runner.invoke(cli, ["privacy"])

        assert result.exit_code == 0, result.output
        assert "No synced workspaces yet" in result.output
        assert "qualito sync" in result.output

    def test_privacy_list_renders_table(self, tmp_path, monkeypatch):
        _fake_login(monkeypatch, tmp_path)

        now_iso = datetime.now().isoformat()
        synced = [
            _synced_row("claude", None),
            _synced_row("assistant", now_iso),
            _synced_row("intelligence", now_iso),
        ]

        def _fake_privacy(name):
            return {
                "claude": {"sync_content": False, "allow_llm": False},
                "assistant": {"sync_content": True, "allow_llm": False},
                "intelligence": {"sync_content": False, "allow_llm": False},
            }[name]

        with patch(
            "qualito.cloud.fetch_synced_workspaces", return_value=synced
        ), patch(
            "qualito.cloud.fetch_workspace_privacy", side_effect=_fake_privacy
        ), patch(
            "qualito.cloud.fetch_user",
            return_value={"email": "u@example.com", "plan": "free"},
        ):
            runner = CliRunner()
            result = runner.invoke(cli, ["privacy"])

        assert result.exit_code == 0, result.output
        # Header
        assert "Workspace privacy" in result.output
        assert "Sync content" in result.output
        assert "LLM analysis" in result.output
        assert "Last changed" in result.output
        # Rows
        assert "claude" in result.output
        assert "assistant" in result.output
        assert "intelligence" in result.output
        assert "full content" in result.output  # assistant has sync_content=True
        assert "metadata only" in result.output
        # Footer
        assert "Default: full content" in result.output


# ---------------------------------------------------------------------------
# Mode 2 — show
# ---------------------------------------------------------------------------


class TestPrivacyShow:
    def test_privacy_show_single_workspace(self, tmp_path, monkeypatch):
        _fake_login(monkeypatch, tmp_path)

        with patch(
            "qualito.cloud.fetch_workspace_privacy",
            return_value={
                "workspace_name": "claude",
                "sync_content": False,
                "allow_llm": False,
            },
        ):
            runner = CliRunner()
            result = runner.invoke(cli, ["privacy", "claude"])

        assert result.exit_code == 0, result.output
        assert "Workspace: claude" in result.output
        assert "Sync content:" in result.output
        assert "LLM analysis:" in result.output
        assert "metadata only" in result.output
        # Explanation for metadata-only mode
        assert "counts, durations" in result.output
        assert "--full" in result.output

    def test_privacy_show_full_content_swaps_explanation(self, tmp_path, monkeypatch):
        _fake_login(monkeypatch, tmp_path)

        with patch(
            "qualito.cloud.fetch_workspace_privacy",
            return_value={
                "workspace_name": "claude",
                "sync_content": True,
                "allow_llm": False,
            },
        ):
            runner = CliRunner()
            result = runner.invoke(cli, ["privacy", "claude"])

        assert result.exit_code == 0, result.output
        assert "full content" in result.output
        assert "Full content means" in result.output
        assert "--metadata" in result.output

    def test_privacy_show_404_unsynced_workspace(self, tmp_path, monkeypatch):
        _fake_login(monkeypatch, tmp_path)

        with patch(
            "qualito.cloud.fetch_workspace_privacy",
            side_effect=CloudError("Not Found", status_code=404),
        ):
            runner = CliRunner()
            result = runner.invoke(cli, ["privacy", "missing-ws"])

        assert result.exit_code == 0, result.output
        assert "not yet synced" in result.output
        assert "qualito sync" in result.output


# ---------------------------------------------------------------------------
# Mode 3 — set sync_content
# ---------------------------------------------------------------------------


class TestPrivacySetSyncContent:
    def test_privacy_set_metadata(self, tmp_path, monkeypatch):
        _fake_login(monkeypatch, tmp_path)

        with patch(
            "qualito.cloud.set_workspace_privacy", return_value={}
        ) as mock_set:
            runner = CliRunner()
            result = runner.invoke(cli, ["privacy", "claude", "--metadata"])

        assert result.exit_code == 0, result.output
        assert "metadata-only" in result.output
        mock_set.assert_called_once_with("claude", sync_content=False)

    def test_privacy_set_full_with_yes_flag(self, tmp_path, monkeypatch):
        _fake_login(monkeypatch, tmp_path)

        with patch(
            "qualito.cloud.set_workspace_privacy", return_value={}
        ) as mock_set:
            runner = CliRunner()
            result = runner.invoke(
                cli, ["privacy", "claude", "--full", "--yes"]
            )

        assert result.exit_code == 0, result.output
        assert "full content" in result.output
        # No confirmation prompt should appear
        assert "Continue?" not in result.output
        mock_set.assert_called_once_with("claude", sync_content=True)

    def test_privacy_set_full_confirms_and_accepts(self, tmp_path, monkeypatch):
        _fake_login(monkeypatch, tmp_path)

        with patch(
            "qualito.cloud.set_workspace_privacy", return_value={}
        ) as mock_set:
            runner = CliRunner()
            result = runner.invoke(
                cli, ["privacy", "claude", "--full"], input="y\n"
            )

        assert result.exit_code == 0, result.output
        assert "Continue?" in result.output
        assert "full content" in result.output
        mock_set.assert_called_once_with("claude", sync_content=True)

    def test_privacy_set_full_aborts_on_no(self, tmp_path, monkeypatch):
        _fake_login(monkeypatch, tmp_path)

        with patch(
            "qualito.cloud.set_workspace_privacy", return_value={}
        ) as mock_set:
            runner = CliRunner()
            result = runner.invoke(
                cli, ["privacy", "claude", "--full"], input="n\n"
            )

        assert result.exit_code == 0, result.output
        assert "Aborted. No changes." in result.output
        mock_set.assert_not_called()


# ---------------------------------------------------------------------------
# Mode 4 — allow_llm toggle (GET then PATCH)
# ---------------------------------------------------------------------------


class TestPrivacyAllowLlm:
    def test_privacy_allow_llm_toggle(self, tmp_path, monkeypatch):
        _fake_login(monkeypatch, tmp_path)

        with patch(
            "qualito.cloud.fetch_workspace_privacy",
            return_value={
                "workspace_name": "claude",
                "sync_content": True,  # must be preserved
                "allow_llm": False,
            },
        ) as mock_get, patch(
            "qualito.cloud.set_workspace_privacy", return_value={}
        ) as mock_set:
            runner = CliRunner()
            result = runner.invoke(cli, ["privacy", "claude", "--allow-llm"])

        assert result.exit_code == 0, result.output
        mock_get.assert_called_once_with("claude")
        # PATCH with preserved sync_content and new allow_llm=True
        mock_set.assert_called_once_with(
            "claude", sync_content=True, allow_llm=True
        )
        assert "allow_llm=true" in result.output


# ---------------------------------------------------------------------------
# Validation — mutually exclusive flags
# ---------------------------------------------------------------------------


class TestPrivacyValidation:
    def test_privacy_mutually_exclusive_metadata_and_full(self, tmp_path, monkeypatch):
        _fake_login(monkeypatch, tmp_path)

        runner = CliRunner()
        result = runner.invoke(
            cli, ["privacy", "claude", "--metadata", "--full"]
        )

        assert result.exit_code != 0
        assert "mutually exclusive" in result.output
