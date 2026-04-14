"""Tests for `qualito sync` command (Task 3 rewrite).

Covers the interactive picker (at-limit and under-limit flows), the
WorkspaceLimitError rendering path, _parse_403_detail structured + string
fallbacks, the workspace-grouped sync loop with progress callbacks, and the
non-interactive flags (--all, --workspace, --since).
"""

from datetime import datetime, timedelta
from pathlib import Path

from click.testing import CliRunner
from sqlalchemy import insert

import qualito.cloud as cloud_mod
from qualito.cli.main import _render_workspace_limit_error, cli
from qualito.cloud import (
    CloudError,
    WorkspaceLimitError,
    _parse_403_detail,
    sync_runs,
)
from qualito.config import init_project
from qualito.core.db import get_engine, get_sa_connection, runs_table


# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------


def _seed_three_workspaces(db_path: Path) -> None:
    """Insert runs across workspaces alpha, bravo, charlie (2 per workspace)."""
    engine = get_engine(str(db_path))
    conn = get_sa_connection(engine)
    now = datetime.now()
    rows = []
    for i, ws in enumerate(("alpha", "bravo", "charlie")):
        for j in range(2):
            rows.append(
                {
                    "id": f"{ws}-{j}",
                    "workspace": ws,
                    "task": f"task {j}",
                    "status": "completed",
                    "session_type": "interactive",
                    "model": "claude-opus-4-6",
                    "cost_usd": 1.0 + i + j,
                    "input_tokens": 10_000 * (i + 1),
                    "output_tokens": 2_000 * (j + 1),
                    "cache_read_tokens": 0,
                    "started_at": (now - timedelta(hours=(i * 2 + j))).isoformat(),
                }
            )
    try:
        for r in rows:
            conn.execute(insert(runs_table).values(**r))
        conn.commit()
    finally:
        conn.close()


def _fake_login(monkeypatch, tmp_path: Path) -> Path:
    """Write a fake credentials file and monkeypatch CREDENTIALS_PATH."""
    creds_path = tmp_path / ".qualito" / "credentials.json"
    creds_path.parent.mkdir(parents=True, exist_ok=True)
    creds_path.write_text(
        '{"api_key": "test-key", "api_url": "https://api.qualito.ai"}'
    )
    monkeypatch.setattr(cloud_mod, "CREDENTIALS_PATH", creds_path, raising=True)
    return creds_path


def _stub_sync_runs(monkeypatch, captured: list):
    """Replace sync_runs with a stub that records call kwargs and fires callbacks."""

    def _stub(conn, **kwargs):
        captured.append(kwargs)
        workspaces = kwargs.get("workspaces")
        on_batch = kwargs.get("on_batch")
        on_workspace_done = kwargs.get("on_workspace_done")
        by_ws = {}
        if workspaces:
            for ws in workspaces:
                if on_batch:
                    on_batch(ws, 1, 1, 1)
                if on_workspace_done:
                    on_workspace_done(ws, 1)
                by_ws[ws] = 1
        return {
            "synced": len(by_ws),
            "skipped": 0,
            "errors": 0,
            "by_workspace": by_ws,
        }

    # Patch at both the cli.main and cloud module binding — `sync` imports
    # it lazily from qualito.cloud, so we patch the source module.
    monkeypatch.setattr(cloud_mod, "sync_runs", _stub, raising=True)
    monkeypatch.setattr(cloud_mod, "sync_incidents", lambda conn: {"synced": 0, "skipped": 0, "errors": 0}, raising=True)


# ---------------------------------------------------------------------------
# _parse_403_detail — structured dict + string fallback
# ---------------------------------------------------------------------------


class TestParse403Detail:
    def test_structured_workspace_limit(self):
        body = (
            '{"detail": {"error": "workspace_limit",'
            ' "message": "Free plan limited to 3 workspaces.",'
            ' "limit": 3,'
            ' "current_workspaces": ["alpha", "bravo", "charlie"],'
            ' "attempted_workspaces": ["delta"],'
            ' "upgrade_url": "https://app.qualito.ai/settings"}}'
        )
        err = _parse_403_detail(body)
        assert isinstance(err, WorkspaceLimitError)
        assert err.limit == 3
        assert err.current_workspaces == ["alpha", "bravo", "charlie"]
        assert err.attempted_workspaces == ["delta"]
        assert err.upgrade_url == "https://app.qualito.ai/settings"
        assert "Free plan" in str(err)
        assert err.status_code == 403

    def test_plain_string_detail_falls_back(self):
        """Older server returns a plain-string detail — graceful CloudError."""
        body = '{"detail": "Not authorized"}'
        err = _parse_403_detail(body)
        assert isinstance(err, CloudError)
        assert not isinstance(err, WorkspaceLimitError)
        assert "Not authorized" in str(err)
        assert err.status_code == 403

    def test_non_json_body_falls_back(self):
        err = _parse_403_detail("totally not json")
        assert isinstance(err, CloudError)
        assert not isinstance(err, WorkspaceLimitError)
        assert "totally not json" in str(err)

    def test_empty_body_falls_back(self):
        err = _parse_403_detail("")
        assert isinstance(err, CloudError)
        assert not isinstance(err, WorkspaceLimitError)


# ---------------------------------------------------------------------------
# _render_workspace_limit_error
# ---------------------------------------------------------------------------


class TestRenderWorkspaceLimitError:
    def test_renders_all_sections(self, capsys):
        err = WorkspaceLimitError(
            message="Free plan limited to 3 workspaces.",
            limit=3,
            current_workspaces=["alpha", "bravo", "charlie"],
            attempted_workspaces=["delta", "echo"],
            upgrade_url="https://app.qualito.ai/settings",
        )
        _render_workspace_limit_error(err)
        out = capsys.readouterr().out
        assert "Sync failed:" in out
        assert "Free plan limited to 3 workspaces." in out
        assert "Currently synced:" in out
        assert "alpha, bravo, charlie" in out
        assert "Tried to add:" in out
        assert "delta, echo" in out
        assert "Upgrade:" in out
        assert "https://app.qualito.ai/settings" in out

    def test_skips_empty_lists(self, capsys):
        err = WorkspaceLimitError(
            message="Free plan limit reached.",
            limit=3,
            current_workspaces=[],
            attempted_workspaces=[],
            upgrade_url="https://app.qualito.ai/settings",
        )
        _render_workspace_limit_error(err)
        out = capsys.readouterr().out
        assert "Sync failed:" in out
        assert "Currently synced:" not in out
        assert "Tried to add:" not in out
        assert "Upgrade:" in out


# ---------------------------------------------------------------------------
# sync_runs — workspace-grouped outer loop + callbacks
# ---------------------------------------------------------------------------


class TestSyncRunsLoop:
    def test_groups_by_workspace_and_preserves_order(self, tmp_path, monkeypatch):
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        init_project(project_dir=tmp_path, local=True)
        db_path = tmp_path / ".qualito" / "qualito.db"
        _seed_three_workspaces(db_path)
        _fake_login(monkeypatch, tmp_path)

        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)

        calls = []

        def fake_cloud_request(method, path, data=None, timeout=30):
            if "/privacy" in path:
                return {"workspace_name": path.split("/")[-2], "sync_content": True, "allow_llm": False}
            calls.append({"path": path, "runs": [r["id"] for r in data["runs"]]})
            return {"synced": len(data["runs"]), "skipped": 0, "errors": 0}

        monkeypatch.setattr(cloud_mod, "cloud_request", fake_cloud_request, raising=True)

        batch_events = []
        ws_done_events = []

        try:
            result = sync_runs(
                conn,
                workspaces=["charlie", "alpha", "bravo"],
                on_batch=lambda ws, n, total, count: batch_events.append(
                    (ws, n, total, count)
                ),
                on_workspace_done=lambda ws, synced: ws_done_events.append((ws, synced)),
            )
        finally:
            conn.close()

        assert result["synced"] == 6
        assert result["by_workspace"] == {"charlie": 2, "alpha": 2, "bravo": 2}

        # Every batch must contain runs from exactly one workspace
        for c in calls:
            prefixes = {r.split("-")[0] for r in c["runs"]}
            assert len(prefixes) == 1, f"Mixed workspaces in batch: {c['runs']}"

        # Picker order is preserved in the progress callbacks
        ws_order_from_events = [ws for (ws, *_rest) in ws_done_events]
        assert ws_order_from_events == ["charlie", "alpha", "bravo"]

        # Callback arguments shape: (workspace, batch_num, total_batches, count)
        for ws, batch_num, total_batches, count in batch_events:
            assert ws in {"alpha", "bravo", "charlie"}
            assert batch_num == 1
            assert total_batches == 1
            assert count == 2

    def test_batches_of_ten_with_multiple_batches(self, tmp_path, monkeypatch):
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        init_project(project_dir=tmp_path, local=True)
        db_path = tmp_path / ".qualito" / "qualito.db"

        # Seed 25 runs in a single workspace → expect 3 batches (10, 10, 5)
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        now = datetime.now()
        try:
            for i in range(25):
                conn.execute(
                    insert(runs_table).values(
                        id=f"big-{i:02d}",
                        workspace="mega",
                        task="task",
                        status="completed",
                        session_type="interactive",
                        model="claude-opus-4-6",
                        cost_usd=1.0,
                        input_tokens=100,
                        output_tokens=50,
                        cache_read_tokens=0,
                        started_at=(now - timedelta(minutes=i)).isoformat(),
                    )
                )
            conn.commit()
        finally:
            conn.close()

        _fake_login(monkeypatch, tmp_path)

        batches_seen = []

        def fake_cloud_request(method, path, data=None, timeout=30):
            if "/privacy" in path:
                return {"workspace_name": "mega", "sync_content": True, "allow_llm": False}
            batches_seen.append(len(data["runs"]))
            return {"synced": len(data["runs"]), "skipped": 0, "errors": 0}

        monkeypatch.setattr(cloud_mod, "cloud_request", fake_cloud_request, raising=True)

        batch_events = []
        conn = get_sa_connection(engine)
        try:
            result = sync_runs(
                conn,
                on_batch=lambda *args: batch_events.append(args),
            )
        finally:
            conn.close()

        assert batches_seen == [10, 10, 5]
        assert result["synced"] == 25
        # on_batch called three times with (ws, 1/3, total=3, count=10/5)
        assert len(batch_events) == 3
        for i, (ws, num, total, count) in enumerate(batch_events, 1):
            assert ws == "mega"
            assert num == i
            assert total == 3
        assert batch_events[-1][3] == 5


# ---------------------------------------------------------------------------
# Interactive picker — under limit vs at limit
# ---------------------------------------------------------------------------


class TestSyncPicker:
    def _setup(self, tmp_path, monkeypatch, synced_workspace_names):
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setenv("HOME", str(tmp_path))
        # sync uses Path.cwd() via _get_conn — chdir so we don't pick up the
        # real ~/qualito/.qualito/qualito.db or any sibling project DB.
        monkeypatch.chdir(tmp_path)
        init_project(project_dir=tmp_path, local=True)
        db_path = tmp_path / ".qualito" / "qualito.db"
        _seed_three_workspaces(db_path)
        _fake_login(monkeypatch, tmp_path)

        monkeypatch.setattr(
            cloud_mod,
            "fetch_user",
            lambda: {"email": "u@example.com", "plan": "free"},
            raising=True,
        )

        def _fake_fetch_synced():
            now_iso = datetime.now().isoformat()
            return [
                {
                    "workspace_name": name,
                    "first_synced_at": now_iso,
                    "last_synced_at": now_iso,
                    "session_count": 2,
                }
                for name in synced_workspace_names
            ]

        monkeypatch.setattr(
            cloud_mod, "fetch_synced_workspaces", _fake_fetch_synced, raising=True
        )

    def test_picker_under_limit(self, tmp_path, monkeypatch):
        """1 workspace synced, 2 local-only → picker shows 'Already synced (1/3)'."""
        self._setup(tmp_path, monkeypatch, synced_workspace_names=["alpha"])

        captured = []
        _stub_sync_runs(monkeypatch, captured)

        runner = CliRunner()
        # Prompt #1 (picker selection) → 'none' to exit without adding
        result = runner.invoke(cli, ["sync"], input="none\n")
        assert result.exit_code == 0, result.output

        assert "Already synced (1/3):" in result.output
        # Local-only section renders two workspaces with numbered prefixes
        assert "Local only (2):" in result.output
        assert "[1]" in result.output
        assert "[2]" in result.output
        assert "bravo" in result.output
        assert "charlie" in result.output

        # 'none' + only alpha synced_list → sync runs with selected_workspaces=["alpha"]
        assert captured, "sync_runs should have been called"
        assert captured[0]["workspaces"] == ["alpha"]

    def test_picker_at_limit(self, tmp_path, monkeypatch):
        """3 workspaces synced → [1]/[2] prompt (no numbered picker for local-only)."""
        self._setup(
            tmp_path, monkeypatch, synced_workspace_names=["alpha", "bravo", "charlie"]
        )

        # Make all three discovered local workspaces match the 3 synced ones,
        # so there are no local-only workspaces to pick. Everything is already
        # in scope for re-sync.
        captured = []
        _stub_sync_runs(monkeypatch, captured)

        runner = CliRunner()
        # Prompt: choose [1] — sync new sessions to existing workspaces
        result = runner.invoke(cli, ["sync"], input="1\n")
        assert result.exit_code == 0, result.output

        assert "Already synced (3/3):" in result.output
        assert "Free plan is at the workspace limit" in result.output
        assert "[1] Sync new sessions to existing workspaces" in result.output
        assert "[2] Upgrade to Pro" in result.output

        # At-limit path syncs the already-synced list
        assert captured, "sync_runs should have been called"
        assert sorted(captured[0]["workspaces"]) == ["alpha", "bravo", "charlie"]

    def test_picker_at_limit_upgrade_choice(self, tmp_path, monkeypatch):
        """Picking [2] at limit → prints upgrade URL and returns without sync."""
        self._setup(
            tmp_path, monkeypatch, synced_workspace_names=["alpha", "bravo", "charlie"]
        )
        captured = []
        _stub_sync_runs(monkeypatch, captured)

        runner = CliRunner()
        result = runner.invoke(cli, ["sync"], input="2\n")
        assert result.exit_code == 0, result.output
        assert "Upgrade: https://app.qualito.ai/settings" in result.output
        assert not captured, "sync_runs should NOT run when user picks upgrade"

    def test_picker_under_limit_add_one(self, tmp_path, monkeypatch):
        """2 synced, 1 local-only → pick '1' adds bravo."""
        self._setup(tmp_path, monkeypatch, synced_workspace_names=["alpha", "charlie"])

        captured = []
        _stub_sync_runs(monkeypatch, captured)

        runner = CliRunner()
        result = runner.invoke(cli, ["sync"], input="1\n")
        assert result.exit_code == 0, result.output
        assert "Already synced (2/3):" in result.output
        assert "up to 1 more" in result.output
        assert captured
        # synced_list + bravo (the only local-only workspace)
        assert "bravo" in captured[0]["workspaces"]
        assert "alpha" in captured[0]["workspaces"]
        assert "charlie" in captured[0]["workspaces"]


# ---------------------------------------------------------------------------
# Non-interactive flags
# ---------------------------------------------------------------------------


class TestSyncNonInteractive:
    def _setup(self, tmp_path, monkeypatch):
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.chdir(tmp_path)
        init_project(project_dir=tmp_path, local=True)
        db_path = tmp_path / ".qualito" / "qualito.db"
        _seed_three_workspaces(db_path)
        _fake_login(monkeypatch, tmp_path)
        return db_path

    def test_sync_all_skips_picker(self, tmp_path, monkeypatch):
        self._setup(tmp_path, monkeypatch)

        captured = []
        _stub_sync_runs(monkeypatch, captured)

        # fetch_user / fetch_synced should NOT be called on the --all path
        def _boom(*args, **kwargs):
            raise AssertionError("picker should be skipped with --all")

        monkeypatch.setattr(cloud_mod, "fetch_user", _boom, raising=True)
        monkeypatch.setattr(cloud_mod, "fetch_synced_workspaces", _boom, raising=True)

        runner = CliRunner()
        result = runner.invoke(cli, ["sync", "--all"])
        assert result.exit_code == 0, result.output
        assert "Cloud sync status" not in result.output  # picker header absent
        assert captured
        # --all → workspaces=None, since=None
        assert captured[0]["workspaces"] is None
        assert captured[0]["since"] is None

    def test_sync_workspace_filter_skips_picker(self, tmp_path, monkeypatch):
        self._setup(tmp_path, monkeypatch)

        captured = []
        _stub_sync_runs(monkeypatch, captured)

        def _boom(*args, **kwargs):
            raise AssertionError("picker should be skipped with --workspace")

        monkeypatch.setattr(cloud_mod, "fetch_user", _boom, raising=True)
        monkeypatch.setattr(cloud_mod, "fetch_synced_workspaces", _boom, raising=True)

        runner = CliRunner()
        result = runner.invoke(
            cli, ["sync", "--workspace", "alpha", "--workspace", "bravo"]
        )
        assert result.exit_code == 0, result.output
        assert captured
        assert captured[0]["workspaces"] == ["alpha", "bravo"]

    def test_sync_since_skips_picker(self, tmp_path, monkeypatch):
        self._setup(tmp_path, monkeypatch)

        captured = []
        _stub_sync_runs(monkeypatch, captured)

        def _boom(*args, **kwargs):
            raise AssertionError("picker should be skipped with --since")

        monkeypatch.setattr(cloud_mod, "fetch_user", _boom, raising=True)
        monkeypatch.setattr(cloud_mod, "fetch_synced_workspaces", _boom, raising=True)

        runner = CliRunner()
        result = runner.invoke(cli, ["sync", "--since", "2026-04-01"])
        assert result.exit_code == 0, result.output
        assert captured
        assert captured[0]["since"] == "2026-04-01"
        assert captured[0]["workspaces"] is None

    def test_sync_without_login_fails(self, tmp_path, monkeypatch):
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.chdir(tmp_path)
        init_project(project_dir=tmp_path, local=True)

        # CREDENTIALS_PATH is a module-level constant resolved at import;
        # point it at tmp_path so load_credentials() can't see the real file.
        monkeypatch.setattr(
            cloud_mod,
            "CREDENTIALS_PATH",
            tmp_path / ".qualito" / "credentials.json",
            raising=True,
        )

        runner = CliRunner()
        result = runner.invoke(cli, ["sync"])
        assert result.exit_code == 1
        assert "Not logged in" in result.output
