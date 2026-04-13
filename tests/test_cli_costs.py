"""Tests for `qualito costs` command (Task 5 rewrite).

Covers default output, --correct flag recomputation, --explain bypass of the
DB, workspace/day filters, --top limits, NULL cost_usd exclusion, and the
'no waste references' invariant.
"""

from datetime import datetime, timedelta
from pathlib import Path

from click.testing import CliRunner
from sqlalchemy import insert

from qualito.cli.main import _compute_cost, _OUTPUT_UNDERCOUNT_FACTOR, cli
from qualito.config import init_project
from qualito.core.db import get_engine, get_sa_connection, runs_table
from qualito.importer import MODEL_PRICING


# ---------------------------------------------------------------------------
# Shared seed helper
# ---------------------------------------------------------------------------


def _seed_cost_fixture(db_path: Path) -> None:
    """Insert 6 runs: 3 workspaces × 2 models, plus 2 rows with NULL cost_usd."""
    engine = get_engine(str(db_path))
    conn = get_sa_connection(engine)
    now = datetime.now()
    rows = [
        # Workspace 'alpha' — Opus (expensive) + Sonnet
        {
            "id": "alpha-opus-1",
            "workspace": "alpha",
            "task": "expensive opus refactor",
            "status": "completed",
            "session_type": "interactive",
            "model": "claude-opus-4-6",
            "cost_usd": 50.00,
            "input_tokens": 1_000_000,
            "output_tokens": 500_000,
            "cache_read_tokens": 200_000,
            "started_at": (now - timedelta(days=2)).isoformat(),
        },
        {
            "id": "alpha-sonnet-1",
            "workspace": "alpha",
            "task": "cheap sonnet edits",
            "status": "completed",
            "session_type": "interactive",
            "model": "claude-sonnet-4-6",
            "cost_usd": 2.00,
            "input_tokens": 400_000,
            "output_tokens": 100_000,
            "cache_read_tokens": 50_000,
            "started_at": (now - timedelta(days=3)).isoformat(),
        },
        # Workspace 'bravo'
        {
            "id": "bravo-opus-1",
            "workspace": "bravo",
            "task": "bravo investigation",
            "status": "completed",
            "session_type": "delegated",
            "model": "claude-opus-4-6",
            "cost_usd": 20.00,
            "input_tokens": 500_000,
            "output_tokens": 250_000,
            "cache_read_tokens": 100_000,
            "started_at": (now - timedelta(days=5)).isoformat(),
        },
        {
            "id": "bravo-sonnet-1",
            "workspace": "bravo",
            "task": "bravo sonnet tweak",
            "status": "completed",
            "session_type": "interactive",
            "model": "claude-sonnet-4-6",
            "cost_usd": 1.50,
            "input_tokens": 200_000,
            "output_tokens": 80_000,
            "cache_read_tokens": 30_000,
            "started_at": (now - timedelta(days=1)).isoformat(),
        },
        # Workspace 'charlie'
        {
            "id": "charlie-opus-1",
            "workspace": "charlie",
            "task": "charlie opus audit",
            "status": "completed",
            "session_type": "interactive",
            "model": "claude-opus-4-6",
            "cost_usd": 12.00,
            "input_tokens": 300_000,
            "output_tokens": 150_000,
            "cache_read_tokens": 75_000,
            "started_at": (now - timedelta(days=4)).isoformat(),
        },
        {
            "id": "charlie-sonnet-1",
            "workspace": "charlie",
            "task": "charlie sonnet",
            "status": "completed",
            "session_type": "interactive",
            "model": "claude-sonnet-4-6",
            "cost_usd": 0.80,
            "input_tokens": 150_000,
            "output_tokens": 50_000,
            "cache_read_tokens": 20_000,
            "started_at": (now - timedelta(days=10)).isoformat(),
        },
        # Synthetic sessions with NULL cost → excluded from aggregates
        {
            "id": "alpha-synth-1",
            "workspace": "alpha",
            "task": "synthetic — no cost",
            "status": "completed",
            "session_type": "interactive",
            "model": "claude-opus-4-6",
            "cost_usd": None,
            "input_tokens": 9_999_999,
            "output_tokens": 9_999_999,
            "cache_read_tokens": 9_999_999,
            "started_at": (now - timedelta(days=1)).isoformat(),
        },
        {
            "id": "bravo-synth-1",
            "workspace": "bravo",
            "task": "another synthetic",
            "status": "completed",
            "session_type": "delegated",
            "model": "claude-sonnet-4-6",
            "cost_usd": None,
            "input_tokens": 9_999_999,
            "output_tokens": 9_999_999,
            "cache_read_tokens": 9_999_999,
            "started_at": (now - timedelta(days=2)).isoformat(),
        },
    ]
    try:
        for r in rows:
            conn.execute(insert(runs_table).values(**r))
        conn.commit()
    finally:
        conn.close()


def _seed_recent_and_old(db_path: Path) -> None:
    """Seed runs at various date offsets to test --days window."""
    engine = get_engine(str(db_path))
    conn = get_sa_connection(engine)
    now = datetime.now()
    rows = [
        # Fresh — 1 day old → always in scope
        {
            "id": "recent-1",
            "workspace": "alpha",
            "task": "fresh",
            "status": "completed",
            "session_type": "interactive",
            "model": "claude-opus-4-6",
            "cost_usd": 10.00,
            "input_tokens": 100_000,
            "output_tokens": 50_000,
            "cache_read_tokens": 0,
            "started_at": (now - timedelta(days=1)).isoformat(),
        },
        # 14 days old → inside 30-day window, outside 7-day window
        {
            "id": "mid-1",
            "workspace": "alpha",
            "task": "mid-range",
            "status": "completed",
            "session_type": "interactive",
            "model": "claude-opus-4-6",
            "cost_usd": 25.00,
            "input_tokens": 500_000,
            "output_tokens": 250_000,
            "cache_read_tokens": 0,
            "started_at": (now - timedelta(days=14)).isoformat(),
        },
    ]
    try:
        for r in rows:
            conn.execute(insert(runs_table).values(**r))
        conn.commit()
    finally:
        conn.close()


def _seed_known_correction_row(db_path: Path) -> None:
    """Insert a single run with known tokens for --correct math verification."""
    engine = get_engine(str(db_path))
    conn = get_sa_connection(engine)
    now = datetime.now()
    try:
        conn.execute(
            insert(runs_table).values(
                id="known-1",
                workspace="known",
                task="exact math",
                status="completed",
                session_type="interactive",
                model="claude-opus-4-6",
                cost_usd=5.00,  # wrong/raw
                input_tokens=100_000,
                output_tokens=50_000,
                cache_read_tokens=10_000,
                started_at=(now - timedelta(days=1)).isoformat(),
            )
        )
        conn.commit()
    finally:
        conn.close()


def _setup_env(tmp_path: Path, monkeypatch) -> Path:
    """Standard test setup: scope Path.home + HOME + cwd + init project."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    # costs calls _get_conn() which uses Path.cwd() — chdir so we don't hit
    # the real qualito db.
    monkeypatch.chdir(tmp_path)
    init_project(project_dir=tmp_path, local=True)
    return tmp_path / ".qualito" / "qualito.db"


# ---------------------------------------------------------------------------
# _compute_cost pure function
# ---------------------------------------------------------------------------


class TestComputeCostHelper:
    def test_raw_cost_formula(self):
        pricing = MODEL_PRICING["claude-opus-4-6"]
        # input=100k, output=50k, cache_read=10k
        raw = _compute_cost(pricing, 100_000, 50_000, 10_000, correct=False)
        expected = (
            (100_000 * pricing["input"] / 1_000_000)
            + (50_000 * pricing["output"] / 1_000_000)
            + (10_000 * pricing["cache_read"] / 1_000_000)
        )
        assert abs(raw - expected) < 1e-6

    def test_corrected_cost_applies_1_9x(self):
        pricing = MODEL_PRICING["claude-opus-4-6"]
        corrected = _compute_cost(pricing, 100_000, 50_000, 10_000, correct=True)
        expected = (
            (100_000 * pricing["input"] / 1_000_000)
            + (50_000 * _OUTPUT_UNDERCOUNT_FACTOR * pricing["output"] / 1_000_000)
            + (10_000 * pricing["cache_read"] / 1_000_000)
        )
        assert abs(corrected - expected) < 1e-6
        assert _OUTPUT_UNDERCOUNT_FACTOR == 1.9


# ---------------------------------------------------------------------------
# Default output (no flags)
# ---------------------------------------------------------------------------


class TestCostsDefaultOutput:
    def test_renders_all_sections(self, tmp_path, monkeypatch):
        db_path = _setup_env(tmp_path, monkeypatch)
        _seed_cost_fixture(db_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["costs"])
        assert result.exit_code == 0, result.output
        out = result.output

        assert "Qualito Costs — last 30 days" in out
        assert "Total:" in out
        assert "By workspace" in out
        assert "By model" in out
        assert "Top" in out
        assert "most expensive sessions" in out
        # Disclaimer footer (raw mode, not --correct)
        assert "About these numbers" in out
        assert "27361" in out  # github issue reference
        assert "1.9x" in out
        # All three workspaces should appear in the by-workspace table
        assert "alpha" in out
        assert "bravo" in out
        assert "charlie" in out
        # Both models should appear in the by-model table
        assert "claude-opus-4-6" in out
        assert "claude-sonnet-4-6" in out

    def test_no_waste_references(self, tmp_path, monkeypatch):
        """Task 5: the old DQI/waste framing must be gone from costs output."""
        db_path = _setup_env(tmp_path, monkeypatch)
        _seed_cost_fixture(db_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["costs"])
        assert result.exit_code == 0, result.output
        lowered = result.output.lower()
        assert "waste" not in lowered
        assert "dqi" not in lowered

    def test_empty_data_shows_friendly_message(self, tmp_path, monkeypatch):
        _setup_env(tmp_path, monkeypatch)
        runner = CliRunner()
        result = runner.invoke(cli, ["costs"])
        assert result.exit_code == 0, result.output
        assert "No cost data in the last 30 days" in result.output

    def test_null_cost_rows_excluded(self, tmp_path, monkeypatch):
        """Runs with cost_usd IS NULL must not leak into aggregates."""
        db_path = _setup_env(tmp_path, monkeypatch)
        _seed_cost_fixture(db_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["costs", "--top", "20"])
        assert result.exit_code == 0, result.output
        # Synthetic rows had 9,999,999 output tokens; if they leaked they'd
        # show in the Top table. Also their tasks should never appear.
        assert "synthetic — no cost" not in result.output
        assert "another synthetic" not in result.output
        assert "9999999" not in result.output


# ---------------------------------------------------------------------------
# --correct flag
# ---------------------------------------------------------------------------


class TestCostsCorrect:
    def test_correct_labels_every_cost(self, tmp_path, monkeypatch):
        db_path = _setup_env(tmp_path, monkeypatch)
        _seed_cost_fixture(db_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["costs", "--correct"])
        assert result.exit_code == 0, result.output
        assert "(corrected)" in result.output
        # At least one cost per section should have the corrected label
        assert result.output.count("(corrected)") >= 4
        # Disclaimer switches to corrected footer
        assert "CORRECTED costs" in result.output

    def test_correct_math_matches_formula(self, tmp_path, monkeypatch):
        """Verify the corrected Total line matches (input×in + output×1.9×out + cache×cr) / 1e6."""
        db_path = _setup_env(tmp_path, monkeypatch)
        _seed_known_correction_row(db_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["costs", "--correct"])
        assert result.exit_code == 0, result.output

        pricing = MODEL_PRICING["claude-opus-4-6"]
        expected = (
            (100_000 * pricing["input"] / 1_000_000)
            + (50_000 * 1.9 * pricing["output"] / 1_000_000)
            + (10_000 * pricing["cache_read"] / 1_000_000)
        )
        # Rendered as ~$X.XX (corrected)
        expected_str = f"${expected:.2f}"
        assert expected_str in result.output, (
            f"expected corrected cost {expected_str}, got:\n{result.output}"
        )

    def test_without_correct_uses_raw_cost_usd(self, tmp_path, monkeypatch):
        """Without --correct, the Total must reflect the raw cost_usd column."""
        db_path = _setup_env(tmp_path, monkeypatch)
        _seed_known_correction_row(db_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["costs"])
        assert result.exit_code == 0, result.output
        assert "$5.00" in result.output
        assert "(corrected)" not in result.output


# ---------------------------------------------------------------------------
# --explain flag
# ---------------------------------------------------------------------------


class TestCostsExplain:
    def test_explain_bypasses_db(self, tmp_path, monkeypatch):
        """--explain must not hit the DB. Monkeypatch _get_conn to explode if called."""
        _setup_env(tmp_path, monkeypatch)

        import qualito.cli.main as main_mod

        def _boom(*args, **kwargs):
            raise RuntimeError("--explain must not open a DB connection")

        monkeypatch.setattr(main_mod, "_get_conn", _boom, raising=True)

        runner = CliRunner()
        result = runner.invoke(cli, ["costs", "--explain"])
        assert result.exit_code == 0, result.output

    def test_explain_output_contains_pricing_and_links(self, tmp_path, monkeypatch):
        _setup_env(tmp_path, monkeypatch)
        runner = CliRunner()
        result = runner.invoke(cli, ["costs", "--explain"])
        assert result.exit_code == 0, result.output
        out = result.output
        # Pricing table
        assert "Pricing per million tokens" in out
        assert "claude-opus-4-6" in out
        assert "claude-sonnet-4-6" in out
        assert "claude-haiku-4-5" in out
        # GitHub issue + 1.9 correction mention
        assert "https://github.com/anthropics/claude-code/issues/27361" in out
        assert "1.9x" in out
        # The header from _print_costs_explain
        assert "how cost is calculated" in out


# ---------------------------------------------------------------------------
# Filters: --workspace, --days, --top
# ---------------------------------------------------------------------------


class TestCostsFilters:
    def test_workspace_filter(self, tmp_path, monkeypatch):
        db_path = _setup_env(tmp_path, monkeypatch)
        _seed_cost_fixture(db_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["costs", "--workspace", "alpha"])
        assert result.exit_code == 0, result.output
        out = result.output
        assert "Workspace: alpha" in out
        assert "alpha" in out
        # bravo and charlie should not appear in the By workspace section
        # (they may still be absent from Top since only alpha has data)
        # Scope the assertion to the By workspace section:
        ws_section = out.split("By workspace")[1].split("By model")[0]
        assert "alpha" in ws_section
        assert "bravo" not in ws_section
        assert "charlie" not in ws_section

    def test_days_filter_excludes_old_runs(self, tmp_path, monkeypatch):
        """--days 7 should exclude a 14-day-old run; --days 30 should include it."""
        db_path = _setup_env(tmp_path, monkeypatch)
        _seed_recent_and_old(db_path)

        runner = CliRunner()

        result_7 = runner.invoke(cli, ["costs", "--days", "7"])
        assert result_7.exit_code == 0, result_7.output
        # Only the recent-1 row (1 session)
        assert "1 sessions" in result_7.output
        # 30-day run should be excluded → its task string does not appear
        assert "mid-range" not in result_7.output

        result_30 = runner.invoke(cli, ["costs", "--days", "30"])
        assert result_30.exit_code == 0, result_30.output
        assert "2 sessions" in result_30.output
        assert "mid-range" in result_30.output

    def test_top_limits_session_table(self, tmp_path, monkeypatch):
        """--top 5 caps the Top N session rows to exactly 5."""
        db_path = _setup_env(tmp_path, monkeypatch)

        # Seed 8 cost-bearing runs so --top 5 has something to cap
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        now = datetime.now()
        try:
            for i in range(8):
                conn.execute(
                    insert(runs_table).values(
                        id=f"top-{i}",
                        workspace=f"ws{i % 3}",
                        task=f"task number {i}",
                        status="completed",
                        session_type="interactive",
                        model="claude-opus-4-6",
                        cost_usd=10.0 + i,  # unique costs for stable ordering
                        input_tokens=100_000,
                        output_tokens=50_000,
                        cache_read_tokens=0,
                        started_at=(now - timedelta(hours=i)).isoformat(),
                    )
                )
            conn.commit()
        finally:
            conn.close()

        runner = CliRunner()
        result = runner.invoke(cli, ["costs", "--top", "5"])
        assert result.exit_code == 0, result.output
        out = result.output

        assert "Top 5 most expensive sessions" in out
        # Count the rendered rows in the Top section by counting task markers
        top_section = out.split("Top 5 most expensive sessions")[1]
        # Strip at the disclaimer start — the "About these numbers" header
        top_section = top_section.split("About these numbers")[0]
        row_count = sum(
            1 for line in top_section.splitlines() if line.strip().startswith("20")
        )
        assert row_count == 5, f"expected 5 rows, got {row_count}:\n{top_section}"
