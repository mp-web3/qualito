"""Qualito CLI entry point."""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import click
from sqlalchemy import and_, case, func, select

from qualito import __version__

from qualito.core.db import (
    get_engine,
    get_sa_connection,
    runs_table,
)


def _get_conn(project_dir: Path | None = None):
    """Resolve config and return a SA connection + config.

    Checks for global ~/.qualito/ first, then local .qualito/ for backward compat.
    """
    if project_dir is None:
        project_dir = Path.cwd()

    global_dir = Path.home() / ".qualito"
    local_dir = project_dir / ".qualito"

    if not global_dir.exists() and not local_dir.exists():
        click.echo("Qualito not initialized. Run 'qualito init' first.")
        raise SystemExit(1)

    from qualito.config import load_config

    config = load_config(project_dir)
    db_path = config.db_path
    if not db_path.is_absolute():
        db_path = project_dir / db_path

    if not db_path.exists():
        # Try creating the DB (first run after init)
        db_path.parent.mkdir(parents=True, exist_ok=True)

    engine = get_engine(str(db_path))
    conn = get_sa_connection(engine)
    return conn, config


def _since_date(days: int) -> str:
    """Return ISO date string for N days ago."""
    return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")


def _fmt_cost(val) -> str:
    """Format a cost value as $X.XX."""
    if val is None:
        return "$0.00"
    return f"${val:.2f}"


def _fmt_pct(val) -> str:
    """Format a value as percentage."""
    if val is None:
        return "N/A"
    return f"{val:.1f}%"


def _fmt_tokens(n) -> str:
    """Format a token count humanized (e.g. '4.2M', '125k', '0.05M').

    Used by status and costs commands for compact column layouts.
    """
    if n is None:
        return "0"
    try:
        n = int(n)
    except (TypeError, ValueError):
        return "0"
    if n == 0:
        return "0"
    if n >= 1_000_000:
        val = n / 1_000_000
        if val >= 100:
            return f"{val:.0f}M"
        if val >= 10:
            return f"{val:.1f}M"
        return f"{val:.2f}M"
    if n >= 1_000:
        val = n / 1_000
        if val >= 100:
            return f"{val:.0f}k"
        return f"{val:.1f}k"
    return str(n)


def _fmt_relative_time(iso_str) -> str:
    """Format an ISO date/time string as a relative time (e.g. '2 hours ago').

    Accepts None or empty string and returns 'never'.
    """
    if not iso_str:
        return "never"
    try:
        # Handle both date-only and full ISO strings; strip timezone 'Z'
        s = str(iso_str).replace("Z", "+00:00")
        ts = datetime.fromisoformat(s)
        if ts.tzinfo is not None:
            ts = ts.replace(tzinfo=None)
    except (TypeError, ValueError):
        return str(iso_str)

    now = datetime.now()
    delta = now - ts
    secs = delta.total_seconds()
    if secs < 0:
        return "just now"
    if secs < 60:
        return "just now"
    if secs < 3600:
        mins = int(secs // 60)
        return f"{mins} minute{'s' if mins != 1 else ''} ago"
    if secs < 86400:
        hours = int(secs // 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    if secs < 86400 * 7:
        days = int(secs // 86400)
        return f"{days} day{'s' if days != 1 else ''} ago"
    if secs < 86400 * 30:
        weeks = int(secs // (86400 * 7))
        return f"{weeks} week{'s' if weeks != 1 else ''} ago"
    if secs < 86400 * 365:
        months = int(secs // (86400 * 30))
        return f"{months} month{'s' if months != 1 else ''} ago"
    years = int(secs // (86400 * 365))
    return f"{years} year{'s' if years != 1 else ''} ago"


@click.group()
@click.version_option(version=__version__)
def cli():
    """Qualito — Measure and improve AI agent delegation quality."""
    pass


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------


def _get_workspace_summary(conn) -> list[dict]:
    """Query per-workspace summary: run count, session types, tokens, cost, last activity."""
    rows = conn.execute(
        select(
            runs_table.c.workspace,
            func.count().label("run_count"),
            func.sum(case((runs_table.c.session_type == "interactive", 1), else_=0)).label("interactive"),
            func.sum(case((runs_table.c.session_type == "delegated", 1), else_=0)).label("delegated"),
            func.coalesce(func.sum(runs_table.c.cost_usd), 0).label("total_cost"),
            func.coalesce(func.sum(runs_table.c.input_tokens), 0).label("input_tokens"),
            func.coalesce(func.sum(runs_table.c.output_tokens), 0).label("output_tokens"),
            func.coalesce(func.sum(runs_table.c.cache_read_tokens), 0).label("cache_read_tokens"),
            func.max(runs_table.c.started_at).label("last_started_at"),
        )
        .group_by(runs_table.c.workspace)
        .order_by(func.count().desc())
    ).mappings().fetchall()
    return [dict(r) for r in rows]


def _dqi_label(score: float | None) -> str:
    """Return a human-readable DQI label."""
    if score is None:
        return "N/A"
    if score >= 0.8:
        return "Excellent"
    if score >= 0.7:
        return "Good"
    if score >= 0.5:
        return "Fair"
    return "Needs attention"


def _parse_selection(text: str, max_n: int) -> list[int] | None:
    """Parse user selection like '1,3,5' or 'all' or 'none'.

    Returns list of 0-based indices, or None for 'none'.
    """
    text = text.strip().lower()
    if text == "all":
        return list(range(max_n))
    if text == "none":
        return None
    indices = []
    for part in text.split(","):
        part = part.strip()
        if part.isdigit():
            idx = int(part) - 1  # 1-based to 0-based
            if 0 <= idx < max_n:
                indices.append(idx)
    return indices if indices else None


def _compute_date_range(choice: str) -> tuple[str, str] | None:
    """Convert date range choice to (start, end) ISO strings or None for all time."""
    now = datetime.now()
    if choice == "b":
        start = (now - timedelta(days=30)).strftime("%Y-%m-%d")
        return (start, now.strftime("%Y-%m-%d"))
    elif choice == "c":
        start = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        return (start, now.strftime("%Y-%m-%d"))
    elif choice == "d":
        start = click.prompt("Start date (YYYY-MM-DD)")
        end = click.prompt("End date (YYYY-MM-DD)", default=now.strftime("%Y-%m-%d"))
        return (start, end)
    return None  # all time


def safe_add_mcp_to_claude_json() -> bool:
    """Add qualito MCP server to ~/.claude.json if not already present.

    Returns True if added, False otherwise.
    """
    target = Path.home() / ".claude.json"
    mcp_entry = {
        "command": "uvx",
        "args": ["--from", "qualito[mcp]", "qualito-mcp"],
    }

    if not target.exists():
        data = {"mcpServers": {"qualito": mcp_entry}}
        tmp = target.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, indent=2) + "\n")
        os.rename(tmp, target)
        click.echo("Added qualito to ~/.claude.json. Restart Claude Code to activate.")
        return True

    raw = target.read_text()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        click.echo("Warning: ~/.claude.json is malformed. Add MCP server manually:")
        click.echo('  "mcpServers": { "qualito": { "command": "uvx", '
                   '"args": ["--from", "qualito[mcp]", "qualito-mcp"] } }')
        return False

    if not isinstance(data, dict):
        data = {"mcpServers": {}}

    mcp_servers = data.get("mcpServers", {})
    if "qualito" in mcp_servers:
        click.echo("MCP server already configured in ~/.claude.json.")
        return False

    if "mcpServers" not in data:
        data["mcpServers"] = {}
    data["mcpServers"]["qualito"] = mcp_entry

    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n")
    os.rename(tmp, target)
    click.echo("Added qualito to ~/.claude.json. Restart Claude Code to activate.")
    return True


def _is_uvx() -> bool:
    """Check if running via uvx (temporary environment)."""
    exe = sys.executable or ""
    # uvx runs from a temp/cache directory, not a standard install path
    return "uvx" in exe or "/tmp/" in exe or "/.cache/uv/" in exe


def _print_next_steps():
    """Print next steps after setup completes."""
    prefix = "uvx qualito" if _is_uvx() else "qualito"
    click.echo("\nNext steps:")
    click.echo(f"  {prefix} login    # connect to cloud dashboard")
    click.echo(f"  {prefix} sync     # push data to cloud")
    if _is_uvx():
        click.echo()
        click.echo("Or install permanently: uv tool install qualito")


def _display_results_table(summaries: list[dict]):
    """Display workspace results table."""
    if not summaries:
        click.echo("No data to display.")
        return

    click.echo(f"\n{'Workspace':<20} {'Sessions':>8} {'Interactive':>12} {'Delegated':>10} {'Cost (est.)':>12}")
    click.echo("-" * 66)
    total_runs = 0
    total_interactive = 0
    total_delegated = 0
    total_cost = 0
    for s in summaries:
        ws = (s["workspace"] or "?")[:18]
        runs = s["run_count"]
        interactive = s.get("interactive", 0) or 0
        delegated = s.get("delegated", 0) or 0
        cost = s["total_cost"]
        total_runs += runs
        total_interactive += interactive
        total_delegated += delegated
        total_cost += cost
        click.echo(f"{ws:<20} {runs:>8} {interactive:>12} {delegated:>10} {'~' + _fmt_cost(cost):>12}")
    click.echo("-" * 66)
    click.echo(f"{'Total':<20} {total_runs:>8} {total_interactive:>12} {total_delegated:>10} {'~' + _fmt_cost(total_cost):>12}")


# ---------------------------------------------------------------------------
# qualito setup
# ---------------------------------------------------------------------------


def _report_setup_progress(api_url: str, token: str, step: str, detail: str = ""):
    """Report setup progress to the cloud API (best-effort)."""
    import urllib.request

    try:
        data = json.dumps({"token": token, "step": step, "detail": detail}).encode()
        req = urllib.request.Request(
            f"{api_url}/api/setup/progress",
            data=data,
            method="POST",
        )
        req.add_header("Content-Type", "application/json")
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # Best-effort, don't fail setup


def _run_interactive_setup_first_run():
    """First-run setup: init, discover, import, score, MCP config."""
    from qualito.config import init_project
    from qualito.importer import discover_all_projects, import_project

    # 1. Init global directory
    click.echo("Initializing Qualito...\n")
    config, qualito_dir = init_project(local=False)
    db_path = config.db_path
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path

    # 2. Discover projects
    projects = discover_all_projects()
    if not projects:
        click.echo("No Claude Code projects found in ~/.claude/projects/")
        click.echo("Use Claude Code in a project first, then run 'qualito setup' again.")
        return

    # 3. Display table
    click.echo(f"Found {len(projects)} Claude Code project(s):\n")
    click.echo(f"  {'#':<4} {'Project':<30} {'Sessions':>10}")
    click.echo("  " + "-" * 48)
    for i, p in enumerate(projects, 1):
        click.echo(f"  {i:<4} {p['name']:<30} {p['session_count']:>10}")
    click.echo()

    # 4. Select projects
    selection_text = click.prompt(
        "Select projects [1-N, all, none]", default="all"
    )
    selected_indices = _parse_selection(selection_text, len(projects))
    if selected_indices is None:
        click.echo("No projects selected.")
        return
    selected = [projects[i] for i in selected_indices]

    # 5. Date range
    click.echo("\nImport date range?")
    click.echo("  a) All time")
    click.echo("  b) Last 30 days")
    click.echo("  c) Last 7 days")
    click.echo("  d) Custom")
    date_choice = click.prompt("Choice", default="a")
    date_range = _compute_date_range(date_choice)

    # 6. Workspace naming
    click.echo()
    workspace_names = {}
    for p in selected:
        default_name = p["name"]
        name = click.prompt(
            f"  {p['name']} → workspace name",
            default=default_name,
        )
        workspace_names[p["folder"]] = name

    # 7. Import with progress
    click.echo("\nImporting sessions...")
    engine = get_engine(str(db_path))
    conn = get_sa_connection(engine)
    try:
        total_imported = 0
        total_skipped = 0
        with click.progressbar(selected, label="  Importing", show_pos=True) as bar:
            for p in bar:
                ws_name = workspace_names[p["folder"]]
                result = import_project(
                    project_key=p["folder"],
                    workspace_name=ws_name,
                    conn=conn,
                    date_range=date_range,
                )
                total_imported += result["imported"]
                total_skipped += result["skipped"]

        click.echo(f"\n  Imported {total_imported} sessions, skipped {total_skipped}")

        # 8. Display results
        if total_imported > 0:
            summaries = _get_workspace_summary(conn)
            _display_results_table(summaries)
    finally:
        conn.close()

    # 9. MCP config
    click.echo()
    if click.confirm("Add MCP server for in-editor access?", default=True):
        safe_add_mcp_to_claude_json()

    click.echo("\nSetup complete. Run 'qualito setup' anytime to add more projects.")
    _print_next_steps()


def _run_interactive_setup_rerun():
    """Re-run setup: show current state, offer options."""
    from qualito.config import load_config
    from qualito.importer import discover_all_projects, import_project

    config = load_config()
    db_path = config.db_path
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path

    engine = get_engine(str(db_path))
    conn = get_sa_connection(engine)
    try:
        # Show current state
        run_count = conn.execute(
            select(func.count().label("n")).select_from(runs_table)
        ).mappings().fetchone()["n"]
        ws_count = conn.execute(
            select(func.count(runs_table.c.workspace.distinct()).label("n"))
        ).mappings().fetchone()["n"]
        click.echo(
            f"Found existing Qualito config. "
            f"{ws_count} workspace(s), {run_count} runs imported.\n"
        )

        # Menu
        click.echo("  a) Add more projects")
        click.echo("  b) Re-import everything")
        click.echo("  c) Re-score existing runs")
        click.echo("  d) Exit")
        choice = click.prompt("Choice", default="a")

        if choice == "d":
            return

        if choice == "c":
            # Re-score existing runs
            click.echo("\nRe-scoring existing runs...")
            from qualito.core.dqi import store_dqi
            from qualito.core.evaluator import auto_evaluate

            runs = conn.execute(select(runs_table.c.id)).mappings().fetchall()
            with click.progressbar(runs, label="  Scoring", show_pos=True) as bar:
                for r in bar:
                    auto_evaluate(r["id"], conn=conn)
                    store_dqi(r["id"], conn=conn)

            summaries = _get_workspace_summary(conn)
            _display_results_table(summaries)
            return

        if choice == "b":
            # Re-import everything (idempotent)
            projects = discover_all_projects()
            if not projects:
                click.echo("No Claude Code projects found.")
                return

            click.echo(f"\nRe-importing {len(projects)} project(s)...")
            total_imported = 0
            total_skipped = 0
            with click.progressbar(projects, label="  Importing", show_pos=True) as bar:
                for p in bar:
                    result = import_project(
                        project_key=p["folder"],
                        workspace_name=p["name"],
                        conn=conn,
                    )
                    total_imported += result["imported"]
                    total_skipped += result["skipped"]

            click.echo(
                f"\n  Imported {total_imported} new sessions, "
                f"skipped {total_skipped}"
            )
            summaries = _get_workspace_summary(conn)
            _display_results_table(summaries)
            return

        if choice == "a":
            # Add more — filter out already-imported workspaces
            projects = discover_all_projects()
            if not projects:
                click.echo("No Claude Code projects found.")
                return

            existing_ws = {
                r["workspace"]
                for r in conn.execute(
                    select(runs_table.c.workspace.distinct())
                ).mappings().fetchall()
            }
            new_projects = [p for p in projects if p["name"] not in existing_ws]

            if not new_projects:
                click.echo("All discovered projects are already imported.")
                return

            click.echo(f"\nFound {len(new_projects)} new project(s):\n")
            click.echo(f"  {'#':<4} {'Project':<30} {'Sessions':>10}")
            click.echo("  " + "-" * 48)
            for i, p in enumerate(new_projects, 1):
                click.echo(
                    f"  {i:<4} {p['name']:<30} {p['session_count']:>10}"
                )
            click.echo()

            selection_text = click.prompt(
                "Select projects [1-N, all, none]", default="all"
            )
            selected_indices = _parse_selection(
                selection_text, len(new_projects)
            )
            if selected_indices is None:
                click.echo("No projects selected.")
                return
            selected = [new_projects[i] for i in selected_indices]

            # Workspace naming
            workspace_names = {}
            for p in selected:
                name = click.prompt(
                    f"  {p['name']} → workspace name",
                    default=p["name"],
                )
                workspace_names[p["folder"]] = name

            click.echo("\nImporting...")
            total_imported = 0
            with click.progressbar(
                selected, label="  Importing", show_pos=True
            ) as bar:
                for p in bar:
                    ws_name = workspace_names[p["folder"]]
                    result = import_project(
                        project_key=p["folder"],
                        workspace_name=ws_name,
                        conn=conn,
                    )
                    total_imported += result["imported"]

            click.echo(f"\n  Imported {total_imported} new sessions")
            summaries = _get_workspace_summary(conn)
            _display_results_table(summaries)
    finally:
        conn.close()


@cli.command(help="First-time setup: import sessions, configure MCP, optional cloud sync")
@click.argument("token", required=False)
def setup(token):
    """Set up Qualito — discover and import Claude Code sessions.

    With TOKEN: import locally, validate with cloud, and auto-sync.
    Without TOKEN: interactive guided setup.
    """
    if token:
        import urllib.error
        import urllib.request

        from qualito.config import init_project
        from qualito.importer import discover_all_projects, import_project

        config, qualito_dir = init_project(local=False)
        db_path = config.db_path
        if not db_path.is_absolute():
            db_path = Path.cwd() / db_path

        projects = discover_all_projects()
        if not projects:
            click.echo("No Claude Code projects found in ~/.claude/projects/")
            return

        click.echo(f"Found {len(projects)} project(s). Importing all...\n")
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        total_imported = 0
        total_skipped = 0
        try:
            for p in projects:
                result = import_project(
                    project_key=p["folder"],
                    workspace_name=p["name"],
                    conn=conn,
                )
                total_imported += result["imported"]
                total_skipped += result["skipped"]
        finally:
            conn.close()

        click.echo(f"Imported {total_imported} sessions locally.")

        if total_imported > 0:
            engine = get_engine(str(db_path))
            summary_conn = get_sa_connection(engine)
            try:
                summaries = _get_workspace_summary(summary_conn)
                _display_results_table(summaries)
            finally:
                summary_conn.close()

        # Auto-configure MCP
        safe_add_mcp_to_claude_json()

        # Attempt cloud validation + sync
        from qualito.cloud import DEFAULT_API_URL

        api_url = os.environ.get("QUALITO_API_URL", DEFAULT_API_URL)

        click.echo("\nConnecting to cloud...")
        try:
            req_data = json.dumps({"token": token}).encode()
            req = urllib.request.Request(
                f"{api_url}/api/setup/validate",
                data=req_data,
                method="POST",
            )
            req.add_header("Content-Type", "application/json")
            req.add_header("User-Agent", "qualito-cli")

            with urllib.request.urlopen(req, timeout=10) as resp:
                validate_result = json.loads(resp.read().decode())

            api_key = validate_result["api_key"]

            # Save credentials
            from qualito.cloud import save_credentials

            save_credentials(api_key, api_url)

            # Report progress (best-effort, updates dashboard SSE tracker)
            _report_setup_progress(api_url, token, "connected")
            _report_setup_progress(
                api_url, token, "importing", f"{total_imported} sessions"
            )
            _report_setup_progress(api_url, token, "scoring")

            # Attempt sync
            if total_imported > 0:
                from qualito.cloud import sync_incidents, sync_runs

                # Workspace picker for free plan users with >3 workspaces
                selected_workspaces = None
                engine = get_engine(str(db_path))
                picker_conn = get_sa_connection(engine)
                try:
                    ws_rows = picker_conn.execute(
                        select(
                            runs_table.c.workspace,
                            func.count().label("cnt"),
                        ).group_by(runs_table.c.workspace).order_by(runs_table.c.workspace)
                    ).fetchall()
                    all_ws = [row[0] for row in ws_rows]
                    ws_counts = {row[0]: row[1] for row in ws_rows}

                    if len(all_ws) > 3:
                        # Check if user is on free plan
                        is_free = True
                        try:
                            me_req = urllib.request.Request(
                                f"{api_url}/api/auth/me",
                                method="GET",
                            )
                            me_req.add_header("Authorization", f"Bearer {api_key}")
                            me_req.add_header("User-Agent", "qualito-cli")
                            with urllib.request.urlopen(me_req, timeout=10) as me_resp:
                                me_data = json.loads(me_resp.read().decode())
                                if me_data.get("plan") == "pro":
                                    is_free = False
                        except Exception:
                            pass

                        if is_free:
                            click.echo(
                                "\nFree plan syncs up to 3 workspaces. "
                                "Select which to sync:"
                            )
                            for i, ws in enumerate(all_ws, 1):
                                click.echo(f"  [{i}] {ws} ({ws_counts[ws]} runs)")
                            selection = click.prompt(
                                "\nEnter numbers separated by commas (e.g. 1,2,3)",
                                type=str,
                            )
                            indices = [
                                int(s.strip()) for s in selection.split(",")
                                if s.strip().isdigit()
                            ]
                            selected_workspaces = [
                                all_ws[i - 1] for i in indices
                                if 1 <= i <= len(all_ws)
                            ][:3]
                            if not selected_workspaces:
                                click.echo("No valid workspaces selected. Skipping sync.")
                                selected_workspaces = []
                finally:
                    picker_conn.close()

                if selected_workspaces is not None and len(selected_workspaces) == 0:
                    pass  # User selected nothing, skip sync
                else:
                    click.echo("\nSyncing to cloud...")
                    engine = get_engine(str(db_path))
                    sync_conn = get_sa_connection(engine)

                    current_ws = {"name": None}

                    def _on_batch(ws, batch_num, total_batches, runs_in_batch):
                        if current_ws["name"] != ws:
                            click.echo(f"\n  {ws}")
                            current_ws["name"] = ws
                        click.echo(
                            f"    Batch {batch_num}/{total_batches} — sent {runs_in_batch} runs ✓"
                        )

                    def _on_workspace_done(ws, ws_synced):
                        click.echo(f"  ✓ {ws} synced ({ws_synced} sessions)")

                    try:
                        run_result = sync_runs(
                            sync_conn,
                            workspaces=selected_workspaces,
                            on_batch=_on_batch,
                            on_workspace_done=_on_workspace_done,
                        )
                        inc_result = sync_incidents(sync_conn)
                        click.echo(
                            f"\nSynced {run_result['synced']} runs to cloud."
                        )
                    finally:
                        sync_conn.close()

            # Report complete (triggers dashboard reload via SSE)
            try:
                _report_setup_progress(api_url, token, "complete")
            except Exception:
                pass  # Non-critical — data is already synced

            click.echo(
                "\nView your runs at: https://app.qualito.ai/runs"
            )

        except Exception as e:
            click.echo(f"\nSync failed: {e}")
            click.echo(
                "Your data is imported locally. Some runs may have synced — "
                "run `qualito sync` to resume (already-synced runs will be skipped)."
            )
    else:
        # Interactive setup
        global_dir = Path.home() / ".qualito"
        if global_dir.exists() and (global_dir / "config.toml").exists():
            _run_interactive_setup_rerun()
        else:
            _run_interactive_setup_first_run()


@cli.command(help="See your local and cloud sync state")
@click.option("--dir", "project_dir", type=click.Path(exists=True, path_type=Path),
              default=None, help="Project directory (default: cwd)")
def status(project_dir: Path | None):
    """Show Qualito status: local sessions, cloud sync, per-workspace breakdown."""
    if project_dir is None:
        project_dir = Path.cwd()

    global_dir = Path.home() / ".qualito"
    local_dir = project_dir / ".qualito"

    if not local_dir.exists() and not global_dir.exists():
        click.echo("Qualito not initialized. Run 'qualito init' first.")
        raise SystemExit(1)

    from qualito.config import load_config

    config = load_config(project_dir)

    db_path = config.db_path
    if not db_path.is_absolute():
        db_path = project_dir / db_path

    # Display-friendly DB path: replace home prefix with ~
    try:
        display_db = "~/" + str(db_path.relative_to(Path.home()))
    except ValueError:
        display_db = str(db_path)

    click.echo("Qualito Status\n")

    # -----------------------------------------------------------------------
    # Local section
    # -----------------------------------------------------------------------
    click.echo("Local")
    click.echo(f"  Database: {display_db}")

    summaries: list[dict] = []
    total_runs = 0
    total_interactive = 0
    total_delegated = 0

    if db_path.exists():
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            summaries = _get_workspace_summary(conn)
        finally:
            conn.close()

        for s in summaries:
            total_runs += int(s.get("run_count") or 0)
            total_interactive += int(s.get("interactive") or 0)
            total_delegated += int(s.get("delegated") or 0)

        click.echo(f"  Sessions: {total_runs} imported")
        click.echo(f"    Interactive: {total_interactive}")
        click.echo(f"    Delegated: {total_delegated}")
        click.echo(f"  Workspaces: {len(summaries)}")
    else:
        click.echo("  Sessions: 0 imported (no database)")
        click.echo("    Interactive: 0")
        click.echo("    Delegated: 0")
        click.echo("  Workspaces: 0")

    # Per-workspace table
    if summaries:
        click.echo("")
        click.echo("  Per workspace:")
        header = (
            f"    {'workspace':<16} {'sessions':>8} {'in tokens':>11} "
            f"{'out tokens':>12} {'~cost':>10}   {'last':<}"
        )
        click.echo(header)
        for s in summaries:
            ws = str(s.get("workspace") or "")[:16]
            sessions = int(s.get("run_count") or 0)
            in_tok = _fmt_tokens(s.get("input_tokens"))
            out_tok = _fmt_tokens(s.get("output_tokens"))
            cost_val = float(s.get("total_cost") or 0.0)
            cost_str = f"~${cost_val:,.2f}" if cost_val < 1000 else f"~${cost_val:,.0f}"
            last = _fmt_relative_time(s.get("last_started_at"))
            click.echo(
                f"    {ws:<16} {sessions:>8} {in_tok:>11} "
                f"{out_tok:>12} {cost_str:>10}   {last}"
            )

    # -----------------------------------------------------------------------
    # Cloud section
    # -----------------------------------------------------------------------
    click.echo("")

    from qualito.cloud import CloudError, load_credentials

    creds = load_credentials()
    if not creds:
        click.echo("Cloud: not logged in. Run: qualito login")
    else:
        from qualito.cloud import fetch_synced_workspaces, fetch_user

        user_info: dict = {}
        synced: list[dict] = []
        cloud_error: str | None = None
        try:
            user_info = fetch_user()
        except CloudError as e:
            cloud_error = str(e)
        if cloud_error is None:
            try:
                synced = fetch_synced_workspaces()
            except CloudError as e:
                cloud_error = str(e)

        if cloud_error is not None:
            click.echo(f"Cloud: error reaching api.qualito.ai ({cloud_error})")
        else:
            email = user_info.get("email") or "unknown"
            plan = (user_info.get("plan") or "free").lower()
            click.echo(f"Cloud ({email}, {plan} plan)")

            if synced:
                synced_names = sorted(
                    [str(w.get("workspace_name") or "") for w in synced if w.get("workspace_name")]
                )
                synced_sessions = sum(int(w.get("session_count") or 0) for w in synced)
                last_sync_iso = max(
                    (w.get("last_synced_at") for w in synced if w.get("last_synced_at")),
                    default=None,
                )

                click.echo(f"  Synced workspaces: {', '.join(synced_names)}")
                click.echo(f"  Synced sessions: {synced_sessions}")
                click.echo(f"  Last sync: {_fmt_relative_time(last_sync_iso)}")
                click.echo("  Dashboard: https://app.qualito.ai/runs")

                local_names = {
                    str(s.get("workspace") or "") for s in summaries if s.get("workspace")
                }
                synced_set = set(synced_names)
                local_only = sorted(local_names - synced_set)
                if local_only:
                    click.echo("")
                    click.echo("Local-only workspaces (not synced):")
                    click.echo(f"  {', '.join(local_only)}")

                if plan == "free":
                    click.echo("")
                    click.echo("To sync more workspaces (free plan limit: 3):")
                    click.echo("  - Upgrade to Pro: https://app.qualito.ai/settings")
                    click.echo("  - Or unsync: qualito sync --unsync <workspace>")
            else:
                click.echo("  No workspaces synced yet. Run: qualito sync")

    # -----------------------------------------------------------------------
    # Cost disclaimer footer
    # -----------------------------------------------------------------------
    click.echo("")
    click.echo(
        "⚠  Costs are estimates. Claude Code session files undercount output_tokens"
    )
    click.echo(
        "   ~1.9x (upstream bug, never fixed). Reliable for comparing workspaces"
    )
    click.echo(
        "   and tracking trends, but absolute totals are understated."
    )
    click.echo("   Run `qualito costs --explain` for details.")


# ---------------------------------------------------------------------------
# dqi import
# ---------------------------------------------------------------------------

@cli.command(
    name="import",
    help="Import Claude Code sessions for measurement (--force to re-process)",
)
@click.option("--dir", "project_dir", type=click.Path(exists=True, path_type=Path),
              default=None, help="Project directory (default: cwd)")
@click.option("--workspace", default=None, help="Override workspace name")
@click.option("--all-projects", is_flag=True, default=False,
              help="Import all discovered Claude Code projects")
@click.option("--force", is_flag=True, default=False,
              help="Re-process existing sessions with current extraction logic")
def import_sessions(
    project_dir: Path | None,
    workspace: str | None,
    all_projects: bool,
    force: bool,
):
    """Import existing Claude Code sessions into Qualito."""
    if project_dir is None:
        project_dir = Path.cwd()

    from qualito.config import load_config

    config = load_config(project_dir)
    db_path = config.db_path
    if not db_path.is_absolute():
        db_path = project_dir / db_path

    if force:
        if not db_path.exists():
            click.echo("No Qualito database found. Run 'qualito setup' first.")
            raise SystemExit(1)

        from qualito.importer import reimport_all

        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            click.echo("Re-importing all sessions with updated extraction...")
            click.echo("This will delete and re-import all existing run records.\n")

            result = reimport_all(conn)

            imported = result["interactive"] + result["delegated"]
            click.echo(f"Re-imported {imported} sessions.")
            click.echo(f"  Interactive: {result['interactive']}")
            click.echo(f"  Delegated:   {result['delegated']}")
            click.echo(f"  Skipped (VS Code): {result['skipped_vscode']}")
            click.echo(
                f"  Skipped (empty/unknown): "
                f"{result['skipped_unknown'] + result['skipped_empty']}"
            )

            if imported > 0:
                summaries = _get_workspace_summary(conn)
                _display_results_table(summaries)
        finally:
            conn.close()
        return

    if all_projects:
        # Import all discovered projects into the global DB
        from qualito.importer import discover_all_projects, import_project

        projects = discover_all_projects()
        if not projects:
            click.echo("No Claude Code projects found in ~/.claude/projects/")
            return

        click.echo(f"Found {len(projects)} project(s):")
        for p in projects:
            click.echo(f"  {p['name']} ({p['session_count']} sessions)")

        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        try:
            total_imported = 0
            total_skipped = 0
            total_cost = 0.0

            for p in projects:
                ws_name = workspace or p["name"]
                result = import_project(
                    project_key=p["folder"],
                    workspace_name=ws_name,
                    conn=conn,
                )
                total_imported += result["imported"]
                total_skipped += result["skipped"]
                total_cost += result["total_cost"]
                if result["imported"] > 0:
                    click.echo(
                        f"  {p['name']}: imported {result['imported']}, "
                        f"skipped {result['skipped']}, "
                        f"cost ${result['total_cost']:.2f}"
                    )

            click.echo(f"\nTotal: imported {total_imported}, "
                       f"skipped {total_skipped}, cost ${total_cost:.2f}")
        finally:
            conn.close()
        return

    # Single-project import (original behavior)
    global_dir = Path.home() / ".qualito"
    local_dir = project_dir / ".qualito"
    if not global_dir.exists() and not local_dir.exists():
        click.echo("Qualito not initialized. Run 'qualito init' first.")
        raise SystemExit(1)

    from qualito.importer import find_session_files, import_all

    ws = workspace or config.workspace or "default"

    # Show discoverable sessions first
    files = find_session_files(project_dir)
    if not files:
        click.echo(f"No Claude Code sessions found for {project_dir}")
        click.echo("Sessions are stored in ~/.claude/projects/")
        return

    click.echo(f"Found {len(files)} session file(s) for {project_dir}")

    engine = get_engine(str(db_path))
    conn = get_sa_connection(engine)
    try:
        result = import_all(conn, project_dir=project_dir, workspace=ws)

        click.echo("\nImport Summary")
        click.echo("=" * 40)
        click.echo(f"  Imported:    {result['imported']}")
        click.echo(f"  Skipped:     {result['skipped']}")
        click.echo(f"  Total cost:  ${result['total_cost']:.2f}")
        if result["imported"] > 0:
            click.echo(f"  Avg DQI:     {result['avg_dqi']:.3f}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# qualito costs
# ---------------------------------------------------------------------------

# Empirical output_tokens undercount factor (upstream bug #27361).
# 128-call Opus session: 23,725 recorded vs 45,050 re-tokenized → 1.9x.
_OUTPUT_UNDERCOUNT_FACTOR = 1.9


def _compute_cost(
    pricing: dict,
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int,
    correct: bool,
) -> float:
    """Recompute cost from tokens with optional 1.9x correction on output."""
    out = output_tokens * _OUTPUT_UNDERCOUNT_FACTOR if correct else output_tokens
    return (
        (input_tokens * pricing["input"] / 1_000_000)
        + (out * pricing["output"] / 1_000_000)
        + (cache_read_tokens * pricing["cache_read"] / 1_000_000)
    )


def _print_costs_explain(model_pricing: dict):
    """Print pricing constants + upstream bug explanation, then return."""
    click.echo()
    click.echo("qualito costs — how cost is calculated and why it's an estimate")
    click.echo()
    click.echo("How cost is computed")
    click.echo("  cost_usd = (input_tokens   × input_price")
    click.echo("              + output_tokens × output_price")
    click.echo("              + cache_read    × cache_read_price) / 1,000,000")
    click.echo()
    click.echo("  Pricing per million tokens:")
    display_rows = [
        ("claude-opus-4-6", model_pricing["claude-opus-4-6"]),
        ("claude-sonnet-4-6", model_pricing["claude-sonnet-4-6"]),
        ("claude-haiku-4-5", model_pricing["claude-haiku-4-5-20251001"]),
    ]
    for label, p in display_rows:
        i = f"${p['input']:.2f}"
        o = f"${p['output']:.2f}"
        cr = f"${p['cache_read']:.2f}"
        click.echo(
            f"    {label:<20} input {i:>6}   output {o:>6}   cache_read {cr:>6}"
        )
    click.echo()
    click.echo("  Cost is derived from tokens — Claude Code does not record cost directly.")
    click.echo("  qualito reads token counts from your local Claude Code session files")
    click.echo("  (~/.claude/projects/) and applies the model-specific pricing above.")
    click.echo()
    click.echo("The upstream bug")
    click.echo("  Claude Code's session JSONL files do not record the final message_stop")
    click.echo("  event from the Anthropic API. The output_tokens field is captured from")
    click.echo("  mid-stream snapshots instead of the final tally, causing a ~1.9x")
    click.echo("  undercount on output_tokens.")
    click.echo()
    click.echo("  Empirical measurement (from the original bug report): a 128-call Opus")
    click.echo("  session recorded 23,725 output tokens in JSONL vs 45,050 tokens when")
    click.echo("  the generated content was re-tokenized with tiktoken — a 1.9x ratio.")
    click.echo()
    click.echo("  Bug report (auto-closed without a fix on 2026-03-24):")
    click.echo("    https://github.com/anthropics/claude-code/issues/27361")
    click.echo()
    click.echo("  No upstream fix is available. There are three known duplicate reports")
    click.echo("  (#22671, #22686, #25941). The bug affects every tool that derives cost")
    click.echo("  from Claude Code session files, not just qualito.")
    click.echo()
    click.echo("What's affected, what isn't")
    click.echo("  Affected:    output_tokens only")
    click.echo("  Accurate:    input_tokens, cache_read_input_tokens")
    click.echo()
    click.echo("  input and cache_read are set at request start, before streaming, and")
    click.echo("  recorded correctly. Only the streaming output count is wrong.")
    click.echo()
    click.echo("Why it matters more than it sounds")
    click.echo("  For Opus, output costs $75 per million tokens versus $15 for input — a")
    click.echo("  5x premium per token. Even though output_tokens is the smaller count in")
    click.echo("  most sessions, the output portion typically dominates total cost. A 1.9x")
    click.echo("  undercount on output tokens often means true spend is 30-50% higher than")
    click.echo("  shown for Opus-heavy workspaces.")
    click.echo()
    click.echo("The --correct workaround")
    click.echo("  qualito costs --correct multiplies output_tokens by 1.9 and recomputes")
    click.echo("  cost end-to-end. The result is an estimate: 1.9 is one empirical data")
    click.echo("  point, not a guaranteed correction factor for every workload. For the")
    click.echo("  authoritative number, check your Anthropic billing dashboard.")
    click.echo()


@cli.command(help="Analyze spending by workspace, model, and time")
@click.option("--workspace", default=None, help="Filter by workspace")
@click.option("--days", default=30, help="Lookback period in days (default: 30)")
@click.option("--top", default=10, help="Show top N most expensive sessions (default: 10)")
@click.option("--correct", is_flag=True, default=False,
              help="Apply 1.9x correction to output_tokens to compensate for upstream undercount")
@click.option("--explain", is_flag=True, default=False,
              help="Print pricing constants + upstream bug explanation and exit")
def costs(workspace: str | None, days: int, top: int, correct: bool, explain: bool):
    """Analyze spending by workspace, model, and time."""
    from qualito.importer import DEFAULT_PRICING, MODEL_PRICING

    if explain:
        _print_costs_explain(MODEL_PRICING)
        return

    conn, _config = _get_conn()
    try:
        since = _since_date(days)
        base_conds = [
            runs_table.c.started_at >= since,
            runs_table.c.cost_usd.isnot(None),
        ]
        if workspace:
            base_conds.append(runs_table.c.workspace == workspace)

        # Aggregate by (workspace, model) — single source for per-workspace
        # and per-model views in both raw and corrected modes.
        ws_model_rows = conn.execute(
            select(
                runs_table.c.workspace,
                runs_table.c.model,
                func.count().label("runs"),
                func.coalesce(func.sum(runs_table.c.input_tokens), 0).label("input"),
                func.coalesce(func.sum(runs_table.c.output_tokens), 0).label("output"),
                func.coalesce(func.sum(runs_table.c.cache_read_tokens), 0).label("cache_read"),
                func.coalesce(func.sum(runs_table.c.cost_usd), 0).label("raw_cost"),
            ).where(and_(*base_conds))
            .group_by(runs_table.c.workspace, runs_table.c.model)
        ).mappings().fetchall()

        if not ws_model_rows:
            click.echo(f"\nNo cost data in the last {days} days.")
            if workspace:
                click.echo(f"  (filtered by workspace: {workspace})")
            click.echo()
            return

        def _cost_for_row(model, input_t, output_t, cache_read_t, raw_cost):
            if not correct:
                return float(raw_cost or 0)
            pricing = MODEL_PRICING.get(model or "", DEFAULT_PRICING)
            return _compute_cost(pricing, input_t or 0, output_t or 0, cache_read_t or 0, True)

        enriched = []
        for r in ws_model_rows:
            c = _cost_for_row(r["model"], r["input"], r["output"], r["cache_read"], r["raw_cost"])
            enriched.append({
                "workspace": r["workspace"] or "?",
                "model": r["model"] or "unknown",
                "runs": r["runs"],
                "input": r["input"],
                "output": r["output"],
                "cache_read": r["cache_read"],
                "cost": c,
            })

        total_runs = sum(r["runs"] for r in enriched)
        total_input = sum(r["input"] for r in enriched)
        total_output = sum(r["output"] for r in enriched)
        total_cache_read = sum(r["cache_read"] for r in enriched)
        total_cost = sum(r["cost"] for r in enriched)
        avg_per_run = total_cost / total_runs if total_runs > 0 else 0

        # Collapse to per-workspace
        by_ws: dict[str, dict] = {}
        for r in enriched:
            w = r["workspace"]
            d = by_ws.setdefault(w, {"runs": 0, "input": 0, "output": 0, "cache_read": 0, "cost": 0})
            d["runs"] += r["runs"]
            d["input"] += r["input"]
            d["output"] += r["output"]
            d["cache_read"] += r["cache_read"]
            d["cost"] += r["cost"]
        ws_list = sorted(
            [{"workspace": k, **v} for k, v in by_ws.items()],
            key=lambda x: x["cost"], reverse=True,
        )

        # Collapse to per-model
        by_model: dict[str, dict] = {}
        for r in enriched:
            m = r["model"]
            d = by_model.setdefault(m, {"runs": 0, "input": 0, "output": 0, "cache_read": 0, "cost": 0})
            d["runs"] += r["runs"]
            d["input"] += r["input"]
            d["output"] += r["output"]
            d["cache_read"] += r["cache_read"]
            d["cost"] += r["cost"]
        model_list = sorted(
            [{"model": k, **v} for k, v in by_model.items()],
            key=lambda x: x["cost"], reverse=True,
        )

        # Top-N most expensive. In --correct mode, fetch extra and re-sort
        # client-side since 1.9x on output can reshuffle the order.
        fetch_limit = top * 3 if correct else top
        top_rows = conn.execute(
            select(
                runs_table.c.id,
                runs_table.c.workspace,
                runs_table.c.model,
                runs_table.c.task,
                runs_table.c.input_tokens,
                runs_table.c.output_tokens,
                runs_table.c.cache_read_tokens,
                runs_table.c.cost_usd,
                runs_table.c.started_at,
            ).where(and_(*base_conds))
            .order_by(runs_table.c.cost_usd.desc())
            .limit(fetch_limit)
        ).mappings().fetchall()

        top_sessions = []
        for r in top_rows:
            c = _cost_for_row(
                r["model"], r["input_tokens"], r["output_tokens"],
                r["cache_read_tokens"], r["cost_usd"],
            )
            top_sessions.append({
                "workspace": r["workspace"] or "?",
                "model": r["model"] or "unknown",
                "task": r["task"] or "",
                "output": r["output_tokens"] or 0,
                "cost": c,
                "started_at": r["started_at"] or "",
            })
        top_sessions.sort(key=lambda x: x["cost"], reverse=True)
        top_sessions = top_sessions[:top]

        # ------------------------------------------------------------------
        # Render
        # ------------------------------------------------------------------
        def fmt_c(v: float) -> str:
            base = f"~{_fmt_cost(v)}"
            return f"{base} (corrected)" if correct else base

        click.echo(f"\nQualito Costs — last {days} days")
        if workspace:
            click.echo(f"Workspace: {workspace}")
        click.echo()
        click.echo(
            f"Total: {fmt_c(total_cost)}   {total_runs} sessions   "
            f"{fmt_c(avg_per_run)} avg/run"
        )
        click.echo(
            f"Tokens: ~{_fmt_tokens(total_input)} in   "
            f"~{_fmt_tokens(total_output)} out   "
            f"~{_fmt_tokens(total_cache_read)} cache_read"
        )

        # By workspace
        click.echo()
        click.echo("By workspace")
        click.echo(
            f"  {'workspace':<22} {'sessions':>8}   "
            f"{'in / out tokens':<20}   {'cost':<22} {'avg/run'}"
        )
        for r in ws_list:
            in_out = f"{_fmt_tokens(r['input'])} / {_fmt_tokens(r['output'])}"
            avg = r["cost"] / r["runs"] if r["runs"] > 0 else 0
            click.echo(
                f"  {r['workspace'][:22]:<22} {r['runs']:>8}   "
                f"{in_out:<20}   {fmt_c(r['cost']):<22} {fmt_c(avg)}"
            )

        # By model
        click.echo()
        click.echo("By model")
        click.echo(
            f"  {'model':<26} {'sessions':>8}   "
            f"{'in / out':<20}   {'cost':<22} {'%':>5}"
        )
        for r in model_list:
            in_out = f"{_fmt_tokens(r['input'])} / {_fmt_tokens(r['output'])}"
            pct = (r["cost"] / total_cost * 100) if total_cost > 0 else 0
            click.echo(
                f"  {r['model'][:26]:<26} {r['runs']:>8}   "
                f"{in_out:<20}   {fmt_c(r['cost']):<22} {pct:>4.0f}%"
            )

        # Top N
        if top_sessions:
            click.echo()
            click.echo(f"Top {len(top_sessions)} most expensive sessions")
            click.echo(
                f"  {'date':<12} {'workspace':<18} {'model':<22} "
                f"{'out tokens':>10}   {'cost':<22} task"
            )
            for r in top_sessions:
                date = (r["started_at"] or "")[:10]
                task = (r["task"] or "").replace("\n", " ")[:40]
                click.echo(
                    f"  {date:<12} {r['workspace'][:18]:<18} "
                    f"{r['model'][:22]:<22} "
                    f"{_fmt_tokens(r['output']):>10}   "
                    f"{fmt_c(r['cost']):<22} \"{task}\""
                )

        # Disclaimer
        click.echo()
        if correct:
            click.echo("⚠  Showing CORRECTED costs (output_tokens × 1.9 to compensate for upstream")
            click.echo("   undercount). This is an estimate, not authoritative. Compare to your")
            click.echo("   Anthropic billing dashboard for true totals.")
            click.echo("   Use `qualito costs` (no flag) to see raw recorded values.")
        else:
            click.echo("⚠  About these numbers")
            click.echo("   Cost is computed from token counts in Claude Code session files. Those")
            click.echo("   files have a known upstream bug: output_tokens are recorded as mid-stream")
            click.echo("   snapshots instead of final values, causing a ~1.9x undercount. Only")
            click.echo("   output_tokens is affected — input_tokens and cache_read are accurate.")
            click.echo()
            click.echo("   Bug report (auto-closed without a fix on 2026-03-24):")
            click.echo("     https://github.com/anthropics/claude-code/issues/27361")
            click.echo()
            click.echo("   Practical impact: for Opus-heavy workspaces, true spend is typically")
            click.echo("   30-50% higher than shown. Opus output costs 5x more per token than")
            click.echo("   input, so the (undercounted) output portion dominates total cost.")
            click.echo()
            click.echo("   Use `qualito costs --correct` to apply a 1.9x correction to output_tokens")
            click.echo("   and recompute. The result is an estimate, not authoritative — your")
            click.echo("   Anthropic billing dashboard is the only true source of total spend.")
            click.echo("   Use `qualito costs --explain` for the full pricing breakdown.")
        click.echo()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# dqi dashboard
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# dqi login
# ---------------------------------------------------------------------------

@cli.command(help="Authenticate with the Qualito cloud")
@click.option("--api-key", default=None, help="API key (skips browser flow)")
@click.option("--api-url", default=None, help="API URL (default: https://api.dqi.dev)")
def login(api_key: str | None, api_url: str | None):
    """Authenticate with the Qualito cloud API."""
    from qualito.cloud import (
        DEFAULT_API_URL,
        CloudError,
        cloud_request,
        save_credentials,
    )

    url = api_url or DEFAULT_API_URL

    if api_key:
        # Validate key by calling /api/auth/me
        save_credentials(api_key, url)
        try:
            user = cloud_request("GET", "/api/auth/me")
            click.echo(f"Logged in as {user.get('email', 'unknown')}")
        except CloudError as e:
            # Remove invalid credentials
            from qualito.cloud import delete_credentials
            delete_credentials()
            click.echo(f"Login failed: {e}")
            raise SystemExit(1)
    else:
        click.echo("To log in:")
        click.echo("  1. Go to https://app.qualito.ai/settings")
        click.echo("  2. Copy your API key")
        click.echo("  3. Run: qualito login --api-key <your-key>")


# ---------------------------------------------------------------------------
# dqi logout
# ---------------------------------------------------------------------------

@cli.command(help="Remove cloud credentials")
def logout():
    """Remove Qualito cloud credentials."""
    from qualito.cloud import delete_credentials

    if delete_credentials():
        click.echo("Logged out")
    else:
        click.echo("Not logged in")


# ---------------------------------------------------------------------------
# dqi sync
# ---------------------------------------------------------------------------


def _render_workspace_limit_error(err) -> None:
    """Print the parsed workspace_limit 403 detail in the target format."""
    click.echo(f"Sync failed: {err}")
    if err.current_workspaces:
        click.echo(f"  Currently synced: {', '.join(err.current_workspaces)}")
    if err.attempted_workspaces:
        click.echo(f"  Tried to add:     {', '.join(err.attempted_workspaces)}")
    click.echo("")
    click.echo(f"  Upgrade: {err.upgrade_url}")


def _run_sync_with_progress(
    conn,
    selected_workspaces: list[str] | None,
    since_date: str | None = None,
    include_incidents: bool = True,
) -> dict:
    """Execute a sync with per-workspace progress output.

    Shared by `qualito sync` and `qualito setup` so they render identical
    progress. Handles CloudError and WorkspaceLimitError by printing the
    parsed detail and exiting. Returns the run sync result dict on success.
    """
    from qualito.cloud import (
        CloudError,
        WorkspaceLimitError,
        sync_incidents,
        sync_runs,
    )

    # Pre-compute local session counts per workspace for header rendering.
    count_stmt = select(runs_table.c.workspace, func.count().label("cnt"))
    if since_date:
        count_stmt = count_stmt.where(runs_table.c.started_at >= since_date)
    if selected_workspaces:
        count_stmt = count_stmt.where(runs_table.c.workspace.in_(selected_workspaces))
    count_stmt = count_stmt.group_by(runs_table.c.workspace)
    ws_counts = {row[0]: row[1] for row in conn.execute(count_stmt).fetchall()}

    # Workspaces that actually have runs in scope. Empty selections should
    # still succeed (with a "nothing to sync" result) rather than hanging.
    effective_workspaces = (
        [w for w in selected_workspaces if ws_counts.get(w, 0) > 0]
        if selected_workspaces is not None
        else [w for w, c in ws_counts.items() if c > 0]
    )

    if not effective_workspaces:
        click.echo("Nothing to sync.")
        return {"synced": 0, "skipped": 0, "errors": 0, "by_workspace": {}}

    click.echo("")
    click.echo("Syncing runs...")
    click.echo("")

    current_ws = {"name": None}

    def on_batch(ws, batch_num, total_batches, count):
        if current_ws["name"] != ws:
            current_ws["name"] = ws
            click.echo(f"  {ws} ({ws_counts.get(ws, 0)} sessions)")
        click.echo(
            f"    Batch {batch_num}/{total_batches} — sending {count} runs... ✓"
        )

    def on_workspace_done(ws, synced_count):
        click.echo(f"  ✓ {ws} synced")
        click.echo("")

    try:
        run_result = sync_runs(
            conn,
            since=since_date,
            workspaces=selected_workspaces,
            on_batch=on_batch,
            on_workspace_done=on_workspace_done,
        )
    except WorkspaceLimitError as err:
        _render_workspace_limit_error(err)
        raise SystemExit(1)
    except CloudError as err:
        click.echo(f"Sync failed: {err}")
        raise SystemExit(1)

    if include_incidents:
        try:
            sync_incidents(conn)
        except CloudError as err:
            click.echo(f"Incidents sync failed: {err}")

    synced_count = run_result.get("synced", 0)
    by_ws = run_result.get("by_workspace", {})
    synced_ws_count = sum(1 for c in by_ws.values() if c > 0)

    click.echo(
        f"Synced {synced_count} sessions across {synced_ws_count} workspaces."
    )
    click.echo("View at: https://app.qualito.ai/runs")
    return run_result


@cli.command(help="Push local sessions to the cloud dashboard")
@click.option("--since", default=None, help="Sync runs since date (ISO format, non-interactive)")
@click.option("--all", "sync_all", is_flag=True, help="Sync all runs (non-interactive)")
@click.option(
    "--workspace",
    "workspaces",
    multiple=True,
    help="Workspace(s) to sync, repeatable (non-interactive)",
)
def sync(since: str | None, sync_all: bool, workspaces: tuple[str, ...]):
    """Sync local data to the Qualito cloud.

    With no flags: interactive picker that shows synced vs local-only workspaces.
    With --all, --since, or --workspace: skips the picker.
    """
    from qualito.cloud import (
        CloudError,
        fetch_synced_workspaces,
        fetch_user,
        load_credentials,
    )

    creds = load_credentials()
    if not creds:
        click.echo("Not logged in. Run 'qualito login' first.")
        raise SystemExit(1)

    conn, config = _get_conn()
    try:
        since_date = None if sync_all else since

        # ---------- Non-interactive paths: skip the picker ----------
        if workspaces:
            _run_sync_with_progress(conn, list(workspaces), since_date)
            return

        if sync_all or since:
            _run_sync_with_progress(conn, None, since_date)
            return

        # ---------- Interactive picker ----------
        try:
            user_info = fetch_user()
            synced_cloud = fetch_synced_workspaces()
        except CloudError as err:
            click.echo(f"Cannot reach cloud: {err}")
            raise SystemExit(1)

        plan = (user_info.get("plan") or "free").lower()
        is_free = plan != "pro"
        limit = 3 if is_free else None

        synced_meta = {
            (w.get("workspace_name") or ""): w
            for w in synced_cloud
            if w.get("workspace_name")
        }
        synced_list = sorted(synced_meta.keys())

        local_summaries = _get_workspace_summary(conn)
        local_only = [
            s
            for s in local_summaries
            if s.get("workspace") and s["workspace"] not in synced_meta
        ]

        at_limit = is_free and len(synced_list) >= (limit or 0)

        plan_label = (
            f"{plan} plan, {limit} workspace limit"
            if is_free
            else f"{plan} plan, unlimited workspaces"
        )
        click.echo(f"Cloud sync status ({plan_label})")
        click.echo("")

        # --- Already synced section ---
        if is_free:
            click.echo(f"Already synced ({len(synced_list)}/{limit}):")
        else:
            click.echo(f"Already synced ({len(synced_list)}):")
        if synced_list:
            for name in synced_list:
                meta = synced_meta[name]
                sc = int(meta.get("session_count") or 0)
                last = _fmt_relative_time(meta.get("last_synced_at"))
                click.echo(
                    f"  ✓ {name[:16]:<16} {sc:>4} sessions   (last sync: {last})"
                )
        else:
            click.echo("  (none)")
        click.echo("")

        # --- Local-only section ---
        click.echo(f"Local only ({len(local_only)}):")
        if local_only:
            for i, s in enumerate(local_only, 1):
                count = int(s.get("run_count") or 0)
                prefix = "•" if at_limit else f"[{i}]"
                click.echo(
                    f"  {prefix} {(s['workspace'] or '')[:16]:<16} {count:>4} sessions"
                )
        else:
            click.echo("  (none)")
        click.echo("")

        # --- Pick workspaces to sync ---
        selected_workspaces: list[str]

        if at_limit:
            click.echo("Free plan is at the workspace limit. Options:")
            click.echo("  [1] Sync new sessions to existing workspaces (default)")
            click.echo("  [2] Upgrade to Pro for unlimited workspaces")
            choice = click.prompt("\nChoose [1/2]", default="1")
            choice = str(choice).strip()
            if choice == "2":
                click.echo("")
                click.echo("Upgrade: https://app.qualito.ai/settings")
                return
            selected_workspaces = synced_list
        else:
            if not local_only:
                if not synced_list:
                    click.echo("No local workspaces to sync.")
                    return
                selected_workspaces = synced_list
            else:
                remaining = (limit - len(synced_list)) if is_free else None
                hint = (
                    f" (up to {remaining} more)"
                    if remaining is not None and remaining > 0
                    else ""
                )
                selection = click.prompt(
                    f"Choose workspaces to add (by number, comma-separated, or 'none'){hint}",
                    default="none",
                )
                sel_text = str(selection).strip().lower()
                new_picks: list[str] = []
                if sel_text not in ("none", ""):
                    indices = _parse_selection(sel_text, len(local_only)) or []
                    new_picks = [local_only[i]["workspace"] for i in indices]

                if is_free and remaining is not None and len(new_picks) > remaining:
                    click.echo(
                        f"Free plan only allows {remaining} more workspace(s). "
                        f"Keeping the first {remaining}."
                    )
                    new_picks = new_picks[:remaining]

                selected_workspaces = synced_list + new_picks

        if not selected_workspaces:
            click.echo("Nothing to sync.")
            return

        _run_sync_with_progress(conn, selected_workspaces, since_date)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# qualito privacy
# ---------------------------------------------------------------------------


def _fmt_privacy_mode(enabled: bool) -> str:
    """Render a privacy boolean as 'full content' or 'metadata only'."""
    return "full content" if enabled else "metadata only"


@cli.command(help="View or change per-workspace sync privacy settings")
@click.argument("workspace", required=False)
@click.option("--metadata", is_flag=True, default=False,
              help="Set workspace to metadata-only sync")
@click.option("--full", is_flag=True, default=False,
              help="Set workspace to full content sync (requires confirmation)")
@click.option("--allow-llm", is_flag=True, default=False,
              help="Opt into LLM-based analysis (future Pro feature)")
@click.option("--no-allow-llm", is_flag=True, default=False,
              help="Opt out of LLM-based analysis")
@click.option("--yes", "-y", is_flag=True, default=False,
              help="Skip confirmation prompt for --full")
def privacy(
    workspace: str | None,
    metadata: bool,
    full: bool,
    allow_llm: bool,
    no_allow_llm: bool,
    yes: bool,
):
    """View and change per-workspace sync privacy settings.

    With no arguments: lists every synced workspace and its current privacy.
    With WORKSPACE only: shows detail + options for that workspace.
    With WORKSPACE --metadata | --full: changes sync_content mode.
    With WORKSPACE --allow-llm | --no-allow-llm: toggles LLM analysis opt-in.
    """
    from qualito.cloud import (
        CloudError,
        fetch_synced_workspaces,
        fetch_user,
        fetch_workspace_privacy,
        load_credentials,
        set_workspace_privacy,
    )

    # Flag validation
    if metadata and full:
        raise click.UsageError("--metadata and --full are mutually exclusive.")
    if allow_llm and no_allow_llm:
        raise click.UsageError("--allow-llm and --no-allow-llm are mutually exclusive.")

    setting_flags = metadata or full or allow_llm or no_allow_llm
    if setting_flags and not workspace:
        raise click.UsageError(
            "workspace name required when setting privacy mode."
        )

    if not load_credentials():
        click.echo("Not logged in. Run 'qualito login' first.")
        raise SystemExit(1)

    # Mode 3: change sync_content
    if metadata or full:
        try:
            if metadata:
                set_workspace_privacy(workspace, sync_content=False)
                click.echo(f"OK. {workspace} is now metadata-only.")
                return
            # --full path
            if not yes:
                click.echo(
                    f"This will sync prompts, tool outputs, and file paths to"
                )
                click.echo(
                    f"https://app.qualito.ai for workspace '{workspace}'."
                )
                click.echo(
                    "Use 'qualito audit secrets' to scan for accidental secrets before syncing."
                )
                if not click.confirm("Continue?", default=False):
                    click.echo("Aborted. No changes.")
                    return
            set_workspace_privacy(workspace, sync_content=True)
            click.echo(f"OK. {workspace} now syncs full content.")
            return
        except CloudError as err:
            click.echo(f"Error: {err}", err=True)
            raise SystemExit(1)

    # Mode 4: toggle allow_llm without touching sync_content
    if allow_llm or no_allow_llm:
        try:
            current = fetch_workspace_privacy(workspace)
            new_allow = bool(allow_llm)
            set_workspace_privacy(
                workspace,
                sync_content=bool(current.get("sync_content", False)),
                allow_llm=new_allow,
            )
        except CloudError as err:
            click.echo(f"Error: {err}", err=True)
            raise SystemExit(1)
        if new_allow:
            click.echo(
                f"OK. {workspace} allow_llm=true (no feature uses this yet; "
                f"reserved for future Pro LLM-based analysis)."
            )
        else:
            click.echo(f"OK. {workspace} allow_llm=false.")
        return

    # Mode 2: show single workspace detail
    if workspace:
        try:
            data = fetch_workspace_privacy(workspace)
        except CloudError as err:
            if getattr(err, "status_code", None) == 404:
                click.echo(
                    f"Workspace '{workspace}' not yet synced. Run: qualito sync"
                )
                return
            click.echo(f"Error: {err}", err=True)
            raise SystemExit(1)

        sync_content = bool(data.get("sync_content", False))
        allow_llm_cur = bool(data.get("allow_llm", False))

        click.echo(f"Workspace: {workspace}")
        click.echo(f"  Sync content:  {_fmt_privacy_mode(sync_content)}")
        click.echo(f"  LLM analysis:  {_fmt_privacy_mode(allow_llm_cur)}")
        click.echo()
        if sync_content:
            click.echo(
                "Full content means: everything syncs including prompts, tool"
            )
            click.echo(
                "outputs, file paths, and task text."
            )
            click.echo()
            click.echo("To change:")
            click.echo(
                f"  qualito privacy {workspace} --metadata    "
                "(switch to metadata only)"
            )
            click.echo(
                f"  qualito privacy {workspace} --allow-llm   "
                "(opt into LLM analysis — future Pro feature)"
            )
        else:
            click.echo(
                "Metadata only means: counts, durations, types, scores, IDs, timestamps."
            )
            click.echo(
                "It does NOT ship: task text, tool call outputs, file paths, prompts."
            )
            click.echo()
            click.echo("To change:")
            click.echo(
                f"  qualito privacy {workspace} --full       "
                "(opt into full content sync)"
            )
            click.echo(
                f"  qualito privacy {workspace} --allow-llm  "
                "(opt into LLM analysis — future Pro feature)"
            )
        return

    # Mode 1: list all synced workspaces
    try:
        synced = fetch_synced_workspaces()
    except CloudError as err:
        click.echo(f"Error: {err}", err=True)
        raise SystemExit(1)

    if not synced:
        click.echo("No synced workspaces yet. Run: qualito sync")
        return

    plan_label: str | None = None
    try:
        user_info = fetch_user()
        plan = (user_info.get("plan") or "free").lower()
        plan_label = f"{plan} plan"
    except CloudError:
        plan_label = None

    header = "Workspace privacy"
    if plan_label:
        header += f" ({plan_label})"
    click.echo(header)
    click.echo()
    click.echo(
        f"{'Workspace':<16} {'Sync content':<16} "
        f"{'LLM analysis':<16} Last changed"
    )

    for ws in synced:
        name = str(ws.get("workspace_name") or "")
        if not name:
            continue
        try:
            data = fetch_workspace_privacy(name)
        except CloudError as err:
            click.echo(f"{name[:16]:<16} error: {err}")
            continue
        sync_content = bool(data.get("sync_content", False))
        allow_llm_cur = bool(data.get("allow_llm", False))
        last = _fmt_relative_time(ws.get("last_synced_at"))
        click.echo(
            f"{name[:16]:<16} {_fmt_privacy_mode(sync_content):<16} "
            f"{_fmt_privacy_mode(allow_llm_cur):<16} {last}"
        )

    click.echo()
    click.echo(
        "Default: metadata only. Use 'qualito privacy <workspace> --full' "
        "to share content."
    )


# ---------------------------------------------------------------------------
# dqi dashboard
# ---------------------------------------------------------------------------

@cli.command(help="Launch the local web dashboard")
@click.option("--port", default=8090, help="Port (default: 8090)")
@click.option("--host", default="127.0.0.1", help="Host (default: 127.0.0.1)")
def dashboard(port: int, host: str):
    """Launch the Qualito dashboard web UI."""
    try:
        import uvicorn
    except ImportError:
        click.echo("Dashboard dependencies not installed. Run:")
        click.echo("  uv pip install 'qualito[dashboard]'")
        raise SystemExit(1)

    from qualito.dashboard.app import create_app

    app = create_app()
    click.echo(f"Starting Qualito Dashboard at http://{host}:{port}")
    uvicorn.run(app, host=host, port=port)
