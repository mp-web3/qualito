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
    evaluations_table,
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


@click.group()
@click.version_option(version=__version__)
def cli():
    """Qualito — Measure and improve AI agent delegation quality."""
    pass


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------


def _get_workspace_summary(conn) -> list[dict]:
    """Query per-workspace summary: run count, avg DQI, total cost."""
    join = runs_table.outerjoin(
        evaluations_table,
        and_(evaluations_table.c.run_id == runs_table.c.id,
             evaluations_table.c.eval_type == "dqi"),
    )
    rows = conn.execute(
        select(
            runs_table.c.workspace,
            func.count().label("run_count"),
            func.avg(evaluations_table.c.score).label("avg_dqi"),
            func.coalesce(func.sum(runs_table.c.cost_usd), 0).label("total_cost"),
        ).select_from(join)
        .group_by(runs_table.c.workspace)
        .order_by(runs_table.c.workspace)
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

    click.echo(f"\n{'Workspace':<25} {'Runs':>6} {'Avg DQI':>10} {'Cost':>10}")
    click.echo("-" * 55)
    all_dqi = []
    for s in summaries:
        ws = (s["workspace"] or "?")[:23]
        runs = s["run_count"]
        avg = s["avg_dqi"]
        cost = s["total_cost"]
        if avg is not None:
            all_dqi.append(avg)
        dqi_str = f"{avg:.3f}" if avg is not None else "N/A"
        click.echo(f"{ws:<25} {runs:>6} {dqi_str:>10} {_fmt_cost(cost):>10}")
    click.echo("-" * 55)

    overall_avg = sum(all_dqi) / len(all_dqi) if all_dqi else None
    if overall_avg is not None:
        label = _dqi_label(overall_avg)
        click.echo(
            f"\nAverage DQI: {overall_avg:.2f} ({label}) — "
            f"scores 0-1. Above 0.7 is good, below 0.5 needs attention."
        )


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


@cli.command()
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
                click.echo("Syncing to cloud...")
                from qualito.cloud import sync_incidents, sync_runs

                engine = get_engine(str(db_path))
                sync_conn = get_sa_connection(engine)
                try:
                    run_result = sync_runs(sync_conn)
                    inc_result = sync_incidents(sync_conn)
                    click.echo(
                        f"Synced {run_result['synced']} runs to cloud."
                    )
                finally:
                    sync_conn.close()

            # Report complete (triggers dashboard reload via SSE)
            _report_setup_progress(api_url, token, "complete")

            click.echo(
                "\nSynced to cloud. Open app.qualito.ai to see your dashboard."
            )

        except Exception:
            click.echo(
                "\nCould not connect to cloud. "
                "Your data is imported locally."
            )
            _print_next_steps()
    else:
        # Interactive setup
        global_dir = Path.home() / ".qualito"
        if global_dir.exists() and (global_dir / "config.toml").exists():
            _run_interactive_setup_rerun()
        else:
            _run_interactive_setup_first_run()


# ---------------------------------------------------------------------------
# qualito init
# ---------------------------------------------------------------------------


@cli.command()
@click.option("--dir", "project_dir", type=click.Path(exists=True, path_type=Path),
              default=None, help="Project directory (default: cwd)")
@click.option("--local", is_flag=True, default=False,
              help="Create per-project .qualito/ instead of global ~/.qualito/")
def init(project_dir: Path | None, local: bool):
    """Initialize Qualito (global ~/.qualito/ by default, --local for per-project)."""
    if project_dir is None:
        project_dir = Path.cwd()

    from qualito.config import init_project

    # Check for Claude Code markers
    markers = []
    if (project_dir / "CLAUDE.md").exists():
        markers.append("CLAUDE.md")
    if (project_dir / ".claude.json").exists():
        markers.append(".claude.json")
    if (project_dir / ".claude").is_dir():
        markers.append(".claude/")

    config, qualito_dir = init_project(project_dir, local=local)

    mode = "local" if local else "global"
    click.echo(f"Initialized Qualito ({mode}) in {qualito_dir}")
    click.echo(f"  Workspace: {config.workspace}")
    click.echo(f"  DB: {config.db_path}")
    click.echo(f"  Config: {qualito_dir / 'config.toml'}")
    if markers:
        click.echo(f"  Detected: {', '.join(markers)}")
    else:
        click.echo("  No Claude Code markers found (CLAUDE.md, .claude.json, .claude/)")


@cli.command()
@click.option("--dir", "project_dir", type=click.Path(exists=True, path_type=Path),
              default=None, help="Project directory (default: cwd)")
def status(project_dir: Path | None):
    """Show Qualito status for the current project."""
    if project_dir is None:
        project_dir = Path.cwd()

    global_dir = Path.home() / ".qualito"
    local_dir = project_dir / ".qualito"

    if local_dir.exists():
        qualito_dir = local_dir
        mode = "local"
    elif global_dir.exists():
        qualito_dir = global_dir
        mode = "global"
    else:
        click.echo("Qualito not initialized. Run 'qualito init' first.")
        raise SystemExit(1)

    from qualito.config import load_config

    config = load_config(project_dir)

    # Resolve DB path
    db_path = config.db_path
    if not db_path.is_absolute():
        db_path = project_dir / db_path

    click.echo(f"Mode: {mode}")
    click.echo(f"Workspace: {config.workspace}")
    click.echo(f"Config: {qualito_dir / 'config.toml'}")
    click.echo(f"DB: {db_path} ({'exists' if db_path.exists() else 'missing'})")

    # SLOs
    click.echo(f"SLOs: quality={config.slo_quality:.0%}, "
               f"availability={config.slo_availability:.0%}, "
               f"cost=${config.slo_cost:.2f}")

    # Run count
    if db_path.exists():
        engine = get_engine(str(db_path))
        conn = get_sa_connection(engine)
        row = conn.execute(
            select(func.count().label("n")).select_from(runs_table)
        ).mappings().fetchone()
        run_count = row["n"]
        conn.close()
        click.echo(f"Runs: {run_count}")
    else:
        click.echo("Runs: 0 (no database)")


# ---------------------------------------------------------------------------
# dqi import
# ---------------------------------------------------------------------------

@cli.command(name="import")
@click.option("--dir", "project_dir", type=click.Path(exists=True, path_type=Path),
              default=None, help="Project directory (default: cwd)")
@click.option("--workspace", default=None, help="Override workspace name")
@click.option("--all-projects", is_flag=True, default=False,
              help="Import all discovered Claude Code projects")
def import_sessions(project_dir: Path | None, workspace: str | None, all_projects: bool):
    """Import existing Claude Code sessions into Qualito."""
    if project_dir is None:
        project_dir = Path.cwd()

    from qualito.config import load_config

    config = load_config(project_dir)
    db_path = config.db_path
    if not db_path.is_absolute():
        db_path = project_dir / db_path

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
# dqi score
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--workspace", default=None, help="Filter by workspace")
@click.option("--days", default=30, help="Lookback period in days (default: 30)")
@click.option("--limit", default=20, help="Max rows to show (default: 20)")
def score(workspace: str | None, days: int, limit: int):
    """Show DQI scores for recent runs."""
    conn, config = _get_conn()
    try:
        since = _since_date(days)
        join = evaluations_table.join(runs_table, evaluations_table.c.run_id == runs_table.c.id)
        conds = [evaluations_table.c.eval_type == "dqi", runs_table.c.started_at >= since]
        if workspace:
            conds.append(runs_table.c.workspace == workspace)

        rows = conn.execute(
            select(
                runs_table.c.id, runs_table.c.workspace, runs_table.c.task_type,
                evaluations_table.c.score.label("dqi"),
                evaluations_table.c.categories, runs_table.c.cost_usd,
                runs_table.c.started_at,
            ).select_from(join)
            .where(and_(*conds))
            .order_by(runs_table.c.started_at.desc())
            .limit(limit)
        ).mappings().fetchall()

        if not rows:
            click.echo(f"No DQI scores found in the last {days} days.")
            return

        # Header
        click.echo(f"\nDQI Scores (last {days} days)")
        click.echo("=" * 90)
        click.echo(
            f"{'Run ID':<12} {'Workspace':<18} {'Task Type':<12} "
            f"{'DQI':>6} {'Tier':<10} {'Cost':>8} {'Date':<12}"
        )
        click.echo("-" * 90)

        dqi_scores = []
        for r in rows:
            run_id = r["id"][:10] if r["id"] else "?"
            ws = (r["workspace"] or "?")[:16]
            task_type = (r["task_type"] or "other")[:10]
            dqi_val = r["dqi"]
            dqi_str = f"{dqi_val:.3f}" if dqi_val is not None else "N/A"
            if dqi_val is not None:
                dqi_scores.append(dqi_val)

            # Parse tier from categories JSON
            tier_label = "?"
            cats = r["categories"]
            if cats:
                try:
                    parsed = json.loads(cats) if isinstance(cats, str) else cats
                    tier_label = parsed.get("tier_label", "?")
                except (json.JSONDecodeError, TypeError):
                    pass

            cost_str = _fmt_cost(r["cost_usd"])
            date_str = (r["started_at"] or "")[:10]

            click.echo(
                f"{run_id:<12} {ws:<18} {task_type:<12} "
                f"{dqi_str:>6} {tier_label:<10} {cost_str:>8} {date_str:<12}"
            )

        click.echo("-" * 90)

        # Averages
        if dqi_scores:
            avg_dqi = sum(dqi_scores) / len(dqi_scores)
            click.echo(f"\nAverage DQI: {avg_dqi:.3f}  ({len(dqi_scores)} scored runs)")

            # Trend: last 10 vs previous 10
            if len(dqi_scores) >= 10:
                recent_10 = dqi_scores[:10]
                prev_10 = dqi_scores[10:20] if len(dqi_scores) >= 20 else dqi_scores[10:]
                if prev_10:
                    recent_avg = sum(recent_10) / len(recent_10)
                    prev_avg = sum(prev_10) / len(prev_10)
                    diff = recent_avg - prev_avg
                    if diff > 0.02:
                        arrow = "^ improving"
                    elif diff < -0.02:
                        arrow = "v declining"
                    else:
                        arrow = "= stable"
                    click.echo(
                        f"Trend: {arrow} "
                        f"(recent 10: {recent_avg:.3f}, prev: {prev_avg:.3f})"
                    )
        click.echo()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# dqi costs
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--workspace", default=None, help="Filter by workspace")
@click.option("--days", default=30, help="Lookback period in days (default: 30)")
def costs(workspace: str | None, days: int):
    """Cost breakdown and waste analysis."""
    conn, config = _get_conn()
    try:
        since = _since_date(days)
        base_conds = [runs_table.c.started_at >= since]
        if workspace:
            base_conds.append(runs_table.c.workspace == workspace)

        # Overall stats
        stats = conn.execute(
            select(
                func.count().label("total_runs"),
                func.coalesce(func.sum(runs_table.c.cost_usd), 0).label("total_spend"),
                func.avg(runs_table.c.cost_usd).label("avg_per_run"),
            ).where(and_(*base_conds))
        ).mappings().fetchone()

        total_runs = stats["total_runs"]
        total_spend = stats["total_spend"] or 0
        avg_per_run = stats["avg_per_run"]

        click.echo(f"\nCost Analysis (last {days} days)")
        click.echo("=" * 60)
        click.echo(f"Total runs:     {total_runs}")
        click.echo(f"Total spend:    {_fmt_cost(total_spend)}")
        click.echo(f"Avg per run:    {_fmt_cost(avg_per_run)}")

        # Cost by workspace
        ws_rows = conn.execute(
            select(
                runs_table.c.workspace,
                func.count().label("runs"),
                func.coalesce(func.sum(runs_table.c.cost_usd), 0).label("total"),
                func.avg(runs_table.c.cost_usd).label("avg"),
            ).where(and_(*base_conds))
            .group_by(runs_table.c.workspace)
            .order_by(func.coalesce(func.sum(runs_table.c.cost_usd), 0).desc())
        ).mappings().fetchall()

        if ws_rows:
            click.echo(f"\n{'Workspace':<25} {'Runs':>6} {'Total':>10} {'Avg':>10}")
            click.echo("-" * 55)
            for r in ws_rows:
                click.echo(
                    f"{(r['workspace'] or '?'):<25} {r['runs']:>6} "
                    f"{_fmt_cost(r['total']):>10} {_fmt_cost(r['avg']):>10}"
                )

        # Cost by task type
        type_rows = conn.execute(
            select(
                runs_table.c.task_type,
                func.count().label("runs"),
                func.coalesce(func.sum(runs_table.c.cost_usd), 0).label("total"),
                func.avg(runs_table.c.cost_usd).label("avg"),
            ).where(and_(*base_conds))
            .group_by(runs_table.c.task_type)
            .order_by(func.coalesce(func.sum(runs_table.c.cost_usd), 0).desc())
        ).mappings().fetchall()

        if type_rows:
            click.echo(f"\n{'Task Type':<25} {'Runs':>6} {'Total':>10} {'Avg':>10}")
            click.echo("-" * 55)
            for r in type_rows:
                click.echo(
                    f"{(r['task_type'] or 'other'):<25} {r['runs']:>6} "
                    f"{_fmt_cost(r['total']):>10} {_fmt_cost(r['avg']):>10}"
                )

        # Waste: runs with DQI < 0.5
        waste_join = runs_table.join(
            evaluations_table,
            and_(evaluations_table.c.run_id == runs_table.c.id,
                 evaluations_table.c.eval_type == "dqi"),
        )
        waste_conds = list(base_conds) + [evaluations_table.c.score < 0.5]

        waste = conn.execute(
            select(
                func.count().label("waste_runs"),
                func.coalesce(func.sum(runs_table.c.cost_usd), 0).label("waste_cost"),
            ).select_from(waste_join).where(and_(*waste_conds))
        ).mappings().fetchone()

        waste_runs = waste["waste_runs"]
        waste_cost = waste["waste_cost"] or 0
        waste_pct = (waste_cost / total_spend * 100) if total_spend > 0 else 0

        click.echo(f"\nWaste (DQI < 0.5)")
        click.echo("-" * 40)
        click.echo(f"Wasteful runs:  {waste_runs}")
        click.echo(f"Wasted cost:    {_fmt_cost(waste_cost)} ({waste_pct:.1f}% of total)")

        # Top 5 most expensive runs
        top_rows = conn.execute(
            select(
                runs_table.c.id, runs_table.c.workspace, runs_table.c.task_type,
                runs_table.c.cost_usd, runs_table.c.started_at,
            ).where(and_(*base_conds, runs_table.c.cost_usd.isnot(None)))
            .order_by(runs_table.c.cost_usd.desc())
            .limit(5)
        ).mappings().fetchall()

        if top_rows:
            click.echo(f"\nTop 5 Most Expensive Runs")
            click.echo(f"{'Run ID':<12} {'Workspace':<18} {'Task Type':<12} {'Cost':>8} {'Date':<12}")
            click.echo("-" * 66)
            for r in top_rows:
                click.echo(
                    f"{(r['id'] or '?')[:10]:<12} "
                    f"{(r['workspace'] or '?')[:16]:<18} "
                    f"{(r['task_type'] or 'other')[:10]:<12} "
                    f"{_fmt_cost(r['cost_usd']):>8} "
                    f"{(r['started_at'] or '')[:10]:<12}"
                )

        click.echo()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# dqi incidents
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--workspace", default=None, help="Filter by workspace")
@click.option("--status", "status_filter", default=None,
              help="Filter by status (e.g. detected, confirmed, resolved)")
@click.option("--all", "show_all", is_flag=True, default=False,
              help="Show all incidents including resolved")
def incidents(workspace: str | None, status_filter: str | None, show_all: bool):
    """Show active incidents."""
    conn, config = _get_conn()
    try:
        from qualito.core.db import get_incidents

        if show_all:
            inc_status = None
        elif status_filter:
            inc_status = status_filter
        else:
            # Default: show non-resolved
            inc_status = None  # We'll filter manually

        rows = get_incidents(
            conn,
            workspace=workspace,
            status=inc_status if status_filter else None,
        )

        # If not show_all and no explicit status, filter out resolved
        if not show_all and not status_filter:
            rows = [r for r in rows if r.get("status") not in ("resolved", "auto_resolved", "false_positive")]

        if not rows:
            click.echo("No incidents found.")
            return

        click.echo(f"\nIncidents ({len(rows)} found)")
        click.echo("=" * 110)
        click.echo(
            f"{'ID':>4} {'Sev':<9} {'Title':<35} {'Workspace':<16} "
            f"{'Status':<14} {'Affected':>8} {'Cost':>8} {'Since':<12}"
        )
        click.echo("-" * 110)

        for r in rows:
            title = (r.get("title") or "?")[:33]
            ws = (r.get("workspace") or "?")[:14]
            sev = (r.get("severity") or "?")[:7]
            st = (r.get("status") or "?")[:12]
            affected = r.get("total_affected_runs") or 0
            cost = r.get("cost_impact_usd")
            since = (r.get("created_at") or "")[:10]

            click.echo(
                f"{r.get('id', '?'):>4} {sev:<9} {title:<35} {ws:<16} "
                f"{st:<14} {affected:>8} {_fmt_cost(cost):>8} {since:<12}"
            )

        click.echo()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# dqi slo
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--workspace", default=None, help="Filter by workspace")
@click.option("--days", default=30, help="Lookback period in days (default: 30)")
def slo(workspace: str | None, days: int):
    """Show SLO compliance."""
    conn, config = _get_conn()
    try:
        since = _since_date(days)
        base_conds = [runs_table.c.started_at >= since]
        if workspace:
            base_conds.append(runs_table.c.workspace == workspace)

        # Load SLO targets from config
        slo_quality_threshold = config.slo_quality  # e.g. 0.60
        slo_avail_target = config.slo_availability  # e.g. 0.95
        slo_cost_threshold = config.slo_cost  # e.g. 3.00

        # Total runs
        total_row = conn.execute(
            select(
                func.count().label("total"),
                func.sum(case((runs_table.c.status == "completed", 1), else_=0)).label("completed"),
                func.sum(case((runs_table.c.cost_usd < slo_cost_threshold, 1), else_=0)).label("under_cost"),
            ).where(and_(*base_conds))
        ).mappings().fetchone()

        total = total_row["total"] or 0
        completed = total_row["completed"] or 0
        under_cost = total_row["under_cost"] or 0

        # Quality: runs with DQI >= threshold
        join = evaluations_table.join(runs_table, evaluations_table.c.run_id == runs_table.c.id)
        quality_conds = list(base_conds) + [evaluations_table.c.eval_type == "dqi"]

        quality_row = conn.execute(
            select(
                func.count().label("total_scored"),
                func.sum(case((evaluations_table.c.score >= slo_quality_threshold, 1), else_=0)).label("quality_ok"),
            ).select_from(join).where(and_(*quality_conds))
        ).mappings().fetchone()

        total_scored = quality_row["total_scored"] or 0
        quality_ok = quality_row["quality_ok"] or 0

        # Compute percentages
        quality_pct = (quality_ok / total_scored) if total_scored > 0 else None
        avail_pct = (completed / total) if total > 0 else None
        cost_pct = (under_cost / total) if total > 0 else None

        click.echo(f"\nSLO Compliance (last {days} days)")
        click.echo("=" * 65)
        click.echo(
            f"{'SLO':<20} {'Current':>10} {'Target':>10} {'Status':>10}"
        )
        click.echo("-" * 65)

        # Quality
        q_current = _fmt_pct(quality_pct * 100) if quality_pct is not None else "N/A"
        q_target = _fmt_pct(slo_quality_threshold * 100)
        q_pass = quality_pct is not None and quality_pct >= slo_quality_threshold
        q_status = "PASS" if q_pass else ("FAIL" if quality_pct is not None else "N/A")
        click.echo(f"{'Quality':<20} {q_current:>10} {q_target:>10} {q_status:>10}")

        # Availability
        a_current = _fmt_pct(avail_pct * 100) if avail_pct is not None else "N/A"
        a_target = _fmt_pct(slo_avail_target * 100)
        a_pass = avail_pct is not None and avail_pct >= slo_avail_target
        a_status = "PASS" if a_pass else ("FAIL" if avail_pct is not None else "N/A")
        click.echo(f"{'Availability':<20} {a_current:>10} {a_target:>10} {a_status:>10}")

        # Cost
        c_current = _fmt_pct(cost_pct * 100) if cost_pct is not None else "N/A"
        c_target = f"<{_fmt_cost(slo_cost_threshold)}"
        c_pass = cost_pct is not None and cost_pct >= 0.80  # 80% of runs under threshold
        c_status = "PASS" if c_pass else ("FAIL" if cost_pct is not None else "N/A")
        click.echo(f"{'Cost':<20} {c_current:>10} {c_target:>10} {c_status:>10}")

        click.echo("-" * 65)

        # Overall
        all_pass = q_pass and a_pass and c_pass
        if total == 0:
            click.echo("No runs found — cannot assess SLO compliance.")
        else:
            click.echo(f"Overall: {'ALL PASS' if all_pass else 'NOT MET'}  ({total} runs)")

        # Per-workspace breakdown (only when no workspace filter)
        if not workspace:
            ws_rows = conn.execute(
                select(runs_table.c.workspace.distinct())
                .where(runs_table.c.started_at >= since)
                .order_by(runs_table.c.workspace)
            ).mappings().fetchall()

            if len(ws_rows) > 1:
                click.echo(f"\nPer-Workspace Breakdown")
                click.echo(
                    f"{'Workspace':<25} {'Quality':>10} {'Avail':>10} {'Cost':>10}"
                )
                click.echo("-" * 60)

                for ws_row in ws_rows:
                    ws_name = ws_row["workspace"]
                    ws_conds = [runs_table.c.started_at >= since, runs_table.c.workspace == ws_name]

                    # Quality for this workspace
                    wq = conn.execute(
                        select(
                            func.count().label("total_scored"),
                            func.sum(case((evaluations_table.c.score >= slo_quality_threshold, 1), else_=0)).label("ok"),
                        ).select_from(join).where(and_(
                            evaluations_table.c.eval_type == "dqi", *ws_conds
                        ))
                    ).mappings().fetchone()

                    ws_q = (
                        _fmt_pct(wq["ok"] / wq["total_scored"] * 100)
                        if wq["total_scored"] > 0 else "N/A"
                    )

                    # Availability for this workspace
                    wa = conn.execute(
                        select(
                            func.count().label("total"),
                            func.sum(case((runs_table.c.status == "completed", 1), else_=0)).label("ok"),
                        ).where(and_(*ws_conds))
                    ).mappings().fetchone()

                    ws_a = (
                        _fmt_pct(wa["ok"] / wa["total"] * 100)
                        if wa["total"] > 0 else "N/A"
                    )

                    # Cost for this workspace
                    wc = conn.execute(
                        select(
                            func.count().label("total"),
                            func.sum(case((runs_table.c.cost_usd < slo_cost_threshold, 1), else_=0)).label("ok"),
                        ).where(and_(*ws_conds))
                    ).mappings().fetchone()

                    ws_c = (
                        _fmt_pct(wc["ok"] / wc["total"] * 100)
                        if wc["total"] > 0 else "N/A"
                    )

                    click.echo(f"{ws_name[:23]:<25} {ws_q:>10} {ws_a:>10} {ws_c:>10}")

        click.echo()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# dqi dashboard
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# dqi login
# ---------------------------------------------------------------------------

@cli.command()
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
        click.echo(f"Open this URL to login: {url}/cli-auth")
        click.echo("Then run: qualito login --api-key <your-key>")


# ---------------------------------------------------------------------------
# dqi logout
# ---------------------------------------------------------------------------

@cli.command()
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

@cli.command()
@click.option("--since", default=None, help="Sync runs since date (ISO format)")
@click.option("--all", "sync_all", is_flag=True, help="Sync all runs")
def sync(since: str | None, sync_all: bool):
    """Sync local data to the Qualito cloud."""
    from qualito.cloud import CloudError, load_credentials, sync_incidents, sync_runs

    creds = load_credentials()
    if not creds:
        click.echo("Not logged in. Run 'qualito login' first.")
        raise SystemExit(1)

    conn, config = _get_conn()
    try:
        since_date = None if sync_all else since

        click.echo("Syncing runs...")
        run_result = sync_runs(conn, since=since_date)

        click.echo("Syncing incidents...")
        inc_result = sync_incidents(conn)

        api_url = creds.get("api_url", "cloud")
        click.echo(
            f"\nSynced {run_result['synced']} runs, "
            f"{inc_result['synced']} incidents to {api_url}"
        )
        if run_result["skipped"] or inc_result["skipped"]:
            click.echo(
                f"Skipped: {run_result['skipped']} runs, "
                f"{inc_result['skipped']} incidents (already synced)"
            )
        if run_result["errors"] or inc_result["errors"]:
            click.echo(
                f"Errors: {run_result['errors']} runs, "
                f"{inc_result['errors']} incidents"
            )
    except CloudError as e:
        click.echo(f"Sync failed: {e}")
        raise SystemExit(1)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# dqi dashboard
# ---------------------------------------------------------------------------

@cli.command()
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
