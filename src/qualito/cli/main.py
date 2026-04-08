"""Qualito CLI entry point."""

import json
from datetime import datetime, timedelta
from pathlib import Path

import click

from qualito import __version__


def _get_conn(project_dir: Path | None = None):
    """Resolve config and return a DB connection + config."""
    if project_dir is None:
        project_dir = Path.cwd()

    qualito_dir = project_dir / ".qualito"
    if not qualito_dir.exists():
        click.echo("Qualito not initialized. Run 'qualito init' first.")
        raise SystemExit(1)

    from qualito.config import load_config
    from qualito.core.db import get_db

    config = load_config(project_dir)
    db_path = config.db_path
    if not db_path.is_absolute():
        db_path = project_dir / db_path

    if not db_path.exists():
        click.echo("No database found. Run some delegations first.")
        raise SystemExit(1)

    conn = get_db(db_path=db_path)
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


@cli.command()
@click.option("--dir", "project_dir", type=click.Path(exists=True, path_type=Path),
              default=None, help="Project directory (default: cwd)")
def init(project_dir: Path | None):
    """Initialize Qualito in the current project."""
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

    config, qualito_dir = init_project(project_dir)

    click.echo(f"Initialized Qualito in {project_dir}")
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

    qualito_dir = project_dir / ".qualito"
    if not qualito_dir.exists():
        click.echo("Qualito not initialized. Run 'qualito init' first.")
        raise SystemExit(1)

    from qualito.config import load_config

    config = load_config(project_dir)

    # Resolve DB path
    db_path = config.db_path
    if not db_path.is_absolute():
        db_path = project_dir / db_path

    click.echo(f"Workspace: {config.workspace}")
    click.echo(f"Config: {qualito_dir / 'config.toml'}")
    click.echo(f"DB: {db_path} ({'exists' if db_path.exists() else 'missing'})")

    # SLOs
    click.echo(f"SLOs: quality={config.slo_quality:.0%}, "
               f"availability={config.slo_availability:.0%}, "
               f"cost=${config.slo_cost:.2f}")

    # Run count
    if db_path.exists():
        from qualito.core.db import get_db

        conn = get_db(db_path=db_path)
        row = conn.execute("SELECT COUNT(*) as n FROM runs").fetchone()
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
def import_sessions(project_dir: Path | None, workspace: str | None):
    """Import existing Claude Code sessions into Qualito."""
    if project_dir is None:
        project_dir = Path.cwd()

    qualito_dir = project_dir / ".qualito"
    if not qualito_dir.exists():
        click.echo("Qualito not initialized. Run 'qualito init' first.")
        raise SystemExit(1)

    from qualito.config import load_config
    from qualito.core.db import get_db
    from qualito.importer import find_session_files, import_all

    config = load_config(project_dir)
    ws = workspace or config.workspace or "default"

    # Show discoverable sessions first
    files = find_session_files(project_dir)
    if not files:
        click.echo(f"No Claude Code sessions found for {project_dir}")
        click.echo("Sessions are stored in ~/.claude/projects/")
        return

    click.echo(f"Found {len(files)} session file(s) for {project_dir}")

    # Get DB connection
    db_path = config.db_path
    if not db_path.is_absolute():
        db_path = project_dir / db_path
    conn = get_db(db_path=db_path)

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
        ws_filter = "AND r.workspace = ?" if workspace else ""
        params: list = [since]
        if workspace:
            params.append(workspace)

        rows = conn.execute(f"""
            SELECT r.id, r.workspace, r.task_type, e.score as dqi,
                   e.categories, r.cost_usd, r.started_at
            FROM evaluations e
            JOIN runs r ON r.id = e.run_id
            WHERE e.eval_type = 'dqi' AND r.started_at >= ? {ws_filter}
            ORDER BY r.started_at DESC
            LIMIT ?
        """, params + [limit]).fetchall()

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
        ws_filter = "AND workspace = ?" if workspace else ""
        ws_filter_r = "AND r.workspace = ?" if workspace else ""
        params: list = [since]
        if workspace:
            params.append(workspace)

        # Overall stats
        stats = conn.execute(f"""
            SELECT COUNT(*) as total_runs,
                   COALESCE(SUM(cost_usd), 0) as total_spend,
                   AVG(cost_usd) as avg_per_run
            FROM runs
            WHERE started_at >= ? {ws_filter}
        """, params).fetchone()

        total_runs = stats["total_runs"]
        total_spend = stats["total_spend"] or 0
        avg_per_run = stats["avg_per_run"]

        click.echo(f"\nCost Analysis (last {days} days)")
        click.echo("=" * 60)
        click.echo(f"Total runs:     {total_runs}")
        click.echo(f"Total spend:    {_fmt_cost(total_spend)}")
        click.echo(f"Avg per run:    {_fmt_cost(avg_per_run)}")

        # Cost by workspace
        ws_rows = conn.execute(f"""
            SELECT workspace,
                   COUNT(*) as runs,
                   COALESCE(SUM(cost_usd), 0) as total,
                   AVG(cost_usd) as avg
            FROM runs
            WHERE started_at >= ? {ws_filter}
            GROUP BY workspace
            ORDER BY total DESC
        """, params).fetchall()

        if ws_rows:
            click.echo(f"\n{'Workspace':<25} {'Runs':>6} {'Total':>10} {'Avg':>10}")
            click.echo("-" * 55)
            for r in ws_rows:
                click.echo(
                    f"{(r['workspace'] or '?'):<25} {r['runs']:>6} "
                    f"{_fmt_cost(r['total']):>10} {_fmt_cost(r['avg']):>10}"
                )

        # Cost by task type
        type_rows = conn.execute(f"""
            SELECT task_type,
                   COUNT(*) as runs,
                   COALESCE(SUM(cost_usd), 0) as total,
                   AVG(cost_usd) as avg
            FROM runs
            WHERE started_at >= ? {ws_filter}
            GROUP BY task_type
            ORDER BY total DESC
        """, params).fetchall()

        if type_rows:
            click.echo(f"\n{'Task Type':<25} {'Runs':>6} {'Total':>10} {'Avg':>10}")
            click.echo("-" * 55)
            for r in type_rows:
                click.echo(
                    f"{(r['task_type'] or 'other'):<25} {r['runs']:>6} "
                    f"{_fmt_cost(r['total']):>10} {_fmt_cost(r['avg']):>10}"
                )

        # Waste: runs with DQI < 0.5
        waste_params: list = [since]
        if workspace:
            waste_params.append(workspace)

        waste = conn.execute(f"""
            SELECT COUNT(*) as waste_runs,
                   COALESCE(SUM(r.cost_usd), 0) as waste_cost
            FROM runs r
            JOIN evaluations e ON e.run_id = r.id AND e.eval_type = 'dqi'
            WHERE r.started_at >= ? {ws_filter_r} AND e.score < 0.5
        """, waste_params).fetchone()

        waste_runs = waste["waste_runs"]
        waste_cost = waste["waste_cost"] or 0
        waste_pct = (waste_cost / total_spend * 100) if total_spend > 0 else 0

        click.echo(f"\nWaste (DQI < 0.5)")
        click.echo("-" * 40)
        click.echo(f"Wasteful runs:  {waste_runs}")
        click.echo(f"Wasted cost:    {_fmt_cost(waste_cost)} ({waste_pct:.1f}% of total)")

        # Top 5 most expensive runs
        top_rows = conn.execute(f"""
            SELECT id, workspace, task_type, cost_usd, started_at
            FROM runs
            WHERE started_at >= ? {ws_filter} AND cost_usd IS NOT NULL
            ORDER BY cost_usd DESC
            LIMIT 5
        """, params).fetchall()

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
        ws_filter = "AND workspace = ?" if workspace else ""
        ws_filter_r = "AND r.workspace = ?" if workspace else ""
        params: list = [since]
        if workspace:
            params.append(workspace)

        # Load SLO targets from config
        slo_quality_threshold = config.slo_quality  # e.g. 0.60
        slo_avail_target = config.slo_availability  # e.g. 0.95
        slo_cost_threshold = config.slo_cost  # e.g. 3.00

        # Total runs
        total_row = conn.execute(f"""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                   SUM(CASE WHEN cost_usd < ? THEN 1 ELSE 0 END) as under_cost
            FROM runs
            WHERE started_at >= ? {ws_filter.replace('workspace', 'workspace')}
        """, [slo_cost_threshold, since] + ([workspace] if workspace else [])).fetchone()

        total = total_row["total"] or 0
        completed = total_row["completed"] or 0
        under_cost = total_row["under_cost"] or 0

        # Quality: runs with DQI >= threshold
        quality_params: list = [slo_quality_threshold, since]
        if workspace:
            quality_params.append(workspace)

        quality_row = conn.execute(f"""
            SELECT COUNT(*) as total_scored,
                   SUM(CASE WHEN e.score >= ? THEN 1 ELSE 0 END) as quality_ok
            FROM evaluations e
            JOIN runs r ON r.id = e.run_id
            WHERE e.eval_type = 'dqi' AND r.started_at >= ? {ws_filter_r}
        """, quality_params).fetchone()

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
            ws_rows = conn.execute(f"""
                SELECT DISTINCT workspace FROM runs
                WHERE started_at >= ?
                ORDER BY workspace
            """, [since]).fetchall()

            if len(ws_rows) > 1:
                click.echo(f"\nPer-Workspace Breakdown")
                click.echo(
                    f"{'Workspace':<25} {'Quality':>10} {'Avail':>10} {'Cost':>10}"
                )
                click.echo("-" * 60)

                for ws_row in ws_rows:
                    ws_name = ws_row["workspace"]

                    # Quality for this workspace
                    wq = conn.execute("""
                        SELECT COUNT(*) as total_scored,
                               SUM(CASE WHEN e.score >= ? THEN 1 ELSE 0 END) as ok
                        FROM evaluations e
                        JOIN runs r ON r.id = e.run_id
                        WHERE e.eval_type = 'dqi' AND r.started_at >= ?
                              AND r.workspace = ?
                    """, [slo_quality_threshold, since, ws_name]).fetchone()

                    ws_q = (
                        _fmt_pct(wq["ok"] / wq["total_scored"] * 100)
                        if wq["total_scored"] > 0 else "N/A"
                    )

                    # Availability for this workspace
                    wa = conn.execute("""
                        SELECT COUNT(*) as total,
                               SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as ok
                        FROM runs
                        WHERE started_at >= ? AND workspace = ?
                    """, [since, ws_name]).fetchone()

                    ws_a = (
                        _fmt_pct(wa["ok"] / wa["total"] * 100)
                        if wa["total"] > 0 else "N/A"
                    )

                    # Cost for this workspace
                    wc = conn.execute("""
                        SELECT COUNT(*) as total,
                               SUM(CASE WHEN cost_usd < ? THEN 1 ELSE 0 END) as ok
                        FROM runs
                        WHERE started_at >= ? AND workspace = ?
                    """, [slo_cost_threshold, since, ws_name]).fetchone()

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
