"""Cloud sync client for Qualito.

Pushes local runs and incidents to the hosted Qualito API.
Credentials stored at ~/.qualito/credentials.json.
"""

import json
import stat
import urllib.error
import urllib.request
from pathlib import Path

from sqlalchemy import select

from qualito.core.db import (
    get_run,
    incidents_table,
    runs_table,
)

DEFAULT_API_URL = "https://api.qualito.ai"
CREDENTIALS_PATH = Path.home() / ".qualito" / "credentials.json"


def load_credentials() -> dict | None:
    """Load API credentials from ~/.qualito/credentials.json.

    Returns dict with 'api_key' and 'api_url', or None if file missing.
    """
    if not CREDENTIALS_PATH.exists():
        return None
    try:
        data = json.loads(CREDENTIALS_PATH.read_text())
        if "api_key" not in data:
            return None
        return data
    except (json.JSONDecodeError, OSError):
        return None


def save_credentials(api_key: str, api_url: str = DEFAULT_API_URL) -> None:
    """Save API credentials to ~/.qualito/credentials.json with 0600 permissions."""
    CREDENTIALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = {"api_key": api_key, "api_url": api_url}
    CREDENTIALS_PATH.write_text(json.dumps(data, indent=2))
    CREDENTIALS_PATH.chmod(stat.S_IRUSR | stat.S_IWUSR)


def delete_credentials() -> bool:
    """Delete credentials file. Returns True if file existed."""
    if CREDENTIALS_PATH.exists():
        CREDENTIALS_PATH.unlink()
        return True
    return False


class CloudError(Exception):
    """Error communicating with the Qualito cloud API."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class WorkspaceLimitError(CloudError):
    """Raised when a free-plan sync would exceed the workspace limit."""

    def __init__(
        self,
        message: str,
        limit: int,
        current_workspaces: list[str],
        attempted_workspaces: list[str],
        upgrade_url: str,
    ):
        super().__init__(message, status_code=403)
        self.limit = limit
        self.current_workspaces = current_workspaces
        self.attempted_workspaces = attempted_workspaces
        self.upgrade_url = upgrade_url


def _parse_403_detail(body_text: str) -> CloudError:
    """Parse a 403 response body into the most specific exception we can.

    Newer servers return a structured ``detail`` dict that lets the CLI render
    exactly which workspaces tripped the limit. Older servers return a plain
    string — fall back to a generic CloudError in that case so the CLI still
    surfaces something useful.
    """
    try:
        payload = json.loads(body_text) if body_text else {}
    except (json.JSONDecodeError, TypeError):
        return CloudError(body_text or "Access denied.", 403)

    detail = payload.get("detail") if isinstance(payload, dict) else None

    if isinstance(detail, str):
        return CloudError(detail, 403)

    if isinstance(detail, dict) and detail.get("error") == "workspace_limit":
        return WorkspaceLimitError(
            message=detail.get(
                "message", "Free plan limited to 3 workspaces. Upgrade to Pro for unlimited."
            ),
            limit=int(detail.get("limit", 3)),
            current_workspaces=list(detail.get("current_workspaces", [])),
            attempted_workspaces=list(detail.get("attempted_workspaces", [])),
            upgrade_url=detail.get("upgrade_url", "https://app.qualito.ai/settings"),
        )

    if isinstance(detail, dict) and "message" in detail:
        return CloudError(str(detail["message"]), 403)

    return CloudError("Access denied.", 403)


def cloud_request(method: str, path: str, data: dict | None = None, timeout: int = 30) -> dict:
    """Make an authenticated HTTP request to the Qualito cloud API.

    Args:
        method: HTTP method (GET, POST, etc.)
        path: API path (e.g. /api/auth/me)
        data: JSON body for POST/PUT requests

    Returns:
        Parsed JSON response dict.

    Raises:
        WorkspaceLimitError: on structured 403 ``workspace_limit`` errors.
        CloudError: On auth failure, network error, or non-2xx response.
    """
    creds = load_credentials()
    if not creds:
        raise CloudError("Not logged in. Run 'qualito login' first.")

    api_url = creds.get("api_url", DEFAULT_API_URL).rstrip("/")
    url = f"{api_url}{path}"

    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Authorization", f"Bearer {creds['api_key']}")
    req.add_header("Content-Type", "application/json")
    req.add_header("User-Agent", "qualito-cli")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 401:
            raise CloudError("Authentication failed. Run 'qualito login' to re-authenticate.", 401)
        body_text = ""
        try:
            body_text = e.read().decode()
        except Exception:
            pass
        if e.code == 403:
            raise _parse_403_detail(body_text)
        raise CloudError(f"API error {e.code}: {body_text}", e.code)
    except urllib.error.URLError as e:
        raise CloudError(f"Cannot reach {api_url}: {e.reason}")


def _collect_run_data(conn, run_id: str) -> dict:
    """Collect a run with its evaluations, tool_calls, and file_activity."""
    return get_run(conn, run_id) or {}


def fetch_user() -> dict:
    """Fetch the authenticated user from /api/auth/me."""
    return cloud_request("GET", "/api/auth/me")


def fetch_synced_workspaces() -> list[dict]:
    """Fetch the list of synced workspaces for the current user.

    Returns a list of dicts with keys: workspace_name, first_synced_at,
    last_synced_at, session_count.
    """
    result = cloud_request("GET", "/api/sync/workspaces")
    if isinstance(result, list):
        return result
    return result.get("workspaces", [])


def sync_runs(
    conn,
    since: str | None = None,
    workspaces: list[str] | None = None,
    on_batch=None,
    on_workspace_done=None,
) -> dict:
    """Sync local runs to the cloud API, workspace-by-workspace.

    Runs are grouped by workspace and each workspace's runs are sent in
    batches of 10. ``on_batch`` is called after every successful batch and
    ``on_workspace_done`` fires once a workspace's batches all complete, so
    the CLI can render per-workspace progress cleanly.

    Args:
        conn: Database connection (SA Connection).
        since: ISO date string — only sync runs started after this date.
               If None, syncs all runs.
        workspaces: List of workspace names to sync. If None, syncs every
            workspace in the DB. Order is preserved so the CLI can surface
            the user's picker order.
        on_batch: Optional callable(workspace, batch_num, total_batches, runs_in_batch)
            called after each successful batch.
        on_workspace_done: Optional callable(workspace, synced_count) called
            after all batches for a workspace finish successfully.

    Returns:
        dict with keys: synced, skipped, errors, by_workspace (mapping
        workspace → synced_count).

    Raises:
        WorkspaceLimitError: when the server rejects the sync with a
            structured workspace_limit 403.
        CloudError: on other API or network errors.
    """
    stmt = select(runs_table.c.id, runs_table.c.workspace).order_by(
        runs_table.c.workspace, runs_table.c.started_at
    )
    if since:
        stmt = stmt.where(runs_table.c.started_at >= since)
    if workspaces:
        stmt = stmt.where(runs_table.c.workspace.in_(workspaces))

    rows = conn.execute(stmt).mappings().fetchall()
    if not rows:
        return {"synced": 0, "skipped": 0, "errors": 0, "by_workspace": {}}

    by_workspace: dict[str, list[dict]] = {}
    for row in rows:
        ws = row["workspace"] or ""
        run_data = _collect_run_data(conn, row["id"])
        if not run_data:
            continue
        by_workspace.setdefault(ws, []).append(run_data)

    if not by_workspace:
        return {"synced": 0, "skipped": 0, "errors": 0, "by_workspace": {}}

    if workspaces:
        # Preserve the caller's ordering so users see progress in the order
        # they picked in the interactive picker.
        ordered = [w for w in workspaces if w in by_workspace]
    else:
        ordered = sorted(by_workspace.keys())

    batch_size = 10
    total_synced = 0
    total_skipped = 0
    total_errors = 0
    per_workspace_synced: dict[str, int] = {}

    for ws_name in ordered:
        runs_for_ws = by_workspace[ws_name]
        if not runs_for_ws:
            continue

        total_batches = (len(runs_for_ws) + batch_size - 1) // batch_size
        ws_synced = 0

        for i in range(0, len(runs_for_ws), batch_size):
            batch_num = i // batch_size + 1
            chunk = runs_for_ws[i : i + batch_size]
            result = cloud_request(
                "POST", "/api/sync/runs", {"runs": chunk}, timeout=120
            )
            batch_synced = result.get("synced", 0)
            total_synced += batch_synced
            total_skipped += result.get("skipped", 0)
            total_errors += result.get("errors", 0)
            ws_synced += batch_synced

            if on_batch:
                on_batch(ws_name, batch_num, total_batches, len(chunk))

        per_workspace_synced[ws_name] = ws_synced

        if on_workspace_done:
            on_workspace_done(ws_name, ws_synced)

    return {
        "synced": total_synced,
        "skipped": total_skipped,
        "errors": total_errors,
        "by_workspace": per_workspace_synced,
    }


def sync_incidents(conn) -> dict:
    """Sync local incidents to the cloud API.

    Returns:
        dict with keys: synced, skipped, errors.
    """
    rows = conn.execute(
        select(incidents_table).order_by(incidents_table.c.created_at)
    ).mappings().fetchall()

    if not rows:
        return {"synced": 0, "skipped": 0, "errors": 0}

    incidents = [dict(r) for r in rows]
    result = cloud_request("POST", "/api/sync/incidents", {"incidents": incidents})
    return {
        "synced": result.get("synced", 0),
        "skipped": result.get("skipped", 0),
        "errors": result.get("errors", 0),
    }
