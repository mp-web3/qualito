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


class SecretsDetectedError(CloudError):
    """Raised when pre-sync scanning finds potential secrets in runs.

    findings_by_run maps run_id -> list[Finding], so the CLI can show
    users which runs are affected and offer skip/abort/review options.
    """

    def __init__(self, message: str, findings_by_run: dict[str, list]):
        super().__init__(message, status_code=None)
        self.findings_by_run = findings_by_run


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


_METADATA_ONLY_RUN_KEEP: frozenset[str] = frozenset({
    "id", "workspace", "task_type", "model", "pipeline_mode", "status",
    "cost_usd", "input_tokens", "output_tokens", "cache_read_tokens",
    "duration_ms", "started_at", "completed_at", "source", "session_type",
    "entrypoint", "claude_version", "session_name", "has_subagents",
    "subagent_count", "error_count", "tool_count", "paper_live_gap",
    "skill_name", "user_id",
})

_METADATA_ONLY_TOOL_CALL_KEEP: frozenset[str] = frozenset({
    "tool_name", "is_error", "phase", "timestamp", "duration_ms",
})

_METADATA_ONLY_FILE_ACTIVITY_KEEP: frozenset[str] = frozenset({
    "action", "timestamp",
})

_METADATA_ONLY_EVALUATION_KEEP: frozenset[str] = frozenset({
    "eval_type", "score", "categories", "created_at",
})


def _strip_run_to_metadata(run: dict) -> dict:
    """Return a metadata-only copy of a run dict.

    Strips every text-bearing field. Keeps categorical, numeric, IDs, and
    timestamps. Filters tool_calls / file_activity / evaluations child lists
    to keep-only columns. Drops the artifacts list entirely (every artifact
    field is sensitive).

    Idempotent — stripping a stripped dict returns the same shape.
    """
    out = {k: v for k, v in run.items() if k in _METADATA_ONLY_RUN_KEEP}

    tool_calls = run.get("tool_calls") or []
    out["tool_calls"] = [
        {k: v for k, v in tc.items() if k in _METADATA_ONLY_TOOL_CALL_KEEP}
        for tc in tool_calls
    ]

    file_activity = run.get("file_activity") or []
    out["file_activity"] = [
        {k: v for k, v in fa.items() if k in _METADATA_ONLY_FILE_ACTIVITY_KEEP}
        for fa in file_activity
    ]

    evaluations = run.get("evaluations") or []
    out["evaluations"] = [
        {k: v for k, v in ev.items() if k in _METADATA_ONLY_EVALUATION_KEEP}
        for ev in evaluations
    ]

    out["artifacts"] = []

    return out


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


def fetch_workspace_privacy(workspace: str) -> dict:
    """Fetch per-workspace privacy setting.

    Returns {workspace_name, sync_content, allow_llm, is_default}. Defaults
    to {sync_content: False, allow_llm: False, is_default: True} if no row
    exists (404). ``is_default`` is True only when the server had no row —
    the CLI uses it to detect first-sync workspaces and prompt for an
    explicit privacy choice. Server responses with a real row always come
    back with ``is_default: False``.
    """
    try:
        result = cloud_request("GET", f"/api/sync/workspaces/{workspace}/privacy")
    except CloudError as e:
        if getattr(e, "status_code", None) == 404:
            return {
                "workspace_name": workspace,
                "sync_content": False,
                "allow_llm": False,
                "is_default": True,
            }
        raise
    if isinstance(result, dict):
        result.setdefault("is_default", False)
    return result


def set_workspace_privacy(
    workspace: str, sync_content: bool, allow_llm: bool | None = None
) -> dict:
    """Upsert per-workspace privacy setting via PATCH.

    allow_llm=None means leave unchanged (server supports partial updates).
    """
    payload: dict = {"sync_content": sync_content}
    if allow_llm is not None:
        payload["allow_llm"] = allow_llm
    return cloud_request(
        "PATCH", f"/api/sync/workspaces/{workspace}/privacy", data=payload
    )


def sync_runs(
    conn,
    since: str | None = None,
    workspaces: list[str] | None = None,
    on_batch=None,
    on_workspace_done=None,
    exclude_runs: set[str] | None = None,
) -> dict:
    """Sync local runs to the cloud API, workspace-by-workspace.

    Runs are grouped by workspace and each workspace's runs are sent in
    batches of 10. Before any upload, runs are scanned for secrets and each
    workspace's privacy setting is fetched — workspaces with sync_content
    disabled have their payloads stripped to metadata-only. ``on_batch`` is
    called after every successful batch and ``on_workspace_done`` fires once
    a workspace's batches all complete, so the CLI can render per-workspace
    progress cleanly.

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
        exclude_runs: Optional set of run IDs to drop before scanning and
            upload. The CLI uses this to skip runs the user dismissed from
            a secret-findings prompt.

    Returns:
        dict with keys: synced, skipped, errors, by_workspace (mapping
        workspace → synced_count).

    Raises:
        SecretsDetectedError: when pre-sync scanning finds any secret in the
            batched runs — raised before any /api/sync/runs POST.
        WorkspaceLimitError: when the server rejects the sync with a
            structured workspace_limit 403.
        CloudError: on other API or network errors.
    """
    from qualito.core.secret_scanner import scan_run

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
        if exclude_runs and run_data.get("id") in exclude_runs:
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

    # Fetch per-workspace privacy settings before scanning so a 403/500 on
    # the privacy endpoint fails fast — no runs leave the machine.
    privacy_by_ws: dict[str, dict] = {
        ws_name: fetch_workspace_privacy(ws_name) for ws_name in ordered
    }

    # Secret scan ALL runs regardless of workspace privacy — secrets block
    # even metadata-only workspaces because findings live in text fields we
    # would otherwise just strip silently.
    findings_by_run: dict[str, list] = {}
    for ws_name in ordered:
        for run_data in by_workspace[ws_name]:
            run_id = run_data.get("id")
            findings = scan_run(run_data)
            if findings:
                findings_by_run[run_id] = findings

    if findings_by_run:
        raise SecretsDetectedError(
            "Potential secrets detected in runs.", findings_by_run
        )

    batch_size = 10
    total_synced = 0
    total_skipped = 0
    total_errors = 0
    per_workspace_synced: dict[str, int] = {}

    for ws_name in ordered:
        runs_for_ws = by_workspace[ws_name]
        if not runs_for_ws:
            continue

        ws_privacy = privacy_by_ws.get(ws_name) or {}
        if not ws_privacy.get("sync_content", False):
            runs_for_ws = [_strip_run_to_metadata(r) for r in runs_for_ws]

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
