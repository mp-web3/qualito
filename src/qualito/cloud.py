"""Cloud sync client for Qualito.

Pushes local runs and incidents to the hosted Qualito API.
Credentials stored at ~/.qualito/credentials.json.
"""

import json
import sqlite3
import stat
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_API_URL = "https://api.qualito.dev"
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


def cloud_request(method: str, path: str, data: dict | None = None) -> dict:
    """Make an authenticated HTTP request to the Qualito cloud API.

    Args:
        method: HTTP method (GET, POST, etc.)
        path: API path (e.g. /api/auth/me)
        data: JSON body for POST/PUT requests

    Returns:
        Parsed JSON response dict.

    Raises:
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
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 401:
            raise CloudError("Authentication failed. Run 'qualito login' to re-authenticate.", 401)
        if e.code == 403:
            raise CloudError("Access denied.", 403)
        body_text = ""
        try:
            body_text = e.read().decode()
        except Exception:
            pass
        raise CloudError(f"API error {e.code}: {body_text}", e.code)
    except urllib.error.URLError as e:
        raise CloudError(f"Cannot reach {api_url}: {e.reason}")


def _collect_run_data(conn: sqlite3.Connection, run_id: str) -> dict:
    """Collect a run with its evaluations, tool_calls, and file_activity."""
    row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if not row:
        return {}
    run = dict(row)

    # Evaluations
    evals = conn.execute(
        "SELECT * FROM evaluations WHERE run_id = ? ORDER BY id", (run_id,)
    ).fetchall()
    run["evaluations"] = [dict(r) for r in evals]

    # Tool calls
    tcs = conn.execute(
        "SELECT * FROM tool_calls WHERE run_id = ? ORDER BY id", (run_id,)
    ).fetchall()
    run["tool_calls"] = [dict(r) for r in tcs]

    # File activity
    fas = conn.execute(
        "SELECT * FROM file_activity WHERE run_id = ? ORDER BY id", (run_id,)
    ).fetchall()
    run["file_activity"] = [dict(r) for r in fas]

    return run


def sync_runs(conn: sqlite3.Connection, since: str | None = None) -> dict:
    """Sync local runs to the cloud API.

    Args:
        conn: Local SQLite connection.
        since: ISO date string — only sync runs started after this date.
               If None, syncs all runs.

    Returns:
        dict with keys: synced, skipped, errors.
    """
    where = "WHERE started_at >= ?" if since else ""
    params = [since] if since else []

    rows = conn.execute(
        f"SELECT id FROM runs {where} ORDER BY started_at ASC", params
    ).fetchall()

    if not rows:
        return {"synced": 0, "skipped": 0, "errors": 0}

    # Collect all runs
    batch = []
    for row in rows:
        run_data = _collect_run_data(conn, row["id"])
        if run_data:
            batch.append(run_data)

    if not batch:
        return {"synced": 0, "skipped": 0, "errors": 0}

    # Send in batches of 50
    total_synced = 0
    total_skipped = 0
    total_errors = 0
    batch_size = 50

    for i in range(0, len(batch), batch_size):
        chunk = batch[i : i + batch_size]
        result = cloud_request("POST", "/api/sync/runs", {"runs": chunk})
        total_synced += result.get("synced", 0)
        total_skipped += result.get("skipped", 0)
        total_errors += result.get("errors", 0)

    return {"synced": total_synced, "skipped": total_skipped, "errors": total_errors}


def sync_incidents(conn: sqlite3.Connection) -> dict:
    """Sync local incidents to the cloud API.

    Returns:
        dict with keys: synced, skipped, errors.
    """
    rows = conn.execute(
        "SELECT * FROM incidents ORDER BY created_at ASC"
    ).fetchall()

    if not rows:
        return {"synced": 0, "skipped": 0, "errors": 0}

    incidents = [dict(r) for r in rows]
    result = cloud_request("POST", "/api/sync/incidents", {"incidents": incidents})
    return {
        "synced": result.get("synced", 0),
        "skipped": result.get("skipped", 0),
        "errors": result.get("errors", 0),
    }
