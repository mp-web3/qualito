# Qualito Privacy

Qualito is local-first. Everything Claude Code captured stays on your machine,
verbatim, forever. Cloud sync is opt-in per workspace and defaults to metadata
only. Secret scanning runs at the sync boundary regardless of workspace mode.

## Local storage — everything, verbatim

The local SQLite database at `~/.qualito/qualito.db` stores the full content of
every imported session: prompts, tool outputs, file paths, summaries, artifacts.
No filtering at import time. The user owns their local data, always.

## Cloud sync — opt-in per workspace

Per workspace, you choose one of two modes:

- **Metadata only** (default): counts, durations, types, scores, IDs, timestamps
  — everything categorical and numeric. No text content.
- **Full content** (opt-in): everything from metadata mode PLUS task text, tool
  outputs, file paths, artifacts, phase summaries.

Change modes:

```
qualito privacy <workspace> --metadata
qualito privacy <workspace> --full     # prompts for confirmation
```

View current settings:

```
qualito privacy                  # table of all synced workspaces
qualito privacy <workspace>      # details for one
```

The server enforces your stated setting. Even if the CLI has a bug or ships
outdated code, the server rejects any sync payload that doesn't match. See
"Defense in depth" below.

### Field-by-field reference

The tables below show which fields in each local table ship to the cloud in
each mode. "KEEP" means always synced, "STRIP" means only synced in Full mode.

**runs table**

| Field | Metadata mode | Full mode |
|---|:-:|:-:|
| id, workspace, task_type, model, pipeline_mode, status | ✓ | ✓ |
| cost_usd, input_tokens, output_tokens, cache_read_tokens | ✓ | ✓ |
| duration_ms, started_at, completed_at | ✓ | ✓ |
| source, session_type, entrypoint, claude_version, session_name | ✓ | ✓ |
| has_subagents, subagent_count, error_count, tool_count | ✓ | ✓ |
| paper_live_gap, skill_name, user_id | ✓ | ✓ |
| task | ❌ | ✓ |
| summary | ❌ | ✓ |
| files_changed | ❌ | ✓ |
| prompt, original_prompt | ❌ | ✓ |
| researcher_summary, implementer_summary, verifier_verdict | ❌ | ✓ |
| prompt_components | ❌ | ✓ |
| branch | ❌ | ✓ |

**tool_calls table**

| Field | Metadata mode | Full mode |
|---|:-:|:-:|
| tool_name, is_error, phase, timestamp, duration_ms | ✓ | ✓ |
| arguments_summary | ❌ | ✓ |
| result_summary | ❌ | ✓ |

**file_activity table**

| Field | Metadata mode | Full mode |
|---|:-:|:-:|
| action, timestamp | ✓ | ✓ |
| file_path | ❌ | ✓ |

**evaluations table**

| Field | Metadata mode | Full mode |
|---|:-:|:-:|
| eval_type, score, categories, created_at | ✓ | ✓ |
| checks | ❌ | ✓ |
| notes | ❌ | ✓ |

**artifacts table**

| Field | Metadata mode | Full mode |
|---|:-:|:-:|
| (entire list) | ❌ dropped | ✓ |

Artifacts are dropped entirely in metadata mode because every artifact field
(title, content, file_path, metadata) is content-bearing.

## Secret scanning

Every run is scanned at sync time, regardless of workspace privacy mode. The
scanner covers ~26 patterns across categories:

- **Cloud provider keys**: AWS access keys, Google Cloud API keys, Azure
  storage account keys
- **LLM API keys**: Anthropic, OpenAI (classic + project-scoped), Hugging Face
- **Developer tokens**: GitHub PAT (classic + fine-grained + OAuth), GitLab
  PAT, Slack
- **Payment APIs**: Stripe live, test, and restricted keys
- **Generic**: JSON Web Tokens (JWT), PEM and PKCS8 private keys
- **HTTP headers**: `Authorization: Bearer` and `X-API-Key` headers
- **Database connection strings**: postgres, mysql, mongodb, redis with
  embedded credentials
- **Environment-style**: `KEY=value` assignments where the name looks
  sensitive (API_KEY, SECRET, TOKEN, PASSWORD, etc.)

If the scanner finds matches:

1. The entire sync blocks before any HTTP POST to the cloud
2. You see a summary of which runs contained which patterns in which fields
3. You choose:
   - **Skip**: exclude the flagged runs from this sync, sync the rest
   - **Abort**: cancel the entire sync
   - **Review**: walk each finding, mark true positive vs false positive

Match previews show only the first 8 characters of the matched value followed
by `...` — the scanner never reveals the full secret in output.

## Offline audit tool

For historical local data that pre-dates the scanner, use the `audit` command
group:

```
qualito audit secrets [--workspace W] [--since DATE]
    Scan local runs for secrets. Per-finding review prompt.
    True positives get flagged in the local DB.

qualito audit list [--workspace W]
    Table of currently flagged runs.

qualito audit unflag <run_id>
    Clear a flag (partial 8-char prefix supported).

qualito audit drop [--yes]
    Delete all flagged runs from the local DB, including
    tool_calls, file_activity, evaluations, and artifacts.
    Prompts for confirmation unless --yes is passed.
```

Flagging is a local marker — it does not delete the secret from the DB. The
sync-time scanner is what actually blocks a secret from leaving your machine,
and it re-scans every run on every sync, so a flagged run will be re-detected
on the next `qualito sync` and you will see the skip/abort/review prompt
again. To remove a secret permanently, use `qualito audit drop` (which
cascades through `tool_calls`, `file_activity`, `evaluations`, `artifacts`,
and `conversations`).

## Defense in depth — server-side enforcement

The server has its OWN allowlist, duplicated from the client's. The flow:

1. CLI calls `fetch_workspace_privacy(workspace)` to read the current setting
2. CLI strips content if `sync_content=False` (per the client allowlist)
3. CLI POSTs the result to `/api/sync/runs`
4. Server reads its own allowlist and scans the payload for STRIP fields
5. If any STRIP field is non-empty in metadata mode, server rejects the entire
   batch with HTTP 400:

```json
{
  "detail": {
    "error": "privacy_violation",
    "message": "Workspace X has sync_content=False but payload contains stripped fields.",
    "workspace": "X",
    "fields_present": ["runs.task", "tool_calls[0].arguments_summary", "..."]
  }
}
```

A buggy client cannot leak. The server's allowlist is the source of truth.

## Changing your mind

```
qualito privacy <workspace> --metadata    # downgrade to metadata-only
qualito privacy <workspace> --full        # upgrade to full content
```

The server honors the new setting on the next sync. Runs that were already
synced under the previous setting are NOT retroactively re-filtered. If you
want those removed, open the cloud dashboard and delete them manually, or use
`qualito audit drop` locally to prevent them from re-syncing.

## What Qualito NEVER does

- Ships your source code to third parties
- Uses your sessions to train any model
- Shares data across users
- Applies machine learning to your content without explicit consent (future
  Pro features will require opt-in via the `allow_llm` flag per workspace)

## Changing modes via flags

The `privacy` command also supports an `allow_llm` flag (reserved for future
Pro LLM-based analysis):

```
qualito privacy <workspace> --allow-llm
qualito privacy <workspace> --no-allow-llm
```

This flag is forward-compatible — no current feature uses it. It exists so
the opt-in model is in place before Pro features ship.
