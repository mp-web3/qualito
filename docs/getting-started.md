# Getting Started

Set up Qualito in under 3 minutes. You'll see your first session scores, costs, and quality signals on the dashboard the moment setup completes.

## What you need

- **Python 3.11+** (Qualito is a Python CLI)
- **`uv`** — fast Python package manager. Install once:
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
- **Existing Claude Code sessions** on your machine (Qualito reads `~/.claude/projects/`). If you've never run Claude Code, install it and run at least one session first.

## Step 1 — Sign up

1. Go to **https://app.qualito.ai/login**
2. Click **Register**
3. Email + password + name → **Create Account**

After registering you'll land on the onboarding page. It will show you a setup command with a one-time **setup token** baked in. Keep that page open — you'll copy the command in the next step.

## Step 2 — Run the setup command

Copy the command from the onboarding page. It looks like:

```bash
uvx qualito@latest setup st_setup_<your_token>
```

Paste it into your terminal and hit enter. The CLI will:

1. Scan `~/.claude/projects/` for Claude Code sessions
2. Import them into a local DB at `~/.qualito/qualito.db`
3. Score each session on multiple dimensions (errors, tool diversity, cache utilization, cost)
4. Configure the Qualito MCP server in your `~/.claude.json` so Claude Code itself can query your metrics
5. Validate the token with the cloud, mint an API key, and save credentials
6. Push your session data to the cloud dashboard

Watch the terminal output — progress is streamed live to the onboarding page via Server-Sent Events.

## Step 3 — See your data

When setup completes, refresh **https://app.qualito.ai** and you'll see:

- **Score** — a composite quality metric per session (0-100)
- **Sessions** — every Claude Code session you've run, sortable by cost, workspace, score
- **Incidents** — auto-detected regressions (error spikes, cost anomalies, quality drops)
- **Recommendations** — actionable suggestions based on your patterns

## Privacy defaults

Qualito is **local-first**. Your session data lives at `~/.qualito/qualito.db` on your machine. When you sync to the cloud, you choose per-workspace what goes up:

- **Full content** (default for new workspaces): prompts, tool outputs, file paths — makes the dashboard genuinely useful
- **Metadata only**: counts, durations, types, scores — no free-text content crosses the wire

Every sync is **scanned for secrets** (AWS keys, API tokens, postgres URLs, passwords). Flagged runs are NOT sent — you review them offline via `qualito audit list`.

Change privacy per-workspace at any time:

```bash
qualito privacy <workspace> --metadata   # switch to metadata-only
qualito privacy <workspace> --full       # switch to full content
```

See [privacy.md](./privacy.md) for the full field-by-field breakdown.

## Daily workflow

After the initial setup, Qualito picks up new sessions automatically the next time you run `sync`:

```bash
qualito sync           # push anything new since last sync
qualito status         # see local vs cloud state
qualito costs          # detailed spend breakdown
qualito audit list     # review runs flagged for secrets
```

## Free tier limits

- **3 workspaces** synced to the cloud
- Unlimited local usage

If you have more than 3 workspaces, Qualito will ask you to pick which three to sync the first time. Switch later with `qualito sync --unsync <workspace>` then re-sync a different one.

**Pro ($29/mo)** — coming soon — unlocks unlimited workspaces, LLM-generated incident root-cause analysis, and incident report summaries.

## MCP integration (optional)

`qualito setup` already configured the MCP server in your `~/.claude.json`. It gives you these tools inside Claude Code:

| Tool | What it does |
|---|---|
| `qualito_setup` | Check setup status, get the setup token |
| `qualito_score` | Your current session score + breakdown |
| `qualito_cost` | Cost analysis across workspaces |
| `qualito_patterns` | Find recurring tool-use patterns in your sessions |
| `qualito_warnings` | Runs flagged for quality issues |
| `qualito_templates` | Proven session templates from your own history |
| `qualito_incidents` | Active quality incidents |
| `qualito_slo` | SLO compliance status |

Use them in a chat like: *"Use qualito_cost to show me my spend this week broken down by workspace."*

## Troubleshooting

**`qualito setup` says "No Claude Code projects found"**
You haven't run Claude Code yet, or your projects live somewhere other than `~/.claude/projects/`. Start a Claude Code session first, then re-run setup.

**`uvx` cached an old version of Qualito**
Force refresh:

```bash
uv cache clean qualito
uvx --refresh qualito@latest setup <token>
```

**Setup token expired**
Tokens are single-use and last 60 minutes. Generate a new one from the onboarding page (or `qualito_setup` MCP tool if you're already logged in).

**Dashboard shows 0 sessions after setup**
Check `qualito status` — does it show "Cloud: 0 synced"? If yes, the sync step may have been skipped. Run `qualito sync --all` explicitly.

**"Server rejected all runs" when syncing**
Usually means the workspaces you selected aren't in your free-tier allotment. Run `qualito status` to see which three workspaces are synced. Unsync one (`qualito sync --unsync X`) and sync the replacement.

**Errors still look truncated in the dashboard**
Sync is idempotent — already-synced runs won't be re-pushed even after a client upgrade. Run `qualito import --force` and sync again to get the latest parser on new sessions. Existing cloud runs stay at their original content size.

## Questions?

- GitHub issues: https://github.com/mp-web3/qualito/issues
- Source: https://github.com/mp-web3/qualito
