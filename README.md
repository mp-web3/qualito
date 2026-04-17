# Qualito

Quality metrics for AI-assisted development. Know if your Claude Code sessions are worth the cost.

[![PyPI](https://img.shields.io/pypi/v/qualito)](https://pypi.org/project/qualito/)
[![Python](https://img.shields.io/pypi/pyversions/qualito)](https://pypi.org/project/qualito/)
[![License](https://img.shields.io/github/license/mp-web3/qualito)](LICENSE)

## What it does

Qualito scores every Claude Code session on multiple quality dimensions (error rate, tool diversity, cache utilization, completion with work) and rolls them into a single **Score** (0-100). It tracks cost, detects incidents (error spikes, cost anomalies, quality drops), and surfaces actionable recommendations.

- Read existing Claude Code sessions — no behavior change needed
- Score each session across multiple dimensions (0-100)
- Track costs, detect incidents, monitor quality trends
- Set SLOs and get alerts when quality drops
- Compare runs before/after a change (observational experiments)

## Quick Start

**See [docs/getting-started.md](docs/getting-started.md)** for the full walkthrough.

Short version:

1. Sign up at **https://app.qualito.ai/login**
2. Copy the setup command shown on the onboarding page (includes a one-time token)
3. Paste into your terminal:
   ```bash
   uvx qualito@latest setup st_setup_<your_token>
   ```
4. Refresh the dashboard → your sessions are live

## Cloud Dashboard

**Free tier:** 3 workspaces synced. **Pro ($29/mo, coming soon):** unlimited workspaces + LLM-generated root-cause + report summaries.

## CLI Commands

| Command | What it does |
|---------|-------------|
| `qualito setup` | First-time setup: import sessions, configure MCP, optional cloud sync |
| `qualito status` | See your local and cloud sync state |
| `qualito import` | Import Claude Code sessions for measurement (`--force` to re-process) |
| `qualito costs` | Analyze spending by workspace, model, and time |
| `qualito privacy` | View or change per-workspace sync privacy settings |
| `qualito audit list` | List flagged runs needing review |
| `qualito sync` | Push local sessions to the cloud dashboard |
| `qualito login` | Authenticate with the Qualito cloud |
| `qualito logout` | Remove cloud credentials |

## View your data

Qualito is CLI-first. Your data lives locally and is accessed through commands:

```bash
qualito status       # local + cloud breakdown with per-workspace tokens + cost
qualito costs        # detailed spend analysis
qualito privacy      # per-workspace sync privacy settings
qualito audit list   # flagged runs needing review
```

For a web UI with charts and history, sync to the cloud and view at
https://app.qualito.ai.

## Privacy

Qualito is local-first. Your session data lives on your machine, and you
control what syncs to the cloud on a per-workspace basis.

- Default: metadata only (counts, durations, types, scores)
- Opt in per workspace to sync full content for a richer dashboard
- Every sync scanned for secrets (AWS keys, API tokens, passwords, etc.)
- Server enforces your stated settings — defense in depth

See [docs/privacy.md](docs/privacy.md) for the full field-by-field breakdown.

## MCP Server

Use Qualito inline in your editor via MCP:

```json
{
  "mcpServers": {
    "qualito": {
      "command": "uvx",
      "args": ["qualito-mcp"]
    }
  }
}
```

Tools available: `qualito_setup`, `qualito_score`, `qualito_cost`, `qualito_patterns`, `qualito_warnings`, `qualito_templates`, `qualito_incidents`, `qualito_slo`.

## Local Development

```bash
git clone https://github.com/mp-web3/qualito.git
cd qualito
uv sync --extra dev --extra server

# Run tests
uv run pytest
```

## License

MIT
