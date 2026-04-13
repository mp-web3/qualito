# Qualito

Quality metrics for AI-assisted development. Know if your Claude Code sessions are worth the cost.

[![PyPI](https://img.shields.io/pypi/v/qualito)](https://pypi.org/project/qualito/)
[![Python](https://img.shields.io/pypi/pyversions/qualito)](https://pypi.org/project/qualito/)
[![License](https://img.shields.io/github/license/mp-web3/qualito)](LICENSE)

## What it does

Qualito analyzes your Claude Code sessions and gives you a **DQI score** (Delegation Quality Index) — a composite metric that measures how effectively you're using AI assistance. It tracks cost, duration, success rate, and quality across every session.

- Import existing Claude Code sessions — no behavior change needed
- Score each session with a DQI composite (0-100)
- Track costs, detect incidents, monitor quality trends
- Set SLOs and get alerts when quality drops
- Run experiments to compare different approaches

## Quick Start

```bash
# Install and set up (imports your existing Claude Code sessions)
uvx qualito setup

# Or install permanently
uv tool install qualito
qualito setup
```

Every Claude Code user already has session data at `~/.claude/projects/`. Qualito reads it — you'll see your first scores in under 2 minutes.

## Cloud Dashboard

```bash
# Authenticate with qualito.ai
qualito login

# Push data to cloud
qualito sync

# Open dashboard
# → https://app.qualito.ai
```

**Free tier:** 3 workspaces. **Pro ($29/mo):** unlimited workspaces + quality scoring.

## CLI Commands

| Command | What it does |
|---------|-------------|
| `qualito setup` | First-time setup: import sessions, configure MCP, optional cloud sync |
| `qualito status` | See your local and cloud sync state |
| `qualito import` | Import Claude Code sessions for measurement (`--force` to re-process) |
| `qualito costs` | Analyze spending by workspace, model, and time |
| `qualito sync` | Push local sessions to the cloud dashboard |
| `qualito login` | Authenticate with the Qualito cloud |
| `qualito logout` | Remove cloud credentials |
| `qualito dashboard` | Launch the local web dashboard |

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

Tools available: `qualito_setup`, `dqi_cost`, `dqi_patterns`, `dqi_warnings`, `dqi_templates`.

## Local Development

```bash
git clone https://github.com/mp-web3/qualito.git
cd qualito
uv sync --extra dev --extra dashboard

# Run tests
uv run pytest

# Local dashboard
uv run qualito dashboard
```

## License

MIT
