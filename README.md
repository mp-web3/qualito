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
# Install
pip install qualito

# Initialize in your project
cd your-project
qualito init

# Import your Claude Code sessions
qualito import

# Score them
qualito score

# See results
qualito status
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

**Free tier:** 100 runs/month, 3 workspaces. **Pro ($29/mo):** unlimited.

## CLI Commands

| Command | What it does |
|---------|-------------|
| `qualito init` | Initialize Qualito in your project |
| `qualito import` | Import Claude Code session logs |
| `qualito score` | Calculate DQI scores for imported runs |
| `qualito status` | Show current DQI status |
| `qualito costs` | Cost breakdown and waste analysis |
| `qualito incidents` | Active quality incidents |
| `qualito slo` | SLO compliance check |
| `qualito dashboard` | Launch local web dashboard |
| `qualito login` | Authenticate with qualito.ai |
| `qualito sync` | Push local data to cloud |
| `qualito logout` | Remove cloud credentials |

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

7 tools available: `dqi_score`, `dqi_cost`, `dqi_patterns`, `dqi_warnings`, `dqi_templates`, `dqi_incidents`, `dqi_slo`.

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
