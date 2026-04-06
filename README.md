# DQI — Delegation Quality Index

Measure and improve AI agent delegation quality. CLI tool + MCP server for Claude Code.

## Installation

```bash
pip install dqi
# or with MCP server support:
pip install "dqi[mcp]"
```

## MCP Server Setup

Add to your `.claude.json` or `.mcp.json`:

```json
{
  "dqi": {
    "command": "uvx",
    "args": ["dqi-mcp"]
  }
}
```

### Available Tools

| Tool | Description |
|------|-------------|
| `dqi_score` | DQI score summary: average, trend, component breakdown, tier distribution |
| `dqi_cost` | Cost analysis: total spend, average per run, daily trend, waste estimate |
| `dqi_patterns` | Repeated task pattern detection with classification and recommendations |
| `dqi_warnings` | Underperforming workspace/task_type combos with actionable suggestions |
| `dqi_templates` | Task type inference and delegation template recommendations |
| `dqi_incidents` | Quality incidents: regressions, anomalies, severity tracking |
| `dqi_slo` | SLO compliance: quality, availability, and cost targets |
