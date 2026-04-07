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

## Deployment

The DQI dashboard deploys as a split-stack: **Railway** (API) + **Vercel** (frontend).

### API (Railway)

1. Connect the repo to Railway
2. Set environment variables:
   - `DQI_JWT_SECRET` — stable random secret for JWT signing
   - `DQI_DIR` — data directory path (e.g. `/data/.dqi`)
   - `CORS_ORIGINS` — comma-separated allowed origins (e.g. `https://dqi.dev,https://www.dqi.dev`)
3. Railway will auto-detect the `Dockerfile` and `railway.json`

### Frontend (Vercel)

1. Set the root directory to `src/dqi/dashboard/frontend`
2. Set environment variable:
   - `VITE_API_URL` — Railway API URL (e.g. `https://api.dqi.dev`)
3. The `vercel.json` rewrites `/api/*` requests to the Railway backend

### Local Development

```bash
# Copy and fill in env vars
cp env.example .env

# API (from repo root)
uv run uvicorn dqi.dashboard.app:create_app --factory --port 8090

# Frontend (from src/dqi/dashboard/frontend/)
npm install && npm run dev
```
