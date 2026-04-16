"""Detect repeated task patterns in delegation history.

Groups runs by normalized task text, classifies patterns,
and recommends automation (script, skill, or review).
"""

import json
import re
from datetime import datetime, timedelta
from pathlib import Path

# Default known skills and aliases — can be overridden via load_skill_config()
_KNOWN_SKILLS: set[str] = set()
_SKILL_ALIASES: dict[str, list[str]] = {}


def load_skill_config(config_path: Path | None = None):
    """Load skill configuration from a JSON file.

    Expected format:
        {"known_skills": ["skill1", ...], "aliases": {"skill1": ["alias1", ...]}}

    Falls back to empty defaults if file doesn't exist.

    Args:
        config_path: Path to skills.json. If None, looks for .qualito/skills.json in cwd.
    """
    global _KNOWN_SKILLS, _SKILL_ALIASES

    if config_path is None:
        config_path = Path.cwd() / ".dqi" / "skills.json"

    if not config_path.exists():
        _KNOWN_SKILLS = set()
        _SKILL_ALIASES = {}
        return

    with open(config_path) as f:
        data = json.load(f)

    _KNOWN_SKILLS = set(data.get("known_skills", []))
    _SKILL_ALIASES = data.get("aliases", {})


def normalize_task(task: str) -> str:
    """Normalize task text into a groupable pattern key.

    Steps:
    1. Take first line only
    2. Replace PR numbers (#NNN) with #N
    3. Replace long numeric IDs (6+ digits) with ID
    4. Lowercase, take first 8 words
    """
    first_line = task.split("\n")[0].strip()
    normalized = re.sub(r"#\d+", "#N", first_line)
    normalized = re.sub(r"\b\d{6,}\b", "ID", normalized)
    words = normalized.lower().split()[:8]
    return " ".join(words)


def classify_pattern(count: int, avg_dqi: float, has_matching_skill: bool) -> str:
    """Classify a pattern as script, review, or skill."""
    if has_matching_skill:
        return "review"
    if count >= 5 and avg_dqi > 0.85:
        return "script"
    return "skill"


def find_matching_skill(pattern: str) -> str | None:
    """Check if a known skill matches this pattern."""
    # Check aliases first (more specific)
    for skill, aliases in _SKILL_ALIASES.items():
        if any(alias in pattern for alias in aliases):
            return skill
    # Fallback: match skill name (with hyphens as spaces)
    for skill in _KNOWN_SKILLS:
        skill_words = skill.replace("-", " ")
        if skill_words in pattern:
            return skill
    return None


def recommend(classification: str, matching_skill: str | None, pattern: str) -> str:
    """Generate a recommendation string."""
    if classification == "review" and matching_skill:
        return f"/{matching_skill} exists, verify usage"
    if classification == "script":
        return "Replace with script"
    return "Create new skill"


def detect_patterns(
    min_count: int = 3,
    since_days: int = 30,
    workspace: str | None = None,
    conn=None,
) -> list[dict]:
    """Query DB and return grouped, classified patterns.

    Args:
        min_count: Minimum occurrences to include in results.
        since_days: Number of days to look back.
        workspace: Optional workspace filter.
        conn: Optional DB connection. If None, opens and closes its own.
    """
    from sqlalchemy import and_, outerjoin, select

    from qualito.core.db import evaluations_table, get_sa_connection, runs_table

    owns_conn = conn is None
    if owns_conn:
        conn = get_sa_connection()

    since_date = (datetime.now() - timedelta(days=since_days)).strftime("%Y-%m-%d")

    r = runs_table
    e = evaluations_table

    conditions = [
        r.c.status == "completed",
        r.c.started_at >= since_date,
    ]
    if workspace:
        conditions.append(r.c.workspace == workspace)

    rows = conn.execute(
        select(
            r.c.id, r.c.task, r.c.cost_usd, r.c.task_type,
            r.c.workspace, r.c.skill_name,
            e.c.score.label("dqi_score"),
        )
        .select_from(
            r.outerjoin(e, and_(e.c.run_id == r.c.id, e.c.eval_type == "dqi"))
        )
        .where(and_(*conditions))
        .order_by(r.c.started_at.desc())
    ).mappings().fetchall()

    if owns_conn:
        conn.close()

    # Group by normalized pattern
    groups: dict[str, dict] = {}
    for row in rows:
        task = row["task"] or ""
        pattern = normalize_task(task)
        if not pattern:
            continue

        if pattern not in groups:
            groups[pattern] = {
                "pattern": pattern,
                "count": 0,
                "total_cost": 0.0,
                "dqi_scores": [],
                "task_types": set(),
                "workspaces": set(),
                "skill_names": set(),
            }

        g = groups[pattern]
        g["count"] += 1
        g["total_cost"] += row["cost_usd"] or 0.0
        if row["dqi_score"] is not None:
            g["dqi_scores"].append(row["dqi_score"])
        if row["task_type"]:
            g["task_types"].add(row["task_type"])
        if row["workspace"]:
            g["workspaces"].add(row["workspace"])
        if row["skill_name"]:
            g["skill_names"].add(row["skill_name"])

    # Filter by min count, compute averages, classify
    results = []
    for g in groups.values():
        if g["count"] < min_count:
            continue

        avg_dqi = sum(g["dqi_scores"]) / len(g["dqi_scores"]) if g["dqi_scores"] else 0.0
        matching_skill = find_matching_skill(g["pattern"])
        classification = classify_pattern(g["count"], avg_dqi, matching_skill is not None)
        rec = recommend(classification, matching_skill, g["pattern"])

        results.append({
            "pattern": g["pattern"],
            "count": g["count"],
            "total_cost": round(g["total_cost"], 2),
            "avg_dqi": round(avg_dqi, 3),
            "type": classification,
            "recommendation": rec,
            "task_types": sorted(g["task_types"]),
            "workspaces": sorted(g["workspaces"]),
            "skill_names": sorted(g["skill_names"]),
        })

    # Sort by total cost descending
    results.sort(key=lambda r: r["total_cost"], reverse=True)
    return results


def print_report(results: list[dict], since_days: int, min_count: int):
    """Print a formatted table report."""
    print(f"\nREPEATED TASK PATTERNS (last {since_days} days, min {min_count} occurrences)\n")

    if not results:
        print("No repeated patterns found.")
        return

    # Column widths
    pat_w = max(len(r["pattern"]) for r in results)
    pat_w = max(pat_w, 7)  # "Pattern" header
    pat_w = min(pat_w, 45)  # cap width

    header = (
        f"{'Pattern':<{pat_w}}  {'Count':>5}  {'Total$':>8}  {'Avg DQI':>7}  "
        f"{'Type':<8}  Recommendation"
    )
    print(header)
    print("-" * len(header))

    for r in results:
        pattern_display = r["pattern"][:pat_w]
        print(
            f"{pattern_display:<{pat_w}}  {r['count']:>5}  "
            f"${r['total_cost']:>7.2f}  {r['avg_dqi']:>7.3f}  "
            f"{r['type']:<8}  {r['recommendation']}"
        )

    total_cost = sum(r["total_cost"] for r in results)
    total_runs = sum(r["count"] for r in results)
    print(f"\n{len(results)} patterns, {total_runs} total runs, ${total_cost:.2f} total cost")
