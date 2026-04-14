"""Secret scanner — pure regex-based detection of secrets in text.

Standalone module: no imports from other qualito modules and no third-party
dependencies. Consumers (sync path, offline audit) walk runs through
scan_run() before shipping content anywhere. Pattern coverage is deliberately
curated — conservative, tuned to avoid flagging UUIDs, SHA256 hashes, and
placeholder literals like ``your_api_key_here``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

__all__ = ["Finding", "PATTERNS", "scan_text", "scan_run"]


@dataclass(frozen=True)
class Finding:
    """A single secret match inside a scanned field.

    Attributes:
        pattern_name: Named pattern from ``PATTERNS`` (e.g. ``aws_access_key``).
        field_path: Dotted path describing where the secret was found
            (e.g. ``runs.task``, ``tool_calls[3].result_summary``).
        match_preview: First 8 characters of the matched value + ``...``.
            Never contains the full secret.
    """

    pattern_name: str
    field_path: str
    match_preview: str


# Pattern registry. Non-raw strings are used where a pattern mixes single and
# double quotes in a character class, so escaping stays unambiguous.
PATTERNS: dict[str, re.Pattern] = {
    # --- Cloud provider keys ---
    "aws_access_key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "aws_secret_in_config": re.compile(
        "(?i)aws_secret_access_key\\s*=\\s*['\"]?[A-Za-z0-9/+]{40}['\"]?"
    ),
    "gcp_api_key": re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"),
    "azure_storage": re.compile(
        r"DefaultEndpointsProtocol=https;AccountName=[^;]+;AccountKey=[A-Za-z0-9+/=]{40,}"
    ),
    # --- LLM API keys ---
    "anthropic_key": re.compile(r"\bsk-ant-api[0-9]{2}-[A-Za-z0-9_-]{80,}\b"),
    "openai_classic": re.compile(r"\bsk-[A-Za-z0-9]{48}\b"),
    "openai_project": re.compile(r"\bsk-proj-[A-Za-z0-9_-]{100,}\b"),
    "huggingface_token": re.compile(r"\bhf_[A-Za-z0-9]{34}\b"),
    # --- Developer tokens ---
    "github_pat_classic": re.compile(r"\bghp_[A-Za-z0-9]{36}\b"),
    "github_pat_fine": re.compile(r"\bgithub_pat_[A-Za-z0-9_]{82}\b"),
    "github_oauth": re.compile(r"\bgho_[A-Za-z0-9]{36}\b"),
    "gitlab_pat": re.compile(r"\bglpat-[A-Za-z0-9_-]{20}\b"),
    "slack_token": re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}\b"),
    # --- Payment APIs ---
    "stripe_live": re.compile(r"\bsk_live_[A-Za-z0-9]{24,}\b"),
    "stripe_test": re.compile(r"\bsk_test_[A-Za-z0-9]{24,}\b"),
    "stripe_restricted": re.compile(r"\brk_(?:live|test)_[A-Za-z0-9]{24,}\b"),
    # --- Generic high-entropy / structured secrets ---
    "jwt": re.compile(r"\beyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b"),
    "pem_private_key": re.compile(
        r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----"
    ),
    "pkcs8_private_key": re.compile(
        r"-----BEGIN (?:ENCRYPTED )?PRIVATE KEY-----"
    ),
    # --- HTTP headers ---
    "bearer_auth": re.compile(
        r"(?i)authorization:\s*bearer\s+[A-Za-z0-9_.~+/=-]{20,}"
    ),
    "api_key_header": re.compile(
        r"(?i)x-api-key\s*:\s*[A-Za-z0-9_.~+/=-]{16,}"
    ),
    # --- Database connection strings with inline credentials ---
    "postgres_url": re.compile(r"postgres(?:ql)?://[^\s:@/]+:[^\s@/]+@[^\s/]+"),
    "mysql_url": re.compile(r"mysql://[^\s:@/]+:[^\s@/]+@[^\s/]+"),
    "mongodb_url": re.compile(r"mongodb(?:\+srv)?://[^\s:@/]+:[^\s@/]+@[^\s/]+"),
    "redis_url": re.compile(r"rediss?://(?::[^\s@/]+)?@[^\s/]+"),
    # --- Environment-style / config-style assignments ---
    "env_key_assignment": re.compile(
        "(?i)(?:api_?key|api_?secret|access_?token|auth_?token|secret_?key|private_?key|password|passwd|pwd)"
        "\\s*[:=]\\s*['\"]?[A-Za-z0-9+/=_.~-]{16,}['\"]?"
    ),
}


_RUNS_FIELDS: tuple[str, ...] = (
    "task",
    "summary",
    "files_changed",
    "prompt",
    "original_prompt",
    "researcher_summary",
    "implementer_summary",
    "verifier_verdict",
    "prompt_components",
    "branch",
)
_TOOL_CALL_FIELDS: tuple[str, ...] = ("arguments_summary", "result_summary")
_FILE_ACTIVITY_FIELDS: tuple[str, ...] = ("file_path",)
_EVALUATION_FIELDS: tuple[str, ...] = ("checks", "notes")
_ARTIFACT_FIELDS: tuple[str, ...] = ("title", "content", "file_path", "metadata")


def _preview(matched: str) -> str:
    return matched[:8] + "..."


def scan_text(text: str | None, field_path: str = "") -> list[Finding]:
    """Scan a single string against every registered pattern.

    Args:
        text: String to scan. ``None`` and empty strings short-circuit to
            an empty list.
        field_path: Caller-supplied label passed through verbatim into each
            Finding so consumers can point at the offending location.

    Returns:
        List of Finding — empty if no match or ``text`` is missing.
    """
    if not text:
        return []
    findings: list[Finding] = []
    for name, pattern in PATTERNS.items():
        for match in pattern.finditer(text):
            findings.append(
                Finding(
                    pattern_name=name,
                    field_path=field_path,
                    match_preview=_preview(match.group(0)),
                )
            )
    return findings


def _scan_child(
    items: object,
    collection_name: str,
    fields: tuple[str, ...],
    findings: list[Finding],
) -> None:
    if not isinstance(items, (list, tuple)):
        return
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        for field in fields:
            value = item.get(field)
            if value is None:
                continue
            if not isinstance(value, str):
                value = str(value)
            findings.extend(
                scan_text(value, f"{collection_name}[{index}].{field}")
            )


def scan_run(run: dict) -> list[Finding]:
    """Scan a run dict for secrets across every text-bearing field.

    Walks the top-level run fields, then the ``tool_calls``, ``file_activity``,
    ``evaluations`` and ``artifacts`` child lists when present. Each Finding's
    ``field_path`` pinpoints the offending location using dotted notation
    (``runs.task`` or ``tool_calls[3].result_summary``).

    Args:
        run: Run dict — typically a SQLAlchemy row mapping merged with its
            child tables, or a JSON dict from the importer. Missing keys and
            non-list / non-dict children are tolerated and skipped.

    Returns:
        List of Finding — empty if no secrets were detected.
    """
    if not isinstance(run, dict):
        return []

    findings: list[Finding] = []

    for field in _RUNS_FIELDS:
        value = run.get(field)
        if value is None:
            continue
        if not isinstance(value, str):
            value = str(value)
        findings.extend(scan_text(value, f"runs.{field}"))

    _scan_child(run.get("tool_calls"), "tool_calls", _TOOL_CALL_FIELDS, findings)
    _scan_child(
        run.get("file_activity"), "file_activity", _FILE_ACTIVITY_FIELDS, findings
    )
    _scan_child(run.get("evaluations"), "evaluations", _EVALUATION_FIELDS, findings)
    _scan_child(run.get("artifacts"), "artifacts", _ARTIFACT_FIELDS, findings)

    return findings
