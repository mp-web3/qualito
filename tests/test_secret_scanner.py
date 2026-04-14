"""Tests for qualito.core.secret_scanner — pure regex secret detection."""

from __future__ import annotations

import pytest

from qualito.core.secret_scanner import PATTERNS, Finding, scan_run, scan_text

AWS_EXAMPLE = "AKIAIOSFODNN7EXAMPLE"


# ----------------------------- positive cases -----------------------------


@pytest.mark.parametrize(
    "name, text",
    [
        # Cloud provider keys
        ("aws_access_key", AWS_EXAMPLE),
        (
            "aws_secret_in_config",
            'aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"',
        ),
        ("gcp_api_key", "AIzaSyD-9tSrke72PouQMnMX-a7eZSW0jkFMBWY"),
        (
            "azure_storage",
            "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey="
            "dGhpc2lzYXRlc3RrZXlhYmNkZWZnaGlqa2xtbm9wcXIxMjM0NTY3ODkw==",
        ),
        # LLM API keys
        ("anthropic_key", "sk-ant-api01-" + "A" * 95),
        ("openai_classic", "sk-1234567890abcdefghij1234567890abcdefghij12345678"),
        ("openai_project", "sk-proj-" + "A" * 110),
        ("huggingface_token", "hf_" + "A" * 34),
        # Developer tokens
        ("github_pat_classic", "ghp_" + "a" * 36),
        ("github_pat_fine", "github_pat_" + "A" * 82),
        ("github_oauth", "gho_" + "b" * 36),
        ("gitlab_pat", "glpat-" + "A" * 20),
        ("slack_token", "xoxb-1234567890-abcdef"),
        # Payment APIs
        ("stripe_live", "sk_live_" + "a" * 24),
        ("stripe_test", "sk_test_" + "a" * 24),
        ("stripe_restricted", "rk_live_" + "a" * 24),
        # Generic
        ("jwt", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.abc123def456"),
        ("pem_private_key", "-----BEGIN RSA PRIVATE KEY-----"),
        ("pkcs8_private_key", "-----BEGIN ENCRYPTED PRIVATE KEY-----"),
        # HTTP headers
        ("bearer_auth", "Authorization: Bearer abcdefghijklmnop12345678901234"),
        ("api_key_header", "x-api-key: abcdefghij12345678"),
        # DB connection strings
        ("postgres_url", "postgres://user:pw123@host:5432/db"),
        ("mysql_url", "mysql://root:hunter2@db.internal:3306/app"),
        ("mongodb_url", "mongodb+srv://admin:supersecret@cluster.mongodb.net"),
        ("redis_url", "redis://:password123@redis.internal:6379"),
        # Environment-style
        ("env_key_assignment", 'password="hunter2hunter2hunter2"'),
    ],
)
def test_pattern_positive(name: str, text: str) -> None:
    findings = scan_text(text, field_path="test")
    names = {f.pattern_name for f in findings}
    assert name in names, f"expected {name!r} to match {text!r}, got {names!r}"
    for f in findings:
        assert f.field_path == "test"
        assert f.match_preview.endswith("...")


# ----------------------------- negative cases -----------------------------


@pytest.mark.parametrize(
    "text",
    [
        "550e8400-e29b-41d4-a716-446655440000",  # UUID
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",  # SHA256
        "foo",
        "bar",
        "test123",
        "your_api_key_here",
        "INSERT_KEY",
        "<REDACTED>",
        "",
    ],
)
def test_negative_no_findings(text: str) -> None:
    assert scan_text(text, "test") == []


def test_none_input_returns_empty_list() -> None:
    assert scan_text(None) == []
    assert scan_text(None, "runs.task") == []


def test_default_field_path_is_empty_string() -> None:
    findings = scan_text(AWS_EXAMPLE)
    assert findings
    assert findings[0].field_path == ""


def test_multiple_secrets_in_one_text() -> None:
    blob = f"{AWS_EXAMPLE} and also ghp_{'a' * 36}"
    findings = scan_text(blob, "runs.summary")
    names = {f.pattern_name for f in findings}
    assert "aws_access_key" in names
    assert "github_pat_classic" in names
    assert all(f.field_path == "runs.summary" for f in findings)


# ----------------------------- preview safety -----------------------------


def test_preview_head_at_most_8_chars() -> None:
    findings = scan_text("ghp_" + "a" * 36, "test")
    assert findings
    for f in findings:
        assert f.match_preview.endswith("...")
        head = f.match_preview[:-3]
        assert len(head) <= 8


def test_preview_never_reveals_full_secret() -> None:
    secret = "ghp_" + "a" * 36  # 40 chars total
    findings = scan_text(secret, "test")
    assert findings
    for f in findings:
        assert secret not in f.match_preview
        assert len(f.match_preview) <= 11


# ----------------------------- scan_run structural -----------------------------


def test_scan_run_finds_secret_in_task() -> None:
    run = {"task": f"run this: {AWS_EXAMPLE}"}
    findings = scan_run(run)
    assert any(f.field_path == "runs.task" for f in findings)
    assert any(f.pattern_name == "aws_access_key" for f in findings)


def test_scan_run_finds_secret_in_summary() -> None:
    run = {"summary": f"completed {AWS_EXAMPLE}"}
    findings = scan_run(run)
    assert any(f.field_path == "runs.summary" for f in findings)


def test_scan_run_covers_all_top_level_run_fields() -> None:
    run = {
        "task": AWS_EXAMPLE,
        "summary": AWS_EXAMPLE,
        "files_changed": AWS_EXAMPLE,
        "prompt": AWS_EXAMPLE,
        "original_prompt": AWS_EXAMPLE,
        "researcher_summary": AWS_EXAMPLE,
        "implementer_summary": AWS_EXAMPLE,
        "verifier_verdict": AWS_EXAMPLE,
        "prompt_components": AWS_EXAMPLE,
        "branch": AWS_EXAMPLE,
    }
    findings = scan_run(run)
    paths = {f.field_path for f in findings}
    for field in run:
        assert f"runs.{field}" in paths


def test_scan_run_tool_calls_indexing() -> None:
    run = {
        "tool_calls": [
            {"arguments_summary": "clean", "result_summary": "clean"},
            {
                "arguments_summary": None,
                "result_summary": f"output: {AWS_EXAMPLE}",
            },
        ],
    }
    findings = scan_run(run)
    assert any(
        f.field_path == "tool_calls[1].result_summary" for f in findings
    )
    assert not any(
        f.field_path.startswith("tool_calls[0].") for f in findings
    )


def test_scan_run_file_activity_path() -> None:
    run = {"file_activity": [{"file_path": f"/tmp/{AWS_EXAMPLE}"}]}
    findings = scan_run(run)
    assert any(f.field_path == "file_activity[0].file_path" for f in findings)


def test_scan_run_evaluations_checks_and_notes() -> None:
    run = {
        "evaluations": [
            {"checks": f"token={AWS_EXAMPLE}", "notes": None},
            {"checks": None, "notes": f"also {AWS_EXAMPLE}"},
        ]
    }
    findings = scan_run(run)
    paths = {f.field_path for f in findings}
    assert "evaluations[0].checks" in paths
    assert "evaluations[1].notes" in paths


def test_scan_run_artifacts_content() -> None:
    run = {
        "artifacts": [
            {
                "title": "deploy.sh",
                "content": f"export KEY={AWS_EXAMPLE}",
                "file_path": "deploy.sh",
                "metadata": None,
            }
        ]
    }
    findings = scan_run(run)
    assert any(f.field_path == "artifacts[0].content" for f in findings)


def test_scan_run_clean_returns_empty() -> None:
    run = {
        "task": "refactor a module",
        "summary": "refactor complete",
        "tool_calls": [{"arguments_summary": "ls -la", "result_summary": "ok"}],
        "file_activity": [{"file_path": "src/foo.py"}],
        "evaluations": [{"checks": "all good", "notes": "nothing suspicious"}],
        "artifacts": [
            {
                "title": "note",
                "content": "hi",
                "file_path": None,
                "metadata": None,
            }
        ],
    }
    assert scan_run(run) == []


def test_scan_run_missing_children_does_not_crash() -> None:
    run = {"task": "no secrets here"}
    assert scan_run(run) == []


def test_scan_run_handles_non_dict_children() -> None:
    run = {"tool_calls": ["not a dict", 42, None]}
    assert scan_run(run) == []


def test_scan_run_handles_non_list_children() -> None:
    run = {
        "task": "clean",
        "tool_calls": "not-a-list",
        "file_activity": None,
        "evaluations": 42,
        "artifacts": {},
    }
    assert scan_run(run) == []


def test_scan_run_non_dict_input() -> None:
    assert scan_run(None) == []  # type: ignore[arg-type]
    assert scan_run("AKIAIOSFODNN7EXAMPLE") == []  # type: ignore[arg-type]
    assert scan_run([]) == []  # type: ignore[arg-type]


def test_scan_run_coerces_non_string_field_values() -> None:
    run = {"files_changed": [f"deploy/{AWS_EXAMPLE}.sh"]}  # list, not string
    findings = scan_run(run)
    assert any(f.field_path == "runs.files_changed" for f in findings)


# ----------------------------- registry shape -----------------------------


def test_pattern_registry_has_all_required_patterns() -> None:
    expected = {
        "aws_access_key",
        "aws_secret_in_config",
        "gcp_api_key",
        "azure_storage",
        "anthropic_key",
        "openai_classic",
        "openai_project",
        "huggingface_token",
        "github_pat_classic",
        "github_pat_fine",
        "github_oauth",
        "gitlab_pat",
        "slack_token",
        "stripe_live",
        "stripe_test",
        "stripe_restricted",
        "jwt",
        "pem_private_key",
        "pkcs8_private_key",
        "bearer_auth",
        "api_key_header",
        "postgres_url",
        "mysql_url",
        "mongodb_url",
        "redis_url",
        "env_key_assignment",
    }
    assert expected.issubset(PATTERNS.keys())
    assert all(hasattr(p, "pattern") for p in PATTERNS.values())


def test_finding_is_frozen() -> None:
    f = Finding(pattern_name="x", field_path="y", match_preview="zzzzzzzz...")
    with pytest.raises(Exception):
        f.pattern_name = "mutated"  # type: ignore[misc]
