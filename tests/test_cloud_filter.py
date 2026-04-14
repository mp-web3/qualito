"""Tests for the metadata-only privacy filter in qualito.cloud.

Phase 7 T3: pure client-side helper that strips a run dict down to
metadata-only fields. No integration with sync_runs() yet — Task 5 wires it in.
"""

from qualito.cloud import (
    _METADATA_ONLY_EVALUATION_KEEP,
    _METADATA_ONLY_FILE_ACTIVITY_KEEP,
    _METADATA_ONLY_RUN_KEEP,
    _METADATA_ONLY_TOOL_CALL_KEEP,
    _strip_run_to_metadata,
)


def _full_run() -> dict:
    """Build a run dict with every KEEP field and several STRIP fields."""
    return {
        # KEEP fields (categorical, numeric, IDs, timestamps)
        "id": "run-123",
        "workspace": "qualito",
        "task_type": "feature",
        "model": "claude-opus-4-6",
        "pipeline_mode": "standard",
        "status": "completed",
        "cost_usd": 0.42,
        "input_tokens": 1000,
        "output_tokens": 500,
        "cache_read_tokens": 200,
        "duration_ms": 12345,
        "started_at": "2026-04-14T10:00:00Z",
        "completed_at": "2026-04-14T10:05:00Z",
        "source": "claude_code",
        "session_type": "interactive",
        "entrypoint": "cli",
        "claude_version": "1.0.0",
        "session_name": "phase-7-t3",
        "has_subagents": False,
        "subagent_count": 0,
        "error_count": 0,
        "tool_count": 7,
        "paper_live_gap": 0.0,
        "skill_name": "reflect",
        "user_id": 42,
        # STRIP fields (text-bearing — must be removed)
        "task": "add privacy filter to cloud.py",
        "summary": "sensitive free-text summary",
        "files_changed": ["src/qualito/cloud.py", "tests/test_cloud_filter.py"],
        "prompt": "secret prompt content",
        "original_prompt": "original secret prompt",
        "researcher_summary": "researcher notes",
        "implementer_summary": "implementer notes",
        "verifier_verdict": "verdict text",
        "prompt_components": {"system": "sys", "user": "usr"},
        "branch": "feature/phase-7-t3",
        # children
        "tool_calls": [
            {
                "tool_name": "Read",
                "is_error": False,
                "phase": "main",
                "timestamp": "2026-04-14T10:01:00Z",
                "duration_ms": 100,
                "arguments_summary": "Read src/qualito/cloud.py",
                "result_summary": "file contents here",
            },
            {
                "tool_name": "Edit",
                "is_error": False,
                "phase": "main",
                "timestamp": "2026-04-14T10:02:00Z",
                "duration_ms": 200,
                "arguments_summary": "edit cloud.py",
                "result_summary": "success",
            },
            {
                "tool_name": "Bash",
                "is_error": True,
                "phase": "main",
                "timestamp": "2026-04-14T10:03:00Z",
                "duration_ms": 50,
                "arguments_summary": "uv run pytest",
                "result_summary": "failed output with sensitive paths",
            },
        ],
        "file_activity": [
            {
                "action": "write",
                "timestamp": "2026-04-14T10:02:30Z",
                "file_path": "/Users/mattiapapa/qualito/src/qualito/cloud.py",
            },
            {
                "action": "write",
                "timestamp": "2026-04-14T10:02:35Z",
                "file_path": "/Users/mattiapapa/qualito/tests/test_cloud_filter.py",
            },
        ],
        "evaluations": [
            {
                "eval_type": "auto",
                "score": 0.85,
                "categories": {"completeness": 0.9},
                "created_at": "2026-04-14T10:05:30Z",
                "checks": ["check1 detail text", "check2 detail text"],
                "notes": "evaluator free-text notes",
            },
        ],
        "artifacts": [
            {"kind": "diff", "content": "- old\n+ new"},
            {"kind": "log", "content": "trace data"},
        ],
    }


_STRIP_FIELDS = {
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
}


def test_strip_preserves_all_keep_fields() -> None:
    run = _full_run()
    out = _strip_run_to_metadata(run)
    for key in _METADATA_ONLY_RUN_KEEP:
        assert key in out, f"expected KEEP field {key!r} in output"
        assert out[key] == run[key]


def test_strip_removes_all_strip_fields() -> None:
    run = _full_run()
    out = _strip_run_to_metadata(run)
    for key in _STRIP_FIELDS:
        assert key not in out, f"STRIP field {key!r} leaked into output"


def test_strip_filters_tool_calls() -> None:
    run = _full_run()
    out = _strip_run_to_metadata(run)
    assert len(out["tool_calls"]) == 3
    for tc in out["tool_calls"]:
        assert "arguments_summary" not in tc
        assert "result_summary" not in tc
        assert set(tc.keys()) <= _METADATA_ONLY_TOOL_CALL_KEEP
        assert "tool_name" in tc
    assert out["tool_calls"][0]["tool_name"] == "Read"
    assert out["tool_calls"][2]["is_error"] is True


def test_strip_filters_file_activity() -> None:
    run = _full_run()
    out = _strip_run_to_metadata(run)
    assert len(out["file_activity"]) == 2
    for fa in out["file_activity"]:
        assert "file_path" not in fa
        assert set(fa.keys()) <= _METADATA_ONLY_FILE_ACTIVITY_KEEP
        assert "action" in fa
        assert "timestamp" in fa


def test_strip_filters_evaluations() -> None:
    run = _full_run()
    out = _strip_run_to_metadata(run)
    assert len(out["evaluations"]) == 1
    ev = out["evaluations"][0]
    assert "checks" not in ev
    assert "notes" not in ev
    assert set(ev.keys()) <= _METADATA_ONLY_EVALUATION_KEEP
    assert ev["score"] == 0.85
    assert ev["eval_type"] == "auto"


def test_strip_drops_artifacts() -> None:
    run = _full_run()
    assert len(run["artifacts"]) == 2
    out = _strip_run_to_metadata(run)
    assert out["artifacts"] == []


def test_strip_idempotent() -> None:
    run = _full_run()
    once = _strip_run_to_metadata(run)
    twice = _strip_run_to_metadata(once)
    assert once == twice


def test_strip_handles_missing_children() -> None:
    run = {
        "id": "run-minimal",
        "workspace": "qualito",
        "status": "completed",
    }
    out = _strip_run_to_metadata(run)
    assert out["id"] == "run-minimal"
    assert out["workspace"] == "qualito"
    assert out["status"] == "completed"
    assert out["tool_calls"] == []
    assert out["file_activity"] == []
    assert out["evaluations"] == []
    assert out["artifacts"] == []
