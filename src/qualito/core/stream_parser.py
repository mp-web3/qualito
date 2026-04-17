"""Parse claude -p stream-json output into structured data.

Extracts tool calls, file activity, token usage, cost, and final result
from stream.jsonl files produced by `claude -p --output-format stream-json`.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ToolCall:
    tool_name: str
    arguments_summary: str = ""
    result_summary: str = ""
    is_error: bool = False
    full_arguments: dict | None = None  # preserved for important tools (si_reason)
    phase: str = "single"
    timestamp: str = ""
    duration_ms: int | None = None
    tool_use_id: str = ""
    full_result: str | None = None  # preserved for important tools (si_reason)


@dataclass
class FileActivity:
    file_path: str
    action: str  # read, write, edit
    timestamp: str = ""


@dataclass
class ParsedStream:
    tool_calls: list[ToolCall] = field(default_factory=list)
    file_activity: list[FileActivity] = field(default_factory=list)
    result: dict | None = None
    usage: dict = field(default_factory=dict)
    cost_usd: float | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0


_ERROR_MAX_LEN = 10_000  # preserve stack traces, multi-line tool_use_error, long bash output


def _summarize(obj: any, max_len: int = 200) -> str:
    """Summarize an object to max_len chars."""
    if obj is None:
        return ""
    s = str(obj) if not isinstance(obj, str) else obj
    return s[:max_len] if len(s) > max_len else s


def _extract_file_path(tool_name: str, args: dict) -> tuple[str, str] | None:
    """Extract file path and action from a tool call."""
    if tool_name == "Read" and "file_path" in args:
        return args["file_path"], "read"
    if tool_name == "Write" and "file_path" in args:
        return args["file_path"], "write"
    if tool_name == "Edit" and "file_path" in args:
        return args["file_path"], "edit"
    return None


_PRESERVE_FULL_ARGS = {
    "si_reason", "si_hypothesize",
    "mcp__second-intelligence__si_reason",
    "mcp__second-intelligence__si_hypothesize",
}


def _add_tool_call(parsed: ParsedStream, pending: dict, name: str, args: dict,
                   tool_use_id: str, phase: str, event: dict):
    """Add a tool call to parsed results."""
    tc = ToolCall(
        tool_name=name,
        arguments_summary=_summarize(args),
        full_arguments=args if name in _PRESERVE_FULL_ARGS else None,
        phase=phase,
        timestamp=event.get("timestamp", ""),
        tool_use_id=tool_use_id,
    )
    parsed.tool_calls.append(tc)
    if tool_use_id:
        pending[tool_use_id] = tc

    fa = _extract_file_path(name, args)
    if fa:
        parsed.file_activity.append(FileActivity(
            file_path=fa[0],
            action=fa[1],
            timestamp=event.get("timestamp", ""),
        ))


def parse_stream(stream_path: Path, phase: str = "single") -> ParsedStream:
    """Parse a stream.jsonl file into structured data."""
    parsed = ParsedStream()
    if not stream_path.exists():
        return parsed

    raw = stream_path.read_text().strip()
    if not raw:
        return parsed

    # Track tool_use blocks to match with tool_result
    pending_tools: dict[str, ToolCall] = {}  # tool_use_id -> ToolCall

    for line in raw.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue

        etype = event.get("type", "")

        # Handle tool_use — can be top-level or nested in assistant message content
        if etype == "tool_use":
            tool = event.get("tool", {})
            name = tool.get("name", "unknown")
            args = tool.get("input", {})
            tool_use_id = tool.get("id", "")
            _add_tool_call(parsed, pending_tools, name, args, tool_use_id, phase, event)

        elif etype == "assistant":
            # Tool calls are nested in assistant message content blocks
            msg = event.get("message", {})
            content = msg.get("content", [])
            for block in content:
                if isinstance(block, dict) and block.get("type") == "tool_use":
                    name = block.get("name", "unknown")
                    args = block.get("input", {})
                    tool_use_id = block.get("id", "")
                    _add_tool_call(parsed, pending_tools, name, args, tool_use_id, phase, event)

        elif etype == "tool_result":
            # Top-level tool_result (older format)
            tool_use_id = event.get("tool_use_id", "")
            if tool_use_id and tool_use_id in pending_tools:
                tc = pending_tools[tool_use_id]
                content = event.get("content", "")
                is_error = bool(event.get("is_error", False))
                tc.result_summary = _summarize(
                    content, max_len=_ERROR_MAX_LEN if is_error else 200
                )
                tc.is_error = is_error
                if tc.tool_name in _PRESERVE_FULL_ARGS:
                    tc.full_result = content if isinstance(content, str) else str(content)

        elif etype == "user":
            # Tool results are nested in user message content blocks (actual claude-code format)
            msg = event.get("message", {})
            content_blocks = msg.get("content", [])
            for block in content_blocks:
                if not isinstance(block, dict) or block.get("type") != "tool_result":
                    continue
                tool_use_id = block.get("tool_use_id", "")
                if tool_use_id and tool_use_id in pending_tools:
                    tc = pending_tools[tool_use_id]
                    content = block.get("content", "")
                    is_error = bool(block.get("is_error", False))
                    tc.result_summary = _summarize(
                        content, max_len=_ERROR_MAX_LEN if is_error else 200
                    )
                    tc.is_error = is_error
                    if tc.tool_name in _PRESERVE_FULL_ARGS:
                        tc.full_result = content if isinstance(content, str) else str(content)

        elif etype == "result":
            # Final result event
            result_data = event.get("structured_output") or event.get("result", "")
            if isinstance(result_data, str):
                try:
                    result_data = json.loads(result_data)
                except json.JSONDecodeError:
                    result_data = {"status": "completed", "summary": result_data}
            if not result_data:
                result_data = {"status": "completed", "summary": "(no output)"}

            parsed.result = result_data
            parsed.usage = event.get("usage", {})
            parsed.cost_usd = event.get("total_cost_usd")
            parsed.input_tokens = parsed.usage.get("input_tokens", 0)
            parsed.output_tokens = parsed.usage.get("output_tokens", 0)
            parsed.cache_read_tokens = parsed.usage.get("cache_read_input_tokens", 0)

    return parsed


def get_recent_activity(stream_path: Path, n: int = 10) -> list[str]:
    """Extract recent activity lines from stream-json for progress display."""
    if not stream_path.exists():
        return []
    raw = stream_path.read_text().strip()
    if not raw:
        return []

    lines = raw.split("\n")
    activity = []
    for line in reversed(lines):
        if len(activity) >= n:
            break
        try:
            event = json.loads(line)
            etype = event.get("type", "")
            subtype = event.get("subtype", "")
            if etype == "tool_use":
                tool_name = event.get("tool", {}).get("name", "unknown")
                activity.append(f"  [tool] {tool_name}")
            elif etype == "assistant" and subtype != "init":
                msg = event.get("message", {})
                content = msg.get("content", [])
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        text = block["text"][:120].replace("\n", " ")
                        activity.append(f"  [text] {text}")
                        break
        except json.JSONDecodeError:
            continue
    activity.reverse()
    return activity
