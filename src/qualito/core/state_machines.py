"""Centralized state machine definitions for Qualito incidents.

Single source of truth for status transitions, labels, icons, and display metadata.
Backend (api.py) and frontend should derive from these definitions.

Qualito's incident machine is observer-only — users fix things in their own
environment and the platform records + watches. DF-style orchestrated experiments
(triaging, experimenting) are out of scope because Qualito has no lever to run
anything on the user's machine.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TransitionDef:
    """A single allowed status transition."""

    from_status: str
    to_status: str
    label: str
    icon: str  # inline SVG string (16x16, stroke-based)
    backward: bool = False
    trigger: str = "manual"  # "manual" | "auto"
    guard: Optional[str] = None  # human-readable precondition


@dataclass
class StateMachine:
    """Named collection of transitions forming a state machine."""

    name: str
    transitions: list[TransitionDef] = field(default_factory=list)
    terminal_states: set[str] = field(default_factory=set)


# ---------------------------------------------------------------------------
# SVG Icons (16x16, inline, stroke-based unless noted)
# ---------------------------------------------------------------------------

ICON_CHECK = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="2.5" stroke-linecap="round" '
    'stroke-linejoin="round"><polyline points="4,12 9,17 20,6"/></svg>'
)
ICON_X = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="2" stroke-linecap="round" '
    'stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/>'
    '<line x1="6" y1="6" x2="18" y2="18"/></svg>'
)
ICON_ROCKET = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="2" stroke-linecap="round" '
    'stroke-linejoin="round"><path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 '
    '5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/>'
    '<path d="M12 15l-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 '
    '2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/>'
    '<path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/>'
    '<path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/></svg>'
)
ICON_EYE = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="2" stroke-linecap="round" '
    'stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 '
    '8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>'
)
ICON_CHECK_CIRCLE = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="2" stroke-linecap="round" '
    'stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>'
    '<polyline points="22,4 12,14.01 9,11.01"/></svg>'
)
ICON_ARROW_LEFT = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="2" stroke-linecap="round" '
    'stroke-linejoin="round"><line x1="19" y1="12" x2="5" y2="12"/>'
    '<polyline points="11,18 5,12 11,6"/></svg>'
)


# ---------------------------------------------------------------------------
# Incident State Machine
# ---------------------------------------------------------------------------

INCIDENT_MACHINE = StateMachine(
    name="incident",
    transitions=[
        TransitionDef(
            from_status="detected",
            to_status="confirmed",
            label="Confirm Incident",
            icon=ICON_CHECK,
            trigger="manual",
        ),
        TransitionDef(
            from_status="detected",
            to_status="auto_resolved",
            label="Dismiss as Noise",
            icon=ICON_X,
            trigger="manual",
        ),
        TransitionDef(
            from_status="detected",
            to_status="auto_resolved",
            label="Auto-dismiss after clean runs",
            icon=ICON_X,
            trigger="auto",
        ),
        TransitionDef(
            from_status="confirmed",
            to_status="fix_deployed",
            label="Mark Fix Deployed",
            icon=ICON_ROCKET,
            trigger="manual",
        ),
        TransitionDef(
            from_status="confirmed",
            to_status="resolved",
            label="Resolve Manually",
            icon=ICON_CHECK_CIRCLE,
            trigger="manual",
        ),
        TransitionDef(
            from_status="fix_deployed",
            to_status="monitoring",
            label="Start Monitoring",
            icon=ICON_EYE,
            trigger="manual",
        ),
        TransitionDef(
            from_status="monitoring",
            to_status="resolved",
            label="Mark Resolved",
            icon=ICON_CHECK_CIRCLE,
            trigger="manual",
        ),
        TransitionDef(
            from_status="monitoring",
            to_status="resolved",
            label="Auto-resolve after clean runs",
            icon=ICON_CHECK_CIRCLE,
            trigger="auto",
        ),
        TransitionDef(
            from_status="monitoring",
            to_status="confirmed",
            label="Reopen",
            icon=ICON_ARROW_LEFT,
            backward=True,
            trigger="auto",
        ),
    ],
    terminal_states={"resolved", "auto_resolved"},
)


# ---------------------------------------------------------------------------
# Status metadata (display colors + labels)
# ---------------------------------------------------------------------------

STATUS_METADATA: dict[str, dict[str, str]] = {
    "detected": {"color": "#71717a", "label": "Detected"},
    "confirmed": {"color": "#3b82f6", "label": "Confirmed"},
    "fix_deployed": {"color": "#22d3ee", "label": "Fix Deployed"},
    "monitoring": {"color": "#34d399", "label": "Monitoring"},
    "resolved": {"color": "#86efac", "label": "Resolved"},
    "auto_resolved": {"color": "#a1a1aa", "label": "Auto-resolved"},
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_valid_transitions(
    machine: StateMachine,
    status: str,
    trigger_filter: Optional[str] = None,
) -> list[TransitionDef]:
    """Return allowed transitions from a given status.

    Args:
        machine: state machine to query
        status: current status
        trigger_filter: if set, return only transitions with this trigger
            ("manual" or "auto")
    """
    results = [t for t in machine.transitions if t.from_status == status]
    if trigger_filter is not None:
        results = [t for t in results if t.trigger == trigger_filter]
    return results


def validate_transition(machine: StateMachine, from_s: str, to_s: str) -> bool:
    """Check whether a transition from from_s to to_s is allowed."""
    return any(
        t.from_status == from_s and t.to_status == to_s
        for t in machine.transitions
    )


def get_manual_transitions(machine: StateMachine, status: str) -> list[TransitionDef]:
    """Convenience: get only manual transitions from a status."""
    return get_valid_transitions(machine, status, trigger_filter="manual")


def is_terminal(machine: StateMachine, status: str) -> bool:
    """Whether a status is terminal (no outgoing transitions allowed by user)."""
    return status in machine.terminal_states


def to_api_response(transitions: list[TransitionDef]) -> list[dict]:
    """Convert transition list to JSON-serializable dicts for API responses."""
    return [
        {
            "from_status": t.from_status,
            "to_status": t.to_status,
            "label": t.label,
            "icon": t.icon,
            "backward": t.backward,
            "trigger": t.trigger,
            "guard": t.guard,
        }
        for t in transitions
    ]
