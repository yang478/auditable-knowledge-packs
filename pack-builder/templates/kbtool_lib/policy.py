from __future__ import annotations

from dataclasses import dataclass
from typing import Any


ALLOWED_ACTIONS = [
    "search_fields",
    "bridge_alias",
    "expand_local",
    "follow_edges",
    "focus_subtree",
    "cross_doc_shift",
    "prune",
    "stop",
]


@dataclass(frozen=True)
class PolicyConfig:
    max_actions: int = 8


@dataclass(frozen=True)
class PolicyResult:
    final_state: dict[str, Any]
    policy_trace: list[dict[str, Any]]


def _copy_state(initial_state: dict[str, Any]) -> dict[str, Any]:
    copied = dict(initial_state)
    copied["pending_actions"] = list(initial_state.get("pending_actions") or [])
    copied["route_candidates"] = list(initial_state.get("route_candidates") or [])
    return copied


def run_policy(initial_state: dict[str, Any], *, config: PolicyConfig) -> PolicyResult:
    state = _copy_state(initial_state)
    pending_actions = state["pending_actions"]
    for action in pending_actions:
        if action not in ALLOWED_ACTIONS:
            raise ValueError(f"Unsupported Phase A action: {action}")

    policy_trace: list[dict[str, Any]] = []
    step = 0
    max_actions = max(0, int(config.max_actions))
    while pending_actions and step < max_actions:
        action = pending_actions.pop(0)
        policy_trace.append(
            {
                "step": step,
                "action": action,
                "stop_reason": "",
            }
        )
        step += 1

    stop_reason = "completed"
    if pending_actions:
        stop_reason = "budget_exhausted"

    policy_trace.append(
        {
            "step": step,
            "action": "stop",
            "stop_reason": stop_reason,
        }
    )
    return PolicyResult(final_state=state, policy_trace=policy_trace)
