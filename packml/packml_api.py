"""FastAPI router for PackML State Machine endpoints."""
import logging
import time
from typing import Optional
from fastapi import APIRouter
from pydantic import BaseModel

from .state_machine import (
    PackMLState, PackMLCommand, VALID_TRANSITIONS, TRANSIENT_STATES,
    get_machine, get_all_machines, init_machines, StateTransition,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/packml", tags=["PackML"])


# ── Request/Response Models ───────────────────────────────────────────────

class CommandRequest(BaseModel):
    machine_code: str
    command: str  # Start, Stop, Hold, Unhold, Abort, Clear, Reset, Complete
    triggered_by: str = "operator"


class CommandResponse(BaseModel):
    ok: bool
    machine_code: str
    state: str
    message: str = ""


# ── Endpoints ─────────────────────────────────────────────────────────────

@router.get("/states")
def get_all_states():
    """Get current PackML state for all machines."""
    machines = get_all_machines()
    if not machines:
        init_machines(["MX100", "MX200"])
        machines = get_all_machines()
    return {
        "machines": {code: m.to_dict() for code, m in machines.items()}
    }


@router.get("/states/{machine_code}")
def get_machine_state(machine_code: str):
    """Get detailed state for a specific machine."""
    m = get_machine(machine_code)
    data = m.to_dict()
    # Add state diagram info: all states with connections
    data["diagram"] = _build_diagram_data(m.state)
    return data


@router.post("/command")
def send_command(req: CommandRequest):
    """Send a PackML command to a machine."""
    m = get_machine(req.machine_code)
    old_state = m.state.value
    ok = m.command(req.command, req.triggered_by)

    if ok:
        return CommandResponse(
            ok=True,
            machine_code=req.machine_code,
            state=m.state.value,
            message=f"{old_state} -> {m.state.value}",
        )
    else:
        return CommandResponse(
            ok=False,
            machine_code=req.machine_code,
            state=m.state.value,
            message=f"Command '{req.command}' not valid in state '{m.state.value}'. "
                    f"Allowed: {m.get_allowed_commands()}",
        )


@router.get("/history/{machine_code}")
def get_history(machine_code: str, limit: int = 50):
    """Get state transition history for a machine."""
    m = get_machine(machine_code)
    history = m.history[-limit:]
    return {
        "machine_code": machine_code,
        "transitions": [
            {
                "from_state": t.from_state,
                "to_state": t.to_state,
                "command": t.command,
                "triggered_by": t.triggered_by,
                "timestamp": t.timestamp,
            }
            for t in reversed(history)
        ],
    }


@router.get("/diagram/{machine_code}")
def get_diagram(machine_code: str):
    """Get state diagram data for SVG rendering."""
    m = get_machine(machine_code)
    return _build_diagram_data(m.state)


# ── Helper ────────────────────────────────────────────────────────────────

# State positions for SVG layout (x, y coordinates in a grid)
STATE_POSITIONS = {
    "Stopped":     (50, 50),
    "Resetting":   (200, 50),
    "Idle":        (350, 50),
    "Starting":    (500, 50),
    "Execute":     (650, 50),
    "Completing":  (650, 180),
    "Complete":    (500, 180),
    "Holding":     (650, 310),
    "Held":        (500, 310),
    "Unholding":   (350, 310),
    "Stopping":    (200, 180),
    "Suspended":   (350, 180),
    "Unsuspending": (350, 240),
    "Aborting":    (50, 310),
    "Aborted":     (200, 310),
    "Clearing":    (50, 180),
}


def _build_diagram_data(current_state: PackMLState) -> dict:
    """Build state diagram data for frontend SVG rendering."""
    states = []
    for st in PackMLState:
        if st == PackMLState.UNDEFINED:
            continue
        pos = STATE_POSITIONS.get(st.value, (0, 0))
        is_transient = st in TRANSIENT_STATES
        states.append({
            "name": st.value,
            "x": pos[0],
            "y": pos[1],
            "is_current": st == current_state,
            "is_transient": is_transient,
            "type": "acting" if is_transient else "wait",
        })

    edges = []
    for from_state, transitions in VALID_TRANSITIONS.items():
        if from_state == PackMLState.UNDEFINED:
            continue
        for cmd, to_state in transitions.items():
            edges.append({
                "from": from_state.value,
                "to": to_state.value,
                "command": cmd.value,
                "active": from_state == current_state,
            })
    # Auto-transitions for transient states
    for from_st, to_st in TRANSIENT_STATES.items():
        edges.append({
            "from": from_st.value,
            "to": to_st.value,
            "command": "SC",
            "active": from_st == current_state,
        })

    return {"states": states, "edges": edges, "current": current_state.value}
