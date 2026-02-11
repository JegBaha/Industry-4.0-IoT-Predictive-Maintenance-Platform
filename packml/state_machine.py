"""PackML State Machine implementation per ISA-TR88.00.02.

Defines the 17-state PackML model with valid transitions, auto-completion
timers for transient states, and per-machine state tracking with history.
"""
import logging
import time
import threading
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Callable

log = logging.getLogger(__name__)


class PackMLState(str, Enum):
    """ISA-TR88.00.02 PackML states."""
    UNDEFINED = "Undefined"
    IDLE = "Idle"
    STARTING = "Starting"
    EXECUTE = "Execute"
    COMPLETING = "Completing"
    COMPLETE = "Complete"
    HOLDING = "Holding"
    HELD = "Held"
    UNHOLDING = "Unholding"
    STOPPING = "Stopping"
    STOPPED = "Stopped"
    ABORTING = "Aborting"
    ABORTED = "Aborted"
    CLEARING = "Clearing"
    RESETTING = "Resetting"
    SUSPENDED = "Suspended"
    UNSUSPENDING = "Unsuspending"


# Commands that operators / systems can issue
class PackMLCommand(str, Enum):
    START = "Start"
    STOP = "Stop"
    HOLD = "Hold"
    UNHOLD = "Unhold"
    SUSPEND = "Suspend"
    UNSUSPEND = "Unsuspend"
    ABORT = "Abort"
    CLEAR = "Clear"
    RESET = "Reset"
    COMPLETE = "Complete"


# Valid transitions: {current_state: {command: next_state}}
VALID_TRANSITIONS: dict[PackMLState, dict[PackMLCommand, PackMLState]] = {
    PackMLState.IDLE: {
        PackMLCommand.START: PackMLState.STARTING,
        PackMLCommand.STOP: PackMLState.STOPPING,
        PackMLCommand.ABORT: PackMLState.ABORTING,
    },
    PackMLState.STARTING: {
        PackMLCommand.STOP: PackMLState.STOPPING,
        PackMLCommand.ABORT: PackMLState.ABORTING,
    },
    PackMLState.EXECUTE: {
        PackMLCommand.HOLD: PackMLState.HOLDING,
        PackMLCommand.STOP: PackMLState.STOPPING,
        PackMLCommand.SUSPEND: PackMLState.SUSPENDED,  # direct for simplicity
        PackMLCommand.COMPLETE: PackMLState.COMPLETING,
        PackMLCommand.ABORT: PackMLState.ABORTING,
    },
    PackMLState.COMPLETING: {
        PackMLCommand.ABORT: PackMLState.ABORTING,
    },
    PackMLState.COMPLETE: {
        PackMLCommand.RESET: PackMLState.RESETTING,
        PackMLCommand.ABORT: PackMLState.ABORTING,
    },
    PackMLState.HOLDING: {
        PackMLCommand.ABORT: PackMLState.ABORTING,
    },
    PackMLState.HELD: {
        PackMLCommand.UNHOLD: PackMLState.UNHOLDING,
        PackMLCommand.STOP: PackMLState.STOPPING,
        PackMLCommand.ABORT: PackMLState.ABORTING,
    },
    PackMLState.UNHOLDING: {
        PackMLCommand.ABORT: PackMLState.ABORTING,
    },
    PackMLState.STOPPING: {
        PackMLCommand.ABORT: PackMLState.ABORTING,
    },
    PackMLState.STOPPED: {
        PackMLCommand.RESET: PackMLState.RESETTING,
        PackMLCommand.ABORT: PackMLState.ABORTING,
    },
    PackMLState.ABORTING: {},
    PackMLState.ABORTED: {
        PackMLCommand.CLEAR: PackMLState.CLEARING,
    },
    PackMLState.CLEARING: {},
    PackMLState.RESETTING: {
        PackMLCommand.ABORT: PackMLState.ABORTING,
    },
    PackMLState.SUSPENDED: {
        PackMLCommand.UNSUSPEND: PackMLState.UNSUSPENDING,
        PackMLCommand.STOP: PackMLState.STOPPING,
        PackMLCommand.ABORT: PackMLState.ABORTING,
    },
    PackMLState.UNSUSPENDING: {
        PackMLCommand.ABORT: PackMLState.ABORTING,
    },
}

# Transient states auto-complete after a delay (simulated processing time)
TRANSIENT_STATES: dict[PackMLState, PackMLState] = {
    PackMLState.STARTING: PackMLState.EXECUTE,
    PackMLState.COMPLETING: PackMLState.COMPLETE,
    PackMLState.HOLDING: PackMLState.HELD,
    PackMLState.UNHOLDING: PackMLState.EXECUTE,
    PackMLState.STOPPING: PackMLState.STOPPED,
    PackMLState.ABORTING: PackMLState.ABORTED,
    PackMLState.CLEARING: PackMLState.STOPPED,
    PackMLState.RESETTING: PackMLState.IDLE,
    PackMLState.UNSUSPENDING: PackMLState.EXECUTE,
}

# Default auto-transition delays in seconds
TRANSIENT_DELAYS: dict[PackMLState, float] = {
    PackMLState.STARTING: 2.0,
    PackMLState.COMPLETING: 1.5,
    PackMLState.HOLDING: 1.0,
    PackMLState.UNHOLDING: 1.0,
    PackMLState.STOPPING: 1.5,
    PackMLState.ABORTING: 0.5,
    PackMLState.CLEARING: 2.0,
    PackMLState.RESETTING: 1.5,
    PackMLState.UNSUSPENDING: 1.0,
}


@dataclass
class StateTransition:
    """Record of a single state transition."""
    from_state: str
    to_state: str
    command: str
    triggered_by: str
    timestamp: float = field(default_factory=time.time)


class PackMLMachine:
    """PackML state machine for a single machine.

    Tracks current state, validates transitions, auto-completes transient
    states after configurable delays, and logs all transitions.
    """

    def __init__(
        self,
        machine_code: str,
        initial_state: PackMLState = PackMLState.STOPPED,
        on_transition: Optional[Callable[["PackMLMachine", StateTransition], None]] = None,
    ):
        self.machine_code = machine_code
        self._state = initial_state
        self._lock = threading.Lock()
        self._history: list[StateTransition] = []
        self._on_transition = on_transition
        self._auto_timer: Optional[threading.Timer] = None
        self._state_entered_at = time.time()

    @property
    def state(self) -> PackMLState:
        return self._state

    @property
    def state_duration(self) -> float:
        return time.time() - self._state_entered_at

    @property
    def history(self) -> list[StateTransition]:
        return list(self._history)

    def get_allowed_commands(self) -> list[str]:
        transitions = VALID_TRANSITIONS.get(self._state, {})
        return [cmd.value for cmd in transitions.keys()]

    def command(self, cmd: str, triggered_by: str = "operator") -> bool:
        """Execute a command. Returns True if transition happened."""
        try:
            packml_cmd = PackMLCommand(cmd)
        except ValueError:
            log.warning(f"[{self.machine_code}] Invalid command: {cmd}")
            return False

        with self._lock:
            transitions = VALID_TRANSITIONS.get(self._state, {})
            if packml_cmd not in transitions:
                log.warning(
                    f"[{self.machine_code}] Command '{cmd}' not valid in state "
                    f"'{self._state.value}'. Allowed: {[c.value for c in transitions]}"
                )
                return False

            next_state = transitions[packml_cmd]
            self._transition(next_state, cmd, triggered_by)
            return True

    def _transition(self, new_state: PackMLState, command: str, triggered_by: str):
        """Internal transition — must hold lock."""
        if self._auto_timer:
            self._auto_timer.cancel()
            self._auto_timer = None

        old_state = self._state
        self._state = new_state
        self._state_entered_at = time.time()

        record = StateTransition(
            from_state=old_state.value,
            to_state=new_state.value,
            command=command,
            triggered_by=triggered_by,
        )
        self._history.append(record)
        if len(self._history) > 500:
            self._history = self._history[-500:]

        log.info(
            f"[{self.machine_code}] {old_state.value} -> {new_state.value} "
            f"(cmd={command}, by={triggered_by})"
        )

        if self._on_transition:
            try:
                self._on_transition(self, record)
            except Exception:
                log.exception("on_transition callback error")

        # Schedule auto-transition for transient states
        if new_state in TRANSIENT_STATES:
            delay = TRANSIENT_DELAYS.get(new_state, 1.5)
            target = TRANSIENT_STATES[new_state]
            self._auto_timer = threading.Timer(
                delay,
                self._auto_complete,
                args=(new_state, target),
            )
            self._auto_timer.daemon = True
            self._auto_timer.start()

    def _auto_complete(self, expected_from: PackMLState, target: PackMLState):
        """Auto-complete a transient state."""
        with self._lock:
            if self._state != expected_from:
                return  # State changed externally
            self._transition(target, "SC", "auto")

    def to_dict(self) -> dict:
        return {
            "machine_code": self.machine_code,
            "state": self._state.value,
            "state_duration": round(self.state_duration, 1),
            "allowed_commands": self.get_allowed_commands(),
        }

    def stop(self):
        if self._auto_timer:
            self._auto_timer.cancel()


# ── Module-level machine registry ─────────────────────────────────────────

_machines: dict[str, PackMLMachine] = {}
_on_transition_cb: Optional[Callable] = None


def set_transition_callback(cb: Callable):
    global _on_transition_cb
    _on_transition_cb = cb


def get_machine(machine_code: str) -> PackMLMachine:
    if machine_code not in _machines:
        _machines[machine_code] = PackMLMachine(
            machine_code,
            on_transition=_on_transition_cb,
        )
    return _machines[machine_code]


def get_all_machines() -> dict[str, PackMLMachine]:
    return dict(_machines)


def init_machines(machine_codes: list[str]):
    for code in machine_codes:
        get_machine(code)
