"""Traceability service — Part tracking with DMC codes (IATF 16949).

Assigns unique Data Matrix Codes to produced parts, tracks production
parameters, and supports forward/reverse traceability.
"""
import logging
import time
import hashlib
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from config import trace_cfg

log = logging.getLogger(__name__)


def generate_dmc(prefix: str, machine_code: str, seq: int) -> str:
    """Generate a unique Data Matrix Code."""
    ts = time.strftime("%y%m%d%H%M%S")
    return f"{prefix}-{machine_code}-{ts}-{seq:04d}"


@dataclass
class PartEvent:
    event_type: str  # produced, inspected, shipped, complaint
    detail: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class TracedPart:
    dmc_code: str
    product_code: str
    machine_code: str
    wo_number: str = ""
    recipe_code: str = ""
    batch_number: str = ""
    quality_status: str = "ok"  # ok, rework, scrap
    parameters: dict = field(default_factory=dict)  # sensor readings at production time
    events: list = field(default_factory=list)
    produced_at: float = field(default_factory=time.time)

    def add_event(self, event_type: str, detail: dict = None):
        self.events.append(PartEvent(event_type, detail or {}))

    def to_dict(self) -> dict:
        return {
            "dmc_code": self.dmc_code,
            "product_code": self.product_code,
            "machine_code": self.machine_code,
            "wo_number": self.wo_number,
            "recipe_code": self.recipe_code,
            "batch_number": self.batch_number,
            "quality_status": self.quality_status,
            "parameters": self.parameters,
            "produced_at": self.produced_at,
            "events": [
                {"event_type": e.event_type, "detail": e.detail, "timestamp": e.timestamp}
                for e in self.events
            ],
        }


class TraceService:
    """In-memory part traceability with forward/reverse trace support."""

    def __init__(self):
        self._parts: dict[str, TracedPart] = {}  # dmc -> part
        self._batch_index: dict[str, list[str]] = {}  # batch -> [dmc_codes]
        self._machine_index: dict[str, deque] = {}  # machine -> deque of dmc_codes
        self._wo_index: dict[str, list[str]] = {}  # wo_number -> [dmc_codes]
        self._seq = 0
        self._init_sample_data()

    def _init_sample_data(self):
        """Create some sample traced parts."""
        samples = [
            ("PRODUCT_A", "MX100", "WO-2026-001", "RCP-001", "BATCH-001"),
            ("PRODUCT_A", "MX100", "WO-2026-001", "RCP-001", "BATCH-001"),
            ("PRODUCT_B", "MX200", "WO-2026-002", "RCP-002", "BATCH-002"),
        ]
        for product, machine, wo, recipe, batch in samples:
            self.create_part(product, machine, wo, recipe, batch, {
                "temperature": 55.2, "vibration": 0.35, "current": 8.5,
            })

    def create_part(self, product_code: str, machine_code: str,
                    wo_number: str = "", recipe_code: str = "",
                    batch_number: str = "", parameters: dict = None) -> TracedPart:
        self._seq += 1
        dmc = generate_dmc(trace_cfg.dmc_prefix, machine_code, self._seq)
        batch = batch_number or f"BATCH-{time.strftime('%y%m%d')}"

        part = TracedPart(
            dmc_code=dmc,
            product_code=product_code,
            machine_code=machine_code,
            wo_number=wo_number,
            recipe_code=recipe_code,
            batch_number=batch,
            parameters=parameters or {},
        )
        part.add_event("produced", {"machine": machine_code, "parameters": parameters})

        self._parts[dmc] = part

        # Update indices
        self._batch_index.setdefault(batch, []).append(dmc)

        if machine_code not in self._machine_index:
            self._machine_index[machine_code] = deque(maxlen=1000)
        self._machine_index[machine_code].append(dmc)

        if wo_number:
            self._wo_index.setdefault(wo_number, []).append(dmc)

        return part

    def get_part(self, dmc_code: str) -> Optional[TracedPart]:
        return self._parts.get(dmc_code)

    def search(self, wo_number: str = None, machine_code: str = None,
               batch_number: str = None, limit: int = 50) -> list[TracedPart]:
        result = list(self._parts.values())
        if wo_number:
            dmcs = self._wo_index.get(wo_number, [])
            result = [self._parts[d] for d in dmcs if d in self._parts]
        if machine_code:
            result = [p for p in result if p.machine_code == machine_code]
        if batch_number:
            dmcs = self._batch_index.get(batch_number, [])
            result = [self._parts[d] for d in dmcs if d in self._parts]
        return sorted(result, key=lambda p: p.produced_at, reverse=True)[:limit]

    def reverse_trace(self, dmc_code: str) -> dict:
        """Find all parts produced under same conditions."""
        part = self._parts.get(dmc_code)
        if not part:
            return {"error": "Part not found"}

        related = {
            "same_batch": [self._parts[d].to_dict() for d in self._batch_index.get(part.batch_number, [])
                          if d in self._parts and d != dmc_code],
            "same_wo": [self._parts[d].to_dict() for d in self._wo_index.get(part.wo_number, [])
                       if d in self._parts and d != dmc_code][:20],
            "batch_number": part.batch_number,
            "wo_number": part.wo_number,
            "machine_code": part.machine_code,
            "recipe_code": part.recipe_code,
        }
        return related

    def add_event(self, dmc_code: str, event_type: str, detail: dict = None) -> bool:
        part = self._parts.get(dmc_code)
        if not part:
            return False
        part.add_event(event_type, detail)
        return True

    def get_batch_report(self, batch_number: str) -> dict:
        dmcs = self._batch_index.get(batch_number, [])
        parts = [self._parts[d] for d in dmcs if d in self._parts]
        ok_count = sum(1 for p in parts if p.quality_status == "ok")
        return {
            "batch_number": batch_number,
            "total_parts": len(parts),
            "ok": ok_count,
            "scrap": sum(1 for p in parts if p.quality_status == "scrap"),
            "rework": sum(1 for p in parts if p.quality_status == "rework"),
            "quality_pct": round(ok_count / len(parts) * 100, 1) if parts else 0,
            "parts": [p.to_dict() for p in parts[:50]],
        }

    def get_stats(self) -> dict:
        return {
            "total_parts": len(self._parts),
            "total_batches": len(self._batch_index),
            "machines": list(self._machine_index.keys()),
        }
