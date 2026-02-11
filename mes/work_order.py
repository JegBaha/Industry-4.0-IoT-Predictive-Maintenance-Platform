"""MES Work Order management — CRUD, state transitions, OEE per order."""
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class WorkOrderEvent:
    event_type: str
    detail: dict = field(default_factory=dict)
    operator: str = "system"
    timestamp: float = field(default_factory=time.time)


@dataclass
class WorkOrder:
    wo_number: str
    product_code: str
    machine_code: str
    target_qty: int
    recipe_id: Optional[int] = None
    status: str = "planned"  # planned, started, paused, completed, cancelled
    actual_qty: int = 0
    scrap_qty: int = 0
    planned_start: Optional[float] = None
    planned_end: Optional[float] = None
    actual_start: Optional[float] = None
    actual_end: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    events: list = field(default_factory=list)

    def start(self, operator: str = "system") -> bool:
        if self.status not in ("planned", "paused"):
            return False
        self.status = "started"
        if self.actual_start is None:
            self.actual_start = time.time()
        self.events.append(WorkOrderEvent("start", operator=operator))
        return True

    def pause(self, operator: str = "system") -> bool:
        if self.status != "started":
            return False
        self.status = "paused"
        self.events.append(WorkOrderEvent("pause", operator=operator))
        return True

    def complete(self, operator: str = "system") -> bool:
        if self.status not in ("started", "paused"):
            return False
        self.status = "completed"
        self.actual_end = time.time()
        self.events.append(WorkOrderEvent("complete", operator=operator))
        return True

    def cancel(self, operator: str = "system") -> bool:
        if self.status in ("completed", "cancelled"):
            return False
        self.status = "cancelled"
        self.actual_end = time.time()
        self.events.append(WorkOrderEvent("cancel", operator=operator))
        return True

    def add_production(self, good_qty: int, scrap: int = 0):
        self.actual_qty += good_qty
        self.scrap_qty += scrap
        self.events.append(WorkOrderEvent("qty_update", {
            "good": good_qty, "scrap": scrap,
            "total_good": self.actual_qty, "total_scrap": self.scrap_qty,
        }))

    def report_scrap(self, qty: int, reason: str = "", operator: str = "system"):
        self.scrap_qty += qty
        self.events.append(WorkOrderEvent("scrap", {"qty": qty, "reason": reason}, operator))

    @property
    def duration_hours(self) -> float:
        if self.actual_start is None:
            return 0
        end = self.actual_end or time.time()
        return (end - self.actual_start) / 3600

    @property
    def performance_pct(self) -> float:
        if self.target_qty <= 0:
            return 0
        return round(self.actual_qty / self.target_qty * 100, 1)

    @property
    def quality_pct(self) -> float:
        total = self.actual_qty + self.scrap_qty
        if total <= 0:
            return 100
        return round(self.actual_qty / total * 100, 1)

    @property
    def oee(self) -> dict:
        availability = 100.0  # Simplified — actual would come from downtime tracking
        performance = self.performance_pct
        quality = self.quality_pct
        oee_val = availability * performance * quality / 10000
        return {
            "availability": availability,
            "performance": performance,
            "quality": quality,
            "oee": round(oee_val, 1),
        }

    def to_dict(self) -> dict:
        return {
            "wo_number": self.wo_number,
            "product_code": self.product_code,
            "machine_code": self.machine_code,
            "target_qty": self.target_qty,
            "actual_qty": self.actual_qty,
            "scrap_qty": self.scrap_qty,
            "status": self.status,
            "recipe_id": self.recipe_id,
            "duration_hours": round(self.duration_hours, 2),
            "performance_pct": self.performance_pct,
            "quality_pct": self.quality_pct,
            "oee": self.oee,
            "planned_start": self.planned_start,
            "actual_start": self.actual_start,
            "actual_end": self.actual_end,
            "created_at": self.created_at,
            "event_count": len(self.events),
        }


class WorkOrderService:
    """In-memory work order management with simulation support."""

    def __init__(self):
        self._orders: dict[str, WorkOrder] = {}
        self._counter = 0
        self._init_sample_data()

    def _init_sample_data(self):
        samples = [
            ("WO-2026-001", "PRODUCT_A", "MX100", 500, "completed"),
            ("WO-2026-002", "PRODUCT_B", "MX200", 300, "completed"),
            ("WO-2026-003", "PRODUCT_A", "MX100", 450, "started"),
            ("WO-2026-004", "PRODUCT_C", "MX200", 200, "planned"),
        ]
        for wo_num, product, machine, qty, status in samples:
            wo = WorkOrder(wo_number=wo_num, product_code=product, machine_code=machine, target_qty=qty)
            if status == "completed":
                wo.status = "completed"
                wo.actual_qty = int(qty * 0.95)
                wo.scrap_qty = int(qty * 0.02)
                wo.actual_start = time.time() - 28800
                wo.actual_end = time.time() - 3600
            elif status == "started":
                wo.status = "started"
                wo.actual_qty = int(qty * 0.4)
                wo.scrap_qty = int(qty * 0.01)
                wo.actual_start = time.time() - 7200
            self._orders[wo_num] = wo

    def create(self, product_code: str, machine_code: str, target_qty: int,
               recipe_id: int = None, planned_start: float = None) -> WorkOrder:
        self._counter += 1
        wo_number = f"WO-2026-{self._counter + 4:03d}"
        wo = WorkOrder(
            wo_number=wo_number,
            product_code=product_code,
            machine_code=machine_code,
            target_qty=target_qty,
            recipe_id=recipe_id,
            planned_start=planned_start,
        )
        self._orders[wo_number] = wo
        log.info(f"Work order created: {wo_number}")
        return wo

    def get(self, wo_number: str) -> Optional[WorkOrder]:
        return self._orders.get(wo_number)

    def get_all(self, status: str = None, machine_code: str = None) -> list[WorkOrder]:
        orders = list(self._orders.values())
        if status:
            orders = [o for o in orders if o.status == status]
        if machine_code:
            orders = [o for o in orders if o.machine_code == machine_code]
        return sorted(orders, key=lambda o: o.created_at, reverse=True)

    def get_active_order(self, machine_code: str) -> Optional[WorkOrder]:
        for o in self._orders.values():
            if o.machine_code == machine_code and o.status == "started":
                return o
        return None

    def get_shift_report(self) -> dict:
        now = time.time()
        shift_start = now - 8 * 3600  # Last 8 hours
        active = [o for o in self._orders.values() if o.actual_start and o.actual_start > shift_start]
        total_target = sum(o.target_qty for o in active)
        total_actual = sum(o.actual_qty for o in active)
        total_scrap = sum(o.scrap_qty for o in active)
        return {
            "shift_orders": len(active),
            "total_target": total_target,
            "total_actual": total_actual,
            "total_scrap": total_scrap,
            "fulfillment_pct": round(total_actual / total_target * 100, 1) if total_target > 0 else 0,
            "quality_pct": round(total_actual / (total_actual + total_scrap) * 100, 1) if (total_actual + total_scrap) > 0 else 100,
            "orders": [o.to_dict() for o in active],
        }
