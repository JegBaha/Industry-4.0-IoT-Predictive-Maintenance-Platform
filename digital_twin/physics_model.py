"""Physics-based models for expected sensor values.

Each machine type has a set of equations that predict expected temperature,
vibration, and current based on operating conditions. These are compared
against actual readings to detect anomalies.
"""
import math
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BearingParams:
    """Bearing geometry for vibration modeling."""
    n_balls: int = 9
    ball_diameter: float = 12.7  # mm
    pitch_diameter: float = 46.0  # mm
    contact_angle: float = 0.0  # degrees
    nominal_rpm: float = 1500.0


@dataclass
class PhysicsModel:
    """Calculates expected sensor values based on operating conditions.

    Models:
    - Temperature: ambient + friction heat + load-dependent heat - cooling
    - Vibration: base vibration + bearing-age degradation + load component
    - Current: no-load current + load-proportional current
    """
    machine_code: str
    ambient_temp: float = 22.0
    base_temp_rise: float = 33.0  # deg C rise from ambient at no-load
    load_temp_coeff: float = 0.15  # deg C per throughput unit
    cooling_time_constant: float = 300.0  # seconds to reach thermal equilibrium

    base_vibration: float = 0.18  # mm/s RMS at no-load, new machine
    vibration_load_coeff: float = 0.003  # mm/s per throughput unit
    bearing_wear_rate: float = 0.00001  # mm/s per hour of operation

    no_load_current: float = 2.5  # A
    load_current_coeff: float = 0.12  # A per throughput unit
    current_temp_coeff: float = 0.01  # A per deg C above ambient (resistance increase)

    bearing: BearingParams = field(default_factory=BearingParams)
    operating_hours: float = 0.0  # Total hours since last maintenance

    def expected_temperature(
        self,
        throughput: int = 0,
        runtime_minutes: float = 60.0,
        ambient: Optional[float] = None,
    ) -> float:
        """Predict expected temperature based on load and runtime."""
        amb = ambient if ambient is not None else self.ambient_temp
        # Thermal model: exponential approach to equilibrium
        time_factor = 1 - math.exp(-runtime_minutes * 60 / self.cooling_time_constant)
        equilibrium_temp = amb + self.base_temp_rise + self.load_temp_coeff * throughput
        return amb + (equilibrium_temp - amb) * time_factor

    def expected_vibration(
        self,
        throughput: int = 0,
        rpm: Optional[float] = None,
    ) -> float:
        """Predict expected vibration level."""
        actual_rpm = rpm if rpm is not None else self.bearing.nominal_rpm
        rpm_factor = actual_rpm / self.bearing.nominal_rpm if self.bearing.nominal_rpm > 0 else 1.0

        base = self.base_vibration * rpm_factor
        load_component = self.vibration_load_coeff * throughput
        wear_component = self.bearing_wear_rate * self.operating_hours

        return base + load_component + wear_component

    def expected_current(
        self,
        throughput: int = 0,
        temperature: float = 22.0,
    ) -> float:
        """Predict expected motor current."""
        temp_delta = max(0, temperature - self.ambient_temp)
        return (
            self.no_load_current
            + self.load_current_coeff * throughput
            + self.current_temp_coeff * temp_delta
        )

    def bpfo(self) -> float:
        """Ball Pass Frequency Outer race."""
        b = self.bearing
        cos_a = math.cos(math.radians(b.contact_angle))
        return (b.n_balls / 2) * (b.nominal_rpm / 60) * (1 - b.ball_diameter / b.pitch_diameter * cos_a)

    def bpfi(self) -> float:
        """Ball Pass Frequency Inner race."""
        b = self.bearing
        cos_a = math.cos(math.radians(b.contact_angle))
        return (b.n_balls / 2) * (b.nominal_rpm / 60) * (1 + b.ball_diameter / b.pitch_diameter * cos_a)

    def bsf(self) -> float:
        """Ball Spin Frequency."""
        b = self.bearing
        cos_a = math.cos(math.radians(b.contact_angle))
        return (b.pitch_diameter / (2 * b.ball_diameter)) * (b.nominal_rpm / 60) * (
            1 - (b.ball_diameter / b.pitch_diameter * cos_a) ** 2
        )

    def ftf(self) -> float:
        """Fundamental Train Frequency."""
        b = self.bearing
        cos_a = math.cos(math.radians(b.contact_angle))
        return (b.nominal_rpm / 60) / 2 * (1 - b.ball_diameter / b.pitch_diameter * cos_a)


# Default models for SmartFact machines
DEFAULT_MODELS = {
    "MX100": PhysicsModel(
        machine_code="MX100",
        base_temp_rise=35.0,
        base_vibration=0.20,
        no_load_current=2.8,
        bearing=BearingParams(n_balls=9, ball_diameter=12.7, pitch_diameter=46.0, nominal_rpm=1500),
    ),
    "MX200": PhysicsModel(
        machine_code="MX200",
        base_temp_rise=30.0,
        base_vibration=0.15,
        no_load_current=2.2,
        bearing=BearingParams(n_balls=11, ball_diameter=10.0, pitch_diameter=52.0, nominal_rpm=1800),
    ),
}
