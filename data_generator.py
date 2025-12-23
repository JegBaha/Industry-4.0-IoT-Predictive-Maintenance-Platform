"""Synthetic sensor data generator."""
from dataclasses import dataclass
from typing import Iterator
import random
import time


@dataclass
class SensorReading:
    machine_code: str
    line: str
    timestamp: float
    temperature: float
    vibration: float
    current: float
    throughput: int
    failure_flag: bool


def generate_stream(
    machines: list[tuple[str, str]],
    base_temp: float = 55.0,
    base_vibration: float = 0.3,
    base_current: float = 10.0,
    failure_rate: float = 0.01,
    seed: int | None = None,
) -> Iterator[SensorReading]:
    rng = random.Random(seed)
    while True:
        for code, line in machines:
            ts = time.time()
            temp = base_temp + rng.uniform(-3, 6)
            vib = base_vibration + rng.uniform(0, 0.4)
            cur = base_current + rng.uniform(-1, 2)
            failure = rng.random() < failure_rate
            throughput = max(0, int(rng.gauss(90, 10)))
            if failure:
                throughput = 0
                vib *= 2.2
                temp += 10
            yield SensorReading(
                machine_code=code,
                line=line,
                timestamp=ts,
                temperature=round(temp, 2),
                vibration=round(vib, 3),
                current=round(cur, 2),
                throughput=throughput,
                failure_flag=failure,
            )
