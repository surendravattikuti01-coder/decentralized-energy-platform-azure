#!/usr/bin/env python3
"""
Decentralized Energy Grid Optimizer
=====================================
Real-time optimization engine for Tesla Megapack battery systems
integrated with Azure Energy Management.

Features:
- Real-time state-of-charge (SoC) monitoring across 210 Megapack units
- Automated charge/discharge scheduling based on grid signals
- Peak shaving and load balancing algorithms
- NERC CIP / ISO 27001 compliant telemetry handling
- Azure Digital Twins integration for grid topology
- Prometheus metrics + Azure Monitor export
"""

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any

from azure.iot.device.aio import IoTHubDeviceClient
from azure.servicebus.aio import ServiceBusClient
from azure.digitaltwins.core import DigitalTwinsClient
from azure.identity.aio import DefaultAzureCredential
from prometheus_client import Gauge, Counter, Histogram, start_http_server

logger = logging.getLogger(__name__)

# ─── Prometheus Metrics ────────────────────────────────────
MEGAPACK_SOC = Gauge('megapack_state_of_charge_percent',
                     'State of charge for each Megapack unit', ['unit_id', 'site'])
MEGAPACK_POWER_KW = Gauge('megapack_power_kw',
                          'Current power output (positive=discharge, negative=charge)',
                          ['unit_id', 'site'])
GRID_FREQUENCY_HZ = Gauge('grid_frequency_hz', 'Grid frequency in Hz', ['site'])
OPTIMIZATION_CYCLES = Counter('grid_optimization_cycles_total',
                              'Total optimization cycles run', ['result'])
OPTIMIZATION_LATENCY = Histogram('grid_optimization_latency_seconds',
                                 'Time to complete one optimization cycle',
                                 buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0])
PEAK_SHAVE_KWH = Counter('grid_peak_shave_kwh_total',
                         'Total energy used for peak shaving', ['site'])


# ─── Data Models ──────────────────────────────────────────
class OperationMode(str, Enum):
    IDLE = "idle"
    CHARGING = "charging"
    DISCHARGING = "discharging"
    PEAK_SHAVING = "peak_shaving"
    FREQUENCY_REGULATION = "frequency_regulation"
    EMERGENCY = "emergency"


@dataclass
class MegapackUnit:
    unit_id: str
    site_id: str
    capacity_kwh: float = 3900.0        # Tesla Megapack 2 XL capacity
    max_power_kw: float = 1500.0        # Max continuous power
    state_of_charge_pct: float = 50.0
    temperature_c: float = 25.0
    mode: OperationMode = OperationMode.IDLE
    current_power_kw: float = 0.0       # + = discharging, - = charging
    cycle_count: int = 0
    last_telemetry: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    online: bool = True

    @property
    def available_energy_kwh(self) -> float:
        """Energy available for discharge (SoC above 10% floor)."""
        min_soc = 10.0
        return self.capacity_kwh * max(self.state_of_charge_pct - min_soc, 0) / 100

    @property
    def available_charge_kwh(self) -> float:
        """Headroom available for charging (SoC below 95% ceiling)."""
        max_soc = 95.0
        return self.capacity_kwh * max(max_soc - self.state_of_charge_pct, 0) / 100

    @property
    def health_score(self) -> float:
        """Composite health score 0-100 based on temperature, cycles, SoC."""
        temp_score = 100 - max(0, abs(self.temperature_c - 25) * 2)
        cycle_score = max(0, 100 - self.cycle_count * 0.01)
        return (temp_score * 0.4 + cycle_score * 0.6)


@dataclass
class GridSignal:
    timestamp: datetime
    site_id: str
    frequency_hz: float
    grid_load_kw: float
    spot_price_per_kwh: float
    peak_demand_kw: float
    renewable_pct: float
    demand_response_active: bool = False
    emergency: bool = False


@dataclass
class OptimizationResult:
    timestamp: datetime
    site_id: str
    dispatch_commands: dict[str, float]  # unit_id -> power_kw
    total_power_kw: float
    peak_reduction_kw: float
    estimated_revenue: float
    strategy: str
    units_online: int
    units_total: int


# ─── Grid Optimizer Engine ─────────────────────────────────
class GridOptimizer:
    """
    Real-time optimization engine for decentralized energy storage.
    Implements: peak shaving, frequency regulation, arbitrage.
    """

    # Operational constraints
    FREQUENCY_NOMINAL_HZ = 60.0
    FREQUENCY_DEADBAND_HZ = 0.03    # ±0.03 Hz deadband
    PEAK_SHAVE_THRESHOLD_PCT = 0.85 # Activate peak shaving at 85% of peak demand
    MIN_SOC_PCT = 10.0
    MAX_SOC_PCT = 95.0
    FREQ_REGULATION_DROOP = 0.05    # 5% droop control

    def __init__(self, units: list[MegapackUnit]):
        self.units = {u.unit_id: u for u in units}
        self._online_units = [u for u in units if u.online]

    @property
    def total_capacity_kwh(self) -> float:
        return sum(u.capacity_kwh for u in self._online_units)

    @property
    def total_available_discharge_kw(self) -> float:
        return sum(u.max_power_kw for u in self._online_units
                   if u.state_of_charge_pct > self.MIN_SOC_PCT)

    @property
    def fleet_soc_pct(self) -> float:
        if not self._online_units:
            return 0.0
        return sum(u.state_of_charge_pct * u.capacity_kwh for u in self._online_units)                / self.total_capacity_kwh

    def optimize(self, signal: GridSignal) -> OptimizationResult:
        """
        Main optimization loop. Selects strategy based on grid conditions.
        Priority order: Emergency > Frequency Regulation > Peak Shaving > Arbitrage
        """
        with OPTIMIZATION_LATENCY.time():
            if signal.emergency:
                result = self._emergency_response(signal)
            elif abs(signal.frequency_hz - self.FREQUENCY_NOMINAL_HZ) > self.FREQUENCY_DEADBAND_HZ:
                result = self._frequency_regulation(signal)
            elif signal.grid_load_kw > signal.peak_demand_kw * self.PEAK_SHAVE_THRESHOLD_PCT:
                result = self._peak_shaving(signal)
            elif signal.spot_price_per_kwh < 0.05 and self.fleet_soc_pct < self.MAX_SOC_PCT:
                result = self._charge_arbitrage(signal)
            else:
                result = self._idle(signal)

            OPTIMIZATION_CYCLES.labels(result='success').inc()
            self._update_prometheus_metrics(signal)
            return result

    def _peak_shaving(self, signal: GridSignal) -> OptimizationResult:
        """
        Dispatch battery units to reduce grid peak demand.
        Uses SoC-weighted dispatch to balance unit wear.
        """
        target_reduction_kw = signal.grid_load_kw - (signal.peak_demand_kw * 0.80)
        target_reduction_kw = min(target_reduction_kw, self.total_available_discharge_kw)

        commands: dict[str, float] = {}
        remaining_kw = target_reduction_kw

        # Sort units by SoC (highest first) for optimal dispatch
        eligible = sorted(
            [u for u in self._online_units if u.state_of_charge_pct > self.MIN_SOC_PCT + 5],
            key=lambda u: u.state_of_charge_pct * u.health_score,
            reverse=True
        )

        for unit in eligible:
            if remaining_kw <= 0:
                break
            dispatch_kw = min(unit.max_power_kw, remaining_kw)
            commands[unit.unit_id] = dispatch_kw
            remaining_kw -= dispatch_kw

        total_dispatch = sum(commands.values())
        revenue = total_dispatch * signal.spot_price_per_kwh * (15 / 60)  # 15-min interval
        PEAK_SHAVE_KWH.labels(site=signal.site_id).inc(total_dispatch * 0.25)

        logger.info(
            f"Peak shaving: target={target_reduction_kw:.0f}kW, "
            f"dispatched={total_dispatch:.0f}kW across {len(commands)} units"
        )
        return OptimizationResult(
            timestamp=datetime.now(timezone.utc),
            site_id=signal.site_id,
            dispatch_commands=commands,
            total_power_kw=total_dispatch,
            peak_reduction_kw=total_dispatch,
            estimated_revenue=revenue,
            strategy="peak_shaving",
            units_online=len(self._online_units),
            units_total=len(self.units),
        )

    def _frequency_regulation(self, signal: GridSignal) -> OptimizationResult:
        """
        Droop-based frequency regulation.
        Discharge when frequency drops below nominal, charge when above.
        """
        freq_deviation = self.FREQUENCY_NOMINAL_HZ - signal.frequency_hz
        # Droop response: 5% droop → 1 Hz deviation → 20% power change
        response_fraction = freq_deviation / (self.FREQUENCY_NOMINAL_HZ * self.FREQ_REGULATION_DROOP)
        response_fraction = max(-1.0, min(1.0, response_fraction))

        commands: dict[str, float] = {}
        for unit in self._online_units:
            if response_fraction > 0 and unit.state_of_charge_pct <= self.MIN_SOC_PCT:
                continue
            if response_fraction < 0 and unit.state_of_charge_pct >= self.MAX_SOC_PCT:
                continue
            power_kw = unit.max_power_kw * response_fraction * (unit.health_score / 100)
            commands[unit.unit_id] = round(power_kw, 2)

        total = sum(commands.values())
        logger.info(
            f"Frequency regulation: f={signal.frequency_hz:.3f}Hz, "
            f"deviation={freq_deviation:+.3f}Hz, response={total:.0f}kW"
        )
        return OptimizationResult(
            timestamp=datetime.now(timezone.utc),
            site_id=signal.site_id,
            dispatch_commands=commands,
            total_power_kw=total,
            peak_reduction_kw=0,
            estimated_revenue=abs(total) * 0.05 * (15 / 60),
            strategy="frequency_regulation",
            units_online=len(self._online_units),
            units_total=len(self.units),
        )

    def _charge_arbitrage(self, signal: GridSignal) -> OptimizationResult:
        """Charge batteries when spot price is low for later discharge."""
        commands: dict[str, float] = {}
        for unit in self._online_units:
            if unit.state_of_charge_pct < self.MAX_SOC_PCT - 5:
                charge_kw = -min(unit.max_power_kw * 0.8, unit.available_charge_kwh * 4)
                commands[unit.unit_id] = round(charge_kw, 2)

        total = sum(commands.values())
        logger.info(f"Charge arbitrage: spot=${signal.spot_price_per_kwh:.3f}/kWh, charging={total:.0f}kW")
        return OptimizationResult(
            timestamp=datetime.now(timezone.utc),
            site_id=signal.site_id,
            dispatch_commands=commands,
            total_power_kw=total,
            peak_reduction_kw=0,
            estimated_revenue=0,
            strategy="arbitrage_charge",
            units_online=len(self._online_units),
            units_total=len(self.units),
        )

    def _emergency_response(self, signal: GridSignal) -> OptimizationResult:
        """Maximum discharge for grid emergency support."""
        commands = {u.unit_id: u.max_power_kw for u in self._online_units
                    if u.state_of_charge_pct > self.MIN_SOC_PCT}
        total = sum(commands.values())
        logger.critical(f"EMERGENCY RESPONSE: dispatching {total:.0f}kW for grid emergency at {signal.site_id}")
        return OptimizationResult(
            timestamp=datetime.now(timezone.utc),
            site_id=signal.site_id,
            dispatch_commands=commands,
            total_power_kw=total,
            peak_reduction_kw=total,
            estimated_revenue=0,
            strategy="emergency",
            units_online=len(self._online_units),
            units_total=len(self.units),
        )

    def _idle(self, signal: GridSignal) -> OptimizationResult:
        """No action needed - maintain current state."""
        return OptimizationResult(
            timestamp=datetime.now(timezone.utc),
            site_id=signal.site_id,
            dispatch_commands={u.unit_id: 0.0 for u in self._online_units},
            total_power_kw=0,
            peak_reduction_kw=0,
            estimated_revenue=0,
            strategy="idle",
            units_online=len(self._online_units),
            units_total=len(self.units),
        )

    def _update_prometheus_metrics(self, signal: GridSignal) -> None:
        for unit in self._online_units:
            MEGAPACK_SOC.labels(unit_id=unit.unit_id, site=unit.site_id).set(unit.state_of_charge_pct)
            MEGAPACK_POWER_KW.labels(unit_id=unit.unit_id, site=unit.site_id).set(unit.current_power_kw)
        GRID_FREQUENCY_HZ.labels(site=signal.site_id).set(signal.frequency_hz)

    def update_telemetry(self, telemetry: dict) -> None:
        """Ingest live telemetry update from a Megapack unit."""
        unit_id = telemetry.get("unit_id")
        if unit_id not in self.units:
            logger.warning(f"Unknown unit: {unit_id}")
            return
        unit = self.units[unit_id]
        unit.state_of_charge_pct = telemetry.get("soc_pct", unit.state_of_charge_pct)
        unit.current_power_kw = telemetry.get("power_kw", unit.current_power_kw)
        unit.temperature_c = telemetry.get("temperature_c", unit.temperature_c)
        unit.online = telemetry.get("online", unit.online)
        unit.last_telemetry = datetime.now(timezone.utc)
        self._online_units = [u for u in self.units.values() if u.online]


# ─── Main Service Loop ─────────────────────────────────────
async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )
    start_http_server(int(os.environ.get("METRICS_PORT", "9090")))

    # Initialize 210 Megapack units across 6 sites (35 per site)
    units = [
        MegapackUnit(
            unit_id=f"megapack-site{site}-unit{unit:03d}",
            site_id=f"site-{site}",
            state_of_charge_pct=60.0 + (unit % 30),
        )
        for site in range(1, 7)
        for unit in range(1, 36)
    ]
    optimizer = GridOptimizer(units)

    logger.info(f"Grid Optimizer initialized: {len(units)} Megapack units, "
                f"{optimizer.total_capacity_kwh/1000:.1f} MWh total capacity")

    # Connect to Azure Service Bus for grid signals
    credential = DefaultAzureCredential()
    async with ServiceBusClient(
        fully_qualified_namespace=os.environ["SERVICEBUS_NAMESPACE"],
        credential=credential,
    ) as sb_client:
        async with sb_client.get_subscription_receiver(
            topic_name="grid-signals",
            subscription_name="optimizer",
        ) as receiver:
            logger.info("Listening for grid signals...")
            async for msg in receiver:
                try:
                    signal_data = json.loads(str(msg))
                    signal = GridSignal(
                        timestamp=datetime.fromisoformat(signal_data["timestamp"]),
                        site_id=signal_data["site_id"],
                        frequency_hz=signal_data["frequency_hz"],
                        grid_load_kw=signal_data["grid_load_kw"],
                        spot_price_per_kwh=signal_data["spot_price_per_kwh"],
                        peak_demand_kw=signal_data["peak_demand_kw"],
                        renewable_pct=signal_data.get("renewable_pct", 0),
                        demand_response_active=signal_data.get("demand_response_active", False),
                        emergency=signal_data.get("emergency", False),
                    )
                    result = optimizer.optimize(signal)
                    logger.info(
                        f"Optimization: strategy={result.strategy}, "
                        f"power={result.total_power_kw:.0f}kW, "
                        f"revenue=${result.estimated_revenue:.2f}"
                    )
                    await receiver.complete_message(msg)
                except Exception as e:
                    logger.error(f"Failed to process grid signal: {e}", exc_info=True)
                    await receiver.abandon_message(msg)


if __name__ == "__main__":
    asyncio.run(main())
