#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import signal
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


#DEFAULT_URL = "http://192.168.3.14:8086"
DEFAULT_URL = "http://192.168.5.14:8086"
DEFAULT_TOKEN = "pf-OGC6AQFmKy64gOzRM12DZrCuavnWeMgRZ2kDMOk8LYK22evDJnoyKGcmY49EgT8HnMDE9GPQeg30vXeHsRQ=="
DEFAULT_ORG = "belimo"
DEFAULT_BUCKET = "actuator-data"
DEFAULT_MEASUREMENT = "_process"
COMMAND_TIMESTAMP = datetime.fromtimestamp(0, tz=timezone.utc)

should_stop = False


@dataclass
class Phase:
    name: str
    mode: str
    duration_seconds: float
    bias: float
    amplitude: float = 0.0
    frequency: float = 0.0
    phase_offset: float = 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write scripted Belimo actuator commands into the InfluxDB _process measurement."
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="InfluxDB URL")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="InfluxDB token")
    parser.add_argument("--org", default=DEFAULT_ORG, help="InfluxDB org")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="InfluxDB bucket")
    parser.add_argument("--measurement", default=DEFAULT_MEASUREMENT, help="Command measurement")
    parser.add_argument(
        "--suite",
        choices=["characterize", "single"],
        default="characterize",
        help="Built-in test suite to run",
    )
    parser.add_argument("--test-number", type=int, required=True, help="Experiment label written to InfluxDB")
    parser.add_argument("--poll-seconds", type=float, default=1.0, help="Command write interval in seconds")
    parser.add_argument("--bias", type=float, default=50.0, help="Center position for single mode")
    parser.add_argument("--amplitude", type=float, default=20.0, help="Amplitude for single mode")
    parser.add_argument("--frequency", type=float, default=0.03, help="Frequency for single mode")
    parser.add_argument(
        "--waveform",
        choices=["constant", "sine", "triangle", "square"],
        default="constant",
        help="Waveform for single mode",
    )
    parser.add_argument("--duration-seconds", type=float, default=60.0, help="Duration for single mode")
    parser.add_argument(
        "--command-log",
        default="data/belimo_command_log.csv",
        help="Local CSV log of every command that gets written",
    )
    parser.add_argument(
        "--manifest-json",
        default="data/belimo_test_manifest.json",
        help="Local JSON summary of the run",
    )
    return parser.parse_args()


def handle_stop(_signum: int, _frame: Any) -> None:
    global should_stop
    should_stop = True


def build_phases(args: argparse.Namespace) -> list[Phase]:
    if args.suite == "single":
        return [
            Phase(
                name=f"single_{args.waveform}",
                mode=args.waveform,
                duration_seconds=args.duration_seconds,
                bias=args.bias,
                amplitude=args.amplitude,
                frequency=args.frequency,
            )
        ]

    return [
        Phase(name="baseline_low", mode="constant", duration_seconds=20, bias=20),
        Phase(name="baseline_mid", mode="constant", duration_seconds=20, bias=50),
        Phase(name="baseline_high", mode="constant", duration_seconds=20, bias=80),
        Phase(name="step_reset", mode="constant", duration_seconds=15, bias=20),
        Phase(name="sine_soft", mode="sine", duration_seconds=60, bias=50, amplitude=20, frequency=0.03),
        Phase(name="triangle_mid", mode="triangle", duration_seconds=45, bias=50, amplitude=25, frequency=0.04),
        Phase(name="square_stress", mode="square", duration_seconds=30, bias=50, amplitude=20, frequency=0.03),
        Phase(name="safe_finish", mode="constant", duration_seconds=15, bias=50),
    ]


def compute_setpoint(phase: Phase, elapsed_seconds: float) -> float:
    if phase.mode == "constant":
        return clamp_position(phase.bias)

    angular = (2.0 * math.pi * phase.frequency * elapsed_seconds) + phase.phase_offset
    if phase.mode == "sine":
        raw_value = phase.bias + (phase.amplitude * math.sin(angular))
    elif phase.mode == "triangle":
        raw_value = phase.bias + (phase.amplitude * (2.0 / math.pi) * math.asin(math.sin(angular)))
    elif phase.mode == "square":
        raw_value = phase.bias + (phase.amplitude if math.sin(angular) >= 0 else -phase.amplitude)
    else:
        raise ValueError(f"Unsupported mode: {phase.mode}")

    return clamp_position(raw_value)


def clamp_position(value: float) -> float:
    return max(0.0, min(100.0, round(value, 6)))


def write_command(
    write_api: Any,
    *,
    bucket: str,
    measurement: str,
    setpoint_position: float,
    test_number: int,
) -> None:
    point = (
        Point(measurement)
        .field("setpoint_position_%", float(setpoint_position))
        .field("test_number", int(test_number))
        .time(COMMAND_TIMESTAMP, WritePrecision.MS)
    )
    write_api.write(bucket=bucket, record=point)


def append_command_log(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "logged_at",
        "phase_name",
        "mode",
        "test_number",
        "setpoint_position_%",
        "bias",
        "amplitude",
        "frequency",
        "elapsed_phase_seconds",
    ]
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def main() -> int:
    args = parse_args()
    command_log_path = Path(args.command_log).expanduser().resolve()
    manifest_path = Path(args.manifest_json).expanduser().resolve()
    command_log_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    phases = build_phases(args)
    run_started_at = datetime.now(UTC)
    total_commands = 0
    sent_rows: list[dict[str, Any]] = []

    print(f"Starting Belimo test suite '{args.suite}' with test_number={args.test_number}")
    print(f"Command log: {command_log_path}")
    print("Press Ctrl+C to stop.\n")

    with InfluxDBClient(url=args.url, token=args.token, org=args.org, verify_ssl=False) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        for phase in phases:
            if should_stop:
                break

            print(
                f"Phase: {phase.name} | mode={phase.mode} | duration={phase.duration_seconds}s "
                f"| bias={phase.bias} | amplitude={phase.amplitude} | frequency={phase.frequency}"
            )
            phase_started = time.monotonic()
            phase_rows: list[dict[str, Any]] = []

            while not should_stop:
                elapsed = time.monotonic() - phase_started
                if elapsed >= phase.duration_seconds:
                    break

                setpoint = compute_setpoint(phase, elapsed)
                write_command(
                    write_api,
                    bucket=args.bucket,
                    measurement=args.measurement,
                    setpoint_position=setpoint,
                    test_number=args.test_number,
                )

                row = {
                    "logged_at": datetime.now(UTC).isoformat(),
                    "phase_name": phase.name,
                    "mode": phase.mode,
                    "test_number": args.test_number,
                    "setpoint_position_%": setpoint,
                    "bias": phase.bias,
                    "amplitude": phase.amplitude,
                    "frequency": phase.frequency,
                    "elapsed_phase_seconds": round(elapsed, 6),
                }
                phase_rows.append(row)
                total_commands += 1

                print(
                    f"  wrote setpoint={setpoint:6.2f} | phase={phase.name} | "
                    f"elapsed={elapsed:5.1f}s | total_commands={total_commands}"
                )

                time.sleep(args.poll_seconds)

            if phase_rows:
                append_command_log(command_log_path, phase_rows)
                sent_rows.extend(phase_rows)

    write_manifest(
        manifest_path,
        {
            "suite": args.suite,
            "test_number": args.test_number,
            "run_started_at": run_started_at.isoformat(),
            "run_finished_at": datetime.now(UTC).isoformat(),
            "poll_seconds": args.poll_seconds,
            "phase_count": len(phases),
            "total_commands": total_commands,
            "phases": [
                {
                    "name": phase.name,
                    "mode": phase.mode,
                    "duration_seconds": phase.duration_seconds,
                    "bias": phase.bias,
                    "amplitude": phase.amplitude,
                    "frequency": phase.frequency,
                }
                for phase in phases
            ],
        },
    )

    print(f"\nFinished. Wrote {total_commands} commands.")
    print(f"Saved command log to {command_log_path}")
    print(f"Saved manifest to {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
