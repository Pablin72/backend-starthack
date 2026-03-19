#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path


DEFAULT_DEVICE_ID = "BELIMO-8"


CAMPAIGN_RUNS = [
    {
        "test_id": "P1_LOW_HOLD_001",
        "test_number": 1001,
        "waveform": "constant",
        "bias": 20.0,
        "amplitude": 0.0,
        "frequency": 0.0,
        "duration_seconds": 30,
        "capture_duration_seconds": 35,
        "test_purpose": "static hold near closed for settling and noise baseline",
    },
    {
        "test_id": "P1_MID_HOLD_001",
        "test_number": 1002,
        "waveform": "constant",
        "bias": 50.0,
        "amplitude": 0.0,
        "frequency": 0.0,
        "duration_seconds": 30,
        "capture_duration_seconds": 35,
        "test_purpose": "static hold near mid opening for settling and noise baseline",
    },
    {
        "test_id": "P1_HIGH_HOLD_001",
        "test_number": 1003,
        "waveform": "constant",
        "bias": 80.0,
        "amplitude": 0.0,
        "frequency": 0.0,
        "duration_seconds": 30,
        "capture_duration_seconds": 35,
        "test_purpose": "static hold near open for settling and noise baseline",
    },
    {
        "test_id": "P2_LOW_SQUARE_001",
        "test_number": 2001,
        "waveform": "square",
        "bias": 20.0,
        "amplitude": 10.0,
        "frequency": 0.02,
        "duration_seconds": 120,
        "capture_duration_seconds": 130,
        "test_purpose": "step-response identification in low operating region",
    },
    {
        "test_id": "P2_MID_SQUARE_001",
        "test_number": 2002,
        "waveform": "square",
        "bias": 50.0,
        "amplitude": 20.0,
        "frequency": 0.02,
        "duration_seconds": 120,
        "capture_duration_seconds": 130,
        "test_purpose": "step-response identification in mid operating region",
    },
    {
        "test_id": "P2_HIGH_SQUARE_001",
        "test_number": 2003,
        "waveform": "square",
        "bias": 80.0,
        "amplitude": 10.0,
        "frequency": 0.02,
        "duration_seconds": 120,
        "capture_duration_seconds": 130,
        "test_purpose": "step-response identification in high operating region",
    },
    {
        "test_id": "P3_LOW_TRIANGLE_001",
        "test_number": 3001,
        "waveform": "triangle",
        "bias": 20.0,
        "amplitude": 10.0,
        "frequency": 0.02,
        "duration_seconds": 120,
        "capture_duration_seconds": 130,
        "test_purpose": "slow ramp tracking and hysteresis in low region",
    },
    {
        "test_id": "P3_MID_TRIANGLE_001",
        "test_number": 3002,
        "waveform": "triangle",
        "bias": 50.0,
        "amplitude": 20.0,
        "frequency": 0.02,
        "duration_seconds": 120,
        "capture_duration_seconds": 130,
        "test_purpose": "slow ramp tracking and hysteresis in mid region",
    },
    {
        "test_id": "P3_HIGH_TRIANGLE_001",
        "test_number": 3003,
        "waveform": "triangle",
        "bias": 80.0,
        "amplitude": 10.0,
        "frequency": 0.04,
        "duration_seconds": 120,
        "capture_duration_seconds": 130,
        "test_purpose": "faster ramp tracking and hysteresis in high region",
    },
    {
        "test_id": "P4_MID_SINE_LOW_001",
        "test_number": 4001,
        "waveform": "sine",
        "bias": 50.0,
        "amplitude": 10.0,
        "frequency": 0.02,
        "duration_seconds": 180,
        "capture_duration_seconds": 190,
        "test_purpose": "low-frequency smooth dynamic characterization at mid bias",
    },
    {
        "test_id": "P4_MID_SINE_MED_001",
        "test_number": 4002,
        "waveform": "sine",
        "bias": 50.0,
        "amplitude": 10.0,
        "frequency": 0.04,
        "duration_seconds": 180,
        "capture_duration_seconds": 190,
        "test_purpose": "medium-frequency smooth dynamic characterization at mid bias",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Belimo healthy identification campaign.")
    parser.add_argument(
        "--campaign",
        choices=["minimum"],
        default="minimum",
        help="Campaign preset to execute",
    )
    parser.add_argument("--device-id", default=DEFAULT_DEVICE_ID, help="Device identifier written into telemetry metadata")
    parser.add_argument("--poll-seconds", type=float, default=0.125, help="Polling interval for both command and capture scripts")
    parser.add_argument("--lookback-seconds", type=int, default=2, help="Initial capture lookback")
    parser.add_argument("--output-dir", default="data/campaign", help="Directory for telemetry, command logs, and manifests")
    parser.add_argument("--notes", default="", help="Optional notes appended to every telemetry row")
    parser.add_argument("--start-index", type=int, default=1, help="1-based run index to start from")
    parser.add_argument("--end-index", type=int, default=0, help="1-based run index to stop at, 0 means until the end")
    return parser.parse_args()


def build_runs(args: argparse.Namespace) -> list[dict]:
    runs = list(CAMPAIGN_RUNS)
    start_index = max(1, args.start_index)
    end_index = len(runs) if args.end_index <= 0 else min(len(runs), args.end_index)
    return runs[start_index - 1:end_index]


def run_subprocess(command: list[str], *, cwd: Path) -> subprocess.Popen[str]:
    return subprocess.Popen(
        command,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def stream_process_output(process: subprocess.Popen[str], prefix: str) -> int:
    if process.stdout is None:
        return process.wait()

    for line in process.stdout:
        sys.stdout.write(f"{prefix}{line}")
    return process.wait()


def main() -> int:
    args = parse_args()
    root_dir = Path(__file__).resolve().parents[1]
    output_dir = (root_dir / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    runs = build_runs(args)
    if not runs:
        print("No runs selected.")
        return 1

    campaign_manifest = {
        "campaign": args.campaign,
        "device_id": args.device_id,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "run_count": len(runs),
        "runs": [],
    }

    for index, run in enumerate(runs, start=1):
        test_id = run["test_id"]
        print(f"\n=== [{index}/{len(runs)}] Starting {test_id} ===")

        telemetry_path = output_dir / f"{test_id}_telemetry.csv"
        command_log_path = output_dir / f"{test_id}_commands.csv"
        manifest_path = output_dir / f"{test_id}_manifest.json"

        collect_cmd = [
            sys.executable,
            "scripts/collect_belimo_data.py",
            "--output",
            str(telemetry_path),
            "--format",
            "csv",
            "--lookback-seconds",
            str(args.lookback_seconds),
            "--poll-seconds",
            str(args.poll_seconds),
            "--duration-seconds",
            str(run["capture_duration_seconds"]),
            "--device-id",
            args.device_id,
            "--test-id",
            test_id,
            "--waveform-type",
            run["waveform"],
            "--bias",
            str(run["bias"]),
            "--amplitude",
            str(run["amplitude"]),
            "--frequency",
            str(run["frequency"]),
            "--test-purpose",
            run["test_purpose"],
            "--quality-label",
            "unreviewed",
            "--notes",
            args.notes,
        ]

        drive_cmd = [
            sys.executable,
            "scripts/run_belimo_test.py",
            "--suite",
            "single",
            "--test-number",
            str(run["test_number"]),
            "--waveform",
            run["waveform"],
            "--bias",
            str(run["bias"]),
            "--amplitude",
            str(run["amplitude"]),
            "--frequency",
            str(run["frequency"]),
            "--duration-seconds",
            str(run["duration_seconds"]),
            "--poll-seconds",
            str(args.poll_seconds),
            "--command-log",
            str(command_log_path),
            "--manifest-json",
            str(manifest_path),
        ]

        collector = run_subprocess(collect_cmd, cwd=root_dir)
        time.sleep(2.0)
        driver = run_subprocess(drive_cmd, cwd=root_dir)

        driver_exit = stream_process_output(driver, f"[{test_id} driver] ")
        collector_exit = stream_process_output(collector, f"[{test_id} collect] ")

        run_record = dict(run)
        run_record["telemetry_path"] = str(telemetry_path)
        run_record["command_log_path"] = str(command_log_path)
        run_record["manifest_path"] = str(manifest_path)
        run_record["driver_exit_code"] = driver_exit
        run_record["collector_exit_code"] = collector_exit
        campaign_manifest["runs"].append(run_record)

        if driver_exit != 0 or collector_exit != 0:
            campaign_manifest["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            (output_dir / "campaign_manifest.json").write_text(
                json.dumps(campaign_manifest, indent=2, ensure_ascii=True),
                encoding="utf-8",
            )
            print(f"\nRun {test_id} failed. Stopping campaign.")
            return 1

    campaign_manifest["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    (output_dir / "campaign_manifest.json").write_text(
        json.dumps(campaign_manifest, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    print("\nCampaign finished successfully.")
    print(f"Campaign manifest: {output_dir / 'campaign_manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
