#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from api.services.calibration_pipeline import run_calibration


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calibrate a simplified healthy actuator model from campaign CSVs.")
    parser.add_argument(
        "--campaign-dir",
        default="data/campaign",
        help="Directory containing telemetry CSV files from the acquisition campaign",
    )
    parser.add_argument(
        "--output-dir",
        default="data/calibration",
        help="Directory where calibration reports and selected healthy cycles will be written",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_calibration(args.campaign_dir, args.output_dir)

    summary = {
        "position_model": result["position_model"],
        "torque_model": result["torque_model"],
        "temperature_model": result["temperature_model"],
        "repeatability_score": result["diagnostics"]["repeatability_score"],
        "invariant_violations": result["diagnostics"]["invariant_violations"],
        "selected_cycle_count": len(result["selected_cycles"]),
        "output_dir": str(Path(args.output_dir).resolve()),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
