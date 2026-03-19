from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from api.services.calibration_pipeline import run_calibration


DEFAULT_CALIBRATION_DIR = Path(__file__).resolve().parents[2] / "data" / "calibration"
DEFAULT_CAMPAIGN_DIR = Path(__file__).resolve().parents[2] / "data" / "campaign"


def get_calibration_dir() -> Path:
    configured = (os.environ.get("BASELINE_CALIBRATION_DIR") or "").strip()
    return Path(configured) if configured else DEFAULT_CALIBRATION_DIR


def get_campaign_dir() -> Path:
    configured = (os.environ.get("BASELINE_CAMPAIGN_DIR") or "").strip()
    return Path(configured) if configured else DEFAULT_CAMPAIGN_DIR


def get_report_path() -> Path:
    return get_calibration_dir() / "healthy_model_report.json"


def load_baseline_report() -> dict[str, Any]:
    report_path = get_report_path()
    if not report_path.exists():
        raise FileNotFoundError(f"Baseline report not found at {report_path}")
    return json.loads(report_path.read_text(encoding="utf-8"))


def get_baseline_summary() -> dict[str, Any]:
    report = load_baseline_report()
    report_path = get_report_path()
    return {
        "report_generated_at": report_path.stat().st_mtime,
        "position_model": report["position_model"],
        "torque_model": report["torque_model"],
        "temperature_model": report["temperature_model"],
        "diagnostics": report["diagnostics"],
        "selected_cycle_count": len(report.get("selected_cycles", [])),
    }


def get_waveform_baseline(waveform_type: str) -> dict[str, Any]:
    report = load_baseline_report()
    baseline = report.get("baseline", {})
    envelopes = report.get("envelopes", {})
    if waveform_type not in baseline:
        raise KeyError(f"Waveform '{waveform_type}' not found in baseline report")
    return {
        "waveform_type": waveform_type,
        "baseline": baseline[waveform_type],
        "envelope": envelopes.get(waveform_type),
    }


def recalibrate_baseline(*, campaign_dir: str | None = None, output_dir: str | None = None) -> dict[str, Any]:
    source_dir = Path(campaign_dir) if campaign_dir else get_campaign_dir()
    target_dir = Path(output_dir) if output_dir else get_calibration_dir()
    return run_calibration(source_dir, target_dir)
