from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from api.services.baseline_model_service import get_calibration_dir, load_baseline_report
from api.services.baseline_simulation_service import simulate_from_command_series, simulate_from_waveform
from api.services.evaluation_response_service import classify_status, summarize_overall, summarize_variable


REQUIRED_MIN_SAMPLES = 2


def evaluate_position(payload: dict[str, Any]) -> dict[str, Any]:
    prepared = prepare_series_payload(payload)
    measured = extract_signal(prepared["frame"], ["position", "position_pct", "feedback_position_%"])
    if measured is None:
        raise ValueError("telemetry_series must include 'position', 'position_pct', or 'feedback_position_%'")
    baseline = build_baseline(prepared, include_command_alignment=True)
    direction_labels = infer_direction_labels(measured)
    position_result = evaluate_signal(
        variable_name="position",
        measured=measured,
        expected=np.asarray(baseline["baseline_traces"]["position"], dtype=float),
        lower=np.asarray(baseline["tolerance_bounds"]["position"]["lower"], dtype=float),
        upper=np.asarray(baseline["tolerance_bounds"]["position"]["upper"], dtype=float),
        direction_labels=direction_labels,
    )
    return build_single_variable_response(
        device_id=prepared["device_id"],
        timestamps=prepared["timestamps"],
        variable_name="position",
        result=position_result,
        invalid_sample_count=prepared["invalid_sample_count"],
        notes=prepared["notes"],
        alignment=baseline["alignment"],
    )


def evaluate_torque(payload: dict[str, Any]) -> dict[str, Any]:
    prepared = prepare_series_payload(payload)
    measured_signed = extract_signal(prepared["frame"], ["torque_signed", "motor_torque_Nmm", "torque"])
    if measured_signed is None:
        raise ValueError("telemetry_series must include 'torque_signed', 'motor_torque_Nmm', or 'torque'")
    measured = np.abs(measured_signed)
    baseline = build_baseline(prepared, include_command_alignment=True)
    direction_labels = infer_direction_labels(
        extract_signal(prepared["frame"], ["position", "position_pct", "feedback_position_%"])
        if extract_signal(prepared["frame"], ["position", "position_pct", "feedback_position_%"]) is not None
        else np.asarray(baseline["baseline_traces"]["position"], dtype=float)
    )
    torque_result = evaluate_signal(
        variable_name="torque",
        measured=measured,
        expected=np.asarray(baseline["baseline_traces"]["torque"], dtype=float),
        lower=np.asarray(baseline["tolerance_bounds"]["torque"]["lower"], dtype=float),
        upper=np.asarray(baseline["tolerance_bounds"]["torque"]["upper"], dtype=float),
        direction_labels=direction_labels,
    )
    return build_single_variable_response(
        device_id=prepared["device_id"],
        timestamps=prepared["timestamps"],
        variable_name="torque",
        result=torque_result,
        invalid_sample_count=prepared["invalid_sample_count"],
        notes=prepared["notes"],
        alignment=baseline["alignment"],
    )


def evaluate_temperature(payload: dict[str, Any]) -> dict[str, Any]:
    prepared = prepare_series_payload(payload)
    measured = extract_signal(prepared["frame"], ["temperature_c", "internal_temperature_deg_C", "temperature"])
    if measured is None:
        raise ValueError("telemetry_series must include 'temperature_c', 'internal_temperature_deg_C', or 'temperature'")
    baseline = build_baseline(prepared, include_command_alignment=False)
    torque_series = extract_signal(prepared["frame"], ["torque_signed", "motor_torque_Nmm", "torque"])
    direction_labels = infer_direction_labels(
        extract_signal(prepared["frame"], ["position", "position_pct", "feedback_position_%"])
        if extract_signal(prepared["frame"], ["position", "position_pct", "feedback_position_%"]) is not None
        else np.asarray(baseline["baseline_traces"]["position"], dtype=float)
    )
    temperature_result = evaluate_signal(
        variable_name="temperature",
        measured=measured,
        expected=np.asarray(baseline["baseline_traces"]["temperature"], dtype=float),
        lower=np.asarray(baseline["tolerance_bounds"]["temperature"]["lower"], dtype=float),
        upper=np.asarray(baseline["tolerance_bounds"]["temperature"]["upper"], dtype=float),
        direction_labels=direction_labels,
    )
    if torque_series is not None:
        temperature_result["summary"]["notes"] = "Expected thermal response was compared against torque-driven healthy baseline behavior."
    return build_single_variable_response(
        device_id=prepared["device_id"],
        timestamps=prepared["timestamps"],
        variable_name="temperature",
        result=temperature_result,
        invalid_sample_count=prepared["invalid_sample_count"],
        notes=prepared["notes"],
        alignment=baseline["alignment"],
    )


def evaluate_combined(payload: dict[str, Any]) -> dict[str, Any]:
    prepared = prepare_series_payload(payload)
    baseline = build_baseline(prepared, include_command_alignment=True)
    position = extract_signal(prepared["frame"], ["position", "position_pct", "feedback_position_%"])
    torque_signed = extract_signal(prepared["frame"], ["torque_signed", "motor_torque_Nmm", "torque"])
    torque = None if torque_signed is None else np.abs(torque_signed)
    temperature = extract_signal(prepared["frame"], ["temperature_c", "internal_temperature_deg_C", "temperature"])

    direction_source = position if position is not None else np.asarray(baseline["baseline_traces"]["position"], dtype=float)
    direction_labels = infer_direction_labels(direction_source)

    variable_results = {
        "position": evaluate_signal(
            variable_name="position",
            measured=position,
            expected=np.asarray(baseline["baseline_traces"]["position"], dtype=float),
            lower=np.asarray(baseline["tolerance_bounds"]["position"]["lower"], dtype=float),
            upper=np.asarray(baseline["tolerance_bounds"]["position"]["upper"], dtype=float),
            direction_labels=direction_labels,
        ),
        "torque": evaluate_signal(
            variable_name="torque",
            measured=torque,
            expected=np.asarray(baseline["baseline_traces"]["torque"], dtype=float),
            lower=np.asarray(baseline["tolerance_bounds"]["torque"]["lower"], dtype=float),
            upper=np.asarray(baseline["tolerance_bounds"]["torque"]["upper"], dtype=float),
            direction_labels=direction_labels,
        ),
        "temperature": evaluate_signal(
            variable_name="temperature",
            measured=temperature,
            expected=np.asarray(baseline["baseline_traces"]["temperature"], dtype=float),
            lower=np.asarray(baseline["tolerance_bounds"]["temperature"]["lower"], dtype=float),
            upper=np.asarray(baseline["tolerance_bounds"]["temperature"]["upper"], dtype=float),
            direction_labels=direction_labels,
        ),
    }

    summary = summarize_overall(variable_results)
    return {
        "device_id": prepared["device_id"],
        "timestamps": round_trace(prepared["timestamps"]),
        "position": format_variable_output(variable_results["position"]),
        "torque": format_variable_output(variable_results["torque"]),
        "temperature": format_variable_output(variable_results["temperature"]),
        "summary": {
            **summary,
            "sample_count": int(len(prepared["frame"])),
            "invalid_sample_count": prepared["invalid_sample_count"],
            "notes": prepared["notes"],
        },
        "alignment": baseline["alignment"],
    }


def prepare_series_payload(payload: dict[str, Any]) -> dict[str, Any]:
    telemetry_series = payload.get("telemetry_series")
    if not isinstance(telemetry_series, list) or not telemetry_series:
        raise ValueError("telemetry_series must be a non-empty array")

    frame = pd.DataFrame(telemetry_series)
    if "timestamp" not in frame.columns:
        raise ValueError("Each telemetry sample must include a timestamp")

    invalid_sample_count = int(frame["timestamp"].isna().sum()) if "timestamp" in frame.columns else 0
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    invalid_sample_count += int(frame["timestamp"].isna().sum())
    frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"])
    if len(frame) < REQUIRED_MIN_SAMPLES:
        raise ValueError("telemetry_series must contain at least two valid samples")

    frame["elapsed_seconds"] = (frame["timestamp"] - frame["timestamp"].iloc[0]).dt.total_seconds()
    dt = frame["elapsed_seconds"].diff().dropna()
    if dt.empty or (dt <= 0).any():
        raise ValueError("telemetry_series must contain strictly increasing timestamps")

    notes = []
    if invalid_sample_count > 0:
        notes.append("Some samples were dropped due to invalid or duplicate timestamps.")

    return {
        "device_id": str(payload.get("device_id", "")).strip() or "unknown-device",
        "frame": frame.reset_index(drop=True),
        "timestamps": frame["elapsed_seconds"].to_numpy(dtype=float),
        "invalid_sample_count": invalid_sample_count,
        "notes": notes,
        "waveform_type": str(payload.get("waveform_type", "")).strip().lower(),
        "payload": payload,
    }


def build_baseline(prepared: dict[str, Any], *, include_command_alignment: bool) -> dict[str, Any]:
    payload = prepared["payload"]
    timestamps = prepared["timestamps"]
    baseline_payload = None

    command_series = payload.get("command_series")
    if isinstance(command_series, list) and command_series:
        command_frame = pd.DataFrame(command_series)
        if "timestamp" not in command_frame.columns:
            raise ValueError("Each command sample must include a timestamp")
        command_frame["timestamp"] = pd.to_datetime(command_frame["timestamp"], utc=True, errors="coerce")
        command_frame = command_frame.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"])
        if len(command_frame) < REQUIRED_MIN_SAMPLES:
            raise ValueError("command_series must contain at least two valid samples")
        command_time = (command_frame["timestamp"] - command_frame["timestamp"].iloc[0]).dt.total_seconds().to_numpy(dtype=float)
        command_values = extract_signal(command_frame, ["commanded_position", "setpoint_position_%", "command"])
        if command_values is None:
            raise ValueError("command_series must include 'commanded_position', 'setpoint_position_%', or 'command'")
        interpolated_commands = np.interp(timestamps, command_time, command_values)
        baseline_payload = simulate_from_command_series(
            {
                "waveform_type": prepared["waveform_type"] or str(payload.get("waveform", {}).get("waveform_type", "")).strip().lower(),
                "timestamps": timestamps.tolist(),
                "command_values": interpolated_commands.tolist(),
                "metadata": payload.get("metadata", {}),
                "initial_position": extract_signal(prepared["frame"], ["position", "position_pct", "feedback_position_%"])[0]
                if extract_signal(prepared["frame"], ["position", "position_pct", "feedback_position_%"]) is not None
                else interpolated_commands[0],
                "initial_temperature": extract_signal(prepared["frame"], ["temperature_c", "internal_temperature_deg_C", "temperature"])[0]
                if extract_signal(prepared["frame"], ["temperature_c", "internal_temperature_deg_C", "temperature"]) is not None
                else None,
            }
        )
        baseline_payload["metadata"]["source"] = "command_series"
    else:
        waveform = payload.get("waveform") or {}
        waveform_type = prepared["waveform_type"] or str(waveform.get("waveform_type", "")).strip().lower()
        if not waveform_type:
            raise ValueError("Provide waveform_type and waveform metadata or a command_series")
        baseline_payload = simulate_from_waveform(
            {
                "waveform_type": waveform_type,
                "timestamps": timestamps.tolist(),
                "bias": waveform.get("bias", payload.get("bias", 50.0)),
                "amplitude": waveform.get("amplitude", payload.get("amplitude", 0.0)),
                "frequency": waveform.get("frequency", payload.get("frequency", 0.0)),
                "metadata": payload.get("metadata", {}),
            }
        )
        baseline_payload["metadata"]["source"] = "waveform_metadata"

    measured_position = extract_signal(prepared["frame"], ["position", "position_pct", "feedback_position_%"])
    alignment = {"applied": False, "offset_samples": 0, "offset_seconds_estimate": 0.0, "method": "none"}
    if include_command_alignment and payload.get("align", False) and measured_position is not None:
        offset_samples = estimate_alignment_offset(np.asarray(baseline_payload["command_values"], dtype=float), measured_position)
        alignment = {
            "applied": True,
            "offset_samples": int(offset_samples),
            "offset_seconds_estimate": estimate_offset_seconds(timestamps, offset_samples),
            "method": "derivative_cross_correlation",
        }
        baseline_payload = shift_baseline_payload(baseline_payload, offset_samples)

    baseline_payload["alignment"] = alignment
    return baseline_payload


def shift_baseline_payload(payload: dict[str, Any], shift: int) -> dict[str, Any]:
    shifted = {
        **payload,
        "baseline_traces": {
            key: round_trace(shift_array(np.asarray(values, dtype=float), shift))
            for key, values in payload["baseline_traces"].items()
        },
        "tolerance_bounds": {
            key: {
                "lower": round_trace(shift_array(np.asarray(value["lower"], dtype=float), shift)),
                "upper": round_trace(shift_array(np.asarray(value["upper"], dtype=float), shift)),
            }
            for key, value in payload["tolerance_bounds"].items()
        },
    }
    return shifted


def evaluate_signal(
    *,
    variable_name: str,
    measured: np.ndarray | None,
    expected: np.ndarray,
    lower: np.ndarray,
    upper: np.ndarray,
    direction_labels: list[str],
) -> dict[str, Any]:
    if measured is None:
        return empty_variable_result()

    residual = measured - expected
    violations = ((measured < lower) | (measured > upper))
    envelope_violation_pct = float(violations.mean()) if len(violations) else 0.0
    half_width = np.maximum((upper - lower) / 2.0, 0.1)
    normalized_residual = float(np.median(np.abs(residual) / half_width))
    median_abs_residual = float(np.median(np.abs(residual)))
    rmse = float(np.sqrt(np.mean(np.square(residual))))
    max_abs = float(np.max(np.abs(residual)))
    dominant_direction = infer_dominant_direction(direction_labels, residual)
    trend = infer_residual_trend(residual)
    status = classify_status(violation_pct=envelope_violation_pct, normalized_residual=normalized_residual)
    insight = summarize_variable(
        variable_name=variable_name,
        violation_pct=envelope_violation_pct,
        normalized_residual=normalized_residual,
        trend=trend,
        dominant_direction=dominant_direction,
    )

    return {
        "measured": measured,
        "expected": expected,
        "lower_bound": lower,
        "upper_bound": upper,
        "residual": residual,
        "envelope_violation_flags": violations.tolist(),
        "summary": {
            "available": True,
            "sample_count": int(len(measured)),
            "invalid_sample_count": 0,
            "median_absolute_residual": round(median_abs_residual, 6),
            "rmse": round(rmse, 6),
            "max_absolute_deviation": round(max_abs, 6),
            "envelope_violation_pct": round(envelope_violation_pct * 100.0, 6),
            "violation_count": int(violations.sum()),
            "normalized_median_abs_residual": round(normalized_residual, 6),
            "status": status,
            "insight": insight,
            "direction": dominant_direction,
        },
    }


def empty_variable_result() -> dict[str, Any]:
    return {
        "measured": None,
        "expected": None,
        "lower_bound": None,
        "upper_bound": None,
        "residual": None,
        "envelope_violation_flags": [],
        "summary": {
            "available": False,
            "sample_count": 0,
            "invalid_sample_count": 0,
            "median_absolute_residual": None,
            "rmse": None,
            "max_absolute_deviation": None,
            "envelope_violation_pct": 0.0,
            "violation_count": 0,
            "normalized_median_abs_residual": 0.0,
            "status": "warning",
            "insight": "Required telemetry field is missing for this evaluator.",
            "direction": None,
        },
    }


def build_single_variable_response(
    *,
    device_id: str,
    timestamps: np.ndarray,
    variable_name: str,
    result: dict[str, Any],
    invalid_sample_count: int,
    notes: list[str],
    alignment: dict[str, Any],
) -> dict[str, Any]:
    response = {
        "device_id": device_id,
        "timestamps": round_trace(timestamps),
        variable_name: format_variable_output(result),
        "summary": {
            "status": result["summary"]["status"],
            "insight": result["summary"]["insight"],
            "sample_count": result["summary"]["sample_count"],
            "invalid_sample_count": invalid_sample_count,
            "notes": notes,
        },
        "alignment": alignment,
    }
    return response


def format_variable_output(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "measured": optional_round_trace(result["measured"]),
        "expected": optional_round_trace(result["expected"]),
        "lower_bound": optional_round_trace(result["lower_bound"]),
        "upper_bound": optional_round_trace(result["upper_bound"]),
        "residual": optional_round_trace(result["residual"]),
        "envelope_violation_flags": result["envelope_violation_flags"],
        "envelope_violation_pct": result["summary"]["envelope_violation_pct"],
        "status": result["summary"]["status"],
        "insight": result["summary"]["insight"],
        "direction": result["summary"]["direction"],
        "metrics": {
            "median_absolute_residual": result["summary"]["median_absolute_residual"],
            "rmse": result["summary"]["rmse"],
            "max_absolute_deviation": result["summary"]["max_absolute_deviation"],
            "violation_count": result["summary"]["violation_count"],
            "sample_count": result["summary"]["sample_count"],
            "invalid_sample_count": result["summary"]["invalid_sample_count"],
            "normalized_median_abs_residual": result["summary"]["normalized_median_abs_residual"],
        },
    }


def extract_signal(frame: pd.DataFrame, candidate_columns: list[str]) -> np.ndarray | None:
    for column in candidate_columns:
        if column in frame.columns:
            series = pd.to_numeric(frame[column], errors="coerce")
            if series.notna().any():
                return series.to_numpy(dtype=float)
    return None


def infer_direction_labels(position_values: np.ndarray) -> list[str]:
    derivative = np.diff(position_values, prepend=position_values[0])
    labels = []
    for value in derivative:
        if value > 0.02:
            labels.append("opening")
        elif value < -0.02:
            labels.append("closing")
        else:
            labels.append("steady")
    return labels


def infer_dominant_direction(direction_labels: list[str], residual: np.ndarray) -> str | None:
    if not direction_labels:
        return None
    label_array = np.asarray(direction_labels)
    strength = {}
    for direction in ("opening", "closing"):
        mask = label_array == direction
        if np.any(mask):
            strength[direction] = float(np.median(np.abs(residual[mask])))
    if not strength:
        return None
    return max(strength, key=strength.get)


def infer_residual_trend(residual: np.ndarray) -> str:
    return "high" if float(np.median(residual)) >= 0 else "low"


def estimate_alignment_offset(command_values: np.ndarray, measured_position: np.ndarray) -> int:
    command_derivative = np.diff(command_values, prepend=command_values[0])
    position_derivative = np.diff(measured_position, prepend=measured_position[0])
    command_derivative = command_derivative - np.mean(command_derivative)
    position_derivative = position_derivative - np.mean(position_derivative)
    max_shift = max(1, min(len(command_values) // 10, 30))
    best_shift = 0
    best_score = -np.inf
    for shift in range(-max_shift, max_shift + 1):
        shifted = shift_array(command_derivative, shift)
        score = float(np.dot(shifted, position_derivative))
        if score > best_score:
            best_score = score
            best_shift = shift
    return best_shift


def estimate_offset_seconds(timestamps: np.ndarray, offset_samples: int) -> float:
    if len(timestamps) < 2 or offset_samples == 0:
        return 0.0
    dt = float(np.median(np.diff(timestamps)))
    return round(offset_samples * dt, 6)


def shift_array(values: np.ndarray, shift: int) -> np.ndarray:
    if shift == 0:
        return values.copy()
    shifted = np.empty_like(values)
    if shift > 0:
        shifted[:shift] = values[0]
        shifted[shift:] = values[:-shift]
    else:
        shifted[shift:] = values[-1]
        shifted[:shift] = values[-shift:]
    return shifted


def round_trace(values: np.ndarray) -> list[float]:
    return [round(float(value), 6) for value in values]


def optional_round_trace(values: np.ndarray | None) -> list[float] | None:
    if values is None:
        return None
    return round_trace(values)


def get_model_metadata() -> dict[str, Any]:
    report = load_baseline_report()
    return {
        "position_model": report["position_model"],
        "torque_model": report["torque_model"],
        "temperature_model": report["temperature_model"],
        "envelope_availability": {key: True for key in sorted(report.get("envelopes", {}).keys())},
        "diagnostics_summary": report["diagnostics"],
        "artifact_source": str(get_calibration_dir()),
    }


def get_model_readiness() -> dict[str, Any]:
    metadata = get_model_metadata()
    return {
        "api_alive": True,
        "model_artifacts_loaded": True,
        "evaluators_ready": {
            "position": True,
            "torque": True,
            "temperature": True,
            "combined": True,
        },
        "artifact_source": metadata["artifact_source"],
    }
