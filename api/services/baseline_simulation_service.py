from __future__ import annotations

import math
from typing import Any

import numpy as np

from api.services.baseline_model_service import load_baseline_report


SUPPORTED_WAVEFORMS = {"constant", "sine", "square", "triangle"}


def get_position_parameters() -> dict[str, Any]:
    report = load_baseline_report()
    position_model = report["position_model"]
    return {
        "model_equation": "dy/dt = clip((u(t-L)-y)/tau_s, -vmax, vmax)",
        "opening": position_model.get("opening", {}),
        "closing": position_model.get("closing", {}),
        "diagnostics": position_model.get("fit_summary", {}),
    }


def get_torque_parameters() -> dict[str, Any]:
    report = load_baseline_report()
    torque_model = report["torque_model"]
    return {
        "model_equation": "tau = tau_0 + k_v * |dy/dt| + k_p * y",
        **torque_model,
    }


def get_temperature_parameters() -> dict[str, Any]:
    report = load_baseline_report()
    temperature_model = report["temperature_model"]
    return {
        "model_equation": "dT/dt = -alpha * (T - T_amb) + beta * torque^2",
        **temperature_model,
    }


def get_envelope_payload(waveform_type: str | None = None) -> dict[str, Any]:
    report = load_baseline_report()
    envelopes = report["envelopes"]
    baselines = report["baseline"]
    if waveform_type:
        if waveform_type not in envelopes:
            raise KeyError(f"Waveform '{waveform_type}' not found in baseline report")
        return {
            waveform_type: _build_single_envelope_payload(waveform_type, baselines[waveform_type], envelopes[waveform_type])
        }
    return {
        key: _build_single_envelope_payload(key, baselines[key], envelopes[key])
        for key in sorted(envelopes.keys())
    }


def _build_single_envelope_payload(waveform_type: str, baseline: dict[str, Any], envelope: dict[str, Any]) -> dict[str, Any]:
    envelope_policy = envelope.get("envelope_policy") or {
        "position": {"mad_scale": 3.0, "min_floor": 0.1},
        "torque": {"mad_scale": 3.0, "min_floor": 0.1},
        "temperature": {"mad_scale": 3.0, "min_floor": 0.1},
    }
    return {
        "waveform_type": waveform_type,
        "reference_trace": {
            "position": baseline["reference_position"],
            "torque": baseline["reference_torque"],
            "temperature": baseline["reference_temperature"],
        },
        "lower_bound": {
            "position": envelope["position_bounds"]["lower"],
            "torque": envelope["torque_bounds"]["lower"],
            "temperature": envelope["temperature_bounds"]["lower"],
        },
        "upper_bound": {
            "position": envelope["position_bounds"]["upper"],
            "torque": envelope["torque_bounds"]["upper"],
            "temperature": envelope["temperature_bounds"]["upper"],
        },
        "envelope_metadata": {
            "construction": "reference trace +/- max(mad_scale*MAD, min_floor) per variable",
            "policy": envelope_policy,
            "position_mad": envelope["position_mad"],
            "torque_mad": envelope["torque_mad"],
            "temperature_mad": envelope["temperature_mad"],
        },
    }


def simulate_from_waveform(payload: dict[str, Any]) -> dict[str, Any]:
    waveform_type = str(payload.get("waveform_type", "")).strip().lower()
    if waveform_type not in SUPPORTED_WAVEFORMS:
        raise ValueError(f"waveform_type must be one of {sorted(SUPPORTED_WAVEFORMS)}")

    duration_seconds = float(payload.get("duration_seconds", 120.0))
    sample_count = int(payload.get("sample_count", 100))
    if sample_count < 2:
        raise ValueError("sample_count must be at least 2")

    timestamps = payload.get("timestamps")
    if timestamps:
        timeline = np.asarray([float(value) for value in timestamps], dtype=float)
    else:
        timeline = np.linspace(0.0, duration_seconds, sample_count)

    bias = float(payload.get("bias", 50.0))
    amplitude = float(payload.get("amplitude", 0.0))
    frequency = float(payload.get("frequency", 0.0))

    command_values = generate_command_values(
        waveform_type=waveform_type,
        timeline=timeline,
        bias=bias,
        amplitude=amplitude,
        frequency=frequency,
    )
    return simulate_from_command_series(
        {
            "waveform_type": waveform_type,
            "timestamps": timeline.tolist(),
            "command_values": command_values,
            "metadata": {
                "bias": bias,
                "amplitude": amplitude,
                "frequency": frequency,
            },
        }
    )


def simulate_from_command_series(payload: dict[str, Any]) -> dict[str, Any]:
    report = load_baseline_report()
    waveform_type = str(payload.get("waveform_type", "")).strip().lower()
    if waveform_type not in report["baseline"]:
        raise ValueError(f"waveform_type must be one of {sorted(report['baseline'].keys())}")

    timestamps = np.asarray([float(value) for value in payload.get("timestamps", [])], dtype=float)
    command_values = np.asarray([float(value) for value in payload.get("command_values", [])], dtype=float)
    if timestamps.size < 2 or command_values.size != timestamps.size:
        raise ValueError("timestamps and command_values must be present and have the same length >= 2")

    normalized_phase = normalize_timeline(timestamps)
    baseline = report["baseline"][waveform_type]
    envelopes = report["envelopes"][waveform_type]
    position_reference, velocity_reference = simulate_position_trace(
        timestamps=timestamps,
        command_values=command_values,
        position_model=report["position_model"],
        initial_position=float(payload.get("initial_position", command_values[0])),
    )
    torque_reference = simulate_torque_trace(
        position_trace=position_reference,
        velocity_trace=velocity_reference,
        torque_model=report["torque_model"],
    )
    temperature_reference = simulate_temperature_trace(
        timestamps=timestamps,
        torque_trace=torque_reference,
        temperature_model=report["temperature_model"],
        initial_temperature=float(payload.get("initial_temperature", baseline["reference_temperature"][0])),
    )

    position_lower = resample_trace(envelopes["position_bounds"]["lower"], normalized_phase)
    position_upper = resample_trace(envelopes["position_bounds"]["upper"], normalized_phase)
    torque_lower = resample_trace(envelopes["torque_bounds"]["lower"], normalized_phase)
    torque_upper = resample_trace(envelopes["torque_bounds"]["upper"], normalized_phase)
    temperature_lower = resample_trace(envelopes["temperature_bounds"]["lower"], normalized_phase)
    temperature_upper = resample_trace(envelopes["temperature_bounds"]["upper"], normalized_phase)

    directions = infer_direction_labels(position_reference)
    return {
        "waveform_type": waveform_type,
        "timestamps": timestamps.tolist(),
        "command_values": command_values.tolist(),
        "baseline_traces": {
            "position": round_trace(position_reference),
            "torque": round_trace(torque_reference),
            "temperature": round_trace(temperature_reference),
        },
        "tolerance_bounds": {
            "position": {"lower": round_trace(position_lower), "upper": round_trace(position_upper)},
            "torque": {"lower": round_trace(torque_lower), "upper": round_trace(torque_upper)},
            "temperature": {"lower": round_trace(temperature_lower), "upper": round_trace(temperature_upper)},
        },
        "direction_labels": directions,
        "metadata": {
            "simulation_mode": "calibrated_model_from_command_series",
            "waveform_type": waveform_type,
            "input_metadata": payload.get("metadata", {}),
        },
    }


def normalize_timeline(timestamps: np.ndarray) -> np.ndarray:
    if timestamps.size < 2:
        return np.zeros_like(timestamps)
    start = float(timestamps[0])
    end = float(timestamps[-1])
    if math.isclose(start, end):
        return np.zeros_like(timestamps)
    return (timestamps - start) / (end - start)


def resample_trace(reference: list[float], normalized_phase: np.ndarray) -> np.ndarray:
    source_x = np.linspace(0.0, 1.0, len(reference))
    return np.interp(normalized_phase, source_x, np.asarray(reference, dtype=float))


def round_trace(values: np.ndarray) -> list[float]:
    return [round(float(value), 6) for value in values]


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


def generate_command_values(
    *,
    waveform_type: str,
    timeline: np.ndarray,
    bias: float,
    amplitude: float,
    frequency: float,
) -> list[float]:
    if waveform_type == "constant" or frequency == 0.0:
        return [round(float(bias), 6) for _ in timeline]

    values = []
    for t in timeline:
        angular = 2.0 * math.pi * frequency * float(t)
        if waveform_type == "sine":
            raw = bias + (amplitude * math.sin(angular))
        elif waveform_type == "triangle":
            raw = bias + (amplitude * (2.0 / math.pi) * math.asin(math.sin(angular)))
        elif waveform_type == "square":
            raw = bias + (amplitude if math.sin(angular) >= 0 else -amplitude)
        else:
            raw = bias
        values.append(round(max(0.0, min(100.0, float(raw))), 6))
    return values


def simulate_position_trace(
    *,
    timestamps: np.ndarray,
    command_values: np.ndarray,
    position_model: dict[str, Any],
    initial_position: float,
) -> tuple[np.ndarray, np.ndarray]:
    opening = position_model.get("opening", {})
    closing = position_model.get("closing", {})
    position = np.zeros_like(command_values, dtype=float)
    velocity = np.zeros_like(command_values, dtype=float)
    position[0] = float(initial_position)

    median_dt = float(np.median(np.diff(timestamps))) if timestamps.size >= 2 else 0.1
    delay_open_samples = max(0, int(round(float(opening.get("delay_L_seconds", 0.0) or 0.0) / max(median_dt, 1e-3))))
    delay_close_samples = max(0, int(round(float(closing.get("delay_L_seconds", 0.0) or 0.0) / max(median_dt, 1e-3))))

    for index in range(1, len(command_values)):
        dt = max(float(timestamps[index] - timestamps[index - 1]), 1e-3)
        current = position[index - 1]
        preview_target = command_values[index]
        is_opening = preview_target >= current
        params = opening if is_opening else closing
        delay_samples = delay_open_samples if is_opening else delay_close_samples
        delayed_index = max(0, index - delay_samples)
        delayed_target = command_values[delayed_index]
        tau = max(float(params.get("time_constant_tau_s_seconds", 1.0) or 1.0), 1e-3)
        vmax = max(float(params.get("max_velocity_vmax_percent_per_second", 1.0) or 1.0), 1e-3)
        desired_velocity = (delayed_target - current) / tau
        clipped_velocity = max(-vmax, min(vmax, desired_velocity))
        velocity[index] = clipped_velocity
        position[index] = max(0.0, min(100.0, current + (clipped_velocity * dt)))

    return position, velocity


def simulate_torque_trace(
    *,
    position_trace: np.ndarray,
    velocity_trace: np.ndarray,
    torque_model: dict[str, Any],
) -> np.ndarray:
    torque = np.zeros_like(position_trace, dtype=float)
    opening = torque_model.get("opening", {})
    closing = torque_model.get("closing", {})

    for index, (position, velocity) in enumerate(zip(position_trace, velocity_trace)):
        params = opening if velocity >= 0 else closing
        tau_0 = float(params.get("tau_0", 0.0) or 0.0)
        k_v = float(params.get("k_v", 0.0) or 0.0)
        k_p = float(params.get("k_p", 0.0) or 0.0)
        torque[index] = tau_0 + (k_v * abs(velocity)) + (k_p * position)
    return torque


def simulate_temperature_trace(
    *,
    timestamps: np.ndarray,
    torque_trace: np.ndarray,
    temperature_model: dict[str, Any],
    initial_temperature: float,
) -> np.ndarray:
    alpha = float(temperature_model.get("alpha", 0.0) or 0.0)
    beta = float(temperature_model.get("beta", 0.0) or 0.0)
    ambient = float(temperature_model.get("ambient_temperature", initial_temperature) or initial_temperature)
    temperature = np.zeros_like(torque_trace, dtype=float)
    temperature[0] = float(initial_temperature)

    for index in range(1, len(torque_trace)):
        dt = max(float(timestamps[index] - timestamps[index - 1]), 1e-3)
        current = temperature[index - 1]
        d_temp = (-alpha * (current - ambient)) + (beta * (float(torque_trace[index - 1]) ** 2))
        temperature[index] = current + (d_temp * dt)

    return temperature
