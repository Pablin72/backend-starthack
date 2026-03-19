from __future__ import annotations

from datetime import datetime
from statistics import fmean, pvariance
from typing import Any


EPSILON = 1e-9


def normalize_sample(raw_sample: dict[str, Any]) -> dict[str, Any]:
    required_fields = ["timestamp", "device_id", "position", "torque", "temperature", "power"]
    missing_fields = [field for field in required_fields if field not in raw_sample]
    if missing_fields:
        raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

    normalized = {
        "timestamp": _normalize_timestamp(raw_sample["timestamp"]),
        "device_id": str(raw_sample["device_id"]).strip(),
        "position": float(raw_sample["position"]),
        "torque": float(raw_sample["torque"]),
        "temperature": float(raw_sample["temperature"]),
        "power": float(raw_sample["power"]),
    }

    if not normalized["device_id"]:
        raise ValueError("device_id cannot be empty")

    setpoint = raw_sample.get("setpoint")
    normalized["setpoint"] = None if setpoint is None else float(setpoint)
    return normalized


def compute_features(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if len(samples) < 2:
        raise ValueError("At least two samples are required to compute features")

    ordered_samples = sorted(samples, key=lambda sample: sample["timestamp"])
    first_sample = ordered_samples[0]
    last_sample = ordered_samples[-1]

    duration_seconds = max(
        0.0,
        (_parse_timestamp(last_sample["timestamp"]) - _parse_timestamp(first_sample["timestamp"])).total_seconds(),
    )
    delta_position = last_sample["position"] - first_sample["position"]
    movement_speed = abs(delta_position) / duration_seconds if duration_seconds > EPSILON else 0.0

    position_values = [sample["position"] for sample in ordered_samples]
    torque_values = [sample["torque"] for sample in ordered_samples]
    temperature_values = [sample["temperature"] for sample in ordered_samples]
    power_values = [sample["power"] for sample in ordered_samples]

    moving_torque_values = _movement_torque_values(ordered_samples)
    oscillation_score = _oscillation_score(ordered_samples)

    latest_position = last_sample["position"]
    latest_torque = last_sample["torque"]

    return {
        "delta_position": round(delta_position, 6),
        "movement_duration": round(duration_seconds, 6),
        "movement_speed": round(movement_speed, 6),
        "torque_per_position": _safe_divide(latest_torque, latest_position),
        "avg_torque_movement": round(fmean(moving_torque_values), 6) if moving_torque_values else None,
        "torque_variance": round(_variance_or_zero(torque_values), 6),
        "temp_rate": _safe_divide(temperature_values[-1] - temperature_values[0], duration_seconds),
        "temp_vs_torque_ratio": _safe_divide(temperature_values[-1], latest_torque),
        "position_variance": round(_variance_or_zero(position_values), 6),
        "oscillation_score": round(oscillation_score, 6),
        "energy_per_movement": round(fmean(power_values) * duration_seconds, 6),
    }


def update_baseline(
    current_baseline: dict[str, Any] | None,
    *,
    latest_features: dict[str, Any],
    latest_temperature: float,
) -> dict[str, Any]:
    current_count = 0 if current_baseline is None else int(current_baseline["sample_count"])
    next_count = current_count + 1

    previous_torque_per_position = None if current_baseline is None else current_baseline["avg_torque_per_position"]
    previous_movement_speed = None if current_baseline is None else current_baseline["typical_movement_speed"]
    previous_temp_min = None if current_baseline is None else current_baseline["normal_temperature_min"]
    previous_temp_max = None if current_baseline is None else current_baseline["normal_temperature_max"]

    return {
        "sample_count": next_count,
        "avg_torque_per_position": _running_average(
            previous_torque_per_position,
            latest_features.get("torque_per_position"),
            current_count,
        ),
        "typical_movement_speed": _running_average(
            previous_movement_speed,
            latest_features.get("movement_speed"),
            current_count,
        ),
        "normal_temperature_min": latest_temperature if previous_temp_min is None else min(previous_temp_min, latest_temperature),
        "normal_temperature_max": latest_temperature if previous_temp_max is None else max(previous_temp_max, latest_temperature),
    }


def _normalize_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp must be a non-empty ISO 8601 string")
    return _parse_timestamp(value).isoformat()


def _parse_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def _movement_torque_values(samples: list[dict[str, Any]]) -> list[float]:
    values: list[float] = []
    for previous_sample, current_sample in zip(samples, samples[1:]):
        if abs(current_sample["position"] - previous_sample["position"]) > EPSILON:
            values.append(current_sample["torque"])
    return values


def _oscillation_score(samples: list[dict[str, Any]]) -> float:
    directions: list[int] = []
    for previous_sample, current_sample in zip(samples, samples[1:]):
        delta = current_sample["position"] - previous_sample["position"]
        if abs(delta) <= EPSILON:
            continue
        directions.append(1 if delta > 0 else -1)

    if len(directions) < 2:
        return 0.0

    sign_changes = sum(1 for previous, current in zip(directions, directions[1:]) if previous != current)
    return sign_changes / (len(directions) - 1)


def _variance_or_zero(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    return float(pvariance(values))


def _safe_divide(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or abs(denominator) <= EPSILON:
        return None
    return round(numerator / denominator, 6)


def _running_average(previous_average: float | None, new_value: float | None, current_count: int) -> float | None:
    if new_value is None:
        return previous_average
    if previous_average is None or current_count <= 0:
        return round(new_value, 6)
    updated_value = ((previous_average * current_count) + new_value) / (current_count + 1)
    return round(updated_value, 6)
