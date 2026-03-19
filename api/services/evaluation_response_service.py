from __future__ import annotations

from typing import Any


def classify_status(*, violation_pct: float, normalized_residual: float) -> str:
    if violation_pct >= 0.20 or normalized_residual >= 1.50:
        return "critical"
    if violation_pct >= 0.05 or normalized_residual >= 0.80:
        return "warning"
    return "normal"


def summarize_variable(
    *,
    variable_name: str,
    violation_pct: float,
    normalized_residual: float,
    trend: str,
    dominant_direction: str | None = None,
) -> str:
    status = classify_status(violation_pct=violation_pct, normalized_residual=normalized_residual)
    if status == "normal":
        if variable_name == "position":
            return "Position response remains within the healthy baseline envelope."
        if variable_name == "torque":
            return "Torque effort remains within the healthy baseline envelope."
        return "Temperature remains within the healthy baseline envelope."

    direction_suffix = ""
    if dominant_direction in {"opening", "closing"}:
        direction_suffix = f" during {dominant_direction}"

    if variable_name == "position":
        if trend == "high":
            return f"Position response is faster or higher than expected{direction_suffix}."
        return f"Position response is slower or laggier than expected{direction_suffix}."
    if variable_name == "torque":
        if trend == "high":
            return f"Torque effort exceeds the healthy baseline{direction_suffix}."
        return f"Torque effort is lower than the healthy baseline{direction_suffix}."
    if trend == "high":
        return f"Temperature is rising faster than expected{direction_suffix}."
    return f"Temperature is lower or cooling faster than expected{direction_suffix}."


def summarize_overall(variable_results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    severity_order = {"normal": 0, "warning": 1, "critical": 2}
    available = {
        key: value
        for key, value in variable_results.items()
        if value["summary"]["available"]
    }
    if not available:
        return {
            "status": "warning",
            "insight": "No valid telemetry variables were available for evaluation.",
            "dominant_variable": None,
        }

    dominant_variable = max(
        available,
        key=lambda key: (
            available[key]["summary"]["envelope_violation_pct"],
            available[key]["summary"]["normalized_median_abs_residual"],
        ),
    )
    worst_status = max(
        (value["summary"]["status"] for value in available.values()),
        key=lambda status: severity_order[status],
    )
    insight = available[dominant_variable]["summary"]["insight"]
    return {
        "status": worst_status,
        "insight": insight,
        "dominant_variable": dominant_variable,
    }
