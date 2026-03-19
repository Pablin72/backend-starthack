from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


EPSILON = 1e-9


@dataclass
class CycleScore:
    run_id: str
    waveform_type: str
    cycle_id: int
    smoothness_score: float
    consistency_score: float
    physical_plausibility_score: float
    total_score: float
    sample_count: int
    start_time: str
    end_time: str


def run_calibration(campaign_dir: str | Path, output_dir: str | Path) -> dict[str, Any]:
    campaign_path = Path(campaign_dir).resolve()
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    raw_runs = load_campaign_runs(campaign_path)
    command_runs = load_command_runs(campaign_path)
    cleaned_runs = [clean_and_normalize_run(run) for run in raw_runs]
    segmented_runs = [segment_run_cycles(run) for run in cleaned_runs]
    cycle_scores = score_cycles(segmented_runs)
    healthy_cycles = select_healthy_cycles(segmented_runs, cycle_scores)

    position_model = calibrate_position_model_from_commands(cleaned_runs, command_runs)
    torque_model = calibrate_torque_model(healthy_cycles)
    temperature_model = calibrate_temperature_model(healthy_cycles)
    baseline = generate_baseline(healthy_cycles, position_model, torque_model, temperature_model)
    envelopes = build_envelopes(healthy_cycles, baseline)
    diagnostics = validate_models(healthy_cycles, baseline, cycle_scores)
    diagnostics_json = {
        key: value
        for key, value in diagnostics.items()
        if key != "cycle_scores"
    }

    result = {
        "position_model": position_model,
        "torque_model": torque_model,
        "temperature_model": temperature_model,
        "baseline": baseline,
        "envelopes": envelopes,
        "diagnostics": diagnostics_json,
        "selected_cycles": [asdict(score) for score in healthy_cycles["selected_scores"]],
    }

    (output_path / "healthy_model_report.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )

    healthy_cycles["calibration_dataset"].to_csv(output_path / "healthy_cycles.csv", index=False)
    diagnostics["cycle_scores"].to_csv(output_path / "cycle_scores.csv", index=False)

    return result


def load_campaign_runs(campaign_dir: Path) -> list[pd.DataFrame]:
    runs = []
    for csv_path in sorted(campaign_dir.glob("*_telemetry.csv")):
        frame = pd.read_csv(csv_path)
        frame["source_file"] = csv_path.name
        runs.append(frame)
    if not runs:
        raise ValueError(f"No telemetry CSV files found in {campaign_dir}")
    return runs


def load_command_runs(campaign_dir: Path) -> dict[str, pd.DataFrame]:
    command_runs: dict[str, pd.DataFrame] = {}
    for csv_path in sorted(campaign_dir.glob("*_commands.csv")):
        frame = pd.read_csv(csv_path)
        test_id = csv_path.name.replace("_commands.csv", "")
        frame["test_id"] = test_id
        frame["logged_at"] = pd.to_datetime(frame["logged_at"], utc=True, errors="coerce")
        numeric_columns = ["test_number", "setpoint_position_%", "bias", "amplitude", "frequency", "elapsed_phase_seconds"]
        for column in numeric_columns:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.sort_values("elapsed_phase_seconds").drop_duplicates(subset=["elapsed_phase_seconds"])
        command_runs[test_id] = frame
    return command_runs


def clean_and_normalize_run(frame: pd.DataFrame) -> pd.DataFrame:
    run = frame.copy()
    run["timestamp"] = pd.to_datetime(run["timestamp"], utc=True, errors="coerce")
    run = run.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"])

    numeric_columns = [
        "bias",
        "amplitude",
        "frequency",
        "feedback_position_%",
        "setpoint_position_%",
        "motor_torque_Nmm",
        "power_W",
        "internal_temperature_deg_C",
        "rotation_direction",
        "test_number",
    ]
    for column in numeric_columns:
        if column in run.columns:
            run[column] = pd.to_numeric(run[column], errors="coerce")

    run["feedback_position_%"] = run["feedback_position_%"].clip(lower=0, upper=100)
    run["setpoint_position_%"] = run["setpoint_position_%"].clip(lower=0, upper=100)

    run = run.set_index("timestamp")
    numeric_for_interp = [
        "feedback_position_%",
        "setpoint_position_%",
        "motor_torque_Nmm",
        "power_W",
        "internal_temperature_deg_C",
    ]
    for column in numeric_for_interp:
        if column in run.columns:
            run[column] = run[column].interpolate(limit=3, limit_direction="both")
            run[column] = run[column].rolling(window=3, min_periods=1, center=True).median()

    run = run.dropna(subset=["feedback_position_%", "setpoint_position_%"])
    run = run.reset_index()

    if len(run) >= 3:
        elapsed_seconds = (run["timestamp"] - run["timestamp"].iloc[0]).dt.total_seconds()
        run["elapsed_seconds"] = elapsed_seconds
        run["dt_seconds"] = run["elapsed_seconds"].diff().fillna(run["elapsed_seconds"].diff().median())
        run["dt_seconds"] = run["dt_seconds"].replace(0, np.nan).bfill().ffill().fillna(0.0)
        run["velocity"] = np.gradient(run["feedback_position_%"], run["elapsed_seconds"].replace(0, np.nan))
        run["velocity"] = pd.Series(run["velocity"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        run["temperature_rate"] = np.gradient(
            run["internal_temperature_deg_C"].ffill().bfill(),
            run["elapsed_seconds"].replace(0, np.nan),
        )
        run["temperature_rate"] = pd.Series(run["temperature_rate"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    else:
        run["elapsed_seconds"] = 0.0
        run["dt_seconds"] = 0.0
        run["velocity"] = 0.0
        run["temperature_rate"] = 0.0

    return run


def segment_run_cycles(frame: pd.DataFrame) -> pd.DataFrame:
    run = frame.copy()
    waveform = str(run["waveform_type"].iloc[0]).strip().lower()

    if waveform == "square":
        signal_indicator = run["setpoint_position_%"].diff().abs().fillna(0.0)
        boundary_indices = list(run.index[signal_indicator > max(1.0, signal_indicator.quantile(0.95) * 0.5)])
    elif waveform == "sine":
        centered = run["setpoint_position_%"] - run["setpoint_position_%"].median()
        boundary_indices = list(run.index[(centered.shift(1) <= 0) & (centered > 0)])
    elif waveform == "triangle":
        slope = run["setpoint_position_%"].diff().fillna(0.0)
        boundary_indices = list(run.index[(slope.shift(1) >= 0) & (slope < 0)])
    else:
        boundary_indices = []

    boundary_indices = sorted(set(boundary_indices))
    if not boundary_indices or boundary_indices[0] != run.index[0]:
        boundary_indices = [run.index[0], *boundary_indices]
    if boundary_indices[-1] != run.index[-1]:
        boundary_indices.append(run.index[-1])

    cycle_ids = np.full(len(run), -1, dtype=int)
    cycle_number = 0
    for start, end in zip(boundary_indices[:-1], boundary_indices[1:]):
        mask = (run.index >= start) & (run.index < end)
        if mask.sum() >= 5:
            cycle_ids[mask] = cycle_number
            cycle_number += 1

    run["cycle_id"] = cycle_ids
    return run[run["cycle_id"] >= 0].copy()


def score_cycles(runs: list[pd.DataFrame]) -> list[CycleScore]:
    scores: list[CycleScore] = []
    waveform_templates = build_waveform_templates(runs)

    for run in runs:
        if "cycle_id" not in run.columns or run.empty:
            continue
        run_id = str(run["test_id"].iloc[0])
        waveform = str(run["waveform_type"].iloc[0])
        template = waveform_templates.get(waveform)

        for cycle_id, cycle in run.groupby("cycle_id"):
            if len(cycle) < 5:
                continue

            velocity = cycle["velocity"].to_numpy()
            acceleration = np.diff(velocity, prepend=velocity[0])
            smoothness_penalty = np.median(np.abs(acceleration))
            smoothness_score = 1.0 / (1.0 + smoothness_penalty)

            consistency_score = compare_cycle_to_template(cycle, template)

            plausibility_penalty = 0.0
            plausibility_penalty += float((cycle["feedback_position_%"].diff().abs() > 20).sum())
            plausibility_penalty += float((cycle["dt_seconds"] <= 0).sum()) * 5.0
            plausibility_penalty += float((cycle["feedback_position_%"].between(0, 100) == False).sum()) * 10.0
            physical_plausibility_score = 1.0 / (1.0 + plausibility_penalty)

            total_score = (0.4 * smoothness_score) + (0.35 * consistency_score) + (0.25 * physical_plausibility_score)
            scores.append(
                CycleScore(
                    run_id=run_id,
                    waveform_type=waveform,
                    cycle_id=int(cycle_id),
                    smoothness_score=float(smoothness_score),
                    consistency_score=float(consistency_score),
                    physical_plausibility_score=float(physical_plausibility_score),
                    total_score=float(total_score),
                    sample_count=int(len(cycle)),
                    start_time=cycle["timestamp"].iloc[0].isoformat(),
                    end_time=cycle["timestamp"].iloc[-1].isoformat(),
                )
            )

    return scores


def build_waveform_templates(runs: list[pd.DataFrame]) -> dict[str, np.ndarray]:
    templates: dict[str, np.ndarray] = {}
    for waveform in {str(run["waveform_type"].iloc[0]) for run in runs if not run.empty}:
        traces = []
        for run in runs:
            if run.empty or str(run["waveform_type"].iloc[0]) != waveform:
                continue
            for _, cycle in run.groupby("cycle_id"):
                if len(cycle) < 5:
                    continue
                traces.append(resample_cycle(cycle["feedback_position_%"].to_numpy(), 100))
        if traces:
            templates[waveform] = np.median(np.vstack(traces), axis=0)
    return templates


def compare_cycle_to_template(cycle: pd.DataFrame, template: np.ndarray | None) -> float:
    if template is None:
        return 0.5
    signal = resample_cycle(cycle["feedback_position_%"].to_numpy(), len(template))
    error = np.median(np.abs(signal - template))
    return float(1.0 / (1.0 + error))


def resample_cycle(values: np.ndarray, n_points: int) -> np.ndarray:
    if len(values) == 0:
        return np.zeros(n_points)
    x_old = np.linspace(0.0, 1.0, len(values))
    x_new = np.linspace(0.0, 1.0, n_points)
    return np.interp(x_new, x_old, values)


def select_healthy_cycles(runs: list[pd.DataFrame], scores: list[CycleScore]) -> dict[str, Any]:
    score_frame = pd.DataFrame(asdict(score) for score in scores)
    if score_frame.empty:
        raise ValueError("No cycles available for calibration")

    selected_scores: list[CycleScore] = []
    selected_frames = []
    for waveform, group in score_frame.groupby("waveform_type"):
        threshold = group["total_score"].quantile(0.7)
        chosen = group[group["total_score"] >= threshold].sort_values("total_score", ascending=False)
        if chosen.empty:
            chosen = group.sort_values("total_score", ascending=False).head(1)

        chosen_records = {(row.run_id, int(row.cycle_id)) for row in chosen.itertuples()}
        for score in scores:
            if score.waveform_type == waveform and (score.run_id, score.cycle_id) in chosen_records:
                selected_scores.append(score)

        for run in runs:
            if run.empty or str(run["waveform_type"].iloc[0]) != waveform:
                continue
            run_id = str(run["test_id"].iloc[0])
            selected_cycle_ids = [cycle_id for score_run_id, cycle_id in chosen_records if score_run_id == run_id]
            if selected_cycle_ids:
                selected_frames.append(run[run["cycle_id"].isin(selected_cycle_ids)].copy())

    calibration_dataset = pd.concat(selected_frames, ignore_index=True).sort_values("timestamp")
    return {
        "selected_scores": selected_scores,
        "calibration_dataset": calibration_dataset,
    }


def calibrate_position_model_from_commands(runs: list[pd.DataFrame], command_runs: dict[str, pd.DataFrame]) -> dict[str, Any]:
    square_runs = [
        run.copy()
        for run in runs
        if not run.empty and str(run["waveform_type"].iloc[0]).strip().lower() == "square"
    ]

    segments: list[dict[str, Any]] = []
    discarded_segments = 0
    response_type_counts = {"exponential": 0, "rate_limited": 0, "mixed": 0}

    for run in square_runs:
        test_id = str(run["test_id"].iloc[0])
        command_run = command_runs.get(test_id)
        if command_run is None or command_run.empty:
            continue

        aligned_run = align_command_and_telemetry(run, command_run)
        run_segments, discarded = extract_clean_step_segments(aligned_run, command_run)
        discarded_segments += discarded
        for segment in run_segments:
            response_type_counts[segment["response_type"]] += 1
        segments.extend(run_segments)

    if not segments:
        return {
            "notes": "No valid square-wave segments found for command-aligned calibration",
            "opening": {},
            "closing": {},
            "fit_summary": {
                "valid_segments": 0,
                "discarded_segments": discarded_segments,
                "response_type_distribution": response_type_counts,
                "behavior_mode": "unknown",
            },
        }

    grouped = {"opening": [], "closing": []}
    for segment in segments:
        grouped[segment["direction"]].append(segment)

    models = {}
    all_residuals = []
    for direction, direction_segments in grouped.items():
        models[direction] = aggregate_segment_parameters(direction_segments)
        all_residuals.extend([segment["fit_error_median_abs"] for segment in direction_segments if segment["fit_error_median_abs"] is not None])

    behavior_mode = max(response_type_counts, key=response_type_counts.get) if any(response_type_counts.values()) else "unknown"

    return {
        "opening": models["opening"],
        "closing": models["closing"],
        "fit_summary": {
            "valid_segments": len(segments),
            "discarded_segments": discarded_segments,
            "response_type_distribution": response_type_counts,
            "behavior_mode": behavior_mode,
            "residual_median_abs": round(float(np.median(all_residuals)), 6) if all_residuals else None,
        },
    }


def align_command_and_telemetry(run: pd.DataFrame, command_run: pd.DataFrame) -> pd.DataFrame:
    aligned = run.copy().sort_values("elapsed_seconds").reset_index(drop=True)
    aligned["command_elapsed_seconds"] = aligned["elapsed_seconds"]
    transitions = detect_command_transitions(command_run)

    offsets = []
    for transition in transitions:
        motion_start = detect_motion_start(aligned, transition["elapsed_seconds"], transition["delta"])
        if motion_start is not None:
            offsets.append(motion_start - transition["elapsed_seconds"])

    aligned["estimated_global_offset_seconds"] = float(np.median(offsets)) if offsets else 0.0
    aligned["aligned_elapsed_seconds"] = aligned["elapsed_seconds"] - aligned["estimated_global_offset_seconds"]
    return aligned


def detect_command_transitions(command_run: pd.DataFrame) -> list[dict[str, Any]]:
    command = command_run.copy()
    command["delta"] = command["setpoint_position_%"].diff().fillna(0.0)
    transitions = command[command["delta"].abs() > 1.0]
    return [
        {
            "elapsed_seconds": float(row["elapsed_phase_seconds"]),
            "from_value": float(command.iloc[max(index - 1, 0)]["setpoint_position_%"]),
            "to_value": float(row["setpoint_position_%"]),
            "delta": float(row["delta"]),
        }
        for index, row in transitions.reset_index(drop=True).iterrows()
    ]


def detect_motion_start(run: pd.DataFrame, command_time: float, command_delta: float, time_column: str = "elapsed_seconds") -> float | None:
    window = run[run[time_column] >= max(0.0, command_time - 2.0)].copy()
    if window.empty:
        return None

    baseline_window = window[window[time_column] <= command_time]
    if baseline_window.empty:
        baseline_position = float(window["feedback_position_%"].iloc[0])
    else:
        baseline_position = float(baseline_window["feedback_position_%"].median())

    threshold = max(0.5, abs(command_delta) * 0.05)
    movement = window[(window["feedback_position_%"] - baseline_position).abs() >= threshold]
    if movement.empty:
        derivative_threshold = max(0.2, np.nanpercentile(window["velocity"].abs(), 75))
        movement = window[window["velocity"].abs() >= derivative_threshold]
    if movement.empty:
        return None
    return float(movement[time_column].iloc[0])


def extract_clean_step_segments(aligned_run: pd.DataFrame, command_run: pd.DataFrame) -> tuple[list[dict[str, Any]], int]:
    transitions = detect_command_transitions(command_run)
    valid_segments: list[dict[str, Any]] = []
    discarded = 0

    for index, transition in enumerate(transitions):
        next_command_time = transitions[index + 1]["elapsed_seconds"] if index + 1 < len(transitions) else None
        pre_window_start = max(0.0, transition["elapsed_seconds"] - 4.0)
        post_window_end = transition["elapsed_seconds"] + 20.0
        if next_command_time is not None:
            post_window_end = min(post_window_end, next_command_time - 0.5)

        segment = aligned_run[
            (aligned_run["aligned_elapsed_seconds"] >= pre_window_start)
            & (aligned_run["aligned_elapsed_seconds"] <= post_window_end)
        ].copy()

        if len(segment) < 8:
            discarded += 1
            continue
        if segment["feedback_position_%"].isna().any():
            discarded += 1
            continue
        if ((segment["feedback_position_%"] <= 0.1) | (segment["feedback_position_%"] >= 99.9)).mean() > 0.9:
            discarded += 1
            continue
        if segment["feedback_position_%"].diff().abs().max() > 25:
            discarded += 1
            continue

        classified = classify_step_segment(segment, transition)
        if classified is None:
            discarded += 1
            continue
        valid_segments.append(classified)

    return valid_segments, discarded


def classify_step_segment(segment: pd.DataFrame, transition: dict[str, Any]) -> dict[str, Any] | None:
    command_time = transition["elapsed_seconds"]
    post = segment[segment["aligned_elapsed_seconds"] >= command_time].copy()
    pre = segment[segment["aligned_elapsed_seconds"] < command_time].copy()
    if pre.empty or post.empty:
        return None

    baseline = float(pre["feedback_position_%"].median())
    final_position = float(post["feedback_position_%"].tail(max(3, len(post) // 5)).median())
    step_size = final_position - baseline
    if abs(step_size) < 1.0:
        return None

    target_63 = baseline + (0.632 * step_size)
    crossing = post[(post["feedback_position_%"] - target_63).abs() <= max(0.5, abs(step_size) * 0.08)]
    tau_estimate = None if crossing.empty else float(crossing["aligned_elapsed_seconds"].iloc[0] - command_time)
    velocity_95 = float(np.nanpercentile(post["velocity"].abs(), 95))

    x = post["aligned_elapsed_seconds"].to_numpy() - command_time
    y = post["feedback_position_%"].to_numpy()
    linear_fit = np.polyfit(x, y, 1)
    linear_prediction = np.polyval(linear_fit, x)
    linear_error = float(np.median(np.abs(y - linear_prediction)))

    if tau_estimate is not None and tau_estimate > 0:
        exp_prediction = baseline + step_size * (1.0 - np.exp(-np.clip(x, 0, None) / max(tau_estimate, 1e-3)))
        exp_error = float(np.median(np.abs(y - exp_prediction)))
    else:
        exp_error = math.inf

    if exp_error < linear_error * 0.8:
        response_type = "exponential"
    elif linear_error < exp_error * 0.8:
        response_type = "rate_limited"
    else:
        response_type = "mixed"

    direction = "opening" if step_size > 0 else "closing"
    onset_time = detect_motion_start(segment, command_time, transition["delta"], time_column="aligned_elapsed_seconds")
    delay = None if onset_time is None else max(0.01, onset_time - command_time)
    vmax = max(0.01, velocity_95)

    if tau_estimate is None or tau_estimate <= 0:
        if response_type == "rate_limited":
            tau_estimate = max(0.1, abs(step_size) / max(vmax, EPSILON) * 0.5)
        else:
            tau_estimate = max(0.1, abs(step_size) / max(vmax, EPSILON) * 0.63)
    else:
        tau_estimate = max(0.01, tau_estimate)

    fit_error = min(linear_error, exp_error if np.isfinite(exp_error) else linear_error)
    return {
        "test_id": str(segment["test_id"].iloc[0]),
        "direction": direction,
        "delay_seconds": round(float(delay), 6),
        "tau_seconds": round(float(tau_estimate), 6),
        "vmax_percent_per_second": round(float(vmax), 6),
        "response_type": response_type,
        "fit_error_median_abs": round(float(fit_error), 6),
        "sample_count": int(len(segment)),
    }


def aggregate_segment_parameters(segments: list[dict[str, Any]]) -> dict[str, Any]:
    if not segments:
        return {
            "delay_L_seconds": None,
            "time_constant_tau_s_seconds": None,
            "max_velocity_vmax_percent_per_second": None,
            "valid_segments": 0,
            "residual_median_abs": None,
        }

    delays = np.asarray([segment["delay_seconds"] for segment in segments], dtype=float)
    taus = np.asarray([segment["tau_seconds"] for segment in segments], dtype=float)
    vmax_values = np.asarray([segment["vmax_percent_per_second"] for segment in segments], dtype=float)
    residuals = np.asarray([segment["fit_error_median_abs"] for segment in segments], dtype=float)

    return {
        "delay_L_seconds": round(float(np.median(np.clip(delays, 0.01, None))), 6),
        "time_constant_tau_s_seconds": round(float(np.median(np.clip(taus, 0.01, None))), 6),
        "max_velocity_vmax_percent_per_second": round(float(np.nanpercentile(np.clip(vmax_values, 0.01, None), 95)), 6),
        "valid_segments": int(len(segments)),
        "residual_median_abs": round(float(np.median(residuals)), 6),
        "parameter_spread": {
            "delay_mad": round(float(np.median(np.abs(delays - np.median(delays)))), 6),
            "tau_mad": round(float(np.median(np.abs(taus - np.median(taus)))), 6),
            "vmax_mad": round(float(np.median(np.abs(vmax_values - np.median(vmax_values)))), 6),
        },
    }


def calibrate_torque_model(healthy_cycles: dict[str, Any]) -> dict[str, Any]:
    df = healthy_cycles["calibration_dataset"].copy()
    moving = df[df["motor_torque_Nmm"].notna()].copy()
    moving["signed_velocity"] = moving["velocity"]
    moving["abs_velocity"] = moving["velocity"].abs()
    moving["torque_signed"] = moving["motor_torque_Nmm"]
    moving["torque_magnitude"] = moving["motor_torque_Nmm"].abs()

    velocity_threshold = max(0.05, float(np.nanpercentile(moving["abs_velocity"], 25)))
    filtered = moving[
        (moving["abs_velocity"] >= velocity_threshold)
        & (moving["torque_magnitude"].notna())
        & (moving["feedback_position_%"].between(0, 100))
    ].copy()

    sign_check = {
        "opening_signed_torque_median": round(float(filtered.loc[filtered["signed_velocity"] > 0, "torque_signed"].median()), 6),
        "closing_signed_torque_median": round(float(filtered.loc[filtered["signed_velocity"] < 0, "torque_signed"].median()), 6),
        "opening_and_closing_have_opposite_signs": bool(
            filtered.loc[filtered["signed_velocity"] > 0, "torque_signed"].median()
            * filtered.loc[filtered["signed_velocity"] < 0, "torque_signed"].median()
            < 0
        ),
    }

    directional_models = {}
    residuals = []
    correlations = {}
    for label, direction_df in {
        "opening": filtered[filtered["signed_velocity"] > 0],
        "closing": filtered[filtered["signed_velocity"] < 0],
    }.items():
        model = fit_directional_torque_model(direction_df)
        directional_models[label] = model
        if model["residual_median_abs"] is not None:
            residuals.append(model["residual_median_abs"])
        correlations[label] = {
            "torque_vs_abs_velocity": safe_correlation(direction_df["torque_magnitude"], direction_df["abs_velocity"]),
            "torque_vs_position": safe_correlation(direction_df["torque_magnitude"], direction_df["feedback_position_%"]),
        }

    return {
        "convention": {
            "torque_used_for_fit": "magnitude_abs(torque_signed)",
            "velocity_used_for_fit": "abs(dy/dt)",
            "direction_split": "opening if dy/dt>0, closing if dy/dt<0",
        },
        "opening": directional_models["opening"],
        "closing": directional_models["closing"],
        "diagnostics": {
            "fit_samples": int(len(filtered)),
            "velocity_threshold": round(float(velocity_threshold), 6),
            "residual_median_abs_overall": round(float(np.median(residuals)), 6) if residuals else None,
            "sign_consistency_check": {
                "pass": sign_check["opening_and_closing_have_opposite_signs"],
                **sign_check,
            },
            "correlations": correlations,
        },
    }


def fit_directional_torque_model(direction_df: pd.DataFrame) -> dict[str, Any]:
    if len(direction_df) < 5:
        return {
            "tau_0": None,
            "k_v": None,
            "k_p": None,
            "valid_segments": 0,
            "residual_median_abs": None,
        }

    design = np.column_stack(
        [
            np.ones(len(direction_df)),
            direction_df["abs_velocity"].to_numpy(),
            direction_df["feedback_position_%"].to_numpy(),
        ]
    )
    target = direction_df["torque_magnitude"].to_numpy()
    coefficients = solve_nonnegative_least_squares(design, target)
    prediction = design @ coefficients
    residual = np.abs(target - prediction)

    return {
        "tau_0": round(float(coefficients[0]), 6),
        "k_v": round(float(coefficients[1]), 6),
        "k_p": round(float(coefficients[2]), 6),
        "valid_segments": int(len(direction_df)),
        "residual_median_abs": round(float(np.median(residual)), 6),
    }


def solve_nonnegative_least_squares(design: np.ndarray, target: np.ndarray) -> np.ndarray:
    n_features = design.shape[1]
    best_coefficients = np.zeros(n_features)
    best_error = math.inf

    for mask in range(1, 1 << n_features):
        active_indices = [index for index in range(n_features) if mask & (1 << index)]
        sub_design = design[:, active_indices]
        sub_coefficients, *_ = np.linalg.lstsq(sub_design, target, rcond=None)
        if np.any(sub_coefficients < 0):
            continue

        coefficients = np.zeros(n_features)
        coefficients[active_indices] = sub_coefficients
        error = float(np.median(np.abs(target - (design @ coefficients))))
        if error < best_error:
            best_error = error
            best_coefficients = coefficients

    if not np.isfinite(best_error):
        return np.zeros(n_features)
    return best_coefficients


def safe_correlation(left: pd.Series, right: pd.Series) -> float | None:
    if len(left) < 2 or len(right) < 2:
        return None
    correlation = left.corr(right)
    if pd.isna(correlation):
        return None
    return round(float(correlation), 6)


def calibrate_temperature_model(healthy_cycles: dict[str, Any]) -> dict[str, Any]:
    df = healthy_cycles["calibration_dataset"].copy()
    temperature_df = df[df["internal_temperature_deg_C"].notna() & df["motor_torque_Nmm"].notna()].copy()
    if temperature_df.empty:
        return {"alpha": 0.0, "beta": 0.0, "ambient_temperature": None, "fit_samples": 0}

    ambient = float(np.nanpercentile(temperature_df["internal_temperature_deg_C"], 5))
    target = temperature_df["temperature_rate"].to_numpy()
    design = np.column_stack(
        [
            -(temperature_df["internal_temperature_deg_C"].to_numpy() - ambient),
            np.square(temperature_df["motor_torque_Nmm"].to_numpy()),
        ]
    )
    coefficients, *_ = np.linalg.lstsq(design, target, rcond=None)
    alpha, beta = [float(value) for value in coefficients]

    return {
        "alpha": round(alpha, 6),
        "beta": round(beta, 6),
        "ambient_temperature": round(ambient, 6),
        "fit_samples": int(len(temperature_df)),
    }


def generate_baseline(
    healthy_cycles: dict[str, Any],
    position_model: dict[str, Any],
    torque_model: dict[str, Any],
    temperature_model: dict[str, Any],
) -> dict[str, Any]:
    df = healthy_cycles["calibration_dataset"].copy()
    baseline_by_waveform = {}

    for waveform, waveform_df in df.groupby("waveform_type"):
        position_trace = np.median(
            np.vstack([resample_cycle(cycle["feedback_position_%"].to_numpy(), 100) for _, cycle in waveform_df.groupby("cycle_id")]),
            axis=0,
        )
        torque_trace = np.median(
            np.vstack([resample_cycle(cycle["motor_torque_Nmm"].to_numpy(), 100) for _, cycle in waveform_df.groupby("cycle_id")]),
            axis=0,
        )
        temp_trace = np.median(
            np.vstack([resample_cycle(cycle["internal_temperature_deg_C"].to_numpy(), 100) for _, cycle in waveform_df.groupby("cycle_id")]),
            axis=0,
        )
        baseline_by_waveform[waveform] = {
            "reference_position": [round(float(value), 6) for value in position_trace],
            "reference_torque": [round(float(value), 6) for value in torque_trace],
            "reference_temperature": [round(float(value), 6) for value in temp_trace],
            "position_model_parameters": position_model,
            "torque_model_parameters": torque_model,
            "temperature_model_parameters": temperature_model,
        }

    return baseline_by_waveform


def build_envelopes(healthy_cycles: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    df = healthy_cycles["calibration_dataset"].copy()
    envelopes = {}
    for waveform, waveform_df in df.groupby("waveform_type"):
        position_traces = np.vstack(
            [resample_cycle(cycle["feedback_position_%"].to_numpy(), 100) for _, cycle in waveform_df.groupby("cycle_id")]
        )
        torque_traces = np.vstack(
            [resample_cycle(cycle["motor_torque_Nmm"].to_numpy(), 100) for _, cycle in waveform_df.groupby("cycle_id")]
        )
        temp_traces = np.vstack(
            [resample_cycle(cycle["internal_temperature_deg_C"].to_numpy(), 100) for _, cycle in waveform_df.groupby("cycle_id")]
        )

        envelopes[waveform] = {
            "position_mad": [round(float(value), 6) for value in mad(position_traces)],
            "torque_mad": [round(float(value), 6) for value in mad(torque_traces)],
            "temperature_mad": [round(float(value), 6) for value in mad(temp_traces)],
            "position_bounds": build_bounds(baseline[waveform]["reference_position"], mad(position_traces)),
            "torque_bounds": build_bounds(baseline[waveform]["reference_torque"], mad(torque_traces)),
            "temperature_bounds": build_bounds(baseline[waveform]["reference_temperature"], mad(temp_traces)),
        }
    return envelopes


def mad(values: np.ndarray) -> np.ndarray:
    median = np.median(values, axis=0)
    return np.median(np.abs(values - median), axis=0)


def build_bounds(reference: list[float], deviation: np.ndarray) -> dict[str, list[float]]:
    reference_array = np.asarray(reference)
    spread = np.maximum(deviation * 3.0, 0.1)
    return {
        "lower": [round(float(value), 6) for value in reference_array - spread],
        "upper": [round(float(value), 6) for value in reference_array + spread],
    }


def validate_models(
    healthy_cycles: dict[str, Any],
    baseline: dict[str, Any],
    cycle_scores: list[CycleScore],
) -> dict[str, Any]:
    df = healthy_cycles["calibration_dataset"].copy()
    residual_rows = []
    invariant_violations = {
        "position_out_of_bounds": int(((df["feedback_position_%"] < 0) | (df["feedback_position_%"] > 100)).sum()),
        "nonpositive_dt": int((df["dt_seconds"] <= 0).sum()),
        "extreme_velocity": int((df["velocity"].abs() > np.nanpercentile(df["velocity"].abs(), 99)).sum()),
    }

    for waveform, waveform_df in df.groupby("waveform_type"):
        ref_position = np.asarray(baseline[waveform]["reference_position"])
        ref_torque = np.asarray(baseline[waveform]["reference_torque"])
        ref_temp = np.asarray(baseline[waveform]["reference_temperature"])
        for cycle_id, cycle in waveform_df.groupby("cycle_id"):
            cycle_position = resample_cycle(cycle["feedback_position_%"].to_numpy(), 100)
            cycle_torque = resample_cycle(cycle["motor_torque_Nmm"].to_numpy(), 100)
            cycle_temp = resample_cycle(cycle["internal_temperature_deg_C"].to_numpy(), 100)
            residual_rows.append(
                {
                    "waveform_type": waveform,
                    "cycle_id": int(cycle_id),
                    "position_residual_median_abs": float(np.median(np.abs(cycle_position - ref_position))),
                    "torque_residual_median_abs": float(np.median(np.abs(cycle_torque - ref_torque))),
                    "temperature_residual_median_abs": float(np.median(np.abs(cycle_temp - ref_temp))),
                }
            )

    residual_frame = pd.DataFrame(residual_rows)
    score_frame = pd.DataFrame(asdict(score) for score in cycle_scores)
    repeatability = float(score_frame["consistency_score"].mean()) if not score_frame.empty else 0.0

    return {
        "repeatability_score": round(repeatability, 6),
        "residual_distribution": {
            "position_median_abs": round(float(residual_frame["position_residual_median_abs"].median()), 6),
            "torque_median_abs": round(float(residual_frame["torque_residual_median_abs"].median()), 6),
            "temperature_median_abs": round(float(residual_frame["temperature_residual_median_abs"].median()), 6),
        },
        "invariant_violations": invariant_violations,
        "cycle_scores": score_frame,
    }
