from __future__ import annotations

from flask import Blueprint, jsonify, request

from api.security import require_frontend_token
from api.services.feature_engine import compute_features, normalize_sample, update_baseline
from api.services.feature_storage import (
    get_baseline_profile,
    get_latest_feature_snapshot,
    get_recent_samples,
    init_db,
    insert_feature_snapshot,
    insert_sample,
    upsert_baseline_profile,
)


feature_bp = Blueprint("features", __name__)
DEFAULT_WINDOW_SIZE = 10


@feature_bp.route("/ingest", methods=["POST"])
@require_frontend_token
def ingest_samples():
    """
    Ingest normalized actuator samples, preserve them, compute features, and refresh baselines.
    ---
    tags:
      - Features
    consumes:
      - application/json
    security:
      - Bearer: []
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          properties:
            storage_kind:
              type: string
              example: raw
            source_name:
              type: string
              example: belimo_influx
            window_size:
              type: integer
              example: 10
            sample:
              type: object
            samples:
              type: array
              items:
                type: object
    responses:
      200:
        description: Samples stored and feature snapshot updated
    """
    payload = request.get_json() or {}
    storage_kind = (payload.get("storage_kind") or "raw").strip().lower()
    source_name = (payload.get("source_name") or ("belimo_influx" if storage_kind == "raw" else "scenario")).strip()
    window_size = int(payload.get("window_size") or DEFAULT_WINDOW_SIZE)

    if storage_kind not in {"raw", "mock"}:
        return jsonify({"status": "error", "message": "storage_kind must be 'raw' or 'mock'"}), 400

    raw_samples = payload.get("samples")
    if raw_samples is None:
        single_sample = payload.get("sample")
        raw_samples = [] if single_sample is None else [single_sample]

    if not raw_samples:
        return jsonify({"status": "error", "message": "Provide 'sample' or 'samples'"}), 400

    try:
        normalized_samples = [normalize_sample(sample) for sample in raw_samples]
    except ValueError as error:
        return jsonify({"status": "error", "message": str(error)}), 400

    inserted_ids = [insert_sample(sample, storage_kind, source_name) for sample in normalized_samples]

    latest_device_id = normalized_samples[-1]["device_id"]
    recent_samples = get_recent_samples(latest_device_id, storage_kind, max(2, window_size))

    snapshot = None
    baseline = get_baseline_profile(latest_device_id)

    if len(recent_samples) >= 2:
        features = compute_features(recent_samples)
        snapshot_id = insert_feature_snapshot(
            storage_kind=storage_kind,
            source_name=source_name,
            device_id=latest_device_id,
            window_start=recent_samples[0]["timestamp"],
            window_end=recent_samples[-1]["timestamp"],
            sample_count=len(recent_samples),
            features=features,
        )
        baseline = upsert_baseline_profile(
            latest_device_id,
            update_baseline(
                baseline,
                latest_features=features,
                latest_temperature=recent_samples[-1]["temperature"],
            ),
        )
        snapshot = {
            "id": snapshot_id,
            "device_id": latest_device_id,
            "storage_kind": storage_kind,
            "source_name": source_name,
            "window_start": recent_samples[0]["timestamp"],
            "window_end": recent_samples[-1]["timestamp"],
            "sample_count": len(recent_samples),
            "features": features,
        }

    return jsonify(
        {
            "status": "success",
            "inserted_sample_ids": inserted_ids,
            "device_id": latest_device_id,
            "storage_kind": storage_kind,
            "source_name": source_name,
            "feature_snapshot": snapshot,
            "baseline": baseline,
        }
    )


@feature_bp.route("/devices/<device_id>/latest", methods=["GET"])
@require_frontend_token
def get_latest_device_state(device_id: str):
    """
    Return the latest computed features and baseline for a device.
    ---
    tags:
      - Features
    security:
      - Bearer: []
    parameters:
      - in: path
        name: device_id
        required: true
        type: string
      - in: query
        name: storage_kind
        type: string
        required: false
    responses:
      200:
        description: Latest device state
    """
    storage_kind = request.args.get("storage_kind", "").strip().lower() or None
    if storage_kind not in {None, "raw", "mock"}:
        return jsonify({"status": "error", "message": "storage_kind must be 'raw' or 'mock'"}), 400

    samples = get_recent_samples(device_id, storage_kind or "raw", 5)
    snapshot = get_latest_feature_snapshot(device_id, storage_kind)
    baseline = get_baseline_profile(device_id)

    return jsonify(
        {
            "status": "success",
            "device_id": device_id,
            "storage_kind": storage_kind or "raw",
            "recent_samples": samples,
            "latest_feature_snapshot": snapshot,
            "baseline": baseline,
        }
    )


@feature_bp.route("/seed-demo", methods=["POST"])
@require_frontend_token
def seed_demo_data():
    """
    Seed the database with a short raw data sequence and an optional mock anomaly sequence.
    ---
    tags:
      - Features
    security:
      - Bearer: []
    responses:
      200:
        description: Demo data seeded
    """
    payload = request.get_json() or {}
    device_id = (payload.get("device_id") or "A1").strip()

    base_samples = [
        {"timestamp": "2026-03-19T10:00:00+00:00", "device_id": device_id, "position": 10, "torque": 0.25, "temperature": 33.5, "power": 8.0, "setpoint": 10},
        {"timestamp": "2026-03-19T10:00:05+00:00", "device_id": device_id, "position": 24, "torque": 0.28, "temperature": 33.8, "power": 8.4, "setpoint": 25},
        {"timestamp": "2026-03-19T10:00:10+00:00", "device_id": device_id, "position": 39, "torque": 0.29, "temperature": 34.1, "power": 8.7, "setpoint": 40},
        {"timestamp": "2026-03-19T10:00:15+00:00", "device_id": device_id, "position": 55, "torque": 0.31, "temperature": 34.4, "power": 9.1, "setpoint": 55},
    ]

    mock_samples = [
        {"timestamp": "2026-03-19T10:10:00+00:00", "device_id": device_id, "position": 56, "torque": 0.42, "temperature": 36.1, "power": 10.2, "setpoint": 60},
        {"timestamp": "2026-03-19T10:10:05+00:00", "device_id": device_id, "position": 57, "torque": 0.48, "temperature": 37.0, "power": 10.8, "setpoint": 60},
        {"timestamp": "2026-03-19T10:10:10+00:00", "device_id": device_id, "position": 57.5, "torque": 0.52, "temperature": 38.2, "power": 11.4, "setpoint": 60},
    ]

    results = []
    for storage_kind, source_name, samples in (
        ("raw", "belimo_influx", base_samples),
        ("mock", "friction_spike", mock_samples),
    ):
        inserted_ids = [insert_sample(normalize_sample(sample), storage_kind, source_name) for sample in samples]
        recent_samples = get_recent_samples(device_id, storage_kind, DEFAULT_WINDOW_SIZE)
        features = compute_features(recent_samples)
        snapshot_id = insert_feature_snapshot(
            storage_kind=storage_kind,
            source_name=source_name,
            device_id=device_id,
            window_start=recent_samples[0]["timestamp"],
            window_end=recent_samples[-1]["timestamp"],
            sample_count=len(recent_samples),
            features=features,
        )
        if storage_kind == "raw":
            baseline = get_baseline_profile(device_id)
            upsert_baseline_profile(
                device_id,
                update_baseline(
                    baseline,
                    latest_features=features,
                    latest_temperature=recent_samples[-1]["temperature"],
                ),
            )

        results.append(
            {
                "storage_kind": storage_kind,
                "source_name": source_name,
                "inserted_sample_ids": inserted_ids,
                "feature_snapshot_id": snapshot_id,
                "features": features,
            }
        )

    return jsonify({"status": "success", "device_id": device_id, "results": results})


@feature_bp.record_once
def on_load(*_: object) -> None:
    init_db()
