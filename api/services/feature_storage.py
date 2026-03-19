import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "mvp.db"


def get_db_path() -> Path:
    raw_path = os.environ.get("MVP_DB_PATH", "").strip()
    if raw_path:
        return Path(raw_path)
    return DEFAULT_DB_PATH


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def init_db() -> None:
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                device_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                position REAL NOT NULL,
                torque REAL NOT NULL,
                temperature REAL NOT NULL,
                power REAL NOT NULL,
                setpoint REAL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS mock_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario TEXT NOT NULL,
                device_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                position REAL NOT NULL,
                torque REAL NOT NULL,
                temperature REAL NOT NULL,
                power REAL NOT NULL,
                setpoint REAL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS feature_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                storage_kind TEXT NOT NULL,
                source_name TEXT NOT NULL,
                device_id TEXT NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                sample_count INTEGER NOT NULL,
                delta_position REAL,
                movement_duration REAL,
                movement_speed REAL,
                torque_per_position REAL,
                avg_torque_movement REAL,
                torque_variance REAL,
                temp_rate REAL,
                temp_vs_torque_ratio REAL,
                position_variance REAL,
                oscillation_score REAL,
                energy_per_movement REAL,
                features_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS baseline_profiles (
                device_id TEXT PRIMARY KEY,
                sample_count INTEGER NOT NULL,
                avg_torque_per_position REAL,
                typical_movement_speed REAL,
                normal_temperature_min REAL,
                normal_temperature_max REAL,
                updated_at TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_raw_samples_device_time ON raw_samples(device_id, timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_mock_samples_device_time ON mock_samples(device_id, timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_feature_snapshots_device_time ON feature_snapshots(device_id, window_end)"
        )

        connection.commit()


@contextmanager
def db_connection():
    init_db()
    connection = sqlite3.connect(get_db_path())
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def insert_sample(sample: dict[str, Any], storage_kind: str, source_name: str) -> int:
    table_name = "raw_samples" if storage_kind == "raw" else "mock_samples"
    source_column = "source" if storage_kind == "raw" else "scenario"
    created_at = _utc_now()

    with db_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            INSERT INTO {table_name} (
                {source_column},
                device_id,
                timestamp,
                position,
                torque,
                temperature,
                power,
                setpoint,
                payload_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_name,
                sample["device_id"],
                sample["timestamp"],
                sample["position"],
                sample["torque"],
                sample["temperature"],
                sample["power"],
                sample.get("setpoint"),
                json.dumps(sample, sort_keys=True),
                created_at,
            ),
        )
        return int(cursor.lastrowid)


def get_recent_samples(device_id: str, storage_kind: str, limit: int) -> list[dict[str, Any]]:
    table_name = "raw_samples" if storage_kind == "raw" else "mock_samples"

    with db_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT *
            FROM {table_name}
            WHERE device_id = ?
            ORDER BY timestamp DESC, id DESC
            LIMIT ?
            """,
            (device_id, limit),
        ).fetchall()

    samples = []
    for row in reversed(rows):
        samples.append(
            {
                "id": row["id"],
                "device_id": row["device_id"],
                "timestamp": row["timestamp"],
                "position": row["position"],
                "torque": row["torque"],
                "temperature": row["temperature"],
                "power": row["power"],
                "setpoint": row["setpoint"],
            }
        )
    return samples


def insert_feature_snapshot(
    *,
    storage_kind: str,
    source_name: str,
    device_id: str,
    window_start: str,
    window_end: str,
    sample_count: int,
    features: dict[str, Any],
) -> int:
    created_at = _utc_now()

    with db_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO feature_snapshots (
                storage_kind,
                source_name,
                device_id,
                window_start,
                window_end,
                sample_count,
                delta_position,
                movement_duration,
                movement_speed,
                torque_per_position,
                avg_torque_movement,
                torque_variance,
                temp_rate,
                temp_vs_torque_ratio,
                position_variance,
                oscillation_score,
                energy_per_movement,
                features_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                storage_kind,
                source_name,
                device_id,
                window_start,
                window_end,
                sample_count,
                features.get("delta_position"),
                features.get("movement_duration"),
                features.get("movement_speed"),
                features.get("torque_per_position"),
                features.get("avg_torque_movement"),
                features.get("torque_variance"),
                features.get("temp_rate"),
                features.get("temp_vs_torque_ratio"),
                features.get("position_variance"),
                features.get("oscillation_score"),
                features.get("energy_per_movement"),
                json.dumps(features, sort_keys=True),
                created_at,
            ),
        )
        return int(cursor.lastrowid)


def get_latest_feature_snapshot(device_id: str, storage_kind: str | None = None) -> dict[str, Any] | None:
    query = """
        SELECT *
        FROM feature_snapshots
        WHERE device_id = ?
    """
    params: list[Any] = [device_id]

    if storage_kind:
        query += " AND storage_kind = ?"
        params.append(storage_kind)

    query += " ORDER BY window_end DESC, id DESC LIMIT 1"

    with db_connection() as connection:
        row = connection.execute(query, tuple(params)).fetchone()

    if row is None:
        return None

    return {
        "id": row["id"],
        "storage_kind": row["storage_kind"],
        "source_name": row["source_name"],
        "device_id": row["device_id"],
        "window_start": row["window_start"],
        "window_end": row["window_end"],
        "sample_count": row["sample_count"],
        "features": json.loads(row["features_json"]),
        "created_at": row["created_at"],
    }


def get_baseline_profile(device_id: str) -> dict[str, Any] | None:
    with db_connection() as connection:
        row = connection.execute(
            "SELECT * FROM baseline_profiles WHERE device_id = ?",
            (device_id,),
        ).fetchone()

    if row is None:
        return None

    return {
        "device_id": row["device_id"],
        "sample_count": row["sample_count"],
        "avg_torque_per_position": row["avg_torque_per_position"],
        "typical_movement_speed": row["typical_movement_speed"],
        "normal_temperature_min": row["normal_temperature_min"],
        "normal_temperature_max": row["normal_temperature_max"],
        "updated_at": row["updated_at"],
    }


def upsert_baseline_profile(device_id: str, baseline: dict[str, Any]) -> dict[str, Any]:
    updated_at = _utc_now()

    with db_connection() as connection:
        connection.execute(
            """
            INSERT INTO baseline_profiles (
                device_id,
                sample_count,
                avg_torque_per_position,
                typical_movement_speed,
                normal_temperature_min,
                normal_temperature_max,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                sample_count = excluded.sample_count,
                avg_torque_per_position = excluded.avg_torque_per_position,
                typical_movement_speed = excluded.typical_movement_speed,
                normal_temperature_min = excluded.normal_temperature_min,
                normal_temperature_max = excluded.normal_temperature_max,
                updated_at = excluded.updated_at
            """,
            (
                device_id,
                baseline["sample_count"],
                baseline["avg_torque_per_position"],
                baseline["typical_movement_speed"],
                baseline["normal_temperature_min"],
                baseline["normal_temperature_max"],
                updated_at,
            ),
        )

    saved = dict(baseline)
    saved["device_id"] = device_id
    saved["updated_at"] = updated_at
    return saved
