#!/usr/bin/env python3
"""
edge/edge_gateway.py
────────────────────
Belimo Smart Actuator — Edge MQTT Gateway
Runs INDEPENDENTLY of app.py — use its own venv and edge/.env.edge

Architecture:
  InfluxDB (live) ──► [Poller Thread]
                            │
                     [Field Mapper + Delta Engine]
                            │
                     [Local Deque Buffer (10 readings)]
                            │
              ┌─────────────┴──────────────┐
              ▼                            ▼
    MQTT Publish                    HTTP POST to Flask
    belimo/api/v1/telemetry         /api/features/ingest
              │
    MQTT Subscribe
    belimo/<device>/commands

Usage:
  cd <repo-root>
  cp edge/.env.edge.example edge/.env.edge   # fill in MQTT_HOST etc.
  python -m edge.edge_gateway
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from concurrent.futures import ThreadPoolExecutor

import requests
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone

COMMAND_TIMESTAMP = datetime.fromtimestamp(0, tz=timezone.utc)

# ── paho-mqtt v2 compatibility shim ──────────────────────────────────────────
try:
    import paho.mqtt.client as mqtt
    from paho.mqtt.enums import CallbackAPIVersion  # paho >= 2.0
    _PAHO_V2 = True
except ImportError:
    import paho.mqtt.client as mqtt  # type: ignore[no-redef]
    _PAHO_V2 = False

# ── Bootstrap ─────────────────────────────────────────────────────────────────
_ENV_PATH = Path(__file__).resolve().parent / ".env.edge"
load_dotenv(_ENV_PATH)          # edge/.env.edge takes priority
load_dotenv(override=False)     # fall back to root .env

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger("edge_gateway")

# ── Config ────────────────────────────────────────────────────────────────────
MQTT_HOST        = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT        = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME    = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD    = os.getenv("MQTT_PASSWORD", "")
MQTT_TLS         = os.getenv("MQTT_TLS", "false").lower() == "true"

DEVICE_ID        = os.getenv("DEVICE_ID", "actuator-01")
TELEMETRY_TOPIC  = "belimo/api/v1/telemetry"
COMMANDS_TOPIC   = f"belimo/{DEVICE_ID}/commands"

#INFLUX_URL       = os.getenv("INFLUX_URL", "http://192.168.3.14:8086")
INFLUX_URL       = os.getenv("INFLUX_URL", "http://192.168.5.14:8086")
INFLUX_TOKEN     = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG       = os.getenv("INFLUX_ORG", "belimo")
INFLUX_BUCKET    = os.getenv("INFLUX_BUCKET", "actuator-data")
INFLUX_MEAS      = os.getenv("INFLUX_MEASUREMENT", "measurements")
INFLUX_LOOKBACK  = int(os.getenv("INFLUX_LOOKBACK_SECONDS", "30"))

FLASK_INGEST_URL = os.getenv("FLASK_INGEST_URL", "http://localhost:5000/api/features/ingest")
FLASK_BASELINE_URL = os.getenv("FLASK_BASELINE_URL", "http://localhost:5000/api/baseline-model/evaluate/combined")
FLASK_TOKEN      = os.getenv("FLASK_TOKEN", "starthack_front_2026_allow")

# ── Waveform context — sent to /api/baseline-model/evaluate/combined
WAVEFORM_TYPE      = os.getenv("WAVEFORM_TYPE", "square")
WAVEFORM_BIAS      = float(os.getenv("WAVEFORM_BIAS", "50.0"))
WAVEFORM_AMPLITUDE = float(os.getenv("WAVEFORM_AMPLITUDE", "20.0"))
WAVEFORM_FREQUENCY = float(os.getenv("WAVEFORM_FREQUENCY", "0.02"))
EVAL_BATCH_SIZE    = int(os.getenv("EVAL_BATCH_SIZE", "10"))

# ── Telegram config ───────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

POLL_SECONDS     = float(os.getenv("POLL_SECONDS", "2.0"))
BUFFER_SIZE      = int(os.getenv("BUFFER_SIZE", "10"))

# Per-signal anomaly thresholds (% change between consecutive readings)
THRESH_TORQUE   = float(os.getenv("TORQUE_ANOMALY_THRESHOLD_PCT",    "10.0"))
THRESH_TEMP     = float(os.getenv("TEMP_ANOMALY_THRESHOLD_PCT",      "5.0"))
THRESH_POWER    = float(os.getenv("POWER_ANOMALY_THRESHOLD_PCT",     "15.0"))
THRESH_POSITION = float(os.getenv("POSITION_ANOMALY_THRESHOLD_PCT",  "20.0"))
TEMP_DELTA_PERSISTENCE = int(os.getenv("TEMP_DELTA_PERSISTENCE", "3"))

# ── InfluxDB field names ───────────────────────────────────────────────────────
INFLUX_FIELDS = [
    "feedback_position_%",
    "setpoint_position_%",
    "motor_torque_Nmm",
    "power_W",
    "internal_temperature_deg_C",
    "rotation_direction",
]

# ── Shared state ──────────────────────────────────────────────────────────────
_buffer: deque[dict[str, Any]] = deque(maxlen=BUFFER_SIZE)
_should_stop = threading.Event()
_mqtt_client: mqtt.Client | None = None
_mqtt_connected = threading.Event()
_influx_write_api: Any | None = None  # Used to send commands back to the actuator

# Previous-reading state for all 4 delta signals
_prev: dict[str, float | None] = {
    "torque":      None,
    "temperature": None,
    "power":       None,
    "position":    None,
}
_temp_exceed_streak: int = 0

# Rolling batch for evaluate/combined — keeps last EVAL_BATCH_SIZE readings
_eval_batch: deque[dict[str, Any]] = deque(maxlen=EVAL_BATCH_SIZE)
_eval_batch_count: int = 0   # total readings since last evaluation POST

# Thread pool for non-blocking HTTP requests to Azure
_http_executor = ThreadPoolExecutor(max_workers=10)

# ═══════════════════════════════════════════════════════════════════════════════
# ── Field Mapper ──────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _influx_row_to_reading(row: dict[str, Any], timestamp: str) -> dict[str, Any] | None:
    """Convert one raw InfluxDB pivot row into a normalised reading dict."""
    try:
        torque_nmm = float(row.get("motor_torque_Nmm") or 0.0)
        reading = {
            "device_id":   DEVICE_ID,
            "timestamp":   timestamp,
            # Flask flat schema uses: position, torque, temperature, power, setpoint
            "position":    float(row.get("feedback_position_%") or 0.0),
            "setpoint":    float(row.get("setpoint_position_%") or 0.0),
            "torque":      round(torque_nmm / 1000.0, 6),   # Nmm → Nm scale
            "temperature": float(row.get("internal_temperature_deg_C") or 0.0),
            "power":       float(row.get("power_W") or 0.0),
            # extra metadata kept in-memory but not sent to Flask
            "_torque_nmm":           torque_nmm,
            "_rotation_direction":   row.get("rotation_direction"),
        }
        return reading
    except (TypeError, ValueError) as exc:
        logger.warning("Row mapping failed: %s | row=%s", exc, row)
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# ── Local Delta Engine  (Torque τ · Temperature T · Power P · Position r) ────
# ═══════════════════════════════════════════════════════════════════════════════

_SIGNAL_THRESHOLDS: dict[str, float] = {
    "torque":      THRESH_TORQUE,
    "temperature": THRESH_TEMP,
    "power":       THRESH_POWER,
    "position":    THRESH_POSITION,
}

# Signal labels for log output — mapped to official Belimo InfluxDB field names:
#   τ → motor_torque_Nmm      (torque in Nmm)
#   T → internal_temperature_deg_C  (PCB temperature in °C)
#   P → power_W               (electrical motor power in W — no pressure field exists on this actuator)
#   r → feedback_position_%   (shaft position 0–100%)
_SIGNAL_LABELS: dict[str, str] = {
    "torque":      "τ",
    "temperature": "T",
    "power":       "P(W)",   # electrical power, NOT pressure
    "position":    "r",
}


def _delta_pct(current: float, previous: float | None) -> tuple[float, bool, str]:
    """
    Compute percentage change between current and previous value.
    Returns (delta_pct, anomaly_flag, signal_key) — internal helper.
    """
    if previous is None or previous == 0.0:
        return 0.0, False, ""
    return round(abs((current - previous) / previous) * 100.0, 2), False, ""


def _compute_all_deltas(reading: dict[str, Any]) -> dict[str, Any]:
    """
    Compute delta_pct and anomaly_flag for all 4 monitored signals.
    Updates _prev in-place.
    Returns a metadata dict ready for the MQTT payload.
    """
    deltas: dict[str, float] = {}
    anomalies: dict[str, bool] = {}
    any_anomaly = False
    global _temp_exceed_streak

    for signal, threshold in _SIGNAL_THRESHOLDS.items():
        current = reading[signal]
        prev    = _prev[signal]

        if prev is None or prev == 0.0:
            d_pct, exceeds = 0.0, False
        else:
            d_pct = round(abs((current - prev) / prev) * 100.0, 2)
            exceeds = d_pct > threshold

        if signal == "temperature":
            _temp_exceed_streak = (_temp_exceed_streak + 1) if exceeds else 0
            flag = _temp_exceed_streak >= TEMP_DELTA_PERSISTENCE
        else:
            flag = exceeds

        deltas[signal]   = d_pct
        anomalies[signal] = flag
        _prev[signal]     = current

        if flag:
            label = _SIGNAL_LABELS[signal]
            logger.warning(
                "⚠️  ANOMALY [%s] %s — Δ=%.1f%% (threshold=%.1f%%)",
                label, signal.upper(), d_pct, threshold,
            )
        any_anomaly = any_anomaly or flag

    return {
        "status":            "anomaly" if any_anomaly else "normal",
        "anomaly_flag":      any_anomaly,
        # per-signal deltas
        "torque_delta_pct":      deltas["torque"],
        "temperature_delta_pct": deltas["temperature"],
        "power_delta_pct":       deltas["power"],
        "position_delta_pct":    deltas["position"],
        "temperature_exceed_streak": _temp_exceed_streak,
        # per-signal flags
        "anomalies": anomalies,
        "rotation_direction": reading.get("_rotation_direction"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ── MQTT Build Payload ────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _build_mqtt_payload(reading: dict[str, Any], meta: dict[str, Any]) -> str:
    """
    Build the MQTT wire payload (nested envelope format).
    Topic: belimo/api/v1/telemetry
    """
    payload = {
        "device_id": reading["device_id"],
        "timestamp": reading["timestamp"],
        "data": {
            "torque_signed":  reading["torque"],
            "temperature_c":  reading["temperature"],
            "position_pct":   reading["position"],
            "setpoint_pct":   reading["setpoint"],
            "power_w":        reading["power"],
        },
        "metadata": meta,
    }
    return json.dumps(payload, ensure_ascii=True)


# ═══════════════════════════════════════════════════════════════════════════════
# ── Flask HTTP Bridge (Async via ThreadPool) ─────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _flask_ingest_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {FLASK_TOKEN}",
        "Content-Type":  "application/json",
    }


def _do_post_ingest_batch(readings: list[dict[str, Any]]) -> None:
    """Internal task for the thread pool to POST a batch of readings."""
    samples = []
    for r in readings:
        samples.append({
            "device_id":   r["device_id"],
            "timestamp":   r["timestamp"],
            "position":    r["position"],
            "torque":      r["torque"],
            "temperature": r["temperature"],
            "power":       r["power"],
            "setpoint":    r["setpoint"],
        })

    body = {
        "storage_kind": "raw",
        "source_name":  "edge_mqtt",
        "samples":      samples,
    }
    try:
        resp = requests.post(FLASK_INGEST_URL, json=body, headers=_flask_ingest_headers(), timeout=5)
        if resp.status_code != 200:
            logger.debug("Flask ingest non-200: %s", resp.status_code)
    except Exception:
        # Silently fail background thread, the main thread handles buffering if needed
        pass


def _post_to_flask_ingest_batch(readings: list[dict[str, Any]]) -> bool:
    """Submits a list of readings to the thread pool for ingestion."""
    if not readings:
        return True
    # Pass a copy to avoid mutation
    _http_executor.submit(_do_post_ingest_batch, list(readings))
    return True



def _flush_buffer_to_flask() -> None:
    """Drain the local buffer in a batch."""
    if not _buffer:
        return
    logger.info("Flushing %d buffered reading(s) to Flask backend …", len(_buffer))
    batch = []
    while _buffer:
        batch.append(_buffer.popleft())
    _post_to_flask_ingest_batch(batch)


def _do_post_eval_batch(telemetry_series: list[dict[str, Any]]) -> None:
    """Background task to POST the eval batch."""
    body = {
        "device_id":    DEVICE_ID,
        "waveform_type": WAVEFORM_TYPE,
        "waveform": {
            "waveform_type": WAVEFORM_TYPE,
            "bias":          WAVEFORM_BIAS,
            "amplitude":     WAVEFORM_AMPLITUDE,
            "frequency":     WAVEFORM_FREQUENCY,
        },
        "telemetry_series": telemetry_series,
    }
    try:
        resp = requests.post(FLASK_BASELINE_URL, json=body, headers=_flask_ingest_headers(), timeout=10)
        if resp.status_code == 200:
            result = resp.json().get("evaluation", {})
            summary = result.get("summary", {})
            status  = summary.get("status", "?")
            insight = summary.get("insight", "")
            logger.info("🧠 BASELINE EVAL [%d samples] → status=%s | %s", len(telemetry_series), status.upper(), insight)
            if status in ("warning", "anomaly"):
                logger.warning("⚠️  BASELINE DEVIATION DETECTED: %s", insight)
                _send_telegram_alert(f"🚨 *ANOMALY (Baseline)* 🚨\n\nDevice: `{DEVICE_ID}`\nStatus: `{status.upper()}`\n\n{insight}")
    except Exception as exc:
        logger.debug("Failed to post eval batch: %s", exc)


def _post_evaluation_batch() -> None:
    """Submits the current _eval_batch snapshot to the thread pool."""
    global _eval_batch_count
    if len(_eval_batch) < 2:
        return

    # Snapshot the queue for the background thread
    telemetry_series = []
    for r in list(_eval_batch):
        telemetry_series.append({
            "timestamp":    r["timestamp"],
            "torque_signed": r["torque"],
            "temperature_c": r["temperature"],
            "position_pct":  r["position"],
            "feedback_position_%": r["position"],
        })

    _http_executor.submit(_do_post_eval_batch, telemetry_series)
    _eval_batch_count = 0


# ── Telegram Alert ────────────────────────────────────────────────────────────

def _do_telegram_alert(msg: str) -> None:
    """Runs inside the thread pool to prevent blocking."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as exc:
        logger.debug("Failed to send Telegram alert: %s", exc)

def _send_telegram_alert(msg: str) -> None:
    """Queue a telegram alert to the background thread pool."""
    _http_executor.submit(_do_telegram_alert, msg)

# ═══════════════════════════════════════════════════════════════════════════════
# ── MQTT Callbacks ────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _on_connect(client: mqtt.Client, userdata: Any, flags: Any, rc: Any, *args: Any) -> None:
    if rc == 0 or (hasattr(rc, "value") and rc.value == 0):
        logger.info("✅ MQTT connected → broker=%s:%s", MQTT_HOST, MQTT_PORT)
        client.subscribe(COMMANDS_TOPIC, qos=1)
        logger.info("Subscribed to commands topic: %s", COMMANDS_TOPIC)
        _mqtt_connected.set()
        # Drain buffer now that we're reconnected
        _flush_buffer_to_flask()
    else:
        logger.error("MQTT connection refused — rc=%s", rc)


def _on_disconnect(client: mqtt.Client, userdata: Any, rc: Any, *args: Any) -> None:
    _mqtt_connected.clear()
    logger.warning("MQTT disconnected — rc=%s. Will reconnect automatically.", rc)


def _on_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
    try:
        payload_str = msg.payload.decode("utf-8")
        command = json.loads(payload_str)
        logger.info("📥 [CMD] Received command on %s: %s", msg.topic, json.dumps(command))
        _handle_command(command)
    except json.JSONDecodeError:
        logger.warning("[CMD] Non-JSON command received: %s", msg.payload)


def _handle_command(command: dict[str, Any]) -> None:
    """
    Execute a remote command received via MQTT.
    Extend this with real actuator control logic.
    """
    action = command.get("action", "").lower()
    if action == "stop":
        logger.info("[CMD] STOP signal received — edge is flagging for shutdown.")
        _should_stop.set()
    elif action == "ping":
        logger.info("[CMD] PING received — edge is healthy.")
    elif action == "set_setpoint":
        value = float(command.get("value", 0.0))
        logger.info("[CMD] SET_SETPOINT → %.1f%%", value)
        if _influx_write_api:
            try:
                # The logger script reads from _process with a fixed epoch timestamp
                p = Point("_process") \
                    .field("setpoint_position_%", float(value)) \
                    .field("test_number", -1) \
                    .time(COMMAND_TIMESTAMP, WritePrecision.MS)
                _influx_write_api.write(bucket=INFLUX_BUCKET, record=p)
                logger.info("   ↳ Written to InfluxDB _process measurement successfully.")
            except Exception as exc:
                logger.error("   ↳ Failed to write command to InfluxDB: %s", exc)
        else:
            logger.warning("   ↳ Cannot write command — InfluxDB write API not initialized.")
    else:
        logger.info("[CMD] Unknown action '%s' — logged and ignored.", action)


# ═══════════════════════════════════════════════════════════════════════════════
# ── MQTT Client Setup ─────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _build_mqtt_client() -> mqtt.Client:
    client_id = f"edge_gateway_{DEVICE_ID}"

    if _PAHO_V2:
        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )
    else:
        client = mqtt.Client(client_id=client_id)  # type: ignore[call-arg]

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or None)
    if MQTT_TLS:
        client.tls_set()

    client.on_connect    = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_message    = _on_message

    # Automatic reconnect: wait 1 s on first retry, up to 60 s
    client.reconnect_delay_set(min_delay=1, max_delay=60)
    return client


# ═══════════════════════════════════════════════════════════════════════════════
# ── InfluxDB Poller ───────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _build_flux_query(start_expr: str) -> str:
    field_filter = " or ".join(f'r["_field"] == "{f}"' for f in INFLUX_FIELDS)
    return f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {start_expr})
  |> filter(fn: (r) => r["_measurement"] == "{INFLUX_MEAS}")
  |> filter(fn: (r) => {field_filter})
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
"""


def _query_influx(
    influx: InfluxDBClient, start_expr: str
) -> list[dict[str, Any]]:
    query_api = influx.query_api()
    tables = query_api.query(query=_build_flux_query(start_expr), org=INFLUX_ORG)
    rows = []
    for table in tables:
        for record in table.records:
            values = dict(record.values)
            ts_raw = values.get("_time")
            if isinstance(ts_raw, datetime):
                ts_str = ts_raw.astimezone(UTC).isoformat()
            else:
                ts_str = str(ts_raw)
            row: dict[str, Any] = {"_time_str": ts_str}
            for field in INFLUX_FIELDS:
                row[field] = values.get(field)
            rows.append(row)
    return rows


# ═══════════════════════════════════════════════════════════════════════════════
# ── Main Loop ─────────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _poll_and_publish(influx: InfluxDBClient, client: mqtt.Client) -> None:
    """
    Main polling loop.
    - Queries InfluxDB for new rows since last_seen_timestamp
    - Maps fields, computes delta, publishes to MQTT, posts to Flask
    - Buffers readings if Flask is unavailable
    """
    last_seen_ts: str | None = None

    logger.info("Edge polling started — device=%s, poll=%.1fs", DEVICE_ID, POLL_SECONDS)
    logger.info("MQTT telemetry  → %s", TELEMETRY_TOPIC)
    logger.info("MQTT commands   ← %s", COMMANDS_TOPIC)
    logger.info("Flask ingest    → %s", FLASK_INGEST_URL)

    while not _should_stop.is_set():
        loop_start = time.monotonic()

        # ── 1. Build Flux start expression ────────────────────────────────────
        if last_seen_ts is not None:
            start_expr = f'time(v: "{last_seen_ts}")'
        else:
            start_expr = f"-{INFLUX_LOOKBACK}s"

        # ── 2. Query InfluxDB ──────────────────────────────────────────────────
        try:
            rows = _query_influx(influx, start_expr)
        except Exception as exc:
            logger.error("InfluxDB query failed: %s", exc)
            time.sleep(POLL_SECONDS)
            continue

        # ── 3. Filter already-seen rows ────────────────────────────────────────
        # Identify the absolute newest timestamp across all returned rows to fast-forward if needed
        all_new_ts = [r["_time_str"] for r in rows if last_seen_ts is None or r["_time_str"] > last_seen_ts]

        if len(all_new_ts) > 100:
            # We are severely backlogged (e.g. 10+ seconds behind).
            # Fast forward to the most recent 20 rows to stay real-time.
            logger.warning("⚠️ Backlog detected (%d rows). Fast-forwarding to real-time.", len(all_new_ts))
            cutoff_ts = sorted(all_new_ts)[-20]
            new_rows = [r for r in rows if r["_time_str"] >= cutoff_ts]
        else:
            new_rows = [r for r in rows if last_seen_ts is None or r["_time_str"] > last_seen_ts]

        if new_rows:
            logger.debug("InfluxDB → %d new row(s)", len(new_rows))

        batch_to_ingest = []

        for row in new_rows:
            ts = row["_time_str"]
            last_seen_ts = ts

            # ── 4. Map fields ──────────────────────────────────────────────────
            reading = _influx_row_to_reading(row, ts)
            if reading is None:
                continue

            # ── 5. Local delta / anomaly detection (τ T P r) ─────────────────
            meta = _compute_all_deltas(reading)
            anomaly = meta["anomaly_flag"]

            # If anomaly detected locally, shoot an alert
            if anomaly:
                alert_text = (
                    f"⚠️ *LOCAL THRESHOLD ALERT* ⚠️\n\n"
                    f"Device: `{DEVICE_ID}`\n"
                    f"Time: `{ts}`\n"
                    f"Values:\n"
                    f"• Pos: {reading['position']}%\n"
                    f"• Pwr: {reading['power']}W (Δ {meta['power_delta_pct']}%)"
                )
                _send_telegram_alert(alert_text)

            # Accumulate for batch ingest
            batch_to_ingest.append(reading)

            # ── 6. MQTT publish (fire-and-forget, non-blocking) ─────────────────
            mqtt_payload = _build_mqtt_payload(reading, meta)
            if _mqtt_connected.is_set():
                result = client.publish(TELEMETRY_TOPIC, mqtt_payload, qos=1)
                if result.rc == 0:
                    logger.info(
                        "📡 MQTT [%s] | pos=%.1f%% τ=%.4fNm T=%.1f°C P=%.1fW "
                        "| Δτ=%.1f%% ΔT=%.1f%% ΔP=%.1f%% Δr=%.1f%% anomaly=%s",
                        ts, reading["position"], reading["torque"],
                        reading["temperature"], reading["power"],
                        meta["torque_delta_pct"], meta["temperature_delta_pct"],
                        meta["power_delta_pct"],  meta["position_delta_pct"],
                        anomaly,
                    )
                else:
                    logger.warning("MQTT publish failed rc=%s — buffering", result.rc)
                    _buffer.append(reading)
            else:
                logger.warning("MQTT disconnected — buffering reading (buf=%d)", len(_buffer))
                _buffer.append(reading)

            # ── 7. Accumulate eval batch ──────────────────────────────────
            global _eval_batch_count
            _eval_batch.append(reading)
            _eval_batch_count += 1
            if _eval_batch_count >= EVAL_BATCH_SIZE:
                _post_evaluation_batch()

        # Submit the whole polling cycle's readings to Flask in ONE request
        if batch_to_ingest:
            _post_to_flask_ingest_batch(batch_to_ingest)

        # ── 8. Sleep remainder of poll interval ────────────────────────────────
        elapsed = time.monotonic() - loop_start
        sleep_s = max(0.0, POLL_SECONDS - elapsed)
        _should_stop.wait(timeout=sleep_s)


# ═══════════════════════════════════════════════════════════════════════════════
# ── Signal Handler ────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _handle_signal(signum: int, _frame: Any) -> None:
    logger.info("Signal %s received — shutting down edge gateway …", signum)
    _should_stop.set()


# ═══════════════════════════════════════════════════════════════════════════════
# ── Entry Point ───────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> int:
    global _mqtt_client

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info("═══════════════════════════════════════════════")
    logger.info("  Belimo Edge Gateway — starting up")
    logger.info("  Device     : %s", DEVICE_ID)
    logger.info("  Broker     : %s:%s (TLS=%s)", MQTT_HOST, MQTT_PORT, MQTT_TLS)
    logger.info("  InfluxDB   : %s | bucket=%s", INFLUX_URL, INFLUX_BUCKET)
    logger.info("  Flask API  : %s", FLASK_INGEST_URL)
    logger.info("  Buffer     : %d readings max", BUFFER_SIZE)
    logger.info("  Poll       : %.1fs", POLL_SECONDS)
    logger.info("  Thresholds : τ=%.0f%% T=%.0f%% P=%.0f%% r=%.0f%%",
                THRESH_TORQUE, THRESH_TEMP, THRESH_POWER, THRESH_POSITION)
    logger.info("  Buffer     : %d readings max", BUFFER_SIZE)
    logger.info("═══════════════════════════════════════════════")

    # ── Build and start MQTT client in background thread ──────────────────────
    _mqtt_client = _build_mqtt_client()
    try:
        _mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    except OSError as exc:
        logger.error("Cannot connect to MQTT broker %s:%s — %s", MQTT_HOST, MQTT_PORT, exc)
        logger.warning("Continuing in HTTP-only mode (MQTT unavailable)")

    _mqtt_client.loop_start()   # Runs MQTT I/O in a background thread

    # ── Wait for MQTT connection (optional — continue anyway after 5 s) ───────
    connected = _mqtt_connected.wait(timeout=5.0)
    if not connected:
        logger.warning("MQTT broker not reachable yet — will retry in background. Proceeding with polling.")

    # ── Connect to InfluxDB ───────────────────────────────────────────────────
    influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, verify_ssl=False)
    
    global _influx_write_api
    _influx_write_api = influx.write_api(write_options=SYNCHRONOUS)

    try:
        _poll_and_publish(influx, _mqtt_client)
    finally:
        logger.info("Edge gateway shutting down …")
        influx.close()
        _mqtt_client.loop_stop()
        _mqtt_client.disconnect()
        logger.info("Edge gateway stopped. Goodbye. 👋")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
