#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from influxdb_client import InfluxDBClient


#DEFAULT_URL = "http://192.168.3.14:8086"
DEFAULT_URL = "http://192.168.5.14:8086"
DEFAULT_TOKEN = "pf-OGC6AQFmKy64gOzRM12DZrCuavnWeMgRZ2kDMOk8LYK22evDJnoyKGcmY49EgT8HnMDE9GPQeg30vXeHsRQ=="
DEFAULT_ORG = "belimo"
DEFAULT_BUCKET = "actuator-data"
DEFAULT_MEASUREMENT = "measurements"
DEFAULT_FIELDS = [
    "feedback_position_%",
    "setpoint_position_%",
    "motor_torque_Nmm",
    "power_W",
    "internal_temperature_deg_C",
    "rotation_direction",
    "test_number",
]


should_stop = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect Belimo telemetry from InfluxDB and save it to CSV, JSON, or JSONL."
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="InfluxDB URL")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="InfluxDB token")
    parser.add_argument("--org", default=DEFAULT_ORG, help="InfluxDB org")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="InfluxDB bucket")
    parser.add_argument("--measurement", default=DEFAULT_MEASUREMENT, help="Measurement to query")
    parser.add_argument(
        "--format",
        choices=["csv", "json", "jsonl"],
        default="csv",
        help="Output file format",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path, for example data/belimo_capture.csv",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=2.0,
        help="How often to poll InfluxDB for new rows",
    )
    parser.add_argument(
        "--lookback-seconds",
        type=int,
        default=180,
        help="Initial lookback window for the first query",
    )
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=0,
        help="Optional max duration. Use 0 to run until Ctrl+C",
    )
    parser.add_argument(
        "--fields",
        nargs="+",
        default=DEFAULT_FIELDS,
        help="Fields to fetch from the measurement",
    )
    parser.add_argument("--device-id", default="BELIMO-8", help="Device identifier written to every saved row")
    parser.add_argument("--test-id", default="", help="Run identifier written to every saved row")
    parser.add_argument("--waveform-type", default="", help="Waveform label written to every saved row")
    parser.add_argument("--bias", type=float, default=None, help="Bias metadata written to every saved row")
    parser.add_argument("--amplitude", type=float, default=None, help="Amplitude metadata written to every saved row")
    parser.add_argument("--frequency", type=float, default=None, help="Frequency metadata written to every saved row")
    parser.add_argument("--test-purpose", default="", help="Purpose label written to every saved row")
    parser.add_argument("--quality-label", default="unreviewed", help="Initial quality label written to every saved row")
    parser.add_argument("--notes", default="", help="Free-text notes written to every saved row")
    return parser.parse_args()


def handle_stop(_signum: int, _frame: Any) -> None:
    global should_stop
    should_stop = True


def build_query(
    *,
    bucket: str,
    measurement: str,
    fields: list[str],
    start_expression: str,
) -> str:
    field_filter = " or ".join(f'r["_field"] == "{field}"' for field in fields)
    return f"""
from(bucket: "{bucket}")
  |> range(start: {start_expression})
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
  |> filter(fn: (r) => {field_filter})
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
"""


def query_rows(
    client: InfluxDBClient,
    *,
    org: str,
    bucket: str,
    measurement: str,
    fields: list[str],
    start_expression: str,
) -> list[dict[str, Any]]:
    query = build_query(
        bucket=bucket,
        measurement=measurement,
        fields=fields,
        start_expression=start_expression,
    )
    query_api = client.query_api()
    tables = query_api.query(query=query, org=org)

    rows: list[dict[str, Any]] = []
    for table in tables:
        for record in table.records:
            values = dict(record.values)
            row = {"timestamp": _normalize_time(values["_time"])}
            for field in fields:
                row[field] = values.get(field)
            rows.append(row)
    return rows


def _normalize_time(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value)


def attach_metadata(row: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    enriched = dict(row)
    enriched["device_id"] = args.device_id
    enriched["test_id"] = args.test_id
    enriched["waveform_type"] = args.waveform_type
    enriched["bias"] = args.bias
    enriched["amplitude"] = args.amplitude
    enriched["frequency"] = args.frequency
    enriched["test_purpose"] = args.test_purpose
    enriched["quality_label"] = args.quality_label
    enriched["notes"] = args.notes
    enriched["cycle_id"] = None
    return enriched


def append_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def append_json(path: Path, rows: list[dict[str, Any]]) -> None:
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
    else:
        existing = []
    existing.extend(rows)
    path.write_text(json.dumps(existing, indent=2, ensure_ascii=True), encoding="utf-8")


def main() -> int:
    args = parse_args()
    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    fieldnames = [
        "timestamp",
        "device_id",
        "test_id",
        "waveform_type",
        "bias",
        "amplitude",
        "frequency",
        "test_purpose",
        "quality_label",
        "notes",
        "cycle_id",
        *args.fields,
    ]
    started_at = time.time()
    last_seen_timestamp: str | None = None
    total_saved = 0

    print(f"Collecting Belimo data from {args.url}")
    print(f"Saving to {output_path} as {args.format}")
    print("Press Ctrl+C to stop.\n")

    with InfluxDBClient(url=args.url, token=args.token, org=args.org, verify_ssl=False) as client:
        while not should_stop:
            start_expression = (
                f'time(v: "{last_seen_timestamp}")'
                if last_seen_timestamp is not None
                else f"-{args.lookback_seconds}s"
            )
            try:
                rows = query_rows(
                    client,
                    org=args.org,
                    bucket=args.bucket,
                    measurement=args.measurement,
                    fields=args.fields,
                    start_expression=start_expression,
                )
            except Exception as error:
                print(f"Query failed: {error}", file=sys.stderr)
                return 1

            new_rows = []
            for row in rows:
                current_timestamp = row["timestamp"]
                if last_seen_timestamp is not None and current_timestamp <= last_seen_timestamp:
                    continue
                new_rows.append(attach_metadata(row, args))

            if new_rows:
                if args.format == "csv":
                    append_csv(output_path, new_rows, fieldnames)
                elif args.format == "jsonl":
                    append_jsonl(output_path, new_rows)
                else:
                    append_json(output_path, new_rows)

                total_saved += len(new_rows)
                last_seen_timestamp = new_rows[-1]["timestamp"]
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] saved {len(new_rows)} rows "
                    f"(total={total_saved}, latest={last_seen_timestamp})"
                )

            if args.duration_seconds > 0 and (time.time() - started_at) >= args.duration_seconds:
                break

            time.sleep(args.poll_seconds)

    print(f"\nDone. Saved {total_saved} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
