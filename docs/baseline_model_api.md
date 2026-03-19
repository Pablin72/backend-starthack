# Baseline Model Comparison API

Base path:

- `/api/baseline-model`

Auth:

- `Authorization: Bearer starthack_front_2026_allow`
- or `X-API-Token: starthack_front_2026_allow`

## Endpoint Summary

- `GET /summary`
- `GET /report`
- `GET /position`
- `GET /torque`
- `GET /temperature`
- `GET /envelopes`
- `GET /diagnostics`
- `GET /waveforms/<waveform_type>`
- `POST /simulate/waveform`
- `POST /simulate/command-series`
- `POST /compare/run`
- `POST /compare/window`

## Request Contracts

### Simulate from waveform metadata

```json
{
  "waveform_type": "square",
  "bias": 50,
  "amplitude": 20,
  "frequency": 0.02,
  "duration_seconds": 60,
  "sample_count": 120
}
```

### Simulate from explicit command series

```json
{
  "waveform_type": "square",
  "timestamps": [0, 1, 2, 3, 4],
  "command_values": [70, 70, 70, 30, 30],
  "metadata": {
    "test_id": "P2_MID_SQUARE_001"
  }
}
```

### Compare a telemetry run against baseline

```json
{
  "waveform_type": "square",
  "timestamps": [0, 1, 2, 3, 4],
  "command_values": [70, 70, 70, 30, 30],
  "telemetry": {
    "position": [50.0, 52.3, 55.1, 53.0, 47.2],
    "torque": [0.63, 0.64, 0.70, 0.81, 0.79],
    "temperature": [26.5, 26.5, 26.6, 26.6, 26.7]
  },
  "align": true,
  "metadata": {
    "run_id": "demo-run-1"
  }
}
```

## Response Highlights

### Position payload

- opening delay
- opening time constant
- opening max velocity
- closing delay
- closing time constant
- closing max velocity
- model equation metadata

### Torque payload

- opening coefficients
- closing coefficients
- sign convention
- torque magnitude convention
- model equation metadata

### Temperature payload

- `alpha`
- `beta`
- ambient temperature
- model equation metadata

### Envelope payload

- waveform type
- reference trace
- lower/upper bounds
- envelope construction metadata

### Comparison payload

- aligned timestamps
- measured traces
- baseline traces
- residual traces
- envelope violation flags
- per-signal summaries
- direction summary
- overall health/anomaly score
- frontend-ready findings

## Comparison Flow

1. Load calibration artifacts from `data/calibration/healthy_model_report.json`
2. Build baseline response from either waveform metadata or command series
3. Optionally align baseline against telemetry using derivative cross-correlation
4. Compute residuals for position, torque, and temperature
5. Check envelope violations for each signal
6. Aggregate transparent metrics:
   - median absolute residual
   - RMSE
   - max absolute deviation
   - violation count
   - violation percentage
7. Produce an explainable health score and dashboard-friendly findings
