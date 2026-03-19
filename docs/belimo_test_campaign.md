# Belimo Healthy Identification Campaign

This campaign uses:

- `scripts/run_belimo_test.py` to write structured commands into `_process`
- `scripts/collect_belimo_data.py` to capture actuator telemetry from `measurements`

All runs assume:

- Raspberry Pi Wi-Fi connected: `BELIMO-8`
- backend environment activated:

```bash
cd "/Users/pabloarcos/Desktop/Start Hack/backend-starthack"
source .venv312/bin/activate
```

## File Naming Convention

Use one `test_id` per run and keep all three artifacts:

- telemetry CSV
- command log CSV
- run manifest JSON

Recommended pattern:

- telemetry: `data/campaign/<test_id>_telemetry.csv`
- command log: `data/campaign/<test_id>_commands.csv`
- manifest: `data/campaign/<test_id>_manifest.json`

## Operating Regions

- low bias: `20`
- mid bias: `50`
- high bias: `80`

## Amplitude Classes

- small: `10`
- moderate: `20`

## Frequency Classes

- low: `0.02`
- medium: `0.04`

## General Procedure Per Run

1. Open terminal A and start telemetry capture.
2. Open terminal B and start the command sequence.
3. Let both complete.
4. Add any visible notes to a campaign sheet.
5. Inspect whether the resulting CSV looks usable.

Terminal A template:

```bash
python scripts/collect_belimo_data.py \
  --output "data/campaign/<test_id>_telemetry.csv" \
  --format csv \
  --lookback-seconds 2 \
  --poll-seconds 1 \
  --duration-seconds <duration> \
  --device-id BELIMO-8 \
  --test-id <test_id> \
  --waveform-type <waveform> \
  --bias <bias> \
  --amplitude <amplitude> \
  --frequency <frequency> \
  --test-purpose "<purpose>" \
  --quality-label unreviewed \
  --notes "<notes>"
```

Terminal B template:

```bash
python scripts/run_belimo_test.py \
  --suite single \
  --test-number <test_number> \
  --waveform <waveform> \
  --bias <bias> \
  --amplitude <amplitude> \
  --frequency <frequency> \
  --duration-seconds <duration> \
  --poll-seconds 1 \
  --command-log "data/campaign/<test_id>_commands.csv" \
  --manifest-json "data/campaign/<test_id>_manifest.json"
```

## Phase 1 - Pre-check and Static Holds

Goal:

- verify synchronization
- estimate baseline noise
- observe settling at fixed operating points

### Run P1-Low Hold

Terminal A:

```bash
python scripts/collect_belimo_data.py \
  --output "data/campaign/P1_LOW_HOLD_001_telemetry.csv" \
  --format csv \
  --lookback-seconds 2 \
  --poll-seconds 1 \
  --duration-seconds 35 \
  --device-id BELIMO-8 \
  --test-id P1_LOW_HOLD_001 \
  --waveform-type constant \
  --bias 20 \
  --amplitude 0 \
  --frequency 0 \
  --test-purpose "static hold near closed for settling and noise baseline"
```

Terminal B:

```bash
python scripts/run_belimo_test.py \
  --suite single \
  --test-number 1001 \
  --waveform constant \
  --bias 20 \
  --amplitude 0 \
  --frequency 0 \
  --duration-seconds 30 \
  --poll-seconds 1 \
  --command-log "data/campaign/P1_LOW_HOLD_001_commands.csv" \
  --manifest-json "data/campaign/P1_LOW_HOLD_001_manifest.json"
```

### Run P1-Mid Hold

Use the same commands with:

- `test_id=P1_MID_HOLD_001`
- `test_number=1002`
- `bias=50`

### Run P1-High Hold

Use the same commands with:

- `test_id=P1_HIGH_HOLD_001`
- `test_number=1003`
- `bias=80`

## Phase 2 - Square-Wave Tests for Position Dynamics

Goal:

- identify delay
- estimate rise/fall behavior
- estimate max speed
- compare opening vs closing asymmetry

### Run P2-Low Square Moderate

Terminal A:

```bash
python scripts/collect_belimo_data.py \
  --output "data/campaign/P2_LOW_SQUARE_001_telemetry.csv" \
  --format csv \
  --lookback-seconds 2 \
  --poll-seconds 1 \
  --duration-seconds 130 \
  --device-id BELIMO-8 \
  --test-id P2_LOW_SQUARE_001 \
  --waveform-type square \
  --bias 20 \
  --amplitude 10 \
  --frequency 0.02 \
  --test-purpose "step-response identification in low operating region"
```

Terminal B:

```bash
python scripts/run_belimo_test.py \
  --suite single \
  --test-number 2001 \
  --waveform square \
  --bias 20 \
  --amplitude 10 \
  --frequency 0.02 \
  --duration-seconds 120 \
  --poll-seconds 1 \
  --command-log "data/campaign/P2_LOW_SQUARE_001_commands.csv" \
  --manifest-json "data/campaign/P2_LOW_SQUARE_001_manifest.json"
```

### Run P2-Mid Square Moderate

Same commands with:

- `test_id=P2_MID_SQUARE_001`
- `test_number=2002`
- `bias=50`
- `amplitude=20`

### Run P2-High Square Moderate

Same commands with:

- `test_id=P2_HIGH_SQUARE_001`
- `test_number=2003`
- `bias=80`
- `amplitude=10`

Repeat one of these runs once more if time allows:

- `P2_MID_SQUARE_002`
- `test_number=2004`

## Phase 3 - Triangle-Wave Tests for Ramp Tracking and Hysteresis

Goal:

- observe ramp lag
- compare opening and closing ramps
- identify asymmetry and friction-like effects

### Run P3-Low Triangle Slow

Terminal A:

```bash
python scripts/collect_belimo_data.py \
  --output "data/campaign/P3_LOW_TRIANGLE_001_telemetry.csv" \
  --format csv \
  --lookback-seconds 2 \
  --poll-seconds 1 \
  --duration-seconds 130 \
  --device-id BELIMO-8 \
  --test-id P3_LOW_TRIANGLE_001 \
  --waveform-type triangle \
  --bias 20 \
  --amplitude 10 \
  --frequency 0.02 \
  --test-purpose "slow ramp tracking and hysteresis in low region"
```

Terminal B:

```bash
python scripts/run_belimo_test.py \
  --suite single \
  --test-number 3001 \
  --waveform triangle \
  --bias 20 \
  --amplitude 10 \
  --frequency 0.02 \
  --duration-seconds 120 \
  --poll-seconds 1 \
  --command-log "data/campaign/P3_LOW_TRIANGLE_001_commands.csv" \
  --manifest-json "data/campaign/P3_LOW_TRIANGLE_001_manifest.json"
```

### Run P3-Mid Triangle Slow

Same commands with:

- `test_id=P3_MID_TRIANGLE_001`
- `test_number=3002`
- `bias=50`
- `amplitude=20`

### Run P3-High Triangle Medium

Same commands with:

- `test_id=P3_HIGH_TRIANGLE_001`
- `test_number=3003`
- `bias=80`
- `amplitude=10`
- `frequency=0.04`

## Phase 4 - Sine-Wave Tests for Smooth Dynamic Characterization

Goal:

- estimate phase lag
- estimate amplitude attenuation
- inspect smoothness and thermal accumulation

### Run P4-Mid Sine Low Frequency

Terminal A:

```bash
python scripts/collect_belimo_data.py \
  --output "data/campaign/P4_MID_SINE_LOW_001_telemetry.csv" \
  --format csv \
  --lookback-seconds 2 \
  --poll-seconds 1 \
  --duration-seconds 190 \
  --device-id BELIMO-8 \
  --test-id P4_MID_SINE_LOW_001 \
  --waveform-type sine \
  --bias 50 \
  --amplitude 10 \
  --frequency 0.02 \
  --test-purpose "low-frequency smooth dynamic characterization at mid bias"
```

Terminal B:

```bash
python scripts/run_belimo_test.py \
  --suite single \
  --test-number 4001 \
  --waveform sine \
  --bias 50 \
  --amplitude 10 \
  --frequency 0.02 \
  --duration-seconds 180 \
  --poll-seconds 1 \
  --command-log "data/campaign/P4_MID_SINE_LOW_001_commands.csv" \
  --manifest-json "data/campaign/P4_MID_SINE_LOW_001_manifest.json"
```

### Run P4-Mid Sine Medium Frequency

Same commands with:

- `test_id=P4_MID_SINE_MED_001`
- `test_number=4002`
- `frequency=0.04`

### Optional Run P4-Low Sine Low Frequency

If time allows:

- `test_id=P4_LOW_SINE_LOW_001`
- `test_number=4003`
- `bias=20`
- `amplitude=10`
- `frequency=0.02`

### Optional Run P4-High Sine Low Frequency

If time allows:

- `test_id=P4_HIGH_SINE_LOW_001`
- `test_number=4004`
- `bias=80`
- `amplitude=10`
- `frequency=0.02`

## Recommended Minimum Matrix If Time Is Tight

Prioritize in this order:

1. `P1_LOW_HOLD_001`
2. `P1_MID_HOLD_001`
3. `P1_HIGH_HOLD_001`
4. `P2_LOW_SQUARE_001`
5. `P2_MID_SQUARE_001`
6. `P2_HIGH_SQUARE_001`
7. `P3_LOW_TRIANGLE_001`
8. `P3_MID_TRIANGLE_001`
9. `P3_HIGH_TRIANGLE_001`
10. `P4_MID_SINE_LOW_001`
11. `P4_MID_SINE_MED_001`

## Quality Review After Each Run

Mark each run as:

- `valid`
- `questionable`
- `invalid`

Review:

- does measured position visibly respond to command?
- are timestamps continuous enough?
- is the run clipped at bounds?
- are there missing or flat signals?
- is the motion smooth and repeatable?

Update `quality_label` later if needed during import/curation.

## Manual Disturbance Policy

Do not manually force the actuator during the healthy identification campaign.

If you want disturbance data later, do it as a separate anomaly campaign with different `test_id`s and never mix it into the healthy baseline set.
