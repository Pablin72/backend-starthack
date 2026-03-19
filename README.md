# backend-starthack

A Python Flask backend with Docker support, deployed to Azure Container Apps via GitHub Actions.

## Local Development

```bash
pip install -r requirements.txt
python app.py
```

The API will be available at `http://localhost:8000`.

Swagger UI will be available at `http://localhost:8000/apidocs/`.

## Capture Belimo Data To CSV Or JSON

When you are connected to the Raspberry Pi Wi-Fi (`BELIMO-8` in your case), you can collect telemetry directly with Python instead of exporting from the Influx UI:

```bash
python scripts/collect_belimo_data.py \
  --output data/belimo_capture.csv \
  --format csv \
  --poll-seconds 2 \
  --duration-seconds 120
```

JSONL:

```bash
python scripts/collect_belimo_data.py \
  --output data/belimo_capture.jsonl \
  --format jsonl \
  --poll-seconds 2 \
  --duration-seconds 120
```

JSON:

```bash
python scripts/collect_belimo_data.py \
  --output data/belimo_capture.json \
  --format json \
  --poll-seconds 2 \
  --duration-seconds 120
```

The script queries the `measurements` measurement from the Belimo InfluxDB, pivots fields into one row per timestamp, and appends only new rows. By default it stores:

- `feedback_position_%`
- `setpoint_position_%`
- `motor_torque_Nmm`
- `power_W`
- `internal_temperature_deg_C`
- `rotation_direction`
- `test_number`

## Drive The Actuator Without The Streamlit UI

To send deterministic command sequences directly into `_process`, use:

```bash
python scripts/run_belimo_test.py --suite characterize --test-number 301
```

This built-in `characterize` suite runs:

- constant holds at 20, 50, and 80
- a reset hold
- a soft sine sweep
- a triangle sweep
- a square-wave stress phase
- a safe finish hold at 50

To run one specific waveform instead:

```bash
python scripts/run_belimo_test.py \
  --suite single \
  --test-number 302 \
  --waveform sine \
  --bias 50 \
  --amplitude 20 \
  --frequency 0.03 \
  --duration-seconds 60
```

Every command is also logged locally to CSV plus a JSON manifest so the exact stimulus used during data collection is preserved.

## Structured Healthy Campaign

The full step-by-step identification campaign is documented in:

- [docs/belimo_test_campaign.md](/Users/pabloarcos/Desktop/Start%20Hack/backend-starthack/docs/belimo_test_campaign.md)

That runbook uses:

- `scripts/run_belimo_test.py` for command excitation
- `scripts/collect_belimo_data.py` for telemetry capture

Each run produces:

- telemetry CSV
- command CSV
- manifest JSON

## Baseline Model API

The backend also exposes the calibrated healthy baseline and comparison endpoints for the frontend.

Reference:

- [docs/baseline_model_api.md](/Users/pabloarcos/Desktop/Start%20Hack/backend-starthack/docs/baseline_model_api.md)

## Project Structure

```text
backend-starthack/
├── app.py
├── api/
│   ├── __init__.py          # Flask application factory (create_app)
│   └── controllers/
│       └── foundry_controller.py
├── requirements.txt
└── Dockerfile
```

## Endpoints

| Method | Path      | Description          |
|--------|-----------|----------------------|
| GET    | `/`       | Welcome message      |
| GET    | `/health` | Health check         |
| POST   | `/api/foundry/test-llm` | Prueba de modelo LLM (requiere token estático) |
| POST   | `/api/features/ingest` | Guarda muestras raw/mock, calcula features y actualiza baseline |
| GET    | `/api/features/devices/<device_id>/latest` | Devuelve las últimas muestras, features y baseline |
| POST   | `/api/features/seed-demo` | Inserta datos demo para probar el MVP |
| GET    | `/api/baseline-model/summary` | Devuelve parámetros y diagnósticos del modelo baseline |
| GET    | `/api/baseline-model/report` | Devuelve el reporte completo del baseline |
| GET    | `/api/baseline-model/waveforms/<waveform_type>` | Devuelve baseline y envelope por waveform |
| POST   | `/api/baseline-model/recalibrate` | Recalibra el baseline desde CSVs de campaña |

## Auth Flow (Frontend -> Backend)

El endpoint `/api/foundry/test-llm` valida un token estático quemado en código.

Envía uno de estos headers:

- `X-API-Token: starthack_front_2026_allow`
- `Authorization: Bearer starthack_front_2026_allow`

Variables de entorno necesarias para auth:

- Ninguna adicional (solo token estático en header)

## MVP Data Layers

El backend ahora separa el pipeline de Alex en cuatro capas persistidas en SQLite:

- `raw_samples`: copia preservada de lecturas reales desde InfluxDB
- `feature_snapshots`: features calculadas sobre una ventana reciente
- `baseline_profiles`: perfil normal por dispositivo
- `mock_samples`: datos sintéticos o alterados para probar anomalías sin tocar los datos raw

Por defecto la base vive en `backend-starthack/data/mvp.db`.
Puedes cambiarla con `MVP_DB_PATH=/ruta/al/archivo.db`.

## API Docs

Interactive Swagger docs:

- `GET /apidocs/` (Swagger UI)
- `GET /apispec_1.json` (OpenAPI JSON)

## Docker

Build and run locally:

```bash
docker build -t backend-starthack .
docker run -p 8000:8000 backend-starthack
```

## Deployment

The GitHub Actions workflow in `.github/workflows/deploy.yml` automatically builds the Docker image and deploys it to **Azure Container Apps** on every push to `main`.

### Required GitHub Secrets

| Secret                        | Description                                         |
|-------------------------------|-----------------------------------------------------|
| `AZURE_CREDENTIALS`           | Azure service principal credentials (JSON)          |
| `AZURE_REGISTRY_LOGIN_SERVER` | Azure Container Registry login server (e.g. `myregistry.azurecr.io`) |
| `AZURE_REGISTRY_USERNAME`     | ACR username                                        |
| `AZURE_REGISTRY_PASSWORD`     | ACR password                                        |
| `AZURE_RESOURCE_GROUP`        | Azure resource group containing the Container App   |
| `AZURE_CONTAINER_APP_NAME`    | Name of the Azure Container App                     |
| `AZURE_FOUNDRY_ENDPOINT`      | Azure AI Foundry endpoint URL                       |
| `AZURE_FOUNDRY_KEY`           | Azure AI Foundry API key                            |
| `AZURE_FOUNDRY_MODEL`         | Azure AI Foundry deployed model name                |

### Creating the Azure service principal

```bash
az ad sp create-for-rbac --name "backend-starthack-sp" \
  --role contributor \
  --scopes /subscriptions/<SUBSCRIPTION_ID>/resourceGroups/<RESOURCE_GROUP> \
  --sdk-auth
```

Copy the JSON output and save it as the `AZURE_CREDENTIALS` secret.
