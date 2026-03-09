# backend-starthack

A Python Flask backend with Docker support, deployed to Azure Container Apps via GitHub Actions.

## Local Development

```bash
pip install -r requirements.txt
python app.py
```

The API will be available at `http://localhost:8000`.

## Endpoints

| Method | Path      | Description          |
|--------|-----------|----------------------|
| GET    | `/`       | Welcome message      |
| GET    | `/health` | Health check         |

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

### Creating the Azure service principal

```bash
az ad sp create-for-rbac --name "backend-starthack-sp" \
  --role contributor \
  --scopes /subscriptions/<SUBSCRIPTION_ID>/resourceGroups/<RESOURCE_GROUP> \
  --sdk-auth
```

Copy the JSON output and save it as the `AZURE_CREDENTIALS` secret.