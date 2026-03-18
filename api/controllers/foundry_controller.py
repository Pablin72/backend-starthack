import logging
import os

from flask import Blueprint, jsonify, request

from azure.ai.inference import ChatCompletionsClient
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import HttpResponseError

from api.security import require_frontend_token

foundry_bp = Blueprint("foundry", __name__)
logger = logging.getLogger("backend-starthack.foundry")


def _build_endpoint_candidates(raw_endpoint: str) -> list[str]:
    base = raw_endpoint.strip().rstrip("/")
    if not base:
        return []

    candidates = [base]
    if "services.ai.azure.com" in base and not base.endswith("/models"):
        candidates.append(f"{base}/models")

    unique_candidates = []
    for candidate in candidates:
        if candidate not in unique_candidates:
            unique_candidates.append(candidate)
    return unique_candidates


@foundry_bp.route("/test-llm", methods=["POST"])
@require_frontend_token
def test_llm():
    """
    Endpoint de prueba para Azure AI Foundry (LLM)
    ---
    tags:
      - AI Foundry
    consumes:
      - application/json
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          properties:
            prompt:
              type: string
              example: "¿Qué es un hackathon?"
    responses:
      200:
        description: Respuesta del modelo LLM
        schema:
          type: object
          properties:
            response:
              type: string
              example: "Un hackathon es un evento..."
            status:
              type: string
              example: success
      400:
        description: Bad Request si no se envía el 'prompt'
    """
    data = request.get_json() or {}
    prompt = data.get("prompt", "")

    if not prompt:
        return jsonify({"error": "El campo 'prompt' es requerido"}), 400

    endpoint = (os.environ.get("AZURE_FOUNDRY_ENDPOINT") or "").strip().rstrip("/")
    key = os.environ.get("AZURE_FOUNDRY_KEY")
    model_name = os.environ.get("AZURE_FOUNDRY_MODEL", "gpt-4o")
    endpoint_candidates = _build_endpoint_candidates(endpoint)

    logger.info(
        "LLM request | origin=%s | model=%s | endpoint=%s | key_set=%s | candidates=%s",
        request.headers.get("Origin", ""),
        model_name,
        endpoint,
        bool(key),
        endpoint_candidates,
    )

    if not endpoint or not key:
        logger.error("Missing Foundry env vars | endpoint_set=%s | key_set=%s", bool(endpoint), bool(key))
        return jsonify({"error": "Faltan credenciales de Azure Foundry en las variables de entorno"}), 500

    try:
        if not endpoint_candidates:
            return jsonify({"status": "error", "message": "AZURE_FOUNDRY_ENDPOINT vacío o inválido"}), 500

        response = None
        last_http_error = None
        used_endpoint = endpoint_candidates[0]

        for endpoint_candidate in endpoint_candidates:
            used_endpoint = endpoint_candidate
            client = ChatCompletionsClient(
                endpoint=endpoint_candidate,
                credential=AzureKeyCredential(key),
            )

            try:
                response = client.complete(
                    messages=[
                        {"role": "system", "content": "Eres un asistente de IA útil para un hackathon."},
                        {"role": "user", "content": prompt},
                    ],
                    model=model_name,
                )
                break
            except HttpResponseError as endpoint_error:
                last_http_error = endpoint_error
                if getattr(endpoint_error, "status_code", None) == 404:
                    continue
                raise

        if response is None and last_http_error is not None:
            raise last_http_error

        llm_reply = response.choices[0].message.content

        return jsonify(
            {
                "status": "success",
                "model": model_name,
                "endpoint": used_endpoint,
                "response": llm_reply,
            }
        )
    except HttpResponseError as error:
        status_code = getattr(error, "status_code", None)
        message = str(error)
        logger.error(
            "Foundry HTTP error | status=%s | endpoint=%s | model=%s | message=%s",
            status_code,
            endpoint,
            model_name,
            message,
        )

        if status_code == 404:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Foundry devolvió 404. Verifica AZURE_FOUNDRY_ENDPOINT y AZURE_FOUNDRY_MODEL.",
                        "details": {
                            "endpoint": endpoint,
                            "endpoint_candidates": endpoint_candidates,
                            "model": model_name,
                            "hint": "Usa un endpoint de inferencia válido (models.ai.azure.com o services.ai.azure.com/models) y un modelo/deployment existente.",
                        },
                    }
                ),
                500,
            )

        return (
            jsonify(
                {
                    "status": "error",
                    "message": message,
                    "details": {
                        "endpoint": endpoint,
                        "endpoint_candidates": endpoint_candidates,
                        "model": model_name,
                        "status_code": status_code,
                    },
                }
            ),
            500,
        )
    except Exception as error:
        logger.exception("Unhandled Foundry error")
        return (
            jsonify(
                {
                    "status": "error",
                    "message": str(error),
                    "details": {
                        "endpoint": endpoint,
                        "endpoint_candidates": endpoint_candidates,
                        "model": model_name,
                    },
                }
            ),
            500,
        )
