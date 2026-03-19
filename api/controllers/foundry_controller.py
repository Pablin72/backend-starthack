import logging
import os
import urllib.parse

from flask import Blueprint, jsonify, request
from openai import AzureOpenAI, OpenAIError

from api.security import require_frontend_token

foundry_bp = Blueprint("foundry", __name__)
logger = logging.getLogger("backend-starthack.foundry")


def _get_base_url(url: str) -> str:
    """Extrae la URL base desde una ruta completa (ej. /openai/deployments) si es el caso."""
    url = url.strip()
    if not url:
        return ""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}/"
    return url


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
    security:
      - Bearer: []
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

    raw_endpoint = os.environ.get("AZURE_FOUNDRY_ENDPOINT", "")
    endpoint = _get_base_url(raw_endpoint)
    key = os.environ.get("AZURE_FOUNDRY_KEY")
    model_name = os.environ.get("AZURE_FOUNDRY_MODEL", "gpt-4o")
    api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")

    logger.info(
        "LLM request (OpenAI SDK) | origin=%s | model=%s | endpoint=%s | api_version=%s | key_set=%s",
        request.headers.get("Origin", ""),
        model_name,
        endpoint,
        api_version,
        bool(key),
    )

    if not endpoint or not key:
        logger.error("Missing Azure OpenAI env vars | endpoint_set=%s | key_set=%s", bool(endpoint), bool(key))
        return jsonify({"error": "Faltan credenciales de Azure OpenAI en las variables de entorno"}), 500

    try:
        client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=key,
            api_version=api_version,
        )

        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "Eres un asistente de IA útil para un hackathon."},
                {"role": "user", "content": prompt},
            ],
        )

        llm_reply = response.choices[0].message.content

        return jsonify(
            {
                "status": "success",
                "model": model_name,
                "endpoint": endpoint,
                "response": llm_reply,
            }
        )
    except OpenAIError as error:
        status_code = getattr(error, "status_code", 500)
        message = str(error)
        logger.error(
            "OpenAI SDK error | status=%s | endpoint=%s | model=%s | message=%s",
            status_code,
            endpoint,
            model_name,
            message,
        )

        return (
            jsonify(
                {
                    "status": "error",
                    "message": message,
                    "details": {
                        "endpoint": endpoint,
                        "raw_endpoint": raw_endpoint,
                        "model": model_name,
                        "status_code": status_code,
                    },
                }
            ),
            status_code if isinstance(status_code, int) and status_code >= 400 else 500,
        )
    except Exception as error:
        logger.exception("Unhandled error parsing LLM response")
        return (
            jsonify(
                {
                    "status": "error",
                    "message": str(error),
                    "details": {
                        "endpoint": endpoint,
                        "raw_endpoint": raw_endpoint,
                        "model": model_name,
                    },
                }
            ),
            500,
        )
