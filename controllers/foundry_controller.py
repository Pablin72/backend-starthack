import os
from flask import Blueprint, request, jsonify

# Importar el SDK de Azure Foundry
from azure.ai.inference import ChatCompletionsClient
from azure.core.credentials import AzureKeyCredential

foundry_bp = Blueprint('foundry', __name__)

@foundry_bp.route("/test-llm", methods=["POST"])
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

    endpoint = os.environ.get("AZURE_FOUNDRY_ENDPOINT")
    key = os.environ.get("AZURE_FOUNDRY_KEY")
    model_name = os.environ.get("AZURE_FOUNDRY_MODEL", "gpt-4o")

    if not endpoint or not key:
        return jsonify({"error": "Faltan credenciales de Azure Foundry en las variables de entorno"}), 500

    try:
        client = ChatCompletionsClient(
            endpoint=endpoint,
            credential=AzureKeyCredential(key)
        )
        
        response = client.complete(
            messages=[
                {"role": "system", "content": "Eres un asistente de IA útil para un hackathon."},
                {"role": "user", "content": prompt}
            ],
            model=model_name
        )
        
        llm_reply = response.choices[0].message.content

        return jsonify({
            "status": "success",
            "model": model_name,
            "response": llm_reply
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
