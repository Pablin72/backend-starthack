import logging
import os
import urllib.parse
import json

from openai import AzureOpenAI, OpenAIError

logger = logging.getLogger("backend-starthack.llm_service")

def _get_base_url(url: str) -> str:
    """Extrae la URL base desde una ruta completa (ej. /openai/deployments) si es el caso."""
    url = url.strip()
    if not url:
        return ""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}/"
    return url

def get_azure_openai_client():
    raw_endpoint = os.environ.get("AZURE_FOUNDRY_ENDPOINT", "")
    endpoint = _get_base_url(raw_endpoint)
    key = os.environ.get("AZURE_FOUNDRY_KEY")
    api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
    
    if not endpoint or not key:
        logger.error("Missing Azure OpenAI env vars for LLM service")
        return None, None

    try:
        client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=key,
            api_version=api_version,
        )
        return client, os.environ.get("AZURE_FOUNDRY_MODEL", "gpt-4o")
    except Exception as e:
        logger.error(f"Error initializing AzureOpenAI client: {e}")
        return None, None

def generate_alert_analysis(evaluation_data: dict) -> str:
    """
    Genera un análisis y mensaje de alerta basado en los datos de evaluación.
    """
    client, model_name = get_azure_openai_client()
    if not client:
        return "Error: No se pudo conectar con el servicio de IA para generar el análisis."

    try:
        # Simplificamos los datos para el prompt para no exceder tokens innecesariamente
        summary = evaluation_data.get("summary", {})
        device_id = evaluation_data.get("device_id", "Unknown")
        
        prompt = f"""
        Actúa como un sistema experto de monitoreo para actuadores industriales (Belimo).
        Analiza el siguiente reporte de evaluación técnica y genera una notificación de alerta breve y profesional.
        
        Detalles del Dispositivo: {device_id}
        Estado General: {summary.get("status", "Unknown")}
        Variable Dominante: {summary.get("dominant_variable", "None")}
        Insight Principal: {summary.get("insight", "None")}
        
        Datos Completos (JSON):
        {json.dumps(evaluation_data, indent=2)}
        
        Instrucciones de salida:
        1. Genera un mensaje conciso (máximo 3-4 líneas).
        2. Indica claramente la severidad.
        3. Provee una recomendación técnica inmediata.
        4. Usa un tono de alerta técnica urgente si el estado es 'critical' o 'warning'.
        """

        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "Eres un asistente de monitoreo industrial experto."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=300,
            temperature=0.3
        )
        
        return response.choices[0].message.content.strip()
    except OpenAIError as e:
        logger.error(f"OpenAI API Error: {e}")
        return "Error al generar el análisis de IA."
    except Exception as e:
        logger.error(f"Unexpected error in generating analysis: {e}")
        return "Error inesperado al procesar el análisis."
