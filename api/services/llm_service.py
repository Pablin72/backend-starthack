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
        Act as an expert monitoring system for industrial actuators (Belimo).
        Analyze the following technical evaluation report and generate a brief and professional alert notification in English.
        
        Device Details: {device_id}
        Overall Status: {summary.get("status", "Unknown")}
        Dominant Variable: {summary.get("dominant_variable", "None")}
        Primary Insight: {summary.get("insight", "None")}
        
        Full Data (JSON):
        {json.dumps(evaluation_data, indent=2)}
        
        Output Instructions:
        1. Generate a concise message in ENGLISH ONLY (maximum 3-4 lines).
        2. Use EMOJIS (🚨, ⚠️, 🔧, 📉, etc.) to make the alert visually striking.
        3. Clearly strongly indicate the severity.
        4. Provide an immediate technical recommendation.
        5. Use an urgent technical alert tone if the status is 'critical' or 'warning'.
        """

        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "You are an expert industrial monitoring assistant."},
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
