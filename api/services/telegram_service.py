import os
import json
import urllib.request
import logging

logger = logging.getLogger("backend-starthack.telegram_service")

def send_telegram_alert(text: str):
    """
    Envía una alerta de monitoreo a Telegram usando la API HTTP configurada en las variables de entorno.
    Incluye botones en línea (Inline Keyboard).
    """
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        logger.warning("Telegram credentials (TELEGRAM_TOKEN, TELEGRAM_CHAT_ID) are missing. Skipping notification.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    payload = {
        "chat_id": chat_id,
        "text": text,
        # Parse mode no es Markdown para evitar errores con caracteres especiales si LLM manda * mal formateados
        # Sin embargo si el frontend no procesa bien es seguro usar HTML o quitarlo
        # Intentaremos usar envio en texto plano enriquecido con emojis
        "reply_markup": {
            "inline_keyboard": [
                [
                    {"text": "🔧 Take Corrective Action", "callback_data": "corrective_action"}
                ],
                [
                    {"text": "ℹ️ Get More Information", "callback_data": "more_info"}
                ]
            ]
        }
    }
    
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            if not result.get("ok"):
                logger.error(f"Failed to send Telegram message: {result}")
            else:
                logger.info("Telegram alert sent successfully.")
    except Exception as e:
        logger.error(f"Error communicating with Telegram API: {e}")
