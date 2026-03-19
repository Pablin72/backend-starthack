import os
import json
import logging
from flask import Blueprint, request, jsonify
import urllib.request
import paho.mqtt.client as mqtt

telegram_bp = Blueprint("telegram_bp", __name__)
logger = logging.getLogger("backend-starthack.telegram_controller")

@telegram_bp.route("/register_webhook", methods=["POST"])
def register_webhook():
    """
    Registra dinámicamente este backend como el webhook de Telegram.
    body: { "url": "https://<TU_CONTAINER_APP_URL>/api/telegram/webhook" }
    """
    token = os.environ.get("TELEGRAM_TOKEN")
    if not token:
        return jsonify({"error": "TELEGRAM_TOKEN is missing"}), 500

    data = request.json or {}
    webhook_url = data.get("url")
    if not webhook_url:
        return jsonify({"error": "Missing 'url' in request body"}), 400

    telegram_url = f"https://api.telegram.org/bot{token}/setWebhook"
    response = _send_telegram_request(telegram_url, {"url": webhook_url})
    
    return jsonify(response or {"error": "Failed to set webhook"}), 200

@telegram_bp.route("/webhook", methods=["POST"])
def telegram_webhook():
    """
    Endpoint para recibir callbacks (webhooks) de Telegram.
    Asegúrate de configurar este webhook en Telegram apuntando a:
    https://<TU_CONTAINER_APP_URL>/api/telegram/webhook
    """
    update = request.json
    if not update:
        return jsonify({"status": "ignored"}), 200

    token = os.environ.get("TELEGRAM_TOKEN")
    if not token:
        logger.error("TELEGRAM_TOKEN missing")
        return jsonify({"error": "No token"}), 500

    # Si es un simple mensaje de texto o comando, por ahora respondemos un OK.
    # En este webhook solo nos interesa el callback_query.
    if "callback_query" in update:
        query = update["callback_query"]
        callback_id = query.get("id")
        data = query.get("data")
        message = query.get("message", {})
        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")
        text = message.get("text", "")

        # Respuesta a Telegram para que quite el icono de "reloj" en el botón
        answer_url = f"https://api.telegram.org/bot{token}/answerCallbackQuery"
        _send_telegram_request(answer_url, {"callback_query_id": callback_id})

        if data == "corrective_action":
            # Cambiamos el teclado para pedir confirmación
            keyboard = {
                "inline_keyboard": [
                    [
                        {"text": "✅ Approve Action", "callback_data": "approve_action"},
                        {"text": "❌ Cancel", "callback_data": "cancel_action"}
                    ]
                ]
            }
            new_text = (
                f"{text}\n\n"
                "⚠️ *Proposed Corrective Action:*\n"
                "1. Override current anomalous trajectory.\n"
                "2. Send MQTT `set_setpoint` command to fallback position (0.0).\n\n"
                "Do you approve this execution?"
            )
            _edit_message(token, chat_id, message_id, new_text, keyboard)

        elif data == "approve_action":
            _edit_message(token, chat_id, message_id, f"{text}\n\n⏳ Executing approved corrective offset to edge gateway...")
            
            # Conexión y publicación MQTT
            mqtt_host = os.getenv("MQTT_HOST", "localhost")
            mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
            device_id = os.getenv("BELIMO_DEVICE_ID", "actuator-01")
            
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            try:
                client.connect(mqtt_host, mqtt_port, 60)
                topic = f"belimo/{device_id}/commands"
                payload = json.dumps({"action": "set_setpoint", "value": 0.0})
                
                client.publish(topic, payload)
                client.disconnect()
                
                _edit_message(token, chat_id, message_id, f"{text}\n\n✅ *Action Executed successfully!* MQTT command published to fallback position 0.0.")
            except Exception as e:
                logger.error(f"Failed to publish MQTT command in Webhook: {e}")
                _edit_message(token, chat_id, message_id, f"{text}\n\n❌ *Error:* Could not reach MQTT broker at {mqtt_host}:{mqtt_port}.")

        elif data == "cancel_action":
            _edit_message(token, chat_id, message_id, f"{text}\n\n🚫 *Action Cancelled.* No commands were sent.")

        elif data == "more_info":
            # Aquí podrías llamar al scan
            _edit_message(token, chat_id, message_id, f"{text}\n\n🔍 *More Information Requested:*\nPlease refer to the dashboard for deep spectral analysis.")

    return jsonify({"status": "ok"}), 200

def _edit_message(token, chat_id, message_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{token}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup

    _send_telegram_request(url, payload)

def _send_telegram_request(url, payload):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        logger.error(f"Telegram API request failed: {e}")
        return None
