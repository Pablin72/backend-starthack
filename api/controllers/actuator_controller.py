import os
import json
import logging
from flask import Blueprint, request, jsonify
import paho.mqtt.client as mqtt

actuator_bp = Blueprint("actuator_bp", __name__)
logger = logging.getLogger("backend-starthack.actuator_controller")

@actuator_bp.route("/command", methods=["POST"])
def send_command():
    """
    Control del actuador
    ---
    tags:
      - Actuator
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          properties:
            action:
              type: string
              example: set_setpoint
            value:
              type: number
              example: 50.0
    responses:
      200:
        description: Comando enviado con éxito
      400:
        description: Faltan parámetros en la solicitud
      500:
        description: Error interno al enviar comando MQTT
    """
    data = request.json
    if not data or "action" not in data or "value" not in data:
        return jsonify({"error": "Se requieren los campos 'action' y 'value'."}), 400

    action = data["action"]
    value = float(data["value"])

    mqtt_host = os.getenv("MQTT_HOST", "test.mosquitto.org")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    device_id = os.getenv("BELIMO_DEVICE_ID", "actuator-01")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    try:
        # Conectar al broker de forma síncrona
        client.connect(mqtt_host, mqtt_port, 60)
        topic = f"belimo/{device_id}/commands"
        payload = json.dumps({"action": action, "value": value})
        
        # Publicar el mensaje MQTT para que el edge gateway lo procese
        result = client.publish(topic, payload)
        result.wait_for_publish()
        client.disconnect()
        
        return jsonify({
            "status": "success",
            "message": f"Comando '{action}' con valor {value} enviado exitosamente a {topic}",
            "mqtt_host": mqtt_host
        }), 200

    except Exception as e:
        logger.error(f"Failed to publish MQTT command in Actuator Endpoint: {e}")
        return jsonify({"error": f"Error al enviar comando MQTT: {str(e)}"}), 500
