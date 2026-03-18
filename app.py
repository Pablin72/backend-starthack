import os

from flask import Flask, jsonify
from flasgger import Swagger
from dotenv import load_dotenv

# Cargar variables de entorno desde .env si existe
load_dotenv()

# Importar Blueprints
from controllers.foundry_controller import foundry_bp

app = Flask(__name__)

swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": "apispec_1",
            "route": "/apispec_1.json",
            "rule_filter": lambda rule: True,
            "model_filter": lambda tag: True,
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/apidocs/",
}

swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "Backend StartHack API",
        "description": "API de backend con documentación Swagger",
        "version": "1.0.0",
    },
}

Swagger(app, config=swagger_config, template=swagger_template)

# Registrar Blueprints
app.register_blueprint(foundry_bp, url_prefix="/api/foundry")

@app.route("/", methods=["GET"])
def index():
    """
    Endpoint de bienvenida
    ---
    tags:
      - General
    responses:
      200:
        description: Respuesta de bienvenida
        schema:
          type: object
          properties:
            message:
              type: string
              example: Welcome to the backend API
            status:
              type: string
              example: ok
    """
    return jsonify({"message": "Welcome to the backend API", "status": "ok"})


@app.route("/health", methods=["GET"])
def health():
    """
    Health check del servicio
    ---
    tags:
      - Health
    responses:
      200:
        description: Estado de salud del backend
        schema:
          type: object
          properties:
            status:
              type: string
              example: healthy
    """
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    # For local development only; production uses gunicorn (see Dockerfile)
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=8000, debug=debug)
