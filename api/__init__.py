import logging
import os

from dotenv import load_dotenv
from flask import Flask, jsonify
from flasgger import Swagger

from api.controllers.foundry_controller import foundry_bp


def create_app() -> Flask:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logger = logging.getLogger("backend-starthack")

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
    app.register_blueprint(foundry_bp, url_prefix="/api/foundry")

    logger.info(
      "Config startup | AZURE_FOUNDRY_ENDPOINT=%s | AZURE_FOUNDRY_MODEL=%s | AZURE_FOUNDRY_KEY_SET=%s",
        os.environ.get("AZURE_FOUNDRY_ENDPOINT", ""),
        os.environ.get("AZURE_FOUNDRY_MODEL", ""),
        bool(os.environ.get("AZURE_FOUNDRY_KEY")),
    )

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

    return app


def get_debug_mode() -> bool:
    return os.environ.get("FLASK_DEBUG", "false").lower() == "true"
