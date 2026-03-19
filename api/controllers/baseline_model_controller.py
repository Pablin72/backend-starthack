from __future__ import annotations

from flask import Blueprint, jsonify, request

from api.security import require_frontend_token
from api.services.comparison_service import (
    evaluate_combined,
    evaluate_position,
    evaluate_temperature,
    evaluate_torque,
    get_model_metadata,
    get_model_readiness,
)
from api.services.llm_service import generate_alert_analysis


baseline_model_bp = Blueprint("baseline_model", __name__)


@baseline_model_bp.route("/metadata", methods=["GET"])
@require_frontend_token
def metadata():
    """
    Return loaded healthy model metadata for frontend and debugging.
    ---
    tags:
      - Baseline Comparison
    security:
      - Bearer: []
    responses:
      200:
        description: Loaded model metadata
    """
    try:
        return jsonify({"status": "success", "metadata": get_model_metadata()})
    except FileNotFoundError as error:
        return jsonify({"status": "error", "message": str(error)}), 404


@baseline_model_bp.route("/readiness", methods=["GET"])
@require_frontend_token
def readiness():
    """
    Confirm the baseline comparison API is alive and model artifacts are loaded.
    ---
    tags:
      - Baseline Comparison
    security:
      - Bearer: []
    responses:
      200:
        description: Baseline comparison readiness status
    """
    try:
        return jsonify({"status": "success", "readiness": get_model_readiness()})
    except FileNotFoundError as error:
        return jsonify({"status": "error", "message": str(error)}), 404


@baseline_model_bp.route("/evaluate/position", methods=["POST"])
@require_frontend_token
def position_evaluation():
    """
    Compare measured position telemetry against the healthy position baseline.
    ---
    tags:
      - Baseline Comparison
    security:
      - Bearer: []
    consumes:
      - application/json
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
    responses:
      200:
        description: Position evaluation result
    """
    payload = request.get_json() or {}
    try:
        return jsonify({"status": "success", "evaluation": evaluate_position(payload)})
    except FileNotFoundError as error:
        return jsonify({"status": "error", "message": str(error)}), 404
    except ValueError as error:
        return jsonify({"status": "error", "message": str(error)}), 400


@baseline_model_bp.route("/evaluate/torque", methods=["POST"])
@require_frontend_token
def torque_evaluation():
    """
    Compare measured torque telemetry against the healthy torque baseline.
    ---
    tags:
      - Baseline Comparison
    security:
      - Bearer: []
    consumes:
      - application/json
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
    responses:
      200:
        description: Torque evaluation result
    """
    payload = request.get_json() or {}
    try:
        return jsonify({"status": "success", "evaluation": evaluate_torque(payload)})
    except FileNotFoundError as error:
        return jsonify({"status": "error", "message": str(error)}), 404
    except ValueError as error:
        return jsonify({"status": "error", "message": str(error)}), 400


@baseline_model_bp.route("/evaluate/temperature", methods=["POST"])
@require_frontend_token
def temperature_evaluation():
    """
    Compare measured temperature telemetry against the healthy thermal baseline.
    ---
    tags:
      - Baseline Comparison
    security:
      - Bearer: []
    consumes:
      - application/json
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
    responses:
      200:
        description: Temperature evaluation result
    """
    payload = request.get_json() or {}
    try:
        return jsonify({"status": "success", "evaluation": evaluate_temperature(payload)})
    except FileNotFoundError as error:
        return jsonify({"status": "error", "message": str(error)}), 404
    except ValueError as error:
        return jsonify({"status": "error", "message": str(error)}), 400


@baseline_model_bp.route("/evaluate/combined", methods=["POST"])
@require_frontend_token
def combined_evaluation():
    """
    Compare position, torque, and temperature telemetry together against the healthy baseline.
    ---
    tags:
      - Baseline Comparison
    security:
      - Bearer: []
    consumes:
      - application/json
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
    responses:
      200:
        description: Combined baseline comparison result
    """
    payload = request.get_json() or {}
    try:
        evaluation_result = evaluate_combined(payload)

        # ------------------------------------------------------------------
        # AI ALERT SIMULATION
        # ------------------------------------------------------------------
        summary_status = evaluation_result.get("summary", {}).get("status", "unknown")
        ai_message = None

        if summary_status in ["warning", "critical"]:
            try:
                # Generate AI analysis
                ai_message = generate_alert_analysis(evaluation_result)
                
                # Print to console for verification as requested
                print(f"\n======== [SIMULATION] AI ALERT GENERATED ({summary_status.upper()}) ========")
                print(ai_message)
                print("==================================================================\n")
                
                # TODO: Implement Telegram Bot Trigger here
                # bot.send_message(chat_id=..., text=ai_message, buttons=...)

            except Exception as e:
                print(f"Error generating AI alert analysis: {e}")
        # ------------------------------------------------------------------

        response = {
            "status": "success",
            "evaluation": evaluation_result
        }
        
        if ai_message:
            response["ai_analysis"] = ai_message

        return jsonify(response)
    except FileNotFoundError as error:
        return jsonify({"status": "error", "message": str(error)}), 404
    except ValueError as error:
        return jsonify({"status": "error", "message": str(error)}), 400
