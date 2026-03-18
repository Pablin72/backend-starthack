import os
from functools import wraps

from flask import jsonify, request


FRONTEND_STATIC_TOKEN = "starthack_front_2026_allow"


def _extract_token_from_headers() -> str:
    x_token = request.headers.get("X-API-Token", "").strip()
    if x_token:
        return x_token

    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header.split(" ", 1)[1].strip()

    return ""


def require_frontend_token(view_function):
    @wraps(view_function)
    def wrapped(*args, **kwargs):
        token = _extract_token_from_headers()
        if not token:
            return jsonify({"status": "error", "message": "Token faltante"}), 401

        if token != FRONTEND_STATIC_TOKEN:
            return jsonify({"status": "error", "message": "Token inválido"}), 401

        return view_function(*args, **kwargs)

    return wrapped