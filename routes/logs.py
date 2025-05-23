from flask import Blueprint, jsonify, request
from services.db_service import run_query
import logging
from datetime import datetime, timedelta, timezone

logs_bp = Blueprint("logs", __name__)

@logs_bp.route("/logs", methods=["GET"])
def get_logs():
    logs_query = "SELECT * FROM logs ORDER BY timestamp DESC"
    logs = run_query(logs_query, fetchall=True)

    print(logs)

    log_list = [
        {
            "id": log[0],
            "user_id": log[1],
            "event_type": log[2],
            "event_description": log[3],
            "timestamp": log[4]
        }
        for log in logs
    ]

    return jsonify(log_list)


def log_usage(api: str, count: int = 1):
    query = f"""
    INSERT INTO usage (api, requests, created_at)
    VALUES ('{api.upper()}', {count}, NOW())
    """
    run_query(query)


@logs_bp.route("/update_rate_limit", methods=["POST"])
def update_rate_limit():
    data = request.json
    twitter_id = data.get("twitter_id")
    new_rate_limit = data.get("rate_limit")

    if not twitter_id:
        return jsonify({"error": "twitter_id es requerido"}), 400

    if not isinstance(new_rate_limit, int) or new_rate_limit <= 0:
        return jsonify({"error": "rate_limit debe ser un número entero positivo"}), 400

    try:
        query = f"UPDATE users SET rate_limit = {new_rate_limit} WHERE twitter_id = '{twitter_id}'"
        run_query(query)
        return jsonify({"message": f"Rate limit actualizado a {new_rate_limit} para twitter_id {twitter_id}"}), 200

    except Exception as e:
        logging.error(f"Error al actualizar rate_limit: {e}")
        return jsonify({"error": "Error interno"}), 500
    
    
@logs_bp.route("/get_rate_limit", methods=["GET"])
def get_rate_limit():
    try:
        twitter_id = request.args.get("twitter_id")

        if not twitter_id:
            return jsonify({"error": "twitter_id es requerido"}), 400

        query = f"SELECT rate_limit FROM users WHERE twitter_id = '{twitter_id}'"
        result = run_query(query, fetchone=True)

        if not result:
            return jsonify({"error": f"Usuario con twitter_id {twitter_id} no encontrado"}), 404

        rate_limit = result[0]
        return jsonify({"rate_limit": rate_limit}), 200

    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({"error": "Error interno"}), 500
    
    
@logs_bp.route("/api-keys", methods=["PUT"])
def update_api_key():
    try:
        data = request.json
        updates = []

        if "openrouter" in data:
            updates.append((1, data["openrouter"]))
        if "socialdata" in data:
            updates.append((2, data["socialdata"]))
        if "rapidapi" in data:
            updates.append((3, data["rapidapi"]))

        if not updates:
            return jsonify({"error": "No se proporcionaron claves para actualizar"}), 400

        for id_key, key_value in updates:
            query = f"UPDATE api_keys SET key = '{key_value}' WHERE id ={id_key}"
            run_query(query)

        return jsonify({"message": "API Keys actualizadas correctamente"}), 200

    except Exception as e:
        logging.error(f"Error al actualizar API Keys: {e}")
        return jsonify({"error": "Error interno"}), 500


@logs_bp.route("/api-keys", methods=["GET"])
def get_api_keys():
    try:
        query = "SELECT id, key FROM api_keys WHERE id IN (1, 2, 3)"
        keys = run_query(query, fetchall=True)

        keys_dict = {
            "openrouter": keys[0][1] if len(keys) > 0 else "",
            "socialdata": keys[1][1] if len(keys) > 1 else "",
            "rapidapi": keys[2][1] if len(keys) > 2 else ""
        }

        return jsonify(keys_dict), 200

    except Exception as e:
        logging.error(f"Error al obtener API Keys: {e}")
        return jsonify({"error": "Error interno"}), 500
    
    
@logs_bp.route("/api-keys/<int:key_id>", methods=["GET"])
def get_api_key(key_id):
    try:
        query = f"SELECT key FROM api_keys WHERE id = {key_id}"
        result = run_query(query, fetchone=True)

        if not result:
            return jsonify({"error": f"API Key con id {key_id} no encontrada"}), 404

        return jsonify({"key": result[0]}), 200

    except Exception as e:
        logging.error(f"Error al obtener API Key: {e}")
        return jsonify({"error": "Error interno"}), 500
    
    
@logs_bp.route("/cleanup-old-records", methods=["DELETE"])
def delete_old_records():
    try:
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
        date_limit = seven_days_ago.strftime('%Y-%m-%d %H:%M:%S')

        queries = [
            f"DELETE FROM random_actions WHERE created_at < '{date_limit}'",
            f"DELETE FROM posted_tweets WHERE created_at < '{date_limit}'"
        ]

        for query in queries:
            run_query(query)

        return jsonify({"message": "Registros antiguos eliminados correctamente"}), 200

    except Exception as e:
        logging.error(f"Error al eliminar registros antiguos: {e}")
        return jsonify({"error": "Error interno al eliminar registros"}), 500