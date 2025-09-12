from flask import Blueprint, jsonify, request
from services.db_service import run_query
import logging
from datetime import datetime, timedelta, timezone
from openai import OpenAI
import http.client
import json

logs_bp = Blueprint("logs", __name__)


def get_openai_api_key():
    query = "SELECT key FROM api_keys WHERE id = 1"
    result = run_query(query, fetchone=True)
    return result[0] if result else None  


def get_rapidapi_key():
    query = "SELECT key FROM api_keys WHERE id = 3" 
    result = run_query(query, fetchone=True)
    return result[0] if result else None  


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
        if "twitterapi" in data:
            updates.append((5, data["twitterapi"]))

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
        query = "SELECT id, key FROM api_keys WHERE id IN (1, 2, 3, 5)"
        keys = run_query(query, fetchall=True)

        keys_dict = {
            "openrouter": keys[0][1] if len(keys) > 0 else "",
            "socialdata": keys[1][1] if len(keys) > 1 else "",
            "rapidapi": keys[2][1] if len(keys) > 2 else "",
            "twitterapi": keys[3][1] if len(keys) > 3 else ""
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
    
    
@logs_bp.route("/cleanup-old-records", methods=["POST"])
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

        users = run_query("SELECT id, username FROM users WHERE username IS NOT NULL", fetchall=True)
        rapidapi_key = get_rapidapi_key()
        openai_key = get_openai_api_key()

        client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=openai_key)

        for user_id, username in users:
            print(f"🟢 Procesando usuario: {username}")
            try:
                conn = http.client.HTTPSConnection("twttrapi.p.rapidapi.com")
                headers = {
                    'x-rapidapi-key': rapidapi_key,
                    'x-rapidapi-host': "twttrapi.p.rapidapi.com"
                }
                conn.request("GET", f"/get-user?username={username}", headers=headers)
                res = conn.getresponse()
                data = json.loads(res.read().decode("utf-8"))
                log_usage("RAPIDAPI")

                result = data
                print(f"🟢 Procesando result: {data}")
                if not result:
                    continue

                prompt = f"""
                You are a Twitter profile analyst. Evaluate the following profile and return only a number from 1 to 100 representing its "AI Score", where 1 means irrelevant and 100 means highly influential or well-optimized.

                Data:
                - Followers: {result.get("followers_count", 0)}
                - Following: {result.get("friends_count", 0)}
                - Tweets posted: {result.get("statuses_count", 0)}
                - Has description: {"Yes" if result.get("description") else "No"}
                - Has profile image: {"Yes" if result.get("profile_image_url_https") else "No"}
                - Has banner: {"Yes" if result.get("profile_banner_url") else "No"}
                - Account age: {result.get("created_at")}
                - Likes count: {result.get("favourites_count")}

                Return ONLY the numeric score.
                """

                score = None
                for model in [
                    "meta-llama/llama-4-scout:free",
                    "google/gemini-2.0-flash-001",
                    "deepseek/deepseek-chat-v3-0324",
                    "openai/gpt-4o-2024-11-20",
                    "anthropic/claude-3.7-sonnet"
                ]:
                    try:
                        response = client.chat.completions.create(
                            model=model,
                            messages=[
                                {"role": "system", "content": "Sos un evaluador de perfiles de redes sociales."},
                                {"role": "user", "content": prompt}
                            ],
                            max_tokens=5,
                            temperature=0
                        )
                        log_usage("OPENROUTER")
                        score_raw = response.choices[0].message.content.strip()
                        print(f"📊 Respuesta cruda del modelo: {score_raw}")
                        score = int(''.join(filter(str.isdigit, score_raw)))
                        break
                    except Exception as e:
                        logging.warning(f"Modelo {model} falló: {e}")

                print(score)
                if score:
                    run_query(f"UPDATE users SET ai_score = {score} WHERE id = {user_id}")

            except Exception as e:
                logging.error(f"Error procesando usuario {username}: {e}")
                continue

        return jsonify({"message": "Registros antiguos eliminados y AI Scores actualizados"}), 200

    except Exception as e:
        logging.error(f"Error general: {e}")
        return jsonify({"error": "Error interno al procesar registros"}), 500
