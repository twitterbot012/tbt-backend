from flask import Blueprint, jsonify, request, session
from services.db_service import run_query

monitored_bp = Blueprint("monitored", __name__)

@monitored_bp.route("/monitored_users", methods=["POST"])
def add_monitored_user():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"error": "Usuario no autenticado"}), 401

    data = request.json
    twitter_username = data.get("twitter_username")
    twitter_user_id = data.get("twitter_user_id")

    if not twitter_username or not twitter_user_id:
        return jsonify({"error": "Faltan parámetros"}), 400

    query = """
    INSERT INTO monitored_users (user_id, twitter_username, twitter_user_id)
    VALUES (%s, %s, %s)
    ON CONFLICT (user_id, twitter_user_id) DO NOTHING
    RETURNING id;
    """
    result = run_query(query, (user_id, twitter_username, twitter_user_id), fetchone=True)

    if result:
        return jsonify({"message": "Usuario monitoreado agregado correctamente"}), 201
    else:
        return jsonify({"error": "El usuario ya estaba siendo monitoreado"}), 409


@monitored_bp.route("/monitored_users", methods=["GET"])
def get_monitored_users():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"error": "Usuario no autenticado"}), 401

    query = "SELECT twitter_username, twitter_user_id FROM monitored_users WHERE user_id = %s"
    users = run_query(query, (user_id,), fetchall=True)

    if not users:
        return jsonify({"message": "No hay usuarios monitoreados"}), 404

    return jsonify([{"twitter_username": u[0], "twitter_user_id": u[1]} for u in users]), 200


@monitored_bp.route("/monitored_users/<twitter_user_id>", methods=["DELETE"])
def delete_monitored_user(twitter_user_id):
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"error": "Usuario no autenticado"}), 401

    query = "DELETE FROM monitored_users WHERE user_id = %s AND twitter_user_id = %s RETURNING id"
    result = run_query(query, (user_id, twitter_user_id), fetchone=True)

    if result:
        return jsonify({"message": "Usuario eliminado correctamente"}), 200
    else:
        return jsonify({"error": "El usuario no estaba siendo monitoreado"}), 404
