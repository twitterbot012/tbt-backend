from flask import Blueprint, request, jsonify
from services.db_service import run_query

keywords_bp = Blueprint("keywords", __name__)

@keywords_bp.route("/add_keyword", methods=["POST"])
def add_keyword():
    data = request.json
    user_id = data.get("user_id")
    keyword = data.get("keyword")

    if not user_id or not keyword:
        return jsonify({"error": "Faltan parámetros"}), 400

    query = "INSERT INTO user_keywords (user_id, keyword) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    run_query(query, (user_id, keyword))

    return jsonify({"message": f"Keyword '{keyword}' agregada para el usuario {user_id}"}), 200


@keywords_bp.route("/get_keywords/<int:user_id>", methods=["GET"])
def get_keywords(user_id):
    query = "SELECT keyword FROM user_keywords WHERE user_id = %s"
    keywords = run_query(query, (user_id,), fetchall=True)
    return jsonify({"user_id": user_id, "keywords": [k[0] for k in keywords]})
