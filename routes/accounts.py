from flask import Blueprint, jsonify, request
from services.db_service import run_query
import requests

accounts_bp = Blueprint("accounts", __name__)

def get_socialdata_api_key():
    query = "SELECT key FROM api_keys WHERE id = 2"  
    result = run_query(query, fetchone=True)
    return result[0] if result else None 


@accounts_bp.route("/account/<string:twitter_id>/refresh-profile", methods=["POST"])
def refresh_user_profile(twitter_id):
    API_KEY = get_socialdata_api_key()
    if not API_KEY:
        return jsonify({"error": "API Key no configurada"}), 500

    try:
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Accept": "application/json"
        }
        url = f"https://api.socialdata.tools/twitter/user/{twitter_id}"
        response = requests.get(url, headers=headers)

        if response.status_code == 402:
            return jsonify({"error": "Créditos insuficientes para la API"}), 402
        if response.status_code == 404:
            return jsonify({"error": "Usuario no encontrado en Twitter"}), 404
        if not response.ok:
            return jsonify({"error": "Error al consultar la API externa"}), response.status_code

        data = response.json()
        username = data.get("screen_name")
        profile_pic = data.get("profile_image_url_https")
        followers_count = data.get("followers_count")
        friends_count = data.get("friends_count")

        if not username or not profile_pic:
            return jsonify({"error": "No se pudo obtener el nombre o la imagen"}), 500

        update_query = f"""
        UPDATE users
        SET username = '{username}', profile_pic = '{profile_pic}', followers = '{followers_count}', following = '{friends_count}'
        WHERE twitter_id = '{twitter_id}'
        """
        run_query(update_query)

        return jsonify({
            "message": "Perfil actualizado correctamente",
            "username": username,
            "profile_pic": profile_pic,
            "followers": followers_count,
            "following": friends_count
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@accounts_bp.route("/accounts", methods=["GET"])
def get_accounts():
    query = "SELECT id, twitter_id, username FROM users"
    accounts = run_query(query, fetchall=True)

    if not accounts:
        
        return jsonify({"message": "No hay cuentas registradas"}), 200

    accounts_list = [{"id": acc[0], "twitter_id": acc[1], "username": acc[2]} for acc in accounts]

    return jsonify(accounts_list), 200


@accounts_bp.route("/account/<string:twitter_id>", methods=["GET"])
def get_account_details(twitter_id):
    user_query = f"""
    SELECT id, username, session, password, language, custom_style, followers, following, status, extraction_filter, profile_pic
    FROM users
    WHERE twitter_id = '{twitter_id}'
    """
    user_data = run_query(user_query, fetchone=True)
    print(user_data)
    if not user_data:
        return jsonify({"error": "Cuenta no encontrada"}), 404

    id = user_data[0]
    
    user_info = {
        "id": user_data[0],
        "username": user_data[1],
        "session": user_data[2],
        "password": user_data[3],
        "language": user_data[4],  
        "custom_style": user_data[5],
        "followers": user_data[6],
        "following": user_data[7],
        "status": user_data[8],
        "extraction_filter": user_data[9],
        "profile_pic": user_data[10]
    }

    monitored_users_query = f"""
    SELECT twitter_username
    FROM monitored_users
    WHERE user_id = '{id}'
    """
    monitored_users = run_query(monitored_users_query, fetchall=True)
    monitored_users_list = [
        {"twitter_username": mu[0]}
        for mu in monitored_users
    ]

    keywords_query = f"""
    SELECT keyword
    FROM user_keywords
    WHERE user_id = '{id}'
    """
    keywords = run_query(keywords_query, fetchall=True)
    keywords_list = [kw[0] for kw in keywords]
    
    posts_count_query = f"""
    SELECT COUNT(*) 
    FROM logs
    WHERE user_id = '{id}' AND event_type = 'POST'
    """
    posts_count_result = run_query(posts_count_query, fetchone=True)
    posts_count = posts_count_result[0] if posts_count_result else 0

    response = {
        "user": user_info,
        "monitored_users": monitored_users_list,
        "keywords": keywords_list,
        "total_posts": posts_count
    }
    return jsonify(response), 200


@accounts_bp.route("/account/<string:twitter_id>", methods=["PUT"])
def update_account(twitter_id):
    data = request.json

    language = data.get("language")
    custom_style = data.get("custom_style")
    monitored_users = data.get("monitored_users", [])
    keywords = data.get("keywords", [])
    extraction_filter = data.get("extraction_filter")
    
    user_query = f"SELECT id FROM users WHERE twitter_id = '{twitter_id}'"
    user_data = run_query(user_query, fetchone=True)
    
    if not user_data:
        return jsonify({"error": "Cuenta no encontrada"}), 404

    user_id = user_data[0]

    update_user_query = f"""
    UPDATE users
    SET language = '{language}', custom_style = '{custom_style}', extraction_filter = '{extraction_filter}'
    WHERE twitter_id = '{twitter_id}'
    """
    run_query(update_user_query)

    run_query(f"DELETE FROM monitored_users WHERE user_id = {user_id}")
    for username in monitored_users:
        run_query(f"INSERT INTO monitored_users (user_id, twitter_username) VALUES ({user_id}, '{username}')")

    run_query(f"DELETE FROM user_keywords WHERE user_id = {user_id}")
    for keyword in keywords:
        run_query(f"INSERT INTO user_keywords (user_id, keyword) VALUES ({user_id}, '{keyword}')")

    return jsonify({"message": "Cuenta actualizada correctamente"}), 200


@accounts_bp.route("/account/<string:twitter_id>", methods=["DELETE"])
def delete_account(twitter_id):
    user_query = f"SELECT id FROM users WHERE twitter_id = '{twitter_id}'"
    user_data = run_query(user_query, fetchone=True)

    if not user_data:
        return jsonify({"error": "Cuenta no encontrada"}), 404

    user_id = user_data[0]

    run_query(f"DELETE FROM monitored_users WHERE user_id = {user_id}")
    run_query(f"DELETE FROM user_keywords WHERE user_id = {user_id}")
    run_query(f"DELETE FROM users WHERE id = {user_id}")

    return jsonify({"message": "Cuenta eliminada correctamente"}), 200