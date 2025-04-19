from flask import Blueprint, redirect, request, session, url_for, jsonify
from requests_oauthlib import OAuth1Session
from services.db_service import run_query
from config import Config
import requests
import logging

auth_bp = Blueprint("auth", __name__)

REQUEST_TOKEN_URL = "https://api.twitter.com/oauth/request_token"
AUTHORIZATION_URL = "https://api.twitter.com/oauth/authenticate"
ACCESS_TOKEN_URL = "https://api.twitter.com/oauth/access_token"
CALLBACK_URL = "http://localhost:5000/auth/callback" 

def get_rapidapi_key():
    query = "SELECT key FROM api_keys WHERE id = 3" 
    result = run_query(query, fetchone=True)
    return result[0] if result else None


@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect("http://localhost:3000"), 200


@auth_bp.route("/save-user", methods=["POST"])
def save_user():
    try:
        data = request.get_json()
        twitter_id = data.get("twitter_id")
        username = data.get("username")
        password = data.get("password") 
        session_token = data.get("session")

        if not twitter_id or not session_token:
            return jsonify({"success": False, "message": "twitter_id y session son obligatorios"}), 400

        query = f"""
        INSERT INTO users (twitter_id, username, password, session)
        VALUES ('{twitter_id}', '{username}', {'NULL' if password is None else f"'{password}'"}, '{session_token}')
        ON CONFLICT (twitter_id) DO UPDATE
        SET username = '{username}', session = '{session_token}'
        RETURNING id;
        """

        user_id = run_query(query, fetchone=True)

        if user_id:
            return jsonify({"success": True, "user_id": user_id[0]}), 201
        else:
            return jsonify({"success": False, "message": "Error al guardar usuario"}), 500

    except Exception as e:
        print(f"❌ Error en /save-user: {e}")
        return jsonify({"success": False, "message": "Error en el servidor"}), 500


@auth_bp.route("/login", methods=["POST"])
def login():
    data = request.json
    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        return jsonify({"error": "Missing Data"}), 400

    rapidapi_key = get_rapidapi_key()
    if not rapidapi_key:
        return jsonify({"error": "Can't find RapidAPI Key"}), 500

    url = "https://twttrapi.p.rapidapi.com/login-email-username"
    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "twttrapi.p.rapidapi.com",
        "Content-Type": "application/x-www-form-urlencoded",
        # 'twttr-proxy': "http://sp4tntbmfv:+lRkdu4bE0E2ecn9uH@ar.smartproxy.com:10001"
    }
    payload = {
        "username_or_email": username,
        "password": password,
        "flow_name": "LoginFlow"
    }

    try:
        response = requests.post(url, headers=headers, data=payload)
        response_data = response.json()

        if response.status_code == 200 and response_data.get("success"):
            return jsonify(response_data), 200
        elif response_data.get("hint") == "Please use second endpoint /login_2fa to continue login.":
            return jsonify({"error": "2FA_REQUIRED", "login_data": response_data.get("login_data")}), 401
        else:
            print(response_data)
            return jsonify({"error": response_data.get("message", "Login failed")}), response.status_code

    except Exception as e:
        logging.error(f"❌ RapidAPI Error: {str(e)}")
        return jsonify({"error": "Server Error"}), 500


@auth_bp.route("/login-2fa", methods=["POST"])
def login_2fa():
    data = request.json
    login_data = data.get("login_data")
    otp = data.get("otp")

    if not login_data or not otp:
        return jsonify({"error": "Missing Data"}), 400

    rapidapi_key = get_rapidapi_key()
    if not rapidapi_key:
        return jsonify({"error": "Can't find RapidAPI Key"}), 500

    url = "https://twttrapi.p.rapidapi.com/login-2fa"
    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "twttrapi.p.rapidapi.com",
        "Content-Type": "application/x-www-form-urlencoded",
        # 'twttr-proxy': "http://sp4tntbmfv:+lRkdu4bE0E2ecn9uH@ar.smartproxy.com:10001"
    }
    payload = {
        "login_data": login_data,
        "response": otp
    }

    try:
        response = requests.post(url, headers=headers, data=payload)
        response_data = response.json()

        if response.status_code == 200 and response_data.get("success"):
            return jsonify(response_data), 200
        else:
            return jsonify({"error": response_data.get("message", "Invalid Code")}), response.status_code

    except Exception as e:
        logging.error(f"❌ RapidAPI Error (2FA): {str(e)}")
        return jsonify({"error": "Server Error"}), 500