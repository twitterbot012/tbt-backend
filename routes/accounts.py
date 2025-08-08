from flask import Blueprint, jsonify, request
from services.db_service import run_query
import requests
from supabase import create_client
import base64
from urllib.parse import urlparse
import uuid
from collections import defaultdict
from datetime import datetime
from routes.logs import log_usage
from openai import OpenAI
import pytz
import resend

accounts_bp = Blueprint("accounts", __name__)

SUPABASE_URL = "https://tmosrdszzpgfdbexstbu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRtb3NyZHN6enBnZmRiZXhzdGJ1Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczOTQ3NTMyOSwiZXhwIjoyMDU1MDUxMzI5fQ.cUiNxjRcnwuelk9XHbRiRgpL88U43OBJbum82vnQlk8" 
BUCKET_NAME = "images"
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
resend.api_key = "re_9hbEHRuy_KeEhu4QXqGb3SR7tMwN2PrBr" 

def get_socialdata_api_key():
    query = "SELECT key FROM api_keys WHERE id = 2"  
    result = run_query(query, fetchone=True)
    return result[0] if result else None 


def get_rapidapi_key():
    query = "SELECT key FROM api_keys WHERE id = 3" 
    result = run_query(query, fetchone=True)
    return result[0] if result else None


def get_openai_api_key():
    query = "SELECT key FROM api_keys WHERE id = 1"
    result = run_query(query, fetchone=True)
    return result[0] if result else None  


@accounts_bp.route("/account/<string:twitter_id>/refresh-profile", methods=["POST"])
def refresh_user_profile(twitter_id):
    API_KEY = get_socialdata_api_key()
    if not API_KEY:
        return jsonify({"error": "API Key no configurada"}), 500

    query = f"SELECT username FROM users WHERE twitter_id = '{twitter_id}'"
    result = run_query(query, fetchone=True)
    username = result[0] if result else None  

    try:
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Accept": "application/json"
        }
        url = f"https://api.socialdata.tools/twitter/user/{username}"
        response = requests.get(url, headers=headers)
        log_usage("SOCIALDATA")
        print(f'response refresh {response.text}')
        if response.status_code == 402:
            return jsonify({"error": "Créditos insuficientes para la API"}), 402
        if response.status_code == 404:
            return jsonify({"error": "Usuario no encontrado en Twitter"}), 404
        if not response.ok:
            return jsonify({"error": "Error al consultar la API externa"}), response.status_code

        data = response.json()
        username = data.get("screen_name")
        name = data.get("name", '(Refresh Profile)')
        profile_pic = data.get("profile_image_url_https")
        followers_count = data.get("followers_count")
        friends_count = data.get("friends_count")

        
        if not username or not profile_pic:
            return jsonify({"error": "No se pudo obtener el nombre o la imagen"}), 500

        update_query = f"""
        UPDATE users
        SET username = '{username}', profile_pic = '{profile_pic}', followers = '{followers_count}', following = '{friends_count}', name = '{name}'
        WHERE twitter_id = '{twitter_id}'
        """
        run_query(update_query)

        return jsonify({
            "message": "Perfil actualizado correctamente",
            "username": username,
            "profile_pic": profile_pic,
            "followers": followers_count,
            "following": friends_count,
            "name": name
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@accounts_bp.route("/account/refresh-all-profiles", methods=["POST"])
def refresh_all_user_profiles():
    API_KEY = get_socialdata_api_key()
    if not API_KEY:
        return jsonify({"error": "API Key no configurada"}), 500

    try:
        users = run_query("SELECT twitter_id FROM users WHERE twitter_id IS NOT NULL")
        if not users:
            return jsonify({"message": "No hay usuarios con twitter_id"}), 200

        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Accept": "application/json"
        }

        updated = []
        failed = []

        for user in users:
            twitter_id = user["twitter_id"]
            try:
                url = f"https://api.socialdata.tools/twitter/user/{twitter_id}"
                response = requests.get(url, headers=headers)
                log_usage("SOCIALDATA")

                if response.status_code == 402:
                    failed.append({"twitter_id": twitter_id, "error": "Créditos insuficientes"})
                    break  

                if response.status_code == 404:
                    failed.append({"twitter_id": twitter_id, "error": "No encontrado"})
                    continue

                if not response.ok:
                    failed.append({"twitter_id": twitter_id, "error": "Error externo"})
                    continue

                data = response.json()
                username = data.get("screen_name")
                name = data.get("name", '(Refresh Profile)')
                profile_pic = data.get("profile_image_url_https")
                followers_count = data.get("followers_count")
                friends_count = data.get("friends_count")

                if not username or not profile_pic:
                    failed.append({"twitter_id": twitter_id, "error": "Faltan datos esenciales"})
                    continue

                update_query = f"""
                UPDATE users
                SET username = '{username}', profile_pic = '{profile_pic}', followers = '{followers_count}', following = '{friends_count}', name = '{name}'
                WHERE twitter_id = '{twitter_id}'
                """
                run_query(update_query)
                updated.append(twitter_id)

            except Exception as inner_error:
                failed.append({"twitter_id": twitter_id, "error": str(inner_error)})

        return jsonify({
            "updated_count": len(updated),
            "failed_count": len(failed),
            "updated": updated,
            "failed": failed
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@accounts_bp.route("/account/<string:twitter_id>/update-profile", methods=["PUT"])
def update_user_profile(twitter_id):
    data = request.json
    new_username = data.get("username")
    new_name = data.get("name")
    new_profile_pic_base64 = data.get("profile_pic")

    result = run_query(f"SELECT session FROM users WHERE twitter_id = '{twitter_id}'", fetchone=True)
    if not result:
        return jsonify({"error": "User not found"}), 404
    session = result[0]

    if not new_username and not new_profile_pic_base64 and not new_name:
        return jsonify({"error": "Username, Image or Name missing."}), 400

    rapidapi_key = get_rapidapi_key()
    if not rapidapi_key:
        return jsonify({"error": "RapidAPI Key Missing"}), 500

    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "twttrapi.p.rapidapi.com",
        "Content-Type": "application/x-www-form-urlencoded",
        "twttr-session": session
    }

    responses = {}
    uploaded_file_url = None
    username_updated = False

    try:
        if new_profile_pic_base64 and new_profile_pic_base64.startswith("data:image/"):
            image_data = base64.b64decode(new_profile_pic_base64.split(",")[1])
            ext = new_profile_pic_base64.split(";")[0].split("/")[1]
            filename = f"{uuid.uuid4()}.{ext}"
            upload_path = f"{twitter_id}/{filename}"

            upload_response = supabase.storage.from_(BUCKET_NAME).upload(
                path=upload_path,
                file=image_data,
                file_options={"content-type": f"image/{ext}"}
            )

            if isinstance(upload_response, dict) and upload_response.get("error"):
                return jsonify({
                    "error": "Error uploading image to Supabase",
                    "details": upload_response["error"]["message"]
                }), 500

            uploaded_file_url = f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET_NAME}/{upload_path}"

            payload = {"image_url": uploaded_file_url}
            url = "https://twttrapi.p.rapidapi.com/update-profile-image"
            res = requests.post(url, headers=headers, data=payload)
            log_usage("RAPIDAPI")
            responses["profile_pic"] = res.json()

            if not res.ok:
                return jsonify({
                    "error": "Error uploading image",
                    "details": res.text
                }), res.status_code

        if new_username:
            payload = {
                "screen_name": new_username,
                "name": new_name or ""
            }
            url = "https://twttrapi.p.rapidapi.com/update-profile"
            res = requests.post(url, headers=headers, data=payload)
            log_usage("RAPIDAPI")

            try:
                res_json = res.json()
            except Exception:
                res_json = {}

            if not res.ok or not res_json.get("success", True):
                return jsonify({
                    "error": "Username is already taken",
                    "details": res_json.get("error", res.text)
                }), res.status_code if not res.ok else 400

            responses["username_name"] = res_json
            username_updated = True

        updates = []
        if username_updated:
            updates.append(f"username = '{new_username}'")
            if new_name:
                updates.append(f"name = '{new_name}'")
        elif new_name:
            updates.append(f"name = '{new_name}'")
        if uploaded_file_url:
            updates.append(f"profile_pic = '{uploaded_file_url}'")

        if updates:
            update_query = f"""
            UPDATE users SET {', '.join(updates)}
            WHERE twitter_id = '{twitter_id}'
            """
            run_query(update_query)

        if uploaded_file_url:
            parsed = urlparse(uploaded_file_url)
            path_to_delete = parsed.path.replace(f"/storage/v1/object/public/{BUCKET_NAME}/", "")
            supabase.storage.from_(BUCKET_NAME).remove([path_to_delete])

        return jsonify({
            "message": "Profile updated",
            "api_response": responses
        }), 200

    except Exception as e:
        return jsonify({"error": f"Error: {str(e)}"}), 500


@accounts_bp.route("/accounts", methods=["GET"])
def get_accounts():
    query = """
            SELECT 
                u.id, u.twitter_id, u.username, u.profile_pic, u.followers, u.following, u.rate_limit,
                COALESCE(ct.collected_count, 0) AS collected_tweets,
                COALESCE(pt.last_post, NULL) AS last_post,
                COALESCE(le.last_extract, NULL) AS last_extract
            FROM users u
            LEFT JOIN (
                SELECT user_id, COUNT(*) AS collected_count
                FROM collected_tweets
                GROUP BY user_id
            ) ct ON u.id = ct.user_id
            LEFT JOIN (
                SELECT user_id, MAX(created_at) AS last_post
                FROM posted_tweets
                GROUP BY user_id
            ) pt ON u.id = pt.user_id
            LEFT JOIN (
                SELECT user_id, MAX(timestamp) AS last_extract
                FROM logs
                WHERE event_type = 'EXTRACT'
                GROUP BY user_id
            ) le ON u.id = le.user_id
    """
    accounts = run_query(query, fetchall=True)

    if not accounts:
        return jsonify({"message": "No hay cuentas registradas"}), 200

    accounts_list = [{
        "id": acc[0],
        "twitter_id": acc[1],
        "username": acc[2],
        "profile_pic": acc[3],
        "followers": acc[4],
        "following": acc[5],
        "rate_limit": acc[6],
        "collected_tweets": acc[7],
        "last_post": acc[8].isoformat() if acc[8] else None,
        "last_extract": acc[9].isoformat() if acc[9] else None
    } for acc in accounts]
    print(accounts_list)

    return jsonify(accounts_list), 200


@accounts_bp.route("/account/<string:twitter_id>", methods=["GET"])
def get_account_details(twitter_id):
    user_query = f"""
    SELECT id, username, session, password, language, custom_style, 
    followers, following, status, extraction_filter, profile_pic, 
    notes, likes_limit, retweets_limit, comments_limit, extraction_method, 
    name, ai_score, follows_limit, verified
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
        "profile_pic": user_data[10],
        "notes": user_data[11],
        "likes_limit": user_data[12],
        "retweets_limit": user_data[13],
        "comments_limit": user_data[14],
        "extraction_method": user_data[15],
        "name": user_data[16],
        "ai_score": user_data[17],
        "follows_limit": user_data[18],
        "verified": user_data[19]
    }
    
    follow_users_query = f"""
    SELECT twitter_username
    FROM follow_users
    WHERE user_id = '{id}'
    """
    
    follow_users = run_query(follow_users_query, fetchall=True)
    follow_users_list = [
        {"twitter_username": mu[0]}
        for mu in follow_users
    ]
    
    like_users_query = f"""
    SELECT twitter_username
    FROM like_users
    WHERE user_id = '{id}'
    """
    like_users = run_query(like_users_query, fetchall=True)
    like_users_list = [
        {"twitter_username": mu[0]}
        for mu in like_users
    ]
    
    comment_users_query = f"""
    SELECT twitter_username
    FROM comment_users
    WHERE user_id = '{id}'
    """
    comment_users = run_query(comment_users_query, fetchall=True)
    comment_users_list = [
        {"twitter_username": mu[0]}
        for mu in comment_users
    ]
    
    retweet_users_query = f"""
    SELECT twitter_username
    FROM retweet_users
    WHERE user_id = '{id}'
    """
    retweet_users = run_query(retweet_users_query, fetchall=True)
    retweet_users_list = [
        {"twitter_username": mu[0]}
        for mu in retweet_users
    ]

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
        "comments": comment_users_list,
        "likes": like_users_list,
        "retweets": retweet_users_list,
        "total_posts": posts_count,
        "follows": follow_users_list
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
    notes = data.get("notes", '')
    retweets = data.get("retweets", [])
    comments = data.get("comments", [])
    follows = data.get("follows", [])
    likes = data.get("likes", [])
    retweets_limit = data.get("retweets_limit", [])
    comments_limit = data.get("comments_limit", [])
    likes_limit = data.get("likes_limit", [])
    follows_limit = data.get("follows_limit", [])
    extraction_method = data.get("extraction_method", 1)

    user_query = f"SELECT id FROM users WHERE twitter_id = '{twitter_id}'"
    user_data = run_query(user_query, fetchone=True)
    
    if not user_data:
        return jsonify({"error": "Cuenta no encontrada"}), 404

    user_id = user_data[0]

    update_user_query = f"""
    UPDATE users
    SET language = '{language}', custom_style = '{custom_style}', extraction_filter = '{extraction_filter}',
    notes = '{notes}', likes_limit = '{likes_limit}', comments_limit = '{comments_limit}', follows_limit = '{follows_limit}',
    retweets_limit = '{retweets_limit}', extraction_method = '{extraction_method}'
    WHERE twitter_id = '{twitter_id}'
    """
    run_query(update_user_query)

    run_query(f"DELETE FROM monitored_users WHERE user_id = {user_id}")
    for username in monitored_users:
        run_query(f"INSERT INTO monitored_users (user_id, twitter_username) VALUES ({user_id}, '{username}')")

    run_query(f"DELETE FROM user_keywords WHERE user_id = {user_id}")
    for keyword in keywords:
        run_query(f"INSERT INTO user_keywords (user_id, keyword) VALUES ({user_id}, '{keyword}')")
        
    run_query(f"DELETE FROM retweet_users WHERE user_id = {user_id}")
    for retweet in retweets:
        run_query(f"INSERT INTO retweet_users (user_id, twitter_username) VALUES ({user_id}, '{retweet}')")
        
    run_query(f"DELETE FROM follow_users WHERE user_id = {user_id}")
    for follow in follows:
        run_query(f"INSERT INTO follow_users (user_id, twitter_username) VALUES ({user_id}, '{follow}')")
        
    run_query(f"DELETE FROM comment_users WHERE user_id = {user_id}")
    for comment in comments:
        run_query(f"INSERT INTO comment_users (user_id, twitter_username) VALUES ({user_id}, '{comment}')")
        
    run_query(f"DELETE FROM like_users WHERE user_id = {user_id}")
    for like in likes:
        run_query(f"INSERT INTO like_users (user_id, twitter_username) VALUES ({user_id}, '{like}')")


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
    run_query(f"DELETE FROM retweet_users WHERE user_id = {user_id}")
    run_query(f"DELETE FROM follow_users WHERE user_id = {user_id}")
    run_query(f"DELETE FROM comment_users WHERE user_id = {user_id}")
    run_query(f"DELETE FROM like_users WHERE user_id = {user_id}")
    run_query(f"DELETE FROM retweet_users WHERE user_id = {user_id}")
    run_query(f"DELETE FROM collected_tweets WHERE user_id = {user_id}")
    run_query(f"DELETE FROM posted_tweets WHERE user_id = {user_id}")

    return jsonify({"message": "Cuenta eliminada correctamente"}), 200


@accounts_bp.route("/usage/requests-per-day", methods=["GET"])
def get_requests_grouped_by_api_and_day():
    query = """
    SELECT api, DATE(created_at) AS day, SUM(requests) AS total_requests
    FROM usage
    GROUP BY api, day
    ORDER BY day DESC, api
    """
    results = run_query(query, fetchall=True)

    if not results:
        return jsonify({"message": "No hay registros de uso"}), 200

    grouped_data = defaultdict(lambda: {})

    for api, day, total_requests in results:
        day_str = day.strftime("%Y-%m-%d") if isinstance(day, datetime) else str(day)
        grouped_data[day_str][api] = total_requests

    return jsonify(grouped_data), 200


# OLD


@accounts_bp.route("old/accounts", methods=["GET"])
def old_get_accounts():
    query = "SELECT id, twitter_id, username FROM users"
    accounts = run_query(query, fetchall=True)

    if not accounts:
        
        return jsonify({"message": "No hay cuentas registradas"}), 200

    accounts_list = [{"id": acc[0], "twitter_id": acc[1], "username": acc[2]} for acc in accounts]

    return jsonify(accounts_list), 200


@accounts_bp.route("old/account/<string:twitter_id>", methods=["GET"])
def old_get_account_details(twitter_id):
    user_query = f"""
    SELECT id, username, session, password, language, custom_style
    FROM users
    WHERE twitter_id = '{twitter_id}'
    """
    user_data = run_query(user_query, fetchone=True)
    
    if not user_data:
        return jsonify({"error": "Cuenta no encontrada"}), 404

    id = user_data[0]
    
    user_info = {
        "id": user_data[0],
        "username": user_data[1],
        "session": user_data[2],
        "password": user_data[3],
        "language": user_data[4],  
        "custom_style": user_data[5]  
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

    response = {
        "user": user_info,
        "monitored_users": monitored_users_list,
        "keywords": keywords_list
    }
    return jsonify(response), 200


@accounts_bp.route("old/account/<string:twitter_id>", methods=["PUT"])
def old_update_account(twitter_id):
    data = request.json

    language = data.get("language")
    custom_style = data.get("custom_style")
    monitored_users = data.get("monitored_users", [])
    keywords = data.get("keywords", [])

    user_query = f"SELECT id FROM users WHERE twitter_id = '{twitter_id}'"
    user_data = run_query(user_query, fetchone=True)
    
    if not user_data:
        return jsonify({"error": "Cuenta no encontrada"}), 404

    user_id = user_data[0]

    update_user_query = f"""
    UPDATE users
    SET language = '{language}', custom_style = '{custom_style}'
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


@accounts_bp.route("/account/<string:twitter_id>/verify-category", methods=["POST"])
def verify_account_category(twitter_id):
    try:
        user_query = f"""
        SELECT id, username, name
        FROM users
        WHERE twitter_id = '{twitter_id}'
        """
        user_data = run_query(user_query, fetchone=True)

        if not user_data:
            return jsonify({"error": "Cuenta no encontrada"}), 404

        user_id, username, name = user_data

        monitored_query = f"""
        SELECT twitter_username
        FROM monitored_users
        WHERE user_id = '{user_id}'
        """
        monitored_users = run_query(monitored_query, fetchall=True)
        monitored_list = [u[0] for u in monitored_users]

        keywords_query = f"""
        SELECT keyword
        FROM user_keywords
        WHERE user_id = '{user_id}'
        """
        keywords = run_query(keywords_query, fetchall=True)
        keywords_list = [k[0] for k in keywords]

        api_key = get_openai_api_key()
        if not api_key:
            return jsonify({"error": "No se pudo obtener la API Key de OpenAI"}), 500

        client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key
        )

        context = f"""
        The user's Twitter handle is @{username} and the name is "{name}".
        They monitor these users: {", ".join(monitored_list)}.
        And these keywords: {", ".join(keywords_list)}.

        Based on this data, is the monitored content consistent with the user's name and handle?
        Return only one of the following values: "1" for verified, "0" for not verified, and "-" for inconclusive.
        """

        models_to_try = [
            "meta-llama/llama-4-scout:free",
            "google/gemini-2.0-flash-001",
            "deepseek/deepseek-chat-v3-0324",
            "openai/gpt-4o-2024-11-20",
            "anthropic/claude-3.7-sonnet"
        ]

        for model in models_to_try:
            try:
                response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You are a content verification assistant."},
                        {"role": "user", "content": context}
                    ],
                    max_tokens=10,
                    temperature=0.2
                )
                log_usage("OPENROUTER")
                if response.choices and response.choices[0].message.content:
                    answer = response.choices[0].message.content.strip()
                    if answer in ["1", "0", "-"]:
                        run_query(f"UPDATE users SET verified = '{answer}' WHERE id = '{user_id}'")
                        return jsonify({"result": answer}), 200
            except Exception as e:
                print(f"Error usando modelo {model}: {e}")

        return jsonify({"error": "No se pudo verificar con ningún modelo"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@accounts_bp.route("/usage/email-today", methods=["POST"])
def send_usage_email():
    try:
        india_time = datetime.now(pytz.timezone("Asia/Kolkata"))
        formatted_date = india_time.strftime("%Y-%m-%d %H:%M:%S")

        today_query = """
        SELECT api, SUM(requests) as total_requests
        FROM usage
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        GROUP BY api
        """
        results = run_query(today_query, fetchall=True)

        if not results:
            return jsonify({"message": "No usage today"}), 200

        html = f"<h2>Daily APIs usage report</h2><p>Date: {formatted_date}</p><ul>"
        for api, total in results:
            html += f"<li><b>{api}</b>: {total} requests</li>"
        html += "</ul>"

        response = resend.Emails.send({
            "from": "onboarding@resend.dev",
            "to": "niranjan.govindaraju.vercel@gmail.com", 
            "subject": f"APIs Usage - Daily Report ({formatted_date})",
            "html": html
        })

        return jsonify({"message": "Email Sent.", "response": response}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- Custom Extract Jobs ---

@accounts_bp.route("/custom-extracts", methods=["POST"])
def create_custom_extract_job():
    """
    Crea un job 'pending' para el Método 3.
    Body esperado:
    {
      "user_id": 1,
      "date_from": "2025-07-01 00:00:00",
      "date_to":   "2025-07-07 23:59:59",
      "max_items": 2000,
      "scope":     "users_keywords" | "keywords_only"
    }
    """
    data = request.get_json(force=True, silent=True) or {}
    user_id = data.get("user_id")
    date_from = data.get("date_from")
    date_to = data.get("date_to")
    max_items = data.get("max_items", 2000)
    scope = data.get("scope", "users_keywords")

    if not user_id or not date_from or not date_to:
        return jsonify({"error": "user_id, date_from y date_to son requeridos"}), 400

    if scope not in ("users_keywords", "keywords_only"):
        return jsonify({"error": "scope inválido"}), 400

    try:
        # Validación básica de usuario
        exists = run_query(f"SELECT 1 FROM users WHERE id = {int(user_id)}", fetchone=True)
        if not exists:
            return jsonify({"error": "Usuario no encontrado"}), 404

        # Validación de límites
        try:
            mi = int(max_items)
        except Exception:
            return jsonify({"error": "max_items debe ser entero"}), 400
        if mi < 1 or mi > 2000:
            return jsonify({"error": "max_items fuera de rango, 1 a 2000"}), 400

        # Inserción
        insert_q = f"""
        INSERT INTO custom_extract_jobs (user_id, date_from, date_to, max_items, scope, status, created_at, updated_at)
        VALUES ({int(user_id)}, '{date_from}', '{date_to}', {mi}, '{scope}', 'pending', NOW(), NOW())
        RETURNING id, user_id, date_from, date_to, max_items, scope, status, extracted_count, note, created_at, updated_at
        """
        row = run_query(insert_q, fetchone=True)
        job = {
            "id": row[0],
            "user_id": row[1],
            "date_from": row[2].isoformat() if row[2] else None,
            "date_to": row[3].isoformat() if row[3] else None,
            "max_items": row[4],
            "scope": row[5],
            "status": row[6],
            "extracted_count": row[7],
            "note": row[8],
            "created_at": row[9].isoformat() if row[9] else None,
            "updated_at": row[10].isoformat() if row[10] else None,
        }
        return jsonify(job), 201

    except Exception as e:
        return jsonify({"error": f"Error creando el job, {str(e)}"}), 500


@accounts_bp.route("/custom-extracts/latest", methods=["GET"])
def get_latest_custom_extract_job():
    """
    Devuelve el último job creado para un user_id.
    Query param requerido: ?user_id=1
    """
    try:
        user_id = request.args.get("user_id", type=int)
        if not user_id:
            return jsonify({"error": "user_id es requerido"}), 400

        q = f"""
        SELECT id, user_id, date_from, date_to, max_items, scope, status, extracted_count, note, created_at, updated_at
        FROM custom_extract_jobs
        WHERE user_id = {user_id}
        ORDER BY created_at DESC
        LIMIT 1
        """
        row = run_query(q, fetchone=True)
        if not row:
            return jsonify({"message": "No jobs found"}), 200

        job = {
            "id": row[0],
            "user_id": row[1],
            "date_from": row[2].isoformat() if row[2] else None,
            "date_to": row[3].isoformat() if row[3] else None,
            "max_items": row[4],
            "scope": row[5],
            "status": row[6],
            "extracted_count": row[7],
            "note": row[8],
            "created_at": row[9].isoformat() if row[9] else None,
            "updated_at": row[10].isoformat() if row[10] else None,
        }
        return jsonify(job), 200

    except Exception as e:
        return jsonify({"error": f"Error consultando jobs, {str(e)}"}), 500


@accounts_bp.route("/custom-extracts/<int:job_id>", methods=["DELETE"])
def delete_custom_extract_job(job_id):
    """
    Elimina un job solo si está en 'pending'.
    """
    try:
        sel = run_query(f"SELECT status FROM custom_extract_jobs WHERE id = {job_id}", fetchone=True)
        if not sel:
            return jsonify({"error": "Job no encontrado"}), 404

        status = sel[0]
        if status != "pending":
            return jsonify({"error": "Solo se pueden borrar jobs en estado pending"}), 400

        run_query(f"DELETE FROM custom_extract_jobs WHERE id = {job_id}")
        return jsonify({"message": "Job borrado"}), 200

    except Exception as e:
        return jsonify({"error": f"Error borrando job, {str(e)}"}), 500
