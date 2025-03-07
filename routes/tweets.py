from flask import Blueprint, jsonify, request, send_file
from services.db_service import run_query
from services.post_tweets import post_tweet 
import logging
import random
from fpdf import FPDF
import os

logging.basicConfig(level=logging.INFO)

tweets_bp = Blueprint("tweets", __name__)

@tweets_bp.route("/tweets", methods=["GET"])
def get_collected_tweets():
    query = "SELECT source_username, tweet_id, tweet_text, created_at FROM collected_tweets ORDER BY created_at DESC LIMIT 50"
    tweets = run_query(query, fetchall=True)
    if not tweets:
        return jsonify({"message": "No hay tweets recolectados"}), 404
    return jsonify([{"source_username": t[0], "tweet_id": t[1], "tweet_text": t[2], "created_at": t[3]} for t in tweets]), 200


@tweets_bp.route("/post_tweet", methods=["POST"])
def post_tweet_route():
    data = request.json
    user_id = data.get("user_id")
    tweet_text = data.get("tweet_text")

    if not user_id or not tweet_text:
        return jsonify({"error": "Faltan parámetros (user_id o tweet_text)"}), 400

    if len(tweet_text) > 280:
        return jsonify({"error": "El texto del tweet excede el límite de 280 caracteres"}), 400

    response, status_code = post_tweet(user_id, tweet_text)

    return jsonify(response), status_code


@tweets_bp.route("/get-all-tweets/<twitter_id>", methods=["GET"])
def get_all_tweets(twitter_id):
    query = f"""
        SELECT user_id, source_value, tweet_id, tweet_text, created_at 
        FROM collected_tweets 
        WHERE user_id = '{twitter_id}' 
        ORDER BY created_at DESC
    """
    tweets = run_query(query, fetchall=True)
    
    if not tweets:
        return jsonify({"message": "No hay tweets recolectados para este usuario"}), 404
    
    return jsonify([
        {"user_id": t[0], "source_value": [1], "tweet_id": t[2], "tweet_text": t[3], "created_at": t[4]} 
        for t in tweets
    ]), 200


@tweets_bp.route("/delete-tweet/<tweet_id>", methods=["DELETE"])
def delete_tweet(tweet_id):
    query = f"DELETE FROM collected_tweets WHERE tweet_id = '{tweet_id}'"
    run_query(query)
    return jsonify({"message": "Tweet eliminado exitosamente"}), 200


@tweets_bp.route("/edit-tweet/<tweet_id>", methods=["PUT"])
def edit_tweet(tweet_id):
    data = request.json
    new_text = data.get("tweet_text")

    if not new_text:
        return jsonify({"error": "Faltan parámetros (tweet_text)"}), 400

    query = f"UPDATE collected_tweets SET tweet_text = '{new_text}' WHERE tweet_id = '{tweet_id}'"
    run_query(query)
    return jsonify({"message": "Tweet actualizado exitosamente"}), 200


@tweets_bp.route("/add-tweet", methods=["POST"])
def add_tweet():
    data = request.json
    user_id = data.get("user_id")
    tweet_text = data.get("tweet_text")

    if not user_id or not tweet_text:
        return jsonify({"error": "Faltan parámetros (user_id o tweet_text)"}), 400

    if len(tweet_text) > 280:
        return jsonify({"error": "El texto del tweet excede el límite de 280 caracteres"}), 400

    query = f"INSERT INTO collected_tweets (user_id, tweet_id, source_value, tweet_text, created_at) VALUES ('{user_id}', {random.randint(10**17, 10**18 -1)},'', '{tweet_text}', NOW())"
    run_query(query)
    return jsonify({"message": "Tweet agregado exitosamente"}), 201


@tweets_bp.route("/generate-pdf", methods=["GET"])
def generate_pdf():
    user_id = request.args.get("user_id")
    print(user_id)
    
    if not user_id:
        return jsonify({"error": "Se requiere el user_id"}), 400

    query = f"""
    SELECT u.username, ct.tweet_text, ct.created_at 
    FROM collected_tweets ct
    INNER JOIN users u ON ct.user_id = u.id
    WHERE ct.user_id = {user_id}
    ORDER BY ct.created_at DESC
    """
    tweets = run_query(query, fetchall=True)

    print(tweets)
    if not tweets:
        return jsonify({"error": "No hay tweets para este usuario"}), 404

    username = tweets[0][0] if tweets else "Unknown User"

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", style="B", size=16)
    pdf.cell(200, 10, f"Collected tweets from: {username}", ln=True, align="C")
    
    pdf.set_font("Arial", size=12)
    for username, tweet_text, created_at in tweets:
        pdf.multi_cell(0, 10, f" - @{username}: {tweet_text}\n")
        pdf.ln(5)

    pdf_filename = f"tweets_backup_{username}.pdf"
    pdf_path = os.path.join("/tmp", pdf_filename)
    pdf.output(pdf_path)

    return send_file(pdf_path, as_attachment=True, download_name=pdf_filename)
