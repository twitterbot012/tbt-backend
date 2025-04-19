import requests
from services.db_service import run_query, log_event
import logging

logging.basicConfig(level=logging.INFO)

def get_rapidapi_key():
    query = "SELECT key FROM api_keys WHERE id = 3" 
    result = run_query(query, fetchone=True)
    return result[0] if result else None  


def post_tweet(user_id, tweet_text):
    query = f"SELECT session FROM users WHERE id = {user_id}"
    result = run_query(query, fetchone=True)

    if not result:
        error_message = f"❌ Usuario {user_id} no encontrado en la base de datos."
        logging.error(error_message)
        log_event(user_id, "ERROR", error_message)
        return {"error": "Usuario no encontrado"}, 404

    session = result[0]
    rapidapi_key = get_rapidapi_key()

    if not rapidapi_key:
        error_message = "❌ No se pudo obtener la API Key de RapidAPI."
        logging.error(error_message)
        log_event(user_id, "ERROR", error_message)
        return {"error": "No se pudo obtener la API Key de RapidAPI"}, 500

    if isinstance(tweet_text, list):
        tweet_text = " ".join(tweet_text)
    tweet_text = str(tweet_text).replace("'", "''") 

    try:
        insert_query = f"""
            INSERT INTO posted_tweets (user_id, tweet_text, created_at)
            VALUES ({user_id}, '{tweet_text}', NOW())
            RETURNING id
        """
        result = run_query(insert_query, fetchone=True)
        if not result:
            raise Exception("No se obtuvo el ID del tweet insertado.")
        internal_id = result[0]
    except Exception as db_error:
        error_message = f"❌ No se pudo guardar el tweet en la base de datos: {db_error}"
        logging.error(error_message)
        log_event(user_id, "ERROR", error_message)
        return {"error": "No se pudo guardar el tweet en la base de datos"}, 500

    url = "https://twttrapi.p.rapidapi.com/create-tweet"
    payload = f"tweet_text={tweet_text}"
    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "twttrapi.p.rapidapi.com",
        "Content-Type": "application/x-www-form-urlencoded",
        "twttr-session": session
    }

    try:
        response = requests.post(url, data=payload, headers=headers)

        if response.status_code == 200 and "data" in response.json():
            tweet_data = response.json()["data"]["create_tweet"]["tweet_result"]["result"]
            tweet_id = tweet_data["rest_id"]
            tweet_text_final = tweet_data["legacy"]["full_text"]
            tweet_url = f"https://twitter.com/{tweet_data['core']['user_result']['result']['legacy']['screen_name']}/status/{tweet_id}"

            try:
                update_query = f"""
                    UPDATE posted_tweets SET tweet_id = '{tweet_id}'
                    WHERE id = {internal_id}
                """
                run_query(update_query)
            except Exception as update_error:
                error_message = f"⚠️ Se publicó en Twitter, pero no se pudo guardar el tweet_id: {update_error}"
                logging.error(error_message)
                log_event(user_id, "ERROR", error_message)
                return {"error": "Tweet publicado, pero no se guardó el tweet_id"}, 500

            success_message = f"✅ Tweet publicado y actualizado en la base: {tweet_text_final[:50]}..."
            logging.info(success_message)
            log_event(user_id, "POST", success_message)

            return {
                "message": "Tweet publicado exitosamente",
                "tweet_id": tweet_id,
                "tweet_text": tweet_text_final,
                "tweet_url": tweet_url
            }, 200

        else:
            error_data = response.json()
            error_message = error_data.get("detail", "Error desconocido")
            full_error_message = f"❌ Error al publicar el tweet: {error_data}"
            logging.error(full_error_message)
            log_event(user_id, "ERROR", full_error_message)
            return {"error": error_message}, response.status_code

    except Exception as e:
        error_message = f"❌ Error inesperado al publicar el tweet: {str(e)}"
        logging.error(error_message)
        log_event(user_id, "ERROR", error_message)
        return {"error": str(e)}, 500