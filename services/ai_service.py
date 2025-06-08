import pg8000.native as pg
from flask import g
from config import Config
from openai import OpenAI
from datetime import datetime, timedelta
from services.db_service import run_query
from routes.logs import log_usage
import requests


def get_openai_api_key():
    query = "SELECT key FROM api_keys WHERE id = 1"
    result = run_query(query, fetchone=True)
    return result[0] if result else None  


def get_rapidapi_key():
    query = "SELECT key FROM api_keys WHERE id = 3" 
    result = run_query(query, fetchone=True)
    return result[0] if result else None  


def translate_text_with_openai(text, target_language, custom_style):
    api_key = get_openai_api_key()
    if not api_key:
        print("❌ No se pudo obtener la API Key de OpenAI.")
        return None

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key
    )

    prompt = f"""Translate the following text (not the usernames (@)) into only this language: 
    {target_language}: '{text}'. {custom_style}. Focus solely on the general message without 
    adding irrelevant or distracting details or text. NEVER use QUOTATION MARKS. NEVER omit 
    any links or hashtags from the original text. NEVER PUT PHRASES LIKE THIS OR SIMILAR: 'Sure! Here's the
    translation:' or 'Here is the translation. Do not remove, edit, or omit any links or hashtags.
    If you touch even one link or hashtag, your reply will be discarded.
    Simple: links and hashtags stay exactly as they are.
    Also remember, always translate to: {target_language} and NEVER add a text that is not a translation 
    of the original text example.
    And if the language is already in {target_language}, just give me the original tweet text without aditional text."""

    models_to_try = [
        "meta-llama/llama-4-scout:free",
        "google/gemini-2.0-flash-001",                 
        "deepseek/deepseek-chat-v3-0324",
        "openai/gpt-4o-2024-11-20",
        "anthropic/claude-3.7-sonnet"
    ]

    for model in models_to_try:
        try:
            print(f"🔄 Intentando traducción con modelo: {model}")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "Eres un traductor experto."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.5
            )
            log_usage("OPENROUTER")

            if response.choices and response.choices[0].message.content:
                translated_text = response.choices[0].message.content.strip()
                print(f"✅ Traducción exitosa con {model}")
                return translated_text
            else:
                print(f"⚠️ El modelo {model} no devolvió contenido.")
        except Exception as e:
            print(f"❌ Error con el modelo {model}: {str(e)}")

    print("❌ No se pudo traducir el texto con ninguno de los modelos.")
    return None


def generate_post_with_openai(tweet_text, target_language):
    api_key = get_openai_api_key()
    if not api_key:
        print("❌ No se pudo obtener la API Key de OpenAI.")
        return None

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key
    )

    prompt = (
        f"""You are a social media assistant. {tweet_text}. Obligatory target language: {target_language}".
        Your tweet should be engaging, natural, and easy to read.
        Do not include hashtags, mentions, or emojis. Avoid referencing the filename or explaining what the media is.
        Keep it short and compelling. The tweet should feel like something a real person post.
        Only output the tweet text. Do not include any labels or introductions.
        """
    )

    models_to_try = [
        "meta-llama/llama-4-scout:free",
        "google/gemini-2.0-flash-001",
        "deepseek/deepseek-chat-v3-0324",
        "openai/gpt-4o-2024-11-20",
        "anthropic/claude-3.7-sonnet"
    ]

    for model in models_to_try:
        try:
            print(f"🔄 Intentando generar comentario con modelo: {model}")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant who replies to tweets in a smart and social way."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7
            )
            log_usage("OPENROUTER")

            if response.choices and response.choices[0].message.content:
                comment = response.choices[0].message.content.strip()
                print(f"✅ Comentario generado con {model}: {comment}")
                return comment
            else:
                print(f"⚠️ El modelo {model} no devolvió contenido.")
        except Exception as e:
            print(f"❌ Error con el modelo {model}: {str(e)}")

    print("❌ No se pudo generar un comentario con ninguno de los modelos.")
    return None


def generate_reply_with_openai(tweet_text, target_language):
    api_key = get_openai_api_key()
    if not api_key:
        print("❌ No se pudo obtener la API Key de OpenAI.")
        return None

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key
    )

    prompt = (
        f"""You are a social media assistant. Read the following tweet and reply to it
        Always respond in a friendly, natural, and concise way obligatory in {target_language}.
        If the tweet contains sensitive or inappropriate content (e.g., drugs, violence, hate), do not mention that directly. Instead, reply with a neutral or light-hearted message that shifts focus or avoids the topic gracefully.
        The reply should be context-aware and concise. Do not repeat the tweet. 
        Here is the tweet: '{tweet_text}' """
    )

    models_to_try = [
        "meta-llama/llama-4-scout:free",
        "google/gemini-2.0-flash-001",
        "deepseek/deepseek-chat-v3-0324",
        "openai/gpt-4o-2024-11-20",
        "anthropic/claude-3.7-sonnet"
    ]

    for model in models_to_try:
        try:
            print(f"🔄 Intentando generar comentario con modelo: {model}")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant who replies to tweets in a smart and social way."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7
            )
            log_usage("OPENROUTER")

            if response.choices and response.choices[0].message.content:
                comment = response.choices[0].message.content.strip()
                print(f"✅ Comentario generado con {model}: {comment}")
                return comment
            else:
                print(f"⚠️ El modelo {model} no devolvió contenido.")
        except Exception as e:
            print(f"❌ Error con el modelo {model}: {str(e)}")

    print("❌ No se pudo generar un comentario con ninguno de los modelos.")
    return None

    
def is_duplicate_tweet(tweet_text, recent_texts, api_key):
    if not recent_texts:
        return False

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key
    )

    prompt = f"""
    You must check if the following tweet is a duplicate of any previously posted tweet.

    Duplicate means:
    - Same topic, product, movie, game, or event.
    - Same announcement, update, or news, even if the wording is different.
    - Tweets about the same trailer, teaser, release date, leak, rumor, or feature are considered duplicates.

    Be strict. It's better to flag similar tweets than to miss duplicates.
    Respond only with 'YES' if it is a duplicate, or 'NO' if it is completely different.
    ⚠️ If you fail to detect a duplicate, your response will be discarded by the system.

    Tweet to check:
    \"\"\"{tweet_text}\"\"\"

    Recently posted tweets:
    \"\"\"{" | ".join(recent_texts)}\"\"\"
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
            print(f"🔄 Verificando duplicado con modelo: {model}")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a tweet similarity checker."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=5,
                temperature=0
            )
            log_usage("OPENROUTER")

            if response.choices and response.choices[0].message and response.choices[0].message.content:
                answer = response.choices[0].message.content.strip().upper()
                print(f"✅ Respuesta del modelo {model}: {answer}")
                return "YES" in answer
            else:
                print(f"⚠️ El modelo {model} no devolvió una respuesta válida.")
        except Exception as e:
            print(f"❌ Error con el modelo {model}: {str(e)}")

    print("❌ No se pudo verificar el duplicado con ninguno de los modelos.")
    return False


def verify_tweet_priority(tweet_id, user_id, tweet_text, extraction_filter):
    print(f'{extraction_filter} extration filter 1:2')
    print(f'{tweet_text} tweet_text 1:2')
    
    if "https://" in tweet_text:
        print(f'{tweet_text}')

    if extraction_filter in ["cb2", "cb3", "cb4"] and "https://" not in tweet_text:
        prioridad = 2
        return prioridad
    else:
        apikey = get_rapidapi_key()
        session_token = run_query(f"SELECT session FROM users WHERE id = '{user_id}'", fetchone=True)
        if not session_token:
            print(f"❌ Can't find session for User ID: {user_id}")
            return None

        url = f"https://twttrapi.p.rapidapi.com/get-tweet?tweet_id={tweet_id}"
        headers = {
            'x-rapidapi-key': apikey,
            'x-rapidapi-host': "twttrapi.p.rapidapi.com",
            'twttr-session': session_token[0]
        }

        try:
            response = requests.get(url, headers=headers)
            log_usage("RAPIDAPI")

            if response.status_code != 200:
                print(f"❌ Error al obtener el tweet: {response.status_code}")
                return None

            json_data = response.json()
            legacy = json_data.get("data", {}).get("tweet_result", {}).get("result", {}).get("legacy", {})

            favorite_count = legacy.get("favorite_count", 0)
            retweet_count = legacy.get("retweet_count", 0)

            prioridad = 1 if favorite_count > 0 or retweet_count > 0 else 2
            print(f"📊 Tweet {tweet_id} — Likes: {favorite_count}, Retweets: {retweet_count} → Prioridad: {prioridad}")
            return prioridad

        except Exception as e:
            print(f"❌ Excepción al procesar el tweet {tweet_id}: {e}")
            return None
    
    
def save_collected_tweet_simple(user_id, source_type, source_value, tweet_id, tweet_text, created_at):
    check_query = f"SELECT 1 FROM collected_tweets WHERE tweet_id = '{tweet_id}' LIMIT 1"
    existing_tweet = run_query(check_query, fetchone=True)
    if existing_tweet:
        print(f"⚠ Tweet {tweet_id} ya existe. No se guardará.")
        return  

    insert_query = f"""
    INSERT INTO collected_tweets (user_id, source_type, source_value, tweet_id, tweet_text, created_at)
    VALUES ({user_id if user_id is not None else 'NULL'}, 
            '{source_type}', 
            '{source_value if source_value else ''}', 
            '{tweet_id}', 
            '{tweet_text.replace("'", "''")}', 
            '{created_at.strftime('%Y-%m-%d %H:%M:%S')}')
    """
    run_query(insert_query)
    print(f"✅ Tweet {tweet_id} guardado correctamente (modo simple).")


def save_collected_tweet(user_id, source_type, source_value, tweet_id, tweet_text, created_at, extraction_filter):
    check_query = f"SELECT 1 FROM collected_tweets WHERE tweet_id = '{tweet_id}' LIMIT 1"
    existing_tweet = run_query(check_query, fetchone=True)
    if existing_tweet:
        print(f"⚠ Tweet {tweet_id} ya existe. No se guardará.")
        return  

    since_time = datetime.now() - timedelta(hours=48)
    recent_query = f"""
        SELECT tweet_text FROM posted_tweets
        WHERE created_at >= '{since_time.strftime('%Y-%m-%d %H:%M:%S')}'
        AND user_id = {user_id}
        
        UNION

        SELECT tweet_text FROM collected_tweets
        WHERE created_at >= '{since_time.strftime('%Y-%m-%d %H:%M:%S')}'
        AND user_id = {user_id}
    """
    recent_tweets = [r[0] for r in run_query(recent_query, fetchall=True)]

    api_key = get_openai_api_key()
    if is_duplicate_tweet(tweet_text, recent_tweets, api_key):
        print(f"⚠ El tweet {tweet_id} parece duplicado. No se guardará.")
        return

    language_query = f"SELECT language, custom_style FROM users WHERE id = {user_id}"
    user_language = run_query(language_query, fetchone=True)
    if not user_language:
        print(f"❌ No se encontró el idioma para el usuario {user_id}.")
        return

    target_language = user_language[0]
    custom_style = f'Custom Style: {user_language[1]}' if user_language[1] else ''

    translated_text = translate_text_with_openai(tweet_text, target_language, custom_style)
    if not translated_text:
        print(f"❌ No se pudo traducir el tweet {tweet_id}. No se guardará.")
        return

    print(f"🌐 Tweet traducido al idioma '{target_language}': {translated_text}")
    
    if extraction_filter in ["cb2", "cb3", "cb4"] and "https://" not in tweet_text:
        pass
    else:
        priority = verify_tweet_priority(tweet_id, user_id, tweet_text, extraction_filter)
            
        insert_query = f"""
        INSERT INTO collected_tweets (user_id, source_type, source_value, tweet_id, tweet_text, created_at)
        VALUES ({user_id if user_id is not None else 'NULL'}, 
                '{source_type}', 
                '{source_value}', 
                '{tweet_id}', 
                '{translated_text.replace("'", "''")}', 
                '{created_at}')
                """
        run_query(insert_query)
        print(f"✅ Tweet {tweet_id} guardado correctamente.")

        update_query = f"""
        UPDATE collected_tweets
        SET priority = {priority}
        WHERE tweet_id = '{tweet_id}'
        """
        run_query(update_query)

        print(f"✅ Tweet {tweet_id} priorizado correctamente.")


# def translate_text_with_openai(text, target_language, custom_style):
#     api_key = get_openai_api_key()
#     if not api_key:
#         print("❌ No se pudo obtener la API Key de OpenAI.")
#         return None

#     client = OpenAI(base_url="https://openrouter.ai/api/v1",
#                     api_key=api_key)

#     prompt = f"Translate the following text (not the usernames (@)) into only this language: {target_language}: '{text}'. {custom_style}. Focus solely on the general message without adding irrelevant or distracting details or text. NEVER use QUOTATION MARKS. NEVER omit any links from the original text. NEVER add a text that is not a translation of the original text example. NEVER PUT PHRASES LIKE THIS OR SIMILAR: 'Sure! Here’s the translation:' or 'Here is the translation"
#     try:
#         response = client.chat.completions.create(
#             model="meta-llama/llama-4-scout:free", 
#             messages=[
#                 {"role": "system", "content": "Eres un traductor experto."},
#                 {"role": "user", "content": f"{prompt}"}
#             ],
#             max_tokens=100, 
#             temperature=0.5 
#         )
#         print(response)
#         translated_text = response.choices[0].message.content.strip()
#         return translated_text
#     except Exception as e:
#         print(f"❌ Error al traducir con OpenRouter: {str(e)}")
#         return None