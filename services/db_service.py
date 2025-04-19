import pg8000.native as pg
from flask import g
from config import Config
from openai import OpenAI
from datetime import datetime, timedelta


def get_openai_api_key():
    query = "SELECT key FROM api_keys WHERE id = 1"
    result = run_query(query, fetchone=True)
    return result[0] if result else None  

def get_db():
    if 'db' not in g:
        g.db = pg.Connection(
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            host=Config.DB_HOST,
            port=int(Config.DB_PORT),
            database=Config.DB_NAME
        )
    return g.db


def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def run_query(query, params=None, fetchone=False, fetchall=False):
    db = get_db()

    if params is None:
        params = ()
    
    try:
        result = db.run(query, params)

        if fetchone:
            return result[0] if result else None
        if fetchall:
            return result

        return None
    except Exception as e:
        print(f"❌ Error en consulta SQL: {str(e)}")
        return None


def log_event(user_id, event_type, description):
    query = f"""
    INSERT INTO logs (user_id, event_type, event_description)
    VALUES ('{user_id}', '{event_type}', '{description}')
    """
    run_query(query)


def translate_text_with_openai(text, target_language, custom_style):
    api_key = get_openai_api_key()
    if not api_key:
        print("❌ No se pudo obtener la API Key de OpenAI.")
        return None

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key
    )

    prompt = f"Translate the following text (not the usernames (@)) into only this language: {target_language}: '{text}'. {custom_style}. Focus solely on the general message without adding irrelevant or distracting details or text. NEVER use QUOTATION MARKS. NEVER omit any links from the original text. NEVER add a text that is not a translation of the original text example. NEVER PUT PHRASES LIKE THIS OR SIMILAR: 'Sure! Here’s the translation:' or 'Here is the translation"

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
                max_tokens=100,
                temperature=0.5
            )

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

    
def is_duplicate_tweet(tweet_text, recent_texts, api_key):
    if not recent_texts:
        return False

    client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)

    prompt = f"""
    Check if the following tweet is a duplicate or conveys the same message as any of the previously posted tweets, even if phrased differently. Respond with only 'YES' or 'NO'.

    Tweet to check:
    \"\"\"{tweet_text}\"\"\"

    Recently posted tweets:
    \"\"\"{" | ".join(recent_texts)}\"\"\"
    """

    try:
        response = client.chat.completions.create(
            model="meta-llama/llama-4-scout:free",
            messages=[
                {"role": "system", "content": "You are a tweet similarity checker."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=5,
            temperature=0
        )

        answer = response.choices[0].message.content.strip().upper()
        return "YES" in answer
    except Exception as e:
        print(f"❌ Error al verificar duplicado con IA: {str(e)}")
        return False

    
def save_collected_tweet(user_id, source_type, source_value, tweet_id, tweet_text, created_at):
    check_query = f"SELECT 1 FROM collected_tweets WHERE tweet_id = '{tweet_id}' LIMIT 1"
    existing_tweet = run_query(check_query, fetchone=True)
    if existing_tweet:
        print(f"⚠ Tweet {tweet_id} ya existe. No se guardará.")
        return  

    since_time = datetime.now() - timedelta(hours=24)
    recent_query = f"""
        SELECT tweet_text FROM posted_tweets
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