import asyncio
import aiohttp
from services.db_service import run_query, log_event
from services.ai_service import save_collected_tweet, generate_reply_with_openai, generate_post_with_openai, save_collected_tweet_simple
from datetime import datetime, timezone
from services.post_tweets import post_tweet
import time
from routes.logs import log_usage
import itertools
import re
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import json
import tempfile

SOCIALDATA_API_URL = "https://api.socialdata.tools/twitter/search"
TWEET_LIMIT_PER_HOUR = 50

def _i(x, default=0):
    try:
        return int(x)
    except (TypeError, ValueError):
        return default

def bump_job_progress(job_id: int, delta: int, note: str = None):
    safe_note = note.replace("'", "''") if note else None
    note_sql = f", note = '{safe_note}'" if safe_note is not None else ""
    # Suma al contador y devuelve total y máximo
    row = run_query(f"""
        UPDATE custom_extract_jobs
        SET extracted_count = COALESCE(extracted_count, 0) + {int(delta)},
            updated_at = NOW()
            {note_sql}
        WHERE id = {int(job_id)}
        RETURNING extracted_count, max_items
    """, fetchone=True)
    if not row:
        return {"extracted_count": 0, "max_items": 0}
    return {"extracted_count": row[0], "max_items": row[1]}

def mark_job_running(job_id):
    run_query(f"""
      UPDATE custom_extract_jobs
      SET status = 'running', updated_at = NOW()
      WHERE id = {job_id}
    """)

def schedule_job_retry(job_id: int, next_run_in_minutes: int, retries: int, note: str = None):
    safe_note = note.replace("'", "''") if note else None
    note_sql = f", note = '{safe_note}'" if safe_note is not None else ""
    run_query(f"""
        UPDATE custom_extract_jobs
        SET status = 'pending',
            retries = {int(retries)},
            next_run_at = (NOW() AT TIME ZONE 'UTC') + INTERVAL '{int(next_run_in_minutes)} minutes',
            updated_at = NOW()
            {note_sql}
        WHERE id = {int(job_id)}
    """)

def finish_job(job_id: int, note: str = None):
    safe_note = note.replace("'", "''") if note else None
    note_sql = f", note = '{safe_note}'" if safe_note is not None else ""
    run_query(f"""
        UPDATE custom_extract_jobs
        SET status = 'done',
            next_run_at = NULL,
            updated_at = NOW()
            {note_sql}
        WHERE id = {int(job_id)}
    """)

def get_active_custom_job(user_id):
    q = f"""
    SELECT id, date_from, date_to, max_items, scope, status, retries, max_retries, next_run_at,
           COALESCE(extracted_count, 0)
    FROM custom_extract_jobs
    WHERE user_id = {int(user_id)}
      AND status IN ('pending','running')
      AND (next_run_at IS NULL OR next_run_at <= (NOW() AT TIME ZONE 'UTC'))
    ORDER BY created_at DESC
    LIMIT 1
    """
    row = run_query(q, fetchone=True)
    if not row:
        return None
    return {
        "id": _i(row[0]),
        "date_from": row[1],
        "date_to": row[2],
        "max_items": _i(row[3], 0),
        "scope": row[4] or "users_keywords",
        "status": row[5],
        "retries": _i(row[6], 0),
        "max_retries": _i(row[7], 24),
        "next_run_at": row[8],
        "extracted_count": _i(row[9], 0),
    }

def _to_unix_ts(dt_val):
    # Acepta datetime o string ISO yyyy-mm-dd HH:MM:SS
    if isinstance(dt_val, datetime):
        if dt_val.tzinfo is None:
            dt_val = dt_val.replace(tzinfo=timezone.utc)
        return int(dt_val.timestamp())
    if isinstance(dt_val, str):
        # Permite 'YYYY-MM-DD' o 'YYYY-MM-DD HH:MM:SS'
        try:
            if len(dt_val.strip()) == 10:
                dt_obj = datetime.strptime(dt_val, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            else:
                dt_obj = datetime.strptime(dt_val, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return int(dt_obj.timestamp())
        except Exception:
            # Último intento, ISO flexible
            return int(datetime.fromisoformat(dt_val).replace(tzinfo=timezone.utc).timestamp())
    raise ValueError("Formato de fecha no soportado")


def get_pending_custom_extract_job(user_id):
    q = f"""
    SELECT id, date_from, date_to, max_items, scope
    FROM custom_extract_jobs
    WHERE user_id = {user_id} AND status = 'pending'
    ORDER BY created_at DESC
    LIMIT 1
    """
    row = run_query(q, fetchone=True)
    if not row:
        return None
    return {
        "id": row[0],
        "date_from": row[1],
        "date_to": row[2],
        "max_items": row[3],
        "scope": row[4] or "users_keywords",
    }


def mark_custom_job_status(job_id, status, extracted_count=0, note=None):
    # Escapamos comillas simples para SQL
    safe_note = note.replace("'", "''") if note else None
    note_sql = f", note = '{safe_note}'" if safe_note is not None else ""

    q = f"""
    UPDATE custom_extract_jobs
    SET status = '{status}',
        extracted_count = {extracted_count},
        updated_at = NOW(){note_sql}
    WHERE id = {job_id}
    """
    run_query(q)


def extract_folder_id(url):
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    return match.group(1) if match else None


def extract_base_name(filename):
    return re.sub(r'_\d+(?=\.\w+$)', '', filename)


def get_socialdata_api_key():
    query = "SELECT key FROM api_keys WHERE id = 2"  
    result = run_query(query, fetchone=True)
    return result[0] if result else None 


def get_extraction_filter(user_id):
    query = f"SELECT extraction_filter FROM users WHERE id = {user_id}"  
    result = run_query(query, fetchone=True)
    return result[0] if result else None 


def get_rapidapi_key():
    query = "SELECT key FROM api_keys WHERE id = 3" 
    result = run_query(query, fetchone=True)
    return result[0] if result else None  


async def get_tweet_limit_per_hour(user_id):
    result = run_query(f"SELECT rate_limit FROM users WHERE id = {user_id}", fetchone=True)
    return _i(result[0], 10) if result else 10


def get_extraction_method(user_id):
    query = f"SELECT extraction_method FROM users WHERE id = {user_id}"
    result = run_query(query, fetchone=True)
    return result[0] if result else 1


def get_like_limit_per_hour(user_id):
    result = run_query(f"SELECT likes_limit FROM users WHERE id = {user_id}", fetchone=True)
    return _i(result[0], 1) if result else 1


def get_comment_limit_per_hour(user_id):
    result = run_query(f"SELECT comments_limit FROM users WHERE id = {user_id}", fetchone=True)
    return _i(result[0], 1) if result else 1


def get_follow_limit_per_hour(user_id):
    result = run_query(f"SELECT follows_limit FROM users WHERE id = {user_id}", fetchone=True)
    return _i(result[0], 1) if result else 1


def get_retweet_limit_per_hour(user_id):
    result = run_query(f"SELECT retweets_limit FROM users WHERE id = {user_id}", fetchone=True)
    return _i(result[0], 1) if result else 1


async def count_tweets_for_user(user_id):
    query = f"""
    SELECT COUNT(*) FROM posted_tweets 
    WHERE user_id = {user_id}
    AND created_at >= NOW() - INTERVAL '1 hour'
    """
    result = run_query(query, fetchone=True)
    return result[0] if result else 0


async def count_tweets_for_user2(user_id):
    query = f"""
    SELECT COUNT(*) FROM collected_tweets 
    WHERE user_id = {user_id}
    AND created_at >= NOW() - INTERVAL '1 hour'
    """
    result = run_query(query, fetchone=True)
    return result[0] if result else 0


async def extract_by_combination(session, user_id, monitored_users, keywords, limit, fetching_event):
    since_timestamp = int(time.time()) - 4 * 60 * 60
    collected_count = 0

    rapidapi_key = get_rapidapi_key()
    if not rapidapi_key:
        print("❌ No se pudo obtener la API Key de RapidAPI.")
        return 0

    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "twitter-api45.p.rapidapi.com",
    }

    combinaciones = list(itertools.product(monitored_users, keywords))

    for username, keyword in combinaciones:
        if fetching_event.is_set():
            print("⏹️ Proceso detenido mientras recorría combinaciones.")
            return collected_count

        if collected_count >= limit:
            return collected_count

        base = f"from:{username} ({keyword}) since_time:{since_timestamp}"
        extraction_filter = get_extraction_filter(user_id)

        query = f"({base})"
        if extraction_filter == "cb2":
            query = f"({base} filter:images)"
        elif extraction_filter == "cb3":
            query = f"({base} filter:native_video)"
        elif extraction_filter == "cb4":
            query = f"({base} filter:media)"
        elif extraction_filter == "cb5":
            query = f"({base} filter:images -filter:videos)"
        elif extraction_filter == "cb6":
            query = f"({base} -filter:images -filter:videos -filter:links)"

        params = {"query": query, "search_type": "Latest"}
        print(f"🔎 Consultando: {query}")

        try:
            async with session.get("https://twitter-api45.p.rapidapi.com/search.php", headers=headers, params=params) as response:
                if response.status != 200:
                    print(f"❌ Error al buscar tweets ({response.status}) para: {query}")
                    log_usage("RAPIDAPI", count=1)
                    continue

                try:
                    data = await response.json()
                except Exception as e:
                    print(f"❌ Error parseando respuesta para {query}: {e}")
                    log_usage("RAPIDAPI", count=1)
                    continue

                timeline = data.get("timeline", [])
                log_usage("RAPIDAPI", count=len(timeline))
                if not timeline:
                    continue

                for tweet in timeline:
                    if fetching_event.is_set() or collected_count >= limit:
                        return collected_count

                    tweet_id = tweet.get("tweet_id")
                    tweet_text = tweet.get("text")
                    created_at = tweet.get("created_at")

                    save_collected_tweet(user_id, "combined", None, tweet_id, tweet_text, created_at, extraction_filter)
                    collected_count += 1
                    await asyncio.sleep(0.1)

        except Exception as e:
            print(f"❌ Error durante la consulta a RapidAPI: {e}")
            continue

    return collected_count


async def extract_by_copy_user(session, user_id, monitored_users, limit, fetching_event):
    since_timestamp = int(time.time()) - 4 * 60 * 60
    collected_count = 0

    rapidapi_key = get_rapidapi_key()
    if not rapidapi_key:
        print("❌ No se pudo obtener la API Key de RapidAPI.")
        return 0

    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "twitter-api45.p.rapidapi.com",
    }

    extraction_filter = get_extraction_filter(user_id)

    for username in monitored_users:
        if fetching_event.is_set():
            print("⏹️ Proceso detenido mientras recorría usuarios.")
            return collected_count

        if collected_count >= limit:
            print(f"✅ Límite alcanzado: {collected_count}/{limit}")
            return collected_count

        base = f"from:{username} since_time:{since_timestamp}"

        if extraction_filter == "cb2":
            query = f"({base} filter:images)"
        elif extraction_filter == "cb3":
            query = f"({base} filter:native_video)"
        elif extraction_filter == "cb4":
            query = f"({base} filter:media)"
        elif extraction_filter == "cb5":
            query = f"({base} filter:images -filter:videos)"
        elif extraction_filter == "cb6":
            query = f"({base} -filter:images -filter:videos -filter:links)"
        else:
            query = f"({base})"

        params = {"query": query, "search_type": "Latest"}
        print(f"🔎 Consultando: {query}")

        try:
            async with session.get("https://twitter-api45.p.rapidapi.com/search.php", headers=headers, params=params) as response:
                if response.status != 200:
                    print(f"❌ Error al buscar tweets ({response.status}) para @{username}")
                    log_usage("RAPIDAPI", count=1)
                    continue

                try:
                    data = await response.json()
                except Exception as e:
                    print(f"❌ Error parseando respuesta para @{username}: {e}")
                    continue

                tweets = data.get("timeline", [])
                log_usage("RAPIDAPI", count=len(tweets))
                if not tweets:
                    print(f"⚠️ No se encontraron tweets para @{username}")
                    continue

                for tweet in tweets:
                    if fetching_event.is_set() or collected_count >= limit:
                        return collected_count

                    tweet_id = tweet.get("tweet_id")
                    tweet_text = tweet.get("text")
                    created_at = tweet.get("created_at")

                    save_collected_tweet(user_id, "full_account_copy", username, tweet_id, tweet_text, created_at, extraction_filter)
                    collected_count += 1
                    print(f"💾 Tweet guardado de @{username}: {tweet_id}")
                    await asyncio.sleep(0.1)

        except Exception as e:
            print(f"❌ Error durante la consulta para @{username}: {e}")
            continue

    print(f"🎯 Extracción completa. Total tweets: {collected_count}/{limit}")
    return collected_count


async def fetch_tweets_for_monitored_users_with_keywords(session, user_id, monitored_users, keywords, limit, fetching_event, extraction_method):
    try:
        if fetching_event.is_set():
            return

        print(f"🔍 Ejecutando extracción para método {extraction_method}")

        if extraction_method == 1:
            count = await extract_by_combination(session, user_id, monitored_users, keywords, limit, fetching_event)

        elif extraction_method == 2:
            count = await extract_by_copy_user(session, user_id, monitored_users, limit, fetching_event)

        elif extraction_method == 3:
            job = get_active_custom_job(user_id)
            if not job:
                print(f"⚠️ Usuario {user_id} sin job activo, pending o running habilitado por ventana o next_run_at.")
                return

            print(f"🧾 Método 3, job #{job['id']} status={job['status']} retries={job['retries']}/{job['max_retries']}")
            mark_job_running(job["id"])

            already = _i(job.get("extracted_count"), 0)
            target  = _i(job.get("max_items"), 2000)
            remaining = max(0, target - already)

            if remaining <= 0:
                finish_job(job["id"], note="objetivo alcanzado")
                return

            per_run_limit = min(_i(limit, remaining), remaining)
            extraction_filter = get_extraction_filter(user_id) or "cb1"

            count = await extract_custom_one_time(
                session, user_id, job, monitored_users, keywords,
                extraction_filter,  # <- ahora sí
                per_run_limit,      # <- NUEVO arg
                fetching_event
            )

            now_ts = int(time.time())
            date_to_ts = int(job["date_to"].replace(tzinfo=timezone.utc).timestamp()) if job["date_to"] else now_ts
            out_of_window = now_ts > date_to_ts

            prog = bump_job_progress(job["id"], _i(count), note=f"+{count} en esta corrida")
            new_remaining = max(0, _i(prog.get("max_items"), target) - _i(prog.get("extracted_count"), already))

            if new_remaining <= 0:
                finish_job(job["id"], note="objetivo alcanzado")
                print(f"🎯 Job #{job['id']} llegó al objetivo, total extraído {prog['extracted_count']}.")
            else:
                if out_of_window:
                    finish_job(job["id"], note="ventana vencida")
                elif _i(job["retries"]) + 1 >= _i(job["max_retries"], 24):
                    finish_job(job["id"], note="max_retries alcanzado")
                else:
                    schedule_job_retry(job["id"], next_run_in_minutes=60, retries=_i(job["retries"]) + 1,
                                       note=f"faltan {new_remaining}, reintento en 1h")
                    print(f"⏰ Job #{job['id']} reintentará en 1h, faltan {new_remaining}.")


        print(f"🎯 Finalizado. Total tweets extraídos: {count}/{limit}")

    except asyncio.CancelledError:
        print(f"⏹️ Tarea cancelada para usuario ID: {user_id}.")
        raise
    except Exception as e:
        log_event(user_id, "ERROR", f"Error obteniendo tweets: {str(e)}")
        print(f"❌ Error al buscar tweets: {e}")


async def fetch_tweets_for_single_user(user_id, fetching_event):
    print(f"🔍 Iniciando búsqueda de tweets para usuario ID: {user_id}...")

    if fetching_event.is_set():
        print(f"⏹️ Proceso detenido para usuario ID: {user_id}.")
        return

    query_users = f"SELECT DISTINCT twitter_username FROM monitored_users WHERE user_id = '{user_id}'"
    monitored_users = run_query(query_users, fetchall=True) or []

    query_keywords = f"SELECT DISTINCT keyword FROM user_keywords WHERE user_id = '{user_id}'"
    monitored_keywords = run_query(query_keywords, fetchall=True) or []
    extraction_method = get_extraction_method(user_id)

    if extraction_method != 3:
        if not monitored_users:
            print(f"⚠ Usuario {user_id} no tiene usuarios o palabras clave monitoreadas.")
            return

    limit_ph = await get_tweet_limit_per_hour(user_id)
    limit = round(limit_ph * 1.3)
    
    async with aiohttp.ClientSession() as session:
        await fetch_tweets_for_monitored_users_with_keywords(
            session,
            user_id,
            [user[0] for user in monitored_users],
            [keyword[0] for keyword in monitored_keywords],
            limit,
            fetching_event,
            extraction_method
        )

    print(f"✅ Búsqueda de tweets completada para usuario ID: {user_id}.")
    
    
async def fetch_tweets_for_all_users(fetching_event):
    print("🔍 Buscando tweets para cada usuario registrado (etapa 1)...")

    query = "SELECT DISTINCT id FROM users"
    users = run_query(query, fetchall=True)
    print(users)

    if not users:
        print("⚠ No hay usuarios registrados en la base de datos.")
        return
    
    users = [u for u in users if u and isinstance(u, (list, tuple)) and len(u) > 0 and u[0] is not None]

    tasks = []
    for user_id in users:
        if fetching_event.is_set():
            print("⏹️ Proceso detenido por solicitud de usuario.")
            return

        print(f"👤 Iniciando búsqueda de tweets para usuario ID: {user_id[0]}")
        task = asyncio.create_task(fetch_tweets_for_single_user(user_id[0], fetching_event))
        tasks.append(task)

        await asyncio.sleep(0.1)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("⏹️ Tareas canceladas por solicitud de detención.")

    print("✅ Búsqueda de tweets completada.")


async def fetch_random_tasks_for_all_users(fetching_event):
    print("🎲 Iniciando tareas aleatorias para cada usuario (etapa 1)...")

    query = "SELECT DISTINCT id FROM users"
    users = run_query(query, fetchall=True)

    if not users:
        print("⚠ No hay usuarios en la base de datos.")
        return
    
    users = [u for u in users if u and isinstance(u, (list, tuple)) and len(u) > 0 and u[0] is not None]

    tasks = []
    for user in users:
        if fetching_event.is_set():
            print("⏹️ Proceso detenido por solicitud de usuario.")
            return

        user_id = user[0]
        print(f"👤 Iniciando tareas aleatorias para usuario ID: {user_id}")
        task = asyncio.create_task(fetch_random_tasks_for_user(user_id, fetching_event))
        tasks.append(task)
        await asyncio.sleep(0.1)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("⏹️ Tareas canceladas por solicitud de detención.")

    print("✅ Tareas aleatorias completadas para todos los usuarios.")


async def fetch_random_tasks_for_user(user_id, fetching_event):
    if fetching_event.is_set():
        print(f"⏹️ Proceso detenido para usuario ID: {user_id}")
        return

    print(f"🎯 Ejecutando acciones aleatorias para usuario ID: {user_id}...")

    session_token = run_query(f"SELECT session FROM users WHERE id = '{user_id}'", fetchone=True)
    if not session_token:
        print(f"❌ No se encontró session para user ID {user_id}")
        return

    like_users = run_query(f"SELECT twitter_username FROM like_users WHERE user_id = '{user_id}'", fetchall=True)
    comment_users = run_query(f"SELECT twitter_username FROM comment_users WHERE user_id = '{user_id}'", fetchall=True)
    retweet_users = run_query(f"SELECT twitter_username FROM retweet_users WHERE user_id = '{user_id}'", fetchall=True)
    language = run_query(f"SELECT language FROM users WHERE id = '{user_id}'", fetchone=True)
    follow_targets = run_query(f"SELECT twitter_username FROM follow_users WHERE user_id = '{user_id}'", fetchall=True)

    async with aiohttp.ClientSession() as session:
        await run_random_actions(session, user_id, [u[0] for u in like_users], "like", get_like_limit_per_hour(user_id), session_token[0], fetching_event)
        await run_random_actions(session, user_id, [u[0] for u in retweet_users], "retweet", get_retweet_limit_per_hour(user_id), session_token[0], fetching_event)
        await run_random_actions(session, user_id, [u[0] for u in comment_users], "reply", get_comment_limit_per_hour(user_id), session_token[0], fetching_event, language)
        await run_random_actions(session, user_id, [u[0] for u in follow_targets], "follow", get_follow_limit_per_hour(user_id), session_token[0], fetching_event)

    print(f"✅ Acciones aleatorias finalizadas para usuario ID: {user_id}")


async def run_random_actions(session, user_id, usernames, action_type, limit, session_token, fetching_event, language=None):
    try:
        if not usernames:
            print(f"⚠️ Sin usuarios configurados para acción {action_type} en user ID {user_id}")
            return

        rapidkey = get_rapidapi_key()

        print(f"🎯 Ejecutando '{action_type}' para user ID {user_id}... (límite: {limit})")
        since_timestamp = int(time.time()) - 4 * 60 * 60
        count = 0
        
        if action_type == "follow":
            check_follows_last_hour = run_query(
                f"""
                SELECT COUNT(*) FROM random_actions
                WHERE user_id = '{user_id}' AND action_type = 'follow'
                AND created_at >= NOW() - INTERVAL '1 hour'
                """,
                fetchone=True
            )
            count = check_follows_last_hour[0] if check_follows_last_hour else 0

            if count >= limit:
                print(f"⛔ Usuario {user_id} ya alcanzó el límite de follows ({limit}) en la última hora.")
                return

            min_followers = 100

            for target_username in usernames:
                if fetching_event.is_set():
                    return

                url = f"https://twttrapi.p.rapidapi.com/user-followers?username={target_username}&count=20"
                headers_followers = {
                    'x-rapidapi-key': rapidkey,
                    'x-rapidapi-host': "twttrapi.p.rapidapi.com"
                }

                try:
                    async with session.get(url, headers=headers_followers) as resp:
                        if resp.status != 200:
                            print(f"❌ Error al obtener followers de @{target_username} ({resp.status})")
                            log_usage("RAPIDAPI")
                            continue

                        data = await resp.json()
                        followers = []
                        instructions = data.get("data", {}).get("user", {}).get("timeline_response", {}).get("timeline", {}).get("instructions", [])

                        for instruction in instructions:
                            if instruction.get("__typename") == "TimelineAddEntries":
                                for entry in instruction.get("entries", []):
                                    try:
                                        user_result = entry["content"]["content"]["userResult"]["result"]
                                        followers.append(user_result)
                                    except KeyError:
                                        continue
                        
                        log_usage("RAPIDAPI", count=len(followers))
                        
                        for user in followers:
                            if fetching_event.is_set():
                                return

                            if count >= limit:
                                print(f"✅ Límite alcanzado ({limit}) para acción 'follow'")
                                return

                            legacy = user.get("legacy", {})
                            verified = user.get("is_blue_verified", False)
                            followers_count = legacy.get("followers_count", 0)
                            username_to_follow = legacy.get("screen_name") or user.get("screen_name")
                            print(f'{username_to_follow} {followers_count} {verified}')
                            if not username_to_follow or not verified or followers_count < min_followers:
                                continue

                            already_followed = run_query(
                                f"SELECT 1 FROM random_actions WHERE twitter_id = '{username_to_follow}' AND action_type = 'follow'",
                                fetchone=True
                            )
                            if already_followed:
                                continue

                            url_follow = "https://twttrapi.p.rapidapi.com/follow-user"
                            payload = f"username={username_to_follow}"
                            headers_follow = {
                                'x-rapidapi-key': rapidkey,
                                'x-rapidapi-host': "twttrapi.p.rapidapi.com",
                                'Content-Type': "application/x-www-form-urlencoded",
                                'twttr-session': session_token
                            }

                            try:
                                async with session.post(url_follow, data=payload, headers=headers_follow) as follow_resp:
                                    log_usage("RAPIDAPI")
                                    if follow_resp.status == 200:
                                        print(f"✅ Seguido @{username_to_follow} ({followers_count} seguidores)")
                                        run_query(f"""
                                            INSERT INTO random_actions (user_id, twitter_id, action_type, created_at)
                                            VALUES ('{user_id}', '{username_to_follow}', 'follow', NOW())
                                        """)
                                        count += 1
                                    else:
                                        print(f"❌ Error al seguir @{username_to_follow} ({follow_resp.status})")
                            except Exception as e:
                                print(f"❌ Excepción al seguir a @{username_to_follow}: {e}")

                            await asyncio.sleep(0.2)

                except Exception as e:
                    print(f"❌ Error general al obtener followers de @{target_username}: {e}")

            return

        for username in usernames:
            if fetching_event.is_set():
                print(f"⏹️ Proceso detenido en acción '{action_type}' para user ID {user_id}")
                return

            if count >= limit:
                print(f"✅ Límite alcanzado ({limit}) para acción '{action_type}'")
                return
            

            query = f"from:{username} since_time:{since_timestamp}"
            params = {"query": query, "search_type": "Latest"}
            headers_rapid = {
                "x-rapidapi-key": rapidkey,
                "x-rapidapi-host": "twitter-api45.p.rapidapi.com",
            }
            search_url = "https://twitter-api45.p.rapidapi.com/search.php"

            async with session.get(search_url, headers=headers_rapid, params=params) as response:
                if response.status != 200:
                    print(f"❌ Error al buscar tweets para {username} ({response.status})")
                    log_usage("RAPIDAPI", count=1) 
                    continue

                try:
                    data = await response.json()
                    tweets = data.get("timeline", [])
                    log_usage("RAPIDAPI", count=len(tweets))
                    if not tweets:
                        print(f"⚠️ No se encontraron tweets para {username}")
                        continue
                except Exception as e:
                    print(f"❌ Error parseando respuesta de RapidApi Search: {e}")
                    log_usage("RAPIDAPI", count=1) 
                    continue

                for tweet in tweets[:10]:
                    if fetching_event.is_set():
                        print(f"⏹️ Proceso detenido mientras se procesaban acciones.")
                        return

                    if count >= limit:
                        print(f"✅ Límite alcanzado ({limit}) para acción '{action_type}'")
                        return

                    tweet_id = tweet.get("tweet_id")
                    tweet_text = tweet.get("text", "")

                    check_query = f"SELECT 1 FROM random_actions WHERE twitter_id = '{tweet_id}'"
                    already_done = run_query(check_query, fetchone=True)
                    if already_done:
                        print(f"⏭️ Acción ya realizada anteriormente sobre tweet {tweet_id[:8]}... Buscando otro.")
                        continue

                    headers_rapid = {
                        'x-rapidapi-key': rapidkey,
                        'x-rapidapi-host': "twttrapi.p.rapidapi.com",
                        'Content-Type': "application/x-www-form-urlencoded",
                        'twttr-session': session_token
                    }

                    if action_type == "like":
                        url = "https://twttrapi.p.rapidapi.com/favorite-tweet"
                        payload = f"tweet_id={tweet_id}"

                    elif action_type == "retweet":
                        url = "https://twttrapi.p.rapidapi.com/retweet-tweet"
                        payload = f"tweet_id={tweet_id}"

                    elif action_type == "reply":
                        if not language:
                            print(f"⚠️ Idioma no definido para user ID {user_id}, se omite reply.")
                            continue

                        generated_comment = generate_reply_with_openai(tweet_text, language)
                        if not generated_comment:
                            print(f"⚠️ No se pudo generar comentario para tweet {tweet_id}")
                            continue

                        url = "https://twttrapi.p.rapidapi.com/create-tweet"
                        payload = f"tweet_text={generated_comment}&in_reply_to_tweet_id={tweet_id}"

                    else:
                        print(f"❌ Acción desconocida: {action_type}")
                        continue

                    try:
                        async with session.post(url, data=payload, headers=headers_rapid) as resp:
                            log_usage("RAPIDAPI")
                            if resp.status == 200:
                                print(f"✅ Acción '{action_type}' realizada sobre tweet {tweet_id[:8]}... {tweet_text[:30]}")
                                count += 1
                                insert_query = f"""
                                    INSERT INTO random_actions (user_id, twitter_id, action_type, created_at)
                                    VALUES ('{user_id}', '{tweet_id}', '{action_type}', NOW())
                                """
                                run_query(insert_query)
                            else:
                                print(f"❌ Error al hacer {action_type} ({resp.status})")
                    except Exception as e:
                        print(f"❌ Excepción en acción {action_type}: {e}")

                    await asyncio.sleep(0.1)

        print(f"🎯 Finalizado '{action_type}' con {count}/{limit} acciones")
    except Exception as e:
        print(f"❌ Error {e}") 

def auto_post_tweet():
    user_id = 1 
    tweet_text = "¡Este es un tweet de prueba!"

    response, status_code = post_tweet(user_id, tweet_text)

    if status_code == 200:
        print("✅ Tweet automático publicado exitosamente.")
    else:
        print(f"❌ Error al publicar el tweet automático: {response.get('error')}")


async def post_tweets_for_all_users(posting_event):
    print("🚀 Iniciando publicación de tweets para cada usuario registrado...")

    query = "SELECT DISTINCT id FROM users"
    users = run_query(query, fetchall=True)
    print(users)

    if not users:
        print("⚠ No hay usuarios registrados en la base de datos.")
        return
    
    users = [u for u in users if u and isinstance(u, (list, tuple)) and len(u) > 0 and u[0] is not None]

    tasks = []
    for user_id in users:
        if posting_event.is_set():
            print("⏹️ Proceso detenido por solicitud de usuario.")
            return

        print(f"📢 Iniciando publicación de tweets para usuario ID: {user_id[0]}")
        task = asyncio.create_task(post_tweets_for_single_user(user_id[0], posting_event))
        tasks.append(task)

        await asyncio.sleep(0.1)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("⏹️ Tareas de publicación canceladas por solicitud de detención.")

    print("✅ Publicación de tweets completada.")


async def post_tweets_for_single_user(user_id, posting_event):
    print(f"📢 Iniciando publicación de tweets para usuario ID: {user_id}...")

    if posting_event.is_set():
        print(f"⏹️ Proceso detenido para usuario ID: {user_id}.")
        return

    method_row = run_query(f"SELECT extraction_method FROM users WHERE id = '{user_id}'", fetchone=True)
    method = method_row[0] if method_row else 1

    tweet_limit = await get_tweet_limit_per_hour(user_id)
    tweets_posted_last_hour = await count_tweets_for_user(user_id)

    if tweets_posted_last_hour >= tweet_limit:
        print(f"⛔ Usuario {user_id} alcanzó el límite de {tweet_limit} tweets por hora. Saltando publicación.")
        return

    if method == 3:
        has_items = run_query(f"""
            SELECT 1
            FROM collected_tweets
            WHERE user_id = '{user_id}' AND priority = 1 AND source = 'custom_one_time'
            LIMIT 1
        """, fetchone=True)
        if not has_items:
            print(f"⚠ Usuario {user_id} en método 3, sin items del job, salto publicación.")
            return

    # REEMPLAZAR este SELECT por el siguiente
    query_tweets = f"""
        SELECT tweet_id, tweet_text
        FROM collected_tweets
        WHERE user_id = '{user_id}' AND priority = 1
    """
    if method == 3:
        # NUEVO, publicar solo lo que vino del custom job
        query_tweets += " AND source = 'custom_one_time'"

    tweets_to_post = run_query(query_tweets, fetchall=True) or []

    if not tweets_to_post:
        print(f"⚠ Usuario {user_id} no tiene tweets pendientes de publicación.")
        return

    async with aiohttp.ClientSession() as session:
        for tweet_id, tweet_text in tweets_to_post:
            if posting_event.is_set():
                print(f"⏹️ Proceso detenido mientras se publicaban tweets.")
                break

            tweets_posted_last_hour = await count_tweets_for_user(user_id)
            if tweets_posted_last_hour >= tweet_limit:
                print(f"⛔ Usuario {user_id} alcanzó el límite mientras publicaba. Deteniendo la publicación.")
                break
            
            media_rows = run_query(f"""
                SELECT file_url FROM collected_media
                WHERE user_id = '{user_id}' AND tweet_id = '{tweet_id}'
            """, fetchall=True)
            media_urls = [row[0] for row in media_rows] if media_rows else []

            extraction_filter = get_extraction_filter(user_id)
            if extraction_filter in ["cb2", "cb3", "cb4"] and "https://" not in tweet_text:
                print(f"❌ No se publico el tweet.")
            else:
                response, status_code = post_tweet(user_id, tweet_text, media_urls=media_urls)

                if status_code == 200:
                    insert_query = f"INSERT INTO posted_tweets (user_id, tweet_text, created_at) VALUES ('{user_id}', '{tweet_text}', NOW())"
                    run_query(insert_query)

                    print(f"✅ Tweet publicado y guardado en posted_tweets: {tweet_text[:50]}...")

                    delete_query = f"DELETE FROM collected_tweets WHERE tweet_id = '{tweet_id}' AND user_id = '{user_id}'"
                    run_query(delete_query)
                    delete_query2 = f"DELETE FROM collected_media WHERE tweet_id = '{tweet_id}' AND user_id = '{user_id}'"
                    run_query(delete_query2)

                    print(f"🗑️ Tweet eliminado de collected_tweets después de ser publicado: {tweet_text[:50]}...")

                    tweets_posted_last_hour += 1 

                else:
                    print(f"❌ No se pudo publicar el tweet: {response.get('error')}")

            await asyncio.sleep(0.1)

    print(f"✅ Publicación de tweets completada para usuario ID: {user_id}.")


async def post_tweets_for_user(session, user_id, tweets, posting_event, tweet_limit, tweets_posted_last_hour):
    try:
        if posting_event.is_set():
            print(f"⏹️ Proceso detenido para usuario ID: {user_id}.")
            return

        print(f"📢 Publicando tweets para usuario ID: {user_id}...")

        for tweet_id, tweet_text in tweets:
            if posting_event.is_set():
                print(f"⏹️ Proceso detenido mientras se publicaban tweets.")
                break

            if tweets_posted_last_hour >= tweet_limit:
                print(f"⛔ Usuario {user_id} alcanzó el límite mientras publicaba. Deteniendo la publicación.")
                break

            check_query = f"SELECT 1 FROM posted_tweets WHERE user_id = '{user_id}' AND tweet_text = '{tweet_text}' LIMIT 1"
            exists = run_query(check_query, fetchone=True)
            
            if exists:
                print(f"⚠ El tweet ya fue publicado previamente. Saltando: {tweet_text[:50]}...")
                continue

            response, status_code = post_tweet(user_id, tweet_text)

            if status_code == 200:
                insert_query = f"INSERT INTO posted_tweets (user_id, tweet_text, created_at) VALUES ('{user_id}', '{tweet_text}', NOW())"
                run_query(insert_query)
                print(f"✅ Tweet guardado en posted_tweets: {tweet_text[:50]}...")
                
                delete_query = f"DELETE FROM collected_tweets WHERE tweet_id = '{tweet_id}' AND user_id = '{user_id}'"
                run_query(delete_query)
                print(f"🗑️ Tweet eliminado de collected_tweets después de ser publicado: {tweet_text[:50]}...")
                
                tweets_posted_last_hour += 1 
            else:
                print(f"❌ No se pudo publicar el tweet: {response.get('error')}")

            await asyncio.sleep(0.1)

    except asyncio.CancelledError:
        print(f"⏹️ Publicación de tweets cancelada para usuario ID: {user_id}.")

    print(f"✅ Publicación de tweets finalizada para usuario ID: {user_id}.")


# async def start_tweet_fetcher():
#     print('🚀 Iniciando el servicio de recolección de tweets...')
#     while True:
#         await fetch_tweets_for_all_users()
    
#         print("⏳ Esperando 5 minutos antes de la próxima búsqueda...")
#         await asyncio.sleep(300)  



# OLD 


''' async def extract_by_combination(session, user_id, monitored_users, keywords, limit, fetching_event):
    since_timestamp = int(time.time()) - 4 * 60 * 60
    collected_count = 0

    socialdata_api_key = get_socialdata_api_key()
    if not socialdata_api_key:
        print("❌ No se pudo obtener la API Key de SocialData.")
        return 0

    headers = {"Authorization": f"Bearer {socialdata_api_key}"}
    combinaciones = list(itertools.product(monitored_users, keywords))

    for username, keyword in combinaciones:
        if fetching_event.is_set():
            print(f"⏹️ Proceso detenido mientras recorría combinaciones.")
            return collected_count

        if collected_count >= limit:
            return collected_count

        base = f"from:{username} ({keyword}) since_time:{since_timestamp}"
        extraction_filter = get_extraction_filter(user_id)

        query = f"({base})"
        if extraction_filter == "cb2":
            query = f"({base} filter:images)"
        elif extraction_filter == "cb3":
            query = f"({base} filter:native_video)"
        elif extraction_filter == "cb4":
            query = f"({base} filter:media)"
        elif extraction_filter == "cb5":
            query = f"({base} filter:images -filter:videos)"
        elif extraction_filter == "cb6":
            query = f"({base} -filter:images -filter:videos -filter:links)"

        params = {"query": query, "type": "Latest"}
        print(f"🔎 Consultando: {query}")

        async with session.get(SOCIALDATA_API_URL, headers=headers, params=params) as response:
            if response.status != 200:
                print(f"❌ Error al buscar tweets ({response.status}) para: {query}")
                log_usage("SOCIALDATA", count=1)
                continue

            try:
                data = await response.json()
            except Exception as e:
                print(f"❌ Error parseando respuesta para {query}: {e}")
                log_usage("SOCIALDATA", count=1)
                continue

            tweets = data.get("tweets", [])
            log_usage("SOCIALDATA", count=len(tweets))
            if not tweets:
                continue

            for tweet in tweets:
                if fetching_event.is_set() or collected_count >= limit:
                    return collected_count

                tweet_id = tweet["id_str"]
                tweet_text = tweet["full_text"]
                created_at = tweet["tweet_created_at"]
                
                # if extraction_filter in ["cb2", "cb3", "cb4"] and "https://" not in tweet_text:
                #     continue 

                save_collected_tweet(user_id, "combined", None, tweet_id, tweet_text, created_at, extraction_filter)
                collected_count += 1
                await asyncio.sleep(0.1)

    return collected_count

    
async def extract_by_copy_user(session, user_id, monitored_users, limit, fetching_event):
    since_timestamp = int(time.time()) - 4 * 60 * 60
    collected_count = 0

    socialdata_api_key = get_socialdata_api_key()
    if not socialdata_api_key:
        print("❌ No se pudo obtener la API Key de SocialData.")
        return 0

    headers = {"Authorization": f"Bearer {socialdata_api_key}"}
    extraction_filter = get_extraction_filter(user_id)

    for username in monitored_users:
        if fetching_event.is_set():
            print(f"⏹️ Proceso detenido mientras recorría usuarios.")
            return collected_count

        if collected_count >= limit:
            print(f"✅ Límite alcanzado: {collected_count}/{limit}")
            return collected_count

        base = f"from:{username} since_time:{since_timestamp}"

        if extraction_filter == "cb2":
            query = f"({base} filter:images)"
        elif extraction_filter == "cb3":
            query = f"({base} filter:native_video)"
        elif extraction_filter == "cb4":
            query = f"({base} filter:media)"
        elif extraction_filter == "cb5":
            query = f"({base} filter:images -filter:videos)"
        elif extraction_filter == "cb6":
            query = f"({base} -filter:images -filter:videos -filter:links)"
        else:
            query = f"({base})" 

        params = {"query": query, "type": "Latest"}
        print(f"🔎 Consultando: {query}")

        async with session.get(SOCIALDATA_API_URL, headers=headers, params=params) as response:
            if response.status != 200:
                print(f"❌ Error al buscar tweets ({response.status}) para @{username}")
                log_usage("SOCIALDATA", count=1)
                continue

            try:
                data = await response.json()
            except Exception as e:
                print(f"❌ Error parseando respuesta para @{username}: {e}")
                continue

            tweets = data.get("tweets", [])
            log_usage("SOCIALDATA", count=len(tweets))
            if not tweets:
                print(f"⚠️ No se encontraron tweets para @{username}")
                continue

            for tweet in tweets:
                if fetching_event.is_set() or collected_count >= limit:
                    return collected_count

                tweet_id = tweet["id_str"]
                tweet_text = tweet["full_text"]
                created_at = tweet["tweet_created_at"]

                # if extraction_filter in ["cb2", "cb3", "cb4"] and "https://" not in tweet_text:
                #     continue 

                save_collected_tweet(user_id, "full_account_copy", username, tweet_id, tweet_text, created_at, extraction_filter)
                collected_count += 1
                print(f"💾 Tweet guardado de @{username}: {tweet_id}")
                await asyncio.sleep(0.1)

    print(f"🎯 Extracción completa. Total tweets: {collected_count}/{limit}")
    return collected_count
'''


async def old_fetch_tweets_for_monitored_users_with_keywords(session, user_id, monitored_users, keywords, limit, fetching_event):
    since_timestamp = int(time.time()) - 4 * 60 * 60
    collected_count = 0

    try:
        if fetching_event.is_set():
            print(f"⏹️ Proceso detenido para usuario ID: {user_id}.")
            return

        print(f"🔍 Buscando tweets para usuario ID: {user_id} con cada keyword por usuario (una sola vez)...")

        socialdata_api_key = get_socialdata_api_key()
        if not socialdata_api_key:
            print("❌ No se pudo obtener la API Key de SocialData.")
            return

        headers = {"Authorization": f"Bearer {socialdata_api_key}"}

        combinaciones = list(itertools.product(monitored_users, keywords))

        print(f"🔢 Total de combinaciones a consultar: {len(combinaciones)}")

        for username, keyword in combinaciones:
            if fetching_event.is_set():
                print(f"⏹️ Proceso detenido mientras recorría combinaciones.")
                return

            if collected_count >= limit:
                print(f"✅ Límite de {limit} tweets alcanzado.")
                return

            query = f"(from:{username} ({keyword}) filter:media since_time:{since_timestamp})"
            params = {"query": query, "type": "Latest"}

            print(f"🔎 Consultando: {query}")

            async with session.get(SOCIALDATA_API_URL, headers=headers, params=params) as response:
                if response.status != 200:
                    print(f"❌ Error al buscar tweets ({response.status}) para: {query}")
                    continue

                try:
                    data = await response.json()
                except Exception as e:
                    print(f"❌ Error parseando respuesta para {query}: {e}")
                    continue

                tweets = data.get("tweets", [])

                if not tweets:
                    print(f"⚠️ No se encontraron tweets para {username} con keyword '{keyword}'.")
                    continue

                for tweet in tweets:
                    if fetching_event.is_set():
                        print(f"⏹️ Proceso detenido mientras se procesaban tweets.")
                        return

                    if collected_count >= limit:
                        print(f"✅ Límite de {limit} tweets alcanzado.")
                        return

                    tweet_id = tweet["id_str"]
                    tweet_text = tweet["full_text"]
                    created_at = tweet["tweet_created_at"]

                    print(f"✅ Nuevo tweet encontrado: {tweet_text[:50]}...")
                    save_collected_tweet(user_id, "combined", None, tweet_id, tweet_text, created_at)
                    print(f"💾 Tweet guardado en la base de datos: {tweet_id}")
                    collected_count += 1

                    await asyncio.sleep(0.1)

        print(f"🎯 Finalizado. Total tweets: {collected_count}/{limit}")

    except asyncio.CancelledError:
        print(f"⏹️ Tarea cancelada para usuario ID: {user_id}.")
        raise 
    except Exception as e:
        log_event(user_id, "ERROR", f"Error obteniendo tweets: {str(e)}")
        print(f"❌ Error al buscar tweets: {e}")


async def old_fetch_tweets_for_single_user(user_id, fetching_event):
    print(f"🔍 Iniciando búsqueda de tweets para usuario ID: {user_id}...")

    if fetching_event.is_set():
        print(f"⏹️ Proceso detenido para usuario ID: {user_id}.")
        return

    query_users = f"SELECT DISTINCT twitter_username FROM monitored_users WHERE user_id = '{user_id}'"
    monitored_users = run_query(query_users, fetchall=True) or []

    query_keywords = f"SELECT DISTINCT keyword FROM user_keywords WHERE user_id = '{user_id}'"
    monitored_keywords = run_query(query_keywords, fetchall=True) or []

    if not monitored_users or not monitored_keywords:
        print(f"⚠ Usuario {user_id} no tiene usuarios o palabras clave monitoreadas.")
        return

    limit_ph = await get_tweet_limit_per_hour(user_id)
    limit = round(limit_ph * 1.3)
    
    async with aiohttp.ClientSession() as session:
        await old_fetch_tweets_for_monitored_users_with_keywords(
            session,
            user_id,
            [user[0] for user in monitored_users],
            [keyword[0] for keyword in monitored_keywords],
            limit,
            fetching_event
        )

    print(f"✅ Búsqueda de tweets completada para usuario ID: {user_id}.")


async def old_fetch_tweets_for_all_users(fetching_event):
    print("🔍 Buscando tweets para cada usuario registrado (etapa 1)...")

    query = "SELECT DISTINCT id FROM users"
    users = run_query(query, fetchall=True)
    print(users)

    if not users:
        print("⚠ No hay usuarios registrados en la base de datos.")
        return

    tasks = []
    for user_id in users:
        if fetching_event.is_set():
            print("⏹️ Proceso detenido por solicitud de usuario.")
            return

        print(f"👤 Iniciando búsqueda de tweets para usuario ID: {user_id[0]}")
        task = asyncio.create_task(old_fetch_tweets_for_single_user(user_id[0], fetching_event))
        tasks.append(task)

        await asyncio.sleep(0.1)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("⏹️ Tareas canceladas por solicitud de detención.")

    print("✅ Búsqueda de tweets completada.")


def _quote_kw(kw: str) -> str:
    kw = (kw or "").strip()
    if not kw:
        return ""
    return f'"{kw}"' if " " in kw and not kw.startswith('"') else kw


def _lang_code_for_user(user_id: int):
    row = run_query(f"SELECT language FROM users WHERE id = '{user_id}'", fetchone=True)
    mapping = {"English": "en", "Spanish": "es", "Español": "es", "Portuguese": "pt", "French": "fr"}
    return mapping.get(row[0]) if row and row[0] else None


async def extract_custom_one_time(session, user_id, job, monitored_users, keywords, extraction_filter, fetching_event):
    """
    Extrae por rango de fechas y envía todo a 'To-Be-Posted', en tu caso collected_tweets con priority = 1.
    """
    rapidapi_key = get_rapidapi_key()
    if not rapidapi_key:
        print("❌ No se pudo obtener la API Key de RapidAPI.")
        return 0

    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "twitter-api45.p.rapidapi.com",
    }

    since_ts = _to_unix_ts(job["date_from"])
    until_ts = _to_unix_ts(job["date_to"])
    hard_cap = job["max_items"] or 2000
    if hard_cap <= 0:
        return 0

    extraction_filter = extraction_filter or get_extraction_filter(user_id) or "cb1"
    lang_code = _lang_code_for_user(user_id)

    def apply_filter(base):
        if extraction_filter == "cb2":
            return f"({base} filter:images)"
        if extraction_filter == "cb3":
            return f"({base} filter:native_video)"
        if extraction_filter == "cb4":
            return f"({base} filter:media)"
        if extraction_filter == "cb5":
            return f"({base} filter:images -filter:videos)"
        if extraction_filter == "cb6":
            return f"({base} -filter:images -filter:videos -filter:links)"
        return f"({base})"

    collected = 0
    queries = set()  # dedupe

    scope = job.get("scope") or "users_keywords"
    kw_list = keywords if keywords else [""]

    if scope == "keywords_only":
        for kw in kw_list:
            kwx = _quote_kw(kw)
            base = f"{kwx} since_time:{since_ts} until_time:{until_ts}"
            if lang_code:
                base += f" lang:{lang_code}"
            queries.add(apply_filter(base.strip()))
    else:
        users = monitored_users or []
        if not users:
            print("⚠️ Scope users_keywords sin usuarios monitoreados.")
            return 0
        for u in users:
            for kw in kw_list:
                kwx = _quote_kw(kw)
                base = f"from:{u} {kwx} since_time:{since_ts} until_time:{until_ts}"
                if lang_code:
                    base += f" lang:{lang_code}"
                queries.add(apply_filter(base.strip()))

    seen = set()

    for q in queries:
        if fetching_event.is_set():
            print("⏹️ Proceso detenido por evento externo.")
            break
        if collected >= hard_cap:
            break

        params = {"query": q, "search_type": "Latest"}
        print(f"🔎 Custom One-Time Extract, consultando: {q}")

        try:
            # intento Latest
            async with session.get("https://twitter-api45.p.rapidapi.com/search.php", headers=headers, params=params) as resp:
                if resp.status != 200:
                    print(f"❌ Error {resp.status} en búsqueda para: {q}")
                    log_usage("RAPIDAPI", count=1)
                    continue
                data = await resp.json()
                timeline = data.get("timeline", [])
                log_usage("RAPIDAPI", count=len(timeline))

            # fallback Top si no hay resultados
            if not timeline:
                params["search_type"] = "Top"
                async with session.get("https://twitter-api45.p.rapidapi.com/search.php", headers=headers, params=params) as resp2:
                    if resp2.status == 200:
                        data2 = await resp2.json()
                        timeline = data2.get("timeline", [])
                        log_usage("RAPIDAPI", count=len(timeline))
                    else:
                        log_usage("RAPIDAPI", count=1)

            if not timeline:
                continue

            for t in timeline:
                if fetching_event.is_set():
                    print("⏹️ Detenido durante el guardado.")
                    break
                if collected >= hard_cap:
                    break

                tweet_id = t.get("tweet_id")
                if not tweet_id or tweet_id in seen:
                    continue
                seen.add(tweet_id)

                tweet_text = t.get("text") or ""
                created_at = t.get("created_at")

                exists = run_query(
                    f"SELECT 1 FROM collected_tweets WHERE user_id = '{user_id}' AND tweet_id = '{tweet_id}' LIMIT 1",
                    fetchone=True
                )
                if exists:
                    continue

                save_collected_tweet(user_id, "custom_one_time", None, tweet_id, tweet_text, created_at, extraction_filter)
                run_query(f"UPDATE collected_tweets SET priority = 1 WHERE user_id = '{user_id}' AND tweet_id = '{tweet_id}'")
                collected += 1

                await asyncio.sleep(0.05)

        except Exception as e:
            print(f"❌ Error en custom extract: {e}")
            continue

    return collected
