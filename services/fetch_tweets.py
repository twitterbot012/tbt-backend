import asyncio
import aiohttp
from services.db_service import run_query, save_collected_tweet, log_event
from config import Config
from datetime import datetime, timezone
from services.post_tweets import post_tweet

SOCIALDATA_API_URL = "https://api.socialdata.tools/twitter/search"
TWEET_LIMIT_PER_HOUR = 10

def get_socialdata_api_key():
    query = "SELECT key FROM api_keys WHERE id = 2"  
    result = run_query(query, fetchone=True)
    return result[0] if result else None 

async def get_tweet_limit_per_hour(user_id):
    query = f"SELECT rate_limit FROM users WHERE id = {user_id}"
    result = run_query(query, fetchone=True)
    return result[0] if result else 10 

async def count_tweets_for_user(user_id):
    query = f"""
    SELECT COUNT(*) FROM collected_tweets 
    WHERE user_id = {user_id}
    AND created_at >= NOW() - INTERVAL '1 hour'
    """
    result = run_query(query, fetchone=True)
    return result[0] if result else 0

async def fetch_tweets_for_user(session, user_id, username, limit, fetching_event):
    """
    Función asíncrona para buscar tweets de un usuario monitoreado.
    Se detiene si el evento fetching_event está activado.
    """
    # Verificar si el proceso debe detenerse
    if fetching_event.is_set():
        print(f"⏹️ Proceso detenido para usuario monitoreado: {username}.")
        return

    # Contar tweets recolectados hoy
    tweets_collected_today = await count_tweets_for_user(user_id)
    if tweets_collected_today >= TWEET_LIMIT_PER_HOUR:
        print(f"⛔ Usuario {user_id} alcanzó el límite de {TWEET_LIMIT_PER_HOUR} tweets hoy. Saltando usuario {username}.")
        return

    print(f"📡 Buscando tweets de usuario monitoreado: {username}")
    headers = {"Authorization": f"Bearer {Config.SOCIALDATA_API_KEY}"}
    params = {"query": f"from:{username}", "type": "Latest"}

    try:
        async with session.get(SOCIALDATA_API_URL, headers=headers, params=params) as response:
            data = await response.json()
            tweets = data.get("tweets", [])[:limit]

            for tweet in tweets:
                # Verificar nuevamente si el proceso debe detenerse
                if fetching_event.is_set():
                    print(f"⏹️ Proceso detenido mientras se procesaban tweets de {username}.")
                    break

                # Contar tweets recolectados hoy
                tweets_collected_today = await count_tweets_for_user(user_id)
                if tweets_collected_today >= TWEET_LIMIT_PER_HOUR:
                    print(f"⛔ Usuario {user_id} alcanzó el límite mientras recolectaba. Deteniendo usuario {username}.")
                    break

                # Extraer datos del tweet
                tweet_id = tweet["id_str"]
                tweet_text = tweet["full_text"]
                created_at = tweet["tweet_created_at"]

                print(f"✅ Nuevo tweet de {username}: {tweet_text[:50]}...")
                save_collected_tweet(user_id, "username", username, tweet_id, tweet_text, created_at)
                print(f"💾 Tweet guardado en la base de datos: {tweet_id}")

                # # Publicar el tweet
                # response, status_code = post_tweet(user_id, tweet_text)
                # if status_code == 200:
                #     # Si el tweet fue publicado exitosamente, eliminarlo de collected_tweets
                #     delete_query = f"DELETE FROM collected_tweets WHERE tweet_id = '{tweet_id}' AND user_id = '{user_id}'"
                #     run_query(delete_query)
                #     print(f"🗑️ Tweet {tweet_id} eliminado de la base de datos después de ser publicado.")
                # else:
                #     print(f"❌ No se pudo publicar el tweet de {username}: {response.get('error')}")

                # Pequeña pausa para evitar sobrecargar el sistema (opcional)
                await asyncio.sleep(0.1)

    except Exception as e:
        log_event(user_id, "ERROR", f"Error obteniendo tweets de {username}: {str(e)}")
        print(f"❌ Error con {username}: {e}")
        
async def fetch_tweets_for_keyword(session, user_id, keyword, limit, fetching_event):
    if fetching_event.is_set():
        print(f"⏹️ Proceso detenido para keyword: {keyword}.")
        return

    tweets_collected_today = await count_tweets_for_user(user_id)
    if tweets_collected_today >= TWEET_LIMIT_PER_HOUR:
        print(f"⛔ Usuario {user_id} alcanzó el límite de {TWEET_LIMIT_PER_HOUR} tweets hoy. Saltando keyword {keyword}.")
        return

    print(f"🔍 Buscando tweets con keyword: {keyword}")
    headers = {"Authorization": f"Bearer {Config.SOCIALDATA_API_KEY}"}
    params = {"query": keyword, "type": "Latest"}

    try:
        async with session.get(SOCIALDATA_API_URL, headers=headers, params=params) as response:
            data = await response.json()
            tweets = data.get("tweets", [])[:limit]

            for tweet in tweets:
                if fetching_event.is_set():
                    print(f"⏹️ Proceso detenido mientras se procesaban tweets con keyword: {keyword}.")
                    break

                tweets_collected_today = await count_tweets_for_user(user_id)
                if tweets_collected_today >= TWEET_LIMIT_PER_HOUR:
                    print(f"⛔ Usuario {user_id} alcanzó el límite mientras recolectaba. Deteniendo keyword {keyword}.")
                    break

                tweet_id = tweet["id_str"]
                tweet_text = tweet["full_text"]
                created_at = tweet["tweet_created_at"]

                print(f"✅ Nuevo tweet con keyword '{keyword}': {tweet_text[:50]}...")
                save_collected_tweet(user_id, "keyword", keyword, tweet_id, tweet_text, created_at)
                print(f"💾 Tweet guardado en la base de datos: {tweet_id}")

                # response, status_code = post_tweet(user_id, tweet_text)
                # if status_code == 201:
                #     print(f"🗑️ Tweet {tweet_id} publicado.")
                # else:
                #     print(f"❌ No se pudo publicar el tweet con keyword '{keyword}': {response.get('error')}")

                await asyncio.sleep(0.1)

    except Exception as e:
        log_event(user_id, "ERROR", f"Error obteniendo tweets con la keyword '{keyword}': {str(e)}")
        print(f"❌ Error con la keyword '{keyword}': {e}")


async def fetch_tweets_for_monitored_users_with_keywords(session, user_id, monitored_users, keywords, limit, fetching_event):
    try:
        if fetching_event.is_set():
            print(f"⏹️ Proceso detenido para usuario ID: {user_id}.")
            return
        
        TWEET_LIMIT_PER_HOUR = await get_tweet_limit_per_hour(user_id)

        tweets_collected_today = await count_tweets_for_user(user_id)
        if tweets_collected_today >= TWEET_LIMIT_PER_HOUR:
            print(f"⛔ Usuario {user_id} alcanzó el límite de {TWEET_LIMIT_PER_HOUR} tweets. Saltando completamente la búsqueda.")
            return

        print(f"🔍 Buscando tweets para usuario ID: {user_id} con palabras clave específicas...")

        query_parts = []
        for username in monitored_users:
            keyword_query = " OR ".join(keywords) 
            query_parts.append(f"(from:{username} ({keyword_query}))")
        full_query = " OR ".join(query_parts) 
        
        socialdata_api_key = get_socialdata_api_key()
        if not socialdata_api_key:
            print("❌ No se pudo obtener la API Key de SocialData.")
            return

        headers = {"Authorization": f"Bearer {socialdata_api_key}"}

        # headers = {"Authorization": f"Bearer {Config.SOCIALDATA_API_KEY}"}
        params = {"query": full_query, "type": "Latest"}

        async with session.get(SOCIALDATA_API_URL, headers=headers, params=params) as response:
            data = await response.json()
            tweets = data.get("tweets", [])[:limit]

            for tweet in tweets:
                if fetching_event.is_set():
                    print(f"⏹️ Proceso detenido mientras se procesaban tweets.")
                    break

                tweets_collected_today = await count_tweets_for_user(user_id)
                if tweets_collected_today >= TWEET_LIMIT_PER_HOUR:
                    print(f"⛔ Usuario {user_id} alcanzó el límite mientras recolectaba. Deteniendo la búsqueda.")
                    break

                tweet_id = tweet["id_str"]
                tweet_text = tweet["full_text"]
                created_at = tweet["tweet_created_at"]

                print(f"✅ Nuevo tweet encontrado: {tweet_text[:50]}...")
                save_collected_tweet(user_id, "combined", None, tweet_id, tweet_text, created_at)
                print(f"💾 Tweet guardado en la base de datos: {tweet_id}")

                # query = f"""
                # SELECT tweet_text FROM collected_tweets WHERE tweet_id = '{tweet_id}' AND user_id = '{user_id}'                
                # """
                # result = run_query(query, fetchone=True)

                # if result != 'None':
                #     response, status_code = post_tweet(user_id, result)
                
                # if status_code == 200:
                #     delete_query = f"DELETE FROM collected_tweets WHERE tweet_id = '{tweet_id}' AND user_id = '{user_id}'"
                #     run_query(delete_query)
                #     print(f"🗑️ Tweet {tweet_id} eliminado de la base de datos después de ser publicado.")
                # else:
                #     print(f"❌ No se pudo publicar el tweet {result}: {response.get('error')}")

                await asyncio.sleep(0.1)

    except asyncio.CancelledError:
        print(f"⏹️ Tarea cancelada para usuario ID: {user_id}.")
        raise 
    except Exception as e:
        log_event(user_id, "ERROR", f"Error obteniendo tweets: {str(e)}")
        print(f"❌ Error al buscar tweets: {e}")
        
# async def fetch_tweets_for_single_user(user_id, fetching_event):
#     """
#     Función asíncrona para buscar tweets para un solo usuario.
#     Se detiene si el evento fetching_event está activado.
#     """
#     print(f"🔍 Iniciando búsqueda de tweets para usuario ID: {user_id}...")

#     # Verificar si el proceso debe detenerse
#     if fetching_event.is_set():
#         print(f"⏹️ Proceso detenido para usuario ID: {user_id}.")
#         return

#     # Contar tweets recolectados hoy
#     tweets_collected_today = await count_tweets_for_user(user_id)
#     print(tweets_collected_today)

#     if tweets_collected_today >= TWEET_LIMIT_PER_HOUR:
#         print(f"⛔ Usuario {user_id} alcanzó el límite de {TWEET_LIMIT_PER_HOUR} tweets hoy. Saltando completamente la búsqueda.")
#         return

#     # Crear una sesión HTTP
#     async with aiohttp.ClientSession() as session:
#         # Consultar usuarios monitoreados
#         query_users = f"SELECT DISTINCT twitter_username FROM monitored_users WHERE user_id = '{user_id}'"
#         monitored_users = run_query(query_users, fetchall=True) or []
#         print(monitored_users)

#         # Consultar keywords monitoreadas
#         query_keywords = f"SELECT DISTINCT keyword FROM user_keywords WHERE user_id = '{user_id}'"
#         monitored_keywords = run_query(query_keywords, fetchall=True) or []
#         print(monitored_keywords)

#         # Si no hay usuarios ni keywords monitoreadas, salir
#         if not monitored_users and not monitored_keywords:
#             print(f"⚠ Usuario {user_id} no tiene usuarios o keywords monitoreadas.")
#             return

#         # Calcular límites
#         user_limit = 11 if len(monitored_users) > 3 else TWEET_LIMIT_PER_HOUR
#         keyword_limit = 11 if len(monitored_keywords) > 3 else TWEET_LIMIT_PER_HOUR

#         # Crear tareas para buscar tweets
#         user_tasks = [
#             fetch_tweets_for_user(session, user_id, username[0], user_limit, fetching_event)
#             for username in monitored_users
#         ]
#         keyword_tasks = [
#             fetch_tweets_for_keyword(session, user_id, keyword[0], keyword_limit, fetching_event)
#             for keyword in monitored_keywords
#         ]

#         # Ejecutar todas las tareas
#         try:
#             await asyncio.gather(*user_tasks, *keyword_tasks)
#         except asyncio.CancelledError:
#             print(f"⏹️ Tareas canceladas para usuario ID: {user_id}.")

#     print(f"✅ Búsqueda de tweets completada para usuario ID: {user_id}.")


async def fetch_tweets_for_single_user(user_id, fetching_event):
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

    limit = 11 if len(monitored_users) > 3 else TWEET_LIMIT_PER_HOUR

    async with aiohttp.ClientSession() as session:
        await fetch_tweets_for_monitored_users_with_keywords(
            session,
            user_id,
            [user[0] for user in monitored_users],
            [keyword[0] for keyword in monitored_keywords],
            limit,
            fetching_event
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

def auto_post_tweet():
    """
    Publica un tweet automáticamente para un usuario específico.
    """
    user_id = 1 
    tweet_text = "¡Este es un tweet de prueba!"

    # Llamar a la función post_tweet
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

    # Verificar límite de tweets por hora
    tweet_limit = await get_tweet_limit_per_hour(user_id)
    tweets_posted_last_hour = await count_tweets_for_user(user_id)

    if tweets_posted_last_hour >= tweet_limit:
        print(f"⛔ Usuario {user_id} alcanzó el límite de {tweet_limit} tweets por hora. Saltando publicación.")
        return

    query_tweets = f"SELECT tweet_id, tweet_text FROM collected_tweets WHERE user_id = '{user_id}'"
    tweets_to_post = run_query(query_tweets, fetchall=True) or []

    if not tweets_to_post:
        print(f"⚠ Usuario {user_id} no tiene tweets pendientes de publicación.")
        return

    async with aiohttp.ClientSession() as session:
        await post_tweets_for_user(session, user_id, tweets_to_post, posting_event, tweet_limit, tweets_posted_last_hour)

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

            # Verificar si el tweet ya fue publicado
            check_query = f"SELECT 1 FROM posted_tweets WHERE user_id = '{user_id}' AND tweet_text = '{tweet_text}' LIMIT 1"
            exists = run_query(check_query, fetchone=True)
            
            if exists:
                print(f"⚠ El tweet ya fue publicado previamente. Saltando: {tweet_text[:50]}...")
                continue

            response, status_code = post_tweet(user_id, tweet_text)

            if status_code == 200:
                # Guardar el tweet en posted_tweets
                insert_query = f"INSERT INTO posted_tweets (user_id, tweet_text, created_at) VALUES ('{user_id}', '{tweet_text}', NOW())"
                run_query(insert_query)
                print(f"✅ Tweet guardado en posted_tweets: {tweet_text[:50]}...")
                
                # Eliminar el tweet de collected_tweets
                delete_query = f"DELETE FROM collected_tweets WHERE tweet_id = '{tweet_id}' AND user_id = '{user_id}'"
                run_query(delete_query)
                print(f"🗑️ Tweet eliminado de collected_tweets después de ser publicado: {tweet_text[:50]}...")
                
                tweets_posted_last_hour += 1  # Incrementar contador de tweets publicados
            else:
                print(f"❌ No se pudo publicar el tweet: {response.get('error')}")

            await asyncio.sleep(0.1)

    except asyncio.CancelledError:
        print(f"⏹️ Publicación de tweets cancelada para usuario ID: {user_id}.")

    print(f"✅ Publicación de tweets finalizada para usuario ID: {user_id}.")


async def start_tweet_fetcher():
    print('🚀 Iniciando el servicio de recolección de tweets...')
    # while True:
    #     await fetch_tweets_for_all_users()
    
    #     print("⏳ Esperando 5 minutos antes de la próxima búsqueda...")
    #     await asyncio.sleep(300)  


