from flask import Flask, jsonify
from flask_cors import CORS
from routes.auth import auth_bp
from routes.logs import logs_bp
from routes.accounts import accounts_bp
from routes.monitored_users import monitored_bp
from routes.tweets import tweets_bp
from config import Config
import threading
import asyncio
from services.fetch_tweets import fetch_tweets_for_all_users, fetch_tweets_for_single_user, fetch_random_tasks_for_all_users, fetch_random_tasks_for_user, post_tweets_for_all_users, post_tweets_for_single_user, old_fetch_tweets_for_all_users
from multiprocessing import Manager
import time
import os
from services.db_service import log_event
import traceback
from utils.logs import now_hhmm

app = Flask(__name__)
app.config.from_object(Config)
cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")
CORS(app, origins=cors_origins, supports_credentials=True)

manager = Manager()
fetching_event = manager.Event()
old_fetching_event = manager.Event()
posting_event = manager.Event()
fetcher_thread = None
old_fetcher_thread = None
poster_thread = None
user_process_threads = {}
user_process_events = {}

# Debug helper
DEBUG_ENABLED = os.getenv("DEBUG_TBOT", "1") == "1"
def dbg(message: str):
    if DEBUG_ENABLED:
        try:
            print(f"[DBG {time.strftime('%H:%M:%S')}] {message}")
        except Exception:
            print(f"[DBG] {message}")

app.register_blueprint(accounts_bp, url_prefix="/api")
app.register_blueprint(auth_bp, url_prefix="/auth")
app.register_blueprint(logs_bp, url_prefix="/logs")
app.register_blueprint(monitored_bp, url_prefix="/api")
app.register_blueprint(tweets_bp, url_prefix="/tweets")

@app.route("/")
def home():
    return {"message": "Bienvenido a la API de Twitter Bot"}

def start_tweet_fetcher():
    """
    Inicia la recolección de tweets en un bucle hasta que se active `fetching_event`
    """
    print('🚀 Iniciando el servicio de recolección de tweets...')
    
    async def fetch_loop():
        with app.app_context():
            while not old_fetching_event.is_set():
                try:
                    print("🔎 Buscando tweets...")
                    task = asyncio.create_task(old_fetch_tweets_for_all_users(old_fetching_event))
                    await task

                    if old_fetching_event.is_set():
                        break

                    print("⏳ Esperando 30 segundos antes de la próxima búsqueda...")
                    for _ in range(14400):  
                        if old_fetching_event.is_set():
                            break
                        time.sleep(1)

                except asyncio.CancelledError:
                    print("⏹️ Tarea cancelada por solicitud de detención.")
                    break
                except Exception as e:
                    print(f"❌ Error en fetch_loop: {e}")
                    break

        print("⏹️ Servicio de recolección detenido.")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(fetch_loop())
    loop.close()

    
def start_tweet_poster():
    """
    Inicia la publicación de tweets en un bucle hasta que se active `posting_event`
    """
    print('🚀 Iniciando el servicio de publicación de tweets...')
    
    async def post_loop():
        with app.app_context():
            while not posting_event.is_set():
                try:
                    print("📢 Publicando tweets...")
                    task = asyncio.create_task(post_tweets_for_all_users(posting_event))
                    await task

                    if posting_event.is_set():
                        break

                    print("⏳ Esperando 1 segundo antes de la próxima publicación...")
                    for _ in range(10):  # Esperar en intervalos de 1 segundo para detectar el stop
                        if posting_event.is_set():
                            break
                        time.sleep(1)

                except asyncio.CancelledError:
                    print("⏹️ Tarea cancelada por solicitud de detención.")
                    break
                except Exception as e:
                    print(f"❌ Error en post_loop: {e}")
                    break

        print("⏹️ Servicio de publicación detenido.")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(post_loop())
    loop.close()


def start_tweet_service():
    """
    Starts the continuous tweet collection and posting service
    """
    print('🚀 Starting continuous tweet service...')
    dbg(f"Thread starting service: daemon loop will run. fetching_event.is_set()={fetching_event.is_set()}")

    async def service_loop():
        last_fetch_ts = 0.0
        last_random_ts = 0.0
        with app.app_context():
            while not fetching_event.is_set():
                try:
                    do_fetch = (last_fetch_ts == 0.0) or ((time.time() - last_fetch_ts) >= 6 * 60 * 60)
                    do_random = (last_random_ts == 0.0) or ((time.time() - last_random_ts) >= 6 * 60 * 60)
                    dbg(f"service_loop tick: do_fetch={do_fetch}, do_random={do_random}, last_fetch_ts={last_fetch_ts}, last_random_ts={last_random_ts}")

                    if do_fetch or do_random:
                        log_event(None, "BATCH", f"start at {now_hhmm()}")

                    # --- FETCH cada 6h ---
                    if do_fetch:
                        print("🔎 Starting fetch (6h window)...")
                        dbg("creating fetch_tweets_for_all_users task…")
                        fetch_task = asyncio.create_task(fetch_tweets_for_all_users(fetching_event))
                        await fetch_task
                        dbg("fetch_tweets_for_all_users task done")
                        if fetching_event.is_set():
                            break
                        last_fetch_ts = time.time()
                        print("✅ Fetch complete.")

                    # --- POST continuo (reparte dentro de la franja) ---
                    dbg("creating post_tweets_for_all_users task…")
                    post_task = asyncio.create_task(post_tweets_for_all_users(fetching_event))
                    await post_task
                    dbg("post_tweets_for_all_users task done")
                    if fetching_event.is_set():
                        break
                    print("✅ Posting cycle complete.")

                    # --- RANDOM ACTIONS cada 6h ---
                    if do_random:
                        print("🎲 Starting random actions (6h window)...")
                        dbg("creating fetch_random_tasks_for_all_users task…")
                        random_task = asyncio.create_task(fetch_random_tasks_for_all_users(fetching_event))
                        await random_task
                        dbg("fetch_random_tasks_for_all_users task done")
                        if fetching_event.is_set():
                            break
                        last_random_ts = time.time()
                        print("✅ Random actions complete.")

                    if do_fetch or do_random:
                        log_event(None, "BATCH", f"end at {now_hhmm()}")

                    print("⏳ Full cycle done. Waiting 60s before restart...")
                    for _ in range(60):
                        if fetching_event.is_set():
                            break
                        await asyncio.sleep(1)  # non-blocking

                except asyncio.CancelledError:
                    print("⏹️ Service cancelled by stop request.")
                    log_event(None, "BATCH", f"end at {now_hhmm()}")
                    break
                except Exception as e:
                    print(f"❌ Error in service_loop: {e}")
                    try:
                        dbg(traceback.format_exc())
                    except Exception:
                        pass
                    log_event(None, "BATCH", f"end at {now_hhmm()}")
                    break

        print("⏹️ Continuous service stopped.")
    
    # Run the async service loop in this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(service_loop())
    loop.close()
    
@app.route("/start-fetch", methods=["POST"])
def start_fetch():
    global fetcher_thread
    dbg("/start-fetch called")
    for user_id, thread in user_process_threads.items():
        if thread.is_alive():
            print(f"⏹️ Deteniendo proceso individual de usuario {user_id} por inicio de proceso global.")
            user_process_events[user_id].set()
            thread.join(timeout=5)

    if fetcher_thread is None or not fetcher_thread.is_alive():
        fetching_event.clear() 
        fetcher_thread = threading.Thread(target=start_tweet_service, daemon=True, name="tweet-service")
        fetcher_thread.start() 
        dbg(f"fetcher_thread started: ident={fetcher_thread.ident}, alive={fetcher_thread.is_alive()}")
        return jsonify({"status": "started"}), 200
    else:
        dbg("/start-fetch ignored: already running")
        return jsonify({"status": "already running"}), 400


@app.route("/stop-fetch", methods=["POST"])
def stop_fetch():
    global fetcher_thread
    dbg("/stop-fetch called")

    if fetcher_thread is not None and fetcher_thread.is_alive():
        print("⏹️ Solicitando detener la recolección de tweets...")
        fetching_event.set() 

        fetcher_thread.join(timeout=10)

        if fetcher_thread.is_alive():
            print("⚠️ El hilo sigue activo, forzando su cierre...")
            fetcher_thread = None  
        else:
            dbg("fetcher_thread joined and stopped successfully")

        return jsonify({"status": "stopped"}), 200
    else:
        dbg("/stop-fetch ignored: not running")
        return jsonify({"status": "not running"}), 400


@app.route("/status-fetch", methods=["GET"])
def status_fetch():
    global fetcher_thread
    alive = fetcher_thread is not None and fetcher_thread.is_alive()
    dbg(f"/status-fetch: alive={alive}, ident={(fetcher_thread.ident if fetcher_thread else None)}, event_set={fetching_event.is_set()}")

    if alive:
        return jsonify({"status": "running"}), 200
    else:
        return jsonify({"status": "stopped"}), 200


@app.route("/start-post", methods=["POST"])
def start_post():
    global poster_thread

    if poster_thread is None or not poster_thread.is_alive():
        posting_event.clear()
        poster_thread = threading.Thread(target=start_tweet_poster, daemon=True)
        poster_thread.start()
        return jsonify({"status": "started"}), 200
    else:
        return jsonify({"status": "already running"}), 400


@app.route("/stop-post", methods=["POST"])
def stop_post():
    global poster_thread

    if poster_thread is not None and poster_thread.is_alive():
        print("⏹️ Solicitando detener la publicación de tweets...")
        posting_event.set() 
        poster_thread.join(timeout=10)

        if poster_thread.is_alive():
            print("⚠️ El hilo sigue activo, forzando su cierre...")
            poster_thread = None

        return jsonify({"status": "stopped"}), 200
    else:
        return jsonify({"status": "not running"}), 400


@app.route("/status-post", methods=["GET"])
def status_post():
    global poster_thread

    if poster_thread is not None and poster_thread.is_alive():
        return jsonify({"status": "running"}), 200
    else:
        return jsonify({"status": "stopped"}), 200


@app.route("/start-process/<user_id>", methods=["POST"])
def start_process_user(user_id):
    if user_id not in user_process_threads or not user_process_threads[user_id].is_alive():
        event = threading.Event()
        user_process_events[user_id] = event
        thread = threading.Thread(target=start_service_for_user, args=(user_id, event), daemon=True)
        user_process_threads[user_id] = thread
        thread.start()
        return jsonify({"status": "started"}), 200
    else:
        return jsonify({"status": "already running"}), 400


@app.route("/stop-process/<user_id>", methods=["POST"])
def stop_process_user(user_id):
    if user_id in user_process_threads and user_process_threads[user_id].is_alive():
        user_process_events[user_id].set()
        user_process_threads[user_id].join(timeout=10)
        return jsonify({"status": "stopped"}), 200
    else:
        return jsonify({"status": "not running"}), 400


@app.route("/status-process/<user_id>", methods=["GET"])
def status_process_user(user_id):
    if user_id in user_process_threads and user_process_threads[user_id].is_alive():
        return jsonify({"status": "running"}), 200
    else:
        return jsonify({"status": "stopped"}), 200


def start_service_for_user(user_id, process_event):
    """
    Servicio continuo de extracción y publicación para un usuario específico.
    """
    print(f'🚀 Iniciando servicio de FETCH + POST para usuario ID: {user_id}...')

    async def service_loop():
        last_fetch_ts = 0.0
        last_random_ts = 0.0
        with app.app_context():
            while not process_event.is_set():
                try:
                    do_fetch = (last_fetch_ts == 0.0) or ((time.time() - last_fetch_ts) >= 6 * 60 * 60)
                    do_random = (last_random_ts == 0.0) or ((time.time() - last_random_ts) >= 6 * 60 * 60)

                    # --- FETCH cada 6h ---
                    if do_fetch:
                        print(f"🔎 Extrayendo tweets para usuario ID: {user_id} (6h)...")
                        fetch_task = asyncio.create_task(fetch_tweets_for_single_user(user_id, process_event))
                        await fetch_task
                        if process_event.is_set():
                            break
                        last_fetch_ts = time.time()

                    # --- POST continuo ---
                    print(f"📢 Publicando tweets para usuario ID: {user_id}...")
                    post_task = asyncio.create_task(post_tweets_for_single_user(user_id, process_event))
                    await post_task

                    if process_event.is_set():
                        break

                    # --- RANDOM cada 6h ---
                    if do_random:
                        print(f"📢 Random actions para usuario ID: {user_id} (6h)...")
                        random_task = asyncio.create_task(fetch_random_tasks_for_user(user_id, process_event))
                        await random_task
                        if process_event.is_set():
                            break
                        last_random_ts = time.time()

                    print(f"⏳ Ciclo completo para usuario {user_id}. Esperando 60s antes de reiniciar...")
                    for _ in range(60):
                        if process_event.is_set():
                            break
                        await asyncio.sleep(1)

                except asyncio.CancelledError:
                    print(f"⏹️ Servicio cancelado para usuario ID: {user_id}.")
                    break
                except Exception as e:
                    print(f"❌ Error en service_loop usuario {user_id}: {e}")
                    break

        print(f"⏹️ Servicio detenido para usuario ID: {user_id}.")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(service_loop())
    loop.close()


# OLD

@app.route("/old/stop-fetch", methods=["POST"])
def old_stop_fetch():
    global old_fetcher_thread

    if old_fetcher_thread is not None and old_fetcher_thread.is_alive():
        print("⏹️ Solicitando detener la recolección de tweets...")
        old_fetching_event.set() 

        old_fetcher_thread.join(timeout=10)

        if old_fetcher_thread.is_alive():
            print("⚠️ El hilo sigue activo, forzando su cierre...")
            old_fetcher_thread = None  

        return jsonify({"status": "stopped"}), 200
    else:
        return jsonify({"status": "not running"}), 400


@app.route("/old/status-fetch", methods=["GET"])
def old_status_fetch():
    global old_fetcher_thread

    if old_fetcher_thread is not None and old_fetcher_thread.is_alive():
        return jsonify({"status": "running"}), 200
    else:
        return jsonify({"status": "stopped"}), 200


@app.route("/old/start-fetch", methods=["POST"])
def old_start_fetch():
    global old_fetcher_thread
    for user_id, thread in user_process_threads.items():
        if thread.is_alive():
            print(f"⏹️ Deteniendo proceso individual de usuario {user_id} por inicio de proceso global.")
            user_process_events[user_id].set()
            thread.join(timeout=5)

    if old_fetcher_thread is None or not old_fetcher_thread.is_alive():
        old_fetching_event.clear() 
        old_fetcher_thread = threading.Thread(target=start_tweet_fetcher, daemon=True)
        old_fetcher_thread.start() 
        return jsonify({"status": "started"}), 200
    else:
        return jsonify({"status": "already running"}), 400


if __name__ == "__main__":
    # manager = Manager()
    # fetching_event = manager.Event()
    # old_fetching_event = manager.Event()
    # posting_event = manager.Event()
    # fetcher_thread = None
    # old_fetcher_thread = None
    # poster_thread = None
    # user_process_threads = {}
    # user_process_events = {}
    
    app.run(debug=True, threaded=True)