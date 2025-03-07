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
from services.fetch_tweets import fetch_tweets_for_all_users
from services.fetch_tweets import post_tweets_for_all_users
from multiprocessing import Manager
import time

app = Flask(__name__)
app.config.from_object(Config)
CORS(app, origins=["http://localhost:3000"], supports_credentials=True)

manager = Manager()
fetching_event = manager.Event()
posting_event = manager.Event()
fetcher_thread = None
poster_thread = None

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
            while not fetching_event.is_set():
                try:
                    print("🔎 Buscando tweets...")
                    task = asyncio.create_task(fetch_tweets_for_all_users(fetching_event))
                    await task

                    if fetching_event.is_set():
                        break

                    print("⏳ Esperando hasta 30 segundos antes de la próxima búsqueda...")
                    for _ in range(30):  # Esperar en intervalos de 1 segundo para detectar el stop
                        if fetching_event.is_set():
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

                    print("⏳ Esperando hasta 30 segundos antes de la próxima publicación...")
                    for _ in range(30):  # Esperar en intervalos de 1 segundo para detectar el stop
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

    
@app.route("/start-fetch", methods=["POST"])
def start_fetch():
    global fetcher_thread

    if fetcher_thread is None or not fetcher_thread.is_alive():
        fetching_event.clear() 
        fetcher_thread = threading.Thread(target=start_tweet_fetcher, daemon=True)
        fetcher_thread.start() 
        return jsonify({"status": "started"}), 200
    else:
        return jsonify({"status": "already running"}), 400

@app.route("/stop-fetch", methods=["POST"])
def stop_fetch():
    global fetcher_thread

    if fetcher_thread is not None and fetcher_thread.is_alive():
        print("⏹️ Solicitando detener la recolección de tweets...")
        fetching_event.set() 

        fetcher_thread.join(timeout=10)

        if fetcher_thread.is_alive():
            print("⚠️ El hilo sigue activo, forzando su cierre...")
            fetcher_thread = None  

        return jsonify({"status": "stopped"}), 200
    else:
        return jsonify({"status": "not running"}), 400

@app.route("/status-fetch", methods=["GET"])
def status_fetch():
    global fetcher_thread

    if fetcher_thread is not None and fetcher_thread.is_alive():
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


if __name__ == "__main__":
    # Usamos `app.run()` con threaded=True para manejar múltiples solicitudes
    app.run(debug=True, threaded=True)
