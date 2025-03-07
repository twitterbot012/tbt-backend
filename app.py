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

app = Flask(__name__)
app.config.from_object(Config)
CORS(app, origins=["http://localhost:3000"], supports_credentials=True)

# Variables globales para el hilo y el evento
fetcher_thread = None
fetching_event = threading.Event()
poster_thread = None
posting_event = threading.Event()

# Registrar blueprints
app.register_blueprint(accounts_bp, url_prefix="/api")
app.register_blueprint(auth_bp, url_prefix="/auth")
app.register_blueprint(logs_bp, url_prefix="/logs")
app.register_blueprint(monitored_bp, url_prefix="/api")
app.register_blueprint(tweets_bp, url_prefix="/tweets")

@app.route("/")
def home():
    return {"message": "Bienvenido a la API de Twitter Bot"}

def start_tweet_fetcher():
    print('🚀 Iniciando el servicio de recolección de tweets...')

    async def fetch_loop():
        with app.app_context():
            while not fetching_event.is_set():
                try:
                    task = asyncio.create_task(fetch_tweets_for_all_users(fetching_event))
                    await task 
                    if fetching_event.is_set():
                        break 
                    print("⏳ Esperando 30 segundos antes de la próxima búsqueda...")
                    await asyncio.sleep(30) 
                except asyncio.CancelledError:
                    print("⏹️ Tarea cancelada por solicitud de detención.")
                    break
                except Exception as e:
                    print(f"❌ Error en fetch_loop: {e}")
                    break

        print("⏹️ Servicio de recolección detenido.")

    asyncio.run(fetch_loop())
    
    
def start_tweet_poster():
    print('🚀 Iniciando el servicio de publicación de tweets...')

    async def post_loop():
        with app.app_context():
            while not posting_event.is_set():
                try:
                    task = asyncio.create_task(post_tweets_for_all_users(posting_event))
                    await task
                    if posting_event.is_set():
                        break
                    print("⏳ Esperando 30 segundos antes de la próxima publicación...")
                    await asyncio.sleep(30)
                except asyncio.CancelledError:
                    print("⏹️ Tarea cancelada por solicitud de detención.")
                    break
                except Exception as e:
                    print(f"❌ Error en post_loop: {e}")
                    break

        print("⏹️ Servicio de publicación detenido.")
        
    asyncio.run(post_loop())

    
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
        fetching_event.set() 
        fetcher_thread.join(timeout=5)
        return jsonify({"status": "stopped"}), 200
    else:
        return jsonify({"status": "not running", "message": "El proceso de recolección no está en ejecución."}), 400

@app.route("/status-fetch", methods=["GET"])
def status_fetch():
    """
    Endpoint para verificar el estado del proceso de recolección.
    """
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
        posting_event.set()
        poster_thread.join(timeout=5)
        return jsonify({"status": "stopped"}), 200
    else:
        return jsonify({"status": "not running", "message": "El proceso de publicación no está en ejecución."}), 400


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