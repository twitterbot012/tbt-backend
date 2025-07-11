import requests
from services.db_service import run_query, log_event
import logging
from routes.logs import log_usage
import uuid
from urllib.parse import urlparse
from supabase import create_client
import httpx
import mimetypes
import re

logging.basicConfig(level=logging.INFO)

mimetypes.add_type("video/quicktime", ".mov")
mimetypes.add_type("video/x-msvideo", ".avi")
mimetypes.add_type("video/x-matroska", ".mkv")
mimetypes.add_type("video/webm", ".webm")
mimetypes.add_type("video/mp4", ".mp4")
mimetypes.add_type("image/webp", ".webp")
mimetypes.add_type("image/png", ".png")
mimetypes.add_type("image/jpeg", ".jpg")
mimetypes.add_type("image/gif", ".gif")

SUPABASE_URL = "https://tmosrdszzpgfdbexstbu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRtb3NyZHN6enBnZmRiZXhzdGJ1Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczOTQ3NTMyOSwiZXhwIjoyMDU1MDUxMzI5fQ.cUiNxjRcnwuelk9XHbRiRgpL88U43OBJbum82vnQlk8" 
BUCKET_NAME = "images"
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_extraction_filter(user_id):
    query = f"SELECT extraction_filter FROM users WHERE id = {user_id}"  
    result = run_query(query, fetchone=True)
    return result[0] if result else None 


def get_rapidapi_key():
    query = "SELECT key FROM api_keys WHERE id = 3"
    result = run_query(query, fetchone=True)
    return result[0] if result else None


def convert_drive_view_to_direct(url):
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    if match:
        file_id = match.group(1)
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url


def get_extension_from_content_type(content_type):
    content_type_clean = content_type.split(";")[0].strip()
    ext = mimetypes.guess_extension(content_type_clean)
    if ext:
        return ext
    fallback_map = {
        "video/quicktime": ".mov",
        "video/x-msvideo": ".avi",
        "video/x-matroska": ".mkv",
        "video/webm": ".webm",
        "video/mp4": ".mp4",
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "application/octet-stream": ".bin",
    }
    return fallback_map.get(content_type_clean, ".bin")


def upload_media_to_supabase_from_url(url, folder="tweets"):
    try:
        response = httpx.get(url, follow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"No se pudo descargar media: {url}")
        file_bytes = response.content
        content_type = response.headers.get("Content-Type", "application/octet-stream")
        ext = get_extension_from_content_type(content_type)
        filename = f"{folder}/{uuid.uuid4()}{ext}"
        upload_url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_NAME}/{filename}"
        headers = {
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": content_type
        }
        upload_response = httpx.put(upload_url, content=file_bytes, headers=headers)
        if upload_response.status_code != 200:
            raise Exception(f"Error al subir a Supabase: {upload_response.text}")
        public_url = f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET_NAME}/{filename}"
        return public_url, filename
    except Exception as e:
        raise Exception(f"❌ Error en subida a Supabase: {e}")


def delete_from_supabase(path):
    try:
        supabase.storage.from_(BUCKET_NAME).remove([path])
        print(f"🗑️ Archivo eliminado de Supabase: {path}")
    except Exception as e:
        print(f"⚠️ No se pudo borrar de Supabase: {e}")


def post_tweet(user_id, tweet_text, media_urls=None):
    if len(tweet_text) > 270:
        print(f"⚠️ Tweet demasiado largo ({len(tweet_text)} caracteres). Se salta publicación.")
        return {"error": "El tweet supera el límite de 270 caracteres y fue descartado."}, 400

    extraction_filter = get_extraction_filter(user_id)
    if extraction_filter in ["cb2", "cb3", "cb4"] and "https://" in tweet_text:
        result = run_query(f"SELECT session FROM users WHERE id = {user_id}", fetchone=True)
        if not result:
            return {"error": "Usuario no encontrado"}, 404
        
        session = result[0]
        rapidapi_key = get_rapidapi_key()
        
        if not rapidapi_key:
            return {"error": "No se pudo obtener la API Key de RapidAPI"}, 500
        
        if isinstance(tweet_text, list):
            tweet_text = " ".join(tweet_text)
        tweet_text = str(tweet_text).replace("'", "''")
        
        try:
            internal_id = run_query(f"INSERT INTO posted_tweets (user_id, tweet_text, created_at) VALUES ({user_id}, '{tweet_text}', NOW()) RETURNING id", fetchone=True)[0]
        except Exception as db_error:
            return {"error": "No se pudo guardar el tweet en la base de datos"}, 500
        
        media_ids = []
        if media_urls:
            for media_url in media_urls[:4]:
                try:
                    media_url = convert_drive_view_to_direct(media_url)
                    public_url, supabase_path = upload_media_to_supabase_from_url(media_url)
                    is_video = any(public_url.lower().endswith(ext) for ext in [".mp4", ".mov", ".avi", ".mkv"])
                    upload_url = f"https://twttrapi.p.rapidapi.com/upload-video" if is_video else f"https://twttrapi.p.rapidapi.com/upload-image"
                    param_name = "video_url" if is_video else "image_url"
                    data = f"{param_name}={public_url}"
                    print("Payload:", data)
                    print("Upload URL:", upload_url)


                    headers = {
                        "x-rapidapi-key": rapidapi_key,
                        "x-rapidapi-host": "twttrapi.p.rapidapi.com",
                        "twttr-session": session,
                        "Content-Type": "application/x-www-form-urlencoded"
                    }
                    print("Headers:", headers)

                    print(f'')
                    resp = requests.post(upload_url, data=data, headers=headers)
                    print("Response:", resp.text)
                    print(f'{resp} RESP RESP RERSP')
                    log_usage("RAPIDAPI")
                    if resp.status_code == 200:
                        media_id = resp.json().get("media_id")
                        print(f'mediaid {media_id}')
                        print(f'mediaid2 {resp.json()}')

                        if media_id:
                            media_ids.append(media_id)
                    delete_from_supabase(supabase_path)
                except Exception as e:
                    print(f"❌ Excepción subiendo media: {e}")
        payload = f"tweet_text={tweet_text}"
        
        if media_ids:
            payload += f"&media_id={','.join(media_ids)}"
        tweet_resp = requests.post("https://twttrapi.p.rapidapi.com/create-tweet", data=payload, headers={
            "x-rapidapi-key": rapidapi_key,
            "x-rapidapi-host": "twttrapi.p.rapidapi.com",
            "Content-Type": "application/x-www-form-urlencoded",
            "twttr-session": session
        })
        
        log_usage("RAPIDAPI")
        
        if tweet_resp.status_code == 200 and "data" in tweet_resp.json():
            try:
                tweet_data = tweet_resp.json()["data"]["create_tweet"]["tweet_result"]["result"]
                tweet_id = tweet_data["rest_id"]
                tweet_url = f"https://twitter.com/{tweet_data['core']['user_result']['result']['legacy']['screen_name']}/status/{tweet_id}"
                run_query(f"UPDATE posted_tweets SET tweet_id = '{tweet_id}' WHERE id = {internal_id}")
                return {"message": "Tweet publicado exitosamente", "tweet_id": tweet_id, "tweet_url": tweet_url}, 200

            except KeyError as e:
                print("❌ Error al parsear respuesta del tweet:", tweet_resp.json())
                return {"error": f"Respuesta inesperada al crear tweet: {e}"}, 500
        return {"error": "No se pudo publicar el tweet"}, tweet_resp.status_code