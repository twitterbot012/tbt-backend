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