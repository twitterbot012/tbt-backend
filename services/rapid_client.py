# services/rapidapi_client.py
import http.client
import urllib.parse
import asyncio
from typing import Optional, Dict, Tuple

HOST = "twttrapi.p.rapidapi.com"
BASE_HEADERS = {
    "x-rapidapi-host": HOST,
}

def _sync_request(
    method: str,
    path: str,
    api_key: str,
    *,
    data: Optional[str] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Tuple[int, bytes]:
    """Petición síncrona con http.client (bloqueante)."""
    conn = http.client.HTTPSConnection(HOST)
    headers = BASE_HEADERS.copy()
    headers["x-rapidapi-key"] = api_key
    if extra_headers:
        headers.update(extra_headers)

    conn.request(method, path, body=data, headers=headers)
    res = conn.getresponse()
    status, raw = res.status, res.read()
    conn.close()
    return status, raw

async def request(
    method: str,
    path: str,
    api_key: str,
    *,
    params: Optional[Dict[str, str]] = None,
    data: Optional[str] = None,
    extra_headers: Optional[Dict[str, str]] = None,
    run_in_executor: bool = True,
) -> Tuple[int, bytes]:
    """Wrapper asíncrono: ejecuta _sync_request en un thread."""
    if params:
        path = f"{path}?{urllib.parse.urlencode(params)}"

    if run_in_executor:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            _sync_request,
            method,
            path,
            api_key,
            data,
            extra_headers,
        )
    else:
        # para usarlo desde código no-async si lo necesitaras
        return _sync_request(method, path, api_key, data=data, extra_headers=extra_headers)
