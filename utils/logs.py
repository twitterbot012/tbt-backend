from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Argentina/Buenos_Aires")

def now_hhmm() -> str:
    return datetime.now(TZ).strftime("%H:%M")
