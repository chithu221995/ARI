# app/db/connection.py

import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncEngine
import urllib.parse as up

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing")

# Convert postgres → asyncpg
raw = DATABASE_URL
raw = raw.replace("postgres://", "postgresql+asyncpg://")
raw = raw.replace("postgresql://", "postgresql+asyncpg://")

parsed = up.urlparse(raw)
qs = dict(up.parse_qsl(parsed.query))

# asyncpg ONLY accepts ssl=require
clean_qs = {}
if qs.get("sslmode") == "require":
    clean_qs["ssl"] = "require"
if qs.get("ssl") == "require":
    clean_qs["ssl"] = "require"

url = up.urlunparse(parsed._replace(
    query=up.urlencode(clean_qs)
))

# ensure ssl=require
if "ssl=require" not in url:
    url = url + ("&ssl=require" if "?" in url else "?ssl=require")

engine: AsyncEngine = create_async_engine(
    url,
    future=True,
    echo=False,
)