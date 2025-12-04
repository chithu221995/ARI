from sqlalchemy import text
from app.db.connection import engine


async def pg_fetch_all(query: str):
    """Run SELECT and return list of dicts."""
    async with engine.connect() as conn:
        result = await conn.execute(text(query))
        rows = result.fetchall()
        return [dict(r._mapping) for r in rows]