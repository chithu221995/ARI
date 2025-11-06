"""
Migration: Add run_errors table for MTTR tracking.
"""
from __future__ import annotations
import logging
import aiosqlite

log = logging.getLogger("ari.migrations")


async def migrate_add_run_errors(db_path: str) -> None:
    """
    Idempotent migration to add run_errors table.
    
    Args:
        db_path: Path to SQLite database
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS run_errors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_type TEXT NOT NULL,
        ticker TEXT,
        provider TEXT,
        event TEXT,
        error_message TEXT,
        created_at TEXT NOT NULL,
        resolved_at TEXT,
        resolved_by TEXT
    )
    """
    
    create_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_run_errors_created ON run_errors(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_run_errors_provider_event ON run_errors(provider, event)",
        "CREATE INDEX IF NOT EXISTS idx_run_errors_resolved ON run_errors(resolved_at)",
        "CREATE INDEX IF NOT EXISTS idx_run_errors_job_ticker ON run_errors(job_type, ticker)",
    ]
    
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute(create_table_sql)
            
            for idx_sql in create_indexes:
                await db.execute(idx_sql)
            
            await db.commit()
        
        log.info("migrate_add_run_errors: table and indexes created successfully")
    except Exception as e:
        log.exception("migrate_add_run_errors: migration failed")
        raise