from __future__ import annotations
import logging
import aiosqlite
from app.core.settings import settings

log = logging.getLogger("ari.cache.db_init")


async def ensure_feedback_tables():
    """
    Create email_events and email_items tables for tracking email interactions.
    Safe to call multiple times - uses CREATE TABLE IF NOT EXISTS.
    """
    db_path = getattr(settings, "SQLITE_PATH", None) or "./ari.db"
    
    log.info("ensure_feedback_tables: initializing feedback schema at %s", db_path)
    
    try:
        async with aiosqlite.connect(db_path) as db:
            # Create email_events table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS email_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    rating INTEGER,
                    comment TEXT,
                    item_url_hash TEXT,
                    user_agent TEXT,
                    ip TEXT,
                    email_sent_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Create indexes for email_events
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_email_events_email_created
                ON email_events (email, created_at);
            """)
            
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_email_events_url_created
                ON email_events (item_url_hash, created_at);
            """)
            
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_email_events_type_created
                ON email_events (event_type, created_at);
            """)
            
            # Create email_items table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS email_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email_log_id INTEGER NOT NULL,
                    ticker TEXT,
                    item_url TEXT,
                    item_url_hash TEXT,
                    domain TEXT,
                    published_at DATETIME,
                    sent_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Create indexes for email_items
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_email_items_log
                ON email_items (email_log_id);
            """)
            
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_email_items_ticker_sent
                ON email_items (ticker, sent_at);
            """)
            
            await db.commit()
            
            log.info("ensure_feedback_tables: feedback schema initialized successfully")
            
    except Exception as e:
        log.exception("ensure_feedback_tables: failed to initialize feedback schema")
        raise


async def init_all_tables():
    """
    Initialize all database tables including feedback tables.
    This is the main entry point for database initialization.
    """
    log.info("init_all_tables: starting database initialization")
    
    # Initialize feedback tables
    await ensure_feedback_tables()
    
    # Add any other table initialization here
    # await ensure_cache_tables()
    # await ensure_user_tables()
    # etc.
    
    log.info("init_all_tables: database initialization complete")