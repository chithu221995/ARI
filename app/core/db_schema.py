import aiosqlite

async def init_db(sqlite_path: str) -> None:
    """
    Ensure minimal DB schema exists. Creates tables and indexes if missing.
    """
    async with aiosqlite.connect(sqlite_path) as db:
        await db.execute("PRAGMA journal_mode=WAL;")

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY,
                url TEXT,
                url_hash TEXT UNIQUE,
                ticker TEXT,
                source TEXT,
                title TEXT,
                published_at TEXT,
                lang TEXT,
                content TEXT,
                text_hash TEXT,
                created_at TEXT
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS summaries (
                id INTEGER PRIMARY KEY,
                ticker TEXT,
                item_url_hash TEXT,
                bullets TEXT,
                why_it_matters TEXT,
                sentiment TEXT,
                created_at TEXT
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS ingest_runs (
                id INTEGER PRIMARY KEY,
                run_type TEXT,
                started_at TEXT,
                finished_at TEXT,
                count INTEGER,
                ok INTEGER DEFAULT 1
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS email_logs (
                id INTEGER PRIMARY KEY,
                to_email TEXT,
                subject TEXT,
                sent_at TEXT,
                items_count INTEGER,
                provider TEXT,
                ok INTEGER DEFAULT 1,
                error TEXT
            )
            """
        )

        # Indexes
        await db.execute("CREATE INDEX IF NOT EXISTS idx_articles_url_hash ON articles(url_hash)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_articles_ticker_published ON articles(ticker, published_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_summaries_item_url_hash ON summaries(item_url_hash)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ingest_runs_type_started ON ingest_runs(run_type, started_at)")

        await db.commit()