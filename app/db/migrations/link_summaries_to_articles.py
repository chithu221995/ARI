"""
Migration to add url column to summaries table for linking to articles.
This enables tracking which articles were actually sent in emails.
"""
import aiosqlite
import logging

log = logging.getLogger("ari.migrations.link_summaries")


async def migrate_link_summaries_to_articles(db_path: str) -> None:
    """
    Add url column to summaries if missing, and backfill from articles table.
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            # Check if url column exists
            cursor = await db.execute("PRAGMA table_info(summaries)")
            columns = await cursor.fetchall()
            await cursor.close()
            
            column_names = [col[1] for col in columns]
            
            if "url" not in column_names:
                log.info("Adding url column to summaries table")
                await db.execute("ALTER TABLE summaries ADD COLUMN url TEXT")
                await db.commit()
                log.info("url column added successfully")
                
                # Backfill url from articles using item_url_hash match
                log.info("Backfilling urls from articles table")
                await db.execute("""
                    UPDATE summaries
                    SET url = (
                        SELECT a.url 
                        FROM articles a 
                        WHERE a.url_hash = summaries.item_url_hash
                        LIMIT 1
                    )
                    WHERE url IS NULL
                """)
                await db.commit()
                
                # Count successful backfills
                cursor = await db.execute(
                    "SELECT COUNT(*) FROM summaries WHERE url IS NOT NULL"
                )
                count = (await cursor.fetchone())[0]
                await cursor.close()
                log.info(f"Backfilled {count} summary urls from articles")
            else:
                log.info("url column already exists in summaries, skipping migration")
                
    except Exception as e:
        log.exception(f"Migration failed: {e}")
        raise