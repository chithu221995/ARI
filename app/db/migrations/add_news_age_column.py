"""
Migration to add news_age column to articles table.
This stores the age of the article (in hours) at the time it was fetched.
"""
import aiosqlite
import logging
import random

log = logging.getLogger("ari.migrations.add_news_age")


async def migrate_add_news_age_column(db_path: str) -> None:
    """
    Add news_age REAL column to articles table and populate with random test data.
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            # Check if column already exists
            cursor = await db.execute("PRAGMA table_info(articles)")
            columns = await cursor.fetchall()
            await cursor.close()
            
            column_names = [col[1] for col in columns]
            
            if "news_age" not in column_names:
                log.info("Adding news_age column to articles table")
                await db.execute("ALTER TABLE articles ADD COLUMN news_age REAL")
                await db.commit()
                log.info("news_age column added successfully")
                
                # Populate existing rows with random test data (0.5 to 72 hours)
                cursor = await db.execute("SELECT id FROM articles")
                article_ids = [row[0] for row in await cursor.fetchall()]
                await cursor.close()
                
                if article_ids:
                    log.info(f"Populating {len(article_ids)} existing articles with random news_age values")
                    
                    updates = []
                    for article_id in article_ids:
                        # Random age between 0.5 hours (30 min) and 72 hours (3 days)
                        random_age = round(random.uniform(0.5, 72.0), 2)
                        updates.append((random_age, article_id))
                    
                    await db.executemany(
                        "UPDATE articles SET news_age = ? WHERE id = ?",
                        updates
                    )
                    await db.commit()
                    log.info(f"Updated {len(updates)} articles with random news_age values (0.5-72 hours)")
            else:
                log.info("news_age column already exists, skipping migration")
                
    except Exception as e:
        log.exception(f"Migration failed: {e}")
        raise