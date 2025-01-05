import asyncio
import aiosqlite
from pathlib import Path

async def init_database():
    db_path = Path("race_cache.db")
    
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS race_cache (
                date TEXT PRIMARY KEY,
                data BLOB,
                updated_at TIMESTAMP
            )
        """)
        await db.commit()
    print("数据库初始化完成")

if __name__ == "__main__":
    asyncio.run(init_database()) 