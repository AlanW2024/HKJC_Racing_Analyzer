from datetime import datetime
from typing import Optional, List
import aiosqlite
import pickle
from .models import RaceData

class DataCache:
    """数据缓存管理"""
    def __init__(self, db_path: str = "race_cache.db"):
        self.db_path = db_path
        self.memory_cache = {}
        self.cache_ttl = 3600
    
    async def init(self):
        """初始化数据库"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS race_cache (
                    date TEXT PRIMARY KEY,
                    data BLOB,
                    updated_at TIMESTAMP
                )
            """)
            await db.commit() 